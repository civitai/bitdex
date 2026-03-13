# Phase C: Time Handling -- Spec Compliance Audit

**Auditor:** Claude Opus 4.6
**Date:** 2026-02-21
**Spec Source:** `docs/plans/roadmap-performance-and-persistence.md`, Phase C (lines 221-272)

---

## Summary

| Item | Description | Status |
|------|-------------|--------|
| C1 | Deferred alive tracking | PARTIAL |
| C2 | Range bucket bitmaps | PARTIAL |
| C3 | Query parser bucket snapping | PARTIAL |
| C4 | Config schema extension | COMPLIANT |
| C5 | Cache key stabilization | COMPLIANT |
| C6 | Tests: Deferred alive lifecycle | PARTIAL |
| C7 | Tests: Bucket snapping | PARTIAL |

**Overall Phase C Assessment:** PARTIAL -- Core data structures and algorithms are implemented and well-tested at the unit level. The critical gap is the absence of integration between these components and `ConcurrentEngine`, meaning no background timer activates deferred slots, no background timer rebuilds bucket bitmaps, bucket managers are never wired into query execution at the engine level, and no integration tests exercise the end-to-end lifecycle.

---

## C1: Deferred Alive Tracking

**Status:** PARTIAL

### What Exists

**`src/slot.rs` (lines 32-243):** The `SlotAllocator` has full deferred alive infrastructure:

```rust
// slot.rs:34
deferred: BTreeMap<u64, Vec<u32>>,
```

- `schedule_alive(slot, activate_at_unix)` (line 202): Stores `(slot, scheduled_time)` in a `BTreeMap<u64, Vec<u32>>`. Updates the high-water mark but does NOT set the alive bit.
- `activate_due(now_unix)` (line 215): Iterates all BTreeMap keys `<= now_unix`, removes them, sets the alive bit for each slot, returns the list of newly-activated slots.
- `deferred_count()` (line 236): Returns the total number of slots awaiting activation.
- `is_deferred(slot)` (line 241): Checks if a specific slot is in the pending set.

### Gap 1: No background timer in `ConcurrentEngine`

The spec requires: *"Background timer checks pending set every N seconds, activates arrived documents."*

Searching `concurrent_engine.rs` for `activate_due`, `schedule_alive`, `deferred`, or `TimeBucket` produces zero matches. Neither the flush thread nor the merge thread calls `activate_due()`. There is no periodic timer that checks the deferred set and activates documents whose scheduled time has arrived.

### Gap 2: Write path does not route deferred_alive documents

The spec says: *"On insert, if a `deferred_alive` field has a future value, store (slot, scheduled_time) in pending set. Don't set alive bit."*

Searching `mutation.rs` for `deferred_alive`, `schedule_alive`, or `behaviors` shows no references to the config's `deferred_alive` flag. The `diff_document` function always emits an `AliveInsert` mutation op -- it never checks whether a field configured with `deferred_alive: true` has a future value and should instead emit a deferred scheduling op.

The `write_coalescer.rs` similarly has no `DeferredAlive` or `ScheduleAlive` mutation op variant.

### Impact

Documents with future `publishedAt` values will be immediately visible in query results. The entire point of C1 -- invisible future-dated content until its scheduled time -- is non-functional at the engine level. The `SlotAllocator` primitives are correct but never invoked by the write or background processing paths.

---

## C2: Range Bucket Bitmaps

**Status:** PARTIAL

### What Exists

**`src/time_buckets.rs` (lines 1-381):** Full `TimeBucket` and `TimeBucketManager` implementation:

```rust
// time_buckets.rs:8-19
pub struct TimeBucket {
    pub name: String,
    pub duration_secs: u64,
    pub refresh_interval_secs: u64,
    bitmap: RoaringBitmap,
    last_refreshed: u64,
}
```

- `TimeBucketManager::new(field_name, bucket_configs)` (line 57): Constructs from config, sorts buckets by duration ascending.
- `needs_refresh(now)` (line 33): Returns true if `now - last_refreshed >= refresh_interval_secs`.
- `refresh_due(now)` (line 86): Returns all bucket names needing refresh.
- `rebuild_bucket(name, value_iter, now)` (line 101): Rebuilds a bucket's bitmap from `(slot, timestamp)` pairs, filtering to `[now - duration, now]`.
- `snap_duration(duration_secs, tolerance_pct)` (line 132): Finds the closest bucket within tolerance, used for query-time snapping.
- `get_bucket(name)` (line 126), `bucket_count()` (line 156), `total_bitmap_bytes()` (line 160), `field_name()` (line 167): Accessors.

Module is registered in `lib.rs:21`:
```rust
pub mod time_buckets;
```

Comprehensive unit tests cover basic lifecycle, 7d window, refresh timing, refresh_due, snap_duration exact/within/outside tolerance, empty rebuild, sorted order, bucket count, and unknown bucket rebuild (lines 172-381).

### Gap 1: No background refresh timer in `ConcurrentEngine`

The spec requires: *"Background timer rebuilds each bucket on its refresh interval."*

`ConcurrentEngine` does not hold a `TimeBucketManager`. The flush thread and merge thread have no code that calls `refresh_due()` or `rebuild_bucket()`. Bucket bitmaps are never automatically refreshed.

### Gap 2: Buckets are not Tier 1 (always in memory)

The spec says: *"Buckets are Tier 1 (always in memory)."*

Since `ConcurrentEngine` does not instantiate or store `TimeBucketManager`, bucket bitmaps are not part of the ArcSwap snapshot. They exist only when manually constructed in test code.

### Gap 3: No persistence to redb

The spec says: *"Persist to redb."*

`time_buckets.rs` has no reference to `redb`, `persist`, `serialize`, or `BitmapStore`. Bucket bitmaps exist only in memory and are lost on restart.

### Impact

Bucket bitmaps have correct data structures and algorithms but are dead code from the engine's perspective. No bucket is ever created, refreshed, or served during normal engine operation. The query-time bucket snapping in the executor (C3) has the `with_time_buckets()` builder, but `ConcurrentEngine` never calls it.

---

## C3: Query Parser Bucket Snapping

**Status:** PARTIAL

### What Exists

**Two independent snapping paths are implemented:**

#### Path A: Executor-level snapping (`src/executor.rs`, lines 496-516)

```rust
// executor.rs:498-510
FilterClause::Gt(field, value) | FilterClause::Gte(field, value) => {
    if let Some(tb) = &self.time_buckets {
        if field == tb.field_name() {
            if let Some(threshold) = value_to_bitmap_key(value) {
                let duration = self.now_unix.saturating_sub(threshold);
                if let Some(bucket_name) = tb.snap_duration(duration, 0.10) {
                    if let Some(bucket) = tb.get_bucket(bucket_name) {
                        return Ok(bucket.bitmap().clone());
                    }
                }
            }
        }
    }
    // fallback to range_scan
}
```

The `QueryExecutor` has `with_time_buckets(tb, now)` builder (line 97). When a `Gt` or `Gte` clause arrives on the bucketed field, it computes `duration = now - threshold`, snaps to the nearest bucket within 10% tolerance, and returns the bucket bitmap directly. Falls back to `range_scan` if no bucket matches.

The `BucketBitmap` variant is also handled (line 527):
```rust
FilterClause::BucketBitmap { bitmap, .. } => Ok(bitmap.as_ref().clone()),
```

Unit tests cover snapping to 24h, snapping to 7d, no-snap outside tolerance, and no-snap for non-bucketed fields (lines 1187-1304).

#### Path B: Pre-processing snapping (`src/query.rs`, lines 141-188)

```rust
// query.rs:141-148
pub fn snap_range_clauses(
    clauses: &[FilterClause],
    ctx: &BucketSnapContext<'_>,
) -> Vec<FilterClause> {
    clauses.iter().map(|clause| snap_clause(clause, ctx)).collect()
}
```

This provides a `BucketSnapContext` struct (line 121) and `snap_range_clauses()` function that pre-processes filter clauses, replacing `Gt`/`Gte` on bucketed fields with `FilterClause::BucketBitmap { field, bucket_name, bitmap }`. The `BucketBitmap` variant is defined in `query.rs:43-47`:

```rust
BucketBitmap {
    field: String,
    bucket_name: String,
    bitmap: Arc<RoaringBitmap>,
}
```

Unit tests cover exact snap, tolerance snap, no-snap, unknown field, Lt/Lte not snapped, snap inside And clause, and non-integer values (lines 230-358).

### Gap: Neither path is wired into `ConcurrentEngine`

`ConcurrentEngine::query()` and `ConcurrentEngine::execute_query()` construct `QueryExecutor` without calling `with_time_buckets()`:

```rust
// concurrent_engine.rs:637-651
let executor = match tier2_resolver.as_ref() {
    Some(t2) => QueryExecutor::with_tier2(
        &snap.slots, &snap.filters, &snap.sorts,
        self.config.max_page_size, t2,
    ),
    None => QueryExecutor::new(
        &snap.slots, &snap.filters, &snap.sorts,
        self.config.max_page_size,
    ),
};
```

No `with_time_buckets()` call. No `snap_range_clauses()` preprocessing. The executor's `time_buckets` field is always `None` during normal operation, so the Gt/Gte snapping code at lines 498-510 is unreachable.

### Impact

Bucket snapping logic is correct and tested at the unit level, but dead code in production. All range filters on timestamp fields fall through to `range_scan` every time. The `publishedAt < now` cache fragmentation problem described in the spec is not solved.

---

## C4: Config Schema Extension

**Status:** COMPLIANT

### Evidence

**`src/config.rs` (lines 332-365):**

```rust
// config.rs:333-342
pub struct FilterFieldConfig {
    pub name: String,
    pub field_type: FilterFieldType,
    #[serde(default)]
    pub storage: StorageMode,
    #[serde(default)]
    pub behaviors: Option<FieldBehaviors>,
}
```

```rust
// config.rs:345-354
pub struct FieldBehaviors {
    #[serde(default)]
    pub deferred_alive: bool,
    #[serde(default)]
    pub range_buckets: Vec<BucketConfig>,
}
```

```rust
// config.rs:357-365
pub struct BucketConfig {
    pub name: String,
    pub duration_secs: u64,
    pub refresh_interval_secs: u64,
}
```

**Validation (lines 166-201):**
- Rejects `deferred_alive: true` on boolean fields (line 168).
- Validates unique bucket names within a field (line 183).
- Rejects `duration_secs: 0` (line 189).
- Rejects `refresh_interval_secs: 0` (line 195).

**Serialization roundtrip tests (lines 909-1088):**
- TOML parsing with `deferred_alive` and `range_buckets` (line 909).
- YAML parsing with behaviors (line 941).
- Defaults to `None` when behaviors omitted (line 966).
- Validation tests for invalid configs (lines 977-1060).
- TOML serde roundtrip preserves behaviors (line 1064).

### Assessment

All spec requirements met:
- `behaviors` field on `FilterFieldConfig` with `deferred_alive: bool`.
- `range_buckets: Vec<BucketConfig>` with `name`, `duration_secs`, `refresh_interval_secs`.
- Robust validation and serde support.

---

## C5: Cache Key Stabilization

**Status:** COMPLIANT

### Evidence

**`src/cache.rs` (lines 60-67, 109-184):**

The `BucketBitmap` variant produces a stable cache key:

```rust
// cache.rs:63-67
FilterClause::BucketBitmap { field, bucket_name, .. } => Some(CanonicalClause {
    field: field.clone(),
    op: "bucket".to_string(),
    value_repr: bucket_name.clone(),
}),
```

The `bitmap` content is deliberately ignored in the key -- only `field` and `bucket_name` participate. This means `sortAt:bucket:7d` is always the same cache key regardless of when the bucket bitmap was last rebuilt.

**`canonicalize_with_buckets()` (lines 155-184):**

```rust
pub fn canonicalize_with_buckets(
    clauses: &[FilterClause],
    tb: Option<&crate::time_buckets::TimeBucketManager>,
    now_unix: u64,
) -> Option<CacheKey> {
    // ... tries snap_clause_key first, falls through to normal canonicalization ...
}
```

**`snap_clause_key()` (lines 112-150):**

```rust
pub fn snap_clause_key(
    clause: &FilterClause,
    tb: &crate::time_buckets::TimeBucketManager,
    now_unix: u64,
) -> Option<CanonicalClause> {
    // Gt/Gte on bucketed field → "bucket_gte" op with bucket name as value_repr
    // Lt/Lte on bucketed field → "bucket_lte" op with bucket name as value_repr
}
```

This produces stable keys like `sortAt:bucket_gte:24h` instead of raw timestamps.

**Tests (lines 848-1074):**
- `test_bucket_bitmap_produces_stable_key` (line 857): Two `BucketBitmap` clauses with different internal bitmaps but same `bucket_name` produce identical cache keys.
- `test_bucket_bitmap_stable_key_cache_hit` (line 896): Store under one bitmap, look up with different bitmap content -- exact cache hit.
- `test_canonicalize_with_buckets_stable_key_across_different_timestamps` (line 1029): Two slightly different timestamps that both snap to "24h" produce the same cache key.
- `test_snap_clause_key_gte_snaps_to_24h` (line 949), `test_snap_clause_key_gt_snaps_to_7d` (line 964), etc.

### Assessment

All spec requirements met at the implementation level. Cache keys use bucket names instead of raw timestamps. However, `canonicalize_with_buckets()` is only called in `executor.rs:execute_with_cache()` when `time_buckets` is `Some`, which (per C3 gap) never happens in `ConcurrentEngine`. The stabilization logic is correct but not exercised in production.

---

## C6: Tests -- Deferred Alive Lifecycle

**Status:** PARTIAL

### What Exists

**`src/slot.rs` (lines 408-518):** Eight unit tests covering `SlotAllocator` deferred alive primitives:

| Test | Lines | What it verifies |
|------|-------|-----------------|
| `test_schedule_alive_not_immediately_alive` | 409-421 | Slot not alive after schedule; HWM updated; deferred_count = 1 |
| `test_activate_due_future_time_does_not_activate` | 424-433 | Time before schedule does not activate |
| `test_activate_due_past_time_activates_slot` | 436-446 | Time at schedule activates slot; is_alive = true |
| `test_activate_due_drains_correct_subset` | 449-478 | Multiple slots at different times; activates correct subset per timestamp |
| `test_deferred_count_accuracy` | 481-496 | Accurate count with same-timestamp buckets |
| `test_schedule_alive_updates_high_water_mark` | 499-507 | HWM tracks deferred slots correctly |
| `test_is_deferred_false_after_activation` | 510-518 | is_deferred clears after activation |

### Gap 1: No integration test with ConcurrentEngine

The spec requires: *"Insert future doc -> verify not in results -> advance time -> verify appears."*

There is no test that:
1. Configures a field with `deferred_alive: true`.
2. Inserts a document with a future timestamp through `ConcurrentEngine::put()`.
3. Queries and verifies the document is absent.
4. Advances time (or simulates it).
5. Queries and verifies the document appears.

### Gap 2: No test with multiple documents at different scheduled times

The spec says: *"Multiple docs with different scheduled times."* The unit test `test_activate_due_drains_correct_subset` covers this at the `SlotAllocator` level, but not through the full engine pipeline.

### Impact

Unit tests prove `SlotAllocator` primitives are correct. The missing integration tests would expose the C1 gap -- that `ConcurrentEngine` never calls `schedule_alive()` or `activate_due()`.

---

## C7: Tests -- Bucket Snapping

**Status:** PARTIAL

### What Exists

**`src/time_buckets.rs` (lines 172-381):** Ten unit tests covering `TimeBucketManager`:

| Test | What it verifies |
|------|-----------------|
| `test_basic_lifecycle` | 24h bucket rebuild, boundary accuracy |
| `test_7d_window` | 7d bucket boundary accuracy |
| `test_refresh_timing` | `needs_refresh` at exact interval boundary |
| `test_refresh_due` | Correct subset of buckets needing refresh |
| `test_snap_duration_exact` | Exact duration match |
| `test_snap_duration_within_tolerance` | Within 10% tolerance |
| `test_snap_duration_outside_tolerance` | No snap when outside tolerance |
| `test_empty_rebuild` | Empty bitmap after empty rebuild |
| `test_sorted_order` | Buckets sorted by duration ascending |
| `test_bucket_count_and_bytes` / `test_rebuild_unknown_bucket` | Edge cases |

**`src/executor.rs` (lines 1187-1304):** Four executor-level bucket snapping tests:

| Test | What it verifies |
|------|-----------------|
| `test_c3_bucket_snapping_gte_snaps_to_24h` | Gte with exact 24h duration snaps to 24h bucket |
| `test_c3_bucket_snapping_gt_snaps_to_7d` | Gt within 7d tolerance snaps to 7d bucket |
| `test_c3_no_snap_outside_tolerance` | Falls through to range_scan |
| `test_c3_no_snap_for_non_bucketed_field` | Non-bucketed field not snapped |

**`src/query.rs` (lines 230-358):** Seven `snap_range_clauses` tests:

| Test | What it verifies |
|------|-----------------|
| `test_snap_gt_exact_24h` | Gt produces BucketBitmap with correct slots |
| `test_snap_gte_7d_within_tolerance` | Gte within tolerance snaps to 7d |
| `test_no_snap_outside_tolerance` | Remains unchanged |
| `test_no_snap_unknown_field` | Unknown field not snapped |
| `test_lt_lte_not_snapped` | Lt/Lte pass through |
| `test_snap_inside_and_clause` | Recurses into And |
| `test_non_integer_value_not_snapped` | Float values not snapped |

**`src/cache.rs` (lines 848-1074):** Eleven cache key stability tests (see C5 above).

### Gap 1: No refresh cycle correctness integration test

The spec requires: *"Refresh cycle correctness."* There is no test that builds a bucket, advances time past the refresh interval, verifies `refresh_due()` reports it, rebuilds it with new data, and confirms the bitmap changed.

### Gap 2: No query parser snapping integration test

The spec says: *"Query parser snapping to nearest bucket."* The JSON parser (`src/parser/json.rs`) has no bucket snapping. It parses `Gt`/`Gte` as raw `FilterClause` variants. Bucket snapping happens downstream (in executor or `snap_range_clauses`), but the full pipeline from JSON input -> bucket snap -> stable cache key -> correct results is not tested.

### Gap 3: No ConcurrentEngine-level test

No test exercises bucket snapping through `ConcurrentEngine::query()` or `ConcurrentEngine::execute_query()`.

### Impact

The individual components (TimeBucketManager, executor snapping, snap_range_clauses, cache key stabilization) have thorough unit test coverage. What is missing are integration tests proving the full pipeline works end-to-end, and tests for the background refresh cycle within the engine.

---

## Acceptance Criteria Assessment

The spec states:

> **Acceptance:** `publishedAt < now` filter eliminated from queries. Cache keys stable across seconds for bucketed ranges. Future-dated content invisible until scheduled time.

| Criterion | Met? | Notes |
|-----------|------|-------|
| `publishedAt < now` filter eliminated from queries | NO | Bucket snapping is implemented but not wired into ConcurrentEngine. Every query still evaluates raw timestamp filters. |
| Cache keys stable across seconds for bucketed ranges | PARTIAL | The cache key stabilization code is correct and tested, but never exercised in production because bucket managers are not attached to the executor in ConcurrentEngine. |
| Future-dated content invisible until scheduled time | NO | SlotAllocator has the primitives, but the write path never checks deferred_alive config and the background timer does not exist in ConcurrentEngine. |

---

## Recommended Actions (Priority Order)

### P0: Wire deferred alive into write path
- `mutation.rs:diff_document()` must check if any field configured with `deferred_alive: true` has a future value. If so, emit a `DeferredAlive { slot, activate_at }` mutation op instead of `AliveInsert`.
- Add `DeferredAlive` variant to `MutationOp` in `write_coalescer.rs`.
- `WriteCoalescer::apply_prepared()` must call `staging.slots.schedule_alive(slot, activate_at)` for deferred ops.

### P1: Add background timer for deferred alive activation
- In the flush thread (or a separate timer), periodically call `staging.slots.activate_due(now_unix)` with the current system time.
- After activation, publish a new snapshot so activated documents become visible.

### P2: Instantiate TimeBucketManager in ConcurrentEngine
- Read `behaviors.range_buckets` from `config.filter_fields` during `ConcurrentEngine::build()`.
- Construct `TimeBucketManager` instances per field.
- Store them in `ConcurrentEngine` (Tier 1, always in memory).

### P3: Wire bucket managers into query execution
- Pass bucket managers to `QueryExecutor::with_time_buckets()` in `ConcurrentEngine::query()` and `execute_query()`.
- Or use `snap_range_clauses()` to pre-process filters before passing to the executor.

### P4: Add background bucket refresh timer
- In the merge thread (or a separate timer), iterate `TimeBucketManager::refresh_due(now)`.
- Rebuild due buckets from the sort field's data.
- Publish updated bucket bitmaps in the snapshot.

### P5: Persist bucket bitmaps to redb
- Serialize bucket bitmaps on rebuild.
- Load from redb on startup.

### P6: Add integration tests
- C6: Full lifecycle through ConcurrentEngine: insert future doc, query (absent), advance time, query (present).
- C7: Full pipeline: JSON query with range filter -> bucket snap -> stable cache key -> correct results. Refresh cycle correctness.

---

## File Reference

| File | Role in Phase C | Lines of Interest |
|------|----------------|-------------------|
| `src/slot.rs` | Deferred alive data structure + primitives | 32-243 (deferred map, schedule_alive, activate_due) |
| `src/time_buckets.rs` | TimeBucket + TimeBucketManager | 1-170 (full implementation) |
| `src/config.rs` | FieldBehaviors, BucketConfig, validation | 332-365 (structs), 166-201 (validation) |
| `src/executor.rs` | Bucket snapping in evaluate_clause | 96-101 (with_time_buckets), 498-516 (Gt/Gte snapping), 527 (BucketBitmap) |
| `src/query.rs` | BucketBitmap variant, snap_range_clauses | 43-47 (variant), 117-188 (snap functions) |
| `src/cache.rs` | Stable cache keys for buckets | 60-67 (BucketBitmap key), 112-184 (snap_clause_key, canonicalize_with_buckets) |
| `src/concurrent_engine.rs` | Engine (NO Phase C integration) | No deferred alive timer, no bucket managers, no with_time_buckets() calls |
| `src/mutation.rs` | Write path (NO deferred alive check) | No deferred_alive handling |
| `src/write_coalescer.rs` | Mutation ops (NO DeferredAlive variant) | No DeferredAlive op |
| `src/parser/json.rs` | JSON parser (no bucket snapping, by design) | Parser produces raw Gt/Gte; snapping is downstream |
