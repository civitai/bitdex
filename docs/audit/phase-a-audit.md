# Phase A: Bitmap Persistence Layer -- Spec Compliance Audit

**Auditor:** Claude Opus 4.6
**Date:** 2026-02-21
**Scope:** All A-items (A1--A12) from `docs/plans/roadmap-performance-and-persistence.md`
**Baseline commits:** 11a5d75 through 2dceeea

---

## Executive Summary

Phase A infrastructure is **largely wired** with the core data structures and integration paths in place. The two-tier model (Tier 1 snapshot / Tier 2 moka+redb) is functional end-to-end for the read path. However, several spec requirements are **partially implemented or missing entirely**: the moka eviction listener for dirty diffs, Tier 1 merge-to-redb persistence, pending buffer depth metrics, and the full A10/A11 integration test suites. The system will work for queries but **cannot survive a restart without data loss for Tier 1 fields** because the merge thread never writes Tier 1 bitmaps back to redb.

| Item | Status | Severity |
|------|--------|----------|
| A1   | COMPLIANT | -- |
| A2   | COMPLIANT | -- |
| A3   | COMPLIANT | -- |
| A4   | PARTIAL | Medium |
| A5   | COMPLIANT | -- |
| A6   | COMPLIANT | -- |
| A7   | PARTIAL | High |
| A8   | COMPLIANT | -- |
| A9   | COMPLIANT | -- |
| A10  | PARTIAL | Medium |
| A11  | PARTIAL | Medium |
| A12  | MISSING | Low |

---

## A1: Add `storage` field to FilterFieldConfig

**Status: COMPLIANT**

**Evidence:**

`src/config.rs:275-288` -- `StorageMode` enum:
```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMode {
    /// Always in ArcSwap snapshot (default for low-cardinality fields).
    Snapshot,
    /// Loaded on demand via moka cache over redb (for high-cardinality fields).
    Cached,
}
impl Default for StorageMode {
    fn default() -> Self { Self::Snapshot }
}
```

`src/config.rs:334-342` -- `FilterFieldConfig` with `storage` field:
```rust
pub struct FilterFieldConfig {
    pub name: String,
    pub field_type: FilterFieldType,
    #[serde(default)]
    pub storage: StorageMode,
    #[serde(default)]
    pub behaviors: Option<FieldBehaviors>,
}
```

**Gap:** None. The enum has the correct variants, defaults to `Snapshot`, and serializes with `snake_case` for TOML/YAML. Sort fields have no `storage` field (always Tier 1 by design), which is correct per spec ("Sort fields always Snapshot"). However, the spec also says "Validation: sort fields always Snapshot" -- there is no explicit validation that rejects `Cached` on sort fields because the `SortFieldConfig` struct simply does not have a `storage` field, which is a valid (and arguably cleaner) approach.

**Impact:** None.

---

## A2: Add storage config section

**Status: COMPLIANT**

**Evidence:**

`src/config.rs:291-329` -- `StorageConfig` struct:
```rust
pub struct StorageConfig {
    pub bitmap_path: Option<std::path::PathBuf>,
    pub tier2_cache_size_mb: u64,           // default 1024
    pub merge_diff_threshold: usize,        // default 10000
    pub pending_drain_cap: usize,           // default 100
}
```

`src/config.rs:50-52` -- Wired into top-level `Config`:
```rust
#[serde(default)]
pub storage: StorageConfig,
```

**Spec requirement:** "`bitmap_path`, `merge_interval_ms`, `merge_diff_threshold`, `tier2_cache_size`, `pending_drain_cap`. Remove `snapshot_interval_secs`, `wal_flush_strategy` if not already removed."

**Gap analysis:**
- `bitmap_path`: Present. Optional `PathBuf`.
- `merge_interval_ms`: Present at top-level `Config` (line 36), not inside `StorageConfig`. The spec's YAML example puts it inside `storage:`, but having it at top-level is functionally equivalent and was a pre-existing field from the Prerequisite phase.
- `merge_diff_threshold`: Present, default 10000.
- `tier2_cache_size`: Present as `tier2_cache_size_mb` (in megabytes, explicit unit). Spec says "1GB" default; implementation says 1024 MB. Equivalent.
- `pending_drain_cap`: Present, default 100.
- `snapshot_interval_secs` / `wal_flush_strategy`: Confirmed absent from the entire `src/` tree (grep returned zero matches). Properly removed.

**Impact:** None.

---

## A3: Bitmap serialization to/from redb

**Status: COMPLIANT**

**Evidence:**

`src/bitmap_store.rs` -- Full implementation at 519 lines including tests.

Key APIs:
- `BitmapStore::new(path)` -- opens redb file (line 23)
- `BitmapStore::open_temp()` -- in-memory backend for tests (line 40)
- `load_single(field, value) -> RoaringBitmap` (line 67)
- `load_field(field_name) -> HashMap<u64, RoaringBitmap>` (line 91)
- `load_all_fields(field_names) -> HashMap<String, HashMap<u64, RoaringBitmap>>` (line 178)
- `write_batch(entries: &[(&str, u64, &RoaringBitmap)])` (line 125)
- `delete_field_value(field, value)` (line 155)
- `bitmap_count()` -- for metrics (line 222)

Key format: composite string key `"field_name:value"`, e.g. `"tagIds:42"`.

Serialization uses `RoaringBitmap::serialize_into` / `deserialize_from` (lines 79, 113, 141).

**Gap:** None. All spec requirements met: redb table, keyed by (field_name, value), serialize/deserialize RoaringBitmap, batch write support.

**Impact:** None.

---

## A4: moka Tier 2 cache integration

**Status: PARTIAL**

**Evidence:**

`src/tier2_cache.rs` -- 101 lines of production code.

```rust
pub struct Tier2Cache {
    cache: Cache<CacheKey, Arc<RoaringBitmap>>,
}
```

- `new(max_size_mb)` -- creates cache with `weigher` based on `serialized_size()`, `max_capacity` in bytes (line 26)
- `get_or_load(field, value, loader)` -- uses `moka::get_with()` for thundering herd protection (line 46)
- `get(field, value)` -- simple lookup without loading (line 61)
- `insert(field, value, bitmap)` -- direct insert after merge (line 67)
- `invalidate(field, value)` / `invalidate_field(field)` -- targeted invalidation (lines 73, 79)
- `entry_count()` / `weighted_size()` -- metrics (lines 93, 98)

**Spec requirement:** "Eviction listener flushes dirty diffs to redb."

**Gap: NO EVICTION LISTENER.** The moka cache is built with `.support_invalidation_closures()` (line 34) but there is no `.eviction_listener()` registered. The spec explicitly requires that when moka evicts a bitmap with a non-empty diff, the diff should be flushed to redb. This is listed as a risk mitigation in the roadmap:

> "Eviction before merge: If moka evicts a bitmap with a non-empty diff, flush the small diff to redb (load base -> apply diff -> write back)."

Without this listener, if moka evicts a bitmap that has pending mutations buffered in the `PendingBuffer`, those mutations will persist in the buffer until the merge thread drains them. This means eviction does NOT lose data -- the pending buffer acts as a safety net. However, the spec intended for the eviction listener to be the primary mechanism for flushing dirty Tier 2 data before eviction, not the pending buffer.

Additionally, the current architecture stores `Arc<RoaringBitmap>` (clean base bitmaps) in the moka cache, not `VersionedBitmap` with diffs. There is no concept of a "dirty moka entry" -- mutations go to the pending buffer, and moka caches the clean base+pending merged result via `get_or_load`. This is a **design divergence** from the spec (which envisioned VersionedBitmap in moka with dirty diffs), but it is arguably a valid simplification since the pending buffer handles the mutation buffering role.

**Impact: Medium.** The pending buffer compensates for the missing eviction listener, so correctness is maintained. However, under eviction pressure with heavy writes to Tier 2 fields, the pending buffer will grow larger than it would if eviction flushed dirty state to redb. The `drain_heaviest()` mechanism in the merge thread mitigates this, but the buffer could grow unbounded between merge cycles if eviction outpaces drain.

---

## A5: Pending mutation buffer

**Status: COMPLIANT**

**Evidence:**

`src/pending_buffer.rs` -- 144 lines of production code.

```rust
pub struct PendingBuffer {
    entries: HashMap<PendingKey, PendingMutations>,
}
pub struct PendingMutations {
    pub sets: RoaringBitmap,
    pub clears: RoaringBitmap,
}
```

Key APIs:
- `add_set(field, value, slot)` / `add_clear(field, value, slot)` -- buffer mutations (lines 70, 78)
- `take(field, value) -> Option<PendingMutations>` -- consume on read (line 87)
- `has_pending(field, value) -> bool` -- check existence (line 98)
- `drain_heaviest(cap) -> Vec<(PendingKey, PendingMutations)>` -- capped drain for merge (line 106)
- `depth() -> usize` / `total_ops() -> u64` -- metrics (lines 130, 135)
- `PendingMutations::apply_to(bitmap)` -- apply buffered ops to loaded bitmap (line 36)

Conflict resolution: `add_set` removes from `clears`, `add_clear` removes from `sets` -- last-writer-wins semantics per slot.

**Spec requirement:** "Buffer cold Tier 2 mutations. Apply on read (moka miss -> load + apply pending) or on merge (capped drain). Track buffer depth metric."

**Gap:** The `depth()` and `total_ops()` methods exist on `PendingBuffer`, but they are **never called** in production code (only in tests). The spec says "Track buffer depth metric" and the merge cycle should "Report pending buffer depth metric." No Prometheus metric, no log output, no observable metric is emitted.

**Impact:** Low operationally (the data is available if someone adds the metric call), but the spec says it should be observable. This matters for tuning `pending_drain_cap` in production.

---

## A6: Tier-aware flush thread

**Status: COMPLIANT**

**Evidence:**

`src/concurrent_engine.rs:169-175` -- Tier 2 field name collection:
```rust
let tier2_field_names: std::collections::HashSet<String> = config
    .filter_fields
    .iter()
    .filter(|f| f.storage == StorageMode::Cached)
    .map(|f| f.name.clone())
    .collect();
```

`src/concurrent_engine.rs:211-215` -- Extract Tier 2 mutations before apply:
```rust
let tier2_mutations = if !tier2_fields.is_empty() {
    coalescer.take_tier2_mutations(&tier2_fields)
} else {
    Vec::new()
};
```

`src/concurrent_engine.rs:272-289` -- Route Tier 2 mutations to PendingBuffer + invalidate moka:
```rust
if !tier2_mutations.is_empty() {
    let mut p = flush_pending.lock();
    for (field, value, slots, is_set) in &tier2_mutations {
        for &slot in slots {
            if *is_set { p.add_set(field, *value, slot); }
            else { p.add_clear(field, *value, slot); }
        }
    }
    drop(p);
    if let Some(ref t2c) = flush_tier2_cache {
        for (field, value, _, _) in &tier2_mutations {
            t2c.invalidate(field, *value);
        }
    }
}
```

`src/concurrent_engine.rs:85-91` -- Only Tier 1 fields added to FilterIndex:
```rust
for fc in &config.filter_fields {
    if fc.storage == StorageMode::Snapshot {
        filters.add_field(fc.clone());
    }
}
```

**Gap:** None. The flush thread correctly:
1. Extracts Tier 2 mutations before `apply_prepared()` (which only touches Tier 1 fields in FilterIndex)
2. Routes Tier 2 mutations to PendingBuffer
3. Invalidates moka cache entries for mutated Tier 2 keys
4. Handles alive mutations by invalidating all Tier 2 cache entries (lines 301-307)
5. Handles shutdown with final Tier 2 mutation routing (lines 400-411)

**Impact:** None.

---

## A7: Tier-aware merge thread

**Status: PARTIAL**

**Evidence:**

`src/concurrent_engine.rs:434-486` -- Merge thread implementation:
```rust
thread::spawn(move || {
    let sleep_duration = Duration::from_millis(merge_interval_ms);
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(sleep_duration);
        if let Some(ref store) = merge_bitmap_store {
            let to_drain = {
                let mut p = merge_pending.lock();
                p.drain_heaviest(pending_drain_cap)
            };
            if !to_drain.is_empty() {
                let mut batch = Vec::new();
                for ((field, value), mutations) in &to_drain {
                    let mut bitmap = store.load_single(field, *value)
                        .unwrap_or_default();
                    mutations.apply_to(&mut bitmap);
                    batch.push((field.to_string(), *value, bitmap));
                }
                let refs: Vec<_> = batch.iter()
                    .map(|(f, v, b)| (f.as_str(), *v, b)).collect();
                if let Err(e) = store.write_batch(&refs) {
                    eprintln!("merge thread: redb write failed: {e}");
                }
                if let Some(ref t2c) = merge_tier2_cache {
                    for (i, ((field, value), _)) in to_drain.iter().enumerate() {
                        if t2c.contains(field, *value) {
                            t2c.insert(field, *value, Arc::new(batch[i].2.clone()));
                        }
                    }
                }
            }
        }
    }
})
```

**Spec requirement:** "Merge compacts Tier 1 diffs + dirty moka entries + capped pending drain. Batch writes to redb."

The spec describes a 7-step merge cycle:
1. Snapshot current Tier 1 filter diffs
2. Compact filter diffs into bases
3. Compact dirty Tier 2 diffs in moka
4. Drain pending cold Tier 2 mutations (CAPPED)
5. Batch write all merged bases to redb
6. Publish merged snapshot with empty filter diffs
7. Report pending buffer depth metric

**Gaps:**

**GAP 1 (HIGH): No Tier 1 diff compaction or persistence.** The merge thread ONLY drains the pending buffer (step 4) and writes those to redb. It does NOT:
- Snapshot or compact Tier 1 filter diffs (steps 1-2)
- Write Tier 1 merged bases to redb (step 5 partially)
- Publish a merged snapshot with empty diffs (step 6)

This means **Tier 1 filter bitmaps are never persisted to redb by the merge thread.** The A8 startup code loads Tier 1 from redb, but nothing ever writes Tier 1 state TO redb. If the process restarts, Tier 1 filter state will be empty (or stale from a previous run if someone manually seeded the data). This is the most critical gap in the implementation.

**GAP 2 (MEDIUM): No dirty moka entry compaction (step 3).** The current architecture diverges from spec here because moka stores clean `Arc<RoaringBitmap>` rather than `VersionedBitmap` with diffs. This is mitigated by the PendingBuffer design -- Tier 2 mutations are buffered as pending and only applied on read or drain. So there are no "dirty moka entries" to compact. This is a design divergence, not a bug.

**GAP 3 (LOW): No pending buffer depth metric reported (step 7).** The `depth()` method exists on PendingBuffer but is never called by the merge thread.

**Impact: HIGH.** The missing Tier 1 persistence means:
- Process restart loses all Tier 1 filter state (nsfwLevel, type, booleans, etc.)
- The A8 startup code will load empty data from redb for Tier 1 fields
- Sort layer bitmaps are also never persisted (they are always Tier 1)
- The alive bitmap is never persisted
- The only data that survives restart is Tier 2 pending mutations that were drained to redb

This effectively means the system cannot restart without a full re-index from the Postgres WAL (Phase 5, not yet built).

---

## A8: Startup -- load Tier 1 from redb

**Status: COMPLIANT**

**Evidence:**

`src/concurrent_engine.rs:106-124`:
```rust
// Load Tier 1 filter bitmaps from redb on startup (A8)
if let Some(ref store) = bitmap_store {
    let tier1_names: Vec<&str> = config
        .filter_fields
        .iter()
        .filter(|f| f.storage == StorageMode::Snapshot)
        .map(|f| f.name.as_str())
        .collect();
    if !tier1_names.is_empty() {
        let loaded = store.load_all_fields(&tier1_names)?;
        for (field_name, bitmaps) in loaded {
            if !bitmaps.is_empty() {
                if let Some(field) = filters.get_field_mut(&field_name) {
                    field.load_from(bitmaps);
                }
            }
        }
    }
}
```

`src/filter.rs:39-43` -- `FilterField::load_from`:
```rust
pub fn load_from(&mut self, data: HashMap<u64, RoaringBitmap>) {
    for (value, bitmap) in data {
        self.bitmaps.insert(value, VersionedBitmap::new(bitmap));
    }
}
```

**Spec requirement:** "On startup, read all Tier 1 bitmaps from redb into InnerEngine. Publish initial snapshot. Tier 2 warms on demand."

**Gap:** The startup code correctly loads Tier 1 filter bitmaps from redb, uses `load_from` to populate FilterField with VersionedBitmap bases, and the initial snapshot is published via `ArcSwap::new(Arc::new(inner_engine))` (line 149). Tier 2 is not loaded at startup (correct -- warms on demand via moka).

However, this startup code is **effectively a no-op in practice** because the merge thread never writes Tier 1 bitmaps TO redb (see A7 gap). The code is correct but has nothing to load. It would work correctly if A7's Tier 1 persistence were implemented.

Additionally, sort layer bitmaps and the alive bitmap are NOT loaded from redb on startup. The spec says "load Tier 1 bases (~1.5GB)" which includes sort layers and alive. These are not persisted or loaded.

**Impact:** Functionally correct code that is currently dead due to the A7 gap. Once A7 writes Tier 1 to redb, this will work. Sort layers and alive bitmap persistence are separate concerns not explicitly called out in A8's work item but implied by the phase's goal.

---

## A9: Executor -- Tier 2 bitmap resolution

**Status: COMPLIANT**

**Evidence:**

`src/executor.rs:31-35` -- `Tier2Resolver` struct:
```rust
pub struct Tier2Resolver {
    pub cache: Arc<Tier2Cache>,
    pub pending: Arc<parking_lot::Mutex<PendingBuffer>>,
    pub store: Arc<BitmapStore>,
}
```

`src/executor.rs:69-86` -- `QueryExecutor::with_tier2()` constructor:
```rust
pub fn with_tier2(slots, filters, sorts, max_page_size, tier2: &'a Tier2Resolver) -> Self
```

`src/executor.rs:418-432` -- `evaluate_clause` falls through to Tier 2:
```rust
FilterClause::Eq(field, value) => {
    // Try Tier 1 (snapshot FilterIndex) first
    if let Some(filter_field) = self.filters.get_field(field) {
        // ... Tier 1 path
        return Ok(filter_field.get(key).cloned().unwrap_or_default());
    }
    // Fall back to Tier 2 (moka cache + pending + redb)
    if let Some(tier2) = &self.tier2 {
        return self.evaluate_tier2_eq(field, value, tier2);
    }
    Err(BitdexError::FieldNotFound(field.clone()))
}
```

`src/executor.rs:532-554` -- `evaluate_tier2_eq`:
```rust
fn evaluate_tier2_eq(&self, field, value, tier2) -> Result<RoaringBitmap> {
    let bm = tier2.cache.get_or_load(&field_arc, key, move || {
        let mut bitmap = store.load_single(&field_for_loader, key)?;
        // Apply any pending mutations
        if let Some(mutations) = pending.lock().take(&field_for_loader, key) {
            mutations.apply_to(&mut bitmap);
        }
        Ok(bitmap)
    })?;
    Ok(bm.as_ref().clone())
}
```

`src/executor.rs:445-458` -- `In` filter also falls through to Tier 2:
```rust
FilterClause::In(field, values) => {
    if let Some(filter_field) = self.filters.get_field(field) { ... }
    if let Some(tier2) = &self.tier2 {
        return self.evaluate_tier2_in(field, values, tier2);
    }
    Err(BitdexError::FieldNotFound(field.clone()))
}
```

`src/concurrent_engine.rs:617-626` -- `make_tier2_resolver` and query path:
```rust
fn make_tier2_resolver(&self) -> Option<Tier2Resolver> {
    match (&self.tier2_cache, &self.bitmap_store) {
        (Some(cache), Some(store)) => Some(Tier2Resolver { ... }),
        _ => None,
    }
}
```

**Gap:** The Tier 2 fallback covers `Eq` and `In` filter clauses. `NotEq` and `Not` work indirectly because they delegate to `Eq` evaluation then subtract from alive. However, range filters (`Gt`, `Gte`, `Lt`, `Lte`) on Tier 2 fields will fail with `FieldNotFound` because `range_scan` only checks `self.filters` (Tier 1). This is acceptable because range filters on high-cardinality fields (tagIds, userId) are not a realistic query pattern -- you filter by exact tag values, not ranges.

**Impact:** None for the intended use case (tagIds/userId are never range-filtered).

---

## A10: Tests -- Tier 2 cache hit/miss/eviction

**Status: PARTIAL**

**Evidence:**

`src/tier2_cache.rs` tests:

1. **Thundering herd (A10):** `test_thundering_herd_loader_called_once` (line 211) -- 8 threads call `get_or_load()` for the same key, verifies loader called exactly once. **PRESENT and correct.**

2. **Pending mutation correctness (A10):** `test_pending_mutation_applied_on_load` (line 258) -- Seeds redb with a base bitmap, adds pending mutations, simulates cache miss load+apply, verifies result. **PRESENT and correct.** However, this test is in `tier2_cache.rs` tests and manually orchestrates the load+apply pattern rather than testing through the executor's `evaluate_tier2_eq` path.

3. **Eviction with dirty diff (A10):** `test_cache_eviction_under_small_budget` (line 297) -- Verifies moka enforces memory budget by checking `weighted_size < total_inserted_weight`. **PRESENT but incomplete.** This only tests that eviction occurs. It does NOT test that a dirty diff is flushed to redb on eviction (because there is no eviction listener -- see A4 gap).

**Spec requirement:** "Concurrent reads for same cold bitmap (thundering herd). Eviction with dirty diff. Pending mutation correctness."

**Gaps:**
- **No "eviction with dirty diff" test** -- The test only verifies eviction happens, not that dirty state is preserved. Since there is no eviction listener (A4 gap), this test cannot exist in its spec-intended form.
- **No integration test through ConcurrentEngine** -- All A10 tests are unit tests in `tier2_cache.rs`. There is no test that exercises the full path: `ConcurrentEngine::put()` with a Cached field -> flush -> pending buffer -> `ConcurrentEngine::query()` resolving via Tier 2.
- **No concurrent read+write test for Tier 2** -- The thundering herd test only tests concurrent reads; there is no test where writers are mutating Tier 2 fields while readers are querying them.

**Impact: Medium.** The unit tests provide good coverage of individual components, but the lack of end-to-end integration tests means the full Tier 2 pipeline is unverified in an automated test.

---

## A11: Tests -- Startup from redb

**Status: PARTIAL**

**Evidence:**

`src/bitmap_store.rs` tests:

1. `test_restart_write_close_reopen_verify` (line 405) -- Writes bitmaps, drops store, reopens, verifies data survives. **PRESENT.**
2. `test_restart_multiple_batches_later_writes_win` (line 451) -- Multiple batches, later writes overwrite, survives restart. **PRESENT.**
3. `test_restart_delete_persists` (line 483) -- Delete + restart, verifies deletion persists. **PRESENT.**

**Spec requirement:** "Write bitmaps -> restart -> verify state. Crash recovery (kill mid-merge -> restart -> verify consistency)."

**Gaps:**
- **BitmapStore-level tests only** -- These tests verify that `BitmapStore` correctly persists and recovers data. They do NOT test the full ConcurrentEngine restart path: create engine -> insert documents -> shutdown -> create new engine with same redb path -> verify query results are correct.
- **No ConcurrentEngine restart test** -- There is no test that exercises `A8` startup code path (`load_all_fields` -> `load_from`) through an actual engine lifecycle.
- **No crash recovery test** -- The spec asks for "kill mid-merge -> restart -> verify consistency." No test simulates a crash during merge. redb's transactional guarantees should handle this, but it is untested.
- **Moot without A7** -- Even if a ConcurrentEngine restart test existed, it would fail because the merge thread never writes Tier 1 bitmaps to redb (A7 gap). The BitmapStore-level tests work because they manually call `write_batch`.

**Impact: Medium.** The BitmapStore serialization roundtrip is well-tested, but the end-to-end restart path through ConcurrentEngine is not tested and would not work due to the A7 gap.

---

## A12: Benchmark -- Memory reduction validation

**Status: MISSING**

**Evidence:** No benchmark code, no benchmark results, no documentation related to Phase A memory validation.

**Spec requirement:** "Run 5M benchmark, verify bitmap memory ~1.5-2GB (down from 328MB all-in-memory is already low, but at 100M should show ~3-4GB vs current 6.5GB)."

**Gap:** No benchmark has been run or added to `src/bin/benchmark.rs` to validate memory reduction from the two-tier model. The existing benchmark harness does not support configuring fields as `Cached` vs `Snapshot`.

**Impact: Low.** This is a validation task, not a correctness requirement. The memory reduction is expected to work based on the architecture (tagIds and userId moved to Tier 2 = ~80% of filter bitmap memory evictable), but it has not been measured.

---

## Cross-Cutting Gaps

### 1. Tier 1 bitmaps never written to redb

This is the single most critical gap. The A7 merge thread only handles Tier 2 pending buffer drain. Tier 1 filter bitmaps, sort layer bitmaps, and the alive bitmap are never persisted to redb. This means:
- A8 startup loads empty state for Tier 1 fields
- Process restart requires full re-index
- The "eliminates need for WAL, snapshots, and sidecar builder" claim is not yet realized

**Files affected:** `src/concurrent_engine.rs` merge thread (lines 434-486)

### 2. No eviction listener on moka cache

The spec requires dirty diffs to be flushed to redb on eviction. The implementation uses a simpler model (pending buffer as safety net) which works but diverges from spec. Under high write pressure with memory pressure, this could cause the pending buffer to grow unbounded.

**Files affected:** `src/tier2_cache.rs` `Tier2Cache::new()` (line 26)

### 3. No pending buffer depth metric exposure

The `PendingBuffer::depth()` and `total_ops()` methods exist but are never called in production. The merge thread should report these, ideally as Prometheus metrics (Phase 4).

**Files affected:** `src/concurrent_engine.rs` merge thread, `src/pending_buffer.rs`

### 4. Sort layers and alive bitmap not covered by persistence

The spec's Tier 1 definition includes "Alive bitmap, all sort layers" but neither is persisted to or loaded from redb. This is partially an A7 gap and partially a scope question -- the A8 work item only mentions "filter bitmaps" but the phase goal implies all Tier 1 state.

---

## Recommendations

| Priority | Action | Files |
|----------|--------|-------|
| **P0** | Implement Tier 1 filter persistence in merge thread: snapshot diffs, compact into bases, write to redb | `src/concurrent_engine.rs` |
| **P0** | Add alive bitmap and sort layer persistence to redb (serialize/deserialize, startup load, merge write) | `src/concurrent_engine.rs`, `src/bitmap_store.rs` |
| **P1** | Add ConcurrentEngine restart integration test (insert -> shutdown -> reopen -> verify queries) | `tests/` or `src/concurrent_engine.rs` |
| **P1** | Add ConcurrentEngine Tier 2 integration test (Cached field -> put -> query -> verify) | `tests/` or `src/concurrent_engine.rs` |
| **P2** | Add moka eviction listener that flushes pending mutations to redb on eviction | `src/tier2_cache.rs` |
| **P2** | Expose pending buffer depth metric in merge thread (log or Prometheus counter) | `src/concurrent_engine.rs` |
| **P3** | Add memory reduction benchmark with Cached fields configured | `src/bin/benchmark.rs` |
| **P3** | Add crash recovery test (kill mid-merge -> restart -> verify) | `tests/` |
