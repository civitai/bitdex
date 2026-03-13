# Phase D Audit: Bound Caches (Sort Working Set Reduction)

**Date:** 2026-02-21
**Auditor:** Claude Opus 4.6
**Spec Source:** `docs/plans/roadmap-performance-and-persistence.md`, Phase D section
**Codebase Commit:** `2dceeea` (main branch, HEAD)

---

## Executive Summary

Phase D is substantially implemented with 7 of 10 work items compliant or mostly compliant. The two most significant gaps are:

1. **D1 (Data Structure):** The bound cache uses plain `RoaringBitmap` instead of `VersionedBitmap` with diff layer as the spec requires. This is a deliberate simplification that works but forfeits the ability to do lock-free live maintenance via Arc-shared diffs.
2. **D7 (Invalidation Model Change):** The generation-counter lazy invalidation in the trie cache is fully intact and has NOT been replaced by live cache maintenance. The spec calls for a paradigm shift here; the current code adds live bound maintenance as a parallel system alongside the old invalidation model, rather than replacing it.
3. **D3 (Live Maintenance):** Sort-value comparison works, but filter-definition checking is completely absent. The flush thread never verifies whether a mutated document passes a bound's filter predicate before adding it.

---

## Per-Item Audit

### D1: Bound Cache Data Structure

**Status:** PARTIAL

**Spec Requirement:**
> Struct: bitmap + sort field + direction + filter definition + min tracked value + cardinality. Uses `VersionedBitmap` for live maintenance via diff layer.

**Evidence:**

The `BoundEntry` struct at `src/bound_cache.rs:34-50`:

```rust
pub struct BoundEntry {
    bitmap: RoaringBitmap,          // <-- plain RoaringBitmap, NOT VersionedBitmap
    min_tracked_value: u32,
    target_size: usize,
    max_size: usize,
    needs_rebuild: bool,
    last_used: Instant,
}
```

The `BoundKey` struct at `src/bound_cache.rs:17-24` captures filter definition + sort field + direction + tier:

```rust
pub struct BoundKey {
    pub filter_key: CacheKey,
    pub sort_field: String,
    pub direction: SortDirection,
    pub tier: u32,
}
```

**What is present:**
- Bitmap (plain `RoaringBitmap`)
- Sort field (via `BoundKey.sort_field`)
- Direction (via `BoundKey.direction`)
- Filter definition (via `BoundKey.filter_key`, which is a `Vec<CanonicalClause>`)
- Min tracked value (`min_tracked_value: u32`)
- Cardinality (computed via `bitmap.len()` -- not stored separately)
- Target size and max size for bloat control
- LRU timestamp (`last_used`)

**Gap:**
The bitmap is a plain `RoaringBitmap`, not a `VersionedBitmap`. The spec explicitly states "Uses `VersionedBitmap` for live maintenance via diff layer." The `VersionedBitmap` struct exists in `src/versioned_bitmap.rs` but is not used by bound_cache.rs at all (confirmed: grep for `VersionedBitmap` in bound_cache.rs returns zero results).

**Impact:**
Without `VersionedBitmap`, live maintenance operates via direct `bitmap.insert(slot)` calls under a `parking_lot::Mutex` lock in the flush thread. This means:
- The bound bitmap cannot be shared lock-free with readers via ArcSwap the way other bitmaps can.
- Every live maintenance operation requires acquiring the `bound_cache` mutex.
- Readers also acquire the mutex for bound lookup (see `concurrent_engine.rs:838`).

At current scale this is acceptable since bound cache operations are fast (microseconds). At extreme cache counts it could become a contention point, but the meta-index (Phase E) already mitigates the linear-scan concern.

---

### D2: Bound Formation (Promotion Threshold = 0)

**Status:** COMPLIANT

**Spec Requirement:**
> Every unique filter+sort query immediately caches its sort result as a bound bitmap after first execution.

**Evidence:**

Two code paths implement bound formation:

**Path 1 -- `ConcurrentEngine::query()` at `src/concurrent_engine.rs:660-664`:**
```rust
let mut result =
    executor.execute_from_bitmap(effective_bitmap, sort, limit, None, use_simple)?;

// D2: Form or update bound from sort results
self.update_bound_from_results(&snap, sort, &cache_key, &result.ids, None);
```

**Path 2 -- `ConcurrentEngine::execute_query()` at `src/concurrent_engine.rs:713-719`:**
```rust
// D2/D6: Form or update bound from sort results (with cursor for tiered bounds)
self.update_bound_from_results(
    &snap,
    query.sort.as_ref(),
    &cache_key,
    &result.ids,
    query.cursor.as_ref(),
);
```

**Path 3 -- `QueryExecutor::execute_with_cache()` at `src/executor.rs:228-231`:**
```rust
// D2: Form or update bound from sort results (promotion_threshold = 0)
if let (Some(bc), Some(ref key)) = (&self.bound_cache, &cache_key) {
    self.form_or_update_bound(bc, key, sort_clause, &result_ids);
}
```

The `update_bound_from_results` method at `src/concurrent_engine.rs:882-948` forms a new bound if none exists, or rebuilds an existing one if `needs_rebuild` is set:

```rust
if let Some(entry) = bc.get_mut(&bound_key) {
    if entry.needs_rebuild() {
        entry.rebuild(&sorted_slots, |slot| sort_field.reconstruct_value(slot));
    }
} else {
    bc.form_bound(bound_key, &sorted_slots, |slot| sort_field.reconstruct_value(slot));
}
```

**Gap:** None. There is no configurable `bound_promotion_threshold` field in `CacheConfig` -- the config spec listed it but the implementation hardcodes threshold = 0 (immediate formation), which matches the spec's stated behavior. The missing config field is cosmetic since the value would always be 0.

**Impact:** None. Behavior matches spec exactly.

---

### D3: Live Maintenance on Writes

**Status:** PARTIAL

**Spec Requirement:**
> On sort field mutation: check if doc passes each relevant bound's filter definition, compare value against bound's min, set bit if it exceeds. Bits never removed -- bloat control handles cleanup.

**Evidence:**

The flush thread at `src/concurrent_engine.rs:232-269` implements live maintenance:

```rust
// D3/E3: Live maintenance of bound caches on sort field mutations.
let sort_mutations = coalescer.mutated_sort_slots();
if !sort_mutations.is_empty() {
    let mut bc = flush_bound_cache.lock();
    if !bc.is_empty() {
        for (sort_field, slots) in &sort_mutations {
            let matching_keys = bc.bounds_for_sort_field(sort_field);
            if matching_keys.is_empty() {
                continue;
            }
            for &slot in slots {
                let value = staging.sorts
                    .get_field(sort_field)
                    .map(|f| f.reconstruct_value(slot))
                    .unwrap_or(0);

                for bound_key in &matching_keys {
                    if let Some(entry) = bc.get_mut(bound_key) {
                        if entry.needs_rebuild() {
                            continue;
                        }
                        let qualifies = match bound_key.direction {
                            SortDirection::Desc => value > entry.min_tracked_value(),
                            SortDirection::Asc => value < entry.min_tracked_value(),
                        };
                        if qualifies {
                            entry.add_slot(slot);
                        }
                    }
                }
            }
        }
    }
}
```

**What works:**
- Sort value comparison against `min_tracked_value` is correctly implemented with direction-awareness (Desc: `value > min`, Asc: `value < min`).
- `needs_rebuild` check skips maintenance on bloated bounds.
- Bits are only added, never removed (matches spec).
- Uses meta-index via `bounds_for_sort_field()` for O(1) lookup of relevant bounds (Phase E integration).

**Gap -- Missing filter definition check:**
The spec states: "check if doc passes each relevant bound's filter definition." The current code does NOT check whether the mutated slot/document satisfies the bound's filter predicate. It only checks:
1. Does the bound sort by the mutated sort field?
2. Does the new sort value exceed the bound's threshold?

It does NOT verify whether the slot matches the bound's filter definition (e.g., `nsfwLevel=1`). This means a document with `nsfwLevel=2` that gets a high `reactionCount` will be incorrectly added to a bound whose filter key is `nsfwLevel=1`.

**Why this matters:**
This injects false positives into the bound bitmap. The impact is mitigated because:
1. The bound is ANDed with the filter result at query time, so false positives are masked.
2. Bloat control will eventually trigger a rebuild.

However, it accelerates bloat growth and reduces the effectiveness of the bound at reducing the sort working set. At high write rates on popular sort fields, bounds could bloat quickly and need frequent rebuilds.

**Additional gap -- Filter field invalidation is mark-for-rebuild, not live update:**
At `src/concurrent_engine.rs:318-338`, when filter fields change, bounds are simply marked for rebuild rather than having their bitmaps updated live:

```rust
if has_alive {
    for (_, entry) in bc.iter_mut() {
        entry.mark_for_rebuild();
    }
} else {
    for field_name in &changed_filters {
        bc.invalidate_filter_field(field_name);
    }
}
```

This is a reasonable simplification -- live filter maintenance would require re-evaluating the full filter predicate for every mutated slot -- but it deviates from the spec's vision of "live maintenance" for filter changes.

**Impact:** Moderate. False positives in bounds from missing filter checks increase bloat rate and reduce bound effectiveness until the next rebuild. The system still produces correct query results because the bound is intersected with the filter result.

---

### D4: Bloat Control

**Status:** COMPLIANT

**Spec Requirement:**
> Track cardinality. When > bound_max_size, flag for rebuild. Next query triggers fresh sort traversal.

**Evidence:**

`BoundEntry::add_slot()` at `src/bound_cache.rs:131-137`:

```rust
pub fn add_slot(&mut self, slot: u32) -> bool {
    let added = self.bitmap.insert(slot);
    if added && self.bitmap.len() as usize > self.max_size {
        self.needs_rebuild = true;
    }
    added
}
```

Rebuild on next query in `ConcurrentEngine::update_bound_from_results()` at `src/concurrent_engine.rs:941-943`:

```rust
if let Some(entry) = bc.get_mut(&bound_key) {
    if entry.needs_rebuild() {
        entry.rebuild(&sorted_slots, |slot| sort_field.reconstruct_value(slot));
    }
}
```

And in `QueryExecutor::form_or_update_bound()` at `src/executor.rs:334-338`:

```rust
if let Some(entry) = bc.get_mut(&bound_key) {
    if entry.needs_rebuild() {
        entry.rebuild(&sorted_slots, |slot| sort_field.reconstruct_value(slot));
    }
}
```

`BoundEntry::rebuild()` at `src/bound_cache.rs:142-161` replaces the bitmap with a fresh top-K set and clears the rebuild flag:

```rust
pub fn rebuild<F>(&mut self, sorted_slots: &[u32], value_fn: F)
where F: Fn(u32) -> u32,
{
    let take = sorted_slots.len().min(self.target_size);
    let mut new_bitmap = RoaringBitmap::new();
    for &slot in &sorted_slots[..take] {
        new_bitmap.insert(slot);
    }
    self.min_tracked_value = if take > 0 {
        value_fn(sorted_slots[take - 1])
    } else {
        0
    };
    self.bitmap = new_bitmap;
    self.needs_rebuild = false;
    self.last_used = Instant::now();
}
```

Config values at `src/config.rs:242-247`:

```rust
#[serde(default = "default_bound_target_size")]
pub bound_target_size: usize,
#[serde(default = "default_bound_max_size")]
pub bound_max_size: usize,
```

Defaults match spec at `src/config.rs:256-261`:

```rust
fn default_bound_target_size() -> usize { 10_000 }
fn default_bound_max_size() -> usize { 20_000 }
```

**Tests:**
- `test_bound_bloat_triggers_rebuild` at `src/bound_cache.rs:415-429`
- `test_bound_rebuild_resets` at `src/bound_cache.rs:432-454`
- `test_d9_bloat_triggers_rebuild_on_live_maintenance` at `src/bound_cache.rs:607-622`

**Gap:** None. Fully matches spec.

**Impact:** None.

---

### D5: Executor Integration

**Status:** COMPLIANT

**Spec Requirement:**
> Before sort traversal, check for matching bound. If found, AND bound bitmap with filter result to shrink working set.

**Evidence:**

**Primary path -- `ConcurrentEngine::apply_bound()` at `src/concurrent_engine.rs:813-878`:**
```rust
fn apply_bound(
    &self,
    _executor: &QueryExecutor,
    filter_bitmap: roaring::RoaringBitmap,
    use_simple_sort: bool,
    sort: Option<&SortClause>,
    filters: &[FilterClause],
    cursor: Option<&crate::query::CursorPosition>,
) -> (roaring::RoaringBitmap, bool, Option<CacheKey>) {
    // ...
    let mut bc = self.bound_cache.lock();
    // ... tier escalation loop ...
    if let Some(entry) = bc.lookup_mut(&try_key) {
        if !entry.needs_rebuild() {
            // ... cursor-past check ...
            entry.touch();
            let narrowed = &filter_bitmap & entry.bitmap();
            return (narrowed, false, cache_key);
        }
    }
    // ...
}
```

Called from `ConcurrentEngine::query()` at `src/concurrent_engine.rs:657-658`:
```rust
let (effective_bitmap, use_simple, cache_key) =
    self.apply_bound(&executor, filter_bitmap, use_simple_sort, sort, filters, None);
```

And from `ConcurrentEngine::execute_query()` at `src/concurrent_engine.rs:696-703`.

**Secondary path -- `QueryExecutor::apply_bound_if_available()` at `src/executor.rs:248-307`:**
Used by `execute_with_cache()`, also performs the AND narrowing:
```rust
entry.touch();
let narrowed = candidates & entry.bitmap();
return (narrowed, true);
```

**Gap:** None. The bound bitmap is ANDed with the filter result before sort traversal in all query paths.

**Impact:** None.

---

### D6: Tiered Bounds for Deep Pagination

**Status:** COMPLIANT

**Spec Requirement:**
> When cursor value drops below bound's minimum, fall back or spawn new tiered bound. Tiered bounds LRU-evicted.

**Evidence:**

**Tier field in BoundKey** at `src/bound_cache.rs:23`:
```rust
pub tier: u32,
```

**Tier escalation in `apply_bound()`** at `src/concurrent_engine.rs:841-874`:
```rust
let max_tiers = 4u32;
let mut try_key = bound_key;
for _tier in 0..max_tiers {
    if let Some(entry) = bc.lookup_mut(&try_key) {
        if !entry.needs_rebuild() {
            let cursor_past = if let Some(c) = cursor {
                let cursor_val = c.sort_value as u32;
                match sort_clause.direction {
                    SortDirection::Desc => cursor_val < entry.min_tracked_value(),
                    SortDirection::Asc => cursor_val > entry.min_tracked_value(),
                }
            } else {
                false
            };
            if cursor_past {
                try_key = BoundKey {
                    filter_key: try_key.filter_key,
                    sort_field: try_key.sort_field,
                    direction: try_key.direction,
                    tier: try_key.tier + 1,
                };
                continue;
            }
            entry.touch();
            let narrowed = &filter_bitmap & entry.bitmap();
            return (narrowed, false, cache_key);
        }
    }
    break;
}
```

**Tier-aware bound formation in `update_bound_from_results()`** at `src/concurrent_engine.rs:901-929`:
```rust
let tier = if let Some(c) = cursor {
    let cursor_val = c.sort_value as u32;
    let mut t = 0u32;
    let bc = self.bound_cache.lock();
    loop {
        let key = BoundKey { filter_key: ..., tier: t, ... };
        if let Some(entry) = bc.lookup(&key) {
            let past = match sort_clause.direction {
                SortDirection::Desc => cursor_val < entry.min_tracked_value(),
                SortDirection::Asc => cursor_val > entry.min_tracked_value(),
            };
            if past { t += 1; if t >= 4 { break; } continue; }
        }
        break;
    }
    // ...
    t
} else {
    0
};
```

**LRU eviction** at `src/bound_cache.rs:271-286`:
```rust
pub fn evict_lru(&mut self) -> Option<BoundKey> {
    let lru_key = self
        .bounds
        .iter()
        .min_by_key(|(_, entry)| entry.last_used())
        .map(|(key, _)| key.clone());
    if let Some(ref key) = lru_key {
        self.bounds.remove(key);
        if let Some(meta_id) = self.key_to_meta_id.remove(key) {
            self.meta.deregister(meta_id);
            self.meta_id_to_key.remove(&meta_id);
        }
    }
    lru_key
}
```

**Tests:**
- `test_d6_tier_field_in_bound_key` at `src/bound_cache.rs:667-693`
- `test_d6_tiered_bound_lru_eviction` at `src/bound_cache.rs:696-723`
- `test_d6_cursor_past_tier0_range` at `src/bound_cache.rs:726-773`
- Deep pagination benchmark in `src/bin/benchmark.rs:1341-1412` (Phase 5c)

**Gap:** The `evict_lru()` method exists but is never called automatically. There is no automatic eviction policy -- no maximum bound count or memory cap triggers eviction. The method exists for callers to invoke manually, but no code path does so. The spec says "Tiered bounds LRU-evicted based on usage" which implies automatic eviction.

**Impact:** Low. Without automatic eviction, bound count grows monotonically until the process restarts. For most workloads with a finite set of common queries, this is fine. For workloads with high filter diversity producing thousands of unique bound keys, memory could grow unbounded.

---

### D7: Invalidation Model Change

**Status:** NON-COMPLIANT

**Spec Requirement:**
> Shift from generation-counter lazy invalidation to live cache maintenance. Writes directly update relevant cache entries via diff layer. Generation counters become unnecessary for live-maintained caches.

**Evidence:**

The trie cache at `src/cache.rs` still uses generation-counter lazy invalidation. The `TrieCache` struct has:

```rust
field_generations: HashMap<String, u64>,
```

At `src/cache.rs:354-358`:
```rust
pub fn invalidate_field(&mut self, field: &str) {
    let gen = self.field_generations.entry(field.to_string()).or_insert(0);
    *gen += 1;
}
```

This generation-counter system is actively called from the flush thread at `src/concurrent_engine.rs:294-316`:
```rust
if coalescer.has_alive_mutations() {
    let mut c = cache.lock();
    for name in &field_names {
        c.invalidate_field(name);
    }
    // ...
} else {
    let changed = coalescer.mutated_filter_fields();
    if !changed.is_empty() {
        let mut c = cache.lock();
        for name in &changed {
            c.invalidate_field(name);
        }
    }
}
```

**What was done instead:**
The bound cache has its own separate live maintenance system (D3) that coexists with the trie cache's generation-counter system. The trie cache (filter result caching) and the bound cache (sort working set caching) are entirely independent systems:
- Trie cache: generation-counter lazy invalidation (unchanged from Phase 3).
- Bound cache: live maintenance via flush thread + mark-for-rebuild on filter changes.

**Gap:** The spec envisions a unified shift: "Generation counters become unnecessary for live-maintained caches." This has not happened. Generation counters remain the primary invalidation mechanism for the trie cache. The bound cache uses a different approach, but the two systems are not unified.

**Impact:** Moderate conceptually, low practically. The dual system works correctly -- trie cache entries are lazily invalidated via generation counters, and bound entries are maintained live for sort values and marked for rebuild on filter changes. The generation-counter system for the trie cache is well-tested and efficient. The risk is complexity: two invalidation models to reason about instead of one.

---

### D8: Tests -- Bound Formation and Usage

**Status:** COMPLIANT

**Spec Requirement:**
> Bound forms on first query, subsequent queries use it, sort result matches full traversal.

**Evidence:**

Unit tests in `src/bound_cache.rs`:
- `test_bound_formation_basic` (line 376): Forms a bound, verifies cardinality and min_tracked_value.
- `test_bound_formation_truncates_to_target` (line 395): Verifies target_size truncation.
- `test_lookup_miss_for_different_filter` (line 457): Different filter key misses.
- `test_lookup_miss_for_different_sort` (line 469): Different sort field misses.
- `test_lookup_miss_for_different_direction` (line 481): Different direction misses.
- `test_empty_sorted_slots` (line 529): Edge case: empty input.
- `test_total_memory_bytes` (line 541): Memory reporting.

Integration tests in `src/executor.rs`:
- `test_d5_bound_forms_on_first_sort_query` (line 1309): Full executor integration. Forms bound on first query, verifies it exists.
- `test_d5_bound_used_on_second_query` (line 1341): Verifies second query uses the bound and produces the same results.
- `test_d5_no_bound_without_sort` (line 1374): No bound formed for non-sort queries.
- `test_d5_different_filter_misses_bound` (line 1400): Different filter creates a separate bound.

**Gap:** None. Test coverage is thorough for formation and usage.

**Impact:** None.

---

### D9: Tests -- Live Maintenance Correctness

**Status:** COMPLIANT

**Spec Requirement:**
> Insert doc that qualifies for bound -> verify bit set. Bloat control trigger -> verify rebuild.

**Evidence:**

Tests in `src/bound_cache.rs`:
- `test_d9_live_maintenance_adds_qualifying_slot_desc` (line 556): Verifies slot added when value exceeds floor (Desc).
- `test_d9_live_maintenance_skips_below_threshold_desc` (line 575): Verifies threshold check logic.
- `test_d9_live_maintenance_adds_qualifying_slot_asc` (line 589): Verifies slot added when value below ceiling (Asc).
- `test_d9_bloat_triggers_rebuild_on_live_maintenance` (line 607): Verifies `needs_rebuild` flag set when bloat exceeds max_size.
- `test_d9_rebuild_flag_skips_live_maintenance` (line 625): Verifies rebuild flag prevents further additions.
- `test_d9_filter_invalidation_marks_bounds_for_rebuild` (line 648): Verifies filter field invalidation marks correct bounds.

**Gap:** These tests exercise the `BoundEntry` and `BoundCacheManager` data structures directly but do not test the flush thread integration path (the code at `concurrent_engine.rs:232-269`). There is no integration test that inserts a document via `ConcurrentEngine::put()`, waits for the flush thread, and then verifies the bound was updated. However, this is a testing thoroughness concern, not a spec compliance gap -- the D9 spec says "insert doc that qualifies for bound -> verify bit set" which these unit tests satisfy at the data structure level.

**Impact:** Low. An integration test through the flush thread would increase confidence but is not strictly required by the spec.

---

### D10: Benchmark -- Sort Query Latency

**Status:** COMPLIANT

**Spec Requirement:**
> Verify sort queries drop from 20ms+ to <1ms at 100M records with bound caches active.

**Evidence:**

The benchmark harness at `src/bin/benchmark.rs:1246-1338` implements "Phase 5b: Bound Cache Effectiveness" which:
1. Clears the bound cache (`engine.clear_bound_cache()` at line 1256).
2. Runs each sort query cold (no bound -- measures baseline).
3. Runs each sort query warm (100 iterations with bound -- measures p50, p95).
4. Reports speedup ratio (cold / warm p50).

Test queries include:
- Unfiltered sort by reactionCount (hardest case)
- nsfwLevel=1 + sort reactionCount
- nsfwLevel=1 + onSite=true + sort reactionCount
- Tag filter + sort reactionCount
- nsfwLevel=1 + sort commentCount
- nsfwLevel=1 + sort id Asc

Additionally, Phase 5c at line 1341 tests deep pagination (10 pages of cursor-based traversal) to verify tiered bounds maintain performance.

Bound cache stats are reported after both phases.

**Gap:** The acceptance criterion "<1ms with active bounds at 100M" cannot be verified from code alone -- it requires running the benchmark. The infrastructure to measure and report it is complete.

**Impact:** None for code audit. Benchmark results would need to be validated separately.

---

### Config Compliance

**Status:** PARTIAL

**Spec Config:**
```yaml
cache:
  bound_promotion_threshold: 0
  bound_target_size: 10000
  bound_max_size: 20000
```

**Implemented in `src/config.rs:232-270`:**
```rust
pub struct CacheConfig {
    pub max_entries: usize,
    pub decay_rate: f64,
    pub bound_target_size: usize,   // present, default 10_000
    pub bound_max_size: usize,      // present, default 20_000
}
```

**Gap:** `bound_promotion_threshold` is not a config field. The promotion threshold is hardcoded to 0 (every first query forms a bound). While this matches the spec's intended behavior, the config key is absent if anyone wanted to change it.

**Impact:** Negligible. The spec states `bound_promotion_threshold: 0` and the code hardcodes exactly that behavior.

---

## Summary Table

| Item | Description | Status | Key Issue |
|------|-------------|--------|-----------|
| D1 | Bound cache data structure | **PARTIAL** | Uses plain `RoaringBitmap` instead of `VersionedBitmap` with diff layer |
| D2 | Bound formation (threshold=0) | **COMPLIANT** | Every sort query forms/updates a bound immediately |
| D3 | Live maintenance on writes | **PARTIAL** | Sort value comparison works; filter definition check is missing |
| D4 | Bloat control | **COMPLIANT** | Cardinality tracking, rebuild flag, rebuild on next query |
| D5 | Executor integration | **COMPLIANT** | AND with bound bitmap before sort traversal in all paths |
| D6 | Tiered bounds for deep pagination | **COMPLIANT** | Tier escalation, cursor-past detection, LRU eviction method present (but not auto-triggered) |
| D7 | Invalidation model change | **NON-COMPLIANT** | Generation counters still active in trie cache; not replaced by live maintenance |
| D8 | Tests: Bound formation & usage | **COMPLIANT** | Unit + integration tests cover formation, lookup, miss, usage |
| D9 | Tests: Live maintenance correctness | **COMPLIANT** | Covers Desc/Asc qualification, bloat trigger, rebuild skip, filter invalidation |
| D10 | Benchmark: Sort query latency | **COMPLIANT** | Cold vs warm comparison, deep pagination, stats reporting |

---

## Recommendations

### High Priority

1. **D3: Add filter definition check in live maintenance.**
   Before calling `entry.add_slot(slot)`, the flush thread should verify the slot passes the bound's filter predicate. This requires either:
   - Evaluating the filter clauses from `bound_key.filter_key` against the staging snapshot's filter bitmaps for the slot, or
   - Storing a pre-computed filter bitmap reference in the bound entry for fast `contains(slot)` checks.

   Without this, bounds accumulate false positives from unrelated documents, accelerating bloat and reducing working set reduction effectiveness.

2. **D6: Add automatic LRU eviction trigger.**
   The `evict_lru()` method exists but is never called. Add a maximum bound count or memory budget check in `form_bound()` that triggers eviction when the limit is reached. Otherwise bound count grows monotonically.

### Medium Priority

3. **D7: Document the intentional deviation from the invalidation model change.**
   The dual-system approach (generation counters for trie cache, live maintenance for bounds) is pragmatic and correct but deviates from the spec's vision. If this is intentional, the spec should be updated. If not, a plan for migrating the trie cache to live maintenance should be outlined.

### Low Priority

4. **D1: Evaluate VersionedBitmap for bound bitmaps.**
   The plain `RoaringBitmap` + Mutex approach works at current scale. If contention becomes measurable at high bound counts with concurrent readers and the flush thread, consider wrapping bound bitmaps in `VersionedBitmap` for lock-free reader access.

5. **D9: Add flush-thread integration test.**
   Current D9 tests exercise data structures directly. An integration test through `ConcurrentEngine::put()` -> flush thread -> bound update -> query verification would increase confidence in the end-to-end path.

6. **Config: Add `bound_promotion_threshold` to `CacheConfig`.**
   Even though the default is 0 and that is the desired behavior, having the config field makes the system more configurable and self-documenting.
