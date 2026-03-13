# Phase E Audit: Meta-Index (Bitmaps Indexing Bitmaps)

**Auditor:** Claude Opus 4.6
**Date:** 2026-02-21
**Spec Source:** `docs/plans/roadmap-performance-and-persistence.md`, Phase E section
**Codebase SHA:** HEAD of `main` branch

---

## Summary

| E-Item | Requirement | Status | Severity |
|--------|------------|--------|----------|
| E1 | Cache entry ID allocation with recycling | **COMPLIANT** | -- |
| E2 | Meta-bitmap registry | **COMPLIANT** | -- |
| E3 | Write-path integration | **PARTIAL** | Medium |
| E4 | Planner integration | **NON-COMPLIANT** | High |
| E5 | Sort field meta-bitmaps | **COMPLIANT** | -- |
| E6 | Tests: Meta-index correctness | **PARTIAL** | Medium |

**Acceptance criteria status:** PARTIALLY MET. Write-path cache maintenance uses meta-index for sort field lookups (O(1) vs linear) but still uses linear scan for filter field invalidation in the flush loop. Query planner does NOT use the meta-index at all -- both `planner.rs` and the executor bound lookup path use direct HashMap key construction, not meta-bitmap intersection. The `find_matching_entries()` method exists in `MetaIndex` but is never called outside unit tests.

---

## E1: Cache Entry ID Allocation

**Status: COMPLIANT**

### Evidence

`src/meta_index.rs:23-25` -- Type alias:
```rust
pub type CacheEntryId = u32;
```

`src/meta_index.rs:66-69` -- Sequential allocation with free list:
```rust
pub struct MetaIndex {
    next_id: CacheEntryId,
    free_ids: Vec<CacheEntryId>,
    ...
}
```

`src/meta_index.rs:98-106` -- Allocation pops from free list, falls back to sequential:
```rust
fn allocate_id(&mut self) -> CacheEntryId {
    if let Some(id) = self.free_ids.pop() {
        id
    } else {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}
```

`src/meta_index.rs:170-203` -- Deregistration recycles IDs:
```rust
pub fn deregister(&mut self, id: CacheEntryId) {
    // ... removes from all bitmaps ...
    self.free_ids.push(id);
}
```

`src/bound_cache.rs:224-228` -- `BoundCacheManager::form_bound()` calls `deregister` on old entry before registering new one:
```rust
if let Some(old_meta_id) = self.key_to_meta_id.remove(&key) {
    self.meta.deregister(old_meta_id);
    self.meta_id_to_key.remove(&old_meta_id);
}
```

`src/bound_cache.rs:278-284` -- `evict_lru()` properly deregisters from meta-index:
```rust
if let Some(meta_id) = self.key_to_meta_id.remove(key) {
    self.meta.deregister(meta_id);
    self.meta_id_to_key.remove(&meta_id);
}
```

### Gap
None. ID allocation is sequential, recycling works via LIFO free list, eviction cleans up properly.

### Impact
None.

---

## E2: Meta-Bitmap Registry

**Status: COMPLIANT**

### Evidence

`src/meta_index.rs:71-83` -- Three tiers of meta-bitmaps are maintained:
```rust
clause_bitmaps: HashMap<ClauseKey, RoaringBitmap>,  // (field+op+value) -> entry IDs
field_bitmaps: HashMap<FieldKey, RoaringBitmap>,     // field name -> entry IDs
sort_bitmaps: HashMap<SortKey, RoaringBitmap>,       // (sort field+direction) -> entry IDs
```

`src/meta_index.rs:114-167` -- `register()` updates all three levels:
```rust
pub fn register(
    &mut self,
    filter_clauses: &[CanonicalClause],
    sort_field: Option<&str>,
    sort_direction: Option<SortDirection>,
) -> CacheEntryId {
    let id = self.allocate_id();
    // Updates clause_bitmaps, field_bitmaps (deduped), and sort_bitmaps
    ...
}
```

`src/meta_index.rs:170-203` -- `deregister()` cleans all three levels and removes empty bitmaps:
```rust
pub fn deregister(&mut self, id: CacheEntryId) {
    // Removes from clause_bitmaps, field_bitmaps, sort_bitmaps
    // Removes empty bitmaps entirely from the HashMap
    ...
}
```

`src/bound_cache.rs:233-240` -- `BoundCacheManager` registers bounds with the meta-index on creation:
```rust
let meta_id = self.meta.register(
    &key.filter_key,
    Some(&key.sort_field),
    Some(key.direction),
);
self.key_to_meta_id.insert(key.clone(), meta_id);
self.meta_id_to_key.insert(meta_id, key.clone());
```

### Gap
None. The registry covers clause-level, field-level, and sort-level meta-bitmaps. Registration and deregistration are symmetric.

### Impact
None.

---

## E3: Write-Path Integration

**Status: PARTIAL**

### What the spec requires
> Replace linear cache scan with meta-bitmap intersection to find relevant caches for a mutation.

### Evidence of USE (sort field mutations)

`src/concurrent_engine.rs:227-269` -- The flush loop uses `bounds_for_sort_field()` which delegates to the meta-index:
```rust
// D3/E3: Live maintenance of bound caches on sort field mutations.
// Uses meta-index for O(1) lookup of relevant bounds instead of
// linear scan.
{
    let sort_mutations = coalescer.mutated_sort_slots();
    if !sort_mutations.is_empty() {
        let mut bc = flush_bound_cache.lock();
        if !bc.is_empty() {
            for (sort_field, slots) in &sort_mutations {
                // E3: Use meta-index to find matching bounds (O(1) vs linear)
                let matching_keys = bc.bounds_for_sort_field(sort_field);
                ...
            }
        }
    }
}
```

`src/bound_cache.rs:321-327` -- `bounds_for_sort_field()` uses the meta-index:
```rust
pub fn bounds_for_sort_field(&self, sort_field: &str) -> Vec<BoundKey> {
    let entry_ids = self.meta.entries_for_sort_field(sort_field);
    entry_ids
        .iter()
        .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
        .collect()
}
```

This is correctly O(1) vs cache count -- it uses the meta-index bitmap to find relevant bounds.

### Evidence of NON-USE (filter field invalidation)

`src/concurrent_engine.rs:318-338` -- Filter field invalidation uses `invalidate_filter_field()`:
```rust
{
    let changed_filters = coalescer.mutated_filter_fields();
    let has_alive = coalescer.has_alive_mutations();
    if has_alive || !changed_filters.is_empty() {
        let mut bc = flush_bound_cache.lock();
        if !bc.is_empty() {
            if has_alive {
                // Alive changed -- mark ALL bounds for rebuild
                for (_, entry) in bc.iter_mut() {
                    entry.mark_for_rebuild();
                }
            } else {
                for field_name in &changed_filters {
                    bc.invalidate_filter_field(field_name);
                }
            }
        }
    }
}
```

`src/bound_cache.rs:303-314` -- `invalidate_filter_field()` does use the meta-index:
```rust
pub fn invalidate_filter_field(&mut self, field_name: &str) {
    if let Some(entry_ids) = self.meta.entries_for_filter_field(field_name) {
        let ids: Vec<CacheEntryId> = entry_ids.iter().collect();
        for meta_id in ids {
            if let Some(key) = self.meta_id_to_key.get(&meta_id) {
                if let Some(entry) = self.bounds.get_mut(key) {
                    entry.mark_for_rebuild();
                }
            }
        }
    }
}
```

This IS actually using the meta-index for filter field invalidation -- good.

### Evidence of NON-USE (alive invalidation -- linear scan)

`src/concurrent_engine.rs:326-330` -- When alive changes, ALL bounds are linearly scanned:
```rust
if has_alive {
    // Alive changed -- mark ALL bounds for rebuild
    for (_, entry) in bc.iter_mut() {
        entry.mark_for_rebuild();
    }
}
```

This is a linear scan over all bounds. However, this is arguably correct behavior since an alive bitmap change affects ALL bounds (alive is ANDed into every query implicitly). There is no meta-bitmap optimization possible here -- marking all entries is O(N) by necessity.

### Evidence of NON-USE (trie cache -- no meta-index integration)

The spec says "Write-path cache maintenance is O(1) vs cache count." The `TrieCache` (the filter result cache, distinct from bound caches) does NOT use the meta-index at all. Its invalidation path uses generation counters per filter field:

`src/concurrent_engine.rs:294-316` -- Trie cache invalidation:
```rust
if coalescer.has_alive_mutations() {
    let mut c = cache.lock();
    for name in &field_names {
        c.invalidate_field(name);
    }
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

This iterates over changed field names and bumps generation counters -- NOT over cache entries. This is already O(changed fields), not O(cache entries), so it achieves the same complexity goal without needing the meta-index. The generation counter approach from Phase 3 serves the same purpose here.

### Gap

1. The alive-mutation path is a linear scan over all bounds (`iter_mut()`). This is unavoidable since alive affects all bounds -- not a real gap.
2. The `TrieCache` invalidation does not use the meta-index but achieves O(changed_fields) via generation counters, which is functionally equivalent. The spec's phrase "replace linear cache scan" was written before the TrieCache already had generation counters. This is a design divergence but not a correctness or performance gap.

### Impact
**Low-Medium.** The actual write-path bound cache maintenance uses the meta-index for both sort field mutations (`bounds_for_sort_field`) and filter field invalidation (`invalidate_filter_field`). The remaining linear scan (alive mutations) is correct behavior. The TrieCache's generation-counter invalidation is already O(changed_fields) not O(cache_entries). The spirit of E3 is substantially met.

---

## E4: Planner Integration

**Status: NON-COMPLIANT**

### What the spec requires
> Replace linear cache search with meta-bitmap intersection to find matching bound/filter caches for a query.

The `find_matching_entries()` method exists on `MetaIndex` (line 245) and is designed exactly for this purpose. It intersects clause meta-bitmaps and sort meta-bitmaps to find matching cache entry IDs in O(1) vs cache count.

### Evidence of NON-USE

**`src/planner.rs`** -- No reference to `meta_index`, `MetaIndex`, or `find_matching_entries` anywhere in the file. The planner only does cardinality-based clause reordering.

**`src/executor.rs:248-307`** -- `apply_bound_if_available()` constructs a `BoundKey` directly and does a HashMap lookup:
```rust
let mut try_key = BoundKey {
    filter_key: filter_key.clone(),
    sort_field: sort_clause.field.clone(),
    direction: sort_clause.direction,
    tier: 0,
};
let mut bc = bc_mutex.lock();
let max_tiers = 4u32;
for _tier in 0..max_tiers {
    let Some(entry) = bc.lookup_mut(&try_key) else {
        break;
    };
    ...
}
```

**`src/concurrent_engine.rs:831-877`** -- `apply_bound()` also constructs a `BoundKey` and does direct HashMap lookup:
```rust
let bound_key = BoundKey {
    filter_key: filter_key.clone(),
    sort_field: sort_clause.field.clone(),
    direction: sort_clause.direction,
    tier: 0,
};
let mut bc = self.bound_cache.lock();
// ... direct lookup_mut ...
```

**`src/concurrent_engine.rs:882-948`** -- `update_bound_from_results()` also uses direct HashMap get/insert.

**`MetaIndex::find_matching_entries()`** is NEVER called outside of its own unit tests in `meta_index.rs`. Confirmed via grep -- the only call sites are in `src/meta_index.rs` test functions.

### Gap

The query-time bound cache lookup does NOT use meta-bitmap intersection. It constructs an exact `BoundKey` (from the canonical filter key + sort field + direction) and does a `HashMap::get()` on the bounds map.

This has two implications:

1. **It is already O(1) via HashMap.** The HashMap lookup is constant-time. The meta-index `find_matching_entries()` would also be constant-time (via bitmap intersections). So the performance goal of "O(1) vs cache count" is technically met by the existing HashMap approach.

2. **It misses the SUPERSET matching capability.** The meta-index can find cache entries that match a SUPERSET of the query's filter clauses (e.g., a query for `nsfwLevel=1` could match a bound registered for `nsfwLevel=1 AND onSite=true`). The current direct HashMap lookup requires an EXACT match on the canonical filter key. This means a bound formed for a broader query cannot be reused for a narrower query, even when the bound would still be valid.

### Impact

**High for feature completeness, low for performance.** The `find_matching_entries()` method is the most powerful feature of the meta-index: it enables fuzzy/superset matching of bounds to queries via bitmap intersection. Without it, each unique filter combination requires its own bound, increasing memory usage and reducing cache hit rates. However, the current implementation is still O(1) per lookup via HashMap, so the performance acceptance criterion is technically met.

The `find_matching_entries()` method is fully implemented and tested but simply never wired into the query path. This is an integration gap, not a missing implementation.

---

## E5: Sort Field Meta-Bitmaps

**Status: COMPLIANT**

### Evidence

`src/meta_index.rs:44-49` -- Sort key type:
```rust
struct SortKey {
    field: String,
    direction: SortDirection,
}
```

`src/meta_index.rs:79` -- Sort bitmaps stored:
```rust
sort_bitmaps: HashMap<SortKey, RoaringBitmap>,
```

`src/meta_index.rs:145-155` -- Sort key registered on bound creation:
```rust
let sort_key = match (sort_field, sort_direction) {
    (Some(field), Some(dir)) => {
        let sk = SortKey { field: field.to_string(), direction: dir };
        self.sort_bitmaps.entry(sk.clone()).or_default().insert(id);
        Some(sk)
    }
    _ => None,
};
```

`src/meta_index.rs:193-200` -- Sort bitmaps cleaned on deregistration:
```rust
if let Some(ref sk) = reg.sort_key {
    if let Some(bm) = self.sort_bitmaps.get_mut(sk) {
        bm.remove(id);
        if bm.is_empty() {
            self.sort_bitmaps.remove(sk);
        }
    }
}
```

`src/meta_index.rs:213-238` -- Query methods for sort field lookups:
```rust
pub fn entries_for_sort(&self, field: &str, direction: SortDirection) -> Option<&RoaringBitmap>
pub fn entries_for_sort_field(&self, field: &str) -> RoaringBitmap  // both directions
```

`src/bound_cache.rs:321-327` -- Actually used in the write path via `bounds_for_sort_field()`:
```rust
pub fn bounds_for_sort_field(&self, sort_field: &str) -> Vec<BoundKey> {
    let entry_ids = self.meta.entries_for_sort_field(sort_field);
    ...
}
```

`src/concurrent_engine.rs:239` -- Called in flush loop:
```rust
let matching_keys = bc.bounds_for_sort_field(sort_field);
```

### Gap
None. Sort field meta-bitmaps are maintained, queried, and actually used on the write path.

### Impact
None.

---

## E6: Tests: Meta-Index Correctness

**Status: PARTIAL**

### What the spec requires
> Intersection finds correct caches. Eviction updates meta-bitmaps. Sort field tracking.

### Evidence of existing tests

**`src/meta_index.rs` (lines 323-597)** -- 10 unit tests covering:

| Test | What it covers |
|------|---------------|
| `test_register_and_lookup` | Basic registration, filter field lookup, sort spec lookup |
| `test_deregister_frees_id` | ID recycling on deregistration |
| `test_deregister_cleans_up_bitmaps` | All meta-bitmaps cleaned on deregister |
| `test_entries_for_sort_field_both_directions` | Sort field query across both Asc/Desc |
| `test_find_matching_entries_intersection` | Multi-entry intersection finds correct entries |
| `test_find_matching_entries_narrows_with_more_clauses` | More clauses narrow the result set |
| `test_find_matching_entries_no_match` | No match returns empty |
| `test_find_matching_entries_wrong_sort` | Wrong sort direction returns empty |
| `test_multiple_entries_same_clause` | Multiple entries sharing a clause |
| `test_memory_bytes_nonzero` | Memory reporting |
| `test_id_recycling_order` | LIFO recycling order |

**`src/bound_cache.rs` (lines 354-773)** -- Tests that exercise meta-index through `BoundCacheManager`:

| Test | What it covers |
|------|---------------|
| `test_invalidate_filter_field` | Meta-index used for filter field invalidation |
| `test_evict_lru` | LRU eviction deregisters from meta-index |
| `test_d9_filter_invalidation_marks_bounds_for_rebuild` | Meta-index-based invalidation correctness |

### Gap

1. **No integration test for `find_matching_entries()` via `BoundCacheManager`.** Since `find_matching_entries()` is not wired into the query path (see E4), there are no tests proving that the meta-index correctly identifies matching bounds during query execution. The unit tests in `meta_index.rs` test the method directly, but no integration test proves it works end-to-end.

2. **No concurrent/stress test for meta-index under load.** All meta-index tests are single-threaded unit tests. There is no test that exercises meta-index updates (register/deregister) concurrent with reads -- though this may be acceptable since the `BoundCacheManager` is behind a `Mutex`.

3. **No test for the write-path sort-field meta-index lookup.** The `bounds_for_sort_field()` call in the flush loop (`concurrent_engine.rs:239`) is not covered by a dedicated test. The `test_d9_live_maintenance_adds_qualifying_slot_desc` and similar tests in `bound_cache.rs` test the slot-level add logic but do not test the meta-index-driven lookup path.

4. **No test proving eviction updates meta-bitmaps correctly end-to-end.** The `test_evict_lru` test in `bound_cache.rs` tests LRU eviction but does not explicitly verify that meta-bitmaps are updated after eviction (e.g., that `entries_for_filter_field` no longer returns the evicted entry's ID).

### Impact

**Medium.** The meta-index unit tests are thorough for the `MetaIndex` struct in isolation. But the spec calls for tests proving "intersection finds correct caches" -- and the intersection method (`find_matching_entries()`) is never called in production code, so there is no integration test proving the full path works. The eviction/meta-bitmap update path is implicitly tested but not explicitly verified at the meta-bitmap level.

---

## Acceptance Criteria Assessment

The spec states:

> **Acceptance:** Write-path cache maintenance is O(1) vs cache count. Query planner cache lookup is O(1) vs cache count. Correct cache entries identified via tiny bitmap intersections.

| Criterion | Status | Notes |
|-----------|--------|-------|
| Write-path O(1) vs cache count | **MET** | Sort field: `bounds_for_sort_field()` uses meta-index. Filter field: `invalidate_filter_field()` uses meta-index. Alive: linear but correct (affects all). |
| Query planner O(1) vs cache count | **TECHNICALLY MET, SEMANTICALLY NOT MET** | HashMap lookup is O(1), but it does NOT use meta-bitmap intersection. The `find_matching_entries()` superset-matching capability is unused. |
| Correct entries via bitmap intersections | **PARTIALLY MET** | Write path uses bitmap intersections. Query path uses HashMap lookup, not bitmap intersection. |

---

## Recommendations

### Must-fix (to achieve spec compliance)

1. **Wire `find_matching_entries()` into the query-time bound cache lookup.** Replace or augment the direct `BoundKey` HashMap lookup in `apply_bound()` / `apply_bound_if_available()` with a meta-index intersection that can find superset-matching bounds. This is the core value proposition of Phase E for the query path.

2. **Add integration tests for `find_matching_entries()` in query path.** Once wired, add tests showing that a bound formed for `nsfwLevel=1` is found when querying `nsfwLevel=1 AND onSite=true` (superset match).

### Nice-to-have

3. **Add explicit eviction + meta-bitmap verification test.** After `evict_lru()`, assert that `meta_index().entries_for_filter_field(field)` no longer contains the evicted entry's ID.

4. **Add a write-path integration test.** Test that a sort field mutation on the concurrent engine correctly discovers and updates the matching bound via the meta-index path (not just unit-test the `BoundCacheManager` in isolation).

---

## File Reference

| File | Relevant Lines | Role |
|------|---------------|------|
| `src/meta_index.rs` | 1-597 | MetaIndex struct: ID allocation, registry, lookup, tests |
| `src/bound_cache.rs` | 169-351 | BoundCacheManager: embeds MetaIndex, write-path integration |
| `src/concurrent_engine.rs` | 227-269 | Flush loop: E3 sort field maintenance via meta-index |
| `src/concurrent_engine.rs` | 318-338 | Flush loop: filter field invalidation via meta-index |
| `src/concurrent_engine.rs` | 813-877 | apply_bound(): direct HashMap lookup, NOT using meta-index |
| `src/executor.rs` | 248-307 | apply_bound_if_available(): direct HashMap lookup |
| `src/planner.rs` | 1-568 | No meta-index reference at all |
