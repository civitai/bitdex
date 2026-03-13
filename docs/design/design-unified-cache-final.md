# Unified Cache Architecture — Final Design

## Status: APPROVED — Ready for implementation

This document captures the finalized design for merging BitDex's trie cache, bound cache, and time bucket systems into a single unified cache. All open questions have been resolved through design discussion.

---

## 1. Why: The Problem

BitDex currently has three independent caching systems:

| System | Purpose | Key | Value |
|--------|---------|-----|-------|
| **Trie Cache** (`cache.rs`) | Filter result memoization | Canonical filter clauses | Exact filter bitmap |
| **Bound Cache** (`bound_cache.rs`) | Sort working set narrowing | Filter + sort + direction + tier | Approximate top-K bitmap |
| **Time Buckets** (`time_buckets.rs`) | Pre-computed time ranges | Bucket name | Time-windowed bitmap |

These systems don't talk to each other. The bound cache stores **global** top-K bitmaps (the top 10K docs by sort value across *all* documents). When a restrictive time filter is applied, the intersection between the global bound and the filter result collapses:

```
Global top 10K by reactionCount:  mostly old viral images
30-day bucket:                     ~424K recent docs
Intersection:                      ~80 docs → pagination dies by page 3
```

**Current workaround** (commit `9b19b15`): retry without bound on exhaustion. Works but loses all bound acceleration for time-filtered queries — the most common pattern on Civitai.

---

## 2. What: The Unified Cache

### Core Data Structure

Replace the trie cache, bound cache, and their separate meta-indexes with a single flat `HashMap`:

```rust
struct UnifiedCache {
    entries: HashMap<UnifiedKey, UnifiedEntry>,
    meta: MetaIndex,          // single meta-index for all entries
    max_entries: usize,       // LRU eviction threshold
}

/// Key: canonical filters + sort field + direction
struct UnifiedKey {
    filter_clauses: Vec<CanonicalClause>,  // sorted, deterministic
    sort_field: Arc<str>,
    direction: SortDirection,
}

/// Value: dynamically-sized bounded bitmap (starts 1K, grows to max_capacity)
struct UnifiedEntry {
    bitmap: Arc<RoaringBitmap>,     // bounded top-K within filter result
    min_tracked_value: u32,         // sort floor (Desc) or ceiling (Asc)
    capacity: usize,                // current tier: 1K, 2K, 4K, 8K, 16K
    max_capacity: usize,            // ceiling from config (default 16K)
    has_more: bool,                 // more results exist beyond current bound
    needs_rebuild: bool,            // bloat control flag (triggers at 2x capacity)
    rebuilding: AtomicBool,         // rebuild-in-progress guard
    last_used: Instant,             // LRU timestamp
}
```

### What Changed

| Aspect | Old | New |
|--------|-----|-----|
| **Structure** | Trie (prefix matching) + separate HashMap (bounds) | Single flat HashMap |
| **Key** | Filter-only (trie) / filter+sort+tier (bound) | `(filters, sort_field, direction)` — always includes sort |
| **Value** | Filter bitmap OR approximate top-K | Always bounded top-K bitmap |
| **Lookup** | Trie walk + prefix matching | O(1) HashMap exact match |
| **Eviction** | Exponential-decay hit scoring | Pure LRU |
| **Invalidation** | Generation counters (trie) + rebuild flags (bound) | Full live maintenance via meta-index |
| **Meta-indexes** | Two separate (one per cache) | One unified meta-index |
| **Prefix matching** | Supported | Dropped — not needed |
| **Tiers** | 4-tier pagination depth | Replaced by dynamic expansion (1K → 2K → 4K → 8K → 16K) |
| **Entry sizing** | Fixed 10K per entry | Dynamic: start 1K, grow on demand, cap at 16K |
| **total_matched** | u64 counter | Replaced by `has_more: bool` |
| **Superset matching** | Broad bound reuses | Dropped — each entry is specific and cheap to form |

### What Stays the Same

- **Time buckets**: Still pre-computed, still periodic refresh. A bucket clause (`bucket_gte:24h`) is just another filter clause in the unified key.
- **Canonical clause format**: `CanonicalClause { field, op, value_repr }` — same structure, same sorting rules.
- **Meta-index structure**: `(field, op, value) -> Set<EntryID>` — same pattern, unified across all entries.
- **Lock pattern**: Brief mutex locks (~us), never held during heavy computation.
- **ArcSwap snapshot reads**: Lock-free query path unchanged.

---

## 3. Design Decisions (Resolved)

### D1: Eviction — Pure LRU

Entries start small (1K docs, ~4KB bitmap) and grow on demand. Most entries never get paged past page 2, so ~80% stay at 1K. Pure LRU eviction at configurable `max_entries` (default 5000). Linear scan for LRU victim is 8μs at 5K entries (benchmarked — see Appendix B). No decay scoring needed.

### D2: Race-to-Create on Cache Miss

Two concurrent queries with the same key both miss, both compute, last write wins. Results are deterministic (same filter + same sort = same bounded bitmap, same `has_more` boolean). No singleflight mechanism needed — avoids locks on the read path.

### D3: Full Live Maintenance for All Clause Types

All clause types (Eq, NotEq, In, NotIn, Gt, Gte, Lt, Lte, compound And/Or/Not) are live-maintained. No generation-counter fallback. No invalidation.

**Why this works:** Every cached bitmap is bounded (max 10K docs). Updates are cheap — a roaring bitmap insert/remove on a 10K bitmap is nanoseconds.

**Per-slot contains() checks** (validated by microbenchmark — see Appendix B):

```
Flush batch: 50 slots changed nsfwLevel
Meta-index: entries referencing nsfwLevel -> {entry_7, entry_23, ...}

For entry_7 with And(Eq(nsfwLevel,1), Eq(type,"image")):
  -> For each of 50 slots:
       if engine.filter_bitmap("nsfwLevel", 1).contains(slot)
          && engine.filter_bitmap("type", "image").contains(slot):
         -> Check sort qualification (value > min_tracked?)
         -> entry.bitmap.insert(slot)
```

Each `contains()` on a roaring bitmap is O(log containers) — ~30ns per check. With 50 slots x 3 clauses = 150 lookups per entry. At 500 active entries: **1.5ms total** per flush cycle. Well within budget.

**Why not bitmap AND?** Microbenchmark showed bitmap AND (clone batch, intersect with field bitmap) is 2-3x slower due to container-level work even on tiny batch bitmaps. The contains() approach avoids all cloning.

**Scaling:** If entry counts exceed ~1000, group entries by clause signature — many entries share the same filter combo (e.g., `Eq(nsfwLevel,1) AND Eq(type,"image")`). Evaluate the clause combo once, apply results to all entries in the group. This brought 5000 entries from 34ms to 19.5ms in benchmarks.

### D4: Flat HashMap, No Trie

No prefix matching. Every unique `(filters, sort, direction)` is its own entry. On cache miss:

1. Compute filter bitmap — sub-ms (bitmap AND/OR on pre-built field bitmaps)
2. Sort traversal constrained to filter result — ~18ms for 424K (benchmarked), less for smaller
3. Cache the bounded entry at initial capacity (1K) — next query hits in ~μs

The cold-start cost is ~18ms for typical time-filtered queries, paid once per unique combo. Prefix matching added complexity to maintenance and reasoning without enough payoff.

### D5: Dynamic Bound Expansion (replaces fixed tiers)

Instead of fixed 10K per entry or 4-tier pagination, entries start small and grow on demand:

**Growth schedule:** 1K → 2K → 4K → 8K → 16K (doubling), capped at `max_capacity` (configurable, default 16K).

**Trigger:** Cursor exhaustion. When the paginator walks through the bounded bitmap and runs out of results before filling a page:

1. Entry knows its `min_tracked_value` (sort floor of current bound)
2. Sort traversal from that floor, constrained to filter bitmap, fetches next chunk
3. Append new slots to bounded bitmap, update `min_tracked_value`, double capacity
4. If expansion returned fewer results than chunk size, set `has_more = false`

Each entry is a **lazy cursor** into the full sorted result set, materialized in chunks.

**Why this is better than fixed 10K:** At high cache cardinality (10K-100K entries), most entries never get paged past page 2. Starting at 1K saves ~90% memory vs fixed 10K. Entries that ARE paged deeply grow to meet demand.

**Bloat control:** Threshold = 2 × current capacity. An entry at capacity 2K with cardinality growing past 4K flags `needs_rebuild`.

**Live maintenance:** Unchanged. A new slot with sort value below `min_tracked_value` still gets skipped. If the entry later expands, the expansion traversal picks it up.

### D5a: Bloat Rebuild — Stale-While-Revalidate

When `needs_rebuild` is flagged, the query returns the current (bloated) bitmap immediately. The rebuild runs asynchronously, guarded by `rebuilding: AtomicBool` so only one rebuild runs per entry at a time.

The bloated bitmap is still correct — it's a superset of ideal, so sort traversal on a slightly larger bitmap returns the same top-N. Rebuilding in the background keeps all queries fast.

### D5b: has_more Replaces total_matched

`total_matched: u64` is removed. Maintaining the counter correctly for all clause types (especially NotEq, Not, value-change transitions) adds real complexity for negligible benefit. The pagination layer needs to know *whether* more results exist, not *how many*.

`has_more: bool` is set at formation/expansion time based on whether the sort traversal returned a full chunk. The paginator discovers exhaustion naturally when results run out.

### D5c: Configuration

```rust
struct UnifiedCacheConfig {
    max_entries: usize,          // LRU eviction threshold (default 5000)
    initial_capacity: usize,     // starting bound size per entry (default 1000)
    max_capacity: usize,         // ceiling for dynamic expansion (default 16000)
    min_filter_size: usize,      // skip caching if filter result < this (default 1000)
}
```

`min_filter_size`: If a filter result has fewer docs than `initial_capacity`, there's no point forming a bound — sort the full filter result directly (sub-ms). This keeps the long tail of highly-selective queries (specific userId, rare tagId combos) from wasting cache slots.

### D6: Time Bucket Expiry via Diff

When a time bucket periodic rebuild produces a new bitmap:

1. Compute diff: `dropped_slots = old_bitmap AND NOT new_bitmap`
2. Meta-index lookup: find all entries with `bucket_gte:24h` clause
3. Remove `dropped_slots` from each entry's bitmap (if present)
4. No invalidation — entries stay live, just with expired docs removed

New docs entering the bucket are handled by normal live maintenance (flush thread `insert_slot` already works).

Staleness bounded by bucket refresh interval (5min for 24h, 1hr for 30d). This is stale-while-revalidate: queries serve current cache instantly, bucket rebuild happens in background on flush thread, diffs applied when ready.

### D7: Single Meta-Index

One `MetaIndex` for all unified cache entries. Same structure as today:

```rust
struct MetaIndex {
    clause_bitmaps: HashMap<ClauseKey, RoaringBitmap>,   // (field, op, value) -> entry IDs
    field_bitmaps: HashMap<FieldKey, RoaringBitmap>,     // field -> entry IDs (any op/value)
    sort_bitmaps: HashMap<SortKey, RoaringBitmap>,       // (sort_field, direction) -> entry IDs
    registrations: HashMap<CacheEntryId, EntryRegistration>,
}
```

Used only on the write path (flush thread maintenance). Read path is pure HashMap lookup — O(1).

---

## 4. Query Path

```
Query: {nsfwLevel=1, Gte(sortAtUnix, now-30d)}, sort=reactionCount DESC, limit=40

Step 1: ensure_fields_loaded()
   -> Load nsfwLevel filter + reactionCount sort (if pending)

Step 2: Load snapshot via ArcSwap (lock-free, ~1ns)

Step 3: Snap range filter to time bucket
   -> Gte(sortAtUnix, now-2592000) becomes bucket_gte("30d")

Step 4: Canonicalize key
   -> UnifiedKey {
        filter_clauses: [nsfwLevel:eq:1, sortAtUnix:bucket_gte:30d],
        sort_field: "reactionCount",
        direction: Desc,
      }

Step 5: HashMap lookup (O(1))
   -> Hit: return bounded bitmap (top 1K-16K within 30d + nsfwLevel=1)
   -> Miss: compute filter bitmap (sub-ms) -> sort traversal (~18ms) -> cache at initial capacity (1K)

Step 6: Sort the bounded bitmap
   -> 1K-16K candidates through sort layers -> return top 40

Step 7: If cursor exhausts bounded bitmap before filling page:
   -> Expand entry: sort traversal from min_tracked_value -> append next chunk
   -> Double capacity (1K -> 2K -> 4K...), update has_more

Step 8: Return results + cursor + has_more
   -> has_more tells pagination UI whether more pages exist
```

### has_more Instead of total_matched

The unified cache does not track exact result counts. `has_more: bool` tells the pagination UI whether more results exist beyond the current bound. Set at formation/expansion time based on whether the sort traversal returned a full chunk. No counter maintenance needed during live updates.

---

## 5. Flush Thread Maintenance

The flush thread is the single writer. Maintenance order:

```
1. DRAIN MUTATIONS from crossbeam channel
   -> coalescer.prepare()

2. APPLY MUTATIONS TO STAGING
   -> coalescer.apply_prepared(slots, filters, sorts)
   -> activate deferred alive slots

3. UNIFIED CACHE LIVE MAINTENANCE                          [NEW]
   Lock: unified_cache (brief)

   a) Collect changed slots as Vec<u32> with their field values from the coalescer
   b) For each changed filter field:
      -> meta_index.entries_for_filter_field(field) -> affected entry IDs
      -> For each affected entry:
         -> For each changed slot: evaluate filter match via contains() checks
            - Eq/NotEq/In: engine.filter_bitmap(field, value).contains(slot)
            - Gte/Gt/Lt/Lte: compare slot's field value (from flush batch) against threshold
            - BucketBitmap: bucket_bitmap.contains(slot)
            - And/Or: short-circuit evaluation of children
         -> For matching slots: check sort qualification (value > min_tracked?)
         -> Add/remove from entry's bounded bitmap
      -> If >1000 entries affected: group by clause signature, evaluate once per group
   c) For each changed sort field:
      -> meta_index.entries_for_sort_field(field) -> affected entry IDs
      -> For each affected entry:
         -> Check if changed slots' new sort values qualify
         -> Add qualifying slots to bounded bitmap
         -> Bloat control: if cardinality > max_size, flag for rebuild

4. TIME BUCKET INSERT/REMOVE MAINTENANCE
   Lock: time_buckets (brief)
   -> Same as today: add new slots to qualifying buckets, remove deleted slots

5. TIME BUCKET PERIODIC REBUILD (if due)
   -> Lock-free pattern: check -> release -> heavy work -> swap
   -> After swap: compute diff, push through unified cache meta-index    [NEW]

6. COMPACTION (every ~50 flush cycles)
   -> Merge dirty filter diffs into bases

7. PUBLISH SNAPSHOT
   -> inner.store(Arc::new(staging.clone()))
```

### Loading Mode

During bulk inserts (`enter_loading_mode()`):
- Skip all maintenance (steps 3-5)
- Skip snapshot publishing (step 7)
- On exit: compact diffs, clear entire unified cache, publish snapshot

---

## 6. Migration Plan

### Phase 1: UnifiedEntry + UnifiedKey types

**Files:** New `src/unified_cache.rs`

Create the core data structures:
- `UnifiedKey` — `(Vec<CanonicalClause>, Arc<str>, SortDirection)` with Hash + Eq
- `UnifiedEntry` — bounded bitmap + min_tracked + capacity + has_more + rebuilding guard + LRU
- `UnifiedCache` — HashMap + MetaIndex + LRU eviction
- Public API: `lookup()`, `store()`, `expand_entry()`, `update_entry()`, `remove_slots()`, `len()`, `clear()`

**Tests:**
- Store and exact hit
- Miss returns None
- LRU eviction at capacity
- Entry formation from sort traversal at initial capacity (1K)
- Dynamic expansion: 1K → 2K → 4K on cursor exhaustion
- Expansion stops at max_capacity
- Cold entry stays at 1K (never expanded, eventually evicted)
- has_more set correctly on formation and expansion
- Bloat control: threshold = 2 × current capacity

### Phase 2: Meta-Index Registration

**Files:** Modify `src/meta_index.rs` or integrate into `src/unified_cache.rs`

Register all entry types (Eq, NotEq, In, range, compound) with the meta-index. Today only Eq entries are registered in the trie cache — extend to all clause types.

**Tests:**
- NotEq entry registered and findable via `entries_for_filter_field()`
- In entry registered with all values
- Compound And/Or entries registered for each leaf field
- Deregistration on eviction cleans up all bitmaps

### Phase 3: Full Live Maintenance

**Files:** `src/unified_cache.rs`, modify flush thread in `src/concurrent_engine.rs`

Implement per-slot contains() maintenance (validated by microbenchmark — see Appendix B):
1. Collect changed slots with their field values from the coalescer
2. For each affected entry (via meta-index): evaluate filter match per slot via contains() checks
3. Check sort qualification for matching slots
4. Add/remove from bounded bitmap

Handle all clause types:
- **Eq(field, value):** `engine.filter_bitmap(field, value).contains(slot)`
- **NotEq(field, value):** `!engine.filter_bitmap(field, value).contains(slot)`
- **In(field, [v1, v2]):** `field_bitmap(v1).contains(slot) || field_bitmap(v2).contains(slot)`
- **Gte/Gt/Lt/Lte:** compare slot's field value (from flush batch) against threshold — scalar comparison, no bitmap lookup
- **Gte/Gt/Lt/Lte:** reconstruct slot's sort value, compare against threshold
- **BucketBitmap:** `bucket_bitmap.contains(slot)`
- **And(a, b):** evaluate both, short-circuit on first false
- **Or(a, b):** evaluate both, short-circuit on first true
- **Not(a):** evaluate inner, negate

**Tests:**
- Insert slot that matches Eq filter + qualifies for sort -> appears in cache
- Insert slot that matches filter but doesn't qualify for sort -> not in cache
- Insert slot that doesn't match filter -> not in cache
- Delete slot -> removed from cache
- NotEq maintenance: insert with value=1 updates NotEq(field, 2) but not NotEq(field, 1)
- In maintenance: insert with value in set -> added
- Compound And: only added if ALL clauses match
- Compound Or: added if ANY clause matches
- Bloat control: many inserts -> needs_rebuild flagged
- Sort value change: slot moves between entries appropriately

### Phase 4: Time Bucket Diff Integration

**Files:** `src/concurrent_engine.rs` (flush thread periodic rebuild section)

After time bucket rebuild produces new bitmaps:
1. Compute `dropped = old AND NOT new`, `added = new AND NOT old`
2. Meta-index lookup for `bucket_gte:<bucket_name>` entries
3. Remove dropped slots, add added slots (with sort qualification check)

**Tests:**
- Doc ages past 24h -> removed from cache entries with `bucket_gte:24h`
- New doc enters 24h window -> added to qualifying cache entries
- 7d rebuild doesn't affect 24h cache entries
- Rapid bucket rebuild (5-minute cycle) correctly diffs

### Phase 5: Wire Into Query Path

**Files:** `src/concurrent_engine.rs` (`execute_query`, `resolve_filters`, `apply_bound`)

Replace the current two-step flow (trie cache lookup → bound cache narrowing) with single unified cache lookup:

1. Compute filter bitmap (bitmap AND/OR on pre-built field bitmaps) — sub-ms
2. **`min_filter_size` check:** If `filter_bitmap.len() < config.min_filter_size` (default 1000), skip caching entirely — sort the filter bitmap directly (sub-ms for <1K docs). Return results without touching the unified cache. This keeps the long tail of highly-selective queries (specific userId, rare tagId combos) from filling cache slots.
3. Canonicalize `(filters, sort_field, direction)` → `UnifiedKey`
4. `unified_cache.lookup(&key)` → Hit or Miss
5. On hit: sort the bounded bitmap directly (already narrowed)
6. On miss: sort traversal constrained to filter bitmap → form bounded entry at initial capacity → cache
7. On cursor exhaustion: expand entry (see D5)
8. Remove `apply_bound()`, `update_bound_from_results()`, and the retry-without-bound fallback — they're no longer needed

**Tests:**
- Query hits unified cache on second call
- Different sort on same filter = different cache entry
- Cache miss computes and stores correctly
- has_more correctly set on formation and expansion
- Small filter result (<1K docs) bypasses cache entirely, returns correct results
- Small filter result does NOT create a cache entry
- Dynamic expansion works: cursor exhaustion triggers growth
- Pagination through dynamically-expanding entries works without exhaustion

### Phase 6: Remove Old Systems

**Files:** Delete or gut `src/cache.rs` (trie), `src/bound_cache.rs` (bound manager)

- Remove `TrieCache`, `CacheLookup`, prefix matching, generation counters
- Remove `BoundCacheManager`, `BoundKey`, `BoundEntry`, tiered bounds
- Remove separate `self.cache` and `self.bound_cache` from `ConcurrentEngine`
- Replace with single `self.unified_cache: Arc<Mutex<UnifiedCache>>`
- Keep `MetaIndex` (reused in unified cache)
- Keep `TimeBucketManager` (still handles bucket bitmaps, just feeds diffs to unified cache)

**Tests:**
- All existing integration tests pass (swap cache backend, same query results)
- Benchmark suite shows no regression (or improvement on time-filtered sorts)

---

## 7. Test Strategy

### Regression Tests (Must Pass)

All existing tests (145+) must continue to pass. The unified cache is an internal optimization — query results must be identical.

Key existing test files:
- `tests/phase1_integration.rs` — 26 end-to-end filter/sort/delete/patch tests
- `tests/proptest_correctness.rs` — Property test: cursor pagination covers all results exactly once
- `tests/time_handling_test.rs` — Deferred visibility + time bucket tests
- `src/executor.rs` — 32+ unit tests for filter/sort/pagination
- `src/cache.rs` — 28 tests (will be replaced by unified cache tests)
- `src/bound_cache.rs` — 16 tests (will be replaced)

### New Tests: Pagination Robustness

These directly target the bugs we've hit:

```
test_pagination_with_time_filter_no_exhaustion()
  — Filter: Eq(nsfwLevel, 1) + bucket_gte(30d)
  — Sort: reactionCount DESC
  — Page through all results (40 per page)
  — Assert: every page returns results until total exhausted
  — Assert: no gaps, no duplicates across all pages
  — This is the exact scenario that broke before (page 3 returned 0)

test_pagination_24h_sort_oldest()
  — Filter: bucket_gte(24h)
  — Sort: sortAt ASC (oldest first)
  — Verify: results are in ascending time order
  — Verify: all pages return results

test_pagination_1y_deep()
  — Filter: bucket_gte(1y) (~53M docs)
  — Sort: reactionCount DESC
  — Page 50 deep (50 pages x 100 per page = 5000 results)
  — Verify: no exhaustion, monotonically decreasing sort values

test_live_maintenance_preserves_pagination()
  — Start paginating with filter + sort
  — Between page 2 and 3: insert 10 new docs that match the filter
  — Verify: page 3 returns results (live maintenance added new docs to cache)
  — Verify: no duplicates with page 1-2 results

test_time_bucket_expiry_removes_from_cache()
  — Populate cache entry with bucket_gte(24h) filter
  — Simulate bucket rebuild that drops 50 expired slots
  — Verify: expired slots no longer in cached bitmap
  — Verify: cache entry still serves correct results

test_concurrent_queries_same_key()
  — Launch 10 queries with identical (filter, sort, direction)
  — All should return identical results
  — Cache should have exactly 1 entry (race-to-create, last write wins)
```

### New Tests: Live Maintenance Correctness

```
test_eq_live_maintenance()
  — Cache entry for Eq(nsfwLevel, 1) + sort reactionCount DESC
  — Insert doc with nsfwLevel=1, reactionCount=high -> appears in cache
  — Insert doc with nsfwLevel=2 -> does NOT appear in cache
  — Delete doc from cache -> removed

test_noteq_live_maintenance()
  — Cache entry for NotEq(nsfwLevel, 1) + sort reactionCount DESC
  — Insert doc with nsfwLevel=2 -> appears (matches NotEq)
  — Insert doc with nsfwLevel=1 -> does NOT appear

test_in_live_maintenance()
  — Cache entry for In(nsfwLevel, [1, 2]) + sort
  — Insert doc with nsfwLevel=1 -> appears
  — Insert doc with nsfwLevel=3 -> does NOT appear

test_compound_and_live_maintenance()
  — Cache entry for And(Eq(nsfwLevel, 1), Eq(onSite, true)) + sort
  — Insert doc matching both -> appears
  — Insert doc matching only one -> does NOT appear

test_compound_or_live_maintenance()
  — Cache entry for Or(Eq(nsfwLevel, 1), Eq(type, "image")) + sort
  — Insert doc matching either -> appears
  — Insert doc matching neither -> does NOT appear

test_sort_qualification()
  — Cache entry for filter + reactionCount DESC, min_tracked=500
  — Insert doc matching filter with reactionCount=600 -> added (above floor)
  — Insert doc matching filter with reactionCount=400 -> NOT added (below floor)

test_bloat_control()
  — Cache entry at capacity=1000, bloat threshold=2000
  — Add 2000 slots via live maintenance -> needs_rebuild flagged
  — Query returns bloated bitmap immediately (stale-while-revalidate)
  — Background rebuild trims to capacity, clears flag

test_bloat_rebuild_serves_stale()
  — Flag entry for rebuild
  — Query returns current bitmap immediately (does not block)
  — Verify entry is trimmed after rebuild completes
  — Verify rebuilding guard prevents concurrent rebuilds

test_has_more_flag()
  — Form entry where filter has 50K docs, capacity=1K
  — has_more = true (50K > 1K)
  — Expand to 2K -> has_more still true
  — Expand until traversal returns partial chunk -> has_more = false

test_dynamic_expansion_on_pagination()
  — Form entry at capacity 1K
  — Paginate past 1K results -> triggers expansion to 2K
  — Continue paginating -> expansion to 4K
  — Verify no gaps, no duplicates across expansion boundaries

test_expansion_stops_at_max_capacity()
  — Set max_capacity=4K
  — Expand 1K -> 2K -> 4K
  — Attempt further expansion -> stays at 4K, has_more may be false

test_range_clause_uses_flush_values()
  — Cache entry with Gte(reactionCount, 100) + sort
  — Insert doc with reactionCount=150 -> matches (scalar comparison from flush batch)
  — Insert doc with reactionCount=50 -> does NOT match
  — Verify no sort layer traversal or bitmap lookup used for range evaluation
```

### New Tests: Meta-Index Coverage

```
test_meta_index_all_clause_types()
  — Register entries with Eq, NotEq, In, Gte, BucketBitmap, And, Or
  — Verify entries_for_filter_field() returns correct sets
  — Verify entries_for_clause() returns exact matches
  — Deregister and verify cleanup

test_meta_index_batch_lookup()
  — Register 1000 entries
  — Look up entries for a popular field (e.g., nsfwLevel)
  — Verify O(1) lookup time (not linear scan)
```

### Benchmark Validation

Run the existing 20-query benchmark suite before and after migration. Key metrics:

| Query | Target | Notes |
|-------|--------|-------|
| filter_nsfw1_sort_reactions | No regression | Common production query |
| filter_3_clauses_sort_reactions | No regression | Multi-clause + sort |
| sort_reactionCount_desc | No regression | Broad sort |
| **NEW: filter_nsfw1_30d_sort_reactions** | **< 5ms p50** | **Was 10-12ms with retry fallback** |
| **NEW: filter_1y_sort_reactions** | **< 10ms p50** | **Was 15-27s without bound** |

The time-filtered sort queries should see the biggest improvement — from retry-fallback latency to direct cache hit.

---

## 8. Files Affected

| File | Action | Description |
|------|--------|-------------|
| `src/unified_cache.rs` | **NEW** | UnifiedCache, UnifiedKey, UnifiedEntry, public API |
| `src/meta_index.rs` | **MODIFY** | Extend registration to all clause types (currently Eq-only in trie) |
| `src/concurrent_engine.rs` | **MODIFY** | Replace trie+bound cache with unified cache in flush thread + query path |
| `src/cache.rs` | **DELETE** | Trie cache replaced by unified cache |
| `src/bound_cache.rs` | **DELETE** | Bound cache replaced by unified cache |
| `src/time_buckets.rs` | **KEEP** | Still computes bucket bitmaps, now feeds diffs to unified cache |
| `src/query.rs` | **KEEP** | Canonicalization + bucket snapping unchanged |
| `src/planner.rs` | **MODIFY** | Remove prefix-matching awareness, simplify to just cardinality ordering |
| `src/executor.rs` | **MODIFY** | Remove bound-specific code, simplify execute flow |
| `src/lib.rs` | **MODIFY** | Add unified_cache module, remove cache + bound_cache |
| `tests/unified_cache_test.rs` | **NEW** | All new tests from section 7 |
| `src/bin/benchmark.rs` | **MODIFY** | Add time-filtered sort benchmark queries |

---

## 9. Memory Budget

With dynamic sizing, most entries stay at initial capacity (1K = ~4KB bitmap). ~20% expand under pagination.

| Active Entries | Fixed 10K (old design) | Dynamic (mostly 1K) | Savings |
|---------------|----------------------|---------------------|---------|
| 500 | ~20MB | ~2MB | 90% |
| 5,000 | ~200MB | ~20MB | 90% |
| 10,000 | ~400MB | ~40MB | 90% |
| 100,000 | ~4GB | ~400MB | 90% |

Assumes ~80% at 1K, ~15% at 2K-4K, ~5% at 8K-16K. Meta-index and HashMap overhead add <1MB at any scale.

Even at 100K active entries with dynamic sizing, total cache memory is ~400MB — well within the 14.5GB RSS budget. This enables aggressive `max_entries` settings without memory concern.

---

## 10. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Combinatorial explosion of cache entries (many filter+sort combos) | Medium | Memory growth | Dynamic sizing (1K initial) = ~90% less memory than fixed 10K. LRU eviction at configurable max_entries. |
| Flush thread maintenance takes too long with many entries | Low | Write latency | Contains() approach: ~3μs/entry (benchmarked). 500 entries = 1.5ms. Group by clause signature above 1000 entries. |
| Race-to-create wastes CPU on concurrent misses | Low | CPU spike | Results are deterministic; last write wins. Formation cost ~18ms (benchmarked). |
| Dynamic expansion adds query-path latency | Low | Tail latency | Expansion only on cursor exhaustion (~18ms for 424K filter). Stale-while-revalidate for bloat rebuilds. |
| Time bucket diff misses edge cases | Medium | Stale results | Comprehensive test suite for expiry; worst case is 5-minute staleness (same as today) |
| Regression in non-time-filtered queries | Low | Performance | All 145+ existing tests must pass; benchmark suite validates |

---

## Appendix A: Current Architecture Reference

### Trie Cache (`src/cache.rs`) — TO BE REPLACED

- 28 tests, 1427 lines
- `TrieCache` with `TrieNode` tree structure
- `CacheEntry`: bitmap + hit_score + generations + meta_id
- `CacheLookup`: ExactHit / PrefixHit / Miss
- `canonicalize()` + `canonicalize_with_buckets()` (KEEP these functions)
- Generation-counter invalidation (REMOVE)
- Exponential-decay eviction (REPLACE with LRU)
- Eq-only live maintenance (EXTEND to all clause types)

### Bound Cache (`src/bound_cache.rs`) — TO BE REPLACED

- 16 tests, 839 lines
- `BoundKey`: (filter_key, sort_field, direction, tier)
- `BoundEntry`: bitmap + min_tracked_value + target/max_size + needs_rebuild
- `BoundCacheManager` with separate MetaIndex
- Superset matching via `find_superset_bound()` (DROP)
- Tiered bounds (DROP — 10K is deep enough)
- Live maintenance: add-only, bloat control (KEEP pattern in unified cache)

### Meta-Index (`src/meta_index.rs`) — KEEP AND EXTEND

- 610 lines
- `clause_bitmaps`, `field_bitmaps`, `sort_bitmaps`
- `register()` / `deregister()` with ID recycling
- Currently: trie cache registers Eq-only; bound cache registers all
- Unified: register ALL clause types for ALL entries

### Flush Thread Lock Order (current, `src/concurrent_engine.rs`)

```
1. flush_bound_cache.lock()   — sort maintenance
2. flush_time_buckets.lock()  — insert/remove
3. cache.lock()               — trie live updates + invalidation
4. flush_bound_cache.lock()   — filter invalidation
5. docstore.lock()            — batch writes
```

### Flush Thread Lock Order (unified)

```
1. unified_cache.lock()       — all live maintenance (replaces steps 1, 3, 4)
2. flush_time_buckets.lock()  — insert/remove + diff to unified cache
3. docstore.lock()            — batch writes
```

Fewer locks, simpler ordering, single maintenance block.

---

## Appendix B: Microbenchmark Results

**Test file:** `tests/cache_maintenance_bench.rs`
**Setup:** 105M-scale roaring bitmaps (60M nsfwLevel=1, 80M type=image, 424K 30d bucket, etc.), 50-slot flush batch, 2-3 clauses per entry.

### Approach Comparison

| Approach | 100 entries | 500 entries | 1000 entries | 5000 entries |
|----------|------------|------------|-------------|-------------|
| **Bitmap AND** (clone batch, AND with field bitmaps) | 0.84ms | 3.6ms | 10.7ms | 58ms |
| **Contains() per slot** (check membership in each clause bitmap) | 0.32ms | 1.5ms | 3.7ms | 34ms |
| **Grouped** (dedupe clause combos, evaluate once per group) | — | — | — | 19.5ms |

### Why Contains() Wins

Bitmap AND on a 50-bit batch against a 60M-bit field bitmap still does container-level intersection work internally. `contains()` is O(log containers) per check — with 50 slots x 3 clauses = 150 lookups per entry, this is pure pointer chasing with no allocation.

### Recommendations (incorporated into design)

1. **Use contains() approach** — 2-3x faster than bitmap AND
2. **500 entries at 1.5ms is the sweet spot** — well within flush thread budget
3. **Group by clause signature above 1000 entries** — common filter combos evaluated once
4. **Keep flush batches to 50-200 slots** — cost scales linearly with batch size

### Batch Size Sensitivity

| Batch size | 1000 entries (contains) |
|-----------|------------------------|
| 50 slots | 3.7ms |
| 200 slots | ~15ms (extrapolated) |
| 1000 slots | ~75ms (measured) |

For write bursts producing large batches, consider sub-batching the maintenance pass or deferring maintenance until the next flush cycle.

---

## Appendix C: Validated Assumptions

All four assumptions have been microbenchmarked. Test files in `tests/bench_*.rs`.

### C1: HashMap Lookup Latency with Complex Keys — VALIDATED

| N entries | Standard SipHash (hit) | FxHash (hit) |
|----------|----------------------|-------------|
| 500 | 253ns | 154ns |
| 5,000 | 453ns | 309ns |

**Verdict:** Sub-microsecond at all scales. No pre-hashing needed. Optional: switch to `rustc-hash` (FxHashMap) for free 1.4x improvement.

### C2: Time Bucket Diff Cost — VALIDATED

| Bucket | XOR + ANDNOT | Full pipeline (with meta-index lookups) |
|--------|-------------|----------------------------------------|
| 24h (1.5K) | 4μs | 55μs |
| 30d (424K) | 314μs | 1.4ms |
| 1y (53M) | 14ms | 17.6ms |

**Verdict:** 24h/30d negligible. 1y at 17.6ms is acceptable for hourly periodic rebuild on the flush thread.

### C3: Sort Traversal for Bound Formation — VALIDATED (revised upward)

| Filter size | Traversal time |
|------------|---------------|
| 1K-10K | SKIP (< 100μs) |
| 100K | 14ms |
| 424K (30d) | **18ms** |
| 1M | 21ms |
| 5M | 30ms |
| 21M | 63ms (worst) |
| 53M (1y) | 27ms |

**Verdict:** ~18ms for typical 30d filter, not the assumed 10ms. Still acceptable as one-time cold-start cost per cache miss. Bottleneck is AND operations against 52.5M-cardinality sort layers (~500μs per layer × 32 layers). With dynamic expansion starting at 1K instead of 10K, formation is slightly faster (fewer results to collect).

**Future optimization (deferred):** Log encoding / reduced bit depth could shrink from 32 to 8-16 layers, cutting traversal time proportionally. Not needed now — cold-start is one-time per cache entry.

### C4: LRU Eviction Overhead — VALIDATED

| N entries | Linear scan (lookup) | Full evict+reinsert cycle |
|----------|---------------------|--------------------------|
| 500 | 960ns | 3.2μs |
| 5,000 | 8μs | 33μs |
| 10,000 | 15μs | 122μs |

**Verdict:** Linear scan is fine. 8μs at 5K entries, only on cache miss. BTreeMap secondary index provides no practical advantage (full cycle cost dominated by HashMap mutation and bitmap deallocation, not the scan itself).
