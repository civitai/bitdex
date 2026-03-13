# Unified Cache Architecture — Design Discussion

## Purpose

BitDex currently has **two independent caching systems** that solve related problems but don't talk to each other. This causes a real pagination bug when time-filtered queries interact with the bound cache. This document captures the current architecture, the problem, and a proposed direction for unification.

---

## Current Architecture: Two Caches

### 1. Trie Cache (`src/cache.rs`)

**Problem it solves:** "Which document IDs match these filters?"

| Property | Value |
|----------|-------|
| **Stores** | Exact filter result bitmaps (`Arc<RoaringBitmap>`) |
| **Keyed by** | Canonicalized filter clauses (sorted by field name, op, value) |
| **Invalidation** | Per-field generation counters (lazy, O(1) write / O(N) read where N = fields in key) |
| **Eviction** | Exponential-decay hit scores, evict at `max_entries` (10K default) |
| **Lookup cost** | ~1ns (Arc clone) on exact hit |
| **Scope** | Filter bitmaps ONLY — sort is not part of cache key |

**Key features:**
- **Prefix matching**: Query `{a=1, b=2, c=3}` can get a prefix hit from cached `{a=1, b=2}`, then only compute clause `c=3` against the prefix result
- **Bucket snapping (C5)**: `Gte(sortAtUnix, now-86400)` becomes cache key `sortAtUnix:bucket_gte:24h` — stable across requests despite changing timestamps
- **Live updates**: Eq-only entries updated in-place via meta-index during flush (add/remove slots without recomputation)
- **Generation counters**: Each filter field has a monotonic counter. Cache entries store a generation snapshot; stale entries discarded on lookup

**What it does NOT do:**
- No awareness of sort fields or sort order
- No concept of "top-K" or working sets
- Same cache entry used regardless of sort — `{nsfwLevel=1}` sorted by `reactionCount DESC` and `sortAt ASC` both use the same cached filter bitmap

### 2. Bound Cache (`src/bound_cache.rs`)

**Problem it solves:** "What are the approximate top-K documents for this filter+sort combination?"

| Property | Value |
|----------|-------|
| **Stores** | Approximate top-K bitmaps + `min_tracked_value` threshold |
| **Keyed by** | `(filter_key, sort_field, direction, tier)` — 4-tuple |
| **Invalidation** | Live maintenance (add qualifying slots) + rebuild flags |
| **Eviction** | LRU at 100 bounds |
| **Lookup cost** | ~1μs (HashMap + mutex) |
| **Scope** | Sort acceleration — narrows candidates before sort traversal |

**Key features:**
- **Tiered bounds (D6)**: Tier 0 = top 10K, tier 1 = next 10K, up to 4 tiers for deep pagination
- **Live maintenance (D3)**: Flush thread adds newly inserted slots that qualify (`value > min_tracked_value` for DESC)
- **Bloat control (D4)**: When cardinality exceeds `max_size` (20K), flag for rebuild
- **Superset matching (E4)**: Bound for `{nsfwLevel=1}` can narrow query `{nsfwLevel=1, onSite=true}` — broader bound works for more specific queries
- **Meta-index (E3)**: `src/meta_index.rs` — O(1) lookup of which bounds reference a given filter/sort field, enabling targeted invalidation instead of scanning all bounds

**What it does NOT do:**
- No awareness of time buckets or temporal filters
- Bounds are formed from the **global** sort ranking, not per-filter-combination rankings
- No prefix matching — bounds are exact key matches (plus superset matching)

### 3. Time Buckets (`src/time_buckets.rs`)

**Not a cache, but relevant context.** Pre-computed bitmaps for time windows:

| Bucket | Duration | Refresh |
|--------|----------|---------|
| 24h | 86,400s | 5 min |
| 7d | 604,800s | 15 min |
| 30d | 2,592,000s | 1 hour |
| 1y | 31,536,000s | 1 hour |

- Rebuilt on flush thread via single-pass over all alive slots (105M → ~104s rebuild)
- Lock-free rebuild pattern: compute outside lock, swap in briefly
- Live maintenance: new inserts added to qualifying buckets immediately
- At 105M: 24h has ~1.5K docs, 30d has ~424K, 1y has ~53M

---

## Flush Thread: Cache & Index Maintenance Orchestration

The flush thread is the single writer that maintains all caching and indexing state. It drains mutation ops from the crossbeam channel, applies them to staging, then runs maintenance in a fixed order. Understanding this order is critical for unification — today these are **five independent maintenance blocks** with different lock patterns.

### Flush Cycle Order (when mutations exist, not in loading mode)

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. APPLY MUTATIONS TO STAGING                                       │
│    coalescer.apply_prepared(slots, filters, sorts)                  │
│    + activate deferred alive slots whose time has come              │
│    → staging now has the new bitmap state                           │
├─────────────────────────────────────────────────────────────────────┤
│ 2. BOUND CACHE — Sort Live Maintenance (D3/E3)                     │
│    Lock: flush_bound_cache (brief)                                  │
│    For each mutated sort field:                                     │
│      → meta-index O(1) lookup: which bounds sort by this field?    │
│      → For each matching bound:                                     │
│          if slot's new value > min_tracked_value (Desc): add slot   │
│          if slot's new value < min_tracked_value (Asc): add slot    │
│    For newly alive slots:                                           │
│      → add to all __slot__-sorted Desc bounds                      │
│    Note: add-only. Bloat control (D4) handles cleanup.             │
│    Note: if entry.needs_rebuild(), skip it entirely.               │
├─────────────────────────────────────────────────────────────────────┤
│ 3. TIME BUCKET — Insert/Remove Maintenance                         │
│    Lock: flush_time_buckets (brief)                                 │
│    For each newly alive slot:                                       │
│      → reconstruct timestamp from sort field bit layers             │
│      → add to all buckets where cutoff <= ts <= now                 │
│    For each removed slot:                                           │
│      → remove from ALL buckets unconditionally                      │
├─────────────────────────────────────────────────────────────────────┤
│ 4. TRIE CACHE — Live Updates + Invalidation                        │
│    Lock: cache (brief, two sub-phases)                              │
│                                                                     │
│    IF alive changed:                                                │
│      → invalidate ALL filter fields (bump every generation counter) │
│      → reason: NotEq/Not bake alive into results, can't live-update│
│                                                                     │
│    ELSE (filter-only mutations):                                    │
│      a) Live-update Eq entries:                                     │
│         → meta-index lookup: which cache entries have Eq(field, v)? │
│         → Arc::make_mut on cached bitmap, insert/remove slot        │
│         → track which (entry_id, field) pairs were live-updated     │
│      b) Invalidate all changed fields:                              │
│         → bump generation counter (kills non-Eq entries like In,    │
│           NotEq, range filters)                                     │
│      c) Refresh live-updated Eq entries:                            │
│         → advance their stored generation to current                │
│         → prevents the bump in (b) from falsely killing them        │
│                                                                     │
│    This 3-step dance (update → invalidate → refresh) is the most   │
│    subtle part of cache maintenance.                                │
├─────────────────────────────────────────────────────────────────────┤
│ 5. BOUND CACHE — Filter Invalidation (D3)                          │
│    Lock: flush_bound_cache (brief)                                  │
│    IF alive changed:                                                │
│      → mark ALL bounds for rebuild                                  │
│    ELSE:                                                            │
│      → for each changed filter field:                               │
│          bc.invalidate_filter_field(name)                           │
│          → uses meta-index to find bounds referencing this field    │
│          → marks only those bounds for rebuild                      │
│    Note: bounds are NOT rebuilt here — next query does the rebuild  │
├─────────────────────────────────────────────────────────────────────┤
│ 6. COMPACTION (every N cycles, ~5s)                                 │
│    Merge dirty filter diffs into bases to prevent unbounded growth  │
├─────────────────────────────────────────────────────────────────────┤
│ 7. PUBLISH SNAPSHOT                                                 │
│    inner.store(Arc::new(staging.clone()))                           │
│    → Arc-per-bitmap CoW: only clones bitmaps with refcount > 1     │
│    → readers instantly see new state via ArcSwap::load()            │
└─────────────────────────────────────────────────────────────────────┘
```

### Periodic Maintenance (runs even at idle, outside the mutation block)

```
┌─────────────────────────────────────────────────────────────────────┐
│ TIME BUCKET REBUILD (periodic, time-based)                          │
│                                                                     │
│ Each bucket has a refresh_interval (e.g., 24h=5min, 30d=1hr).     │
│ When refresh_due() fires:                                           │
│                                                                     │
│ a) Brief lock: check which buckets are due, get their durations    │
│ b) Release lock, heavy work:                                        │
│    → Single pass over ALL alive slots (105M)                        │
│    → For each slot: reconstruct_value from sort field (32 bitmap    │
│      bit-reads), check against each bucket's cutoff                 │
│    → Build all due bucket bitmaps simultaneously                    │
│    → Takes ~104s at 105M scale                                      │
│ c) Brief lock: swap in new bitmaps (instant)                        │
│ d) Invalidate trie cache for the filter field                       │
│                                                                     │
│ Lock-free pattern ensures queries are NEVER blocked during the     │
│ 104s rebuild — they just see stale bucket bitmaps until swap.      │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Observations for Unification

1. **Three separate locks**: `cache` (trie), `flush_bound_cache` (bounds), `flush_time_buckets` (buckets). Each held briefly but independently.

2. **Two invalidation strategies**: Trie cache uses generation counters (lazy, checked on read). Bound cache uses rebuild flags (checked on read, rebuilt on next query). Time buckets use periodic full rebuild.

3. **Eq vs non-Eq split in trie cache**: Only Eq entries get live updates. Everything else (In, NotEq, range, compound) falls back to generation invalidation. This creates a class divide — Eq entries stay fresh, others go stale until next query.

4. **Bound maintenance is sort-aware but filter-blind**: When a sort value changes, the flush thread adds the slot to qualifying bounds regardless of whether the slot matches the bound's filter. This is correct because the bound is ANDed with the filter bitmap at query time. But it means bounds accumulate slots that don't match their filter — contributing to bloat.

5. **Time bucket invalidation kills trie cache entries**: After a periodic rebuild, `cache.invalidate_field(field_name)` bumps the generation for the bucket's filter field. This invalidates ALL cached filter results that include a time bucket clause, even though the bucket bitmap is pre-computed and snapped. A unified cache could avoid this by updating the cached BucketBitmap in-place.

6. **No coordination between bound invalidation and trie invalidation**: When a filter field changes, the trie cache entry is invalidated AND the bound is marked for rebuild. But they're handled by different code blocks with different locks. In a unified system, one invalidation event could update both.

---

## The Query Flow Today

```
Query: {nsfwLevel=1, Gte(sortAtUnix, now-30d)}, sort=reactionCount DESC, limit=40

Step 1: ensure_fields_loaded()
   → Load nsfwLevel filter + reactionCount sort + sortAt sort (if pending)

Step 2: resolve_filters()
   → Snap Gte to BucketBitmap("30d")                          [time bucket]
   → Canonicalize: {nsfwLevel:eq:1, sortAtUnix:bucket_gte:30d} [stable key]
   → Trie cache lookup (exact/prefix/miss)                     [trie cache]
   → Result: 424K matching IDs

Step 3: apply_bound()
   → Look up bound for {nsfwLevel=1} + reactionCount DESC      [bound cache]
   → Found: global top 10K by reactionCount
   → Narrow: 424K ∩ 10K = ~80 IDs                              ← PROBLEM
   → Return narrowed bitmap

Step 4: execute_from_bitmap()
   → Sort ~80 IDs by reactionCount DESC, return top 40

Step 5: update_bound_from_results()
   → 40 results < target 10K → seed with full traversal

Step 6: post_validate()
   → Check in-flight writes
```

---

## The Problem

The bound cache stores the **globally** top-K documents by sort field. When intersected with a restrictive time filter, the overlap is tiny:

```
Global top 10K by reactionCount:  IDs from all time (many old, viral images)
30-day bucket:                     ~424K recent IDs
Intersection:                      ~80 IDs (most viral images are old)
```

By page 3 of 40-item pages, the intersection is exhausted → 0 results despite 424K matches.

**Current workaround** (commit `9b19b15`): If `execute_from_bitmap` returns 0 results with a cursor and a bound was applied, retry with the full filter bitmap. This works but:
- Pages 1-2 use the fast bound path (~1ms)
- Pages 3+ fall back to un-bounded sort traversal (~10ms at 424K, much worse at 53M for 1y)
- No caching benefit for deep pagination on time-filtered queries

---

## The Insight: These Should Be One System

The trie cache answers: "Which IDs match these filters?" → exact bitmap
The bound cache answers: "What are the top-K of those IDs by sort?" → approximate bitmap

These are stages of the same pipeline. A **unified cache** would store:

```
Key:   (canonical_filters, sort_field, direction)
Value: {
  filter_bitmap: Arc<RoaringBitmap>,     // exact — what trie cache does today
  bounds: [                               // approximate — what bound cache does today
    { tier: 0, bitmap: RoaringBitmap, min_tracked: u32 },
    { tier: 1, bitmap: RoaringBitmap, min_tracked: u32 },
  ]
}
```

Benefits:
1. **Bounds would be per-filter-combination**, not global. The bound for `{nsfwLevel=1, 30d}` would contain the top 10K *within the 30d window*, not the global top 10K
2. **No intersection collapse** — bounds already pre-filtered, no tiny overlap problem
3. **Single invalidation path** — when a filter field changes, invalidate both the filter bitmap and its associated bounds in one operation
4. **Prefix matching extends to bounds** — a cached bound for `{nsfwLevel=1}` sorted by reactionCount could seed the bound for `{nsfwLevel=1, 30d}` sorted by reactionCount (the 30d subset is within the broader result)

---

## Key Design Questions

### 1. Cardinality Explosion

Today: trie cache has ~10K filter entries. Bound cache has ~100 bound entries. These are independent.

Unified: each filter entry could have bounds for every (sort_field, direction) combination. With 5 sort fields × 2 directions = 10 possible bounds per filter entry. At 10K filter entries, that's 100K potential bounds.

**Mitigation:** Bounds are only formed on demand (when a sort query actually runs against that filter). Most filter entries will never have bounds. The practical count would likely be similar to today (~100 active bounds).

### 2. Bound Formation Cost

Per-filter bounds require a sort traversal of the filter result, not the global result. For a 424K filter result, forming a top-10K bound requires traversing 424K candidates through 32 sort bit layers. This is fast (~10ms) compared to the 105M global traversal.

**For small filter results** (e.g., userId=42 → 500 docs): bound is unnecessary. The sort traversal on 500 docs is already sub-millisecond. Detection: if `filter_bitmap.len() < bound_target_size`, skip bound entirely.

### 3. Live Maintenance Complexity

Today's bound maintenance is simple: check if a new slot's sort value qualifies for the bound. With per-filter bounds, we'd also need to check if the slot passes the filter.

**Option A**: Maintain bounds lazily — mark for rebuild on any mutation, rebuild on next query. Simple but slower for first page after writes.

**Option B**: Check filter membership during flush. The flush thread already has the staging snapshot and knows which slots changed. For each changed slot, check which filters it appears in (via meta-index), then update associated bounds.

**Option C**: Hybrid — live maintenance for Eq-based filter entries (fast membership check), lazy rebuild for compound/range entries.

### 4. Cache Key Design

Current trie cache key: `Vec<CanonicalClause>` (just filters)
Current bound key: `(CacheKey, sort_field, direction, tier)`

Unified key options:
- **Nested**: Filter key → filter bitmap, then (sort, direction) → bound. Preserves prefix matching for filters.
- **Flat**: `(filter_key, sort_field, direction)` → { filter_bitmap, bounds[] }. Simpler but loses trie prefix benefits.
- **Trie with bound leaves**: Keep the trie for filters, but leaf nodes optionally carry bounds for different sort fields. Trie structure is preserved, bounds are associated.

### 5. Time Bucket Integration

Time bucket bitmaps are already pre-computed and snapped into filter clauses. In a unified cache, the `BucketBitmap("30d")` clause is just another filter clause in the key. The per-filter bound would automatically be "top-K within 30d" because the filter bitmap already incorporates the time constraint.

No special integration needed — the architecture naturally solves the time bucket pagination problem.

### 6. Superset Matching for Bounds

Today's superset matching: bound for `{nsfwLevel=1}` can narrow `{nsfwLevel=1, onSite=true}`.

In unified architecture: the trie's prefix matching already handles this. A prefix hit for `{nsfwLevel=1}` provides the filter bitmap. If that node also has a bound for `reactionCount DESC`, it can narrow the prefix result before computing the remaining clause.

**Tradeoff**: The bound from `{nsfwLevel=1}` is broader than ideal for `{nsfwLevel=1, onSite=true}`. The intersection `bound(nsfwLevel=1) ∩ filter(nsfwLevel=1 AND onSite=true)` might still be much larger than needed. But it's strictly better than no bound.

---

## Incremental Migration Path

Rather than a full rewrite, this could be done incrementally:

### Phase 1: Filter-Aware Bounds (Quick Win)
- When forming a bound in `update_bound_from_results()`, seed it from the **filter bitmap** (already available), not the global set
- Bound key already includes `filter_key` — just ensure the bound bitmap is formed by traversing only the filter result, not all alive slots
- This might already be happening for the initial formation but not for live maintenance seeding

### Phase 2: Co-locate Bounds with Filter Cache Entries
- Add an optional `bounds: HashMap<(String, SortDirection), BoundEntry>` to `CacheEntry` in the trie cache
- When a trie cache hit occurs AND a sort is requested, check for an associated bound
- Form the bound on first miss, from the cached filter bitmap

### Phase 3: Unified Invalidation
- When a filter field generation changes, invalidate both the filter entry and its associated bounds
- When a sort field changes, update bounds via the existing meta-index but scoped to the filter entry
- Remove the separate `BoundCacheManager` — bounds live inside the trie

### Phase 4: Trie Prefix Bounds
- When a prefix hit provides a partial filter result, check if the prefix node has a bound
- Use the broader bound as a starting point, intersect with the refined filter result
- Avoids forming a new bound from scratch for every filter combination

---

## Performance Expectations

| Scenario | Today | Unified |
|----------|-------|---------|
| Page 1, no time filter | ~1ms (bound hit) | ~1ms (same) |
| Page 1, 30d filter | ~12ms (bound hit, small intersection) | ~1ms (per-filter bound) |
| Page 3+, 30d filter | ~10ms (retry without bound) | ~1ms (per-filter bound still valid) |
| Page 1, 1y filter (53M) | ~20ms (bound helps more here) | ~1-5ms (per-filter bound) |
| Deep pagination, 1y | 15-27s (no bound, 53M traversal) | ~5-10ms (tiered per-filter bound) |
| First query (cold) | ~10ms (form bound + compute filter) | ~10ms (same, form both) |
| Memory | Separate structures, ~180B meta-index | Co-located, potentially less overhead |

The big wins are on time-filtered + sorted queries, which are the **most common real-world pattern** (Civitai's main feed is always time-filtered).

---

## Open Questions for Discussion

1. **Should bounds always be per-filter, or should we keep a global bound as a fallback?** Global bounds are useful when the filter result is very large (e.g., `nsfwLevel=1` alone is 60M docs — a per-filter bound of 10K in 60M is similar to the global bound).

2. **How does this interact with the per-value lazy loading for tagIds?** If a query touches tagIds, the filter bitmap might be stale until the value loads. Should bound formation wait?

3. **Should the trie cache store bounds at every node (prefix + exact), or only at leaf nodes?** Storing at prefix nodes enables broader bound reuse but increases memory.

4. **Is there a simpler fix?** Instead of unifying the caches, could we just form bounds from the filter result (Phase 1 above) and call it done? That solves the immediate pagination bug without architectural change.
