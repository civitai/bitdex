# The Performance Saga — March 13, 2026

A single debugging session took BitDex query latency from **56 seconds to 1.6 milliseconds** on cold cache miss. Here's what happened, layer by layer.

## Starting point

A compound filter query with time bucket and sort — the exact pattern Civitai's shadow mode sends — took **56 seconds** on first load. Subsequent queries hit cache at 22-30 microseconds. The question: why is the first query so catastrophically slow?

## The onion

Each fix peeled back a layer, revealing the next bottleneck underneath.

### Layer 1: Time bucket bitmap clone (1ms per query)

**Problem:** `snap_range_clauses()` did `Arc::new(bucket.bitmap().clone())` on every query, cloning a 20M-bit roaring bitmap even on cache hits where the bitmap was never used.

**Fix:** Arc-wrap `TimeBucket.bitmap`. Snap becomes `Arc::clone()` — a refcount bump at 3ns instead of a 1-3ms memcpy.

**Impact:** Cache hits dropped from ~1.3ms to ~30μs.

### Layer 2: Cache key used raw timestamps (cache miss every query)

**Problem:** The cache key was built from raw query filters before time bucket snapping. Each query had a slightly different `sortAtUnix >= 1772835302` timestamp, producing a unique cache key. Zero cache hits, 100% misses.

**Fix:** Move `snap_range_clauses()` upstream of cache key construction. Cache key now uses stable bucket name `"7d"` instead of moving timestamps.

**Impact:** Queries within the same bucket window share one cache entry. Cache hit rate went from 0% to ~100% for repeated patterns.

### Layer 3: Always-snap to nearest bucket

**Problem:** Queries with timestamps between bucket boundaries (e.g. "last 3 days" between 24h and 7d buckets) returned zero results because snapping failed and the fallback produced an empty bitmap.

**Fix:** When tolerance-based snapping fails, snap to the smallest bucket that covers the requested duration.

**Impact:** No more silent zero-result queries for inter-bucket ranges.

### Layer 4: Cursor tiebreaker O(n) loop (492ms)

**Problem:** `apply_cursor_filter()` iterated every slot with the same sort value as the cursor for the slot-ID tiebreaker. For `collectedCount` where millions of records have value 0, this was O(millions) per page.

**Fix:** Replace per-slot iteration with `RoaringBitmap::remove_range()` — O(containers) instead of O(bits).

**Impact:** Cursor pagination on zero-heavy sort fields: 492ms -> 3.6ms (136x faster).

### Layer 5: Double sort traversal on cache miss

**Problem:** On cold cache miss, two sort traversals ran on the same candidate bitmap: once for the user's 50 results, then again for the 4K cache seed. Both traversed 11-26M candidates through 32 bit layers.

**Fix:** Seed the cache first with one 4K traversal, then serve the user's results from the cached sorted_keys via binary search (~55ns).

**Impact:** Cold miss cut roughly in half.

### Layer 6: Planner estimated 0 cardinality for string filters (5,198ms -> 4.7ms)

**The big one.** The planner's `use_simple_sort` flag was `true` for 26M-candidate result sets.

**Root cause:** `value_to_bitmap_key()` only handled `Bool` and `Integer`. `String` values (like `In(type, ["image"])`) returned `None`, making the planner estimate 0 cardinality. Since 0 < 1000 threshold, it chose `simple_sort_and_paginate` — which reconstructs sort values for ALL 26M candidates into a Vec, then sorts the Vec in memory. O(n log n) on 26M entries.

The correct path — bitmap bifurcation — walks 32 bit layers doing AND operations and takes 4.7ms regardless of candidate count.

**Fix:** Give the planner access to string maps and dictionaries so it can resolve `String` values to bitmap keys for accurate cardinality estimation.

**Impact:** Sort seed: 5,198ms -> 4.7ms (1,100x faster). Total cold miss: 13.2s -> 105ms.

### Layer 7: Reference-AND with distributed In (18x on sparse)

**Problem:** `compute_filters` materialized each clause's full bitmap independently (e.g. 99M bits for `isPublished`), then ANDed with the accumulator. For `In` clauses, it built the full union (28M bits) before ANDing.

**Fix:** Two changes:
1. For Eq/In after the first clause, AND the accumulator directly with borrowed bitmap references via `fused_cow()` — no clone.
2. For In clauses, distribute AND over OR: `(acc & val1) | (acc & val2)` instead of `union(val1, val2) & acc`. When the accumulator is small, all intermediates stay small.

**Impact:** Sparse queries (userId=5418, 1549 slots): resolve_filters 18.4ms -> 1.0ms (18x). Broad queries: 25ms -> 19ms.

## Final numbers

| Metric | Before (session start) | After |
|--------|----------------------|-------|
| First query (cold, compound + sort) | **56,700ms** | **27ms** (broad) / **1.6ms** (sparse) |
| Cache hit | ~1,300μs | **22μs** |
| Sort seed on 26M candidates | 5,198ms | 4.7ms |
| Filter computation (3 clauses, 26M) | 54ms | 19ms |
| Cursor pagination at val=0 | 492ms | 3.6ms |

**Total improvement: 2,100x on broad cold miss. 35,000x on sparse cold miss.**

## Lessons

1. **Profile before optimizing.** We spent time on non-zero bitmaps and radix bucketing for sort fields before discovering the real bottleneck was `use_simple_sort=true`.

2. **The planner matters more than the algorithm.** Bifurcation was always 1,100x faster than simple sort. The algorithm was fine. The planner chose wrong because it couldn't estimate string filter cardinalities.

3. **Add timing instrumentation early.** The `tracing::debug` per-clause timing immediately revealed `simple=true` on 26M candidates — a one-line log that would have saved hours of investigation.

4. **Microbenchmarks can mislead.** The microbench showed bifurcation at 10ms, matching our expectations. But the server took 5.2s because it was using the wrong sort algorithm entirely. Always verify assumptions against the actual running system.

5. **Distribute AND over OR** when the accumulator is small. `(acc & A) | (acc & B)` keeps all intermediates proportional to the accumulator, not the source bitmaps.
