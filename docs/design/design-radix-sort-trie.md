---
status: IMPLEMENTED (Phase 1), PROPOSED (Phases 2-3 — adaptive splitting, leaf sorted vecs)
created: 2026-03-11
updated: 2026-03-13
---

# Radix Sort Trie for Unified Cache

## Status

**Phase 1 (8-bit radix bucketing)** — Fully implemented in `src/radix_sort.rs` with integration in unified cache and executor. 12 unit tests + 8 benchmark groups.
**Phases 2-3 (adaptive splitting, leaf sorted vecs)** — Proposed. Microbenchmarks required before proceeding.

## Context

The unified cache stores bounded top-K result bitmaps per (filter combo, sort field, direction). Each entry also maintains a pre-sorted `Vec<u64>` of packed `(sort_value << 32 | slot_id)` keys for O(log n + limit) binary search pagination on cache hits.

**Current fast path (sorted keys):**
- Binary search for cursor position → take N items → ~55ns for limit=20
- Maintained at all capacities (4K initial, 64K expanded)
- Maintenance cost per flush mutation: binary search O(log n) + `memmove` O(n)
- Already eliminates the 2-5ms 32-layer bitmap traversal on cache hits

**The problem: sorted vecs don't scale past initial capacity.**

Sorted vecs are optimal for small entries (≤4K items) but become a liability after expansion to 64K:

| Entry size | Vec memory | Avg memmove/mutation | Cache behavior |
|---|---|---|---|
| 4,000 (initial) | 31 KB | 16 KB (fits L1 cache) | Fast — binary search + small shift |
| 8,000 | 62 KB | 32 KB (exceeds L1) | Borderline — starting to hurt |
| 16,000 | 125 KB | 64 KB (L2 cache) | Slow — flush thread feels it |
| 64,000 (expanded) | 500 KB | 250 KB (L3 cache) | Very slow — dominates flush cycle |

At scale, the memory cost is the real killer:

| Cache entries at 64K | Vec memory | Radix memory (est) |
|---|---|---|
| 1,000 | 500 MB (hits budget) | ~14 MB |
| 5,000 | 2.5 GB | ~70 MB |
| 10,000 | 5.0 GB | ~140 MB |

The current 512 MB cache budget can only hold ~1,000 expanded entries before eviction kicks in. Radix would allow 30-50x more entries at the same budget.

---

## Threshold Strategy: Vec Below, Radix Above

**The key decision: use sorted vecs for entries at initial capacity (≤4K items), switch to radix on expansion (>4K items).**

This is a natural boundary — it already exists in the code. When `expand()` is called, the entry jumps from `initial_capacity` (4K) to `max_capacity` (64K). That's exactly where the sorted vec cost explodes and radix becomes worth the overhead.

### Why 4K is the right threshold

**Below 4K (sorted vec wins):**
- 31 KB memory — trivial
- 16 KB avg memmove — fits in L1 cache, completes in ~1μs
- Binary search is optimal: O(log 4000) = 12 comparisons → ~55ns
- Radix overhead (256 bucket pointers + cumulative arrays = 2 KB minimum) isn't worth it for 4K items — most buckets would hold only ~16 items, so the radix structure is all overhead

**Above 4K (radix wins):**
- Radix bucket insert/remove: O(1) bitmap op, no memmove
- Memory: 256 buckets × ~(n/256 items each) ≈ same bitmap memory but NO 500 KB vec
- Deep pagination: cumulative rank array enables O(1) offset skip instead of binary search + take
- Maintenance under write load: each flush mutation touches ONE bucket (~250 items at 64K) instead of shifting a 500 KB vec

### How it works in practice

```
Cache entry lifecycle:
  1. Formation (cache miss → slow path traversal)
     → sorted_slots.len() ≤ initial_capacity (4K)
     → Build sorted_keys Vec<u64>
     → radix = None

  2. Pagination triggers expansion (cursor past boundary)
     → expand() called with new slots up to max_capacity (64K)
     → Drop sorted_keys (set to None)
     → Build RadixSortIndex from full bitmap
     → radix = Some(Arc<RadixSortIndex>)

  3. Subsequent queries
     → If sorted_keys.is_some() → binary search path (fast, small entry)
     → If radix.is_some() → radix bucket path (fast, large entry)
     → If neither → bitmap traversal fallback (should not happen normally)

  4. Flush thread maintenance
     → If sorted_keys: binary search + insert/remove (memmove ≤ 16 KB)
     → If radix: bucket insert/remove (O(1) bitmap op)
```

### Why NOT a different threshold

- **1K**: Too low. 1K sorted vec = 8 KB, memmove = 4 KB. Vec is clearly faster here. Radix overhead (2 KB minimum structure) is 25% of the data.
- **8K**: Viable but loses the natural boundary. Would require a new config parameter and a mid-lifecycle transition (not during expand). Complexity for marginal gain.
- **16K+**: Too high. Letting vecs grow to 16K means 125 KB per entry and 64 KB memmoves per mutation. The flush thread will struggle under write load.
- **4K (initial_capacity)**: Perfect. It's already a boundary in the code. `expand()` is the natural transition point. No new config needed — the threshold IS the initial capacity.

---

## Current Architecture (What Exists Today)

Before implementing radix, understand what's already in place:

### UnifiedEntry (src/unified_cache.rs)

```rust
pub struct UnifiedEntry {
    bitmap: Arc<RoaringBitmap>,          // bounded top-K bitmap
    min_tracked_value: u32,              // sort floor/ceiling
    capacity: usize,                     // 4K initial, 64K expanded
    max_capacity: usize,
    has_more: bool,
    total_matched: u64,
    needs_rebuild: bool,
    rebuilding: AtomicBool,
    last_used: Instant,
    meta_id: CacheEntryId,
    sorted_keys: Option<Vec<u64>>,       // ← packed (sort_value << 32 | slot_id)
    direction: SortDirection,
}
```

### Fast Path (src/concurrent_engine.rs, execute_query)

1. Cache lookup via `unified_cache.lock().lookup(&ukey)`
2. If hit + sorted_keys available → `executor.execute_from_sorted_keys()` (binary search)
3. If hit + no sorted_keys → `executor.execute_from_bitmap()` (32-layer traversal)
4. If cursor past boundary → expansion path (fetch more slots, rebuild sorted keys)
5. If miss → slow path (compute filters, traverse, form cache entry)

### Maintenance (flush thread)

- `add_slot(slot, sort_value)`: Binary search insert into sorted_keys + bitmap insert
- `remove_slot(slot, sort_value)`: Binary search remove from sorted_keys + bitmap remove
- `remove_slot_blind(slot)`: Linear scan of sorted_keys (slow, used when sort value unknown)
- `expand()`: Rebuilds sorted_keys from full bitmap after expansion
- `rebuild()`: Full reconstruction from fresh traversal

### Cache Config (src/unified_cache.rs)

```rust
pub struct UnifiedCacheConfig {
    pub max_entries: usize,       // 100_000 (safety cap)
    pub max_bytes: usize,         // 512 MB (primary eviction trigger)
    pub initial_capacity: usize,  // 4_000
    pub max_capacity: usize,      // 64_000
    pub min_filter_size: usize,   // 0 (cache everything)
}
```

---

## Implementation: 3 Phases

### Phase 1: Static 8-Bit Radix Buckets + Cumulative Rank

**New file: `src/radix_sort.rs`**

```rust
pub struct RadixSortIndex {
    /// 256 buckets indexed by top 8 bits of sort value.
    /// None = empty bucket (zero allocation).
    buckets: [Option<RoaringBitmap>; 256],
    /// Cumulative slot counts for O(1) deep pagination offset skipping.
    /// cumulative_desc[i] = total slots in buckets 255..=i (for DESC iteration).
    /// cumulative_asc[i] = total slots in buckets 0..=i (for ASC iteration).
    cumulative_desc: [u32; 256],
    cumulative_asc: [u32; 256],
    /// Dirty flag: set on insert/remove, cleared on cumulative rebuild.
    counts_dirty: bool,
    prefix_bits: u8,  // 8
}
```

Key methods:
- `from_slots(slots: &[u32], value_fn: impl Fn(u32) -> u32) -> Self` — Formation from sorted slots
- `insert(&mut self, slot: u32, sort_value: u32)` — Bucket insert + mark dirty
- `remove(&mut self, slot: u32, sort_value: u32)` — Bucket remove + mark dirty
- `rebuild_counts(&mut self)` — Recompute cumulative arrays
- `offset_to_bucket(&self, offset: usize, direction: SortDirection) -> (u8, usize)` — O(1) skip
- `iter_buckets(direction) -> impl Iterator<Item = (u8, &RoaringBitmap)>` — Ordered iteration
- `memory_bytes(&self) -> usize`

**Changes to `src/unified_cache.rs`:**
- Add `radix: Option<Arc<RadixSortIndex>>` to `UnifiedEntry` (Arc for zero-cost clone out of cache lock)
- `UnifiedEntry::new()` — NO radix at formation time (entry is at initial_capacity, sorted_keys is optimal)
- `expand()` — **This is the transition point.** Build radix from the full bitmap, drop sorted_keys:
  ```rust
  pub fn expand(&mut self, new_slots: &[u32], value_fn: impl Fn(u32) -> u32) -> usize {
      // ... existing bitmap insert + capacity jump ...

      // Transition: drop sorted vec, build radix
      self.sorted_keys = None;
      self.radix = Some(Arc::new(RadixSortIndex::from_bitmap(&self.bitmap, &value_fn)));

      self.max_capacity
  }
  ```
- `add_slot(slot, sort_value)` — If radix present: `Arc::make_mut(&mut radix).insert(slot, sort_value)`. If sorted_keys present: binary search insert (existing code).
- `remove_slot(slot, sort_value)` — Same pattern: radix OR sorted_keys.
- `remove_slot_blind(slot)` — If radix: mark dirty for rebuild (can't remove without sort value). If sorted_keys: linear scan (existing code).
- `rebuild()` — If capacity >= max_capacity: build radix, no sorted_keys. If capacity < max_capacity: build sorted_keys, no radix.
- `memory_bytes()` — Include radix memory when present.

**Changes to `src/executor.rs`:**

New method:
```rust
pub fn execute_from_radix(
    &self,
    radix: &RadixSortIndex,
    sort_clause: &SortClause,
    limit: usize,
    offset: usize,
    cursor: Option<&CursorPosition>,
) -> Result<QueryResult>
```

Logic:
1. If offset > 0 and cumulative counts clean: `offset_to_bucket()` to skip directly to target bucket
2. Iterate buckets in sort order (DESC: 255→0, ASC: 0→255)
3. For each bucket: if cursor prefix > bucket prefix, skip (already past). If equal, pass cursor. If less, no cursor filtering needed.
4. Call `sort_field.top_n(&bucket_bitmap, remaining_limit, descending, cursor)` on the tiny bitmap
5. Collect results until limit reached. Produce final cursor.

**Changes to `src/concurrent_engine.rs`:**

Fast path (execute_query, lines ~1260-1384):
- Extract `radix: Option<Arc<RadixSortIndex>>` alongside bitmap/sorted_keys from cache entry
- Priority: sorted_keys first (small entries), then radix (expanded entries), then bitmap fallback
- Pass offset directly to radix path instead of fetch-and-drop

```rust
// Fast path dispatch:
if let Some(ref sorted_keys) = cached_sorted_keys {
    // Small entry (≤4K) — binary search
    result = executor.execute_from_sorted_keys(sorted_keys, ...)?;
} else if let Some(ref radix) = cached_radix {
    // Expanded entry (>4K) — radix bucket traversal
    result = executor.execute_from_radix(radix, ...)?;
} else {
    // Fallback — full bitmap traversal (shouldn't happen normally)
    result = executor.execute_from_bitmap(...)?;
}
```

**Changes to `src/lib.rs`:** Add `pub mod radix_sort;`

**Maintenance callers** (`maintain_sort_changes`, `maintain_filter_changes`):
- Both already have `sort_value` available via `sorts.get_field(name).reconstruct_value(slot)`
- Check which structure the entry has (sorted_keys or radix) and maintain accordingly
- For `remove_slot_blind` with radix: mark radix dirty, trigger rebuild on next access

---

### Phase 2: Adaptive Splitting

When a bucket exceeds 2048 items (e.g., timestamp sort where most values share top 8 bits), recursively split on next 8 bits:

```rust
enum RadixNode {
    Leaf(RoaringBitmap),
    Split {
        children: Box<[Option<RadixNode>; 256]>,
        total_count: u32,
    },
}
```

- Split triggered during maintenance when bucket exceeds threshold
- Split needs `SortField` access to reconstruct values — done by flush thread which has `&SortIndex`
- Guarantees no traversal processes > 2048 candidates regardless of data distribution

**Why this matters:** The `sortAt` field stores unix timestamps. At 105M records, most timestamps share the same top 8 bits (all within a ~2 year window), so 90%+ of items land in 1-2 buckets. Without adaptive splitting, the radix provides no benefit for timestamp sorts. With it, each bucket is capped at 2048 items.

---

### Phase 3: Leaf-Level Sorted Vecs

For buckets < 64 items, store a pre-sorted `Vec<u32>` alongside the bitmap:

```rust
enum RadixNode {
    Leaf(RoaringBitmap),
    SortedLeaf {
        bitmap: RoaringBitmap,
        sorted_slots: Vec<u32>,  // pre-sorted by sort value
    },
    Split { ... },
}
```

- Eliminates remaining bit-layer traversal for tiny buckets
- Binary search for cursor position, then take N items
- Memory: 64 * 4 = 256 bytes/leaf. Negligible.

**Note:** This stores a sorted Vec, which may appear to conflict with design principle #3 ("No sorted data structures"). However, this is a cache-local optimization on leaf nodes < 64 items — the bitmap remains the authoritative index. The existing `sorted_keys` Vec on UnifiedEntry already sets this precedent.

---

## Microbenchmark Validation Strategy

**IMPORTANT: Benchmarks must pass before any production code is written.**

Create `benches/radix_sort_bench.rs` (add `[[bench]]` entry to Cargo.toml).

### 5 Benchmark Groups

| Group | What it validates | Go/No-Go threshold |
|-------|------------------|-------------------|
| **1. Formation** | Cost to bucket 64K slots by prefix | < 5ms (must be less than traversal it replaces) |
| **2. Bucket traversal vs full traversal** | 32-layer on 64K vs 32-layer on ~250 items | > 5x speedup |
| **3. Deep Pagination** | Cumulative rank skip at offset=4000 vs fetch+drop | > 5x speedup |
| **4. Adaptive Splitting** | Clustered timestamps: 16-bit radix vs flat | > 2x speedup |
| **5. Live Maintenance** | Insert/remove 1000 slots into radix vs sorted vec at 64K | < 2x overhead (ideally faster) |

**Data generation:** Synthetic `SortField` with 3 distributions:
- **Uniform u32** (models reactionCount — best case for 8-bit radix)
- **Clustered timestamps** (models sortAt — adversarial for static radix, tests Phase 2)
- **Skewed** (90% in one bucket — tests adaptive splitting)

### Go/No-Go Decision Matrix

| If this fails... | Then... |
|---|---|
| Groups 1-2 fail | Radix optimization is not viable — abort entirely |
| Group 3 fails | Deep pagination still works via cursor, cumulative counts are optional |
| Group 4 fails | Restrict radix to uniform-distribution fields only (reactionCount, commentCount, collectedCount) |
| Group 5 fails | Need slot-to-prefix side map for removes (small fixed memory cost) |

---

## Files Modified

| File | Change |
|------|--------|
| `src/radix_sort.rs` | **NEW** — RadixSortIndex struct, bucketing, cumulative counts, iteration |
| `src/unified_cache.rs` | Add `radix` field to UnifiedEntry, transition in `expand()`, update maintenance |
| `src/executor.rs` | New `execute_from_radix()` method |
| `src/concurrent_engine.rs` | Fast path dispatches sorted_keys → radix → bitmap fallback |
| `src/lib.rs` | Register `radix_sort` module |
| `src/metrics.rs` | Add radix memory gauge |
| `benches/radix_sort_bench.rs` | **NEW** — 5 criterion benchmark groups |
| `Cargo.toml` | Add `[[bench]]` entry for radix_sort_bench, add `criterion` dev-dependency |

---

## Implementation Sequence

1. **Microbenchmarks first** — Create `benches/radix_sort_bench.rs` with all 5 groups. Run against synthetic data. Validate go/no-go thresholds before touching production code.

2. **Phase 1 core** — Create `src/radix_sort.rs` with `RadixSortIndex`, comprehensive unit tests. Test with all 3 data distributions.

3. **Phase 1 integration** — Wire into UnifiedEntry (transition in `expand()`), executor, concurrent_engine. Small entries keep sorted_keys, expanded entries use radix.

4. **Phase 1 validation** — `cargo test --release` (all existing + new tests pass), E2E tests pass (`node tests/e2e/e2e-unified-cache.mjs`), loadtest comparison on 105M dataset.

5. **Phase 2** — Adaptive splitting (only if Group 4 benchmarks confirm value for clustered data). Critical for sortAt field.

6. **Phase 3** — Leaf sorted vecs (only if Group 2 benchmarks show remaining bottleneck in small-bucket traversal).

---

## Verification Checklist

1. `cargo test --release` — All existing + new unit tests pass
2. `cargo bench --bench radix_sort_bench` — All 5 groups meet go/no-go thresholds
3. Start server, load data, run `node tests/e2e/e2e-unified-cache.mjs` — All test groups pass
4. Loadtest comparison: `target/release/loadtest.exe --mode http --url http://localhost:3001 --workload tests/loadtest/workload.json --concurrency 1,4,8,16,32,64 --duration 10`
5. Compare cache-hit latency: before vs after (target: measurable improvement at high concurrency)
6. Memory: radix entries at 64K should use < 130 KB each (vs 500 KB for sorted vec)
7. Flush thread mutation cost: radix insert/remove should be faster than sorted vec memmove at 64K

---

## Measured Baselines (for comparison)

As of 2026-03-12, with sorted keys optimization and size-based cache eviction:

| Concurrency | QPS (HTTP) | p50 | p95 | Cache hit rate |
|---|---|---|---|---|
| 1 | 1,582 | 0.15ms | 1.88ms | 72% (100% for non-empty results) |
| 4 | 3,557 | 0.21ms | 4.42ms | — |
| 16 | 3,460 | 1.02ms | 16.07ms | — |
| 64 | 3,413 | 14.81ms | 48.05ms | — |

Cache: 2,137 entries, 45 MB, 512 MB budget, 105M records.

**Note:** Most entries in this test are at initial capacity (4K) because the loadtest doesn't paginate deeply. The radix benefit will be most visible when entries expand to 64K under real pagination traffic — that's when the 500 KB sorted vecs become a problem for both memory and flush thread performance.
