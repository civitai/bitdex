# Generational Cache Invalidation

## Problem

The unified cache holds pre-computed query results (filtered + sorted bitmaps) keyed by filter clauses + sort field + direction. When underlying data changes, affected cache entries must be invalidated.

Currently, the time bucket refresh (every 300s for the 24h bucket) triggers `maintain_bucket_changes()`, which **iterates all cache entries** under the cache Mutex to find and update entries affected by the bucket change. At 213K+ entries, this O(n) iteration holds the Mutex for seconds, blocking all query cache lookups.

### Measured Impact

- Time bucket refresh every 5 minutes causes a full server lockup
- QPS drops to zero during the lockup
- p99 query latency spikes to 6 seconds
- Prometheus scrapes gap out (can't reach /metrics)
- The lockup has two phases:
  1. **107M slot scan** (seconds of CPU rebuilding the bucket bitmap)
  2. **O(n) cache iteration** under Mutex (seconds iterating 213K+ entries for invalidation)

### Current Invalidation Paths

The unified cache is invalidated through several paths today:

| Path | Trigger | Cost | Frequency |
|------|---------|------|-----------|
| `maintain_filter_changes()` | Filter field mutations (flush thread) | O(affected entries) via meta-index | Every flush cycle with filter changes |
| `maintain_sort_changes()` | Sort field mutations (flush thread) | O(entries × changed slots) | Every flush cycle with sort changes |
| `maintain_bucket_changes()` | Time bucket refresh | **O(all entries)** | Every 300s (24h bucket) |
| `evict_to_bytes()` | Cache exceeds max_bytes | O(n) LRU scan | When cache is full |

The meta-index (bitmaps tracking which cache entries reference each (field, value) pair) makes filter invalidation targeted. But time bucket invalidation bypasses the meta-index and scans all entries.

## Proposed Solution: Generational Tagging

Replace O(n) iteration-based invalidation with O(1) generation counter checks.

### Core Idea

Each invalidation source (time buckets, filter fields, sort fields) maintains a **generation counter** (`AtomicU64`). Each cache entry is tagged with the generation it was computed against. On cache lookup, compare the entry's generation against the current generation — stale entries are treated as cache misses.

### Data Structure Changes

#### TimeBucketManager

```rust
pub struct TimeBucketManager {
    // ... existing fields ...
    generation: AtomicU64,  // bumped on every bucket refresh
}

impl TimeBucketManager {
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }
}
```

#### CacheEntry (in UnifiedCache)

```rust
pub struct CacheEntry {
    // ... existing fields (bitmap, last_used, etc.) ...
    tb_generation: u64,  // time bucket generation when this entry was created
}
```

#### UnifiedCache

```rust
pub struct UnifiedCache {
    // ... existing fields ...
    tb_generation: AtomicU64,  // current time bucket generation (mirrored from TimeBucketManager)
}
```

### Operations

#### Cache Lookup (query path)

```rust
fn get(&self, key: &UnifiedKey, current_tb_gen: u64) -> Option<&CacheEntry> {
    match self.entries.get(key) {
        Some(entry) if entry.tb_generation >= current_tb_gen => {
            entry.last_used = now();
            Some(entry)
        }
        Some(_stale) => None,  // generation mismatch = cache miss
        None => None,
    }
}
```

The caller reads `tb_generation` from the TimeBucketManager (via the lock it already holds) and passes it to the cache lookup. No additional lock acquisition needed.

#### Cache Insert (query path)

```rust
fn store(&mut self, key: UnifiedKey, entry: CacheEntry, current_tb_gen: u64) {
    let mut entry = entry;
    entry.tb_generation = current_tb_gen;
    self.entries.insert(key, entry);
}
```

New entries are tagged with the current generation so they're immediately valid.

#### Time Bucket Refresh (flush thread)

```rust
// In the flush thread, after rebuilding bucket bitmaps:
tb_manager.bump_generation();
// That's it. No maintain_bucket_changes() call needed.
// Stale entries will be detected on next lookup.
```

**O(1)** instead of O(n). The flush thread no longer needs to acquire the cache Mutex for time bucket invalidation at all.

#### Eviction (LRU sweep)

Stale entries (wrong generation) can be preferentially evicted during LRU sweeps — they're known-useless and can be removed without the "was this entry recently used?" check. This is an optimization, not a requirement.

### Query Path Integration

The query path currently:
1. Acquires `tb_guard = time_buckets.lock()`
2. Acquires `cache.lock()` for lookup
3. Executes query if cache miss
4. Acquires `cache.lock()` for store

With generational tagging:
1. Acquires `tb_guard = time_buckets.lock()`
2. Reads `let tb_gen = tb_guard.generation()` (one atomic read, or field on the guard)
3. Drops tb_guard
4. Acquires `cache.lock()` for lookup, passes `tb_gen`
5. Cache compares `entry.tb_generation` against `tb_gen`
6. On store, tags entry with `tb_gen`

The generation read is essentially free (atomic load). No behavior change for the caller beyond passing the generation value through.

### Extending to Other Invalidation Paths

The same pattern applies to filter and sort invalidation:

| Source | Counter | When bumped | Entry field |
|--------|---------|-------------|-------------|
| Time buckets | `tb_generation` | On bucket refresh (every 300s) | `tb_generation` |
| Filter mutations | `filter_generation` | On flush with filter changes | `filter_generation` |
| Sort mutations | `sort_generation` | On flush with sort changes | `sort_generation` |

A cache entry is valid only if ALL its generation fields match or exceed the current counters. This eliminates `maintain_filter_changes()` and `maintain_sort_changes()` iteration as well.

However, filter/sort invalidation is more targeted today (via meta-index) and only affects entries that reference the changed field/value. A global generation counter for filters would over-invalidate — a change to `userId=123` would invalidate entries for `nsfwLevel=1` too.

**Recommendation**: Start with time bucket generation only (the proven bottleneck). Evaluate extending to filter/sort after measuring the impact. The meta-index approach for filter invalidation is already O(affected entries), not O(all entries), so the benefit of generational tagging there is smaller.

### Memory Impact

- +8 bytes per cache entry (`tb_generation: u64`)
- +8 bytes on TimeBucketManager (`generation: AtomicU64`)
- +8 bytes on UnifiedCache (`tb_generation: AtomicU64`)
- At 213K entries: ~1.7 MB additional memory (negligible)

### Stale Entry Cleanup

Stale entries (generation < current) remain in the HashMap until evicted by LRU or max_bytes pressure. This is acceptable because:
- They occupy memory but don't serve incorrect results (checked on lookup)
- LRU eviction naturally reclaims them (stale entries have old `last_used` timestamps)
- Optional: the LRU sweep can prioritize stale entries for eviction

### Risks

1. **Over-invalidation**: All time-bucket-dependent entries become stale simultaneously on refresh, causing a burst of cache misses. This already happens today — the generational approach doesn't change the miss pattern, just eliminates the O(n) iteration cost of invalidation.

2. **Stale memory pressure**: Stale entries stay in memory until evicted. With 213K entries at ~1.2 KB average, that's ~256 MB of potentially stale entries after a refresh. The LRU eviction sweep will clean these up naturally as new entries are inserted.

3. **Generation overflow**: `u64` overflows after 2^64 increments. At one increment per 300 seconds, overflow would take 1.7 × 10^11 years. Not a concern.

## Implementation Plan

1. Add `generation: AtomicU64` to `TimeBucketManager`, `bump_generation()` and `generation()` methods
2. Add `tb_generation: u64` to cache entry struct, default 0
3. Modify cache lookup to accept `current_tb_gen` and check entry staleness
4. Modify cache store to tag entries with current generation
5. In flush thread: replace `maintain_bucket_changes()` call with `bump_generation()`
6. Wire `tb_generation` through the query path (read from tb lock, pass to cache ops)
7. Tests: verify stale entries return cache miss, verify generation bump invalidates entries
