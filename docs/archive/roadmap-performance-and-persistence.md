# Bitdex V2 — Roadmap: Performance & Persistence

> **Status: COMPLETE (historical)** — All phases (Prereq through E) are implemented. This document references redb which was replaced by custom BitmapFs + DocStore. Kept as historical reference for the phase structure and design rationale. See `docs/design/` for current architecture.

_Derived from benchmark analysis (104.6M records) and design conversations (2026-02-19/20). Incorporates architecture risk review findings._

---

## The Problem

At 100M+ documents, unfiltered sort queries hit 20ms+ because dense sort-layer bitmaps (~12MB each, 32 layers) exceed L3 cache. Memory sits at 7GB for bitmaps alone (12GB RSS). Both numbers grow linearly with dataset size. The current architecture has no mechanism to bound the working set before sort traversal, and no way to keep cold bitmaps off-heap.

## The Goal

Decouple query performance from dataset size. Every query — even unfiltered sorts — should operate on a small, bounded working set. Memory should reflect active usage, not total dataset size. The system should self-tune from query traffic with zero manual configuration.

---

## Reference Documents

- [`docs/design/design-arcswap-redb-reconciliation.md`](../design/design-arcswap-redb-reconciliation.md) — Full design for the diff-based ArcSwap + two-tier redb model
- [`docs/reviews/architecture-risk-review.md`](../reviews/architecture-risk-review.md) — Reviewed risks and resolutions for the concurrency/storage model
- [`docs/benchmarks/benchmark-report.md`](../benchmarks/benchmark-report.md) — 5M to 104.6M scaling analysis

---

## Prerequisite: Diff-Based ArcSwap + Two-Thread Model

**Status:** In progress (`contention-bench` worktree has ArcSwap; diff layer and thread split are next)
**Impact:** Foundational — all roadmap phases build on this concurrency and mutation model.

### What Exists (contention-bench worktree)

The `contention-bench` branch replaces `RwLock<InnerEngine>` with `ArcSwap<InnerEngine>`. Readers call `inner.load()` for a lock-free snapshot. The flush thread works on a private staging copy and publishes atomically via `inner.store()`. Bitmaps use `Arc<RoaringBitmap>` with `Arc::make_mut()` for copy-on-write.

### What Changes

Three things replace the CoW model:

1. **VersionedBitmap with diff layer** — replaces `Arc::make_mut()` cloning
2. **Two-thread model** — flush and merge on separate threads
3. **Arc-wrapped diffs** — eliminates deep clone on publish

### Data Structures

```rust
struct VersionedBitmap {
    base: Arc<RoaringBitmap>,       // immutable, shared with snapshots
    diff: Arc<BitmapDiff>,          // shared via Arc, swapped on publish
    generation: u64,
}

struct BitmapDiff {
    sets: RoaringBitmap,            // bits to add
    clears: RoaringBitmap,          // bits to remove
}
```

### Two-Thread Model

From the architecture risk review (issues 3 + 4): flush and merge must be separate threads. The flush thread handles the fast path. The merge thread handles the slow path. Neither blocks the other.

```
Flush Thread (every 100ms, never blocks):
  1. Drain mutation channel, group ops
  2. Apply sort layer mutations directly to bases (~160 bitmaps, trivial)
  3. Apply filter mutations to working diffs, wrap in new Arc
  4. Publish snapshot via ArcSwap::store()

Merge Thread (every 5s or diff threshold):
  1. Snapshot current filter diffs
  2. Compact filter diffs into bases (in-place when Arc::strong_count == 1, clone otherwise)
  3. Write merged bases to redb in batch transaction
  4. Publish merged snapshot with empty filter diffs
```

Sort layer diffs never reach the read path — the flush thread merges them inline before publishing. This avoids a 3x multiplier on sort traversal (32 layers × 3 ops = 96 ops becomes 32 × 1 = 32).

### Work Items

| # | Item | Files | Notes |
|---|------|-------|-------|
| P1 | Implement `VersionedBitmap` and `BitmapDiff` structs | New: `src/versioned_bitmap.rs` | Base `Arc<RoaringBitmap>`, diff `Arc<BitmapDiff>`, generation counter. Include `apply_diff()`, `is_dirty()`, `merge()` methods. |
| P2 | Convert `FilterField` to use `VersionedBitmap` | `src/filter.rs` | Replace `HashMap<u64, Arc<RoaringBitmap>>` with `HashMap<u64, VersionedBitmap>`. Mutations write to diff instead of `Arc::make_mut()`. |
| P3 | Convert `SortField` to use `VersionedBitmap` | `src/sort.rs` | Replace `Vec<Arc<RoaringBitmap>>` bit layers with `Vec<VersionedBitmap>`. |
| P4 | Convert `SlotAllocator` alive bitmap to `VersionedBitmap` | `src/slot.rs` | Alive bitmap is Tier 1, always merged eagerly like sort layers. |
| P5 | Implement diff fusion in executor | `src/executor.rs` | `compute_filters()` and `evaluate_clause()` must fuse diffs into intersections: `(base & filters) \| (sets & filters) &! clears`. Only needed for filter bitmaps — sort layers have no active diffs. |
| P6 | Split flush thread into flush + merge threads | `src/concurrent_engine.rs` | Flush thread: drain channel → merge sort diffs into bases → accumulate filter diffs in Arc → publish. Merge thread: compact filter diffs → write redb → publish merged. Coordinate via shared state, not locks on the hot path. |
| P7 | Update write coalescer for two-thread model | `src/write_coalescer.rs` | `MutationOp` variants stay the same. The coalescer feeds the flush thread. Sort ops get applied directly to bases. Filter ops get applied to diffs. |
| P8 | Remove Phase 2 config placeholders | `src/config.rs` | Remove `snapshot_interval_secs` and `wal_flush_strategy` fields entirely. |
| P9 | Tests: VersionedBitmap unit tests | `src/versioned_bitmap.rs` | Diff accumulation, merge, generation tracking, `strong_count` in-place mutation. Property tests for diff correctness (apply diff to base = expected bitmap). |
| P10 | Tests: Two-thread flush/merge integration | `src/concurrent_engine.rs` | Concurrent reads during merge, sort layers always clean, filter diffs accumulate correctly, publish ordering. |

**Acceptance:** All existing tests pass. Benchmark suite shows no sort regression (sort layers always merged). Write throughput equal or better than CoW model (no full bitmap clones).

---

## Phase A: Bitmap Persistence Layer

**Status:** Not started
**Depends on:** Prerequisite (VersionedBitmap + two-thread model)
**Impact:** Memory drops from 7-11GB to ~3-4GB. Eliminates need for WAL, snapshots, and sidecar builder.

### What Changes

Today all bitmaps live in memory. This phase makes redb the source of truth for all bitmap state and splits memory into two tiers.

### Two-Tier Architecture

**Tier 1 — ArcSwap snapshot (always in memory, ~1.5GB):**
- Alive bitmap, all sort layers, low-cardinality filters (nsfwLevel, type, booleans)
- Time range bucket bitmaps (Phase C), cache/bound/meta-index bitmaps (Phases D/E)

**Tier 2 — moka concurrent cache over redb (loaded on demand, ~0.5-1GB budget):**
- High-cardinality filters: `tagIds` (~500k values), `userId` (~millions)
- Configurable per field via `storage: snapshot` vs `storage: cached`

Both tiers use `VersionedBitmap` with the diff layer. Mutations accumulate in diffs. Merge thread writes merged bases to redb.

### Flush/Read/Merge Cycle (Updated)

```
Flush Thread (every 100ms, never blocks):
  1. Drain mutation channel, group ops
  2. Sort layer ops → merge directly into bases (fast, ~160 bitmaps)
  3. Tier 1 filter ops → apply to working diffs, wrap in Arc
  4. Tier 2 filter ops → if loaded in moka, apply to diff; if cold, buffer as pending
  5. Publish snapshot via ArcSwap::store()

Merge Thread (every 5s or diff threshold):
  1. Snapshot current Tier 1 filter diffs
  2. Compact filter diffs into bases (in-place when Arc::strong_count == 1)
  3. Compact dirty Tier 2 diffs in moka (same logic)
  4. Drain pending cold Tier 2 mutations (CAPPED — max N per cycle, prioritized by mutation count)
  5. Batch write all merged bases to redb in one transaction
  6. Publish merged snapshot with empty filter diffs
  7. Report pending buffer depth metric

Read:
  1. Tier 1 → lock-free from ArcSwap snapshot, filter diffs fused into intersection
  2. Tier 2 → moka::get_with() (coalesces concurrent misses), miss → load from redb + apply pending → insert
```

### Risk Mitigations (from architecture review)

- **Thundering herd on Tier 2 misses:** Use `moka::get_with()` / `try_get_with()` to coalesce concurrent loads of the same cold bitmap. One deserialization, shared result.
- **Pending drain storm:** Cap pending drain to ~100 bitmaps per merge cycle. Prioritize by mutation count. Track pending buffer depth as a metric — unbounded growth means the cap needs tuning.
- **Eviction before merge:** If moka evicts a bitmap with a non-empty diff, flush the small diff to redb (load base → apply diff → write back). The diff is KB, so this is fast even under eviction pressure.

### Config

```yaml
storage:
  engine: redb
  bitmap_path: "./data/bitmaps.redb"
  flush_interval_ms: 100
  merge_interval_ms: 5000
  merge_diff_threshold: 10000
  tier2_cache_size: 1GB
  pending_drain_cap: 100

filter_fields:
  - name: tagIds
    field_type: multi_value
    storage: cached              # Tier 2
  - name: nsfwLevel
    field_type: single_value
    storage: snapshot            # Tier 1 (default)
```

### Startup

Open redb → load Tier 1 bases (~1.5GB, seconds) → publish initial snapshot → start serving. Tier 2 bitmaps warm on demand via moka. No WAL replay, no snapshot loading.

### Crash Recovery

redb provides transactional durability. On restart, bitmap state is consistent as of the last merge. The Postgres WAL (Phase 5) replays events since the last merge position.

### Work Items

| # | Item | Files | Notes |
|---|------|-------|-------|
| A1 | Add `storage` field to `FilterFieldConfig` | `src/config.rs` | Enum: `Snapshot` (default) / `Cached`. Validation: sort fields always Snapshot. |
| A2 | Add storage config section | `src/config.rs` | `bitmap_path`, `merge_interval_ms`, `merge_diff_threshold`, `tier2_cache_size`, `pending_drain_cap`. Remove `snapshot_interval_secs`, `wal_flush_strategy` if not already removed in prereq. |
| A3 | Bitmap serialization to/from redb | `src/docstore.rs` or new `src/bitmap_store.rs` | redb table for bitmaps keyed by `(field_name, value)`. Serialize/deserialize `RoaringBitmap` via `serialize_into`/`deserialize_from`. Batch write support. |
| A4 | moka Tier 2 cache integration | New: `src/tier2_cache.rs` | Wraps `moka::sync::Cache` (or `future::Cache` if async). Key = `(field, value)`. Use `get_with()` for coalesced loads. Eviction listener flushes dirty diffs to redb. |
| A5 | Pending mutation buffer | `src/tier2_cache.rs` or `src/concurrent_engine.rs` | Buffer cold Tier 2 mutations as `(field, value, Vec<MutationOp>)`. Apply on read (moka miss → load + apply pending) or on merge (capped drain). Track buffer depth metric. |
| A6 | Tier-aware flush thread | `src/concurrent_engine.rs` | Flush must distinguish Tier 1 ops (apply to snapshot diffs) from Tier 2 ops (apply to moka entry or buffer as pending). |
| A7 | Tier-aware merge thread | `src/concurrent_engine.rs` | Merge compacts Tier 1 diffs + dirty moka entries + capped pending drain. Batch writes to redb. |
| A8 | Startup: load Tier 1 from redb | `src/concurrent_engine.rs` | On startup, read all Tier 1 bitmaps from redb into `InnerEngine`. Publish initial snapshot. Tier 2 warms on demand. |
| A9 | Executor: Tier 2 bitmap resolution | `src/executor.rs` | When evaluating a filter clause for a Tier 2 field, resolve bitmap via moka (with `get_with()`) instead of snapshot lookup. |
| A10 | Tests: Tier 2 cache hit/miss/eviction | Integration tests | Concurrent reads for same cold bitmap (thundering herd). Eviction with dirty diff. Pending mutation correctness. |
| A11 | Tests: Startup from redb | Integration tests | Write bitmaps → restart → verify state. Crash recovery (kill mid-merge → restart → verify consistency). |
| A12 | Benchmark: Memory reduction validation | `src/bin/benchmark.rs` | Run 5M benchmark, verify bitmap memory ~1.5-2GB (down from 328MB all-in-memory is already low, but at 100M should show ~3-4GB vs current 6.5GB). |

**Acceptance:** tagIds and userId bitmaps load on demand from redb. Memory at 100M records drops to ~3-4GB RSS. All existing tests and benchmarks pass. Pending buffer depth metric is observable.

---

## Phase B: Sort-by-Slot Optimization

**Status:** Partially exists
**Depends on:** Nothing (independent, can parallelize with prereq or Phase A)
**Impact:** Queries with no explicit sort field become instant at any scale.

### What Already Exists

The executor already skips sort layer traversal when no sort is specified (`executor.rs:74-82`). It iterates the filter result bitmap in ascending slot order and produces a basic cursor with `sort_value: 0`.

### Work Items

| # | Item | Files | Notes |
|---|------|-------|-------|
| B1 | Reverse iteration (newest-first) | `src/executor.rs` | Default no-sort to descending (newest-first by slot). Use roaring's reverse iterator or `rank`/`select` for efficient descending slot traversal. Add sort direction parameter to no-sort path. |
| B2 | Cursor pagination for slot iteration | `src/executor.rs` | Replace `sort_value: 0` placeholder. Descending: "50 slots below slot X". Ascending: "50 slots above slot X". Use roaring range iteration (`range(..cursor_slot)`). |
| B3 | Planner awareness | `src/planner.rs` | Planner should flag no-sort queries explicitly in `QueryPlan` to skip all sort-related computation. Add `skip_sort: bool` to `QueryPlan`. |
| B4 | Tests: Slot-order pagination | `src/executor.rs` | Forward and reverse pagination, cursor correctness, empty pages, single result. |

**Acceptance:** No-sort queries return newest-first by default. Cursor pagination works in both directions. No sort layer bitmaps touched for these queries.

---

## Phase C: Time Handling

**Status:** Not started
**Depends on:** Nothing (independent, but should land before Phase D)
**Impact:** Eliminates cache-fragmenting date filters. Enables scheduled publishing with zero query-time cost.

### Problem

Two patterns fragment cache keys:

1. **Scheduled publishing** — Every query includes `publishedAt < now` to hide future-dated content. "now" changes every second → unique cache key per second → throwaway entries.
2. **Relative date ranges** — "Last 7 days" uses `publishedAt > now - 604800` → unique timestamp per second → same problem.

### Work Items

| # | Item | Files | Notes |
|---|------|-------|-------|
| C1 | Deferred alive tracking | `src/slot.rs`, `src/concurrent_engine.rs` | On insert, if a `deferred_alive` field has a future value, store `(slot, scheduled_time)` in a pending set. Don't set alive bit. Background timer checks pending set every N seconds, activates arrived documents. |
| C2 | Range bucket bitmaps | New: `src/time_buckets.rs` | Per configured bucket (24h, 7d, 30d, etc.), maintain a bitmap of qualifying slots. Background timer rebuilds each bucket on its refresh interval. Buckets are Tier 1 (always in memory). Persist to redb. |
| C3 | Query parser bucket snapping | `src/parser.rs`, `src/executor.rs` | When a range filter on a bucketed timestamp field arrives, find the closest matching bucket and substitute. If no bucket matches, fall back to range evaluation on sort layers. |
| C4 | Config schema extension | `src/config.rs` | Add `behaviors` to field config with `deferred_alive: bool` and `range_buckets: Vec<BucketConfig>`. Each bucket: `name`, `duration`, `refresh_interval`. |
| C5 | Cache key stabilization | `src/cache.rs` | Range filters snapped to buckets produce stable cache keys using the bucket name (e.g., `"7d"`) instead of raw timestamps. Verify trie cache keys are stable across seconds. |
| C6 | Tests: Deferred alive lifecycle | Integration tests | Insert future doc → verify not in results → advance time → verify appears. Multiple docs with different scheduled times. |
| C7 | Tests: Bucket snapping | `src/time_buckets.rs` | Bucket boundary accuracy, refresh cycle correctness, query parser snapping to nearest bucket, fallback for unmatched ranges. |

### Config

```yaml
fields:
  - name: publishedAt
    field_type: timestamp
    behaviors:
      deferred_alive: true
      range_buckets:
        - name: 24h
          duration: 86400
          refresh_interval: 300
        - name: 7d
          duration: 604800
          refresh_interval: 3600
        - name: 30d
          duration: 2592000
          refresh_interval: 3600
        - name: 90d
          duration: 7776000
          refresh_interval: 7200
        - name: 1y
          duration: 31536000
          refresh_interval: 14400
```

**Acceptance:** `publishedAt < now` filter eliminated from queries. Cache keys stable across seconds for bucketed ranges. Future-dated content invisible until scheduled time.

---

## Phase D: Bound Caches (Sort Working Set Reduction)

**Status:** Not started
**Depends on:** Phase A (redb-backed storage), Phase C (stable cache keys from buckets)
**Impact:** All sort queries operate on bounded working sets (~10k candidates), making query time independent of dataset size. This is the core fix for the L3 cache problem.

### Concept

A **bound cache** is a bitmap representing the approximate top-K documents for a given sort field + direction + filter combination. It shrinks the working set before sort traversal, exactly like a tag filter would.

```
Without bound:  alive (100M) ──► sort traversal on 100M ──► 20ms+
With bound:     alive (100M) AND bound_top10k (10k) ──► sort on 10k ──► <1ms
```

### Work Items

| # | Item | Files | Notes |
|---|------|-------|-------|
| D1 | Bound cache data structure | New: `src/bound_cache.rs` | Struct: bitmap + sort field + direction + filter definition + min tracked value + cardinality. Uses `VersionedBitmap` for live maintenance via diff layer. |
| D2 | Bound formation (promotion threshold = 0) | `src/executor.rs`, `src/bound_cache.rs` | Every unique filter+sort query immediately caches its sort result as a bound bitmap after first execution. |
| D3 | Live maintenance on writes | `src/concurrent_engine.rs`, `src/bound_cache.rs` | On sort field mutation: check if doc passes each relevant bound's filter definition, compare value against bound's min, set bit if it exceeds. Bits are never removed — bloat control handles cleanup. |
| D4 | Bloat control | `src/bound_cache.rs` | Track cardinality. When `> bound_max_size`, flag for rebuild. Next query triggers fresh sort traversal to produce tight bound at `bound_target_size`. |
| D5 | Executor integration | `src/executor.rs` | Before sort traversal, check for matching bound. If found, AND the bound bitmap with the filter result to shrink the working set. |
| D6 | Tiered bounds for deep pagination | `src/bound_cache.rs` | When cursor value drops below bound's minimum, fall back to full traversal or spawn a new tiered bound. Tiered bounds LRU-evicted based on usage. |
| D7 | Invalidation model change | `src/cache.rs`, `src/concurrent_engine.rs` | Shift from generation-counter lazy invalidation to live cache maintenance. Writes directly update relevant cache entries via diff layer. Generation counters become unnecessary for live-maintained caches. |
| D8 | Tests: Bound formation and usage | `src/bound_cache.rs` | Bound forms on first query, subsequent queries use it, sort result matches full traversal. |
| D9 | Tests: Live maintenance correctness | Integration tests | Insert doc that qualifies for bound → verify bit set. Bloat control trigger → verify rebuild. |
| D10 | Benchmark: Sort query latency | `src/bin/benchmark.rs` | Verify sort queries drop from 20ms+ to <1ms at 100M records with bound caches active. |

### Config

```yaml
cache:
  max_memory: 2GB
  filter_promotion_threshold: 10
  bound_promotion_threshold: 0
  bound_target_size: 10000
  bound_max_size: 20000
  eviction: lru
```

**Acceptance:** Sort queries at 100M records complete in <1ms with active bounds. Bound caches form automatically from first query. Live maintenance keeps bounds current without full rebuilds. Bloat control prevents unbounded growth.

---

## Phase E: Meta-Index (Bitmaps Indexing Bitmaps)

**Status:** Not started
**Depends on:** Phase D (requires bound caches to exist)
**Impact:** Write-path cache maintenance and query-time cache lookup become constant-cost regardless of how many caches exist.

### Problem

With hundreds or thousands of active cache entries (filter + bound), every write checks each cache for relevance. Every query searches all caches for a match. Both scale linearly with cache count.

### Solution

Assign each cache entry an integer ID. For each discrete filter value that appears in any cache definition, maintain a tiny bitmap of cache entry IDs that use that value.

```
Meta-bitmap "tag=anime":     {3, 17, 42, 89}     ← cache entries requiring tag=anime
Meta-bitmap "nsfwLevel=1":   {3, 7, 17, 201}     ← cache entries requiring nsfwLevel=1
Meta-bitmap "sort=reactions": {3, 42, 89, 201}    ← cache entries sorting by reactions
```

### Work Items

| # | Item | Files | Notes |
|---|------|-------|-------|
| E1 | Cache entry ID allocation | `src/cache.rs` or new `src/meta_index.rs` | Sequential IDs for cache entries. Recycle IDs on eviction. |
| E2 | Meta-bitmap registry | `src/meta_index.rs` | For each discrete value in any cache definition, maintain a `RoaringBitmap` of cache entry IDs. Update on cache create/evict. |
| E3 | Write-path integration | `src/concurrent_engine.rs` | Replace linear cache scan with meta-bitmap intersection to find relevant caches for a mutation. |
| E4 | Planner integration | `src/planner.rs` | Replace linear cache search with meta-bitmap intersection to find matching bound/filter caches for a query. |
| E5 | Sort field meta-bitmaps | `src/meta_index.rs` | Track which caches sort by which field+direction. Include in query-time intersection. |
| E6 | Tests: Meta-index correctness | `src/meta_index.rs` | Intersection finds correct caches. Eviction updates meta-bitmaps. Sort field tracking. |

**Acceptance:** Write-path cache maintenance is O(1) vs cache count. Query planner cache lookup is O(1) vs cache count. Correct cache entries identified via tiny bitmap intersections.

---

## Summary

| Phase | Core Change | Performance Effect | Memory Effect |
|---|---|---|---|
| **Prereq** | VersionedBitmap + two-thread model | Eliminates CoW cloning, sort layers always clean | Negligible diff overhead vs MB of copies |
| **A** | Two-tier redb storage + moka cache | Instant startup, eliminates WAL/snapshots | 7-11GB → ~3-4GB |
| **B** | Sort-by-slot (reverse + cursor) | No-sort queries instant at any scale | — |
| **C** | Deferred alive + time buckets | Eliminates cache-fragmenting date filters | Modest (bucket bitmaps) |
| **D** | Bound caches | Sort queries bounded to ~10k working set | Controlled by cache budget |
| **E** | Meta-index | Write + planning cost constant vs cache count | Negligible (tiny bitmaps) |

### Net Result

After all phases, every query — regardless of dataset size, filter combination, or sort field — operates on a small bounded working set. Memory reflects what's actively queried, not the full dataset. Caches form automatically from traffic, maintain themselves live from writes via the diff layer, and die from disuse. No manual tuning. Sub-millisecond at 150M, 500M, or a billion documents.

---

## Dependency Graph

```
Prereq (Diff+ArcSwap+Two-Thread) ──► Phase A (Persistence) ──┐
                                                               ├──► Phase D (Bounds) ──► Phase E (Meta-Index)
                                     Phase C (Time) ──────────┘
                                     Phase B (Sort-by-Slot, independent, anytime)
```

### Parallelization Opportunities

- **Phase B** is fully independent — can be built by a separate agent at any time.
- **Phase C** is independent of A but should complete before D begins (bound caches need stable cache keys).
- **Prereq** must complete before A starts. All other phases flow from there.
- Within the prereq, work items P1-P4 (data structures) can parallelize, then P5-P7 (integration) build on them, then P8-P10 (cleanup + tests) finalize.
- Within Phase A, items A1-A2 (config) and A3-A4 (storage/cache infra) can parallelize, then A5-A9 (integration) build on them.
