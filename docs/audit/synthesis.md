# Bitdex V2 -- Spec Compliance Audit Synthesis

**Date:** 2026-02-21
**Auditor:** Claude Opus 4.6
**Scope:** All roadmap phases (Prereq P1-P10, Phase A A1-A12, Phase B B1-B4, Phase C C1-C7, Phase D D1-D10, Phase E E1-E6)
**Spec source:** `docs/plans/roadmap-performance-and-persistence.md`

---

## 1. Executive Summary

- **Data structures are solid; integration is the gap.** Every phase has well-implemented, well-tested primitives (VersionedBitmap, TimeBucketManager, BoundCacheManager, MetaIndex, Tier2Cache, PendingBuffer). The recurring failure is that these primitives are not wired into `ConcurrentEngine` and the production read/write paths.

- **The diff-based write path -- the foundational architectural innovation -- is not functioning as designed.** Filter diffs are eagerly merged by the flush thread, the executor never fuses diffs on the read path, and the merge thread does not compact Tier 1 filter diffs. This defeats the performance model that every subsequent phase depends on.

- **Persistence is incomplete: Tier 1 bitmaps are never written to redb.** The merge thread only drains Tier 2 pending buffer mutations. Sort layers, alive bitmap, and Tier 1 filter bitmaps are never persisted. Process restart loses all in-memory state.

- **Phases C, D, and E have production-ready components that are dead code.** Time bucket snapping, deferred alive, bound cache live maintenance filter checks, and meta-index superset matching are all implemented and unit-tested but never invoked by the engine.

- **Phase B is the most complete phase**, with only minor gaps (ascending direction, edge-case tests, dead `skip_sort` flag).

---

## 2. Compliance Scorecard

### Prerequisite Phase: Diff-Based ArcSwap + Two-Thread Model

| Item | Description | Status | Note |
|------|-------------|--------|------|
| P1 | VersionedBitmap and BitmapDiff structs | COMPLIANT | All fields and methods match spec |
| P2 | FilterField uses VersionedBitmap | COMPLIANT | Mutations write to diff layer |
| P3 | SortField uses VersionedBitmap | COMPLIANT | Bit layers use VersionedBitmap |
| P4 | SlotAllocator alive uses VersionedBitmap | COMPLIANT | Eagerly merged per spec |
| P5 | Diff fusion in executor | NON-COMPLIANT | `apply_diff()` exists but is never called; executor reads merged bases only |
| P6 | Split flush/merge threads | PARTIAL | Both threads exist but merge thread does not compact Tier 1 filter diffs |
| P7 | Write coalescer two-thread model | NON-COMPLIANT | Filter diffs eagerly merged by flush thread, contradicting spec |
| P8 | Remove Phase 2 config placeholders | COMPLIANT | Clean removal confirmed |
| P9 | VersionedBitmap unit tests | COMPLIANT | 15 tests; minor gap: no proptest |
| P10 | Two-thread integration tests | PARTIAL | Basic tests only; missing filter-diff-accumulation and merge-compaction tests |

### Phase A: Bitmap Persistence Layer

| Item | Description | Status | Note |
|------|-------------|--------|------|
| A1 | `storage` field on FilterFieldConfig | COMPLIANT | Snapshot/Cached enum, correct defaults |
| A2 | Storage config section | COMPLIANT | All fields present with correct defaults |
| A3 | Bitmap serialization to/from redb | COMPLIANT | Full BitmapStore with batch write, load_single, load_all_fields |
| A4 | moka Tier 2 cache integration | PARTIAL | No eviction listener for dirty diffs; pending buffer compensates |
| A5 | Pending mutation buffer | COMPLIANT | PendingBuffer with conflict resolution; depth metrics exist but are unexposed |
| A6 | Tier-aware flush thread | COMPLIANT | Correctly routes Tier 2 mutations to pending buffer |
| A7 | Tier-aware merge thread | PARTIAL | Only drains Tier 2 pending buffer; no Tier 1 diff compaction or persistence |
| A8 | Startup: load Tier 1 from redb | COMPLIANT | Code correct but effectively a no-op (A7 never writes Tier 1) |
| A9 | Executor: Tier 2 bitmap resolution | COMPLIANT | Eq and In filters fall through to moka+redb+pending |
| A10 | Tests: Tier 2 cache hit/miss/eviction | PARTIAL | Unit tests only; no ConcurrentEngine integration test |
| A11 | Tests: Startup from redb | PARTIAL | BitmapStore-level only; no engine restart test |
| A12 | Benchmark: Memory reduction | MISSING | No benchmark for two-tier memory savings |

### Phase B: Sort-by-Slot Optimization

| Item | Description | Status | Note |
|------|-------------|--------|------|
| B1 | Reverse iteration (newest-first) | COMPLIANT | Descending default works; no ascending direction parameter |
| B2 | Cursor pagination for slot iteration | COMPLIANT | `remove_range` cursor narrowing; `sort_value: 0` placeholder remains |
| B3 | Planner awareness | PARTIAL | `skip_sort` field exists but executor never reads it (dead code) |
| B4 | Tests: Slot-order pagination | PARTIAL | Descending + cursor tested; missing ascending, empty page, single result |

### Phase C: Time Handling

| Item | Description | Status | Note |
|------|-------------|--------|------|
| C1 | Deferred alive tracking | PARTIAL | SlotAllocator primitives work; no write-path integration or background timer |
| C2 | Range bucket bitmaps | PARTIAL | TimeBucketManager implemented; not instantiated in ConcurrentEngine |
| C3 | Query parser bucket snapping | PARTIAL | Executor + query.rs paths implemented; neither wired into ConcurrentEngine |
| C4 | Config schema extension | COMPLIANT | FieldBehaviors with deferred_alive and range_buckets, full validation |
| C5 | Cache key stabilization | COMPLIANT | Stable bucket-name keys; code correct but unreachable in production |
| C6 | Tests: Deferred alive lifecycle | PARTIAL | Unit tests for SlotAllocator; no engine-level integration test |
| C7 | Tests: Bucket snapping | PARTIAL | Thorough unit tests; no engine-level or refresh-cycle integration test |

### Phase D: Bound Caches

| Item | Description | Status | Note |
|------|-------------|--------|------|
| D1 | Bound cache data structure | PARTIAL | Uses plain RoaringBitmap, not VersionedBitmap |
| D2 | Bound formation (threshold=0) | COMPLIANT | Immediate formation on first query |
| D3 | Live maintenance on writes | PARTIAL | Sort value check works; filter definition check missing |
| D4 | Bloat control | COMPLIANT | Cardinality tracking, needs_rebuild, rebuild on query |
| D5 | Executor integration | COMPLIANT | AND with bound bitmap before sort traversal |
| D6 | Tiered bounds for deep pagination | COMPLIANT | Tier escalation + LRU method exists (not auto-triggered) |
| D7 | Invalidation model change | NON-COMPLIANT | Generation counters still active; not replaced by live maintenance |
| D8 | Tests: Bound formation and usage | COMPLIANT | Unit + integration coverage |
| D9 | Tests: Live maintenance correctness | COMPLIANT | Data structure tests cover all cases |
| D10 | Benchmark: Sort query latency | COMPLIANT | Cold/warm comparison infrastructure present |

### Phase E: Meta-Index

| Item | Description | Status | Note |
|------|-------------|--------|------|
| E1 | Cache entry ID allocation | COMPLIANT | Sequential + LIFO free list recycling |
| E2 | Meta-bitmap registry | COMPLIANT | Three tiers: clause, field, sort |
| E3 | Write-path integration | PARTIAL | Sort field lookup uses meta-index; alive path is linear (unavoidable) |
| E4 | Planner integration | NON-COMPLIANT | `find_matching_entries()` exists but is never called; HashMap lookup used instead |
| E5 | Sort field meta-bitmaps | COMPLIANT | Maintained and used on write path |
| E6 | Tests: Meta-index correctness | PARTIAL | Unit tests thorough; no integration test for query-path superset matching |

### Aggregate

| Status | Count | Percentage |
|--------|------:|------------|
| COMPLIANT | 27 | 55% |
| PARTIAL | 16 | 33% |
| NON-COMPLIANT | 4 | 8% |
| MISSING | 1 | 2% |
| **Total** | **48** | |

---

## 3. Critical Issues

These break correctness, lose data, or prevent core architecture from functioning.

### 3.1 Tier 1 bitmaps never persisted to redb (A7)

**Impact: Data loss on restart.**

The merge thread only drains Tier 2 pending buffer mutations to redb. Tier 1 filter bitmaps, sort layer bitmaps, and the alive bitmap are never written to redb. The startup code (A8) correctly loads from redb, but there is nothing to load.

**Consequence:** Process restart loses all in-memory index state. The system cannot restart without a full re-index from the Postgres WAL (Phase 5, not yet built). This defeats the "eliminates need for WAL, snapshots, and sidecar builder" claim.

**Files:** `src/concurrent_engine.rs` (merge thread, lines 434-486)

### 3.2 Diff-based write path not functioning (P5 + P7)

**Impact: Performance regression; foundational architecture defeated.**

The flush thread eagerly merges ALL bitmaps (filter + sort + alive) before publishing. The executor never sees dirty filter diffs and never calls `apply_diff()`. The `VersionedBitmap::apply_diff()` method is dead code in the production read path.

This means every flush cycle calls `Arc::make_mut()` on filter bitmaps. When readers hold snapshots with `strong_count > 1`, this clones entire base bitmaps -- exactly the O(N) CoW model the spec was designed to eliminate.

**Consequence:** The system operates in "CoW mode" with the additional overhead of maintaining diff layers that are immediately merged. All downstream phases (A, D, E) that depend on filter diffs accumulating between merge cycles are affected.

**Files:** `src/write_coalescer.rs` (lines 308-315), `src/executor.rs` (evaluate_clause), `src/filter.rs` (get method)

### 3.3 Merge thread does not compact Tier 1 filter diffs (P6)

**Impact: Architectural commitment broken.**

The spec's two-thread split was designed to keep the flush thread fast (~sub-ms) by offloading expensive merge work. The flush thread currently does ALL merge work inline. Under heavy read+write load, this generates MB of transient bitmap copies via `Arc::make_mut()` clone cascades on shared bitmaps.

**Consequence:** Combined with 3.2, the system has no mechanism for deferred filter diff compaction. The merge thread's only current role is Tier 2 pending buffer drain.

**Files:** `src/concurrent_engine.rs` (flush thread lines 177-432, merge thread lines 434-486)

---

## 4. High Priority Issues

These significantly degrade performance or leave major features unwired.

### 4.1 Phase C integration gap: Time handling is dead code

All three Phase C components (deferred alive, time buckets, bucket snapping) are implemented and unit-tested but never wired into `ConcurrentEngine`:

- **C1:** No background timer calls `activate_due()`. Write path does not check `deferred_alive` config. Future-dated documents are immediately visible.
- **C2:** No `TimeBucketManager` instantiated in engine. No background refresh. No redb persistence for buckets.
- **C3:** `QueryExecutor::with_time_buckets()` never called. All range filters on timestamp fields fall through to `range_scan`.

**Consequence:** `publishedAt < now` cache fragmentation unsolved. Future-dated content visible immediately. All Phase C acceptance criteria unmet.

**Files:** `src/concurrent_engine.rs`, `src/mutation.rs`, `src/write_coalescer.rs`

### 4.2 D3: Bound cache live maintenance missing filter definition check

The flush thread checks whether a mutated document's sort value exceeds a bound's threshold but does NOT verify the document passes the bound's filter predicate. A document with `nsfwLevel=2` and high `reactionCount` will be incorrectly added to a bound for `nsfwLevel=1`.

**Consequence:** False positives in bounds accelerate bloat growth. Bounds need more frequent rebuilds. Query results remain correct (bound is ANDed with filter result).

**Files:** `src/concurrent_engine.rs` (lines 232-269)

### 4.3 E4: Meta-index superset matching unused in query path

`MetaIndex::find_matching_entries()` is fully implemented and tested but never called outside unit tests. Query-time bound lookup uses direct `BoundKey` HashMap construction instead of meta-bitmap intersection.

**Consequence:** Each unique filter combination requires its own bound. A bound for `nsfwLevel=1` cannot be reused when querying `nsfwLevel=1 AND onSite=true`. Higher memory usage and lower cache hit rates.

**Files:** `src/concurrent_engine.rs` (apply_bound), `src/executor.rs` (apply_bound_if_available), `src/planner.rs`

### 4.4 D7: Invalidation model not unified

The trie cache still uses generation-counter lazy invalidation. The bound cache uses a separate live-maintenance + mark-for-rebuild system. The spec envisions a single paradigm shift to live maintenance, eliminating generation counters.

**Consequence:** Two invalidation models to reason about. The generation-counter system works correctly but adds complexity. Not a correctness issue.

**Files:** `src/cache.rs`, `src/concurrent_engine.rs`

---

## 5. Medium Priority Issues

Missing tests, edge cases, minor feature gaps.

### 5.1 Missing integration tests (P10, A10, A11, C6, C7, E6)

A recurring pattern: unit tests for primitives are thorough, but ConcurrentEngine-level integration tests are absent. Specific gaps:

| Phase | Missing Test | Why It Matters |
|-------|-------------|----------------|
| P10 | Filter diffs visible in published snapshots | Cannot test until P5/P7 fixed |
| P10 | Merge thread compaction correctness | Cannot test until P6 fixed |
| A10 | ConcurrentEngine Tier 2 end-to-end (put -> flush -> query via moka) | Full pipeline unverified |
| A11 | ConcurrentEngine restart (insert -> shutdown -> reopen -> verify) | Restart path untested and currently broken (A7) |
| C6 | Deferred alive through engine (insert future doc -> advance time -> query) | Feature not wired |
| C7 | Bucket snapping through engine query path | Feature not wired |
| E6 | `find_matching_entries()` via query path | Feature not wired |

### 5.2 Missing edge-case tests (B4)

- Ascending slot-order pagination (feature not implemented)
- Empty page (cursor beyond all results)
- Single result page with cursor emission
- Cursor field value assertions

### 5.3 Missing benchmarks (A12)

No benchmark validates memory reduction from the two-tier model. Existing benchmark harness does not support configuring fields as `Cached` vs `Snapshot`.

### 5.4 moka eviction listener absent (A4)

No eviction listener flushes dirty state to redb on cache eviction. The PendingBuffer compensates, but under eviction pressure with heavy writes, the buffer could grow between merge cycles.

### 5.5 Pending buffer depth metrics unexposed (A5)

`PendingBuffer::depth()` and `total_ops()` exist but are never called in production code. No Prometheus metric, no log output.

### 5.6 D6: Automatic LRU eviction not triggered

`BoundCacheManager::evict_lru()` exists but is never called automatically. No maximum bound count or memory budget triggers eviction. Bound count grows monotonically.

### 5.7 P9: No property-based tests

VersionedBitmap has 15 manual tests but no `proptest`/`quickcheck` property tests for diff correctness.

### 5.8 B3: `skip_sort` flag is dead code

`QueryPlan::skip_sort` is correctly set by the planner but never read by the executor. The no-sort decision is driven by `sort: Option<&SortClause>` matching instead.

---

## 6. Cross-Cutting Themes

### Theme 1: Primitives exist, integration layer is missing

This is the dominant pattern across the entire codebase. Every phase has well-built data structures and algorithms that are tested in isolation. The gap is consistently in `ConcurrentEngine` -- the integration point where primitives are assembled into production behavior.

| Component | Primitive | Integration Status |
|-----------|----------|-------------------|
| VersionedBitmap::apply_diff() | Working, tested | Never called by executor |
| TimeBucketManager | Working, tested | Never instantiated in engine |
| SlotAllocator::schedule_alive/activate_due | Working, tested | Never called by write path or timer |
| MetaIndex::find_matching_entries() | Working, tested | Never called by query path |
| QueryExecutor::with_time_buckets() | Working, tested | Never called by engine |
| BoundCacheManager::evict_lru() | Working, tested | Never called automatically |
| PendingBuffer::depth()/total_ops() | Working, tested | Never called in production |

### Theme 2: Diff-based write path not implemented

The core architectural innovation -- deferred filter diff compaction with read-time fusion -- is not functioning. The flush thread merges everything eagerly. This has a cascading effect:

1. **P5:** Executor does not fuse diffs (no dirty diffs exist)
2. **P6:** Merge thread has no Tier 1 diffs to compact
3. **P7:** Write coalescer treats all bitmap types identically
4. **A7:** Merge thread has no Tier 1 work to persist
5. **D1:** Bound cache uses plain RoaringBitmap (no VersionedBitmap needed since diffs are not accumulated)

### Theme 3: Persistence gap

Nothing in Tier 1 is persisted to redb:

| Component | Persisted? | Impact on Restart |
|-----------|-----------|-------------------|
| Tier 1 filter bitmaps | No | Lost |
| Sort layer bitmaps | No | Lost |
| Alive bitmap | No | Lost |
| Time bucket bitmaps | No | Lost (also not instantiated) |
| Tier 2 pending buffer (drained portion) | Yes | Survives |
| Tier 2 base bitmaps in redb | Yes | Survives |
| Document store (redb) | Yes | Survives |

### Theme 4: Config and metrics plumbing incomplete

Several config fields and metrics are defined but not consumed:
- `bound_promotion_threshold` missing from CacheConfig (hardcoded to 0)
- `PendingBuffer::depth()` never called
- No Prometheus metrics for any Phase A/D/E component (Phase 4 not started)

---

## 7. Recommended Fix Stages

### Stage 1: Diff-Based Write Path (Foundation)

**Addresses:** P5, P6, P7
**Dependencies:** None (foundational)
**Estimated Complexity:** HIGH

This is the single most impactful change. All subsequent stages depend on the diff model working correctly.

| Task | Files | Description |
|------|-------|-------------|
| Stop eagerly merging filter diffs in flush thread | `src/write_coalescer.rs` | Remove filter merge block at lines 308-315. Keep sort layer and alive eager merge. |
| Implement diff fusion in executor | `src/executor.rs`, `src/filter.rs` | Add `get_versioned()` to FilterField. Use `apply_diff()` in evaluate_clause. Update union/intersection. |
| Move filter diff compaction to merge thread | `src/concurrent_engine.rs` | Merge thread: snapshot Tier 1 diffs, compact into bases, publish merged snapshot. |
| Add integration tests for diff accumulation | `src/concurrent_engine.rs` | Test filter diffs visible in snapshot, merge compaction, concurrent reads during merge. |
| Add proptest for VersionedBitmap | `src/versioned_bitmap.rs` | Property: `apply_diff(base, diff) == merge(base, diff).base` for random operations. |

### Stage 2: Persistence + Lazy Loading + LRU (see Section 8)

**Addresses:** A7, A4, A10, A11, A12, D6
**Dependencies:** Stage 1 (diff model must work for Tier 1 persistence)
**Estimated Complexity:** HIGH

| Task | Files | Description |
|------|-------|-------------|
| Add Tier 1 persistence in merge thread | `src/concurrent_engine.rs` | After compacting Tier 1 diffs, batch-write merged bases to redb. Include sort layers and alive bitmap. |
| Add alive/sort bitmap serialization to BitmapStore | `src/bitmap_store.rs` | New tables or key prefixes for alive and sort layer bitmaps. |
| Add sort layer + alive loading on startup | `src/concurrent_engine.rs` | Extend A8 startup to load sort layers and alive from redb. |
| Add moka eviction listener | `src/tier2_cache.rs` | On eviction, flush pending mutations for that key to redb. |
| Implement lazy loading for Tier 1 filter bitmaps | `src/filter.rs`, `src/concurrent_engine.rs` | See Section 8 for design. |
| Implement LRU for unloading unused bitmaps | `src/filter.rs`, `src/concurrent_engine.rs` | See Section 8 for design. |
| Add automatic bound LRU eviction trigger | `src/bound_cache.rs` | Add max bound count; call `evict_lru()` in `form_bound()` when limit reached. |
| ConcurrentEngine restart integration test | `src/concurrent_engine.rs` or `tests/` | Insert -> shutdown -> reopen same redb -> verify queries. |
| ConcurrentEngine Tier 2 integration test | `src/concurrent_engine.rs` or `tests/` | Put with Cached field -> flush -> query via moka -> verify. |
| Memory reduction benchmark | `src/bin/benchmark.rs` | Configure tagIds/userId as Cached, measure RSS vs all-Snapshot baseline. |
| Expose pending buffer depth metric | `src/concurrent_engine.rs` | Log or counter for `PendingBuffer::depth()` in merge thread. |

### Stage 3: Time Handling + Engine Wiring (Phase C)

**Addresses:** C1, C2, C3, C6, C7
**Dependencies:** Stage 2 (buckets should persist to redb; deferred alive interacts with alive bitmap persistence)
**Estimated Complexity:** MEDIUM

| Task | Files | Description |
|------|-------|-------------|
| Add DeferredAlive mutation op | `src/mutation.rs`, `src/write_coalescer.rs` | diff_document checks `deferred_alive` config; emits DeferredAlive op for future values. |
| Wire schedule_alive into flush thread | `src/concurrent_engine.rs` | DeferredAlive op calls `slots.schedule_alive(slot, activate_at)`. |
| Add background activation timer | `src/concurrent_engine.rs` | Periodic `slots.activate_due(now_unix)` in flush or merge thread; publish updated snapshot. |
| Instantiate TimeBucketManager in engine | `src/concurrent_engine.rs` | Read `behaviors.range_buckets` from config; construct per-field managers. |
| Wire bucket managers into query execution | `src/concurrent_engine.rs` | Call `with_time_buckets()` on executor construction, or use `snap_range_clauses()` preprocessing. |
| Add background bucket refresh timer | `src/concurrent_engine.rs` | Periodic `refresh_due()` check and `rebuild_bucket()` in merge thread. |
| Persist bucket bitmaps to redb | `src/time_buckets.rs`, `src/bitmap_store.rs` | Serialize on rebuild, load on startup. |
| Integration tests | `src/concurrent_engine.rs` or `tests/` | Deferred alive lifecycle, bucket snapping through engine, refresh cycle correctness. |

### Stage 4: Query-Path Optimization (Phase D/E Polish)

**Addresses:** D3, D7, E4, E6, B1, B3, B4
**Dependencies:** Stage 1 (diff model needed for D7), Stage 3 (stable cache keys needed for bound formation)
**Estimated Complexity:** MEDIUM

| Task | Files | Description |
|------|-------|-------------|
| Add filter definition check in D3 live maintenance | `src/concurrent_engine.rs` | Before `entry.add_slot(slot)`, verify slot passes bound's filter predicate. |
| Wire `find_matching_entries()` into query path | `src/concurrent_engine.rs`, `src/executor.rs` | Replace or augment direct BoundKey HashMap lookup with meta-bitmap intersection for superset matching. |
| Unify invalidation model (D7) or document divergence | `src/cache.rs`, `src/concurrent_engine.rs` | Either migrate trie cache to live maintenance or explicitly document the dual-system design. |
| Wire `skip_sort` into executor (B3) | `src/executor.rs` | Have executor consult `plan.skip_sort` or remove the dead field. |
| Add ascending slot-order direction (B1) | `src/executor.rs` | Optional direction parameter to `slot_order_paginate`. |
| Add missing edge-case tests (B4, E6) | `src/executor.rs`, `src/meta_index.rs` | Empty page, single result, cursor assertions, superset matching integration. |

---

## 8. Lazy Loading + LRU Design Notes

### Current Architecture

The existing two-tier model is:

- **Tier 1 (Snapshot):** Always in the ArcSwap snapshot. All bitmaps loaded at startup, held in memory indefinitely. FilterIndex contains `HashMap<String, Arc<FilterField>>` where each field has `HashMap<u64, VersionedBitmap>`.
- **Tier 2 (Cached):** moka concurrent cache backed by redb. Bitmaps loaded on demand via `get_or_load()`, evicted by moka's memory budget. PendingBuffer handles mutations to cold (not-in-moka) bitmaps.

### The Problem

Tier 1 loads ALL bitmaps for Snapshot fields at startup and never unloads them. For fields like `nsfwLevel` (5 values) this is fine. But if a Tier 1 field has moderate cardinality (hundreds of values), rarely-queried values waste memory. There is no mechanism to unload unused Tier 1 bitmaps.

Tier 2 handles this for high-cardinality fields via moka, but the boundary is configured statically per field. There is no gradient between "always in memory" and "always on demand".

### Proposed Design: Unified Lazy Loading + LRU

Replace the static Tier 1/Tier 2 split with a unified model where ALL filter bitmaps can be lazily loaded and LRU-evicted, with configurable pinning for hot bitmaps.

#### Option A: Extend moka to cover Tier 1 (Recommended)

Use moka as a unified cache for all filter bitmaps, with differentiated policies:

```
FilterBitmapCache {
    cache: moka::sync::Cache<(field, value), Arc<RoaringBitmap>>,
    pinned: HashSet<(field, value)>,     // never evicted (alive, small fields)
    pending: PendingBuffer,              // unchanged
    store: BitmapStore,                  // redb backing store
}
```

**How it fits:**
- **Startup:** Load all pinned bitmaps from redb into moka with infinite weight (or pin via `invalidation_closures`). For non-pinned Tier 1 fields, load lazily on first query.
- **Reads:** All filter bitmap reads go through `cache.get_or_load()`. Pinned entries never evict. Non-pinned entries evict under memory pressure via moka's LRU/LFU policy.
- **Writes:** Mutations for loaded bitmaps apply to the diff layer (current model). Mutations for unloaded bitmaps go to PendingBuffer (current model). No change needed.
- **Merge thread:** Compacts diffs, writes to redb, optionally evicts cold entries.

**Changes needed:**
1. `src/filter.rs`: FilterField needs a `load_bitmap(value) -> Option<&VersionedBitmap>` method that returns `None` for unloaded values (triggers moka load).
2. `src/executor.rs`: evaluate_clause must handle the moka load path for ALL filter fields, not just Tier 2. The current Tier 1/Tier 2 branching can be unified.
3. `src/concurrent_engine.rs`: Remove the static Tier 1/Tier 2 field classification. Instead, configure per-field pinning policy.
4. `src/tier2_cache.rs`: Extend to handle all filter fields, not just `Cached` fields. Add pinning support.
5. `src/config.rs`: Replace `storage: snapshot | cached` with `storage: { pin: true, ... }` or similar.

**Advantages:**
- Single code path for all filter bitmap access (simplifies executor).
- Memory pressure handled uniformly by moka's eviction policy.
- Pinned bitmaps get the same fast-path as current Tier 1 (always in cache, no load penalty).
- Cold bitmaps for ANY field (including previously-Tier-1 moderate-cardinality fields) can be evicted.

#### Option B: Add LRU to FilterField directly

Keep the Tier 1/Tier 2 split but add an LRU eviction layer to FilterField's `HashMap<u64, VersionedBitmap>`:

```
FilterField {
    hot: HashMap<u64, VersionedBitmap>,    // recently accessed
    cold_keys: LruCache<u64>,              // tracks access order
    store: Option<Arc<BitmapStore>>,       // for lazy reload
}
```

When a bitmap is accessed, touch its LRU entry. A background sweep moves untouched bitmaps to redb and removes them from `hot`. On access of an evicted key, reload from redb.

**Disadvantage:** Duplicates moka's eviction logic. Two LRU systems (moka for Tier 2, custom for Tier 1).

### Recommendation

**Option A** is strongly preferred. It leverages the existing moka infrastructure, provides a single unified cache with proven eviction policies, and requires less new code. The key change is removing the hard Tier 1/Tier 2 boundary and treating moka as the universal filter bitmap cache with per-field pinning.

### Impact on Existing Code

| Component | Change Required |
|-----------|----------------|
| `src/tier2_cache.rs` | Extend to handle all fields; add pinning API |
| `src/executor.rs` | Unify Tier 1/Tier 2 bitmap resolution into single path |
| `src/concurrent_engine.rs` | Remove Tier 1/Tier 2 field classification; configure pinning |
| `src/filter.rs` | FilterField may become thin (pinned bitmaps) or removed entirely for cached fields |
| `src/config.rs` | Replace `storage: snapshot | cached` with pinning config |
| `src/pending_buffer.rs` | No change (still handles mutations to unloaded bitmaps) |
| `src/bitmap_store.rs` | No change (still the backing store) |
| `src/concurrent_engine.rs` merge thread | Must persist ALL filter bitmaps, not just Tier 2 pending drain |

### Interaction with Diff Model (Stage 1)

The lazy loading + LRU design works best after the diff-based write path (Stage 1) is functioning. When a bitmap is loaded from redb into moka, it arrives as a clean base. The diff layer accumulates mutations until the merge thread compacts and persists. If the bitmap is evicted before merge, the pending buffer captures the outstanding mutations. On reload, pending mutations are applied.

This is exactly the current Tier 2 flow, extended to all filter fields.
