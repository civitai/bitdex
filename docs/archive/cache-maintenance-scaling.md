# Cache Maintenance Scaling

Status: **Functional, with known limits at extreme scale**
Last updated: 2026-03-13

## What We Built

The unified cache surgically updates cached query results when documents are inserted, updated, or deleted — without invalidating and rebuilding from scratch. The flush thread applies bitmap mutations to staging, then maintains affected cache entries in-place via `add_slot` / `remove_slot` operations.

Three optimizations landed in this pass:

1. **MetaIndex field-level narrowing** — `maintain_filter_changes` and `maintain_sort_changes` use `entries_for_filter_field()` / `entries_for_sort_field()` to visit only entries that reference mutated fields, instead of scanning all entries linearly.

2. **Clause-level narrowing for Eq filters** — For the common case (Eq operator), `entries_for_clause(field, "eq", value)` narrows to entries matching the specific changed value. Non-Eq operators (In, Gt, Lt, NotEq, ranges) fall through to field-level as a safety net.

3. **Maintenance budget cap** — When estimated work (`affected_entries * changed_slots`) exceeds `max_maintenance_work` (default 50,000), affected entries are marked for rebuild instead of per-slot evaluation. Prevents positive feedback loops under burst writes.

### Reverse index

Added `meta_id_to_key: HashMap<CacheEntryId, UnifiedKey>` for O(1) lookup from MetaIndex bitmap results back to cache entries. Previously `entry_by_meta_id()` was a linear scan.

## Current Performance (bench-write-stress.mjs)

Benchmark: 5,000 documents, varying cache entry counts, each write changes all fields (worst case).

| Cache Entries | Single Writer ops/s | p50 | max | c=8 ops/s | c=8 max | Correctness |
|---|---|---|---|---|---|---|
| 0 | 241 | 3.1ms | 20ms | 464 | 46ms | PASS |
| 100 | 315 | 2.4ms | 15ms | 565 | 41ms | PASS |
| 1,000 | 340 | 1.0ms | 18ms | 575 | 39ms | PASS |
| 5,000 | 455 | 710us | 26ms | 844 | 35ms | PASS |
| 10,000 | 709 | 540us | 30ms | 1,596 | 33ms | PASS |
| 50,000 | 1,682 | 440us | 23ms | 3,084 | 41ms | PASS |

Before this fix, 50K entries had **148-second max latency spikes** under concurrent load. Now capped at ~50ms.

## What Changed (Before → After at 50K entries)

| Metric | Before | After | Improvement |
|---|---|---|---|
| Single writer max | 4,247ms | 23ms | **185x** |
| c=1 concurrent max | 143,253ms | 50ms | **2,865x** |
| c=8 concurrent max | 148,097ms | 41ms | **3,612x** |
| c=8 ops/s | 30 | 3,084 | **103x** |
| Correctness at 50K | 4/5 FAIL | 5/5 PASS | Fixed |

## Known Gaps & Risks

### 1. Clause-level narrowing only covers Eq

The clause-level optimization finds entries with `Eq("category", "5")` directly via MetaIndex. Entries using `In`, `Gt`, `Lt`, `Gte`, `Lte`, `NotEq`, or `Not` fall through to the broader field-level path (all entries mentioning the field).

**Risk**: If production workloads use many range-filter caches on high-cardinality fields, the narrowing degrades to field-level for those entries. Current Civitai traffic is ~90% Eq filters, so this is low risk.

**To improve**: Extend MetaIndex to support range-aware lookups. For `In` clauses, register each value separately. For range clauses, register the field+op and use bitmap intersections to narrow.

### 2. Budget cap marks entries for rebuild

When `affected_entries * changed_slots > 50,000`, all affected entries get `needs_rebuild = true`. The next query for each entry triggers a full traversal (cache miss), which is slower than a cache hit.

**Risk**: Under sustained high write throughput with many cache entries, queries may see elevated miss rates until entries are rebuilt. This is a correctness-preserving degradation — results are always correct, just slower.

**When it triggers**: With the clause-level narrowing, the budget cap rarely triggers because affected_entries is small. It's a safety net for pathological cases (e.g., changing all fields on every write with 50K+ caches sharing few distinct filter values).

**To improve**: Instead of all-or-nothing, maintain the first N entries and mark the rest. Or prioritize hot entries (high hit count) for maintenance and mark cold entries for rebuild.

### 3. `reconcile_bytes()` is O(N) every flush

`reconcile_bytes()` iterates all cache entries and sums `memory_bytes()` (which calls `bitmap.serialized_size()`). This runs every flush cycle that has mutations.

**Risk**: At 50K entries, this adds ~5-10ms per flush. Not a spike risk, but unnecessary overhead.

**To improve**: Track byte deltas incrementally in `add_slot`, `remove_slot`, `remove_slot_blind`. Return the delta from each method and accumulate in the maintenance functions. Remove `reconcile_bytes()` from the hot path; keep it as a periodic consistency check.

### 4. `remove_slot_from_all()` is O(N) on delete

When a document is deleted, `remove_slot_from_all(slot)` iterates all entries and calls `remove_slot_blind(slot)`. Each call is O(log containers) on the bitmap — fast per-entry but linear in entry count.

**Risk**: Low. Deletes are infrequent compared to upserts, and `remove_slot_blind` is cheap (~10-50ns per entry). At 50K entries: ~500us. At 1M: ~10ms.

**To improve**: Could skip entries that definitely don't contain the slot (would need a per-slot → entry_id reverse index, which is expensive to maintain). Not worth it unless delete rates increase significantly.

### 5. Key cloning in maintenance functions

The maintenance functions clone `UnifiedKey` (which contains `Vec<CanonicalClause>` with Strings) when collecting affected entries. This is necessary to avoid borrow checker conflicts between `meta_id_to_key` (immutable) and `entries` (mutable).

**Risk**: At 50K affected entries, this is ~50K key clones with heap allocations. Adds ~1-2ms. Not a spike risk but adds constant overhead.

**To improve**: Use `Arc<UnifiedKey>` or store keys in a slab indexed by `CacheEntryId`. Or restructure the entries HashMap to allow split borrows (e.g., `entries: HashMap<CacheEntryId, (UnifiedKey, UnifiedEntry)>` with a separate key→id index).

### 6. Sort maintenance can't narrow by value

For sort mutations, MetaIndex narrows by sort field + direction, but all entries sorting by that field are checked. With 4 sort fields, ~25% of all entries are affected per sort mutation.

**Risk**: With few sort fields (typical), sort maintenance costs scale with total entries / sort_field_count. At 50K entries and 4 sort fields: ~12,500 entries per mutation. The `sort_qualifies()` fast path skips most (~99%) without calling `slot_matches_filter()`, so actual cost is low.

**To improve**: Not easily narrowable — sort qualification depends on the entry's current min_tracked_value, not a static property. The budget cap handles pathological cases.

## Production Fit

Civitai production characteristics:
- ~2,500 distinct query patterns → ~2,500 cache entries
- Write rate: ~10-50 upserts/second
- Most upserts change 1-2 fields (reactionCount, commentCount)
- Filters are ~90% Eq

At these parameters:
- Clause narrowing reduces affected entries to ~50-100 per mutation (not 2,500)
- Budget cap never triggers (50 entries * 1 slot = 50, well under 50,000 threshold)
- Per-flush maintenance: ~50 entries * 1 slot * ~300ns = ~15us
- Max latency: <30ms

The current implementation is well-suited for production. The gaps above matter at 10K+ cache entries with burst writes — a scenario to plan for as the product scales.

## Files

- `src/unified_cache.rs` — Maintenance functions, budget cap, clause narrowing
- `src/meta_index.rs` — MetaIndex with clause/field/sort lookups
- `src/concurrent_engine.rs` — Flush thread wiring (lines 554-585)
- `tests/e2e/e2e-cache-maintenance.mjs` — 9 E2E correctness tests
- `tests/e2e/bench-write-stress.mjs` — Stress benchmark (0 to 1M caches)
- `tests/e2e/bench-write-path.mjs` — Write path benchmark (fan-out, throughput)
