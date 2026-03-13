# Bitdex V2 -- Spec Compliance Implementation Plan

**Date:** 2026-02-21
**Source:** `docs/audit/synthesis.md` (audit findings), `docs/plans/roadmap-performance-and-persistence.md` (authoritative spec), `docs/reviews/architecture-risk-review.md` (risk constraints), `CLAUDE.md` (inviolable design principles)

---

## Scope Summary

1. **The diff-based write path -- the foundational architectural innovation -- is not functioning as designed.** The flush thread eagerly merges ALL bitmaps (filter + sort + alive) before publishing. The executor never fuses diffs. The merge thread has no Tier 1 work. This defeats the performance model every subsequent phase depends on. (P5, P6, P7 -- NON-COMPLIANT)

2. **Tier 1 bitmaps are never persisted to redb.** Process restart loses all in-memory index state. Sort layers, alive bitmap, and Tier 1 filter bitmaps are never written to disk. The merge thread only drains Tier 2 pending buffer mutations. (A7 -- PARTIAL)

3. **Phases C, D, and E have production-ready primitives that are dead code.** TimeBucketManager, deferred alive, MetaIndex superset matching, and bound cache filter definition checks are all implemented, unit-tested, but never wired into `ConcurrentEngine`. (C1-C3, D3, E4 -- PARTIAL/NON-COMPLIANT)

4. **Missing integration tests across all phases.** Unit tests for primitives are thorough, but ConcurrentEngine-level integration tests are absent for diff accumulation, merge compaction, restart, Tier 2 end-to-end, time handling, and meta-index query-path integration. (P10, A10, A11, C6, C7, E6 -- PARTIAL)

5. **Scorecard: 55% COMPLIANT, 33% PARTIAL, 8% NON-COMPLIANT, 2% MISSING.** The 4 NON-COMPLIANT items (P5, P7, D7, E4) and the 1 MISSING item (A12) represent the highest-impact gaps. Fixing Stage 1 unblocks all downstream stages.

---

## Stage 1: Diff-Based Write Path

**Addresses:** P5, P6, P7 + P9, P10
**Dependencies:** None (foundational -- all subsequent stages depend on this)
**Estimated Complexity:** HIGH
**Primary files:** `src/write_coalescer.rs`, `src/filter.rs`, `src/executor.rs`, `src/concurrent_engine.rs`, `src/versioned_bitmap.rs`

### Design Specification

Per the architecture-risk-review.md (issues 3, 4, 5) and roadmap spec:

- **Flush thread** (every 100ms, never blocks): drain mutation channel, merge sort diffs into bases (fast, ~160 bitmaps), accumulate filter diffs in Arc, merge alive eagerly, publish snapshot via ArcSwap.
- **Merge thread** (every 5s or diff threshold): snapshot current Tier 1 filter diffs, compact into bases (in-place when `Arc::strong_count == 1`), publish merged snapshot with empty filter diffs.
- **Executor** (read path): when evaluating a filter clause against a Tier 1 field, use `VersionedBitmap::apply_diff()` to fuse the diff into the candidate intersection. Sort layers have no active diffs (merged by flush thread).

### Tasks

#### S1.1: Stop eagerly merging filter diffs in flush thread

**Files:** `src/write_coalescer.rs`
**What to change:**
- In `WriteBatch::apply()` (line 308-315), remove the block that calls `field.merge_dirty()` for filter fields. Keep the sort layer `merge_dirty()` (lines 293-306) and alive `merge_alive()` (line 318) -- these must still merge eagerly per architecture-risk-review.md issue 4.
- The filter mutations at lines 256-267 (inserts) and 263-267 (removes) remain unchanged -- they write to the VersionedBitmap diff layer via `insert_bulk()` / `remove_bulk()`.

**Acceptance criteria:**
- After `WriteBatch::apply()`, filter `VersionedBitmap`s have non-empty diffs (`.is_dirty() == true`).
- Sort layer `VersionedBitmap`s have empty diffs (`.is_dirty() == false`).
- Alive bitmap has empty diff (merged eagerly).
- Existing `WriteBatch` unit tests still pass (the tests call `apply` then check via `get()` which accesses base -- these tests will need updating in S1.3).

**Dependencies:** None

#### S1.2: Add `get_with_diff()` method to FilterField

**Files:** `src/filter.rs`
**What to change:**
- Add a new method `get_versioned(&self, value: u64) -> Option<&VersionedBitmap>` that returns the raw `VersionedBitmap` including its diff layer.
- Add `union_with_diff(&self, values: &[u64], candidates: &RoaringBitmap) -> RoaringBitmap` that computes the union of multiple values while fusing diffs against candidates. For each value, call `vb.apply_diff(candidates)` and union the results.
- Add `apply_diff_eq(&self, value: u64, candidates: &RoaringBitmap) -> Option<RoaringBitmap>` as a convenience for single-value diff fusion.
- The existing `get()` method must be updated: remove the `debug_assert!(!vb.is_dirty())` on line 101, because filter bitmaps will now routinely have dirty diffs in published snapshots. Replace with a method that reads the merged base (for sort-path usage or other base-only callers).

**Acceptance criteria:**
- `get_versioned()` returns `Some` for values that have bitmaps (even dirty ones).
- `apply_diff_eq()` returns the correct fused bitmap for dirty VersionedBitmaps.
- `union_with_diff()` returns the correct union with diffs applied.
- Unit tests cover: clean bitmap, dirty bitmap with sets only, dirty bitmap with clears only, dirty bitmap with both, missing value.

**Dependencies:** S1.1 (filter diffs are now dirty in snapshots)

#### S1.3: Implement diff fusion in executor

**Files:** `src/executor.rs`
**What to change:**
- In `evaluate_clause()` (line 416-529), modify the `FilterClause::Eq` branch (lines 418-432):
  - Instead of `filter_field.get(key).cloned().unwrap_or_default()`, use `filter_field.get_versioned(key)` and if the bitmap is dirty, call `vb.apply_diff(candidates)` where candidates is the alive bitmap. If not dirty, use the base directly.
  - **Key design decision:** For the Eq path, we do not have candidates yet (this is a single-clause evaluation). Use `vb.apply_diff(&alive)` to produce the correct bitmap. This is acceptable because the diff is small (KB) and alive is the broadest possible candidate set.
- Modify `FilterClause::In` branch (lines 445-458): use `filter_field.union_with_diff()` with alive as candidates.
- Modify `FilterClause::NotEq` branch (lines 435-443): the eq_bitmap now comes from apply_diff, so NotEq automatically inherits correctness.
- In `compute_filters()` (line 397-413): no change needed -- it calls `evaluate_clause` which now handles diffs.
- Modify the `range_scan` method (used by Gt/Gte/Lt/Lte): iterate over `VersionedBitmap`s with `apply_diff` instead of base-only access.
- Update `FilterField::iter()` callers: the range_scan currently iterates via `filter_field.iter()` which returns base-only bitmaps. Add an `iter_versioned()` method to FilterField that returns `(&u64, &VersionedBitmap)` pairs for diff-aware range scans.

**Acceptance criteria:**
- A query against a Tier 1 filter field with dirty diffs returns correct results (includes sets, excludes clears).
- A query with multiple Eq clauses (AND) with dirty diffs returns correct intersection.
- A query with In clause and dirty diffs returns correct union.
- A query with NotEq/Not clause and dirty diffs returns correct complement.
- Range queries (Gt, Lt, etc.) with dirty diffs return correct results.
- All existing executor tests pass (update any that assert on internals).

**Dependencies:** S1.2 (new FilterField methods)

#### S1.4: Move filter diff compaction to merge thread

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In the merge thread (lines 434-486), add Tier 1 filter diff compaction:
  1. Load the current snapshot via `self.inner.load()`.
  2. Clone the snapshot (Arc-per-field clone -- cheap, O(num_fields)).
  3. For each Tier 1 filter field, call `field.merge_dirty()` to compact diffs into bases.
  4. Publish the merged snapshot via `self.inner.store()`.
  5. The merge thread already has a sleep loop -- add the diff compaction before the Tier 2 pending drain.
- **Coordination with flush thread:** The flush thread publishes dirty-diff snapshots. The merge thread reads the latest snapshot, compacts, and publishes a clean snapshot. Between merge cycles, the flush thread keeps publishing dirty snapshots. This is safe because:
  - The merge thread's compacted snapshot may be immediately overwritten by the flush thread's next dirty publish.
  - Readers always see the latest state (dirty or clean). Dirty is correct thanks to S1.3 diff fusion.
  - The merge thread reduces memory pressure by compacting diffs that have accumulated over ~5 seconds.
- **Implementation pattern:** The merge thread needs mutable access to filter fields. Since it operates on a clone (via Arc-per-field CoW), it can call `Arc::make_mut()` on the field Arcs in its local clone without affecting the published snapshot or the flush thread's staging copy.

**Acceptance criteria:**
- After a merge cycle, the published snapshot has clean (non-dirty) filter bitmaps.
- During the merge cycle, the flush thread continues publishing dirty snapshots unblocked.
- Readers during a merge cycle see correct results (either dirty+fused or clean).
- The merge thread does not hold any lock that blocks the flush thread.
- Bitmap state is identical whether read via dirty diff fusion or after merge compaction (proptest S1.7).

**Dependencies:** S1.1 (filter diffs accumulate), S1.3 (executor handles dirty snapshots)

#### S1.5: Scope alignment verification

**Files:** `docs/reviews/architecture-risk-review.md` (read-only reference)
**What to verify:**
- Issue 3 (merge blocking flush): Confirm flush and merge are separate threads, merge never blocks flush.
- Issue 4 (sort diff fusion overhead): Confirm sort diffs are merged by flush thread before publishing; readers never see dirty sort diffs.
- Issue 5 (snapshot clone copies all diffs): Confirm filter diffs are `Arc<BitmapDiff>` (already the case per `versioned_bitmap.rs`). Snapshot clone copies Arc pointers, not bitmap data.
- Verify the two-thread model in `concurrent_engine.rs` matches the spec diagram in the roadmap.

**Acceptance criteria:**
- Written verification (code comments or test assertions) confirming each risk-review issue is addressed.
- No sort layer `is_dirty()` in any published snapshot.
- Diff `Arc<BitmapDiff>` is shared across snapshot clones (verify via `Arc::ptr_eq` in test).

**Dependencies:** S1.1, S1.4

#### S1.6: Update WriteBatch tests for new filter diff behavior

**Files:** `src/write_coalescer.rs` (test module)
**What to change:**
- Tests that call `batch.apply()` then check filter bitmap state via `field.get()` must be updated. After S1.1, filter diffs are not merged by `apply()`. Tests should either:
  - Call `field.merge_dirty()` before asserting on `get()`, OR
  - Use `get_versioned()` and check `contains()` on the VersionedBitmap, OR
  - Call `apply_diff()` with a full-range candidate bitmap.
- Add new test: `test_apply_filter_diffs_not_merged` -- after apply, verify `field.get_versioned(value).unwrap().is_dirty() == true`.
- Add new test: `test_apply_sort_diffs_merged` -- after apply, verify `sort_field.layer(n).unwrap().is_dirty() == false` for all mutated sort layers.
- Add new test: `test_apply_alive_merged` -- after apply, verify alive bitmap has no dirty diff.

**Acceptance criteria:**
- All WriteBatch tests pass with the new diff model.
- New tests explicitly verify the sort-merged / filter-deferred split.

**Dependencies:** S1.1, S1.2

#### S1.7: VersionedBitmap proptest (P9 gap)

**Files:** `src/versioned_bitmap.rs`
**What to change:**
- Add `proptest` dependency to `Cargo.toml` if not already present.
- Add property-based test: for any random sequence of insert/remove operations on a VersionedBitmap, `apply_diff(full_universe)` produces the same bitmap as `merge()` then `base()`.
- Add property: `merge()` is idempotent -- merging twice produces the same result.
- Add property: diff operations are order-independent for the same bit (last-write-wins per the set/clear exclusivity invariant).
- Add property: `contains(bit)` after any sequence of operations matches `apply_diff({bit})`.

**Acceptance criteria:**
- All property tests pass for 10,000+ random cases.
- No panics under any input sequence.

**Dependencies:** None (can parallelize with S1.1-S1.4)

#### S1.8: Integration tests for diff accumulation and merge compaction (P10 gap)

**Files:** `src/concurrent_engine.rs` (test module) or `tests/` integration test
**What to change:**
- **Test: filter diffs visible in published snapshot.** Insert documents via `put()`, wait for flush, verify the snapshot's filter fields have dirty diffs (`is_dirty() == true`). Query and verify correct results via diff fusion.
- **Test: merge thread compaction.** Insert documents, wait for flush (dirty snapshot), wait for merge cycle, verify snapshot's filter fields are clean (`is_dirty() == false`). Query and verify same results as before merge.
- **Test: concurrent reads during merge.** Spawn reader threads querying while merge is in progress. All queries must return correct results. No panics, no deadlocks.
- **Test: sort layers always clean.** After any sequence of puts/deletes, verify no sort layer bitmap in the published snapshot has `is_dirty() == true`.
- **Test: filter diffs accumulate across flush cycles.** Insert doc A in flush cycle 1, insert doc B in flush cycle 2 (before merge). Verify the filter bitmap for the shared value has both bits in its diff. Query returns both docs.

**Acceptance criteria:**
- All 5 integration tests pass.
- Tests are resilient to timing (use `wait_for_flush` pattern with retries).

**Dependencies:** S1.1, S1.3, S1.4

---

## Stage 2: Persistence + Lazy Loading + LRU

**Addresses:** A7, A4, A10, A11, A12, D6
**Dependencies:** Stage 1 (diff model must work for Tier 1 persistence)
**Estimated Complexity:** HIGH
**Primary files:** `src/concurrent_engine.rs`, `src/bitmap_store.rs`, `src/tier2_cache.rs`, `src/filter.rs`, `src/executor.rs`, `src/bound_cache.rs`, `src/config.rs`

### Design Specification

Per synthesis Section 8 (Option A -- recommended): replace the static Tier 1/Tier 2 split with a unified moka cache for all filter bitmaps, with per-field pinning for hot fields.

**Startup:** Load pinned bitmaps from redb into moka. Non-pinned Tier 1 fields load lazily on first query.
**Reads:** All filter bitmap reads go through moka `get_or_load()`. Pinned entries never evict.
**Writes:** Mutations for loaded bitmaps go to diff layer (current model). Mutations for unloaded bitmaps go to PendingBuffer (current model).
**Merge thread:** Compacts diffs, writes ALL filter bitmaps (not just Tier 2) to redb, evicts cold entries.

### Tasks

#### S2.1: Add alive, sort layer, and slot counter persistence to BitmapStore

**Files:** `src/bitmap_store.rs`
**What to change:**
- Add new redb table `TABLE_ALIVE` (or key prefix `__alive:0`) for the alive bitmap.
- Add new redb table `TABLE_SORT_LAYERS` (or key prefix `__sort:<field>:<layer>`) for sort layer bitmaps.
- Add new redb table `TABLE_META` (or key prefix `__meta:slot_counter`) for the slot counter high-water mark.
- Add methods:
  - `write_alive(&self, bitmap: &RoaringBitmap) -> Result<()>`
  - `load_alive(&self) -> Result<Option<RoaringBitmap>>`
  - `write_sort_layers(&self, field: &str, layers: &[&RoaringBitmap]) -> Result<()>`
  - `load_sort_layers(&self, field: &str, num_layers: usize) -> Result<Option<Vec<RoaringBitmap>>>`
  - `write_slot_counter(&self, counter: u32) -> Result<()>`
  - `load_slot_counter(&self) -> Result<Option<u32>>`
- Batch these writes into the same redb transaction as filter bitmap writes for atomicity.

**Acceptance criteria:**
- Round-trip test: write alive + sort layers + slot counter, read back, verify identical.
- Batch write with filter bitmaps in same transaction succeeds atomically.
- Missing data on load returns `None` (clean startup from empty db).

**Dependencies:** None (can start immediately)

#### S2.2: Tier 1 persistence in merge thread

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In the merge thread, after compacting Tier 1 filter diffs (from S1.4), add:
  1. Serialize all Tier 1 filter bases to redb via `BitmapStore::write_batch()`.
  2. Serialize the alive bitmap via `BitmapStore::write_alive()`.
  3. Serialize all sort layer bases via `BitmapStore::write_sort_layers()`.
  4. Serialize the slot counter via `BitmapStore::write_slot_counter()`.
  5. All writes in a single redb transaction (batch write).
- **Performance:** At 5M records, Tier 1 filter bitmaps are ~65MB (excluding tagIds/userId which are Tier 2). Sort layers ~55MB. Alive ~0.5MB. Total ~120MB per merge cycle. With NVMe sequential writes at 3GB/s, this takes ~40ms -- well within the 5-second merge interval.
- **Optimization:** Only write filter fields that had dirty diffs in this merge cycle. Sort layers and alive only need writing if they changed.

**Acceptance criteria:**
- After a merge cycle, redb contains current Tier 1 filter bases, sort layers, alive bitmap, and slot counter.
- Writes are batched in a single transaction.
- Merge thread completes within 5-second interval at 5M scale.
- No stale data in redb (generation tracking or always-overwrite).

**Dependencies:** S1.4 (merge thread compacts diffs), S2.1 (persistence API)

#### S2.3: Startup loading of alive, sort layers, and slot counter from redb

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In `ConcurrentEngine::build()`, after loading Tier 1 filter bitmaps (existing A8 code, lines 107-124):
  1. Load alive bitmap from redb via `BitmapStore::load_alive()`. If present, restore into `SlotAllocator`.
  2. Load sort layers from redb via `BitmapStore::load_sort_layers()` for each configured sort field. Restore into `SortIndex`.
  3. Load slot counter from redb via `BitmapStore::load_slot_counter()`. Restore into `SlotAllocator`.
- Add `SlotAllocator::restore_from(alive: RoaringBitmap, counter: u32)` method to `src/slot.rs`.
- Add `SortField::load_layers(layers: Vec<RoaringBitmap>)` method to `src/sort.rs`.

**Acceptance criteria:**
- After restart, `alive_count()` matches pre-shutdown count.
- After restart, `slot_counter()` matches pre-shutdown counter.
- After restart, sort queries return identical ordering to pre-shutdown.
- Empty redb (fresh start) works correctly -- no bitmaps loaded, engine starts clean.

**Dependencies:** S2.1 (persistence API), S2.2 (data must be in redb to load)

#### S2.4: Add moka eviction listener for dirty diffs (A4 gap)

**Files:** `src/tier2_cache.rs`
**What to change:**
- Add an eviction listener to the moka cache that fires when a cache entry is evicted:
  1. Check if the evicted bitmap has a non-empty pending buffer entry (via `PendingBuffer::has_pending(field, value)`).
  2. If yes, load the base from redb, apply pending mutations, write back to redb, remove from pending buffer.
  3. If no pending mutations, the eviction is clean -- no action needed.
- **Note:** The eviction listener runs on the moka background thread. It must have access to `Arc<BitmapStore>` and `Arc<Mutex<PendingBuffer>>`. Pass these as captured variables in the listener closure.

**Acceptance criteria:**
- Test: insert a Tier 2 bitmap with pending mutations, force eviction (by exceeding cache budget), verify redb has the updated bitmap.
- Test: evict a clean bitmap (no pending), verify no redb write occurs.
- No deadlocks between eviction listener and merge thread (both access PendingBuffer).

**Dependencies:** None (can start immediately)

#### S2.5: Automatic bound cache LRU eviction (D6 gap)

**Files:** `src/bound_cache.rs`, `src/concurrent_engine.rs`
**What to change:**
- Add `max_bound_count: usize` field to `BoundCacheManager` (configurable, default 100).
- In `form_bound()`, after creating a new bound, check if `self.len() > max_bound_count`. If so, call `self.evict_lru()` to remove the least-recently-used bound.
- Wire `max_bound_count` from config: add `bound_max_count` to `CacheConfig` in `src/config.rs`.

**Acceptance criteria:**
- After inserting `max_bound_count + 1` bounds, the total count is capped at `max_bound_count`.
- The evicted bound is the one with the oldest `last_access` timestamp.
- Existing bound cache tests still pass.

**Dependencies:** None

#### S2.6: Expose pending buffer depth metric (A5 gap)

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In the merge thread, after draining pending buffer, log the current depth:
  ```rust
  let depth = merge_pending.lock().depth();
  if depth > 0 {
      eprintln!("merge thread: pending buffer depth after drain: {depth}");
  }
  ```
- Add `pending_depth()` method to `ConcurrentEngine` for programmatic access (for future Prometheus integration in Phase 4).

**Acceptance criteria:**
- Pending buffer depth is logged when non-zero after merge cycle.
- `pending_depth()` returns the current pending buffer depth.

**Dependencies:** None

#### S2.7: ConcurrentEngine restart integration test (A11 gap)

**Files:** `tests/restart_test.rs` (new integration test file)
**What to change:**
- Create an integration test that:
  1. Opens a `ConcurrentEngine` with a temp directory for both docstore and bitmap_store.
  2. Inserts 100 documents with various filter values and sort values.
  3. Waits for flush + merge cycle to complete (ensure data is persisted).
  4. Queries and records expected results.
  5. Shuts down the engine.
  6. Opens a NEW `ConcurrentEngine` with the same paths.
  7. Queries with the same parameters. Verifies identical results.
  8. Verifies `alive_count()`, `slot_counter()`, and sort ordering match pre-shutdown values.
- Also test edge cases: empty engine restart, restart after deletes, restart after upserts.

**Acceptance criteria:**
- Restart produces identical query results for all test queries.
- `alive_count()` matches across restart.
- `slot_counter()` matches across restart.
- Sort queries return identical ordering.
- Deleted documents remain deleted after restart.

**Dependencies:** S2.2, S2.3 (persistence must work end-to-end)

#### S2.8: ConcurrentEngine Tier 2 integration test (A10 gap)

**Files:** `tests/tier2_test.rs` (new integration test file) or `src/concurrent_engine.rs` test module
**What to change:**
- Create an integration test with a config that has `tagIds` as `Cached` (Tier 2):
  1. Put documents with various tagIds values.
  2. Wait for flush (pending buffer receives Tier 2 mutations).
  3. Query for a specific tagId -- should load from redb via moka, apply pending, return correct results.
  4. Query same tagId again -- should hit moka cache.
  5. Verify cache hit count if moka exposes metrics.
- Test thundering herd: spawn 10 concurrent readers for the same cold tagId. Verify only one redb load occurs (moka `get_with` coalescing).

**Acceptance criteria:**
- Tier 2 queries return correct results.
- Pending mutations are applied to loaded bitmaps.
- Concurrent readers do not cause duplicate loads.

**Dependencies:** S2.2 (Tier 2 data must be in redb)

#### S2.9: Memory reduction benchmark (A12 gap)

**Files:** `src/bin/benchmark.rs`
**What to change:**
- Add a benchmark mode that configures `tagIds` and `userId` as `Cached` fields.
- Run the existing 5M insert + query benchmark with this config.
- Report bitmap memory with and without Tier 2 caching.
- Expected result: bitmap memory drops from ~328MB to ~65MB for Tier 1 only, with Tier 2 adding ~50-100MB via moka cache budget.

**Acceptance criteria:**
- Benchmark runs without errors.
- Memory report shows Tier 2 fields NOT counted in snapshot bitmap memory.
- Tier 2 cache size respects configured budget.
- Query latency for Tier 2 fields within 2x of Tier 1 (acceptable moka overhead).

**Dependencies:** S2.7, S2.8 (basic persistence must work)

---

## Stage 3: Time Handling Integration

**Addresses:** C1, C2, C3, C6, C7
**Dependencies:** Stage 2 (buckets should persist to redb; deferred alive interacts with alive bitmap persistence)
**Estimated Complexity:** MEDIUM
**Primary files:** `src/concurrent_engine.rs`, `src/mutation.rs`, `src/write_coalescer.rs`, `src/time_buckets.rs`, `src/bitmap_store.rs`

### Tasks

#### S3.1: Add DeferredAlive mutation op and write-path integration

**Files:** `src/mutation.rs`, `src/write_coalescer.rs`, `src/concurrent_engine.rs`
**What to change:**
- In `diff_document()` (`src/mutation.rs`), check if the config has a field with `deferred_alive: true`. If so, read the document's value for that field. If the value is in the future (greater than current unix timestamp), emit a `MutationOp::DeferredAlive { slot, activate_at }` instead of `MutationOp::AliveInsert`.
- Add `MutationOp::DeferredAlive { slot: u32, activate_at: u64 }` variant to the enum in `src/write_coalescer.rs`.
- In `WriteBatch::group_and_sort()`, collect `DeferredAlive` ops into a new `deferred_alive: Vec<(u32, u64)>` field.
- In `WriteBatch::apply()`, call `slots.schedule_alive(slot, activate_at)` for each deferred alive op (the method already exists on `SlotAllocator`).
- In the flush thread, no special handling needed -- `schedule_alive` stores the pending activation internally.

**Acceptance criteria:**
- A document with a future `publishedAt` value does NOT appear in query results (alive bit not set).
- A document with a past `publishedAt` value gets normal `AliveInsert` treatment.
- `SlotAllocator::schedule_alive()` is called with the correct activation timestamp.

**Dependencies:** Stage 1 (write path must be functioning correctly)

#### S3.2: Add background activation timer

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In the flush thread loop (or merge thread loop -- merge is better since it runs less frequently and activation does not need sub-second precision):
  1. Call `staging.slots.activate_due(now_unix)` where `now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()`.
  2. If any slots were activated, ensure the next snapshot publish includes the updated alive bitmap.
- Since the merge thread already runs every 5 seconds, this is a natural place for activation checks. However, the alive bitmap is owned by the flush thread's staging copy. Options:
  - **Option A (simpler):** Check in the flush thread on every cycle. `activate_due()` is cheap (O(pending count), typically small). This ensures sub-second activation precision.
  - **Option B:** Send an activation request via the mutation channel. The flush thread processes it like any other mutation.
- **Recommended:** Option A -- call in flush thread since it already owns staging.

**Acceptance criteria:**
- A document scheduled for activation at time T becomes visible in queries within one flush cycle after time T.
- Multiple documents with different activation times activate independently.
- Activation under load does not block the flush thread (O(pending count) is small).

**Dependencies:** S3.1 (deferred alive mutations must be flowing)

#### S3.3: Instantiate TimeBucketManager in ConcurrentEngine

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In `ConcurrentEngine::build()`:
  1. Scan `config.filter_fields` for fields with `behaviors.range_buckets` defined.
  2. For each, construct a `TimeBucketManager` with the field name and bucket configs.
  3. Store as `time_buckets: Option<Arc<TimeBucketManager>>` on `ConcurrentEngine`.
- The `TimeBucketManager` is already implemented in `src/time_buckets.rs`. It just needs to be instantiated and wired.

**Acceptance criteria:**
- With a config that has `publishedAt` with range buckets, the engine has a non-None `time_buckets`.
- Without bucket config, `time_buckets` is None.

**Dependencies:** None

#### S3.4: Wire bucket managers into query execution

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In `ConcurrentEngine::query()` and `execute_query()`, when constructing the `QueryExecutor`:
  1. If `self.time_buckets` is Some, call `executor.with_time_buckets(tb, now_unix)`.
  2. Compute `now_unix` from system clock.
- The executor already has `with_time_buckets()` and the bucket snapping logic in `evaluate_clause()` (lines 498-516). It just needs to be wired.

**Acceptance criteria:**
- A range query on a bucketed timestamp field (e.g., `publishedAt > now - 7d`) snaps to the `7d` bucket bitmap instead of doing a full range scan.
- A range query that does not match any bucket falls through to normal range_scan.
- Cache keys use bucket names (stable) instead of raw timestamps (fragmented).

**Dependencies:** S3.3 (TimeBucketManager must exist)

#### S3.5: Add background bucket refresh timer

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In the merge thread, after Tier 2 drain and Tier 1 persistence:
  1. If `time_buckets` is Some, call `tb.refresh_due(now_unix)`.
  2. For each bucket that needs refresh, call `tb.rebuild_bucket(bucket_name, &alive_bitmap)` using the current alive bitmap from the latest snapshot.
  3. Persist refreshed bucket bitmaps to redb via a new `BitmapStore::write_bucket()` method.
- The `TimeBucketManager` already has `refresh_due()` and bucket management methods.

**Acceptance criteria:**
- Bucket bitmaps are refreshed on their configured interval.
- Refreshed buckets reflect the current alive set.
- Buckets persist to redb and survive restart.

**Dependencies:** S3.3, S2.2 (persistence infrastructure)

#### S3.6: Persist bucket bitmaps to redb

**Files:** `src/bitmap_store.rs`, `src/time_buckets.rs`
**What to change:**
- Add redb key prefix `__bucket:<field>:<bucket_name>` for time bucket bitmaps.
- Add `BitmapStore::write_bucket(field: &str, bucket_name: &str, bitmap: &RoaringBitmap)`.
- Add `BitmapStore::load_bucket(field: &str, bucket_name: &str) -> Result<Option<RoaringBitmap>>`.
- On startup, if TimeBucketManager is configured, load bucket bitmaps from redb.

**Acceptance criteria:**
- Bucket bitmaps survive restart.
- Fresh start with no buckets in redb works (buckets rebuild from alive bitmap on first refresh cycle).

**Dependencies:** S2.1 (bitmap store API)

#### S3.7: Integration tests for time handling

**Files:** `tests/time_handling_test.rs` (new)
**What to change:**
- **Test: deferred alive lifecycle.** Insert a document with `publishedAt = now + 60`. Verify not in query results. Advance system time (or use a mock clock). Verify document appears after activation.
  - **Note:** System time mocking is tricky. Alternative: use a configurable `now_fn` in the engine, or insert with `publishedAt = 1` (past) and verify immediate visibility, then insert with `publishedAt = u64::MAX` (far future) and verify invisibility.
- **Test: bucket snapping through engine.** Configure `publishedAt` with 24h and 7d buckets. Insert documents with various timestamps. Query with `publishedAt > now - 7d`. Verify the query uses the bucket bitmap (observable via query plan or cache key stability).
- **Test: refresh cycle correctness.** Insert documents, trigger bucket refresh, insert more documents. After next refresh, bucket should include new documents.

**Acceptance criteria:**
- Deferred alive documents are invisible until activation time.
- Bucket snapping produces correct query results.
- Bucket refresh captures newly inserted documents.

**Dependencies:** S3.1 through S3.6

---

## Stage 4: Query-Path Optimization

**Addresses:** D3, D7, E4, B1, B3, B4
**Dependencies:** Stage 1 (diff model needed for D7), Stage 3 (stable cache keys needed for bound formation)
**Estimated Complexity:** MEDIUM
**Primary files:** `src/concurrent_engine.rs`, `src/executor.rs`, `src/planner.rs`, `src/bound_cache.rs`, `src/meta_index.rs`, `src/cache.rs`

### Tasks

#### S4.1: Add filter definition check in D3 live maintenance

**Files:** `src/concurrent_engine.rs`
**What to change:**
- In the flush thread's bound cache live maintenance (lines 232-269), before `entry.add_slot(slot)`:
  1. Retrieve the bound's filter definition (the `filter_key` from the `BoundKey`).
  2. Check if the mutated slot passes the bound's filter predicate by evaluating the slot against the filter clauses from the staging snapshot.
  3. Only call `entry.add_slot(slot)` if the slot passes the filter check.
- **Implementation detail:** The bound's `filter_key` is a canonical string of filter clauses. We need to reconstruct the filter clauses from the key, or store them alongside the bound. The `BoundCacheManager` should store the original `Vec<FilterClause>` alongside each bound entry.
- Add a `filter_clauses: Vec<FilterClause>` field to the bound cache entry struct (or to `BoundKey`).
- In `form_bound()`, store the filter clauses.
- In the flush thread, use `executor.slot_matches_filters(slot, &entry.filter_clauses())` to check qualification.
  - **Caveat:** The flush thread does not have a full `QueryExecutor`. Instead, check filter membership directly against the staging `FilterIndex`: for each clause in the bound's filter definition, check if the slot's bit is set in the corresponding filter bitmap.

**Acceptance criteria:**
- A document with `nsfwLevel=2` and high `reactionCount` is NOT added to a bound for `nsfwLevel=1`.
- A document that passes the bound's filter predicate IS added.
- Existing bound cache tests still pass.

**Dependencies:** Stage 1 (write path must correctly handle diffs for filter check accuracy)

#### S4.2: Wire MetaIndex `find_matching_entries()` into query path

**Files:** `src/concurrent_engine.rs`, `src/executor.rs`, `src/planner.rs`
**What to change:**
- In `ConcurrentEngine::apply_bound()` (lines 813-878), replace the direct `BoundKey` HashMap lookup with MetaIndex-based superset matching:
  1. Use `MetaIndex::find_matching_entries()` to find all bounds whose filter definition is a subset of (or equal to) the current query's filter clauses.
  2. Among matching bounds, select the one with the tightest filter (most clauses) to minimize working set.
  3. Fall through to existing BoundKey lookup as a fallback if MetaIndex returns no matches.
- This enables bound reuse: a bound for `nsfwLevel=1` can be used when querying `nsfwLevel=1 AND onSite=true` (the query is a superset of the bound's filter).

**Acceptance criteria:**
- A query with filters `{A, B}` can use a bound formed for filter `{A}` (superset matching).
- A query with filter `{A}` still uses an exact-match bound for `{A}`.
- The tightest matching bound is preferred over a looser one.
- Query results remain correct (bound is still ANDed with full filter result).
- MetaIndex integration test verifies the full path: form bound for filter X, query with filter X+Y, bound is used.

**Dependencies:** None (MetaIndex primitives already work)

#### S4.3: Clean up invalidation model (D7)

**Files:** `src/cache.rs`, `src/concurrent_engine.rs`
**What to change:**
- **Decision: document the dual-system design rather than unify.** The trie cache uses generation-counter lazy invalidation (for filter result caching). The bound cache uses live maintenance (for sort working set caching). These serve different purposes:
  - Trie cache: exact bitmap results for filter combinations. Invalidated when filter bitmaps change. Generation counters are cheap and correct.
  - Bound cache: approximate sort bounds. Maintained live from writes. Rebuilt on bloat.
- Add code comments documenting this intentional dual design in `src/cache.rs` and `src/concurrent_engine.rs`.
- If generation counters should be removed in the future, add a TODO comment referencing the roadmap.

**Acceptance criteria:**
- Both invalidation systems are documented with rationale.
- No behavioral changes (correctness preserved).

**Dependencies:** None

#### S4.4: Wire `skip_sort` into executor or remove dead code (B3)

**Files:** `src/executor.rs`, `src/planner.rs`
**What to change:**
- **Option A (recommended):** Remove the `skip_sort` field from `QueryPlan`. The executor already handles no-sort queries via `sort: Option<&SortClause>` matching (when sort is `None`, `slot_order_paginate` is used). The `skip_sort` flag is redundant.
- **Option B:** Wire `skip_sort` into the executor so that `execute_from_bitmap()` respects it. This would mean even when `sort` is `Some`, if `skip_sort` is true, use slot-order pagination.
- Option A is cleaner -- the planner's decision to skip sort should be expressed by the caller passing `sort: None`, not by a flag.

**Acceptance criteria:**
- No dead `skip_sort` field in `QueryPlan`.
- All existing tests pass.
- No behavioral regression.

**Dependencies:** None

#### S4.5: Add ascending slot-order direction (B1 gap)

**Files:** `src/executor.rs`
**What to change:**
- In `slot_order_paginate()`, add a `direction: SortDirection` parameter (or an `ascending: bool` flag).
- When ascending, iterate the bitmap in forward order using roaring's default iterator.
- When descending (current default), iterate in reverse using `RoaringBitmap::iter().rev()` or `select`/`rank` operations.
- Wire the direction through `execute_from_bitmap()` -- the caller can pass a `SortDirection` for no-sort queries.
- Default remains descending (newest-first) when no sort is specified.

**Acceptance criteria:**
- Ascending slot-order pagination returns oldest-first results.
- Descending slot-order pagination returns newest-first results (no regression).
- Cursor pagination works correctly in both directions.

**Dependencies:** None

#### S4.6: Add missing edge-case tests (B4, E6)

**Files:** `src/executor.rs` (test module), `tests/edge_cases.rs` (new)
**What to change:**
- **B4 tests:**
  - Ascending slot-order pagination (after S4.5).
  - Empty page: cursor beyond all results returns empty IDs with no next cursor.
  - Single result page with correct cursor emission.
  - Cursor field value assertions (sort_value correctness).
- **E6 test:**
  - `find_matching_entries()` invoked via the query path (after S4.2).
  - Form bound for filter `{nsfwLevel=1}`, query with `{nsfwLevel=1, onSite=true}`, verify bound is used via superset matching.

**Acceptance criteria:**
- All edge-case tests pass.
- No panics on empty result sets or out-of-range cursors.

**Dependencies:** S4.2 (for E6 test), S4.5 (for B4 ascending test)

---

## Benchmarking Protocol

### After Each Stage

1. **Create 3 worktrees:**
   - `baseline` -- the current `main` branch (pre-fix).
   - `stage-N` -- the branch after completing Stage N.
   - (Optionally) `stage-N-1` -- the previous stage for regression comparison.

2. **Port benchmark binary:** Copy `src/bin/benchmark.rs` and its dependencies to each worktree so all three use identical benchmark code. This ensures differences reflect engine changes, not benchmark changes.

3. **Run 5M benchmark sequentially** on each worktree:
   ```bash
   cargo run --release --bin benchmark -- --records 5000000 --threads 4 --remap-ids --stages insert,query
   ```
   Run one at a time to avoid contamination from concurrent disk/CPU usage.

4. **Compare:**
   - Insert throughput (docs/sec)
   - Query latency (p50, p95 for each query type)
   - Bitmap memory
   - RSS

5. **Regression threshold:** >10% degradation in any metric flags the stage for investigation.

### After All Stages

6. **Full 104M load with persistent data:**
   ```bash
   cargo run --release --bin benchmark -- --records 104600000 --threads 4 --remap-ids --stages insert,query --keep-data
   ```
   The `--keep-data` flag (to be added) prevents deleting the redb directory after the benchmark.

7. **Restart test:**
   ```bash
   cargo run --release --bin benchmark -- --stages query --data-dir ./benchmark_data
   ```
   Reopen from the persisted redb data (no insert phase). Verify all 20 query types return results. Compare query latencies to the pre-shutdown run.

8. **Memory validation:**
   - At 104M: bitmap memory should be ~6.5GB (Tier 1) or ~3-4GB (with Tier 2 caching for tagIds/userId).
   - RSS should be within 50% of bitmap memory.
   - Pending buffer depth should stabilize (not grow unbounded).

---

## Agent Team Structure

### Agent Assignment

#### Agent 1: Write-Path Agent
**Scope:** Stage 1 (S1.1-S1.6) + Stage 2 persistence core (S2.1-S2.3)
**Owned files:**
- `src/write_coalescer.rs` -- exclusive
- `src/concurrent_engine.rs` -- flush thread + merge thread + build method (shared with Agent 2 for query path)
- `src/bitmap_store.rs` -- exclusive
- `src/slot.rs` -- restore_from method
- `src/sort.rs` -- load_layers method

**Responsibilities:**
- Stop eager filter merging (S1.1)
- Move filter compaction to merge thread (S1.4)
- Add persistence for alive, sort, slot counter (S2.1-S2.3)

#### Agent 2: Read-Path Agent
**Scope:** Stage 1 read path (S1.2-S1.3) + Stage 4 query optimization (S4.1-S4.5)
**Owned files:**
- `src/filter.rs` -- exclusive
- `src/executor.rs` -- exclusive
- `src/planner.rs` -- exclusive
- `src/bound_cache.rs` -- D3 filter check, D6 LRU (S2.5)
- `src/meta_index.rs` -- E4 wiring

**Responsibilities:**
- Add diff-aware filter methods (S1.2)
- Implement diff fusion in executor (S1.3)
- Add filter definition check in live maintenance (S4.1)
- Wire MetaIndex superset matching (S4.2)
- Add ascending slot-order (S4.5)

#### Agent 3: Integration + Time Agent
**Scope:** Stage 2 integration (S2.4-S2.9) + Stage 3 time handling (S3.1-S3.7)
**Owned files:**
- `src/tier2_cache.rs` -- eviction listener (S2.4)
- `src/mutation.rs` -- deferred alive op
- `src/time_buckets.rs` -- persistence wiring
- `src/config.rs` -- bound_max_count, any config additions
- `tests/` -- all integration test files

**Responsibilities:**
- moka eviction listener (S2.4)
- Restart integration test (S2.7)
- Tier 2 integration test (S2.8)
- All time handling wiring (S3.1-S3.7)
- Memory benchmark (S2.9)

#### QA Agent
**Scope:** S1.6-S1.8, S4.3, S4.4, S4.6, benchmarking protocol
**Owned files:**
- `src/versioned_bitmap.rs` -- proptest only (S1.7)
- Test modules in existing files (S1.6, S1.8)
- `tests/edge_cases.rs` (S4.6)
- `docs/` -- benchmark results, code comments (S4.3)

**Responsibilities:**
- WriteBatch test updates (S1.6)
- VersionedBitmap proptest (S1.7)
- Integration tests for diff accumulation (S1.8)
- Dead code cleanup (S4.4)
- Invalidation model documentation (S4.3)
- Edge-case tests (S4.6)
- Run benchmark protocol after each stage

### Integration Points (Coordination Required)

| Point | Agents | Coordination |
|-------|--------|-------------|
| `FilterField` API changes | Agent 1 (write) + Agent 2 (read) | Agent 2 adds `get_versioned()`, `apply_diff_eq()`, `union_with_diff()`. Agent 1 uses `merge_dirty()`. Methods must not conflict. |
| `concurrent_engine.rs` flush thread | Agent 1 (write) + Agent 3 (time) | Agent 1 owns the diff model changes. Agent 3 adds deferred alive and bucket refresh. Non-overlapping code regions. |
| `concurrent_engine.rs` merge thread | Agent 1 (persistence) + Agent 3 (time) | Agent 1 adds Tier 1 persistence. Agent 3 adds bucket persistence. Merge in separate code blocks. |
| `concurrent_engine.rs` query path | Agent 2 (optimization) + Agent 3 (time) | Agent 2 modifies `apply_bound()`. Agent 3 modifies `query()`/`execute_query()` to wire time buckets. Non-overlapping. |
| Integration tests | All agents + QA | QA owns test files. Other agents provide test specifications. |

### Execution Order

```
Week 1:  S1.1-S1.4 (Agent 1+2 in parallel, core diff model)
         S1.7 (QA, proptest -- independent)
         S2.1 (Agent 1, persistence API -- independent)
         S2.5-S2.6 (Agent 3, LRU + metrics -- independent)

Week 2:  S1.5-S1.6 (QA, verification + test updates)
         S1.8 (QA, integration tests)
         S2.2-S2.3 (Agent 1, Tier 1 persistence)
         S2.4 (Agent 3, eviction listener)
         S4.3-S4.4 (QA, cleanup)
         Benchmark: Stage 1 5M comparison

Week 3:  S2.7-S2.8 (Agent 3, integration tests)
         S2.9 (Agent 3, memory benchmark)
         S3.1-S3.4 (Agent 3, time handling core)
         S4.1-S4.2 (Agent 2, query optimization)
         Benchmark: Stage 2 5M comparison

Week 4:  S3.5-S3.7 (Agent 3, time handling completion)
         S4.5-S4.6 (Agent 2 + QA, edge cases)
         Benchmark: Stage 3 + Stage 4 5M comparison
         Benchmark: Full 104M load + restart test
```

---

## Risk Mitigation

### Stage 1 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Filter diff fusion introduces correctness bugs | Medium | HIGH | Proptest (S1.7) catches edge cases. Integration test (S1.8) verifies end-to-end. Run ALL existing tests after every commit. |
| Performance regression from diff fusion overhead | Low-Medium | Medium | apply_diff is 3 bitmap ops on small diffs (KB). Benchmarked at 5M to verify. Sort layers unaffected (still merged eagerly). |
| Merge thread publishes stale snapshot (race with flush) | Low | HIGH | The merge thread's published snapshot may be immediately overwritten by the flush thread. This is correct by design -- both produce valid snapshots. Verify with concurrent test (S1.8). |
| Context overflow during implementation | Medium | HIGH | **Commit after every task completion (S1.1, S1.2, etc.).** Agent work is lost on context overflow if uncommitted. |

### Stage 2 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| redb lock conflict (STATUS_HEAP_CORRUPTION) | Medium | HIGH | Always kill stale `benchmark.exe` processes before running. Use separate redb files for docstore vs bitmap store. |
| redb write takes too long, blocking merge thread | Low | Medium | Cap write batch size. At 5M, ~120MB total -- ~40ms on NVMe. Monitor merge cycle duration. |
| Restart loads stale data (merge was in progress at shutdown) | Low | Medium | redb transactions are atomic. Partial writes are rolled back. Restart sees the last committed state. |
| Eviction listener deadlock with merge thread | Low | HIGH | Both access PendingBuffer via Mutex. Ensure consistent lock ordering. Eviction listener holds PendingBuffer lock briefly (single key drain). |

### Stage 3 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| System clock mocking for deferred alive tests | Medium | Medium | Use injected `now_fn` or test with extreme timestamps (past/far-future) to avoid clock dependency. |
| Bucket refresh under heavy write load | Low | Medium | Bucket refresh runs on merge thread, which is low-priority. Large rebuilds (~100ms for alive bitmap scan) are acceptable on 5-second intervals. |

### Stage 4 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Filter definition check in D3 adds flush thread latency | Low-Medium | Medium | The check is O(filter clauses * 1 bitmap lookup per clause). For typical queries (2-3 clauses), this is microseconds per slot. Only runs for slots that pass the sort value threshold. |
| MetaIndex superset matching returns incorrect bounds | Low | Medium | The bound is ANDed with the full filter result, so false positives cannot corrupt query results. They only waste memory. Integration test (S4.6) verifies correctness. |

### Cross-Cutting

- **Commit after every task.** Context overflow kills uncommitted work. Agents must commit with descriptive messages after completing each numbered task (S1.1, S1.2, etc.).
- **Run `cargo test` after every commit.** Catch regressions immediately. Do not batch multiple tasks without testing.
- **Avoid file conflicts between agents.** The agent assignment table above specifies file ownership. When two agents must modify the same file (e.g., `concurrent_engine.rs`), they must work on non-overlapping regions and coordinate merge order.
- **Benchmark numbers vary 2-3x with system load.** Always compare runs from the same session. Run benchmarks sequentially, not in parallel.
