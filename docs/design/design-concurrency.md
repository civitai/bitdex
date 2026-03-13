---
status: IMPLEMENTED
created: 2026-02-19
updated: 2026-03-13
---

# Concurrency Architecture

This document describes the concurrency architecture of Bitdex V2 as implemented in the codebase. All source references use relative paths from the repository root.

## 1. Overview

Bitdex V2 uses a lock-free read path with batched, channel-driven writes. The core pattern:

- **Readers** load immutable snapshots via `ArcSwap::load()` -- zero contention, no locks, no atomic refcount ops on the hot path.
- **Writers** compute diffs (old doc vs new doc) and send `MutationOp` messages through a bounded crossbeam channel. A single flush thread drains the channel, applies batched mutations to a private staging copy, and atomically publishes a new snapshot via `ArcSwap::store()`.
- **Persistence** is handled by a separate merge thread that periodically snapshots the published state and writes it to the filesystem via BitmapFs.

Key source files:

| File | Role |
|---|---|
| `src/concurrent_engine.rs` | `ConcurrentEngine` struct, flush thread, merge thread, lazy loading, query dispatch |
| `src/write_coalescer.rs` | `MutationOp` enum, `WriteBatch` grouping, `WriteCoalescer`, `MutationSender` |
| `src/versioned_bitmap.rs` | `VersionedBitmap` with base + diff layers, CoW via `Arc::make_mut()` |
| `src/concurrency.rs` | `InFlightTracker` for write-read overlap detection |

## 2. Snapshot Architecture

The published bitmap state lives in `ArcSwap<InnerEngine>`:

```
inner: Arc<ArcSwap<InnerEngine>>
```

`InnerEngine` (`src/concurrent_engine.rs`, line ~56) contains:

- `slots: SlotAllocator` -- alive bitmap, slot counter, clean bitmap
- `filters: FilterIndex` -- all filter fields (each containing `HashMap<u64, VersionedBitmap>`)
- `sorts: SortIndex` -- all sort fields (each containing `Vec<VersionedBitmap>` bit layers)

**Read path:** `snapshot()` calls `self.inner.load()`, returning a zero-cost `Guard<Arc<InnerEngine>>`. The Guard dereferences to the snapshot without incrementing any atomic refcount. Old snapshots are deallocated by the flush thread's next `store()` call, not by readers.

**Write path:** The flush thread owns a private `staging: InnerEngine` clone. After applying mutations, it calls `inner.store(Arc::new(staging.clone()))`. The `clone()` is cheap because of Arc-per-bitmap CoW (see section 3).

## 3. Arc-per-Bitmap CoW

Three levels of Arc wrapping enable O(num_fields) snapshot clones:

1. **`Arc<RoaringBitmap>`** -- Each bitmap (base) inside `VersionedBitmap` is Arc-wrapped. `VersionedBitmap::merge()` uses `Arc::make_mut(&mut self.base)` which only clones the bitmap data when `strong_count > 1` (i.e., when a published snapshot still holds a reference). When `strong_count == 1`, mutation is in-place with no allocation (`src/versioned_bitmap.rs`, lines 226-235).

2. **`Arc<FilterField>` / `Arc<SortField>`** -- Each field in the filter/sort indexes is Arc-wrapped. Cloning `FilterIndex` or `SortIndex` bumps per-field refcounts without touching the bitmaps inside them.

3. **`Arc<BitmapDiff>`** -- The diff layer in each `VersionedBitmap` is also Arc-wrapped, so snapshot clones share diff state until a mutation forces a CoW via `Arc::make_mut(&mut self.diff)`.

The net result: `staging.clone()` copies a handful of Arc pointers (one per field, plus one per bitmap that was mutated since the last publish). At 104M records with ~15 fields, this is microseconds.

## 4. VersionedBitmap

`VersionedBitmap` (`src/versioned_bitmap.rs`) is the core data structure for all bitmaps in the system. It separates the last-compacted state from pending mutations:

```
struct VersionedBitmap {
    base: Arc<RoaringBitmap>,     // last-compacted state
    diff: Arc<BitmapDiff>,        // pending sets + clears
    generation: u64,              // bumped on each merge
    is_loaded: bool,              // false = unloaded placeholder
}
```

**BitmapDiff** tracks `sets: RoaringBitmap` and `clears: RoaringBitmap` separately. These are mutually exclusive per bit -- inserting a bit removes it from clears, and vice versa (lines 31-40).

### Mutation

All mutations go through the diff layer:

- `insert(bit)` -- `Arc::make_mut(&mut self.diff).insert(bit)` (removes from clears, adds to sets)
- `remove(bit)` -- `Arc::make_mut(&mut self.diff).remove(bit)` (removes from sets, adds to clears)
- `or_into_base(bitmap)` -- bulk OR directly into base, bypassing diff. Used during `put_bulk()` initial loading where the staging copy is private.

### Query-Time Fusion

Three methods fuse base + diff without materializing a full merged bitmap:

- **`apply_diff(candidates)`** -- the hot path. Intersects candidates with base, ORs in candidates AND diff.sets, subtracts diff.clears. Result size is bounded by `|candidates|`, not `|base|` (lines 151-156).
- **`fused()`** -- returns a fully materialized `base | sets - clears`. Used when no candidate set is available (single-clause evaluation).
- **`fused_cow()`** -- returns `Cow::Borrowed(&base)` when the diff is empty (zero-copy), `Cow::Owned(merged)` when dirty. Used for serialization to BitmapFs.

### Compaction

`merge()` folds the diff into the base via `Arc::make_mut()`, replaces the diff with an empty `Arc<BitmapDiff>`, and bumps the generation counter. Merge is blocked when `is_loaded == false` to prevent compacting diffs against an empty placeholder (lines 226-235).

## 5. Flush Thread

The flush thread (`src/concurrent_engine.rs`, spawned at line ~371) is a single background thread that owns the private `staging: InnerEngine`. Its loop:

1. **Sleep** with adaptive backoff: starts at `flush_interval_us` (configurable), doubles on idle up to `flush_interval_us * 10`, resets to minimum when work is done.

2. **Phase 1: Drain channel** -- `coalescer.prepare()` calls `WriteBatch::drain_channel()` (non-blocking `try_recv` loop), then `group_and_sort()` which groups ops by target bitmap and sorts slot IDs within each group for optimal roaring-rs `extend()` performance.

3. **Phase 1b: Drain lazy load channel** -- Applies any field data loaded by query threads via `ensure_fields_loaded()` to keep staging in sync with published snapshots.

4. **Phase 2: Apply mutations** -- `coalescer.apply_prepared()` applies grouped mutations to `staging.slots`, `staging.filters`, `staging.sorts`. Filter removes are applied before inserts, and sort clears before sets, to handle upsert diff semantics correctly (see `WriteBatch::apply()` at line ~259 in `write_coalescer.rs`).

5. **Maintenance** (skipped in loading mode):
   - **Deferred alive activation** -- slots with `DeferredAlive` ops whose `activate_at` timestamp has passed get their alive bits set.
   - **Positive existence set updates** -- new distinct values in per-value lazy-loading fields are added to their `ArcSwap<HashSet<u64>>`.
   - **Time bucket live maintenance** -- newly alive/deleted slots are added to/removed from qualifying time buckets.
   - **Unified cache maintenance** -- targeted invalidation: removes deleted slots from all cache entries, updates entries affected by filter/sort mutations. Sort-only flushes with no filter changes skip filter cache invalidation.
   - **Periodic compaction** -- every 50 flush cycles, merges all dirty filter diffs into their bases to keep diff layers small.
   - **Idle eviction sweeps** -- at configurable intervals, evicts per-value filter bitmaps that haven't been queried within their `idle_seconds` window (wall-clock based).
   - **Time bucket refresh** -- rebuilds time-range bitmaps whose validity has expired (time-based, not mutation-based).

6. **Publish snapshot** -- `inner.store(Arc::new(staging.clone()))` makes the new state visible to readers atomically.

7. **Phase 3: Docstore batch write** -- drains the docstore channel and writes documents in batch via `docstore.lock().put_batch()`.

On shutdown, the flush thread performs a final drain-apply-compact-publish cycle and a final docstore drain.

## 6. Merge Thread

The merge thread (`src/concurrent_engine.rs`, spawned at line ~819) handles periodic persistence:

1. Sleeps for `merge_interval_ms` (configurable).
2. Checks the `dirty_since_snapshot` `AtomicBool` flag. If no mutations have occurred since the last snapshot, skips the write entirely (prevents continuous ~20GB rewrites at idle).
3. Loads the current published snapshot via `ArcSwap::load_full()` (atomic refcount bump, safe to use from a non-flush thread).
4. Clones and compacts all filter diffs (`merge_dirty()` on each field).
5. Skips fields that are still pending lazy load (empty placeholders) or per-value lazy fields to avoid overwriting real data on disk.
6. Writes the full snapshot to BitmapFs via `write_full_snapshot()`: filter bitmaps, alive bitmap, sort layer bitmaps, slot counter.
7. Persists time bucket bitmaps and named cursors (e.g., pg-sync replication progress).

The merge thread reads from the published snapshot, not from staging. This means it never contends with the flush thread.

## 7. Loading Mode

Loading mode is a binary toggle (`AtomicBool`) for bulk insert throughput:

```rust
pub fn enter_loading_mode(&self)   // sets loading_mode = true
pub fn exit_loading_mode(&self)    // sets loading_mode = false
```

When active, the flush thread skips:

- Snapshot publishing (`inner.store(...)`)
- All maintenance: time buckets, cache maintenance, compaction, eviction
- The `staging.clone()` call that triggers the Arc refcount cascade

This eliminates the dominant cost at scale: `staging.clone()` bumps refcounts on every Arc-wrapped bitmap. With 104M records and ~15 fields, this means thousands of `Arc::make_mut()` calls on the next mutation cycle would deep-clone every HashMap in every FilterField, turning each flush into a multi-millisecond operation.

**On exit:** The flush thread detects the `was_loading && !is_loading` transition, compacts all filter diffs, clears the unified cache (stale from loading), and force-publishes the staging state.

Positive existence set updates still run during loading mode so that per-value lazy loading works correctly for queries that arrive during bulk loading.

## 8. Lazy Bitmap Loading

At startup, only the alive bitmap and slot counter are loaded eagerly from BitmapFs (always needed, tiny). All filter and sort field bitmaps are deferred.

**Pending sets** (`src/concurrent_engine.rs`, lines ~87-91):

- `pending_filter_loads: Arc<Mutex<HashSet<String>>>` -- single_value and boolean fields awaiting full-field load
- `pending_sort_loads: Arc<Mutex<HashSet<String>>>` -- sort fields awaiting layer load
- `lazy_value_fields: Arc<Mutex<HashSet<String>>>` -- multi_value fields (e.g., `tagIds`) that use per-value loading and are never "fully loaded"

**`ensure_fields_loaded()`** (line ~1245) is called at the start of every `query()` call:

1. **Fast path:** if all pending sets are empty and no lazy value fields exist, returns immediately (two mutex checks, nanoseconds).
2. **Collect needed fields** from the query's filter clauses and sort field.
3. **Stamp eviction** for queried multi_value values (wall-clock millis in `DashMap`).
4. **Load from BitmapFs:** reads bitmap data from disk for needed fields/values.
5. **Per-value filtering:** for lazy value fields, only values not already in the snapshot AND present in the positive existence set are loaded. The existence set (`ArcSwap<HashSet<u64>>`) is built from `.fpack` file headers at startup -- checking it is ~22 microseconds vs 30-50ms for a disk miss.
6. **Publish immediately:** clones the current snapshot, applies loaded data, publishes via `ArcSwap::store()`. Also sends loaded data to the flush thread's lazy load channel so staging stays in sync.

**Unloaded VersionedBitmaps:** When a field is unloaded, its entries become `VersionedBitmap::new_unloaded()` -- empty base with `is_loaded = false`. Mutations still accumulate in the diff layer. `merge()` is a no-op while unloaded, preventing diff compaction against an empty placeholder. On reload via `load_base()`, the persisted bitmap is OR'd into the base, and subsequent `merge()` calls fold in the accumulated diffs.

## 9. In-Flight Tracking

`InFlightTracker` (`src/concurrency.rs`) uses a `DashSet<u32>` to track slot IDs currently being mutated:

```
put(id, doc):
  1. in_flight.mark_in_flight(id)          // DashSet::insert
  2. ... compute diff, send ops to channel ...
  3. in_flight.clear_in_flight(id)          // DashSet::remove
```

**Reader post-validation** (`post_validate()`, line ~2044): after executing a query against the snapshot, checks if any result IDs overlap with the in-flight set. If there is overlap, those IDs are re-evaluated against a fresh snapshot to ensure consistency. The fast path (`has_in_flight() == false`) skips this entirely -- no atomic reads, no set intersection.

This is a lightweight optimistic concurrency scheme: the common case (no overlap) adds zero cost to reads. The rare case (read hits an in-flight write) pays a small re-evaluation cost for only the overlapping IDs.

## 10. Cache Concurrency

The unified query cache is separate from the ArcSwap snapshot:

```
unified_cache: Arc<parking_lot::Mutex<UnifiedCache>>
```

This separation is deliberate: the cache holds pre-computed query results (filtered + sorted bitmaps) that span multiple fields. Embedding it in the snapshot would cause it to be cloned on every publish.

**Lock characteristics:**

- Locks are brief: cache lookup is a trie traversal (microseconds), cache store inserts a single entry (microseconds).
- The flush thread holds the lock only during targeted maintenance operations (slot removal from entries, filter/sort mutation propagation).
- Query threads hold the lock only for lookup and store -- bitmap computation happens outside the lock.

**Targeted invalidation:**

- When a flush cycle contains only sort mutations (no filter changes), filter cache entries are not invalidated. Sort is applied after cache lookup, so filter-only cache entries remain valid.
- When filter fields change, only cache entries containing the changed fields are invalidated via the unified cache's maintenance methods.
- When slots are deleted, they are surgically removed from all cache entries (`remove_slot_from_all`).
- On loading mode exit, the entire cache is cleared (accumulated mutations may have made any entry stale).
