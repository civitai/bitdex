# Per-Value Lazy Writes — Design Doc

**Status:** Draft for review (Justin + Gemini + GPT)
**Author:** Ivy
**Date:** 2026-04-07

## Problem statement

Today, mutations to a high-cardinality filter field (e.g. `postId` with 22.5 M
distinct values) require the entire field to be **loaded into memory** as a
`HashMap<u64, VersionedBitmap>`. Concretely:

1. A query touches `postId` → triggers `ensure_fields_loaded` → spawns the
   lazy loader → reads the entire field from disk (~12 seconds for postId at
   109 M scale) → ships the result back to the flush thread via `lazy_tx`.
2. The flush thread calls `field.load_field_complete(bitmaps)` which holds
   the FilterField write lock for ~1.5 s while inserting 22.5 M HashMap
   entries.
3. While the write lock is held, concurrent queries on `postId` block.
4. Once loaded, every mutation requires the field to STAY loaded — `postId`
   then occupies ~1 GB of RAM.

This was the cause of the wedge during the v1.0.144 prod incident and is the
fundamental reason the band-aid fixes (chunked load, bounded drain, RwLock vs
Mutex) keep playing whack-a-mole. **Loading 22.5 M values into memory just to
flip 2 bits is the bug. We should never have done this.**

## Insight from Justin

ShardStore is already shard-keyed and supports `append_ops(key, ops)` which
writes an op to a shard's ops log on disk **without loading the shard's
snapshot into memory**. The read path replays the ops log over the snapshot
on `read()`. This is the per-shard equivalent of WAL — already half-built.

The remaining work is to teach the *engine* to use it that way: stop
materializing entire fields into in-memory HashMaps; treat the in-memory
`FilterField.bitmaps` as a cache of *hot values only*; route mutations
through ShardStore's per-shard ops log when the value's bitmap isn't
already cached; lazily load individual values on first read.

## Goals

1. **Eliminate the multi-second `load_field_complete` cost** entirely. Loading
   a single value's bitmap should be ~ms, not the full field's ~seconds.
2. **Bound memory by working set, not total cardinality.** A field with 22 M
   values where only 1 % are queried should occupy ~1 % of the RAM it does
   today.
3. **Same persistence + crash safety guarantees** as today. WAL cursor only
   advances after on-disk state reflects the corresponding mutations. Idempotent
   replay on crash.
4. **No regression for low-cardinality fields.** `nsfwLevel` (7 values) should
   still be fully loaded eagerly because the cost is trivial.
5. **No relaxation of read correctness.** Queries against per-value-lazy fields
   must return the same results as queries against fully-loaded fields.

## Non-goals (for this work)

- Eviction policies for the in-memory cache. We currently never evict; this
  doc keeps that behavior. Eviction is a follow-up.
- Sort field per-value lazy writes. Sort fields have a fundamentally different
  shape (32 fixed bit-layers, not millions of value-keyed entries) and are
  already small enough to load fully. They're out of scope.
- Removing the staging system entirely. That's a related but separate refactor
  that can land after this one. (See "Open question: drop staging?" below.)

## High-level design

### Two field modes

```rust
pub enum FieldStorageMode {
    /// Field is small and stays fully loaded in memory.
    /// Today's behavior. Used for low-cardinality fields like nsfwLevel.
    EagerInMemory,

    /// Field is sparse-cached: only hot values live in memory. Cold values
    /// live on disk. Mutations to cold values append to the shard's ops
    /// log without loading the snapshot. First read of a cold value
    /// loads its bitmap from the shard.
    PerValueLazy,
}
```

`PerValueLazy` is opt-in via `FilterFieldConfig` and is the new default for
fields with `eager_load: false`. Existing fields keep their current behavior
unless explicitly switched.

### FilterField under PerValueLazy

```rust
pub struct FilterField {
    /// Hot value cache. Same shape as today but populated only when a value
    /// is touched, not eagerly via load_field_complete.
    bitmaps: parking_lot::RwLock<HashMap<u64, VersionedBitmap>>,

    /// Pointer to the ShardStore that backs this field's persistence.
    /// Used by mutations to write directly to the on-disk ops log when a
    /// value isn't in the cache, and by reads to lazy-load values.
    /// None for EagerInMemory fields.
    shard_store: Option<Arc<FilterShardStore>>,

    /// Set of values *known* to have on-disk presence (loaded from
    /// shard metadata at startup). Used to distinguish "missing on disk"
    /// from "missing entirely" for read paths.
    /// Approximate — see "Open question: existence tracking" below.
    on_disk_keys: Option<Arc<RwLock<RoaringBitmap>>>,

    config: FilterFieldConfig,
}
```

### Mutation path (PerValueLazy)

`field.insert_bulk(value, slots)` becomes:

```rust
pub fn insert_bulk(&self, value: u64, slots: impl IntoIterator<Item=u32>) {
    // Fast path: value is already cached in memory. Mutate in place.
    {
        let r = self.bitmaps.read();
        if r.contains_key(&value) {
            drop(r);
            let mut w = self.bitmaps.write();
            if let Some(vb) = w.get_mut(&value) {
                vb.insert_bulk(slots);
                return;
            }
            // Fall through if it was evicted between read and write
        }
    }

    // Slow path: value is not cached. Write the op to ShardStore directly.
    // No HashMap insert, no bitmap allocation, no lock contention with
    // 22 M-entry caches.
    if let Some(ref store) = self.shard_store {
        let shard_key = value_to_shard(value);
        let op = BitmapOp::Insert { value, slots: slots.into_iter().collect() };
        store.append_op(&shard_key, &op).expect("shard append");
    }
    // (Op is now durable in the shard's ops log on disk. The next read of
    // `value` will pick it up via ShardStore::read which auto-applies ops.)
}
```

`remove_bulk` is symmetric.

### Read path (PerValueLazy)

Reads use `apply_diff_eq(value, candidates)` and similar. Today it reads
from `self.bitmaps.read()`. Under per-value lazy, it becomes:

```rust
pub fn apply_diff_eq(&self, value: u64, candidates: &RoaringBitmap)
    -> Option<RoaringBitmap>
{
    // Fast path: cached
    {
        let r = self.bitmaps.read();
        if let Some(vb) = r.get(&value) {
            return Some(if vb.is_dirty() {
                vb.apply_diff(candidates)
            } else {
                candidates & vb.base().as_ref()
            });
        }
    }

    // Slow path: load from ShardStore (auto-applies pending ops on read)
    if let Some(ref store) = self.shard_store {
        let shard_key = value_to_shard(value);
        let snapshot = store.read(&shard_key).ok().flatten()?;
        // snapshot is a HashMap<u64, RoaringBitmap> for this shard's values
        let bitmap = snapshot.values.get(&value)?.clone();
        let result = candidates & &bitmap;

        // Promote to cache (single-writer optimization: ensure only one
        // thread inserts even if multiple readers race)
        {
            let mut w = self.bitmaps.write();
            w.entry(value).or_insert_with(|| VersionedBitmap::new(bitmap));
        }

        Some(result)
    } else {
        None
    }
}
```

Range scans (`for_each_versioned` callers) need a different approach since
they need to iterate ALL values, not point-lookup. Options:

A) **Iterate via ShardStore**: walk all shards, decode all values from each
   shard's snapshot+ops, yield to the closure. Cost = full field load
   spread across shards. Could be parallelized.

B) **Force-load the field** before the range scan via `ensure_value_loaded`
   for all on-disk keys. Same total cost but materialized in cache.

C) **Bound range queries on per-value-lazy fields.** Document that range
   filters on `postId`-class fields are O(field size) and discouraged.

Recommendation: **A** (iterate via ShardStore directly), keep range scans
correct but slow. They're rare in our workload.

### Persistence + crash safety (PerValueLazy)

Today: WAL writes → in-memory mutation → merge thread persists → cursor
advances.

Under per-value lazy:
- Mutations append directly to ShardStore's per-shard ops log on disk.
  This IS the persistence step. No separate merge needed.
- The shard's `compact()` is called periodically by the existing background
  compaction logic to roll the ops log into the snapshot, bounding ops log
  growth. Same as today.
- WAL cursor advances after the ShardStore append returns. The op is
  durable on disk before the cursor moves.
- On crash: WAL has the op (already fsync'd). ShardStore has the op
  (already fsync'd at least up to the point of crash). Replay is
  idempotent because each op carries a `(value, slot)` and applying it
  twice produces the same result.

**This is actually STRONGER than today's invariant.** Today, the ops live
in the in-memory HashMap until the merge thread runs (every 5 s). Under
per-value lazy, the ops are on disk immediately.

### Concurrency (per-shard write serialization)

ShardStore::append_op is documented as not thread-safe for concurrent writes
to the same shard. We need single-writer access per shard. Options:

A) **Per-shard mutex** (`HashMap<ShardKey, Mutex<()>>`). Writers acquire
   the relevant shard's mutex before append. ~256 mutexes for the existing
   shard scheme. Cheap.

B) **Single global writer queue** keyed by shard. A pool of N writer
   threads each handle a partition of shards.

C) **Write serialization at the FilterField level** — same as the current
   write lock, just cheaper because it's per-shard granularity.

Recommendation: **A** (per-shard mutex). Simplest. Matches ShardStore's
existing assumptions. Concurrency is bounded by number of shards (~256)
which exceeds CPU count, so contention is minimal.

## Open questions

### 1. Existence tracking (`on_disk_keys`)

The read path needs to distinguish "value not in cache, might be on disk"
from "value doesn't exist anywhere". Options:

A) **Always check ShardStore on cache miss.** Costs one disk seek + read
   per cold lookup. Acceptable for our workload (lookups are rare and
   bounded by query semantics).

B) **Maintain an in-memory `RoaringBitmap` of all known on-disk keys**,
   loaded from shard metadata at startup, updated on writes. Smaller
   than the full HashMap (~128 MB for 22 M postId values vs ~1 GB).

C) **Bloom filter.** False-positive lookups go to disk and miss. False
   negatives are not allowed (we'd return wrong results), so the bloom
   must be sized for ~0% false-negative rate with periodic rebuilds.

Recommendation: **B** (existence bitmap). Memory overhead is manageable
and lookups stay O(1). The bitmap is a flat structure with no per-VB
overhead.

### 2. Cache eviction

We currently never evict. Under per-value lazy, the cache will only
contain values that have been *touched*. For our workload that's a much
smaller working set than the field cardinality. Evidence: even with no
eviction, memory should drop dramatically.

**Recommendation:** No eviction in v1. Add eviction in v2 if memory growth
is observed.

### 3. Range scans

`executor.rs::range_scan` iterates all values via `for_each_versioned`.
Under per-value lazy, this would have to load from ShardStore for every
value. For postId at 22.5 M values that's slow.

**Mitigation 1:** range scans on per-value-lazy fields trigger a one-time
"fully load this field" path that loads everything into the cache, then
operates on the in-memory snapshot. Reverts to today's behavior for that
single query.

**Mitigation 2:** document that range scans on per-value-lazy fields are
expensive and should be avoided.

In practice we don't currently issue range scans on `postId` so this is
mostly theoretical.

**Recommendation:** Mitigation 2 (document). Add Mitigation 1 if needed.

### 4. Drop the staging system?

If FilterField mutations go directly to ShardStore, the flush thread no
longer needs to apply them. The whole "staging InnerEngine + publish via
ArcSwap" becomes vestigial for filter fields.

The staging system still has uses:
- Slot allocator state (alive bitmap, slot counter, deferred activations)
- Sort field eager merge (we kept this)
- Cache state and time buckets
- Atomic visibility of new fields added via PATCH /config

**Recommendation:** Keep the staging system for now, but note that the
flush thread's apply path becomes much simpler. Removing staging entirely
is a follow-up that can wait until we've validated per-value lazy in
production.

### 5. Backpressure

ShardStore::append_op does fsync per call. For high-volume ops streams,
this could become disk-bound. Today the WAL fsync is the only disk write
on the hot path; under per-value lazy, every op also does a shard ops log
fsync.

**Mitigation:** Batch ops by shard before fsync. The CoalescerSink already
buffers ops; we'd add a "flush per shard" pass that calls
`shard_store.append_ops(shard_key, &batched_ops)` for each unique shard,
fsyncing once per shard instead of once per op.

**Recommendation:** Implement batched per-shard append from day one.

### 6. Sort fields

Out of scope for this design — sort fields have 32 bit_layers per field, not
millions of value-keyed entries. They're already small enough to load fully.
The lazy fuse work (SortField::layer_fused) handles sort field perf.

## Implementation plan (estimate)

| Phase | Work | Estimate |
|-------|------|----------|
| 1 | Add `FieldStorageMode` to FilterFieldConfig + plumb through engine startup. | 1 h |
| 2 | Implement `value_to_shard` mapping + per-shard `Mutex` table. | 1 h |
| 3 | Refactor `FilterField::insert_bulk` / `remove_bulk` to route to ShardStore on cache miss. | 2 h |
| 4 | Refactor read path (`apply_diff_eq`, `union_with_diff`, etc.) to lazy-load on cache miss. | 2 h |
| 5 | Build the on-disk-keys existence bitmap loader at startup. | 1 h |
| 6 | Add focused unit tests: cold value insert + read, cold value remove, mixed cached/cold reads. | 2 h |
| 7 | Replay validation against the captured 791 MB caplog at 109 M scale. | 1 h |
| 8 | E2E tests: persistence after restart, crash recovery, query result equivalence. | 1 h |

**Total: ~11 hours of focused implementation + validation work.**

## Validation criteria (must pass before commit)

1. **Apply path latency:** unchanged or better. Current best is ~22 μs in
   isolated test. Target: same.
2. **Memory:** RSS for the 109 M dataset drops measurably for postId-class
   fields. Target: postId in-memory size goes from ~1 GB to working-set
   size (probably <100 MB).
3. **Persistence:** replay → kill server → restart → verify last applied
   ops are visible in queries. Cursor invariant intact.
4. **Query correctness:** replay tool's per-request CSV shows ≥99 %
   match against pre-change behavior on the same caplog. (Allows for
   eventual-consistency tolerance.)
5. **Concurrency:** 50 concurrent broad include_docs queries during
   sustained ops at ≥100 ops/sec. No wedging, no >1 s queries.
6. **Range scans:** still produce correct results (slow is OK).

## Risks

- **ShardStore append_op fsync overhead.** Not benchmarked at scale. If
  per-shard fsync is too expensive, we need batching (already in plan).
- **Existence bitmap correctness.** Has to stay in sync with disk. Bug
  here would silently return wrong results. Mitigate with
  proptest-style tests.
- **Cold-start latency.** First query for any value pays ~ms to load.
  Probably fine but should measure.
- **Subtle race between cache promote and concurrent eviction.** Not an
  issue in v1 (no eviction), but design v2 with this in mind.

## Questions for review

1. Is the per-shard mutex sufficient for write concurrency, or do we need
   something more sophisticated (e.g. an lru-style write coalescer)?
2. The existence bitmap (option B in §1) — is RoaringBitmap of 22 M values
   ~128 MB? Let me verify with a microbench before committing.
3. Should range scans on per-value-lazy fields be **rejected at parse
   time** (force the user to ack the cost) or **silently slow**?
4. Is there a clean way to make this incremental — e.g. ship per-value
   lazy for postId only, leave other fields alone, then expand?
5. Anything I'm missing about ShardStore's compaction interaction with
   the per-shard ops log when multiple writers append?
