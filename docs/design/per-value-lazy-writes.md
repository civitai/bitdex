# Per-Value Lazy Writes — Design Doc (v2)

**Status:** Draft for Justin + Gemini + GPT review
**Author:** Ivy
**Date:** 2026-04-07
**Supersedes:** v1 of this doc (based on incomplete understanding of ShardStore)

## TL;DR

Make filter field writes **route to the ShardStore per-shard ops log** when
the value's bitmap isn't already in the in-memory cache. Let ops accumulate
on disk in the ops log. Reads lazily apply them when they load the value,
and the existing janitor / compaction rolls the ops log into the snapshot
when it gets too big. Cache the materialized result for subsequent reads.

This is the use case ShardStore was **designed for**. The current code
bypasses it by fully materializing the entire field into an in-memory
`HashMap<u64, VersionedBitmap>` on first access, which is what caused the
v1.0.144 wedge.

---

## What the v1 doc got wrong

The v1 draft proposed building a new per-value-lazy infrastructure from
scratch. After an ExploreRust audit of `src/shard_store.rs`,
`src/shard_store_bitmap.rs`, and the `lazy_value_fields` code path in
`src/concurrent_engine.rs`, it turns out **most of the infrastructure
already exists**:

- **256 fixed shards per filter field**, value mapped via
  `(value >> 8) & 0xFF`. Stable forever. `shard_store_bitmap.rs:656-668`.
- **`FilterShardStore::load_field_values(field, &[u64])`** already does
  grouped-by-bucket reads and returns only the requested values.
  `shard_store_bitmap.rs:901-922`.
- **`FilterOp::SetBit { value, bit }`** single-value-targeted op variants
  already exist. ShardStore's read path already replays them over a
  `BucketSnapshot` HashMap on read. `shard_store_bitmap.rs:58-67, 273-297`.
- **`ShardStore::append_op`** already appends ops to a shard's on-disk ops
  log. It fsyncs per call (see §Problems below). `shard_store.rs:559-582`.
- **Compaction is crash-safe** via write-temp-then-atomic-rename.
  `shard_store.rs:307-331`.
- **`per_value_lazy: bool`** config flag already exists and determines
  whether a field is routed to the full-field load path
  (`pending_filter_loads`) or the per-value load path
  (`lazy_value_fields`). `concurrent_engine.rs:404-412`.
- **`tagIds` has been in per-value-lazy mode in prod for months** without
  issues at ~34 K distinct values.

Phase 1 of this work was **a single config flag flip** for `postId` /
`postedToId`: set `per_value_lazy: true`. Local replay validation shows
apply latency drops to 7-22 μs (was 540-2000 ms under eager load), flush
thread runs cleanly with zero wedging, and memory stays bounded to the
working set. That change is already in prod (it was only my local test
config that still had the old setting).

Phase 2 — the subject of this doc — closes the remaining gap on the
**write** side.

## The problem that remains after Phase 1

With `per_value_lazy: true`, the **read path** is correct and fast: a
query for `postId=42` calls `load_field_values("postId", [42])`, which
reads only the bucket containing 42, extracts 42's bitmap, caches it in
the in-memory `FilterField.bitmaps` HashMap, and returns.

The **write path**, however, still goes through `FilterField::insert_bulk`
(`src/filter.rs`). When an op arrives for a value that isn't cached:

```rust
pub fn insert_bulk(&self, value: u64, slots: impl IntoIterator<Item=u32>) {
    let mut w = self.bitmaps.write();
    w.entry(value)                                        // <-- cold value
        .or_insert_with(VersionedBitmap::new_empty)       // <-- fresh empty VB
        .insert_bulk(slots);                              // <-- bits land in VB's diff
}
```

This **creates an in-memory VersionedBitmap** and holds the new slots in
its diff layer. The on-disk base for that value is untouched — it'll get
merged in on the next read via `load_base` OR-ing into the VB.

Consequences:

1. **Memory grows with touched values.** Every unique value that ever
   receives a write gets a VB allocation (~96 bytes per entry in the
   HashMap). For `postId` at 22.5 M values, if every value eventually
   gets touched, the cache grows back to ~2 GB — re-introducing the
   memory profile Phase 1 was supposed to fix, just slowly instead of
   all at once.

2. **Writes don't flow through ShardStore's ops log.** They sit in
   memory until the merge thread eventually persists them. On crash,
   the WAL reader replays them — so it's crash-safe — but the per-shard
   persistence path that was designed for exactly this isn't being used.

3. **No coordination between cache residency and write path.** The write
   path assumes all values it touches will stay in memory forever
   (since we never evict). An eviction policy can't land without
   rethinking this.

## Goals

1. **Writes to cold values don't allocate in-memory VBs.** They append
   directly to the ShardStore per-shard ops log.
2. **Writes to hot (cached) values update the cache AND the ops log**,
   keeping the cache as the authoritative in-memory state AND keeping
   the on-disk ops log durable. The cache is the read accelerator; the
   ops log is the write accelerator + persistence.
3. **The existing janitor / compaction** rolls the ops log into the
   per-shard snapshot when ops count exceeds a threshold. No new
   background work — use what's there.
4. **Cache eviction** becomes possible because the cache is no longer
   the only place mutations live. Evict freely; evicted values reload
   from disk (snapshot + any un-compacted ops) on next read.
5. **Same crash safety** as today: WAL is the source of truth, ShardStore
   is the read-optimized projection, WAL cursor advances after disk
   durability of the corresponding snapshot+ops state.

## Non-goals

- Sort field per-value-lazy writes. Sort fields have 32 fixed bit layers,
  not millions of value-keyed entries. Out of scope.
- Eviction algorithm refinement. v1 uses simple LRU; smarter policies
  (cost-based, weighted) are a follow-up.
- Removing the staging system. Orthogonal refactor.
- Making range scans efficient on high-cardinality per-value-lazy fields.
  Range scans are rare and should be guarded at the planner level.

---

## Design

### The write path under per_value_lazy

For a `per_value_lazy: true` filter field, `FilterField::insert_bulk`
becomes:

```rust
pub fn insert_bulk(&self, value: u64, slots: impl IntoIterator<Item=u32>) {
    let slot_vec: Vec<u32> = slots.into_iter().collect();

    // Fast path: value is already cached. Update the cache in place.
    // The op ALSO gets appended to the shard ops log (see below) so a
    // later cache eviction doesn't lose the mutation.
    {
        let r = self.bitmaps.read();
        if r.contains_key(&value) {
            drop(r);
            let mut w = self.bitmaps.write();
            if let Some(vb) = w.get_mut(&value) {
                vb.insert_bulk(slot_vec.iter().copied());
                // fall through to ops log append
            } else {
                // raced with eviction; skip in-place update
            }
        }
    }

    // Append to the per-shard ops buffer. The buffer is flushed to
    // ShardStore::append_ops periodically by the janitor thread (batched
    // by shard so we fsync once per shard per flush cycle, not per op).
    self.shard_ops_buffer.push(value, BitmapOp::BatchSet {
        value,
        slots: slot_vec,
    });
}
```

`remove_bulk` is symmetric with `BatchClear`.

The `shard_ops_buffer` is a lightweight append-only log keyed by shard
bucket. It lives in the `FilterField` itself and is drained by the
existing janitor thread (or a dedicated one — see §Janitor below).

### The read path (mostly unchanged)

Reads already call through to `FilterField::apply_diff_eq` and friends,
which check the in-memory cache. On a cache miss, the query path triggers
`ensure_fields_loaded` which calls `load_field_values` to fetch the value
from ShardStore. Two additions:

1. **Flush the pending ops buffer for that shard** before reading the
   shard from disk, so the read reflects the latest writes. Either:
   (a) synchronously flush the buffer in `load_field_values` before the
   shard read, OR (b) have `load_field_values` include pending buffered
   ops in the returned state.

   Option (a) is simpler — we guarantee that `ShardStore::read` sees
   everything. Option (b) avoids the fsync on the read path.

   **Recommendation:** (a) synchronously flush. Reads are rare for cold
   values, and we want the fewest moving parts.

2. **Post-load promote** — after reading the value from disk, insert it
   into the in-memory cache (the existing code does this via
   `load_values`). This is unchanged.

### The shard ops buffer

```rust
/// Per-filter-field buffer of pending ops, grouped by shard bucket.
/// Drained periodically by the janitor thread or synchronously by a
/// read that needs up-to-date state.
pub struct ShardOpsBuffer {
    /// 256 slots — one per bucket (shards are (value >> 8) & 0xFF).
    buckets: [parking_lot::Mutex<Vec<FilterOp>>; 256],
}

impl ShardOpsBuffer {
    pub fn push(&self, value: u64, op: FilterOp) {
        let bucket = ((value >> 8) & 0xFF) as usize;
        self.buckets[bucket].lock().push(op);
    }

    /// Drain all pending ops for a specific bucket. Called by the
    /// janitor (periodic flush) or load_field_values (synchronous
    /// before-read flush).
    pub fn drain_bucket(&self, bucket: u8) -> Vec<FilterOp> {
        std::mem::take(&mut *self.buckets[bucket as usize].lock())
    }
}
```

Why a fixed `[Mutex<Vec<FilterOp>>; 256]`:

- Matches ShardStore's 256-bucket layout exactly.
- Zero-alloc for the container.
- Per-bucket locks give us bounded contention (256-way sharded).
- Drain by bucket is O(ops) and moves the Vec out with zero-copy.

### The janitor

The existing ShardStore has a background merge/compaction thread. Under
this design, it picks up one additional responsibility:

```rust
loop {
    sleep(janitor_interval);  // e.g. 5 seconds

    for each per_value_lazy filter field:
        for bucket in 0..256:
            let pending = field.shard_ops_buffer.drain_bucket(bucket);
            if !pending.is_empty() {
                // Batched per-bucket append: one fsync per bucket, not per op.
                filter_store.append_ops_to_bucket(field_name, bucket, &pending)?;
            }

        // Compaction: if any bucket's ops log exceeds threshold, compact.
        // This already exists in ShardStore — just make sure it's called.
        filter_store.compact_if_needed(field_name)?;
}
```

**Key insight:** we stop fsyncing per op. The janitor fsyncs per shard per
interval. At 5 s intervals with 256 shards, worst case is 256 fsyncs per
interval — but only for shards that had any ops, which is typically a
small fraction. Average case: few fsyncs per interval.

### WAL cursor invariant

Today: WAL reader applies → in-memory mutations → merge thread persists
→ WAL cursor advances.

Under this design: WAL reader applies → hot values update cache + buffer
pending op, cold values push pending op → janitor drains buffer to
ShardStore → compaction runs occasionally → **WAL cursor advances after
the janitor successfully flushes**.

The invariant is **unchanged**: the cursor only advances to position X
after the effects of position X are durable on disk. Durability is now
achieved by the janitor flushing the buffer to ShardStore (same thing
the merge thread does today for the in-memory snapshot — just through
a different write path).

**Crash recovery:** WAL is the source of truth. On restart, the WAL
reader resumes from the on-disk cursor, replays any missed ops, the
engine applies them, the janitor persists them, cursor advances. Same
idempotence guarantees as today (ops are `(value, slot)` pairs that
are idempotent under set union / set difference).

### Cache eviction

With writes persisted through the ops log, the in-memory cache can be
bounded and evictable:

```rust
pub struct FilterField {
    bitmaps: RwLock<LruCache<u64, VersionedBitmap>>,  // <-- LruCache, not HashMap
    shard_ops_buffer: Arc<ShardOpsBuffer>,
    max_cached_values: Option<usize>,  // per-field bound from config
    config: FilterFieldConfig,
}
```

On write to a hot value:
1. Update the cache entry in place.
2. Append to the shard ops buffer (so if the cache evicts this entry
   before the janitor runs, the mutation survives).
3. LruCache moves the entry to MRU.

On eviction:
- No-op for the correctness path. The on-disk state (snapshot + ops log)
  already reflects everything via the buffer.
- The evicted VB is simply dropped. Memory freed.

On read miss:
- Load via `load_field_values` (which synchronously flushes the shard
  ops buffer for the relevant bucket first).
- Promote to LruCache.

**Using `lru::LruCache` is explicit.** `moka` is not used anywhere in
BitDex per project rules.

---

## Crash safety walk-through

Scenario 1: crash during normal write
1. Client sends op to `/ops`
2. Server appends to WAL, fsyncs, returns 200
3. WAL reader picks it up, calls `FilterField::insert_bulk`
4. Op added to shard ops buffer (in memory)
5. **CRASH**

Recovery:
- WAL cursor is at the position BEFORE step 3 (cursor only advances
  after janitor persist)
- Restart replays WAL from that cursor
- Ops reprocessed idempotently
- Janitor runs, flushes buffer to disk
- Cursor advances
- ✅ No data loss

Scenario 2: crash during janitor flush
1. Janitor drained bucket B into a Vec
2. `filter_store.append_ops_to_bucket(field, B, &pending)` — fsync
   succeeded for shard 3 but not shard 5 when **CRASH**

Recovery:
- Cursor is at the position BEFORE the janitor cycle started
- Restart replays WAL from that cursor
- Ops reprocessed, buffered again
- Next janitor cycle re-appends to shards 3 and 5
- Shard 3 now has duplicate ops (set-bit applied twice)
- ✅ Idempotent — set union is idempotent; same result

Scenario 3: crash during compaction
- Already handled by ShardStore's write-temp-then-atomic-rename
  (`shard_store.rs:307-331`)
- ✅ Unchanged from today

Scenario 4: cache evicted an entry with pending buffered ops
- Read comes in for the evicted value
- `load_field_values` synchronously flushes the shard ops buffer first
- Reads shard state (which now includes the flushed ops via the on-disk
  ops log) + replays
- Returns correct result
- ✅ Correct

---

## Concurrency

### Per-shard contention

The `shard_ops_buffer.buckets[bucket].lock()` is the only per-shard lock
on the write path. Contention is bounded by how many writers hit the
same bucket simultaneously. For 256 buckets with ~uniform hash
distribution, at 100 ops/sec that's <1 op/sec per bucket. Essentially
zero contention.

### Read vs write on the same value

1. Reader takes read lock on `FilterField.bitmaps`, checks cache. Miss.
2. Drops read lock, calls `ensure_fields_loaded`.
3. Writer arrives, takes bucket buffer lock, appends op. Drops.
4. `ensure_fields_loaded` → `load_field_values` → sync flush of bucket
   buffer (re-takes the same lock, drains, calls
   `append_ops_to_bucket`) → ShardStore::read (now reflects the write)
   → returns bitmap including the writer's op.
5. Reader promotes to cache, returns.

**Race:** what if another writer arrives between step 4 drain and step 5
promote? The new op lands in the buffer. The reader's cached value
doesn't reflect it. Next read of the same value sees the cached value
(which is stale) until the janitor runs.

**Mitigation:** the next cache read for the same value will check the
shard ops buffer for that bucket and merge in any pending ops. Concretely:

```rust
pub fn apply_diff_eq(&self, value: u64, candidates: &RoaringBitmap)
    -> Option<RoaringBitmap>
{
    let r = self.bitmaps.read();
    if let Some(vb) = r.get(&value) {
        let mut fused = if vb.is_dirty() {
            vb.apply_diff(candidates)
        } else {
            candidates & vb.base().as_ref()
        };

        // Merge any pending buffered ops for this value.
        let bucket = ((value >> 8) & 0xFF) as u8;
        let pending = self.shard_ops_buffer.peek_bucket(bucket);
        for op in pending {
            if op.value() == value {
                fused = apply_op_to_bitmap(fused, op);
            }
        }
        return Some(fused);
    }
    // Slow path: load from disk
    ...
}
```

`peek_bucket` takes a read lock and clones the Vec. This is fine because
the buffer is small (ops between janitor flushes).

**Alternative:** bump a cache-entry epoch on every write and invalidate
on read. More complex but avoids the peek on every read. Defer unless
benchmarks show the peek is a hotspot.

---

## Failure modes

1. **Disk full during janitor flush.** The janitor logs an error and
   leaves the ops in the buffer. WAL cursor does not advance. Next
   janitor cycle retries. If disk stays full, memory grows in the
   buffer. Eventually admin intervention is needed. Same failure mode
   as today.

2. **ShardStore corruption during read.** The shard file is corrupted
   for value V. `load_field_values` returns an error. Query fails with
   a clear error message pointing at the shard. Operator rebuilds the
   shard from WAL or from a backup. New failure mode introduced by
   this design but same mitigation as any storage corruption.

3. **Cache eviction thrashing.** A query loads 1 M values, the LRU
   evicts older entries, subsequent reads miss. The bucket-grouped
   reads in `load_field_values` keep this bounded — each bucket is
   read once even if we need 1000 values from it. Worst case: 256
   shard reads per query, ~5 ms each = 1.2 seconds. Rare (only on
   IN-clauses with many values across many buckets).

---

## Rollout plan

1. **Phase 1 (done):** `postId` / `postedToId` / `remixOfId` set to
   `per_value_lazy: true` in prod. Already deployed. Validated locally
   via replay — apply is 7-22 μs, no wedging.

2. **Phase 2a:** add `shard_ops_buffer` + write-path routing behind a
   feature flag. Mutations still go through `FilterField.bitmaps`
   HashMap; the buffer is just a shadow write that we don't yet consult
   on read. Run in prod with the flag off; turn on in a canary and
   validate that buffer drains match expectations.

3. **Phase 2b:** switch the read path to peek the buffer, switch the
   write path to skip the in-memory cache allocation for cold values.

4. **Phase 2c:** replace `RwLock<HashMap>` with `RwLock<LruCache>`.
   Add `max_cached_values` config per field. Start with a generous
   bound (e.g. 1 M entries) and tune down.

5. **Phase 2d:** retire the old `load_field_complete` path entirely
   for per-value-lazy fields. Nothing should ever call it under the
   new design.

Each phase is independently deployable and revertible.

---

## Implementation estimate

| Phase | Work | Estimate |
|-------|------|----------|
| 2a | `ShardOpsBuffer` struct + shadow-write plumbing | 2 h |
| 2a | Janitor integration (drain + `append_ops_to_bucket`) | 2 h |
| 2a | Synchronous flush in `load_field_values` | 1 h |
| 2a | Tests: crash recovery, buffer drain, shadow write correctness | 2 h |
| 2b | Write path routing (cold → buffer only, hot → cache + buffer) | 2 h |
| 2b | Read path peek of pending ops buffer | 1 h |
| 2b | Tests: read-your-writes on cold values, race between write and read | 3 h |
| 2c | `LruCache` swap + eviction tests | 2 h |
| 2d | Remove dead code paths, update docs | 1 h |
| — | Replay validation + memory profiling | 2 h |

**Total: ~18 hours.** Up from the v1 doc's "11 hours" but I'm being more
honest about test time. Still way under GPT's "2-3x for production-ready"
because most of the hard parts (ShardStore, per-value lazy read,
compaction crash safety) are already in the codebase.

---

## Validation criteria

Must pass before merging:

1. **Apply latency unchanged or better.** Target: ≤50 μs p99 under
   sustained replay load. Current Phase 1: 7-22 μs in isolated tests.

2. **Cold write path does not allocate a VersionedBitmap.** Verified
   via a microbench that mutates N cold values and measures heap
   growth (should be O(ops) not O(values)).

3. **Read-after-write on cold values.** Replay tool's per-request match
   rate ≥99.5 %. Specifically: insert ops for cold value V, then query
   V, verify the slots appear.

4. **Crash recovery.** Replay → SIGKILL → restart → query → verify all
   acknowledged ops are visible.

5. **Cache eviction under LRU.** Fill cache to `max_cached_values`,
   touch new value, verify LRU entry evicted, verify re-read of
   evicted value returns correct result (including any buffered ops).

6. **Janitor flush correctness.** Write N ops, wait for janitor, verify
   shard ops log on disk contains N ops, verify snapshot+ops yield the
   expected bitmap after ShardStore::read.

7. **Memory bounded under sustained writes.** Replay for 10 minutes,
   measure RSS. Should plateau, not grow linearly with ops.

8. **Concurrency stress test.** 50 concurrent readers on the same
   field while a writer appends to the buffer at 100 ops/sec. No
   deadlocks, no stale reads visible longer than one janitor interval.

---

## Open questions (for review)

### Q1. Where does `shard_ops_buffer` live?

Options:
- (a) As a field on `FilterField` itself. Co-located with the cache.
- (b) On `FilterIndex` with a per-field map. Decouples.
- (c) Centralized in `ConcurrentEngine` with a `{field: ShardOpsBuffer}`
  map. Maximally decoupled but adds a lookup indirection.

**Recommendation:** (a). It's a natural extension of FilterField and
doesn't require another lookup.

### Q2. When do we peek the buffer on read?

- Always — adds a per-read cost even when the buffer is empty (which
  is the common case after a recent janitor flush).
- Only when a cache-dirty epoch has changed — precise but requires
  maintaining the epoch.
- Never on the cache-hit path; only on the cache-miss path (and
  there, synchronously via `load_field_values`).

**Recommendation:** the third option. Cache-hit reads don't peek; they
rely on the fact that writes update the cache in place AND the buffer.
Cache-miss reads peek synchronously via `load_field_values`. The only
failure case is a cached value that was written-to, then evicted, then
re-loaded from stale disk state — but the janitor flushes the buffer
before the next read so this is only a window of ~janitor_interval.

### Q3. Shadow-write phase (2a) — is it worth it?

It's extra work, but it lets us validate the buffer + janitor in prod
without any read-path changes. Low risk, good signal.

**Recommendation:** yes. Deploy the shadow-write first, measure drain
rates and buffer sizes for a few days, then flip the read path.

### Q4. Should we build the read-path buffer peek at all?

Alternative: after every write to a hot cache value, also mark the
entry "dirty" and bump an epoch. Read path checks the epoch against
the last-flushed epoch; if stale, force-flush the bucket before
returning. More complex than always-peek but avoids the per-read
buffer walk.

**Recommendation:** start with always-peek on cache-miss only (Q2's
third option). Revisit if benchmarks show it's a hotspot.

### Q5. What's the janitor interval?

5 s matches the existing merge thread cadence. Each janitor cycle
fsyncs one shard per bucket with pending ops, so worst case is 256
fsyncs per cycle. At typical 100 ops/sec distributed across buckets,
~10-20 buckets have pending ops per cycle, so ~10-20 fsyncs every 5 s.
Cheap.

If we want tighter read-after-write latency, we could shorten the
interval, but we'd pay more fsyncs.

**Recommendation:** 5 s default, configurable.

### Q6. Does ShardStore's compaction interact correctly with the buffer?

The buffer holds ops that haven't been flushed to disk yet. Compaction
rolls the disk ops log into the snapshot. If compaction runs while the
buffer has pending ops, those ops won't be in the compacted snapshot —
they'll be appended to the (now-compacted) ops log on the next janitor
flush. The on-disk state remains correct because
`ShardStore::read` always replays any pending ops over the snapshot.

**Recommendation:** no change needed. ShardStore handles it.

### Q7. Do we need eviction from day one?

v1 of this doc said no; Gemini+GPT said yes. I now agree: without
eviction, a query like `postId IN (5M cold IDs)` reloads 5M bitmaps
into the cache and we're back to the v1.0.144 wedge.

**Recommendation:** LRU with a per-field `max_cached_values` bound.
Ship v1 with a generous bound (1 M) and tune down based on prod memory.

---

## Relationship to today's code

| Today's code | Under this design |
|---|---|
| `FilterField.bitmaps: HashMap<u64, VB>` | `LruCache<u64, VB>` |
| `insert_bulk` creates a fresh VB for cold values | `insert_bulk` routes cold writes to `shard_ops_buffer` |
| No write persistence until merge thread | Janitor flushes per-bucket every 5 s |
| `load_field_complete` loads ENTIRE field | Never called for per-value-lazy fields |
| Read path never consults buffer | Read cache-miss path synchronously flushes buffer |
| `append_op` fsync per call | `append_ops_to_bucket` fsync per bucket per janitor cycle |

## Relationship to prior review feedback

**Gemini v1 review:**
- ✅ Cache coherence race (stale promote): fixed by sync flush on miss
- ✅ Per-op fsync anti-pattern: fixed by janitor batching
- ✅ Fixed `[Mutex<Vec<FilterOp>>; 256]` array: matches this design
- ✅ LRU eviction from day one: in Phase 2c
- ✅ Reject range scans on PerValueLazy: separate planner work (note below)
- ⚠️ Note: moka is banned, use `lru::LruCache`

**GPT v1 review:**
- ✅ All writes flow through a single source of truth (the ops log):
  yes, even hot-value writes append to the buffer
- ✅ WAL stays source of truth; ShardStore is async projection: yes
- ✅ Cache entry epoch / invalidation: sync-flush-on-miss achieves the
  same goal without explicit epochs
- ✅ Empty-bitmap / tombstone semantics: ShardStore already handles
  this via `FilterOp::BatchClear` and compaction
- ✅ `I/O error vs absent` distinction: call out in implementation that
  `load_field_values` returns `Result<Option<_>>`, not `Option<Option<_>>`
- ✅ Singleflight for cold loads: addressed by the sync-flush lock per
  bucket (two readers loading the same bucket will naturally serialize)
- ⚠️ Range scans on per-value-lazy fields: document as "slow" rather
  than "rejected" for now — they're not common in our query mix

---

## Not in scope (but worth noting)

- **Dropping the staging system** — separate doc, separate review.
- **Sort field per-value-lazy** — sort fields don't have this shape.
- **Eviction policy refinement** — LRU is fine for v1.
- **Query planner guardrails on range scans** — separate PR.
