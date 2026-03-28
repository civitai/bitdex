---
status: IMPLEMENTED
created: 2026-02-19
updated: 2026-03-28
---

# Storage Architecture

Bitdex V2 uses custom filesystem stores for persistence. There is no embedded database (no redb, no SQLite, no RocksDB). The OS page cache manages hot/cold data transparently.

| Store | Purpose | Location |
|-------|---------|----------|
| **ShardStore** | Bitmap persistence (filter, sort, alive, metadata) | `src/shard_store.rs`, `src/shard_store_bitmap.rs`, `src/shard_store_meta.rs` |
| **DocStore V2** | Sharded document storage (append-only tuple logs) | `src/docstore.rs` |
| **BitmapFs** (legacy) | Original bitmap persistence — still used by backfill.rs | `src/bitmap_fs.rs` |

---

## ShardStore (Production — replaces BitmapFs)

**Source:** `src/shard_store.rs` (1,485 lines), `src/shard_store_bitmap.rs` (1,723 lines), `src/shard_store_meta.rs` (292 lines)

ShardStore is a generic unified storage engine with pluggable codecs and sharding strategies. It replaced BitmapFs as the primary bitmap persistence layer.

### Type System

`ShardStore<S, O, Sh>` where:
- `S: SnapshotCodec` — how to serialize/deserialize snapshot sections
- `O: OpCodec<Snapshot = S::Snapshot>` — how to serialize/deserialize ops, tied to snapshot type
- `Sh: ShardingStrategy` — how to map keys to shard file paths

### Three Bitmap Stores

All defined in `src/shard_store_bitmap.rs`:

1. **`AliveBitmapStore`** — Single alive bitmap
   - Codec: `BitmapSnapshotCodec` + `BitmapOpCodec`
   - Sharding: `SingletonShard` → `system/alive.shard`
   - Ops: set/clear individual bits

2. **`FilterBitmapStore`** — Packed bucket shards per field
   - Codec: `BucketSnapshotCodec` + `FilterOpCodec`
   - Sharding: `FieldValueBucketShard` → `filter/{field}/{xx}.shard`
   - Each shard contains multiple values with an index table (same bucketing as old `.fpack` files)
   - Ops: tagged with `value_id` to target specific bitmap within bucket

3. **`SortBitmapStore`** — Per-bit-layer shards
   - Codec: `SortFieldSnapshotCodec` + `SortLayerOpCodec`
   - Sharding: `SortFieldShard` → `sort/{field}.shard` (all layers packed)
   - Ops: tagged with `bit_position` to target individual layers

### MetaStore

**Source:** `src/shard_store_meta.rs`

Simple atomic files for metadata that doesn't need generations:
- `slot_counter` — u32 monotonic slot counter
- `deferred_alive` — BTreeMap of future activation timestamps
- `time_buckets` — pre-computed time range bitmaps
- `cursors/{name}` — named cursor values (UTF-8 text, used by pg-sync)

### Shard File Format

28-byte header, CRC32-protected ops log:

```
[28 bytes: header]
  [8 bytes: magic "SHRDSTR\0"]
  [4 bytes: version (u32 LE)]
  [4 bytes: generation (u32 LE)]
  [4 bytes: snapshot_len (u32 LE)]
  [4 bytes: num_ops (u32 LE)]
  [4 bytes: flags]
[snapshot_len bytes: serialized snapshot]
[ops log: N × (4-byte len + op bytes + 4-byte CRC32)]
```

### Generation Model

- `pin_generation()` freezes current gen; new writes go to gen N+1
- Used by capture start/stop to bracket time windows for replay
- Compaction: per-shard, triggered when ops count exceeds threshold (default 1,000)
- Compaction reads snapshot + ops → writes fresh snapshot with zero ops

### Directory Layout (ShardStore)

```
shardstore/
  filter/{field_name}/{xx}.shard     # filter bitmaps, hex-bucketed
  sort/{field_name}.shard            # sort layer bitmaps, packed per field
  system/alive.shard                 # alive bitmap
  meta/slot_counter                  # u32 slot counter
  meta/deferred_alive                # BTreeMap of future activations
  cursors/{name}                     # named cursor values
  bounds/                            # persistent cache (BoundStore)
```

---

## DocStore V2 (Production)

**Source:** `src/docstore.rs` (3,199 lines)

DocStore V2 uses append-only tuple logs with no compression. This replaced V1 (zstd-compressed msgpack) and is 2.6x faster at batch reads (21us vs 56us).

### V2 Format: Append-Only Tuples

Each shard file is an append-only log of `(slot_id, field_index, value)` tuples:

```
[u32 magic = 0x42445832 ("BDX2")]
[u32 version]
[u32 flags]
[u32 num_tuples]
[tuples: N × (u32 slot_id, u16 field_index, u16 value_len, value_len bytes)]
```

**Reads:** LIFO scan — read tuples from end to start. First match for `(slot_id, field_index)` wins. No decompression needed.

**Writes:** Append new tuples to the end. No read-modify-write cycle. Updated fields shadow old values naturally.

**Compaction:** Reader-triggered when stale tuple percentage exceeds threshold (default 30%). A reader that detects staleness sends the shard to a compaction channel for background cleanup.

### Field Dictionary Encoding

Field names mapped to `u16` indices via persistent dictionary at `docs/meta/field_dict.bin`. For low-cardinality string fields (type, availability, blockedFor, baseModel), `FieldDictionary` provides string-to-integer mapping.

### DocCache

**Source:** `src/doc_cache.rs` (786 lines)

DashMap-based in-memory cache. Cache-on-read (first query populates), write-through (flush thread populates on writes). LRU eviction at 1GB. Drops doc reads from 16ms/doc (disk) to <1us/doc (memory).

### Directory Layout (DocStore)

```
docs/
  meta/field_dict.bin                # field name ↔ u16 dictionary
  shards/{xx}/{NNNNNN}.bin           # shard files, hex-nested
```

Hex-nested directory structure keeps each dir under ~1000 files at 105M+ scale.

### Write Paths

- **BulkWriter** — Parallel shard writer with per-shard locks. Used during dump processing. Throughput: 290K docs/s.
- **Flush thread batch** — `put_batch()` groups by shard, appends tuples. Used during steady-state operation.
- **Single put** — Appends tuples for a single document update.

---

## BitmapFs (Legacy)

**Source:** `src/bitmap_fs.rs` (1,137 lines)

BitmapFs was the original bitmap persistence layer. It has been replaced by ShardStore for all production I/O paths (ConcurrentEngine writes, dump processor, merge thread). It is still used by `src/pg_sync/backfill.rs` for live field updates during server operation.

**Key differences from ShardStore:**
- No ops log — full overwrite on every save
- No generation model — single snapshot only
- `.fpack` files instead of `.shard` files for filter bitmaps
- `.sort` files instead of `.shard` files for sort layers

BitmapFs will be fully removed post-V2 validation.

---

## Persistence Lifecycle

### Data Flow to Disk

1. **Writers** compute diffs and send `MutationOp`s to a crossbeam channel.
2. **Flush thread** drains the channel, batches mutations, applies to the staging `InnerEngine`, and writes documents to DocStore via `put_batch()`.
3. **Merge thread** periodically writes bitmap state to ShardStore via `write_inner_to_store()`. Uses `AliveBitmapStore`, `FilterBitmapStore`, `SortBitmapStore`, and `MetaStore`.

### Startup: Lazy Bitmap Loading

On startup, only the alive bitmap and slot counter are loaded eagerly (always needed, small). All filter and sort field bitmaps are tracked as "pending" in `pending_filter_loads` / `pending_sort_loads` sets.

`ensure_fields_loaded()` is called at the start of every `query()` and `execute_query()`. Fast path: if pending sets are empty, just two mutex checks (~nanoseconds). On first query touching a pending field, bitmaps are loaded from ShardStore, the snapshot is updated, and the new snapshot is published via ArcSwap.

### WAL (Sync V2)

The V2 sync pipeline introduces a write-ahead log (`src/ops_wal.rs`) for ops received from the `bitdex-sync` sidecar. This WAL is separate from bitmap persistence — it logs incoming ops for crash recovery and deduplication, not bitmap state.

Postgres remains the source of truth. On restart, any mutations since the last WAL cursor are replayed from the `BitdexOps` PG table.

---

## Cache Persistence (BoundStore)

**Source:** `src/bound_store.rs` (1,083 lines)

The BoundStore persists unified cache entries to disk, enabling warm cache restarts. Located at `shardstore/bounds/`.

- `meta.bin` loaded eagerly on startup
- Bitmap shards lazy-loaded on first query per sort field
- Tombstoning invalidates unloaded entries on mutations
- Purge via `DELETE /cache/persistent`

Sort queries are 2-13x faster at 104M scale via pre-filtered working sets.

---

## Measured Performance

| Metric | Value |
|--------|-------|
| DocStore on disk (100M records) | ~6 GB |
| Shard count (105M records) | ~205K shards |
| Startup time | instant (<1s) |
| Lazy load: nsfwLevel (7 values) | 87ms |
| Lazy load: reactionCount sort (32 layers) | 84ms |
| Lazy load: sortAt sort (105M, 32 layers) | 22s |
| Lazy load: tagIds (31K values, 79% of memory) | 6.6s |
| BulkWriter throughput | 290K docs/s |
| DocStore V2 vs V1 batch reads | 2.6x faster (21us vs 56us) |
