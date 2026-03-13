---
status: IMPLEMENTED
created: 2026-02-19
updated: 2026-03-13
---

# Storage Architecture

Bitdex V2 uses two custom filesystem stores for persistence. There is no embedded database (no redb, no SQLite, no RocksDB). The OS page cache manages hot/cold data transparently.

| Store | Purpose | Location |
|-------|---------|----------|
| **BitmapFs** | Bitmap persistence (filter, sort, alive, metadata) | `src/bitmap_fs.rs` |
| **DocStore** | Sharded document storage (zstd-compressed msgpack) | `src/docstore.rs` |

Both stores use the same atomic write pattern: write to `{name}.tmp`, then rename over the target. This is atomic on POSIX and close-enough on NTFS.

---

## BitmapFs

**Source:** `src/bitmap_fs.rs`

BitmapFs persists all bitmap state to a directory tree. The root path is set via `StorageConfig.bitmap_path` in the engine config (`src/config.rs`, line 365-370). If `bitmap_path` is `None`, bitmaps are memory-only.

### Directory Layout

```
bitmaps/
  filter/{field_name}/{xx}.fpack   # filter bitmaps, hex-bucketed
  sort/{field_name}.sort           # sort layer bitmaps, packed per field
  system/alive.roar                # alive bitmap
  meta/slot_counter.bin            # u32 slot counter
  cursors/{name}                   # named cursor values (UTF-8 text)
  time_buckets/{name}.roar         # pre-computed time range bitmaps
```

Directories `filter/`, `sort/`, `system/`, `meta/` are created on `BitmapFs::new()` (lines 38-48). `cursors/` and `time_buckets/` are created on first write.

### File Formats

#### Filter Bitmaps: `.fpack` files

Each filterable field gets a subdirectory under `filter/`. Values are bucketed by `(value >> 8) & 0xFF` into up to 256 pack files per field (line 120-122).

**Pack file format** (lines 113-116):

```
[u32 num_entries]
[index: N x (u64 value, u32 offset, u32 length)]   # 16 bytes per entry
[packed serialized roaring bitmaps]
```

- High-cardinality fields (e.g., tagIds with 31K values) produce ~256 pack files, each containing ~120 entries.
- Low-cardinality fields (e.g., nsfwLevel with 7 values) produce 1-2 tiny pack files.

Reading supports three granularities:
- `load_field()` (line 298) -- reads all `.fpack` files for a field, returns all `(value, bitmap)` pairs
- `load_field_values()` (line 210) -- reads only the buckets containing requested values, deserializes only matching entries
- `list_field_keys()` (line 324) -- reads only the header index from each `.fpack` file, returns the set of value IDs without deserializing any bitmaps (used for positive existence sets)

Writing is via `write_batch()` (line 383) which groups entries by `(field, bucket)` and writes one pack file per group, or `write_filter_bucket()` (line 375) for streaming saves.

#### Sort Layer Bitmaps: `.sort` files

Each sortable field is stored as a single packed file at `sort/{field}.sort` (line 417-419).

**Sort file format** (lines 412-414):

```
[u8 num_layers]
[index: N x (u8 bit_position, u32 offset, u32 length)]   # 9 bytes per entry
[packed serialized roaring bitmaps]
```

A u32 sort field produces 32 layers (one per bit position). All layers are written/read as a single file via `write_sort_layers()` (line 422) and `load_sort_layers()` (line 460).

#### Alive Bitmap: `system/alive.roar`

A single serialized `RoaringBitmap` written via `write_bitmap_atomic()` (line 65). Written first during `write_full_snapshot()` (line 588) so partial saves still produce a usable restart.

#### Slot Counter: `meta/slot_counter.bin`

A 4-byte little-endian `u32` (lines 514-533).

#### Cursors: `cursors/{name}`

Plain UTF-8 text files, one per named cursor. Used by pg-sync to track replication position. Written atomically via `write_cursor()` (line 625), loaded individually via `load_cursor()` (line 633) or all at once via `load_all_cursors()` (line 643). Temp files (`.tmp` suffix) are skipped when loading.

#### Time Buckets: `time_buckets/{name}.roar`

Individual `.roar` files per time bucket (24h, 7d, 30d, 1y). Written via `write_time_bucket()` (line 545), loaded in bulk via `load_time_buckets()` (line 554).

### Full Snapshot Write

`write_full_snapshot()` (line 579) writes all state in this order:

1. Alive bitmap (critical -- enables restart)
2. Slot counter (critical -- prevents slot reuse)
3. Sort layers (one `.sort` file per field)
4. Filter bitmaps (grouped into `.fpack` files)

Critical metadata is written first so partial saves still produce a usable restart.

---

## DocStore

**Source:** `src/docstore.rs`

DocStore persists documents as packed shard files on the filesystem. Each document is stored as msgpack-encoded field pairs with per-field dictionary encoding, zstd-compressed per shard.

### Directory Layout

```
docs/
  meta/field_dict.bin              # field name <-> u16 dictionary (msgpack)
  meta/schema/v{N}.json           # schema history (defaults per version)
  shards/{xx}/{NNNNNN}.bin        # shard files, hex-nested
```

Shard IDs are computed as `slot_id >> 9` (512 documents per shard, `SHARD_SHIFT = 9`, line 35). The hex-nested directory uses the top byte of the shard ID: `shards/{(shard_id >> 8) & 0xFF:02x}/{shard_id:06}.bin` (lines 124-131). This keeps each directory under ~1000 files at 105M+ scale (~205K shards total).

### Shard File Format

**Version 1** (line 40, `SHARD_VERSION = 1`):

```
[u32 version=1]
[u32 num_entries]
[index: N x (u32 slot_id, u32 data_offset, u32 data_length)]   # 12 bytes per entry
[u32 uncompressed_size]
[zstd-compressed data block]
```

The data block is a single zstd-compressed blob containing all documents concatenated. Offsets in the index table point into the *uncompressed* data. Reading a single document requires decompressing the entire shard, then binary-searching the index for the slot ID (lines 386-402).

Writes use the same atomic tmp+rename pattern (lines 449-454).

### Document Encoding

Each document is stored as a list of `(u16 field_index, PackedValue)` pairs, serialized as msgpack (lines 501-525).

**Field dictionary encoding** (lines 133-182): Field names are mapped to `u16` indices via a persistent dictionary stored at `docs/meta/field_dict.bin`. The dictionary is msgpack-encoded as a `Vec<String>` and grows monotonically. New fields are appended on first encounter and the dictionary is atomically rewritten.

**Schema versioning** (lines 481-498): Each encoded document is prefixed with a 2-byte version header: `[0x00 marker][u8 version]`. The `0x00` marker byte distinguishes versioned docs from legacy ones (msgpack arrays always start at `0x90+`). Legacy pre-versioning documents return version 0.

**Default value elision** (lines 511-516): Fields whose value matches the schema default are omitted from the encoded document. On read, the document's schema version is used to look up historical defaults and reconstruct elided fields. Schema history is persisted as JSON files at `docs/meta/schema/v{N}.json` (lines 267-304). This saves 30-40% on typical Civitai documents.

### Write Operations

- **Single put** (`put()`, line 620): Read existing shard, decode index, merge new entry at sorted position, rewrite entire shard. Used for upserts during normal operation.
- **Batch put** (`put_batch()`, line 651): Group documents by shard, read-merge-write each shard once. Used by the flush thread for batched writes.
- **BulkWriter** (line 802): Parallel shard writer with per-shard `DashMap<u32, Mutex<()>>` locks. Most shards are written by exactly one thread (zero contention). Used during initial data loading for throughput (290K/s measured).

### Read Operations

- **Single get** (`get()`, line 565): Read shard file, decompress, binary search index, decode document.
- **Shard get** (`get_shard()`, line 588): Decompress once, decode all documents in the shard. Used when bulk-reading is more efficient than per-slot lookups.

---

## Persistence Lifecycle

### Data Flow to Disk

1. **Writers** compute diffs and send `MutationOp`s to a crossbeam channel.
2. **Flush thread** drains the channel, batches mutations, applies to the staging `InnerEngine`, and writes documents to DocStore via `put_batch()`.
3. **Merge thread** periodically writes a full bitmap snapshot to BitmapFs via `save_snapshot()` (`src/concurrent_engine.rs`, line 2291). This writes alive bitmap, slot counter, sort layers, and filter bitmaps.

### Startup: Lazy Bitmap Loading

On startup, only the alive bitmap and slot counter are loaded eagerly (always needed, small). All filter and sort field bitmaps are tracked as "pending" in `pending_filter_loads` / `pending_sort_loads` sets.

`ensure_fields_loaded()` is called at the start of every `query()` and `execute_query()`. Fast path: if pending sets are empty, just two mutex checks (~nanoseconds). On first query touching a pending field, bitmaps are loaded from BitmapFs, the snapshot is updated, and the new snapshot is published via ArcSwap.

### No WAL

There is no write-ahead log. Postgres is the source of truth. On restart, any mutations since the last bitmap snapshot are replayed from Postgres via CDC sync. Cursors stored in BitmapFs track the replication position so only the delta needs to be replayed.

---

## Cache Persistence (APPROVED, not yet implemented)

A design for persisting the unified cache to disk exists in `docs/design/design-unified-cache-persistence.md` but has not been built. The key idea is a `BoundStore` with `meta.bin` + `.ucpack` shard files that would enable warm cache restarts, eliminating the cold-start penalty where the first query against a sort field must traverse all bitmaps to build the bound cache.

---

## Measured Performance

| Metric | Value |
|--------|-------|
| DocStore on disk (100M records) | ~6 GB |
| Shard count (105M records) | ~205K shards |
| Shard size (compressed) | ~75 KB average |
| Startup time | instant (<1s) |
| Lazy load: nsfwLevel (7 values) | 87ms |
| Lazy load: reactionCount sort (32 layers) | 84ms |
| Lazy load: sortAt sort (105M, 32 layers) | 22s |
| Lazy load: tagIds (31K values, 79% of memory) | 6.6s |
| BulkWriter throughput | 290K docs/s |
| Fused parse+bitmap loader sustained | 320-460K docs/s |

If queries never touch a field, its bitmaps never enter memory. The tagIds field (31K distinct values, 79-80% of filter bitmap memory at every scale) is only loaded when a query first filters on a tag.
