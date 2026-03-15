# DocStore V2: Always-Appendable BitTuple Log

> Shards are always-appendable raw msgpack tuple logs. No compression — zstd only achieves 1.8x on our data while adding 340 us of decompression per read. Raw tuple scans are 2.6x faster than compressed reads at 21 us/doc (8 threads, real Civitai data). Writes are free appends at 512 MB/s. Background compaction deduplicates stale tuples via atomic swap. OS page cache manages hot-set caching automatically. Upserts append; newest tuple wins on read.

---

## Problem

DocStore V1 stores complete, compressed documents per slot. Each `write_shard_file` call compresses the entire shard and rewrites it atomically. This blocks parallel CSV loading (55% of gather time is docstore writes) and makes upserts expensive (read-decompress-merge-compress-write per shard).

## Design

Shards are append-only logs of `(slot_id, field_idx, value)` tuples. Always writable. No compression. Reads scan the log and assemble documents on demand — newest tuple per (slot, field) wins.

### Shard Format

```
[Header: 16 bytes, uncompressed, always readable]
  u32 LE: magic = 0x42445832 ("BDX2")
  u32 LE: version = 2
  u32 LE: flags = 0 (reserved)
  u32 LE: num_tuples (updated on compaction, 0 otherwise)

[Tuple log: append-only, uncompressed]
  For each tuple:
    u32 LE: slot_id
    u16 LE: field_idx     (from shared field dictionary at docs/meta/field_dict.bin)
    u16 LE: value_length  (bytes of msgpack-encoded value)
    [value_length bytes]: msgpack-encoded PackedValue
```

Each tuple is 8 + value_length bytes. A tag assignment: 11 bytes. A URL string: 8 + 2 + url_length bytes.

### Write Path

Append tuple bytes to the end of the shard file. Per-shard `DashMap<u32, Mutex<BufWriter<File>>>` serializes concurrent writes to the same shard. Contention measured at 0.3-1.1% with 8 threads across 2048 shards (L3 benchmark). Files opened lazily on first write, header written once.

### Read Path

Read the entire shard file, skip the 16-byte header, scan tuples in reverse (LIFO). For the target slot_id, collect the first occurrence of each field_idx (newest wins). Assemble a `StoredDoc` from the collected fields. Truncation-safe: the parser stops at EOF or an incomplete tuple header.

### Compaction

Background worker picks up shards that have accumulated stale tuples (from upserts or deletes). Reads all tuples, deduplicates per (slot_id, field_idx) keeping the newest, writes a clean shard to a `.tmp` file, atomically renames over the original (`remove_file` + `rename` for Windows safety). Updates `num_tuples` in the header.

Compaction is optional cleanup, not a required step. Shards work correctly with stale tuples — LIFO read handles dedup. Compaction reduces disk usage and read scan time.

**Measured:** 4.1 ms per shard, parallelizable to 30 seconds total across 28 threads for 205K shards.

### V1 Backward Compatibility

The reader checks the first 4 bytes of any shard file. If the magic is `BDX2`, it uses the V2 tuple scan reader. Otherwise it falls back to the V1 decoder (index + zstd). Existing V1 shards continue to work without migration.

---

## Why No Compression

Benchmarks showed compression is not worth the cost for our data:

| Format | Size/shard | Ratio | Write speed | Read speed (single doc) |
|--------|-----------|-------|-------------|------------------------|
| Raw msgpack (V2) | 191 KB | 1.0x | 512 MB/s (append) | **4 us** |
| Zstd level 1 (V1) | 98 KB | 1.8x | 344K docs/s | **367 us** |

Zstd achieves only 1.8x compression on our integer-heavy msgpack data. The decompression cost (340 us per shard) dominates read latency. Raw tuple scans are 92x faster for single docs and 2.6x faster for batch reads of 20 docs across 8 threads.

Disk trade-off: ~62 GB uncompressed vs ~20 GB compressed at 107M records. On NVMe, 42 GB extra is irrelevant.

---

## Concurrent Read Performance (measured, real Civitai data)

239K real shards, 1000 iterations, 8 threads:

| Scenario | V1 compressed | V2 tuple scan | Speedup |
|----------|--------------|---------------|---------|
| 100 docs across 100 shards | 56 us/doc | 21 us/doc | **2.6x** |
| 100 docs from 1 shard | 6.2 us/doc | 4.2 us/doc | **1.5x** |
| 20 docs (typical query) | ~350 us total | ~87 us total | **4x** |

Page-cached shards (hot queries): ~4 us per doc. No decompression overhead.

---

## Loading Pipeline

```
For each CSV (tags, images, resources, tools, techniques):
  Rayon block reader → parse rows →
    build filter/sort bitmaps (same as current)
    append_tuple_raw per field to docstore shards (free, no compression)
  Save bitmaps to BitmapFs per field, drop from memory

After all CSVs: flush_v2_writers()
Bitmaps already on disk. Docstore already on disk. Done.
```

No scatter phase. No gather phase. No scratch files. Each CSV row goes directly to bitmaps + docstore in one pass.

### Memory During Loading

- Enrichment lookups: ~1.3 GB (posts, model_versions, models)
- Current CSV's bitmaps: ~5.1 GB max (tagIds)
- File handles: 205K shards × DashMap<Mutex<BufWriter>> = ~20 MB
- **Peak: ~7 GB**

---

## Thread Safety

Per-shard `DashMap<u32, Mutex<BufWriter<File>>>`. Each shard gets its own mutex. Contention is low because 205K shards >> thread count.

**Measured (L3):** 0.3-1.1% contention at 8 threads, 2048 shards. The shared DashMap approach is 2.7x faster than per-thread exclusive files due to file handle overhead.

Reads require no locking — append-only files are safe for concurrent `fs::read`.

---

## Disk Layout

```
docs/
  meta/
    field_dict.bin          # field name <-> u16 (shared, loaded once at startup)
    schema_history.bin      # version -> defaults (same as V1)
  shards/
    00/
      000000.bin            # V2 tuple log (or V1 compressed — auto-detected)
      000001.bin
      ...
    01/
      ...
```

Same hex-nested directory structure as V1. V2 shards live in the same location. The reader auto-detects format per file.

---

## Validated Assumptions

All benchmarks run against real Civitai data (107M records, 239K shards) or realistic synthetic equivalents.

| ID | What | Expected | Measured | Status |
|----|------|----------|----------|--------|
| A1 | Lock-free appends (8 threads, O_APPEND) | >500 MB/s, 0 corruption | 512 MB/s, 0 corruption | **PASS** |
| A3 | Compaction speed per shard | <0.5 ms | 4.1 ms (30s @ 28 threads) | **PASS** |
| L3 | Per-shard mutex contention | <1% | 0.3-1.1% | **PASS** |
| — | Compression ratio | 3-4x assumed | **1.8x actual** | Not worth it |
| — | LIFO read vs compressed | comparable? | **92x faster** (4 us vs 367 us) | V2 wins |
| — | Concurrent reads (100 docs, 8T) | ? | **2.6x faster** (21 us vs 56 us) | V2 wins |

---

## Migration

V1 shards continue working. The reader auto-detects format. New loads produce V2 shards. No migration command needed for existing data — V1 shards serve until they're naturally replaced by a fresh load.

Future: a `compact --migrate` command could convert V1 shards to V2 format shard-by-shard for uniformity, but this is not required for correctness.

---

## Comparison

| | V1 (compressed) | V2 (tuple log) |
|---|---|---|
| Format | sorted index + zstd blob | append-only raw tuples |
| Write | read-modify-write (2 ms/shard) | append (free) |
| Read (single doc) | 367 us (decompress) | 4 us (scan) |
| Read (20 docs, 8 threads) | ~350 us | ~87 us |
| Upsert | full shard rewrite | append one tuple |
| Concurrent writes | Mutex + RMW | Mutex + append |
| Disk size (107M records) | ~20 GB | ~62 GB |
| Parallel CSV loading | Blocked | Enabled |
| Compaction | N/A (always compressed) | Background dedup, optional |
