# Plan: PG Loader Scatter-Gather Rewrite

**Status:** Draft
**Problem:** `bitdex-pg-sync load` OOMs at 48 GB loading ~107M records
**Root cause:** `HashMap<u32, ImageScalars>` holds ~25 GB in memory (URL/hash strings + HashMap overhead). See `docs/pg-loader-oom-analysis.md` for the full breakdown.
**Goal:** Peak memory under 12 GB. Scale with disk, not RAM.

---

## Design: Two-Phase Scatter-Gather Pipeline

Replace the current "accumulate everything in memory, finalize at the end" pipeline with a disk-staged approach. The filesystem acts as an intermediate scratch area, organized to match the final access pattern.

### Phase 1 — Scatter (CSV -> Scratch Shards)

Stream each Postgres CSV and route every row to a scratch shard file on disk. Shard assignment: `shard_id = slot >> 9` (512 slots per shard, ~205K shards for 107M records). Each shard file is append-only.

**Processing order:**

1. Load enrichment lookup HashMaps from CSVs (Post, ModelVersion, Model) — needed to resolve foreign keys during scatter. ~1.5 GB total.
2. Stream `images.csv` — for each row, enrich from Post lookup, write a scratch tuple containing all scalar fields (url, hash, nsfwLevel, userId, sortAt, etc.) to the shard file for `slot >> 9`. Also build scalar filter bitmaps and sort bitmaps directly (nsfwLevel, userId, type, etc.) and the alive bitmap — same as today.
3. **Drop `post_map`** — no longer needed.
4. Stream `tags.csv` — for each `(tag_id, image_id)` row, write a scratch tuple to the shard file. Also build tagIds filter bitmaps — same as today.
5. Stream `tools.csv` — same pattern, toolIds filter bitmaps.
6. Stream `techniques.csv` — same pattern, techniqueIds filter bitmaps.
7. Stream `resources.csv` — enrich from MV/Model lookups, write scratch tuples. Build modelVersionIds + baseModel filter bitmaps.
8. **Drop `mv_map`, `model_map`** — no longer needed.
9. Merge all bitmap accumulators, apply to engine staging in a single pass.
10. Save bitmap snapshot to disk.
11. **Drop bitmap accumulators and engine staging.** Bitmaps are persisted — no reason to hold ~8 GB in memory during Phase 2. The engine lazy-loads them back on first query (existing startup path).

**Memory at peak during Phase 1:** enrichment lookups (~1.5 GB) + bitmap accumulators (~8 GB) + CSV read buffers (~4 MB) + file handle LRU. No per-image scalar accumulation.

**Key difference from current code:** Image scalars (url, hash, etc.) go to disk immediately instead of into a 25 GB HashMap. Filter/sort bitmaps are still built exactly as today — no change to the bitmap accumulation logic.

**Important: Do not delete CSV files.** CSVs stay on disk until the full load is verified. If the pod dies mid-Phase 2, scatter can resume from CSVs without re-downloading from Postgres. Only scratch shard files are cleaned up, and only after docstore completion.

### Phase 2 — Gather + Emit (Scratch Shards -> DocStore)

Process scratch shards in parallel via rayon. Each worker:

1. Reads one scratch shard file into memory (~50-200 KB).
2. Sorts tuples by slot_id locally (max 512 slots, fits in L2 cache).
3. Groups tuples by slot. For each slot, assembles the full document: scalar fields from image tuples, multi-value fields from tag/tool/technique/resource tuples.
4. Encodes the document via `BulkWriter::encode_json()` (reuse existing).
5. Sends encoded docs to a bounded channel for docstore writing.
6. Drops the shard buffer.

A writer thread drains the bounded channel and calls `BulkWriter::write_batch_encoded()`. Backpressure: workers block when the channel is full.

**Memory at peak during Phase 2:** rayon thread pool × shard buffer size (~200 KB × 8 threads = ~1.6 MB) + bounded channel buffer + docstore write buffers. Negligible.

**Note:** Bitmaps are saved to disk and accumulators dropped between phases. Phase 2 only produces docstore shards — it does not touch bitmaps. Memory during Phase 2 is just rayon workers + docstore write buffers.

---

## Scratch Shard File Format

Binary, append-only, self-describing per-tuple.

```
Tuple layout:
  [1 byte]  field_tag     — identifies the source table/field
  [4 bytes] slot_id       — u32 LE
  [N bytes] payload       — field_tag-specific, compact

Field tags:
  0x01  ImageScalars    — fixed-size payload:
                           u8 nsfw_level
                           u64 user_id
                           u8 image_type
                           u64 sort_at
                           u8 poi | minor | has_meta | on_site (packed flags byte)
                           u64 post_id
                           u64 posted_to_id
                           u8 availability
                           u8 blocked_for
                           u64 published_at_ms
                           u8 url_len, [url_len bytes] url
                           u8 hash_len, [hash_len bytes] hash
                           (variable size, typically ~120 bytes)

  0x02  TagId           — u32 tag_id (4 bytes)
  0x03  ToolId          — u32 tool_id (4 bytes)
  0x04  TechniqueId     — u32 technique_id (4 bytes)
  0x05  ModelVersionId  — u32 mv_id, u8 detected (5 bytes)
  0x06  BaseModel       — u8 base_model_enum (1 byte)
  0x07  ResourcePoi     — (0 bytes, tag presence is the signal)
```

Each shard file covers 512 slot IDs. Total scratch disk usage: ~60-90 GB (temporary, deleted after Phase 2 completes).

### Volume: Tags Dominate Everything

The `tags.csv` is **5.4 billion rows / 63 GB** — over 50x the image count (many-to-many relationship). This is the dominant scatter workload by far. Each tag row writes a 9-byte tuple (1 tag + 4 slot + 4 tag_id) to a shard file, producing ~27 GB of scratch shard data from tags alone. The file handle LRU and write buffering must be tested against this volume specifically. Images.csv (~107M rows) is comparatively easy.

### The Scatter IS the Sort

Do not pre-sort CSVs. Sorting buys nothing over scatter-gather and would double disk usage for the 63 GB tags CSV. The shard bucketing achieves per-shard locality in a single streaming pass. The in-shard sort during Phase 2 gather is trivial — at most 512 slots worth of tuples, fits in L2 cache.

---

## Backpressure Mechanisms

### Phase 1: File Handle LRU

205K shard files, but OS limits open file descriptors. Maintain an LRU pool of ~1024 open file handles. Evict least-recently-used with flush + close. Reopen in append mode on next write. CSV rows for adjacent IDs cluster by shard, so the LRU won't thrash badly.

### Phase 2: Bounded DocStore Channel

Workers send encoded `(slot, Vec<u8>)` docs to a `crossbeam::channel::bounded(N)` channel. A single writer thread drains and calls `write_batch_encoded()`. Workers block when the channel is full. This mirrors the existing NDJSON loader pattern (`sync_channel::<BitmapAccum>(2)` + writer thread cap).

---

## Reusable Components from Existing Code

| Component | Location | Reuse |
|---|---|---|
| `BitmapAccum` fold/reduce + merge | `src/loader.rs:43-235` | Direct reuse for Phase 1 bitmap building |
| `BulkWriter::encode_json()` | `src/docstore.rs:941` | Direct reuse for Phase 2 doc encoding |
| `BulkWriter::write_batch_encoded()` | `src/docstore.rs:859` | Direct reuse for Phase 2 docstore writes |
| `apply_bitmap_maps()` | `src/concurrent_engine.rs:4090` | Direct reuse for Phase 1 staging apply |
| `copy_streams::build_image_bitmaps()` | `src/pg_sync/copy_streams.rs:273` | Direct reuse for Phase 1 image bitmap building |
| CSV parsing functions | `src/pg_sync/copy_queries.rs` | Direct reuse for all CSV parsing |
| `download_all_tables()` | `src/pg_sync/bulk_loader.rs:372` | Keep as-is (Phase 0) |
| Enrichment lookup loading | `src/pg_sync/bulk_loader.rs:488-537` | Keep as-is (post_map, mv_map, model_map) |
| `cleanup_orphan_bitmaps()` | `src/pg_sync/bulk_loader.rs:1120` | Keep as-is (AND enrichment bitmaps against alive) |

The NDJSON loader (`src/loader.rs:249-516`) demonstrates the target architecture:
- 3-stage pipeline: reader thread -> rayon parse+encode -> apply+write
- `sync_channel` backpressure between stages
- Writer thread cap (`ds_handles.len() >= writer_cap`) for docstore backpressure
- Single staging clone, bitmap application during streaming (not at the end)

---

## What Changes vs Current Code

**Deleted:**
- `ImageScalars` struct and `HashMap<u32, ImageScalars>` — replaced by scratch shard files
- `ResourceEnrichment` struct and `HashMap<u32, ResourceEnrichment>` — replaced by scratch tuples
- `finalize_from_bitmaps()` — replaced by Phase 2 gather
- `scalars_to_json()` — replaced by Phase 2 doc assembly from scratch tuples
- Second `clone_staging()` + `apply_bitmap_maps()` call for multi-value bitmaps — single apply instead

**New:**
- `ScratchWriter` — append-only binary writer with file handle LRU
- `ScratchReader` — reads a shard file, sorts by slot, yields grouped tuples
- `scatter_images()` — streams images.csv, writes scratch tuples + builds bitmaps
- `scatter_tags/tools/techniques/resources()` — streams enrichment CSVs, writes scratch tuples + builds bitmaps
- `gather_shards()` — rayon parallel shard processing, doc assembly, bounded docstore writes

**Unchanged:**
- Phase 0 (CSV download from PG)
- All bitmap accumulation logic
- Engine initialization, loading mode, snapshot save
- Enrichment lookup loading

---

## Memory Budget at 107M Records

| Phase | Component | Memory |
|---|---|---|
| Phase 1 | Enrichment lookups (post/mv/model) | ~1.5 GB |
| Phase 1 | Bitmap accumulators (all fields) | ~8 GB |
| Phase 1 | CSV read buffers | ~4 MB |
| Phase 1 | File handle LRU (1024 handles) | negligible |
| Phase 1 | **Peak** | **~10 GB** |
| Phase 2 | Rayon worker buffers (8 × 200 KB) | ~1.6 MB |
| Phase 2 | Bounded channel buffer | ~10 MB |
| Phase 2 | **Peak** | **< 1 GB** |
| Between phases | Bitmap snapshot saved, accumulators dropped | ~0 GB (freed) |
| Steady state | Published bitmaps in engine (lazy-loaded on first query) | ~7.5 GB |

Total peak: ~10 GB during Phase 1 (bitmap building). Phase 2 runs under 1 GB because bitmaps are persisted and dropped between phases. Well within 48 GB with room for future growth.

---

## Disk Budget

| Item | Size | Lifetime |
|---|---|---|
| Downloaded CSV files (tags.csv = 63 GB) | ~100 GB | Retained until load verified, then deleted manually |
| Scratch shard files | ~60-90 GB | Deleted after Phase 2 completes |
| Bitmap snapshot (.fpack/.sort/.roar) | ~7 GB | Permanent |
| DocStore shards (zstd msgpack) | ~15-20 GB | Permanent |

Peak disk usage during loading: ~180-220 GB (CSVs + scratch + permanent). PVC must be provisioned for this. After load verification, deleting CSVs reclaims ~100 GB.

---

## Implementation Order

1. **`ScratchWriter` + `ScratchReader`** — the new primitive. Write scratch tuple format, file handle LRU, shard-based read+sort+group. Unit test with small data.
2. **`scatter_*` functions** — adapt existing CSV processing loops to write scratch tuples instead of HashMap entries. Bitmap building stays inline (no change). Integration test: verify shard files contain correct data.
3. **`gather_shards`** — rayon parallel shard processing with bounded channel to BulkWriter. Integration test: verify docstore output matches current pipeline.
4. **Wire into `run_bulk_load_copy`** — replace the current Phase 2-6 with scatter+gather. Keep Phase 0 (download) and Phase 1 (enrichment loading) as-is. Drop HashMap path.
5. **Test at scale** — run against 107M dataset, verify peak RSS < 12 GB, docstore output matches, bitmaps correct. **Focus benchmarking and memory testing on the tags scatter** (5.4B rows, 63 GB CSV, ~27 GB scratch output) — this is the dominant workload and the stress test for file handle LRU + write buffering.
6. **Delete dead code** — `ImageScalars`, `ResourceEnrichment`, `finalize_from_bitmaps`, `scalars_to_json`, old two-pass apply.

---

## Why Not the Previous Approaches

**SlotArena (mmap):** Appeared disk-backed but mmap dirty pages consumed RSS. The kernel tracks every written page in physical memory; under pressure, the OOM killer fires before writeback completes. 107M × 512 bytes = 55 GB of dirty pages.

**Arena-free HashMap:** Eliminated the mmap but replaced it with 25 GB of HashMap + heap strings. Better than 55 GB but still OOMs at 48 GB.

**Scatter-gather:** Explicit `write()` calls produce clean pages the kernel can flush and reclaim freely. Reads are bounded (one shard at a time). Memory is controlled by the application, not the kernel's page cache heuristics.
