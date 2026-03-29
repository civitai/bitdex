# Single-Pass V2 Loader

> CSV files processed in one pass: parse row, build bitmap, append V2 docstore tuple. No scatter, no scratch files, no gather phase. Bitmaps streamed to BitmapFs per-field, docstore tuples appended directly to V2 shard files. Peak memory ~5.5 GB.

---

## Architecture

Each CSV is processed independently with all 28 cores:

```
For each CSV (one at a time, biggest first):
  mmap file → split into N byte ranges (one per thread)
  Each thread: scan bytes → parse rows → insert into Vec<RoaringBitmap> + append V2 docstore tuples
  After all threads join: merge thread-local bitmaps (OR)
  Save merged bitmaps to BitmapFs → drop from memory
  Next CSV
```

No intermediate files. No scatter. No gather. The docstore V2 append-only format lets multiple CSVs write to the same shard file without read-modify-write.

## CSV Processing Order

| Step | CSV | Size | Bitmaps Built | Enrichment | Notes |
|------|-----|------|---------------|------------|-------|
| 1 | tags.csv | 63 GB, 4.48B rows | tagIds filter | None | mmap + 28-thread parallel, Vec<RoaringBitmap> |
| 2 | images.csv | 14 GB, 107M rows | nsfwLevel, userId, type, hasMeta, onSite, isPublished, availability, blockedFor + sort fields (sortAt, publishedAt, id) + alive bitmap | Post map (~400 MB) | Sequential scan, V2 tuple per field |
| 3 | resources.csv | 777 MB | baseModel, modelVersionIds | MV + Model maps (~900 MB) | Enriched from model_versions.csv + models.csv |
| 4 | tools.csv | 50 MB | toolIds | None | Simple two-column processor |
| 5 | techniques.csv | 71 MB | techniqueIds | None | Simple two-column processor |
| 6 | metrics.csv | 640 MB, 45M rows | reactionCount, commentCount, collectedCount sort bitmaps | None | ClickHouse TSV download |

Enrichment maps loaded lazily — only when the CSV that needs them is about to be processed, dropped immediately after.

## Memory Budget

| Phase | Component | Size |
|-------|-----------|------|
| Tags | Vec<RoaringBitmap> × 28 threads (300K entries each) | ~8 GB peak (thread-local, dropped after merge) |
| Tags | Merged roaring bitmaps (27K non-empty) | ~5.3 GB |
| Tags | Bitmap save to BitmapFs | 5.3 GB written, memory freed |
| Images | Post enrichment map | ~400 MB |
| Images | Filter + sort bitmaps | ~1.5 GB |
| Resources | MV + Model maps | ~900 MB |
| Metrics | ClickHouse metrics HashMap | ~1.3 GB |

Peak: **~8 GB during tag thread processing** (28 × 300K Vec<RoaringBitmap>). After tags complete and bitmaps save, memory drops to ~1.5 GB for subsequent steps.

## Measured Performance

### Tags (4.48 billion rows, 63 GB)

| Metric | Value |
|--------|-------|
| Aggregate throughput | **17.6M rows/sec** |
| Wall time | **254.5 seconds** |
| Threads | 28 (mmap byte-range split) |
| Distinct tags | 27,608 |
| Bitmap save | 12.2s (5.28 GB to BitmapFs) |

### Full Pipeline (estimated from measured components)

| Step | Estimated Time |
|------|---------------|
| Tags | ~255s |
| Images | ~60s |
| Resources | ~10s |
| Tools + Techniques | ~5s |
| Metrics | ~15s |
| **Total** | **~345s (~6 min)** |

## Learnings: Roaring Bitmap Insert Performance

### The core constraint

Roaring bitmap `insert(slot)` with random slot IDs across a 124M range costs **~2,000 ns (2 μs) per insert**. This is the fundamental bottleneck for bitmap-building workloads.

The cost is dominated by **L3 cache misses**. Each insert must:
1. Find the correct 16-bit container (binary search over containers array)
2. Check if the container exists, possibly create/convert it
3. Insert into the container's backing store (array or bitset)

With 300K bitmaps × 124M slot range, the working set far exceeds L3 cache (~30 MB). Every insert is effectively a cache miss at ~100-200 ns memory latency, plus the container logic overhead.

### Alternatives benchmarked

| Strategy | Single-thread | Multi-thread (28) | Notes |
|----------|--------------|-------------------|-------|
| **Direct roaring insert** | 0.5M/s (2024 ns) | **17.6M/s** (via mmap) | Current approach. Cache misses dominate. |
| **Vec push + sort + from_sorted_iter** | **36M/s** (27 ns) | Loses to merge cost | 36x faster single-threaded! But merge of 28 thread-local sets kills it. |
| **Batched Vec + periodic flush** | 29M/s parallel phase | 3.3M/s total (merge 4.3s) | Merge of 31K HashMaps from 8 threads takes 88% of total time. |
| **Shared DashMap<Mutex<RoaringBitmap>>** | 1.6M/s | 1.6M/s (no scaling) | Lock overhead > insert cost. Scales backwards at 28 threads. |
| **Pure Vec push (no bitmap)** | **144M/s** (6 ns) | — | Theoretical read+accumulate speed without bitmap overhead. |

### Key insight

The **merge** is the real bottleneck in multi-threaded bitmap building, not the insert. Parallelism helps the accumulation phase but creates a merge problem:
- 28 threads × 31K bitmaps = 868K bitmap OR operations during merge
- Each OR walks both bitmaps' container structure — expensive for large bitmaps
- Sequential merge means one core does all the work while 27 sit idle

### Why mmap + Vec<RoaringBitmap> wins

The mmap approach splits the file by byte range. Each thread processes a different region of slot IDs (due to CSV ordering by imageId), giving better cache locality than random access. The Vec<RoaringBitmap> direct-indexed by tag_id avoids HashMap hashing overhead. The merge is included in the 254s measurement — it's fast because each thread's bitmaps are relatively small (only slots from that byte range) and the OR operation is efficient for non-overlapping slot ranges.

### Future optimization paths

1. **Parallel merge**: Split the 31K tag IDs across 28 threads for the merge phase. Each thread merges ~1100 tags independently.
2. **Sorted input**: If tags.csv were sorted by imageId, each thread's inserts would be sequential within containers — potentially 10x fewer cache misses.
3. **Bitset accumulation**: Use raw `Vec<u64>` bitsets (15.5 MB per tag at 124M slots) during loading, convert to roaring after. Only viable for tags with >50% density (none qualify).
4. **Roaring bulk insert**: Accumulate slot IDs in a Vec, sort, call `from_sorted_iter` once per tag. 36x faster single-threaded, but need merge-free multi-threading.
5. **Tag-sharded threads**: Each thread owns a range of tag IDs. Rows for other tags get routed. Zero merge, but adds cross-thread communication.

## DocStore V2 Integration

The single-pass loader writes V2 docstore tuples via `append_tuple_raw()` during CSV processing. Each field becomes a separate tuple:

```
(slot_id: u32, field_idx: u16, value: msgpack bytes)
```

V2 shards are append-only — no compression, no freeze step. Multiple CSVs append to the same shard file concurrently (per-shard Mutex). The server reads documents by scanning the tuple log with LIFO ordering (newest tuple wins for each field).

Tags are NOT written to docstore during the tag CSV step — they'd generate 4.48B individual tuples. Instead, tag arrays are assembled per-image during the images CSV step.

## Comparison: Loader Evolution

| Version | Architecture | Tags Time | Total Time | Peak RSS | Memory Model |
|---------|-------------|-----------|------------|----------|-------------|
| V1 (HashMap accumulator) | PG COPY → HashMap → bitmaps | — | OOM at 40+ GB | 40+ GB | All in memory |
| V2 (Scatter-gather) | CSV → scratch shards → gather | 16 min scatter + 10 min gather | 28 min | 20.4 GB | Scratch files |
| V3 (Streaming save) | Same as V2 + stream to BitmapFs | Same | Same | **7.4 GB** | Per-field save+drop |
| **V4 (Single-pass)** | **mmap → parallel bitmap + V2 docstore** | **4.2 min** | **~6 min** | **~8 GB** | **No intermediate files** |

## Files

### Implementation
- `src/pg_sync/single_pass.rs` — Single-pass V2 loader implementation
- `src/pg_sync/scatter_gather.rs` — Legacy scatter-gather (still available as fallback)
- `examples/test_single_pass.rs` — Test harness for local runs

### Archived Benchmarks (`benches/loader-perf/`)

These are the microbenchmarks that produced the numbers in this doc. They're standalone Rust binaries (not part of the test suite) — copy into `scratch/src/bin/` to re-run.

- `bench_insert_strategy.rs` — Compares direct roaring insert vs Vec+sort+from_sorted_iter vs from_iter. Proved 36x single-thread speedup for deferred construction.
- `bench_tag_pipeline.rs` — Full pipeline microbench: pure insert, parse+insert, Vec alloc, multi-threaded scaling (1/4/8/16/28 threads), merge cost. Proved roaring insert is 2μs/op with random slots.
- `bench_batched_flush.rs` — Batched Vec push with periodic roaring flush, 8 real threads. Proved merge cost dominates (88% of total time).
- `bench_shared_bitmap.rs` — DashMap<Mutex<RoaringBitmap>> contention test. Proved shared concurrent bitmaps scale backwards at 28 threads.
