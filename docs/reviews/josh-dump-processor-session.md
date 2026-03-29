# Josh's Dump Processor Session -- Docstore Performance & the Road to Data Silos

**Agent:** Josh (josh-dump-processor)
**Session:** ccfdbb8c-9a39-4581-a292-ef398549654b
**Date:** 2026-03-27 to 2026-03-28
**Reviewer:** Conversation Reviewer (2026-03-28)

---

## Context

Josh built and optimized `src/dump_processor.rs`, a config-driven CSV/TSV bulk loader for BitDex's Sync V2 pipeline. The session processed the full Civitai dataset: 4.73B rows across 6 phases (tags, images, resources, tools, techniques, metrics). The core tension throughout the session was that bitmap operations are fast but docstore writes are slow, and the two share a single pipeline.

---

## 1. The Docstore Write Bottleneck

The fundamental finding: **docstore I/O dominated wall-clock time for multi-value phases**, particularly tags (4.48B rows). Bitmap parsing and merging ran at 26-37M rows/sec across 28 Rayon threads, but the moment docstore writes entered the picture, throughput collapsed.

The root cause is architectural. BulkWriter uses a `DashMap<shard_id, Mutex<BufWriter<File>>>` structure. Each docstore write requires:
1. Hash the slot ID to find the shard
2. Lock the shard's Mutex
3. Encode the tuple (field index + value bytes)
4. Write through BufWriter to the underlying file

At 5.4B rows (tags phase), this means 5.4B Mutex acquisitions through a shared DashMap. Even with sharding, the contention from 28 concurrent Rayon threads hitting the same shard set creates severe lock congestion.

**Key measurement:** Without docstore writes, tags parsed at ~37M rows/sec. With per-row docstore writes, throughput dropped to ~3M rows/sec. Docstore I/O consumed roughly 85% of per-row time.

---

## 2. Approaches Tried and Their Outcomes

### Approach 1: Parallel Bucket Writes + Zero-Clone ShardStore (worked)

**What:** Bypassed `write_full_filter` to do bucketing + parallel writes directly via `into_par_iter`. Added `write_filter_bucket_raw` that encodes directly from `&[(u64, &RoaringBitmap)]` without creating intermediate BucketSnapshot or cloning bitmaps.

**Result:** Total time dropped from 18m26s to **16m03s**. Resources phase improved 2x (1m48s to 51s). This was the best baseline before docstore optimization attempts.

**Why it worked:** The old path cloned every bitmap into a BucketSnapshot (owned HashMap), then serialized. The new path encodes from references. The encoder only borrows -- the clone was unnecessary overhead baked into the API contract.

### Approach 2: Pipeline Save Overlap (backfired)

**What:** Moved the `prev_save_handle.join()` from before processing to before saving, so the next phase's processing could overlap with the prior phase's disk save.

**Result:** Tags improved 12s (from zero-alloc parse fix bundled with it), but every other phase regressed. Images went from 6m10s to 7m23s (+73s). Total regressed from 16m03s to 18m16s.

**Why it failed:** Disk I/O contention. Tags bitmap save (45s of writes) ran concurrently with images enrichment (47s of reads loading posts.csv). Both hammered the same disk. The overlap turned sequential I/O into random I/O. This is a fundamental constraint on single-disk systems -- you cannot pipeline readers and writers without an I/O scheduler or separate storage devices.

### Approach 3: Channel-Based Docstore Writer (failed)

**What:** Spawned a dedicated writer thread. Rayon parse threads sent `(slot_id, field_idx, value_bytes)` tuples through a bounded crossbeam channel (100K capacity). Writer thread drained and wrote to BulkWriter.

**Result:** Worse than direct writes. At 5.4B rows, the single writer thread became the bottleneck. 28 parse threads filled the bounded channel instantly, then blocked on `send()`. Effective parallelism dropped to 1 core.

**Why it failed:** A single consumer can never keep pace with 28 producers at this scale. The channel just moved the bottleneck from Mutex contention to channel backpressure. CPU utilization dropped from 28 cores to ~10.

### Approach 4: Batched Channel Writes (marginal improvement, still too slow)

**What:** Each Rayon thread accumulated 10K tuples locally, then sent the batch as a Vec through the channel. Reduced channel operations from 5.4B to 540K.

**Result:** Throughput climbed from ~1.9M/s to ~3.3M/s. Still 10x slower than bitmap-only parsing (37M/s). The writer thread was still single-threaded, just receiving bigger chunks.

**Why it was insufficient:** Batching reduced channel overhead but didn't solve the core problem: a single writer thread doing per-tuple Mutex acquisitions on BulkWriter. The I/O itself (BufWriter flush, fsync) was the floor, not the channel.

### Approach 5: Direct Writes with Buffer Reuse (abandoned)

**What:** Attempted to eliminate per-row `Vec` allocation in `PackedValue::Mi(vec![value])` by hand-encoding msgpack bytes into a reusable buffer.

**Result:** Abandoned during implementation. The complexity of hand-encoding msgpack for multi-value append wasn't justified when the Mutex contention remained the dominant cost.

### Approach 6: Config-Driven filter_only Skip (shipped)

**What:** Fields marked `filter_only: true` in the data schema skip docstore writes entirely. The dump processor reads the DataSchema from IndexDefinition, filters filter_only fields out of BulkWriter's field list, and `field_to_idx().get(target)` returns None for those fields -- docstore writes are skipped naturally with no special-case code.

**Result:** Tags phase dropped from ~25-30 minutes (with docstore) to **177s** (bitmap only). Total pipeline: **11m21s** at 7.0M rows/sec. This was the final shipped configuration.

**Why it worked:** It acknowledged the architectural reality -- tags exist only for bitmap filtering, not document retrieval. Writing 5.4B docstore tuples for data that will never be read from docstore is pure waste.

---

## 3. The filter_only Decision in Depth

The `filter_only` flag was not a hack. It reflected a genuine data classification insight:

- **Tags** (4.48B join rows): Used exclusively for bitmap intersection (`tagIds IN [...]`). No query ever retrieves "what tags does image X have?" from BitDex -- that comes from Postgres. Writing tags to docstore would consume ~65GB of encoded tuples that are never read.
- **Tools, Techniques** (4-6M rows each): Same pattern as tags but small enough that docstore writes are negligible (~3 seconds total).
- **Images** (107M rows): Fields like nsfwLevel, type, userId need to be in docstore for document retrieval alongside query results. These are NOT filter_only.

The decision was config-driven: the data schema's `field_mapping` entries carry `filter_only: boolean`. The dump processor honors this without hardcoding field names. Any future field can be marked filter_only without code changes.

Justin's feedback was explicit on this point: "It should be driven by the config, not something that gets passed in by the docstore, since the config says that it's filter_only."

---

## 4. Shard Count and Performance

The session revealed that shard count interacts with two competing concerns:

1. **Parallelism during writes:** More shards = less Mutex contention = more concurrent writers. The parallel bucket write optimization (Approach 1) exploited this -- resources phase went 2x faster when buckets were written in parallel via `into_par_iter`.

2. **I/O amplification during reads:** Each ShardStore shard is a separate file. Sort layers went from 1 file (BitmapFs packed all 32 bit layers) to 32 separate files (ShardStore, one per bit layer). Alexandra later fixed this by packing sort layers back into single shards (32 writes to 1 write per field).

3. **Docstore shards:** BulkWriter shards the docstore by slot ID into hex-nested directories (~256 shards). At 107M records, each shard handles ~420K records. The DashMap lookup + Mutex acquisition per shard is the bottleneck at billions of rows.

The general finding: shard counts that work well for querying (many small files for lazy loading) create write amplification during bulk loads. This tension is structural.

---

## 5. Design Decisions Relevant to Future Storage

### V2 Docstore Is Still Active (and Shouldn't Be)

A significant finding: despite a V3 docstore design doc existing (`docs/design/docstore-v3-oplog.md`) and the ShardStore being the intended storage layer, the dump processor still writes through the V2 BulkWriter path with per-shard BufWriters. Justin was surprised: "Did the V3 doc not get implemented? I don't know why we even still have the V2 stuff."

A scout assessed the migration scope at ~1000 lines across 3-4 weeks. The V3 design uses ShardStore's `DocOp::Append` which would allow append-only writes without per-shard Mutex contention. This is the path forward but hasn't been wired in.

### BulkWriter Flush Gap

Josh discovered that BulkWriter never explicitly calls `flush_v2_writers()`. It relies on `BufWriter::Drop` to flush, which is indirect and depends on all Arc references being dropped. This could cause data loss if the process crashes before the Arc refcount reaches zero. Not a production issue yet (the dump processor's Arc goes out of scope cleanly) but a correctness gap.

### Enrichment I/O Is a Hidden Cost

Loading enrichment data (e.g., posts.csv at 22.8M rows for images phase) takes 47 seconds and 5.9GB of memory. This is sequential I/O that cannot overlap with bitmap saves without causing disk contention (Approach 2's failure). Future architectures should consider memory-mapping enrichment data or pre-loading it before the dump pipeline starts.

### LCS Dictionary Bug

Enrichment-derived fields (availability, baseModel) weren't getting their dictionaries populated because the processing loop only iterated `request.fields` (direct CSV columns), not enrichment targets. Josh fixed this by adding an `enrichment_targets` collection that processes enrichment-derived field values through the same dictionary encoding path. The fix exposed a broader pattern: enrichment fields are second-class citizens in the dump pipeline and need explicit handling at every stage.

---

## 6. What Led to the Conclusion That a New Approach Is Needed

The session told a clear story through elimination:

1. **Mutex sharding doesn't scale to billions of rows.** DashMap with per-shard Mutex worked at 100M rows but collapsed at 5.4B.
2. **Channel-based decoupling doesn't help when the consumer is the bottleneck.** Moving writes to a dedicated thread just moved the queue from the Mutex to the channel.
3. **Batching helps marginally but doesn't change the fundamental I/O cost.** 10K-tuple batches reduced overhead from 5.4B to 540K operations, but each batch still hit the same Mutex and BufWriter.
4. **Pipeline overlap fails on single-disk systems.** You can't overlap readers and writers without random I/O degradation.
5. **The only thing that worked was not writing.** filter_only eliminated the problem by acknowledging that not all data belongs in the docstore.

The implication for data silos: the bulk load path needs a fundamentally different write strategy than the online upsert path. Bulk loads should write large sequential blocks (not per-row tuples) with no locking. The ShardStore V3 DocOp::Append model points in this direction -- append-only ops logs that can be written lock-free per thread, then compacted later.

---

## 7. Performance Timeline (Chronological)

| Milestone | Total Time | Key Change |
|-----------|-----------|------------|
| Initial baseline | 18m26s | All 6 phases, sequential saves |
| Parallel bucket writes + zero-clone | **16m03s** | Rayon per-bucket, encode from refs |
| + Pipeline overlap (regressed) | 18m16s | Disk I/O contention killed it |
| + Overlap reverted, zero-alloc parse kept | 16m03s | Back to best |
| + Alexandra's inline save + remove per-phase reload | ~12-13m | Eliminated 8 min of redundant reloads |
| + filter_only skip for tags | **11m21s** | Tags docstore writes eliminated |
| Final validated result | **11m21s** | 4.73B rows, 7.0M/s aggregate |

---

## Key Takeaways for Data Silo Design

1. **Separate bulk and online write paths.** The BulkWriter's per-row Mutex model works for online upserts (low contention) but fails catastrophically at bulk scale. Data silos need a bulk-ingest mode with sequential, lock-free writes.

2. **Classify data by access pattern, not just by field.** filter_only was the right abstraction -- it separates "indexed for filtering" from "stored for retrieval." Data silos should formalize this classification.

3. **Don't pipeline I/O on shared storage.** Overlapping reads and writes on the same disk is worse than sequential. If data silos span multiple storage devices, pipelining becomes viable.

4. **Append-only logs beat random-access stores for bulk loads.** The V3 ShardStore DocOp::Append model is the right direction. Each thread appends to its own log segment, compaction merges later.

5. **Enrichment is a first-class concern.** Enrichment data (joins from external CSVs) should be pre-loaded or memory-mapped, not loaded inline during the dump pipeline. The 47-second enrichment load for images is avoidable overhead.
