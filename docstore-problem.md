## Problem: Bulk Document Storage During High-Speed CSV Ingestion

We have a bitmap index engine in Rust that ingests CSV data (PG COPY output) at 100M+ row scale. The system builds roaring bitmap indexes AND stores document field values on disk for later retrieval.

### What we are ingesting
- ~107M image records from a 14GB CSV (headerless, comma-delimited)
- Each record has ~15 fields (mix of integers, strings, booleans)
- Fields include: id, url, nsfwLevel, hash, type, userId, postId, computed fields like hasMeta, sort fields like publishedAt
- Some fields come from enrichment lookups (join on postId to posts.csv to get publishedAt, availability)
- Processing is parallelized with rayon (28 threads), each thread gets a byte-range of the mmap'd CSV

### The bitmap side (fast, not the problem)
- Filter bitmaps: one RoaringBitmap per distinct value per field
- Sort bitmaps: 32 bit-layer bitmaps per numeric sort field
- Building bitmaps from parsed rows takes ~23 thread-seconds for 10M rows - fast

### The document storage side (slow, THE problem)
- Each row needs its ~15 field values persisted to disk for later document retrieval
- Currently using an append-only V2 tuple log format: each field written as a separate tuple to per-shard files via BufWriter
- Sharding: slot_id >> 9 = shard_id (512 docs per shard). With 107M rows, that is ~210K shard files
- Each field value is ~5-10 bytes (msgpack-encoded)
- Per-row: serialize 15 fields individually, write to the shard file
- Measured: 486 thread-seconds for 10M rows - 85% of total row processing time is docstore writes
- Breakdown: serialization = 22s, actual write = 486s (serialization is not the bottleneck)

### What we have tried
- Batching all 15 fields into one write call per row: reduced lock acquisitions 15x but only 5% improvement
- Larger BufWriter buffers (8KB to 256KB): 24-30% improvement
- Pre-creating shard files vs lazy creation: 2x improvement in microbench
- Larger shard sizes (512 to 4096 docs/shard): 31% improvement
- DashMap vs Vec for shard writer lookup: no difference single-threaded

### Constraints
- Documents must be retrievable by slot_id after ingestion (for serving alongside query results)
- The system has a ShardStore framework with SnapshotCodec + OpCodec traits used for bitmap persistence already. It supports snapshot section (full state) + append-only ops log per shard file, with generation management and compaction
- During bulk load, all 107M documents are new (no updates/merges needed)
- IDs in the CSV are roughly but not perfectly sequential (~12% out-of-order jumps)
- Memory budget: reasonable (current peak is ~32GB RSS during ingestion, machine has 64GB)
- The bitmap processing and document storage happen in the same rayon parallel loop

### What we want
- Get document storage from 486 thread-seconds down to something comparable to bitmap processing (73 thread-seconds)
- Open to re-architecting the document storage format, changing shard sizes, using the ShardStore snapshot system, or any other approach
- Must still support single-document retrieval by slot_id after bulk load completes
