## Follow-up: Read-Side Constraints for Document Storage

Thanks for the proposals. Before we commit to an approach, here is the read-side context that drove the current design.

### How documents are retrieved

The bitmap index engine processes queries like: "find images where nsfwLevel=2 AND tagIds includes 42, sorted by reactionCount descending, limit 50"

The engine resolves this entirely via bitmap operations, producing an ordered Vec of slot_ids (e.g., [8234112, 7891023, 6543210, ...]). Then optionally, the caller wants the full document fields for those IDs to return in the API response.

So the retrieval pattern is:
- Given a list of 20-200 slot_ids from a query result
- Fetch the document fields for each slot_id
- slot_ids are scattered across the full 0..107M range (not contiguous)
- This happens on every query, latency-sensitive (target: sub-ms per doc after cache)

### Why 512-doc shards existed

The original design used small shards so that:
1. Reading one document only requires loading a small file (~75KB for 512 docs), not a multi-MB partition
2. The OS page cache naturally caches hot shards (popular images get their shard cached)
3. Upserts (single-document updates in steady state) only need to read/write a small file
4. The sharded hex-directory structure keeps any single directory under ~1000 files

### In-memory document cache

We have a DashMap-based in-memory cache (DocCache) that caches individual documents after first read. Cache hit is sub-microsecond. Cache miss goes to disk. At steady state, most queries hit cache. The disk read path matters mainly for:
- Cold start (first queries after restart before cache warms)
- Cache eviction (LRU, 1GB budget)
- Long-tail queries hitting rarely-accessed documents

### Upsert path (steady state, not bulk load)

During normal operation (not bulk loading), the system receives individual document upserts via an outbox poller:
- Read old document from disk (or cache)
- Diff old vs new fields
- Update only changed bitmaps
- Write updated document back to disk

This read-modify-write cycle benefits from small shards because only the affected shard needs to be read and rewritten.

### What this means for your proposals

The bulk load path (107M all-new inserts) and the steady-state path (individual upserts) have very different I/O patterns. The proposals you gave optimize bulk load perfectly but we need to make sure the resulting format still supports:

1. Fast random point lookup by slot_id (scattered IDs, not contiguous)
2. Efficient single-document upsert (read old doc, write new doc)
3. Reasonable cold-start behavior (not needing to load giant files for first few queries)

Questions:
- With the WAL + offset index approach: how would single-doc upserts work? Append a new version and update the index? That creates a compaction problem over time.
- With large partitions (256K-1M docs): a single upsert means reading and rewriting a 10-50MB partition file?
- Is there a way to use one format for bulk load (fast sequential writes) and a different optimized format for serving (fast random reads + upserts), with a conversion step between them?
