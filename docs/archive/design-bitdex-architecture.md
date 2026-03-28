# BitDex Architecture: Bitmaps, Caches, and Tuple Ingestion

This document captures the architectural vision for BitDex as a bitmap-indexed document store. It explains what the system is, how its parts connect, and where it's headed.

---

## What BitDex Is

BitDex is a bitmap index engine that stores documents and answers queries. Give it filter predicates, a sort field, and a limit. It returns an ordered list of IDs in microseconds.

Two structures make this work:

1. **The bitdex** — roaring bitmaps that answer "which documents match?" Filter bitmaps map `(field, value) -> set of slot IDs`. Sort bitmaps decompose numeric values into bit layers for top-N traversal.

2. **The docstore** — sharded, compressed document cache on disk. Once bitmaps identify the matching IDs, the docstore serves the actual content. NVMe random reads take microseconds; a single shard scan returns 512 documents in under a millisecond.

Bitmaps are the index. The docstore is the cache. Together they replace a traditional database for read-heavy filtered + sorted access patterns.

---

## Bit Tuples: The Universal Input

Every piece of data entering BitDex has the same shape:

```
(slot_id, field, value)
```

A tag assignment is `(image_42, "tagIds", 1234)`. A user ownership record is `(image_42, "userId", 9999)`. A sort timestamp is `(image_42, "sortAt", 1700000000)`. A URL for document serving is `(image_42, "url", "https://...")`.

This three-part structure — **slot, field, value** — is the atomic unit of ingestion. Everything a loader does reduces to producing these tuples and routing them to two destinations:

- **Bitmap fields** (filterable/sortable): the slot_id bit gets set in the bitmap for that (field, value) pair.
- **Document fields** (servable): the value gets written to the docstore shard for that slot_id.

Some fields are both. Some are bitmap-only (boolean filters). Some are document-only (url, hash). The schema configuration declares which is which.

---

## Bit Stacks: How Sorting Works

A sortable numeric field decomposes into N bitmaps, one per bit position. A 32-bit field uses 32 bitmaps. Together they form a **bit stack** — a stack of sieves.

To find the top-K documents by a value:

1. Start with the candidate set (the filter result bitmap).
2. At the most significant bit layer, AND with that layer's bitmap to keep only candidates with that bit set (for descending) or unset (for ascending).
3. If the result has enough candidates, narrow the set. If too few, skip that bit and move down.
4. Repeat through all bit layers until you've resolved the top K.

This produces sorted results without storing sorted arrays. The cost scales with the number of bit layers times the bitmap operation speed — millions of AND operations per second on modern CPUs, each touching compressed data that fits in L2/L3 cache.

With 32 layers, you represent values from 0 to ~4 billion. The cost depends on how full the sieves are: values near 0% or 100% density compress well. 50% density costs the most, but even then the operations are fast because roaring bitmaps compress contiguous and regular bit patterns aggressively.

---

## Bound Caches: Materialized Windows

A bound cache is a pre-computed bitmap that answers "what are the top 10,000 results for this filter + sort combination?" It starts at 4 KB and grows on demand.

When a query arrives:
- **Hot (in memory):** The cache returns the answer directly. 12-16 microseconds.
- **Warm (disk shard):** The cache loads from a persisted shard. ~337 microseconds.
- **Cold (full traversal):** No cache exists. Full filter + sort through bit stacks. 1.6 ms for sparse queries, 40-70 ms for broad ones.

After the first cold query, the result becomes a bound cache entry. Subsequent identical queries hit the hot path. The flush thread maintains these caches live — when new documents arrive, it adds qualifying slots to existing bounds. When filter fields change, it marks affected entries for rebuild.

These are materialized views that maintain themselves. No manual refresh, no scheduled rebuilds. The data drives the cache.

---

## Loaders: Tuple Ingestion Pipelines

A loader converts external data (CSV files, NDJSON, Postgres streams) into bit tuples and routes them to bitmaps and docstore. The loader's job is mechanical: parse, route, write, release.

### The Per-CSV Burn-Down Pattern

Process one data source at a time, largest first. Each source produces bit tuples that flow into bitmaps and docstore. When a source is fully processed, save its bitmaps to disk and release all memory before starting the next source.

For the Civitai image index at 107M records:

| Step | Source | Rows | On disk | Produces |
|------|--------|------|---------|----------|
| 1 | tags.csv | 4.5B | 63 GB | tagIds filter bitmaps (~5.1 GB) + docstore tag fields |
| 2 | resources.csv | ~10M | 777 MB | modelVersionIds + baseModel bitmaps + docstore resource fields |
| 3 | images.csv | 107M | 14 GB | scalar filter + sort bitmaps + docstore scalar fields |
| 4 | tools.csv | ~1M | 50 MB | toolIds bitmaps + docstore tool fields |
| 5 | techniques.csv | ~1M | 71 MB | techniqueIds bitmaps + docstore technique fields |

At each step, memory holds only the current source's bitmaps. Tags is the largest at ~5.1 GB, but once saved to disk it's gone. The next source starts with clean memory. Peak memory never exceeds one source's bitmaps plus enrichment lookups.

### Docstore Field Appends

Each CSV writes its fields to the docstore independently. The docstore shard does not require a complete document on write. It stores `(slot_id, field, value)` entries — the same bit tuple structure — and assembles full documents on read by scanning the shard for all entries matching a slot_id.

This means:
- Tags writes `(slot_42, "tagIds", [100, 200, 300])` to shard N.
- Images writes `(slot_42, "url", "https://...")` and `(slot_42, "nsfwLevel", 8)` to shard N.
- On read, the shard scan collects all entries for slot_42 and returns the assembled document.

Order does not matter. Grouping does not matter. The shard is a flat collection of entries, compressed as a block. The slot_id index in the shard header allows fast lookup when fetching individual documents.

---

## The Document Cache Model

The docstore is a cache, not a source of truth. The source of truth is the upstream data (Postgres, CSV files, API). BitDex reconstructs its state from source data on every full load.

Because the docstore is a cache:
- It can be rebuilt at any time from source data.
- It can store partial documents (only the fields that have been loaded so far).
- It can evolve its schema without migration — just reload with the new schema.
- NVMe read speed (880K reads/sec from benchmarks) makes the cache fast enough to serve alongside query results.

The bitmaps are also a cache — they can be reconstructed from the docstore or from source data. But bitmaps are the working set: they live in memory during operation and persist to disk between restarts.

---

## Schema Configuration

Every BitDex index has a schema that declares its fields, types, and behaviors:

```json
{
  "filter_fields": [
    { "name": "userId", "value_type": "Integer" },
    { "name": "tagIds", "value_type": "IntegerArray", "multi_value": true },
    { "name": "type", "value_type": "MappedString", "string_map": {...} },
    { "name": "hasMeta", "value_type": "ExistsBoolean" }
  ],
  "sort_fields": [
    { "name": "sortAt", "bits": 32 },
    { "name": "reactionCount", "bits": 32 }
  ],
  "data_schema": {
    "fields": [
      { "source": "id", "target": "id", "value_type": "Integer" },
      { "source": "url", "target": "url", "doc_only": true },
      ...
    ]
  }
}
```

A field marked `doc_only` goes to the docstore but not to bitmaps. A field marked filterable gets a filter bitmap. A field marked sortable gets a bit stack. The schema drives the entire ingestion pipeline — no code changes needed to add or remove fields.

Indexes can be created on demand. At measured rates (1M bitmap inserts in 500ms), a new filter index on 107M records takes seconds, not hours. This makes index creation a runtime operation, not a deployment decision.

---

## Measured Performance (107M Civitai Images)

| Metric | Value |
|--------|-------|
| Bitmap memory (all filters + sorts) | 7.5 GB |
| tagIds alone (31K values, 79% of filter memory) | 5.1 GB |
| RSS at steady state (with lazy loading) | 14.5 GB |
| Per-record bitmap cost | ~62 bytes |
| Docstore write throughput (BulkWriter) | 290K docs/s |
| NDJSON full load (105M records) | 5 min 29s (320K/s) |
| Tag CSV scatter rate (pure I/O) | 14.4M rows/s |
| Hot cache query | 12-16 microseconds |
| Warm cache (disk shard restore) | ~337 microseconds |
| Cold query (sparse filter) | ~1.6 ms |
| Cold query (broad filter) | 40-70 ms |
| Document fetch (NVMe shard read) | < 1 ms |

---

## Future Directions

### Vector Bit Stacks

A vector embedding is a list of numbers. Each dimension can be represented as a bit stack — the same structure used for sort fields. A 512-dimension embedding becomes 512 bit stacks. Cosine similarity becomes a series of bitmap operations across the stacks.

The key constraint: the working set must fit in L3 cache for sub-microsecond performance. At 107M records with 512 dimensions of 1-bit quantization, that's 512 * 13 MB = ~6.5 GB — large but feasible on modern CPUs with 32+ MB L3 if the query first narrows the candidate set via filter bitmaps.

### AI-Driven Indexing

A small language model (function-scale, like Gemma) can sit between user queries and the bitmap index. It translates natural language to filter + sort predicates, discovers which indexes to create, and learns from query patterns which caches to pre-warm.

Larger models direct the small ones: "when users ask for X, route to these filters." The small model executes at inference speed; the large model trains it at human speed. The result is semantic search backed by bitmap-speed retrieval.

### Schema DDL

Future schema definitions will use a compact notation:

```
images
    id? 1
    user*?          // filterable, relationship
    type? ''[]      // low-cardinality-string, backed by bit dictionary
    tags*[]? | tag  // multi-value relationship, filterable

user
    id? 1
    name?~ ''       // full-text searchable
    images*[]       // reverse relationship, index on demand
```

Where `*` marks relationships, `?` marks filterable, `''` marks strings, `1` marks numbers, and `[]` marks arrays. Relationships appear on both sides and indexes are prepared on demand.
