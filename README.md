# BitDex

A purpose-built, in-memory bitmap index engine. Takes filter predicates + sort parameters, returns an ordered list of integer IDs. Bitmaps all the way down.

**In:** Filter clauses + sort field + direction + limit
**Out:** Ordered list of matching IDs + optional full documents

Built for datasets in the 100M+ record range on a single node. No clustering, no replication, no full-text search — just fast filtering, sorting, and document retrieval via roaring bitmap operations.

## Performance

Tested against 105M records (Civitai image dataset) on a single machine:

### Query Latency

| Query Type | p50 | p99 |
|---|---|---|
| Sparse filter (userId=1) | 0.03ms | 0.05ms |
| Dense filter (nsfwLevel=1, 90M matches) | 0.03ms | 0.06ms |
| Dense filter + sort (sortAt desc) | 0.08ms | 0.15ms |
| Mixed filters + sort | 0.10ms | 0.25ms |

### Concurrent Throughput (HTTP, 105M records)

| Concurrency | QPS | p50 | p95 | p99 |
|--:|--:|--:|--:|--:|
| 1 | 1,461 | 0.64ms | 1.53ms | 1.77ms |
| 4 | 6,164 | 0.60ms | 1.36ms | 1.73ms |
| 8 | 11,799 | 0.62ms | 1.33ms | 1.98ms |
| 16 | 19,223 | 0.66ms | 1.83ms | 3.97ms |
| 32 | 18,974 | 1.00ms | 5.14ms | 13.43ms |
| 64 | 22,606 | 2.37ms | 5.93ms | 11.13ms |

### Memory

| Scale | Bitmap Memory | RSS |
|--:|--:|--:|
| 5M | 328 MB | 1.20 GB |
| 50M | 2.95 GB | 6.09 GB |
| 100M | 6.19 GB | 11.66 GB |
| 105M | 6.51 GB | 14.51 GB |

## How It Works

### Bitmap Index Architecture

Every filterable field value gets its own [roaring bitmap](https://roaringbitmap.org/). A query like `nsfwLevel=1 AND type="image"` becomes a bitwise AND of two bitmaps — O(compressed size), not O(record count).

Sortable fields are decomposed into bit layers (one bitmap per bit position). Top-N retrieval walks bits MSB-to-LSB using AND operations, extracting the highest/lowest values without scanning or sorting.

### Key Components

- **Filter bitmaps** — One roaring bitmap per distinct value per field. Boolean, integer, string, and multi-value fields supported.
- **Sort layer bitmaps** — Numeric fields decomposed into N bitmaps (one per bit). A u32 sort field = 32 bitmaps.
- **Bound cache** — Pre-computed approximate top-K bitmaps per sort field. Reduces sort working set by 10-100x.
- **Trie cache** — Query result cache keyed by canonically sorted filter clauses. Prefix matching for partial hits.
- **ArcSwap snapshots** — Lock-free reads via immutable snapshots. Writers publish atomically. Zero reader contention.
- **Document store** — Custom packed-shard filesystem store keyed by slot ID. Enables upsert diffing and serving full documents alongside query results (via `include_docs: true`).
- **Lazy loading** — Bitmaps load per-field on first query. Server starts in <1s at 105M records; fields load on demand (typically <100ms each).
- **Clean deletes** — Deletes clear all filter/sort bitmap bits, keeping bitmaps permanently clean. No alive bitmap AND in the query hot path.

## Getting Started

### Build

```bash
# Library only
cargo build --release

# HTTP server
cargo build --release --features server --bin server

# Load tester
cargo build --release --features loadtest --bin loadtest

# Benchmark harness
cargo build --release --bin benchmark
```

### Run the Server

```bash
cargo run --release --features server --bin server -- --port 3001 --data-dir ./data
```

The server starts blank. Create an index, then load data.

### Create an Index

```bash
curl -X POST http://localhost:3001/api/indexes \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_index",
    "config": {
      "filter_fields": [
        {"name": "status", "field_type": "single_value"},
        {"name": "category", "field_type": "single_value"},
        {"name": "tags", "field_type": "multi_value"},
        {"name": "active", "field_type": "boolean"}
      ],
      "sort_fields": [
        {"name": "createdAt", "bits": 32, "signed": false},
        {"name": "score", "bits": 32, "signed": true}
      ]
    }
  }'
```

### Load Data

```bash
curl -X POST http://localhost:3001/api/indexes/my_index/load \
  -H "Content-Type: application/json" \
  -d '{
    "path": "/path/to/data.ndjson",
    "id_field": "id"
  }'
```

Data files are newline-delimited JSON (NDJSON). Each line is a document with an integer ID field and the fields defined in your config.

### Query

```bash
curl -X POST http://localhost:3001/api/indexes/my_index/query \
  -H "Content-Type: application/json" \
  -d '{
    "filters": [
      {"Eq": ["status", {"String": "published"}]},
      {"Eq": ["active", {"Bool": true}]}
    ],
    "sort": {"field": "createdAt", "direction": "Desc"},
    "limit": 20,
    "include_docs": true
  }'
```

Response:

```json
{
  "ids": [9823, 9817, 9801],
  "total_matched": 4521983,
  "elapsed_us": 142,
  "cursor": {"slot": 9801, "sort_value": 1709251200},
  "documents": [
    {"id": 9823, "fields": {"status": "published", "category": "art", "score": 4250}},
    {"id": 9817, "fields": {"status": "published", "category": "photo", "score": 3891}},
    {"id": 9801, "fields": {"status": "published", "category": "art", "score": 3544}}
  ]
}
```

Set `include_docs: false` (or omit it) to return only IDs — useful when you just need the ordered ID list and will fetch documents from your own data layer.

## API Reference

### Index Management

| Method | Path | Description |
|---|---|---|
| POST | `/api/indexes` | Create a new index |
| GET | `/api/indexes` | List all indexes |
| GET | `/api/indexes/{name}` | Get index info |
| DELETE | `/api/indexes/{name}` | Delete an index |

### Data

| Method | Path | Description |
|---|---|---|
| POST | `/api/indexes/{name}/load` | Bulk load from NDJSON file |
| GET | `/api/indexes/{name}/load/status` | Check load progress |
| POST | `/api/indexes/{name}/documents/upsert` | Upsert documents |
| DELETE | `/api/indexes/{name}/documents` | Delete documents by ID |

### Query & Stats

| Method | Path | Description |
|---|---|---|
| POST | `/api/indexes/{name}/query` | Execute a query |
| POST | `/api/indexes/{name}/document` | Get a single document by slot ID |
| POST | `/api/indexes/{name}/documents` | Get documents by slot IDs (batch) |
| GET | `/api/indexes/{name}/stats` | Index statistics |
| GET | `/api/health` | Health check |

### Filter Clauses

```json
{"Eq": ["field", {"Integer": 42}]}
{"NotEq": ["field", {"String": "draft"}]}
{"In": ["field", [{"Integer": 1}, {"Integer": 2}]]}
{"Gt": ["field", {"Integer": 100}]}
{"Lt": ["field", {"Integer": 50}]}
{"Gte": ["field", {"Integer": 100}]}
{"Lte": ["field", {"Integer": 50}]}
{"Not": {"Eq": ["field", {"String": "hidden"}]}}
{"And": [{"Eq": ["a", {"Integer": 1}]}, {"Eq": ["b", {"Integer": 2}]}]}
{"Or": [{"Eq": ["a", {"Integer": 1}]}, {"Eq": ["a", {"Integer": 2}]}]}
```

Value types: `Integer`, `Float`, `Bool`, `String`.

### Sort

```json
{"field": "score", "direction": "Desc"}
{"field": "createdAt", "direction": "Asc"}
```

### Pagination

Cursor-based (for production use):

```json
{
  "filters": [...],
  "sort": {"field": "score", "direction": "Desc"},
  "limit": 20,
  "cursor": {"slot": 9801, "sort_value": 4250}
}
```

Offset-based (for compatibility):

```json
{
  "filters": [...],
  "sort": {"field": "score", "direction": "Desc"},
  "limit": 20,
  "offset": 100
}
```

## Load Testing

The built-in load tester measures throughput and latency at configurable concurrency levels.

### Modes

- **`direct`** — Embeds the engine, loads from disk, queries the bitmap layer directly (no HTTP overhead)
- **`http`** — Sends requests to a running server (tests the full stack including serialization and networking)

### Usage

```bash
# Test against a running server
cargo run --release --features loadtest --bin loadtest -- \
  --mode http --url http://localhost:3001 \
  --concurrency 1,4,8,16,32,64 \
  --duration 10

# Test bitmap layer directly
cargo run --release --features loadtest --bin loadtest -- \
  --mode direct --data-dir ./data \
  --concurrency 1,4,8,16,32,64 \
  --duration 10
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--mode` | `direct` | `direct` or `http` |
| `--data-dir` | `./data` | Data directory (direct mode) |
| `--url` | `http://localhost:3001` | Server URL (http mode) |
| `--index` | `civitai` | Index name |
| `--concurrency` | `1,4,8,16,32,64` | Comma-separated concurrency levels |
| `--duration` | `10` | Seconds per concurrency level |
| `--warmup` | `3` | Warmup seconds before measuring |
| `--no-warmup` | | Skip warmup phase |
| `--workload` | built-in | Path to JSON workload file |

### Custom Workload

Create a JSON file with your queries:

```json
{
  "queries": [
    {
      "label": "homepage",
      "filters": [
        {"Eq": ["status", {"String": "published"}]}
      ],
      "sort": {"field": "createdAt", "direction": "Desc"},
      "limit": 20
    },
    {
      "label": "user_lookup",
      "filters": [
        {"Eq": ["userId", {"Integer": 42}]}
      ]
    }
  ]
}
```

```bash
cargo run --release --features loadtest --bin loadtest -- \
  --mode http --workload my-workload.json
```

## Project Structure

```
src/
  engine.rs              Core bitmap engine (filter + sort execution)
  concurrent_engine.rs   ArcSwap lock-free snapshot reads + flush thread
  executor.rs            Query executor + pagination
  filter.rs              Filter field bitmap storage
  sort.rs                Sort layer bitmap storage + bit traversal
  query.rs               Query types (FilterClause, SortClause, Value)
  planner.rs             Cardinality-based query planning
  cache.rs               Trie cache with prefix matching
  unified_cache.rs       Bounded top-K result cache per (filter, sort, direction)
  bound_cache.rs         Approximate top-K bitmaps for sort acceleration
  meta_index.rs          Bitmaps indexing bitmaps for cache invalidation
  mutation.rs            Mutation operations (insert, update, delete)
  write_coalescer.rs     Crossbeam channel batched flush + invalidation
  docstore.rs            Packed-shard filesystem document store
  bitmap_fs.rs           Bitmap persistence (pack files per field)
  config.rs              Configuration types
  slot.rs                Slot allocator + alive bitmap
  versioned_bitmap.rs    Base+diff+generation bitmaps with lazy merge
  time_buckets.rs        Pre-computed time range bitmaps
  loader.rs              Bulk data loading (NDJSON → bitmaps)
  server.rs              HTTP server (axum)
  bin/
    server.rs            Server binary entry point
    benchmark.rs         Benchmark harness (20 query types)
    loadtest.rs          Concurrent load tester
```

## Testing

```bash
# All Rust unit + integration tests
cargo test --release

# All self-contained E2E tests (builds server, runs 6 suites, 31 tests)
node tools/run-e2e.mjs

# Skip rebuild if binary is current
node tools/run-e2e.mjs --skip-build
```

E2E test suites:

| Suite | Tests | What it covers |
|-------|-------|---------------|
| Write Handling | 7 | Insert, upsert, delete, concurrent, multi-value |
| Eviction | 5 | Load, idle, evict, reload, existence set |
| Query Operators | 4 | Range (Gt/Gte/Lt/Lte), NotEq, combined |
| Error Handling | 5 | Invalid JSON, unknown index, empty index, slot recycling |
| Pagination & Overhead | 6 | Cursor pagination, cache acceleration, expansion, overhead |
| Save/Unload/Lazy | 4 | Snapshot save, query after save, mutation survival, stats |

Full testing guide: [`docs/testing.md`](docs/testing.md)

## Documentation

```
docs/
  api.md                    API reference
  testing.md                Testing guide + coverage gap analysis
  config-schema.md          Configuration reference
  bitdex-civitai-schema.md  Civitai dataset schema
  benchmarks/               Performance reports and baselines
  design/                   Architecture and design docs
  plans/                    Roadmaps and implementation plans
  reviews/                  Architecture reviews and QA
  audit/                    Phase completion audits
  in/                       Original design conversations
```

Key docs:
- **[Performance Baselines](docs/benchmarks/performance-baseline.md)** — Consolidated numbers with commit hashes and regression thresholds
- **[Benchmark Report](docs/benchmarks/benchmark-report.md)** — 5M-105M scaling analysis
- **[API Reference](docs/api.md)** — Full endpoint documentation

## License

MIT
