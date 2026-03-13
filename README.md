# BitDex

A purpose-built, in-memory bitmap index engine. Takes filter predicates + sort parameters, returns an ordered list of integer IDs. Bitmaps all the way down.

**In:** Filter clauses + sort field + direction + limit
**Out:** Ordered list of matching IDs + optional full documents

Built for datasets in the 100M+ record range on a single node. No clustering, no replication, no full-text search — just fast filtering, sorting, and document retrieval via roaring bitmap operations.

## Performance

Tested against 105M records (Civitai image dataset) on a single machine (Windows 11, NVMe SSD):

### Concurrent Throughput (HTTP, 105M records)

| Concurrency | QPS | p50 | p95 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 8,530 | 0.10ms | 0.17ms | 0.20ms | 1.22ms |
| 4 | 25,343 | 0.14ms | 0.23ms | 0.34ms | 24.06ms |
| 8 | 46,915 | 0.16ms | 0.23ms | 0.29ms | 12.61ms |
| 16 | 63,562 | 0.23ms | 0.36ms | 0.47ms | 15.96ms |
| 32 | 71,415 | 0.42ms | 0.69ms | 0.89ms | 22.27ms |
| 64 | 82,104 | 0.73ms | 1.30ms | 1.63ms | 6.80ms |

Production workload mix (2,516 real Civitai traffic queries from `tests/loadtest/workload.json`). Unified cache at 99.98% hit rate. The loadtest auto-detects this workload file when run from the repo root.

### Query Latency (single-threaded benchmark harness, cache warm)

| Query Type | p50 |
|---|---|
| Sparse filter (userId Eq) | 0.041ms |
| Dense filter (nsfwLevel Eq, 90M matches) | 7.84ms |
| Sort + filter (nsfwLevel=1, reactionCount Desc) | 1.68ms |
| Sort + filter (id Asc) | 1.61ms |
| Range filter + 3-clause sort | 6.08ms |

Bound cache provides 2-13x speedup on sort queries. Full breakdown in [`docs/benchmarks/performance-baseline.md`](docs/benchmarks/performance-baseline.md).

### Memory

| Scale | Bitmap Memory | RSS |
|--:|--:|--:|
| 5M | 328 MB | 1.20 GB |
| 50M | 2.95 GB | 6.09 GB |
| 100M | 6.19 GB | 11.66 GB |
| 105M | 6.51 GB | 14.51 GB |

Scaling is linear at ~62 bytes/record. With lazy loading, RSS starts near zero and fields load on demand — only queried fields consume memory.

## How It Works

### Bitmap Index Architecture

Every filterable field value gets its own [roaring bitmap](https://roaringbitmap.org/). A query like `nsfwLevel=1 AND type="image"` becomes a bitwise AND of two bitmaps — O(compressed size), not O(record count).

Sortable fields are decomposed into bit layers (one bitmap per bit position). Top-N retrieval walks bits MSB-to-LSB using AND operations, extracting the highest/lowest values without scanning or sorting.

### Key Components

- **Filter bitmaps** — One roaring bitmap per distinct value per field. Boolean, integer, string, and multi-value fields supported.
- **Sort layer bitmaps** — Numeric fields decomposed into N bitmaps (one per bit). A u32 sort field = 32 bitmaps. Top-N via MSB-to-LSB traversal.
- **Unified cache** — Bounded top-K result cache per (filter combo, sort field, direction). 99.98% hit rate under production workload. ~103 bytes/entry.
- **Bound cache** — Pre-computed approximate top-K bitmaps per sort field. Reduces sort working set by 10-100x. 2-13x speedup on sort queries.
- **ArcSwap snapshots** — Lock-free reads via immutable snapshots. Writers publish atomically via crossbeam channels. Zero reader contention.
- **Document store** — Custom packed-shard filesystem store (512 docs/shard, zstd-compressed msgpack). Enables upsert diffing and serving full documents via `include_docs: true`.
- **Lazy loading** — Bitmaps load per-field on first query. Server starts in <1s at 105M records; fields load on demand (typically <100ms each).
- **Idle eviction** — High-cardinality multi-value fields (e.g., tagIds with 31K+ values) automatically evict rarely-queried values from memory after a configurable idle period. Reloads from disk on next query.
- **Save and unload** — Zero-copy bitmap snapshot save via `fused_cow()`, then unload all fields from memory. Combined with lazy loading, enables memory reclamation without restart.
- **Clean deletes** — Deletes clear all filter/sort bitmap bits, keeping bitmaps permanently clean. No alive bitmap AND in the query hot path.

## Getting Started

### Build

```bash
# Library only
cargo build --release

# HTTP server
cargo build --release --features server --bin bitdex-server

# Load tester
cargo build --release --features loadtest --bin bitdex-loadtest

# Benchmark harness
cargo build --release --bin bitdex-benchmark
```

#### SIMD build (nightly)

The `simd` feature enables vectorized bitmap operations via Rust's `portable_simd`. This accelerates bitwise AND/OR/XOR and popcount across roaring bitmap containers (processing 4-8 u64 words per instruction with AVX2/AVX-512 instead of one at a time).

Requires Rust nightly. The `portable_simd` API broke in nightly 1.95+ (January 2026); use `nightly-2025-12-15` until the `roaring` crate updates.

```bash
rustup install nightly-2025-12-15

# Build with SIMD
cargo +nightly-2025-12-15 build --release --features server,simd --bin bitdex-server
```

#### Docker

Production and SIMD Docker images are in the `docker/` directory.

```bash
# Production image (stable Rust, fat LTO, target-cpu=znver5)
docker build -t bitdex:latest -f docker/Dockerfile .

# SIMD image (pinned nightly, roaring portable_simd)
docker build -t bitdex:simd -f docker/Dockerfile.simd .

# Run
docker run -p 3000:3000 -v bitdex-data:/data bitdex:latest
```

The production image sets `MALLOC_CONF` for jemalloc memory return tuning (important in K8s to avoid OOMKill). Both images compile with `-C target-cpu=znver5` for AMD EPYC (AVX2, BMI2, POPCNT). Change to `znver4` for Genoa/Bergamo or `native` for auto-detection.

### Run the Server

```bash
cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./data
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
cargo run --release --features loadtest --bin bitdex-loadtest -- \
  --mode http --url http://localhost:3001 \
  --concurrency 1,4,8,16,32,64 \
  --duration 10

# Test bitmap layer directly
cargo run --release --features loadtest --bin bitdex-loadtest -- \
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
cargo run --release --features loadtest --bin bitdex-loadtest -- \
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

# All self-contained E2E tests (builds server, runs 10 suites, 59 tests)
node tests/e2e/run-e2e.mjs

# Skip rebuild if binary is current
node tests/e2e/run-e2e.mjs --skip-build
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
| LowCardinalityString | 7 | Auto-dictionary, case-insensitive, upsert, doc serving, persistence |
| Delisting | 5 | Availability filtering, delist/relist, blockedFor, combined |
| Schema Versioning | 7 | Default elision, reconstruction, missing fields, round-trip, snapshot |
| Cache Maintenance | 9 | Filter/sort/delete maintenance, multi-value, fan-out, burst writes |

Full testing guide: [`docs/guide/testing.md`](docs/guide/testing.md)

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
- **[API Reference](docs/guide/api.md)** — Full endpoint documentation

## License

MIT
