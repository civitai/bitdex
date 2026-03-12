# BitDex Load Testing Guide

## Quick Start

```bash
# 1. Start the server (port 3001 to avoid conflicts with other dev servers)
cargo run --release --features server --bin server -- --port 3001

# 2. Run the Rust loadtest against it
cargo run --release --bin loadtest --features loadtest -- \
  --mode http --url http://localhost:3001 \
  --workload tools/workload.json \
  --concurrency 1,4,16,64 --duration 10
```

## The Rust Loadtest Binary

**Location:** `src/bin/loadtest.rs`

The loadtest supports two modes:

### HTTP Mode (recommended)

Tests the full stack: HTTP parsing, query execution, serialization, cache.

```bash
cargo run --release --bin loadtest --features loadtest -- \
  --mode http \
  --url http://localhost:3001 \
  --workload tools/workload.json \
  --concurrency 1,4,16,64 \
  --duration 10 \
  --warmup 3
```

Uses `ureq` with thread-local HTTP agents — one OS thread per concurrency level, each with its own connection. This avoids connection pool contention and gives clean latency numbers.

### Direct Mode

Embeds the engine in-process. Bypasses HTTP entirely — tests pure bitmap query performance.

```bash
cargo run --release --bin loadtest --features loadtest -- \
  --mode direct \
  --data-dir ./data \
  --workload tools/workload.json \
  --concurrency 1,4,16,64
```

Note: Direct mode creates a fresh engine each run, so the first run includes cold lazy-load costs. Run twice for warm-cache numbers.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--mode <direct\|http>` | `direct` | Query execution mode |
| `--url <URL>` | `http://localhost:3001` | Server URL (HTTP mode) |
| `--data-dir <PATH>` | `./data` | Data directory (direct mode) |
| `--index <NAME>` | `civitai` | Index name |
| `--workload <PATH>` | built-in 13 queries | JSON workload file |
| `--concurrency <LIST>` | `1,4,8,16,32,64` | Comma-separated concurrency levels |
| `--duration <SECS>` | `10` | Seconds to measure at each level |
| `--warmup <SECS>` | `3` | Random-query warmup before measuring each level |
| `--no-warmup` | | Skip the initial deterministic warmup pass |

### Warmup Behavior

1. **Deterministic warmup** (unless `--no-warmup`): Runs every query in the workload twice sequentially. First pass triggers lazy bitmap loads from disk. Second pass seeds the unified cache.
2. **Per-level warmup** (`--warmup N`): N seconds of random queries before measurement starts at each concurrency level. Not measured.

### Output

```
 concurrency     queries         QPS         p50         p95         p99       p99.9         max
--------------------------------------------------------------------------------------------
           1       25259        1684      0.16ms      0.47ms     14.13ms     46.44ms    134.88ms
           4       63030        4202      0.23ms      0.98ms     22.69ms     63.45ms    162.51ms
          16       66559        4437      0.65ms      3.65ms     87.15ms    354.98ms   1028.79ms
          64       66643        4437      7.75ms     27.37ms    179.33ms    728.38ms   2237.72ms
```

## Workload Files

### Built-in Workload

Without `--workload`, the loadtest uses 13 hard-coded Civitai queries (homepage sorts, user lookups, mixed filters). Good for quick regression testing, but too small for realistic cache pressure testing.

### Real-Traffic Workload (`tools/workload.json`)

2,516 queries generated from real Civitai traffic data. Format:

```json
{
  "queries": [
    {
      "label": "feed_sortAt",
      "filters": [],
      "sort": { "field": "sortAt", "direction": "Desc" }
    },
    {
      "label": "user_12345_reactions",
      "filters": [{ "Eq": ["userId", { "Integer": 12345 }] }],
      "sort": { "field": "reactionCount", "direction": "Desc" }
    }
  ]
}
```

Query mix:
- **16 feed variants** — 4 sort fields × (no filter + 3 nsfw levels)
- **1,000 user queries** — top 500 users by 24h profile views × 2 sorts
- **500 model version queries** — top 500 MVs by 24h views
- **1,000 tag queries** — top 500 tags by 24h views × (sortAt + sfw+reactions)

### Regenerating the Workload

The workload is generated from CSV files containing real Civitai traffic data (24h view counts):

```bash
node tools/gen-workload.mjs
# Output: Generated 2516 queries → tools/workload.json
```

Source CSVs in `tools/`:
- `user_profile_views_24h.csv` — userId, views
- `model_version_views_24h.csv` — modelVersionId, views
- `tag_views_24h.csv` — tagId, name, views
- `model_views_24h.csv` — modelId, views (not currently used in workload)

To update with fresh traffic data, export new CSVs from ClickHouse/analytics and re-run `gen-workload.mjs`.

## Interpreting Results

### What p50 tells you

Cache-hit query latency. With a warm unified cache:
- **Sorted vec path** (≤4K entry): ~0.01-0.03ms server-side (binary search)
- **Radix path** (expanded 64K entry): ~0.1-0.3ms server-side
- **HTTP overhead**: adds ~0.1-0.3ms wall time per request

### What p99 tells you

Tail latency, typically caused by:
- **Per-value lazy loading**: Tag bitmaps loaded from disk on first access (2-100ms each)
- **Cache misses**: First-time filter computation + cache formation
- **Flush thread contention**: Brief cache mutex holds during maintenance

### What QPS plateau tells you

QPS typically plateaus at c=4-16. Beyond that, adding threads increases wall time without increasing throughput. The ceiling is determined by:
1. The p99 tail — slow queries block threads
2. Cache mutex contention at high concurrency
3. HTTP connection overhead (ureq sync calls)

### Baseline Numbers (105M records, 2,516-query workload)

| Concurrency | QPS | p50 | p95 | p99 |
|---|---|---|---|---|
| c=1 | ~1,700 | 0.16ms | 0.47ms | 14ms |
| c=4 | ~4,200 | 0.23ms | 0.98ms | 23ms |
| c=16 | ~4,400 | 0.65ms | 3.7ms | 87ms |
| c=64 | ~4,400 | 7.8ms | 27ms | 179ms |

These numbers are with the sorted vec + radix dual-path cache (43 MB, 2,517 entries). The built-in 13-query workload achieves ~22K QPS at c=4 due to 100% hot cache with zero diversity.

## Tips

- **Always use `--release`** — debug builds are 10-100x slower
- **Kill stale server processes** before rebuilding: `taskkill //F //IM server.exe` (Windows) or `pkill server` (Linux)
- **Port conflicts**: Use `--port 3001` if port 3000 is taken by other dev servers
- **First query after cold start** triggers lazy bitmap loading (can take 1-40s for tagIds at 105M). The warmup phase handles this automatically.
- **Compare runs from the same session** — system load, thermal throttling, and OS page cache state affect numbers significantly
