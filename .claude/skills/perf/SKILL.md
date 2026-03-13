---
name: perf
description: Performance measurement guide — memory baselines at scale, how to measure bitmap/RSS memory, benchmark commands, and regression thresholds. Use when doing performance work, measuring memory impact, or running benchmarks.
disable-model-invocation: false
user-invocable: true
---

# BitDex Performance & Memory Measurement

Guide for measuring performance, understanding memory baselines, and running benchmarks.

## Memory Baselines (Civitai dataset, remapped IDs, 4 threads)

| Scale | Bitmap Memory | RSS | Worst Query p50 |
|------:|-------------:|----:|----------------:|
| 5M | 328 MB | 1.20 GB | 0.83ms |
| 50M | 2.95 GB | 6.09 GB | 13.5ms |
| 100M | 6.19 GB | 11.66 GB | 18.7ms |
| 104.6M | 6.49 GB | 12.14 GB | 21.1ms |
| 104.6M (bound cache) | 6.51 GB | 14.51 GB | 6.08ms |

**Key facts:**
- tagIds dominates filter memory at 79-80% across all scales
- Scaling is ~62 bytes/record for bitmap memory (linear)
- RSS overhead is ~48% from allocator + OS page cache
- Document store on disk: ~6 GB at 100M records

### 150M Extrapolation

| Component | Estimated Size |
|---|---|
| Filter bitmaps | ~8.1 GB |
| Sort bitmaps | ~1.1 GB |
| Trie cache | ~160 MB |
| **Total bitmap memory** | **~9.3 GB** |
| **Total RSS** | **~17.4 GB** |

## How to Measure Memory

### During Benchmark Runs

The benchmark binary reports bitmap memory and RSS:

```bash
# Full benchmark with memory reporting
cargo run --release --bin bitdex-benchmark -- --file <ndjson> --stages insert,query

# Query-only (against existing data)
cargo run --release --bin bitdex-benchmark -- --bench-dir <dir> --stages query
```

The benchmark output includes:
- Per-field bitmap memory (filter + sort breakdowns)
- Total bitmap memory
- RSS (from OS process stats)
- Per-query-type p50/p95/p99 latencies

### During Server Operation

Use the stats endpoint:

```bash
# Per-index stats including bitmap memory
curl http://localhost:3001/api/indexes/<name>/stats

# Prometheus metrics (includes memory gauges)
curl http://localhost:3001/metrics
```

### Measuring Impact of a Change

1. Run the benchmark **before** your change on the same data
2. Make your change
3. Run the benchmark **after** on the same data
4. Compare bitmap memory, RSS, and query latencies

**Important:** Benchmark numbers vary 2-3x with system load. Always compare runs from the **same session** (back-to-back).

## Regression Thresholds

From `docs/benchmarks/performance-baseline.md`:
- **Memory**: >5% bitmap memory increase requires justification
- **Query latency**: >10% p50 regression gets flagged
- **Write throughput**: >10% regression gets flagged

## Benchmark Reports

- **Consolidated baselines**: `docs/benchmarks/performance-baseline.md` (authoritative)
- **Scaling analysis**: `docs/benchmarks/benchmark-report.md` (5M→104.6M)
- **Bound cache impact**: `docs/benchmarks/benchmark-comparison-loading-mode.md`
- **Write regression analysis**: `docs/benchmarks/write-regression-loading-mode.md`
- **Write path benchmarks**: `docs/benchmarks/write-path-benchmarks.md`
- **Loadtest guide**: `docs/benchmarks/loadtest-guide.md`

## Throwaway Performance Experiments

Use `/microbench` for quick hypothesis testing. Never put microbenchmarks in `tests/` — use the scratch crate.

## Dataset Locations

- **Full dataset v1**: `C:\Dev\Repos\open-source\bitdex\data\images-full.ndjson` (59 GB, 104.6M records, NO url field)
- **Full dataset v2**: `C:\Dev\Repos\open-source\bitdex\data\images-full-v2.ndjson` (101 GB, 105.3M records, HAS url/hash/availability)
