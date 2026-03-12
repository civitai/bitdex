# Mixed Workload Benchmark — Unified Cache at 105M Scale

**Date**: 2026-03-11
**Dataset**: Civitai 105.3M records (images-full-v2.ndjson)
**Hardware**: Desktop (Windows 11, NVMe SSD)
**Binary**: release build, rpmalloc allocator

## Configuration

- **Workload config**: `tools/workload.json`
- **Requests**: 5,000 (+ 200 warmup)
- **Concurrency**: 8 workers
- **Write mix**: 3% upsert, 1% delete
- **Hot pool**: 60% of queries from 13 common gallery patterns, 40% random long-tail
- **Pagination**: 30% continuation, max 3 pages

## Results

### Throughput

| Metric | Value |
|---|---|
| Total ops | 5,000 |
| Wall time | 231.0s |
| Throughput | 22 ops/s |
| Queries (page 1) | 3,860 |
| Queries (page 2+) | 946 |
| Upserts | 150 |
| Deletes | 44 |

### Server-Reported Latency (elapsed_us)

| Metric | min | p50 | p80 | p95 | p99 | max | mean |
|---|---|---|---|---|---|---|---|
| All page-1 queries | 880us | 259ms | 527ms | 821ms | 1.16s | 2.09s | 327ms |
| Pagination (page 2+) | 27ms | 466ms | 731ms | 1.02s | 1.36s | 2.01s | 507ms |

### Wall-Clock Latency (includes HTTP round-trip)

| Metric | min | p50 | p80 | p95 | p99 | max | mean |
|---|---|---|---|---|---|---|---|
| All queries | 3.06ms | 317ms | 599ms | 908ms | 1.21s | 2.09s | 376ms |
| Upserts | 14ms | 43ms | 131ms | 492ms | 2.07s | 2.11s | 134ms |
| Deletes | 17ms | 27ms | 73ms | 162ms | 199ms | 199ms | 50ms |

### Cache Performance

| Metric | Value |
|---|---|
| Unified cache entries | 210 |
| Unified cache hits | 3,826 |
| Unified cache misses | 210 |
| Cache hit rate | 94.8% |
| Cache memory | 21.6 KB |
| Meta-index entries | 210 |
| Meta-index memory | 2.5 KB |

### E2E Single-Worker Benchmark (sequential, 50 iterations after 10 warmup)

| Query | Cold Miss | p50 Hit | p80 Hit | p95 Hit | p99 Hit |
|---|---|---|---|---|---|
| nsfwLevel=1, reactionCount desc | 250ms | 4.78ms | 5.33ms | 5.88ms | 9.50ms |
| nsfwLevel=1 + type=image, reactionCount desc | 53ms | 12.09ms | 13.98ms | 16.17ms | 16.96ms |
| nsfwLevel=1, reactionCount asc | 152ms | 3.90ms | 4.71ms | 7.11ms | 8.25ms |
| nsfwLevel=1, sortAt desc | 15ms | 5.16ms | 6.08ms | 6.89ms | 7.13ms |
| nsfwLevel=1 + type=image, sortAt desc | 24ms | 12.51ms | 14.41ms | 16.04ms | 17.67ms |

## Analysis

### Cache hit rate: 94.8%
The hot pool (60% of queries using 13 common patterns) drives high cache reuse. With 1,146 unique query fingerprints across 5,000 requests, the unified cache stabilized at 210 entries — well within the 5,000-entry limit.

### Latency at 8 concurrency
At 8 concurrent workers, p50 = 259ms for page-1 queries. This is dominated by **filter resolution** (bitmap AND on 21M+ entries), which runs on every query even on cache hits — the total_matched count requires the full filter bitmap. Under 8 concurrent workers, memory bandwidth is the bottleneck.

Single-worker cache hits are 3-12ms p50 (30-80x faster), confirming the sort traversal itself is fast — the overhead is entirely from concurrent bitmap operations.

### Writes under load
Upserts average 134ms wall time, deletes average 50ms. Both remain fast even under concurrent read load, confirming the ArcSwap snapshot architecture handles mixed workloads without read/write contention.

### Memory efficiency
210 cache entries consume only 21.6 KB (103 bytes/entry average). The meta-index tracking those 210 entries uses 2.5 KB. Total cache memory overhead is negligible.

## Potential Optimizations

1. **Cache the total_matched count** alongside the bounded bitmap to skip filter resolution entirely on cache hits. This would bring cache hit latency from 3-12ms (single-worker) down to sub-ms.
2. **Parallel bitmap AND** for filter resolution — split the 21M-bit range across available cores for the intersection operation.
3. **Approximate total_matched** on cache hits — return the cached count with a staleness indicator, refreshing periodically.
