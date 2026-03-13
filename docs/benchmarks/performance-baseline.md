# BitDex V2 — Performance Baselines

**Last Updated**: 2026-03-13
**Platform**: Windows 11 Pro, Desktop (NVMe SSD), 4 threads (benchmark), 8 threads (mixed workload)
**Allocator**: rpmalloc (release builds)

> These baselines are from Justin's dev machine. Production hardware will differ.
> Numbers vary 2-3x with system load — always compare runs from the same session.

---

## 1. Query Latency Baselines (at ~105M Records)

### Benchmark Harness — Single-Threaded, Cache-Warm (104.6M, commit 6fb2b78)

Source: `docs/benchmarks/benchmark-comparison-loading-mode.md` (Feb 21, 2026). Bound cache enabled.

| Query Type | p50 | p95 | Cache State | Notes |
|---|---|---|---|---|
| Sparse filter (userId Eq) | 0.041ms | — | warm | Essentially free at any scale |
| Dense filter (nsfwLevel Eq 1) | 7.84ms | — | warm | 1.7x slower than pre-ArcSwap baseline; likely session variance |
| Multi-value filter (tagIds In popular) | 7.82ms | — | warm | Similar to dense filter |
| Sort-only (reactionCount Desc) | 3.64ms | — | warm, bounds | 2.5x faster than no-bounds baseline (9.01ms) |
| Sort + filter (nsfwLevel=1, reactionCount Desc) | 1.68ms | — | warm, bounds | 4.6x faster than no-bounds (7.71ms). Common production pattern |
| Sort + filter (commentCount) | 1.87ms | — | warm, bounds | 3.8x faster than no-bounds |
| Sort + filter (id Asc) | 1.61ms | — | warm, bounds | 13.1x faster than no-bounds (21.13ms) |
| Range filter + sort (3 clause sort) | 6.08ms | — | warm, bounds | 1.7x faster than no-bounds (10.56ms) |
| Filter OR 3 tags | 26.20ms | — | warm | Worst filter-only query. Dense bitmap union |
| Prefix cache shared A | 5.90ms | — | warm | Trie cache prefix match |

### E2E HTTP — Single-Worker, Cache-Warm (105.3M, commit ~769c87f)

Source: `docs/benchmarks/benchmark-mixed-workload.md` (Mar 11, 2026). Unified cache enabled.

| Query Type | Cold Miss | p50 Hit | p95 Hit | Notes |
|---|---|---|---|---|
| nsfwLevel=1, reactionCount desc | 250ms | 4.78ms | 5.88ms | Common gallery query |
| nsfwLevel=1 + type=image, reactionCount desc | 53ms | 12.09ms | 16.17ms | Two-clause filter + sort |
| nsfwLevel=1, reactionCount asc | 152ms | 3.90ms | 7.11ms | Reverse sort direction |
| nsfwLevel=1, sortAt desc | 15ms | 5.16ms | 6.89ms | Time-based sort |
| nsfwLevel=1 + type=image, sortAt desc | 24ms | 12.51ms | 16.04ms | Two-clause filter + time sort |

### HTTP Loadtest — Real Traffic Workload (105M, 2026-03-13)

Source: `tests/loadtest/workload.json` (2,516 real Civitai traffic queries). Stable build: fat LTO, codegen-units=1. Unified cache warm.

| Concurrency | QPS | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 1 | 8,530 | 0.10ms | 0.17ms | 0.20ms | 1.22ms |
| 4 | 25,343 | 0.14ms | 0.23ms | 0.34ms | 24.06ms |
| 8 | 46,915 | 0.16ms | 0.23ms | 0.29ms | 12.61ms |
| 16 | 63,562 | 0.23ms | 0.36ms | 0.47ms | 15.96ms |
| 32 | 71,415 | 0.42ms | 0.69ms | 0.89ms | 22.27ms |
| 64 | 82,104 | 0.73ms | 1.30ms | 1.63ms | 6.80ms |
| 128 | 77,430 | 1.58ms | 2.78ms | 3.46ms | 9.21ms |

Saturates at c=64 (~82K QPS). c=128 adds no throughput, only latency. Near-linear scaling from c=1 to c=32.

### E2E HTTP — 8 Concurrent Workers (105.3M)

Under concurrent load, memory bandwidth is the bottleneck. These numbers include filter resolution on every request (total_matched computation).

| Metric | p50 | p80 | p95 | p99 | max |
|---|---|---|---|---|---|
| Page-1 queries (server elapsed) | 259ms | 527ms | 821ms | 1.16s | 2.09s |
| Pagination page 2+ (server elapsed) | 466ms | 731ms | 1.02s | 1.36s | 2.01s |
| All queries (wall clock incl. HTTP) | 317ms | 599ms | 908ms | 1.21s | 2.09s |

> The gap between single-worker (3-12ms) and 8-worker (259ms) is almost entirely from concurrent bitmap operations saturating memory bandwidth, not from contention.

### Pre-Bound-Cache Baselines (104.6M, commit 763a008)

Source: `docs/benchmarks/benchmark-report.md` (Feb 19, 2026). No bound cache. Useful for regression comparison of filter-only queries since there was no session variance concern.

| Query Type | p50 | p95 | p99 |
|---|---|---|---|
| Sparse filter (userId Eq) | 0.034ms | 0.060ms | 0.069ms |
| Dense filter (nsfwLevel Eq 1) | 4.665ms | 5.934ms | 6.761ms |
| Multi-value filter (nsfwLevel In) | 4.749ms | 5.905ms | 6.616ms |
| Sort-only (reactionCount Desc) | 9.010ms | 12.545ms | 16.034ms |
| Sort + filter (nsfwLevel=1, reactionCount Desc) | 7.706ms | 10.542ms | 11.985ms |
| Worst case (filter_sort_id_asc) | 21.126ms | 27.918ms | 31.802ms |
| Filter OR 3 tags | 15.112ms | 21.640ms | 25.794ms |

---

## 2. Memory Baselines

Source: `docs/benchmarks/benchmark-report.md` and `docs/benchmarks/benchmark-comparison-loading-mode.md`.

| Scale | Bitmap Memory | RSS | Key Commit | Date |
|---|---|---|---|---|
| 5M | 328 MB | 1.20 GB | 763a008 | Feb 19 |
| 50M | 2.95 GB | 6.09 GB | 763a008 | Feb 19 |
| 100M | 6.19 GB | 11.66 GB | 763a008 | Feb 19 |
| 104.6M (no bounds) | 6.49 GB | 12.14 GB | 763a008 | Feb 19 |
| 104.6M (with bounds) | 6.51 GB | 14.51 GB | 6fb2b78 | Feb 21 |
| 150M (extrapolated) | ~9.3 GB | ~17.4 GB | — | — |

### Memory Breakdown at 104.6M

| Component | Size | % of Bitmap |
|---|---|---|
| Filter bitmaps | 5.63 GB | 86.7% |
| — tagIds | 4.48 GB | 79.6% of filter |
| — modelVersionIds | 738 MB | 13.1% of filter |
| — userId | 263 MB | 4.7% of filter |
| Sort bitmaps | 757 MB | 11.7% |
| Trie cache | 111 MB | 1.7% |
| Bound cache | 3.70 KB | negligible |
| Meta-index | 270 B | negligible |

Scaling is linear: ~62 bytes/record bitmap memory. RSS overhead stabilizes at ~48% above bitmap memory at scale (allocator + OS page cache).

---

## 3. Write Throughput Baselines

### Bulk Loading (put_bulk_loading, single-threaded bitmap path)

Source: `docs/benchmarks/benchmark-report.md`, `docs/benchmarks/benchmark-comparison-loading-mode.md`, CLAUDE.md MEMORY.

| Scale | Rate | Wall Time | Commit | Notes |
|---|---|---|---|---|
| 1M (ArcSwap, loading mode) | 70,153/s | 14.25s | 6fb2b78 | Was 82K/s on RwLock baseline |
| 5M (ArcSwap, loading mode) | 56,113/s | 89.11s | 6fb2b78 | 9% below RwLock baseline |
| 104.6M (ArcSwap, loading mode) | 28,316/s | ~70 min | 6fb2b78 | Degradation from growing bitmaps |
| 104.6M (pre-loading-mode, RwLock) | 35,325/s | ~49 min | 763a008 | Original baseline |

### Fused Parse+Bitmap Loader Pipeline

Source: CLAUDE.md MEMORY section (various commits, Jan-Feb 2026).

| Optimization Stage | Sustained Rate | Notes |
|---|---|---|
| Fused parse+bitmap (rayon) | 460K/s | Commit dfc977c |
| Direct JSON-to-msgpack encoding | 365K/s | Commit c10c57c, 105M in 5m29s at 320K/s sustained |
| Encode in parse fold | 345K/s | Commit 3702df7 |
| Parallel docstore writes | 290K/s | Commit 8e2137a, per-shard locking |
| put_bulk (benchmark harness) | 641K/s at 104M | Commits 1217f61-61e2032 |

> The 641K/s figure is bitmap-path-only throughput (no docstore). The 320-365K/s figures include full pipeline with docstore writes.

### Single Upsert / Delete Under Load (105.3M, 8 concurrent workers)

Source: `docs/benchmarks/benchmark-mixed-workload.md`.

| Operation | p50 (wall clock) | mean | Notes |
|---|---|---|---|
| Upsert | 43ms | 134ms | Includes HTTP round-trip. p95=492ms |
| Delete | 27ms | 50ms | Includes HTTP round-trip. p95=162ms |

### Rebuild from Docstore (105.3M, channel-based merge)

Source: `rebuild_bench --full` runs on Justin's dev machine (Mar 13, 2026).

Rebuilds all bitmap indexes (18 filter + 5 sort fields) from the on-disk docstore using packed decode + channel-based merge (rayon workers → bounded channel → single merge thread).

| Phase | Time | Rate | Peak RSS | Notes |
|---|---|---|---|---|
| Build (read + merge) | 98-120s | 876K-1.1M docs/s | 20-21 GB | Varies with system load |
| Persist (save_and_unload) | 37-49s | — | +0-2 GB during write | Zero-copy via fused_cow() |
| **Total (build + persist)** | **149-159s (~2.5 min)** | **662K-706K docs/s e2e** | **20-22 GB peak** | |
| Disk footprint | — | — | 8 GB | 7.2 GB filter + 866 MB sort + 15 MB system |

Usage:
```bash
# Benchmark binary (measures each phase separately)
cargo run --release --bin rebuild_bench -- --data-dir ./data --index civitai --full

# Server with --rebuild flag (same pipeline, starts serving after)
cargo run --release --features server --bin server -- --rebuild --port 3001 --data-dir ./data
```

---

## 4. Cache Performance

### Unified Cache Under Mixed Workload (105.3M, 8 workers)

Source: `docs/benchmarks/benchmark-mixed-workload.md` (Mar 11, 2026).

| Metric | Value |
|---|---|
| Cache hit rate | 94.8% |
| Cache entries | 210 (of 5,000 max) |
| Unique query fingerprints | 1,146 across 5,000 requests |
| Cache memory | 21.6 KB total |
| Memory per entry | ~103 bytes |
| Meta-index entries | 210 |
| Meta-index memory | 2.5 KB |

### Cache Hit vs Miss Latency (single-worker E2E)

| Query | Cold Miss | Cache Hit p50 | Speedup |
|---|---|---|---|
| nsfwLevel=1, reactionCount desc | 250ms | 4.78ms | 52x |
| nsfwLevel=1 + type, reactionCount desc | 53ms | 12.09ms | 4.4x |
| nsfwLevel=1, reactionCount asc | 152ms | 3.90ms | 39x |
| nsfwLevel=1, sortAt desc | 15ms | 5.16ms | 2.9x |
| nsfwLevel=1 + type, sortAt desc | 24ms | 12.51ms | 1.9x |

Cold miss times vary widely based on filter selectivity and sort field. Cache hits are consistently 3-13ms for single-worker HTTP round-trips.

### Trie Cache (Benchmark Harness, pre-unified)

| Scale | Trie Cache Size | Entries |
|---|---|---|
| 5M | 5.32 MB | 10 |
| 50M | 52.27 MB | 10 |
| 100M | 106.14 MB | 10 |
| 104.6M | 111.07 MB | 10 |

> The old trie cache stored full bitmaps per entry (~11 MB/entry at 105M). The unified cache stores only bounded bitmaps (~103 bytes/entry), a >100,000x reduction.

---

## 5. Bound Cache Impact

### Warm Bound Cache vs No Bounds (104.6M)

Source: `docs/benchmarks/benchmark-comparison-loading-mode.md`, bound cache cold/warm comparison.

| Query | No Bounds p50 | Warm Bounds p50 | Speedup |
|---|---|---|---|
| sort_reactionCount_desc | 9.01ms | 3.64ms | 2.5x |
| filter_nsfw1_sort_reactions | 7.71ms | 1.68ms | 4.6x |
| filter_tag_sort_reactions | 7.48ms | 2.14ms | 3.5x |
| filter_sort_commentCount | 7.03ms | 1.87ms | 3.8x |
| filter_sort_id_asc | 21.13ms | 1.61ms | 13.1x |
| filter_nsfw1_onSite_sort | 9.03ms | 4.96ms | 1.8x |
| filter_3_clauses_sort | 10.56ms | 6.08ms | 1.7x |

### Cold vs Warm Bound Cache (same session)

| Query | Cold p50 | Warm p50 | Speedup |
|---|---|---|---|
| all_sort_reactions | 10.05ms | 3.91ms | 2.6x |
| nsfw1_sort_reactions | 7.09ms | 1.52ms | 4.7x |
| nsfw1_onSite_sort_reactions | 9.58ms | 4.09ms | 2.3x |
| tag_sort_reactions | 7.32ms | 1.56ms | 4.7x |
| nsfw1_sort_commentCount | 8.54ms | 1.68ms | 5.1x |
| nsfw1_sort_id_asc | 22.15ms | 1.83ms | 12.1x |

Bound cache overhead: 6 bounds = 2.28 KB. Meta-index: 6 entries = 180 B. Negligible.

---

## 6. Regression Thresholds

These are guidelines, not hard gates. Hardware, OS, background load, and session variance all affect numbers.

| Metric | Baseline | Regression Threshold | Notes |
|---|---|---|---|
| **Sparse filter p50** (userId Eq) | 0.034-0.041ms | >0.1ms (>2x) | Should stay sub-100us at any scale |
| **Dense filter p50** (nsfwLevel Eq) | 4.7-7.8ms | >15ms (>2x worst) | Session variance is real; compare same session |
| **Sort + filter p50** (common case, bounds warm) | 1.7ms | >3.5ms (>2x) | Must keep bounds enabled |
| **Worst sort p50** (filter_sort_id_asc, bounds warm) | 1.6ms | >3.2ms (>2x) | Was 21ms without bounds |
| **Cache hit rate** (mixed workload) | 94.8% | <90% | Hot-pool-driven; real traffic may differ |
| **Single-worker cache hit p50** | 3-13ms | >25ms (>2x worst) | E2E HTTP including round-trip |
| **8-worker concurrent p50** | 259ms | >500ms | Memory-bandwidth-bound; hardware-dependent |
| **Bitmap memory at 105M** | 6.51 GB | >7.8 GB (+20%) | Linear scaling; watch tagIds growth |
| **RSS at 105M** | 14.51 GB | >17.4 GB (+20%) | Includes ArcSwap dual-snapshot overhead |
| **Bulk load rate (5M)** | 56K/s | <39K/s (-30%) | Loading mode enabled |
| **Bulk load rate (104M)** | 28K/s | <20K/s (-30%) | Degrades with bitmap size; expected |
| **Fused pipeline rate** | 320-365K/s sustained | <225K/s (-30%) | Full pipeline including docstore |
| **Upsert under load p50** | 43ms | >100ms (>2x) | 8 concurrent workers |
| **Delete under load p50** | 27ms | >60ms (>2x) | 8 concurrent workers |
| **Rebuild from docstore (105M)** | 149-159s total | >240s (+60%) | Build + persist; system-load-sensitive |
| **Rebuild peak RSS (105M)** | 20-22 GB | >30 GB (+40%) | Channel merge bounds memory |

### How to Use These Thresholds

1. **Same-session comparison is essential.** Benchmark numbers vary 2-3x with system load. Never compare runs from different sessions as a regression signal.
2. **Filter-only regressions are hard to distinguish from session variance.** The bound cache comparison showed apparent 1.5-1.7x filter slowdowns that were likely system load, not code regression.
3. **Sort query regressions are reliable** because bound cache improvement (2-13x) overwhelms session noise.
4. **Memory regressions are reliable** — bitmap memory is deterministic for the same dataset.
5. **If a threshold is exceeded**, re-run on a quiet system and compare against a known-good commit in the same session before investigating.

---

## Data Sources

| Document | Content | Date |
|---|---|---|
| `docs/benchmarks/benchmark-report.md` | 5M/50M/100M/104.6M scaling analysis, no bound cache | Feb 19, 2026 |
| `docs/benchmarks/benchmark-comparison-loading-mode.md` | 104.6M bound cache before/after, write perf | Feb 21, 2026 |
| `docs/benchmarks/benchmark-mixed-workload.md` | 105.3M mixed workload, unified cache, 8 workers | Mar 11, 2026 |
| `CLAUDE.md` | Memory tables, loading pipeline throughput | Ongoing |
| Loadtest (real traffic workload) | 105M HTTP throughput, c=1 to c=128, stable build | Mar 13, 2026 |
