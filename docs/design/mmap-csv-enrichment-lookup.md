---
status: EXPERIMENT COMPLETE
created: 2026-04-04
author: Scarlet (team lead)
experiment-by: csv-mmap-experiment agent
---

# Mmap CSV Enrichment Lookup — Experiment Results

> Replace heap-based `HashMap<i64, LookupRow>` enrichment with mmap'd offset-index lookups.
> Save 6-14 GB heap at 107M scale. Build 10x faster. Lookup 6-8x slower but acceptable.

## Problem

Enrichment tables (posts, resources, model_versions, models) are currently parsed into `HashMap<i64, LookupRow>` before the images dump phase. At 107M scale:
- Posts: ~40M rows, ~2-3 GB heap
- Resources: ~777MB CSV
- Full enrichment chain: 6-15 GB heap

This consumes a large portion of the 28 GB RSS budget and takes 30+ seconds to build.

## Approach: Offset Index + Mmap

Instead of parsing the entire CSV into heap, build a lightweight index that maps keys to byte offsets in the mmap'd file. Lookups parse the CSV line on demand from the mmap.

### Four variants tested

| Approach | Description |
|---|---|
| A — Full HashMap | Current: parse all rows into `HashMap<i64, Vec<Option<String>>>` |
| B — HashMap offset index | `HashMap<i64, u64>` (key → byte offset) + mmap. Parse on demand. |
| C — Sorted Vec index | Sorted `Vec<(i64, u64)>` + binary search + mmap. Parse on demand. |
| D — Dense Vec index | `Vec<u64>` indexed by key (when keys are dense i64). O(1) lookup + mmap. |

## Benchmark Results (1M rows, 10 columns, 100K random lookups)

| Approach | Build time | Index memory | Lookup latency | Throughput |
|---|---|---|---|---|
| A — Full HashMap | 789 ms | ~156 MB | **110 ns/lookup** | 9.1M/sec |
| B — HashMap + mmap | 155 ms | ~28 MB | 931 ns/lookup | 1.07M/sec |
| C — Sorted Vec + mmap | 88 ms | ~15 MB | 1,298 ns/lookup | 770K/sec |
| D — Dense Vec + mmap | 79 ms | ~8 MB | 699 ns/lookup | 1.43M/sec |

## Projected to 40M Rows (Production Enrichment)

| Approach | Build time | Index memory | 14.6M lookups total |
|---|---|---|---|
| A — Full HashMap | ~32s | ~6-15 GB | **1.6s** |
| B — HashMap + mmap | ~6s | ~1.1 GB | 13.6s |
| C — Sorted Vec + mmap | ~4s | ~0.6 GB | 19s |
| D — Dense Vec + mmap | ~3s | **~0.3 GB** | 10.2s |

## Key Finding

On-demand parse is 6-8x more expensive per lookup than cached parse. The cost comes from:
1. Index lookup (cheap: 10-50 ns)
2. Line-end scan in mmap (memchr, possible page fault: 100-200 ns warm)
3. Field parsing from CSV line (~300-500 ns)

**However:** The benchmark uses random access patterns. Real dump processor lookups are sequential over sorted image IDs → enrichment pages stay warm in OS page cache. Effective cost: ~200-300 ns/lookup, making total ~3-4s for 14.6M lookups.

## Recommendation

**Use Approach D (Dense Vec) when keys are dense integer slot IDs** (posts, resources at Civitai).

- 10x faster build (3s vs 32s)
- 20-50x less memory (300 MB vs 6-15 GB) — **decisive for 28 GB RSS budget**
- Lookup: ~700 ns/lookup (pessimistic), ~200-300 ns with warm pages
- 14.6M lookups: ~3-10s total (acceptable — docstore write is already 85% of wall time)

**Fall back to Approach B (HashMap offset index) for sparse/non-integer keys.**

## Implementation Notes

- Mmap the enrichment CSV file (already done for large files in `load_fast()`)
- Build `Vec<u64>` offset index: scan mmap for line boundaries, extract key from first column
- Lookup: `offsets[key as usize]` → byte offset → read line from mmap → parse fields
- The mmap'd CSV stays in OS page cache — no additional memory budget needed
- After the dump phase, drop the offset Vec (300 MB freed). The mmap is unmapped automatically.

## Real-Data Benchmark (posts.csv 619MB, 23M rows + images-small.csv 2GB, 14.6M rows)

Real data INVERTED the synthetic results. Dense Vec is strictly better on all axes:

| Metric | HashMap (current) | Dense Vec + mmap | Improvement |
|---|---|---|---|
| Build time | 9.6s | **1.3s** | **7.6x faster** |
| Index heap | 1.09 GB | **214 MB** | **5.2x less** |
| Lookup latency | 401 ns/lookup | **251 ns/lookup** | **1.6x faster** |
| 14.6M lookups | 5.83s | **3.66s** | **1.6x faster** |

**Why:** Real enrichment access is sequential-ish (images in CSV order → nearby postIds). Mmap pages stay warm in OS page cache. HashMap at 1GB with 29M slots has poor spatial locality — pointer chasing into cold buckets.

**At 107M scale projection:**
- Build: 5s vs 40s
- Index heap: 320 MB vs 4.5 GB (decisive for 32 GB pod limit)
- Lookups: 19s vs 30s

## Status

Experiment complete. **Strictly better than HashMap on real data — no tradeoff.**
Benchmark binaries:
- `scratch/src/bin/csv_mmap_bench.rs` — synthetic 1M-row comparison
- `scratch/src/bin/csv_mmap_real_bench.rs` — real-data posts/images benchmark

Ready to integrate into the dump pipeline enrichment loader.
