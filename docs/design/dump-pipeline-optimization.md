---
status: IN PROGRESS
created: 2026-04-04
author: Scarlet (team lead), Justin (direction)
last-updated: 2026-04-04 (after compaction experiment + MultiOps full-dump regression)
---

# Dump Pipeline Optimization

> Target: close the gap from ~600K rows/sec toward millions.
> Approach: fix → bench → measure lift → commit → next fix.

## Current Numbers

**Baseline (pre-optimization):** 579K rows/sec (images-small, 14.6M rows)
**Current best:** 474K rows/sec (with from_sorted_iter, different system load — relative gains are valid)

### Per-stage breakdown (14.6M images, after mmap enrichment + alloc fixes)
| Stage | Time | % | Target |
|---|---|---|---|
| Enrichment build | **1.4s** | 6% | Done (mmap Dense Vec) |
| Parse+merge (rayon) | **19.5s** | 80% | See per-row breakdown below |
| Doc compact | ~3.5s | 14% | Zero-copy compaction (DONE, benchmarking) |

### Parse per-row breakdown — REAL TIMING DATA (dump-timing feature, 14.6M images)

Measured via `#[cfg(feature = "dump-timing")]` instrumentation. 20.2µs/row total (32 threads).

| Category | ns/row | % | Status |
|---|---|---|---|
| **doc_encode** | 4,758 | **23.5%** | Field collection + encode + mmap write. Needs sub-instrumentation. |
| **filter_bm_insert** | 2,845 | **14.1%** | HashMap inserts for filter bitmaps. High-cardinality dominates. |
| **config_sort_late** | 2,569 | **12.7%** | DUPLICATE of early — same GREATEST/LEAST computation. **FIX QUEUED.** |
| **enrichment_bm** | 2,104 | **10.4%** | Enrichment-derived bitmap inserts |
| **computed_field** | 2,100 | **10.4%** | Computed field eval + bitmap inserts |
| **config_sort_early** | 1,876 | **9.3%** | First config-computed sort eval. Combined with late = 22%. |
| **enrichment** | 1,727 | **8.5%** | Mmap lookup + CSV parse on demand (was 400ns HashMap, now 251ns Dense Vec) |
| csv_parse | 827 | 4.1% | parse_delimited_line — already efficient |
| indexed_fields | 357 | 1.8% | fill_indexed_fields (fixed — was 80ns est, now reused) |
| deferred_alive | 120 | 0.6% | |
| slot_extract | 92 | 0.5% | |
| filter_expr | 40 | 0.2% | |
| sort_bm_insert | 0 | 0.0% | from_sorted_iter moved all work to post-loop |

**Key insight:** `enriched_get` closure called **8.0 times/row** with O(n) linear scan. Contributes to enrichment_bm + computed_field overhead. Converting to AHashMap or indexed array would help multiple categories.

---

## Completed (committed to main)

| # | Fix | Commit | Result |
|---|-----|--------|--------|
| 1 | Thread-local reusable buffers | 76c6ac1 | +3% (579K → 597K) |
| 2 | HashMap capacity pre-alloc | 2199251 | Noise |
| 3 | ParallelOpsWriter for MV phases | 53984aa | MV phases now parallel (real win at 107M on tags) |
| 4 | ParallelOpsWriter overflow detection | 0a7b957 | **Correctness fix** — ops were silently dropped |
| 5 | Sort bitmap from_sorted_iter | 9917821 | 5.86x faster sort bitmap construction |
| 6 | V2 reference cleanup | 4c139be | Cleanup |
| 7 | madvise hints (8 sites) | 121501d | Sequential/Random/HugePage/DontNeed on all mmaps |
| 8 | Bitmap merge strategy bench | 4c7bbea | MultiOps::union() confirmed 2.7-5.2x faster |

## Rejected After Benchmarking

| Experiment | Result | Why |
|---|---|---|
| **Filter bitmap from_sorted_iter** | REGRESSION (474K → 365K) | High-cardinality fields (userId, postId) create millions of tiny Vec<u32> with 1-5 entries. sort + from_sorted_iter overhead on tiny Vecs exceeds direct insert. Only works for few-keys-many-values (sort layers). |
| **Frozen bitmap merge** | 1.1x-2.4x SLOWER at all thread counts | Rayon parallel tree reduction beats sequential compaction. Frozen OR still materializes full bitmap. Bench: `scratch/src/bin/frozen_merge_bench.rs` |
| **Parallel cold compaction** | 0.68-0.91x baseline (all approaches slower) | Memory-bandwidth saturated on Windows — sequential prefetcher wins. TLB thrash at multiple threads. Bench: `scratch/src/bin/parallel_compact_bench.rs` |
| **MultiOps::union() as default merge** | 632K vs 750K rows/sec (regression) | Micro-bench wins don't translate to full dump. Kept as opt-in `streaming_merge` config. |

---

## In Progress

### Lucy implementing now:
- **Mmap enrichment with Dense Vec** — DONE, committed
  - Real data bench: build 1.3s (vs 9.6s), memory 214MB (vs 1.09GB), lookups 251ns (vs 401ns)
  - Strictly better on ALL axes. See: `docs/design/mmap-csv-enrichment-lookup.md`
- **MultiOps::union() merge** — made configurable via `streaming_merge` flag on DumpRequest
  - Micro-bench: 2.7x faster merge. Full dump bench: REGRESSION (632K vs 750K rows/sec)
  - Kept as opt-in config option, not default. Needs investigation on why full-dump regresses
- **ahash HashMap replacement** — in progress, fixing type boundary issues

### Shared bitmap accumulation — RESULTS IN, hybrid approach:
- **No single approach wins all cardinalities.** Hybrid needed based on field config.
- **Low/mid-card (<50K values: nsfwLevel, tagIds):** Keep per-thread bitmaps, replace rayon reduce with `MultiOps::union()`. 1.8-5x faster merge. Zero structural change.
- **High-card (>50K values: userId, postId):** Per-thread `Vec<(u64, u32)>` tuples → concat → sort → group-by → `from_sorted_iter`. 3x faster, zero locks, ~175MB buffer.
- DashMap<Mutex<bitmap>> is catastrophic at low cardinality (57x slower — 14.6M threads on 5 mutexes)
- Benchmark at: `scratch/src/bin/shared_bitmap_bench.rs`

### Compaction optimization — RESULTS IN, zero-copy is the win:
- **Parallel scan REJECTED** — all parallel approaches slower than sequential baseline
  - Approach 2 (prescan+par): 0.68-0.76x baseline
  - Approach 3 (byte-range): 0.74-0.91x baseline
  - Thread scaling: never beats 1 thread (0.61x at 2T, 0.86x at 32T)
  - Root cause: memory-bandwidth saturated. Cold mmap + no MADV_SEQUENTIAL on Windows = TLB thrash
- **Zero-copy compaction (Approach 5)** — scan without value copies is **43% faster** (335ms vs 584ms at 377MB)
  - Current: `HashMap<key, Vec<u8>>` copies 4.4GB of values to heap, then reads back for write = 9GB traffic
  - Fix: `HashMap<key, (mmap_offset, value_len)>` — write phase reads directly from source mmap = 4.4GB traffic
  - Expected: ~1.7x speedup on compaction scan phase
  - **This is the correct P0 optimization for compact_cold_from**
- Parallel scan may be viable on Linux with MADV_SEQUENTIAL — retest on prod pod separately
- Benchmark at: `scratch/src/bin/parallel_compact_bench.rs`

### Allocation reduction — RESULTS IN, two wins to ship:
- **S2: Reuse parse_delimited_line buffer** — 275ns → 99ns per row (2.8x). 5 min change.
- **S5: Eliminate duplicate sort-value HashMap** — 338ns → 63ns per row (5.4x). 20 min refactor.
- Combined: ~450ns/row saved → ~860ms off parse phase (~8%)
- Rejected: stack arrays (<10ns gain), arena allocator (marginal after S2+S5), columnar parse (risky rewrite)
- Already fixed: indexed_fields + enrichment buffers already use clear+reuse pattern
- Benchmark at: `benches/parse_alloc_bench.rs`

---

## Queued (ready to implement after current investigations)

| Priority | Optimization | Expected Impact | Status |
|---|---|---|---|
| P0 | **Hybrid bitmap merge (cardinality-aware)** | 1.8-5x merge for low/mid-card, 3x for high-card | Benchmarked, Lucy implementing |
| P0 | **ahash HashMap replacement** | 10-20% on HashMap-heavy ops | Committed (4232e13), Lucy finishing remaining files |
| P0 | **Parse buffer reuse (FIX A)** | 2.8x faster field parsing (275→99ns/row) | Benchmarked, Lucy implementing |
| P0 | **Eliminate duplicate sort HashMap (FIX B)** | 5.4x faster sort vals (338→63ns/row) | Benchmarked, Lucy implementing |
| P0 | **Zero-copy compaction** | ~1.7x faster compact scan (eliminate 4.4GB heap alloc) | Benchmarked, ready to implement |
| P2 | **msync(MS_ASYNC) at commit points** | Steady-state durability pipelining | Not started, low priority |
| P2 | **Parallel compact on Linux** | Retest approach 3 with MADV_SEQUENTIAL on prod pod | Deferred until zero-copy ships |
| P2 | **Sparse file pre-allocation** | Eliminate DataSilo grow+remap | Evaluate feasibility |
| P2 | **io_uring + mmap** | Async batched I/O ceiling | Exploratory |

---

## Benchmark Reference

### Bitmap insert strategies
| Approach | 10M random | 32-layer model (7.3M values) |
|---|---|---|
| individual insert() | 1,540ms | 9,592ms |
| sort + from_sorted_iter | **221ms** | **1,638ms (5.86x)** |

### Bitmap merge strategies (8 threads, 100K entries — tagId shape)
| Approach | Time | vs current |
|---|---|---|
| Rayon par_iter reduce (current) | 6.18ms | baseline |
| Sequential pairwise |= | 9.13ms | 1.5x slower |
| **MultiOps::union()** | **2.27ms** | **2.7x faster** |

### Mmap enrichment (real data: posts.csv 23M rows)
| Approach | Build | Memory | 14.6M lookups |
|---|---|---|---|
| HashMap (current) | 9.6s | 1.09 GB | 5.83s (401 ns/ea) |
| **Dense Vec + mmap** | **1.3s** | **214 MB** | **3.66s (251 ns/ea)** |

### Cold compaction scan strategies (377MB synthetic ops log, 1M keys × 300B values)
| Approach | Time | vs baseline |
|---|---|---|
| Baseline (sequential scan) | 584ms | — |
| Approach 2 (prescan + parallel chunks) | 704ms | 0.83x |
| Approach 3 (byte-range parallel) | 586ms | 1.00x |
| Approach 4 (seq scan + Vec sort) | 633ms | 0.92x |
| **Approach 5 (no value copy)** | **335ms** | **1.74x** |

### Roaring fork additions (committed to frozen-mmap-support branch)
| Method | Use case | Performance |
|---|---|---|
| `apply_ops()` | Query-time ops-on-read | 60µs at 10M (container-level CoW) |
| `patch_frozen_inplace()` | Janitor in-place compaction | <1µs per op (direct bit flip) |

---

## Key Findings

1. **Allocator is the #1 bottleneck** — 60%+ of parse time is Vec/String/HashMap allocation, not computation
2. **from_sorted_iter is shape-dependent** — wins big for few-keys-many-values (sort layers), loses for many-keys-few-values (filter fields)
3. **Rayon parallel merge is worse than single-threaded** for memory-bandwidth-bound bitmap OR — MultiOps::union() streaming N-way merge is 2.7-5x better
4. **Mmap + offset index beats HashMap** on real data (sequential access patterns keep pages warm)
5. **madvise hints are free** — always-on, no-ops on Windows, help kernel optimize page management
6. **Parallel scan is memory-bandwidth limited** — on Windows without madvise, sequential prefetcher beats N-thread access. All parallel approaches 0.68-0.91x baseline. Zero-copy (skip heap allocation) is 1.74x.
7. **Micro-bench wins don't always translate** — MultiOps::union() is 2.7x faster in isolation but REGRESSES on full 14.6M dump (632K vs 750K rows/sec). Always validate with full pipeline bench.

## Deferred / Not Doing

- ~~mimalloc/jemalloc locally~~ — jemalloc in prod, can't use on Windows
- ~~LTO fat~~ — release only, minor gain
- ~~SIMD CSV parsing~~ — current parser already zero-copy, diminishing returns
- ~~Enrichment as separate phases~~ — deferred to V3 cross-silo bitmaps
- ~~DataSilo for enrichment~~ — deferred to V3
- ~~Frozen bitmap merge~~ — benchmarked, slower than current parallel reduce
- ~~Filter bitmap from_sorted_iter~~ — regression on high-cardinality fields
- ~~Parallel cold compaction~~ — memory-bandwidth saturated, all parallel approaches slower than sequential (Windows)
- ~~MultiOps::union() as default merge~~ — micro-bench 2.7x faster but full dump regresses (632K vs 750K). Kept as opt-in `streaming_merge`
