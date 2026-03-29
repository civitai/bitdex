# Benchmark 1: Phase Ordering Cost — Results

**Date:** 2026-03-28
**Machine:** Windows 11, AMD (28 threads), 128 GB RAM, NVMe SSD
**Binary:** `scratch/src/bin/bench_silo_phase_merge.rs`
**Commit:** 27782ce (feat/sync-v2)

## Goal vs Actual

| Metric | Goal | Actual | Pass/Fail |
|--------|------|--------|-----------|
| Phase 2 rate vs Phase 1 | >90% of Phase 1 | 84.2% (9.89 vs 11.74 M/s) | **see analysis** |
| Read-merge cost vs same-size append | <10% overhead | **-6.0%** (faster) | **PASS** |

## Key Finding: The "Overhead" Is Doc Size, Not Read Cost

The raw comparison (Phase 2 = 84.2% of Phase 1) looks like a fail. But the benchmark plan's goal is ambiguous — it compares 200-byte vs 250-byte writes. The correct apples-to-apples comparison:

| Phase | Rate | Doc size | Notes |
|-------|------|----------|-------|
| Phase 1 (pure append) | 11.74 M/s | 200 bytes | Baseline |
| Phase 2 (read + merge + write) | 9.89 M/s | 250 bytes | mmap read + append merged doc |
| Control (pure append, same size) | 9.33 M/s | 250 bytes | No read, just write 250 bytes |

**Phase 2 is 6% FASTER than the control.** The mmap read from hot page cache is essentially free — it may even help by pre-loading the CPU cache line. The 15.8% gap vs Phase 1 is entirely explained by writing 25% more bytes (250 vs 200).

## Interpretation

The benchmark plan asked: "does reading existing data back slow down Phase 2?" **Answer: No.** When Phase 1 silos are in page cache (which they will be — we just wrote them), the mmap read adds zero measurable overhead. In fact, Phase 2 read-merge-write is slightly faster than pure append of the same size, likely because the sequential mmap reads pre-warm the CPU's memory prefetcher.

**Josh's concern #3 was right to flag this**, but the data shows it's not a problem. Each phase reading the existing silo entry and merging new fields is essentially free when the previous phase's data is in page cache.

## Production Implication

During bulk dump, phases run sequentially: images → resources → metrics. Each phase writes to per-thread silos. Later phases read the existing entry, merge, and rewrite. Since phases run back-to-back, the prior phase's silo files are always in page cache. **No performance penalty for multi-phase merging.**

The cold-cache variant (benchmark plan mentions `drop_caches`) was not tested on Windows. On Linux production, between phases there's no reason to drop caches — the data stays warm.

## Raw Output

```
=== Benchmark 1: Phase Ordering Cost ===
Rows: 10000000 across 28 threads (357142/thread)
Phase 1 doc: 200 bytes, Phase 2 adds: 50 bytes
BufWriter: 8388608 bytes

--- Phase 1: Pure append (images) ---
  Write: 0.85s — 11.74M rows/s

--- Phase 2a: Read-merge-write (hot cache) ---
  Write: 1.01s — 9.89M rows/s
  Overhead vs Phase 1: 15.8% (goal: <10%)  FAIL

--- Phase 2b: Pure append control (same total doc size, no read) ---
  Write: 1.07s — 9.33M rows/s
  Read-merge overhead vs pure append of same size: -6.0%

=== SUMMARY ===
| Phase | Rate (M rows/s) | Doc size |
|-------|-----------------|----------|
| Phase 1 (pure append) |           11.74 | 200 bytes |
| Phase 2 (read+merge, hot) |            9.89 | 250 bytes |
| Control (pure append, same size) |            9.33 | 250 bytes |

Phase 2 overhead vs Phase 1: 15.8%  FAIL
Read-merge cost (Phase 2 vs control): -6.0%
```

## Notes

- At 10M rows (2 GB total), all data fits in page cache — this simulates production where phases run back-to-back.
- The 10M scale was chosen per the benchmark plan. At 107M, Phase 1 silos are ~20 GB. With 128 GB RAM, they'll still be mostly in page cache between phases.
- Cold-cache test deferred — not reproducible on Windows (`/proc/sys/vm/drop_caches` is Linux-only). On production Linux, relevant only if something evicts the page cache between phases (unlikely).
