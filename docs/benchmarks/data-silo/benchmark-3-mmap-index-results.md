# Benchmark 3: mmap Index Startup — Results

**Date:** 2026-03-28
**Machine:** Windows 11, AMD (28 threads), 128 GB RAM, NVMe SSD
**Binary:** `scratch/src/bin/bench_silo_mmap_index.rs`
**Commit:** 27782ce (feat/sync-v2)

## Goal vs Actual

| Metric | Goal | Actual (cold) | Actual (warm) | Pass/Fail |
|--------|------|---------------|---------------|-----------|
| mmap creation time | <100ms | **0.118ms** | **0.086ms** | **PASS** (830x under goal) |
| First random access | <1ms | **0.006ms** | — | **PASS** (167x under goal) |
| 10K random accesses | <1ms total | 39.7ms (cold) | **0.418ms** | **PASS (warm)** |
| Per-access latency | 7ns target | 3965ns (cold) | **41.8ns** | see notes |

## Key Finding: Cold vs Warm Page Cache

The 7ns/access target from Fredrick's 1M-scale benchmark assumed hot page cache. At 107M (1.3 GB), cold random access triggers OS page faults:

| Scenario | 10K random | Per-access | Notes |
|----------|-----------|------------|-------|
| Cold (first touch) | 39.7ms | 3965ns | Page faults on each new 4KB page |
| After warmup (all pages touched) | **0.418ms** | **41.8ns** | 95x faster — pages in TLB/cache |
| Sequential (always hot) | 0.172ms | 17.2ns | Prefetch-friendly access pattern |
| 100K random (warm) | 3.179ms | 31.8ns | Consistent with 10K warm |

**Warmup cost:** Touching all pages (strided every ~315 entries to hit each 4KB page) takes **329ms**. This is a one-time cost on startup.

**Production implication:** The server runs continuously. After startup + warmup, all index pages live in the OS page cache (1.3 GB easily fits in 128 GB RAM). Steady-state random access will be ~32-42ns — 6x slower than the 7ns from Fredrick's small-scale test, but still **250x faster than DocStore disk reads (16ms)** and **24x faster than DocCache (<1us)**. The mmap approach decisively wins.

## Stability Check (5 cold mmap iterations)

| Run | mmap time | 10K random |
|-----|-----------|-----------|
| 1 | 86us | 18.4ms |
| 2 | 86us | 17.0ms |
| 3 | 98us | 17.6ms |
| 4 | 89us | 16.7ms |
| 5 | 85us | 16.6ms |

mmap creation is rock-stable at ~86us. Cold random access consistent at 16-18ms (stability runs had partial page cache warmth from prior runs).

## Raw Output

```
=== Benchmark 3: mmap Index Startup ===
Entries: 107000000 (1.30 GB)

Generating 1.30 GB index file...
  Generated in memory: 1.02s
  Written to disk: 15.44s (0.08 GB/s)
  Total generation: 16.49s

Index file: 1.30 GB (1391000004 bytes)

mmap creation: 0.118ms  (goal: <100ms)  PASS
First random access (slot 105234493): 0.006ms  (goal: <1ms)  PASS
  -> file_id=24, offset=749873522, length=166
10K random accesses: 39.653ms  (3965.3ns/access)  (goal: <1ms total)  FAIL
  checksum (prevent optimization): 3778925621838
100K random accesses: 260.042ms  (2600.4ns/access)
  checksum: 38221663660256
10K sequential accesses: 0.172ms  (17.2ns/access)
  checksum: 357680186

--- After full warmup (touching all pages) ---
Warmup (touch all pages): 329.083ms  cs=129483370451926
10K random (warm): 0.418ms  (41.8ns/access)  (goal: <1ms)  PASS
  checksum: 3788537233446
100K random (warm): 3.179ms  (31.8ns/access)
  checksum: 37944026148900

--- Stability check (5 iterations) ---
  Run 1: mmap=86us  10K_random=18.377ms (1837.7ns/ea)  cs=3810029677446
  Run 2: mmap=86us  10K_random=16.989ms (1698.9ns/ea)  cs=3810029677446
  Run 3: mmap=98us  10K_random=17.560ms (1756.0ns/ea)  cs=3810029677446
  Run 4: mmap=89us  10K_random=16.676ms (1667.6ns/ea)  cs=3810029677446
  Run 5: mmap=85us  10K_random=16.562ms (1656.2ns/ea)  cs=3810029677446
```

## Notes

- The 7ns/access from the design doc came from Fredrick's 1M-row benchmark where the entire index fits in L3 cache (~13 MB). At 107M (1.3 GB), random access has TLB pressure, but warm performance (42ns) is still far faster than any alternative.
- **Recommendation:** Add a startup warmup pass (~330ms) that touches all index pages. This converts the first-query penalty into a predictable startup cost. This is similar to the existing lazy-load warmup for sort bitmaps.
- File generation takes ~1s in memory + ~15s to write 1.3 GB — but this only happens during bulk load, not on every startup. Normal startup just mmap's the existing file (0.1ms).
- On Linux (production), `madvise(MADV_WILLNEED)` or `MAP_POPULATE` could pre-fault pages during mmap, avoiding the warmup pass entirely.
