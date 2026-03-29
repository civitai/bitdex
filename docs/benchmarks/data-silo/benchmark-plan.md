# Data Silo Benchmark Plan

> Validates design assumptions before implementation. Each benchmark has a goal, methodology, and pass/fail threshold. Run in order — early failures save time on later benchmarks.
>
> Agreed by: Dakota (Doc Keeper) + Josh (Rust Engineer), 2026-03-28
> Design doc: `docs/design/data-silo-architecture.md`

---

## Execution Order: 0 → 3 → 1 → 4 → 2

Cheapest/fastest first. If benchmark 0 fails, the whole design needs rethinking.

---

## Benchmark 0: Full-Scale Write Throughput (BLOCKING)

**Goal:** Sustain >=10M rows/s at 107M rows (not just 1M from Fredrick's bench)
**Why:** The 20.1M/s number was at 1M scale. Must verify it holds when files grow to multi-GB and OS buffer cache pressure increases.

| Metric | Goal | Fredrick's 1M result |
|--------|------|---------------------|
| Write rate (28 threads) | >=10M rows/s | 20.1M rows/s |
| Total write time (107M × 200 bytes) | <11s | extrapolated ~5.3s |

**Methodology:**
- 28 threads, each writing ~200 byte docs to own silo file
- 107M total rows (~3.8M per thread)
- Measure: sustained rate, final file sizes, peak RSS
- Binary: `scratch/src/bin/bench_silo_write_107m.rs`

**If it fails:** The filesystem can't handle multi-GB sequential appends at speed. Would need to investigate: larger write buffers, fewer threads, or a different file layout.

---

## Benchmark 3: mmap Index Startup

**Goal:** mmap 1.4GB index file and first random access in <1s
**Why:** Startup latency matters — server must be ready quickly after restart.

| Metric | Goal |
|--------|------|
| mmap creation time | <100ms |
| First random access | <1ms |
| 10K random accesses | <1ms total (7ns each expected) |

**Methodology:**
- Create a 107M-entry index file (107M × 13 bytes = ~1.4GB)
- Time: mmap call, first access, 10K random lookups
- Binary: `scratch/src/bin/bench_silo_mmap_index.rs`

---

## Benchmark 1: Phase Ordering Cost (read-merge-write)

**Goal:** Cross-phase read-merge-write adds <10% overhead vs single-phase pure append
**Why:** Dump phases write different fields to the same slots. Later phases must read existing data, merge, and rewrite.

| Metric | Goal |
|--------|------|
| Phase 1 rate (pure append) | baseline |
| Phase 2 rate (read + merge + append) | >90% of Phase 1 |
| Phase 2 with cold cache (drop page cache) | >50% of Phase 1 |

**Methodology:**
- Phase 1: Write 10M docs (images fields, ~200 bytes each) to 28 silo files
- Phase 2: Write same 10M docs (resources fields, ~50 bytes) with read-merge-write
- Run twice: once with hot page cache, once after `echo 3 > /proc/sys/vm/drop_caches` (Linux) or equivalent
- Binary: `scratch/src/bin/bench_silo_phase_merge.rs`

**Josh's note:** If phase 1 silos are still in page cache (likely at ~20GB), the read is essentially free. The cold-cache variant is the real test.

---

## Benchmark 4: Cross-Silo Point Reads (CRITICAL for correctness)

**Goal:** Reading a document whose fields span 3 phases takes <100us
**Why:** If cross-silo reads are slow, the multi-phase model breaks. This determines merge-on-write vs merge-on-read.

| Metric | Goal |
|--------|------|
| Single doc read (3 silo lookups + merge) | <100us |
| 10K batch reads | <100ms total |
| vs current DocCache | comparable (<1us hot target) |

**Methodology:**
- Create 3 silo files (images, resources, metrics phases)
- Write 10M docs split across all 3
- mmap all 3, build unified index
- Measure: single slot lookup + merge, 10K random slots, batch sequential
- Binary: `scratch/src/bin/bench_silo_cross_read.rs`

**Josh's note:** This is the most important benchmark for correctness. If cross-silo reads are too slow, we switch to merge-on-write (each phase writes complete docs) instead of merge-on-read.

---

## Benchmark 2: Multi-Value Accumulation Memory (LOW PRIORITY)

**Goal:** Hypothetical — what if we remove filter_only from tagIds?
**Why:** Currently tags are filter_only (skip docstore). If we ever need them in docstore, need to know the memory cost.

| Metric | Goal |
|--------|------|
| Peak RSS for 107M × 42 tags accumulated | <16GB |

**Methodology:**
- Simulate: HashMap<u32, Vec<u32>> accumulating 42 tags per image for 107M images
- Measure peak RSS
- Binary: `scratch/src/bin/bench_silo_multivalue_memory.rs`

**Josh's note:** This is hypothetical. Tags ARE filter_only and will stay that way. The real multi-value fields (tools/techniques) are tiny (4M/6M rows). Don't block implementation on this.

---

## Deferred: Benchmark 5 (Compaction)

Dead space is <1% after 10K upserts. Compaction won't be needed for months in production. Revisit when the steady-state upsert volume is measured.

---

## Results Template

Each benchmark result goes in this directory as `{benchmark-name}-results.md`:

```markdown
# Benchmark N: {name} — Results

**Date:** YYYY-MM-DD
**Machine:** {CPU, RAM, disk}
**Binary:** scratch/src/bin/{name}.rs
**Commit:** {hash}

## Goal vs Actual

| Metric | Goal | Actual | Pass/Fail |
|--------|------|--------|-----------|
| ... | ... | ... | ... |

## Raw Output
{paste terminal output}

## Notes
{anything surprising or noteworthy}
```
