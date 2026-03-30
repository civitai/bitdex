---
status: ACTIVE
created: 2026-03-29
updated: 2026-03-29
author: Edward (team lead)
reviewer: Dakota (doc keeper)
---

# Data Silo Benchmark — Implementation Plan

> Experiment plan to determine whether any silo approach beats current DocStore V2.
> Experiment design: `docs/design/silo-benchmark-experiments.md`
> Original silo design: `docs/design/data-silo-architecture.md` (DEFERRED)
> Original silo impl plan: `docs/design/data-silo-implementation-plan.md` (DEFERRED)

## Background

The initial silo implementation (per-thread large files + index) failed 107M validation:
OOM from missing multi-value accumulation, resources deadlock, double-write bug.
Justin redirected: before fixing bugs, **prove any silo approach actually beats the baseline**.

Justin's vision: deterministic slot locations, no index, zero memory overhead,
snapshot+ops model, flush thread writes, auto-compaction.

Success criteria: an approach ships ONLY if it beats baseline on ALL THREE:
- Write throughput: faster than current
- Read latency: equal or better
- Memory overhead: equal or less

---

## Team

| Role | Agent | Status |
|------|-------|--------|
| Team lead | Edward (ryan) | Active — coordinating |
| Baseline capture | Ollie | Assigned — awaiting dump completion |
| Bug fixes (Exp 1) | Mark | Assigned — fixing 3 bugs on data-silo branch |
| Prototype (Exp 2) | TBD | Blocked on baseline + doc size data |
| ShardStore eval (Exp 3) | TBD (Josh candidate) | Blocked on baseline |
| Doc keeper | Dakota | Reviewing this plan |

---

## Phase 1: Baseline Measurements (IN PROGRESS)

**Owner:** Ollie
**Status:** Dump running — 106M/108.9M rows processed

The baseline dump is running on main (v1.0.99) on port 3001 with RAYON_NUM_THREADS=16.
Data dir: `data/baseline-bench/`.

- [x] Build main branch server (features: server,pg-sync) — release mode
- [x] Create civitai index
- [x] Submit images dump (108.9M rows, with posts.csv enrichment)
- [ ] **B0: Write throughput** — parse server.log for elapsed_ms and rows, compute rows/sec
- [ ] **B1: RSS at completion** — tasklist.exe memory column
- [ ] **B2: Disk usage** — du -sh docstore/ and shardstore/ dirs
- [ ] **B3: File count** — count .bin files in docstore/
- [ ] **B4: Doc size distribution** — sample 200 shard files, parse V2 tuples, per-slot sizes (min/p50/mean/p95/p99/max)
- [ ] **B5: Read latency (cold)** — 100 queries with include_docs, p50/p95/p99
- [ ] **B6: Read latency (warm)** — 100 repeat queries, p50/p95/p99
- [ ] Write results to `data/baseline-bench/results.md`
- [ ] Kill server after capture

**Deliverable:** Baseline numbers that all experiments must beat.

---

## Phase 2: Bug Fixes — Experiment 1 Prep (IN PROGRESS)

**Owner:** Mark
**Branch:** `data-silo` (worktree: `.claude/worktrees/mark-data-silo`)

Three bugs found during code review must be fixed before re-benchmarking:

- [ ] **Fix 1: Double-write cfg gate** — dump_processor.rs ~lines 1968-2021, config_computed_sorts block writes to docstore unconditionally. Wrap in `#[cfg(not(feature = "data-silo"))]`.
- [ ] **Fix 2: Multi-value accumulation** — data_silo.rs local_index grows to 5.4B entries for tags (86GB OOM). Implement per-slot accumulation with HashMap, flush merged docs at batch boundaries. (Design doc concern #2)
- [ ] **Fix 3: filter_only skip** — skip silo writer creation for phases where filter_only=true (tags, etc.)
- [ ] Each fix committed separately with clear messages

**Deliverable:** Clean data-silo branch ready for re-benchmark.

---

## Phase 3: Experiment 1 — Index-Based Silos (BLOCKED on Phase 1 + 2)

**Owner:** TBD (Mark after fixes, or Ollie after baseline)

Re-run the same measurements from Phase 1 but on the data-silo branch with bug fixes.

- [ ] Build data-silo branch (features: server,pg-sync,data-silo)
- [ ] Run images dump (10M row subset for fast iteration)
- [ ] Capture: write throughput, RSS, disk, file count, read latency
- [ ] Compare vs baseline — does it win on ALL THREE metrics?
- [ ] If 10M results are promising, validate at 107M

**Deliverable:** Comparison table: index-based silos vs baseline.

---

## Phase 4: Experiment 2 — Deterministic-Offset Prototype (BLOCKED on Phase 1)

**Owner:** TBD
**Prerequisite:** Doc size distribution from Phase 1 (B4)

Justin's preferred approach. Three variants to evaluate:

### 4a: Fixed-size slots (padded)
- [ ] Determine slot size from doc size distribution (p99 or max)
- [ ] Prototype: `offset = slot_id * slot_size`, direct pwrite/pread
- [ ] Benchmark write throughput and read latency at 10M scale
- [ ] Measure disk waste (padding overhead)

### 4b: Page-aligned variable slots
- [ ] Fixed-size primary page per slot, overflow to secondary file
- [ ] Benchmark — measure overhead of two-file reads for large docs

### 4c: Thin slot table + data region
- [ ] Slot table: slot_id * 8 → (offset, length), ~860MB at 107M
- [ ] Data region: packed variable-length docs
- [ ] Benchmark vs 4a — is the indirection worth the disk savings?

**Deliverable:** Best deterministic-offset variant identified, compared to baseline.

---

## Phase 5: Experiment 3 — ShardStore-Native for Docs (BLOCKED on Phase 1)

**Owner:** TBD (Josh candidate — familiar with ShardStore)

Reuse existing ShardStore infrastructure with doc-specific sharding.

- [ ] Design doc-specific SnapshotCodec + OpCodec + ShardingStrategy
- [ ] Prototype with fewer, larger shards (e.g., SHARD_SHIFT=14 for ~6K shards)
- [ ] Images phase = snapshot, resources/tools/techniques = ops on top
- [ ] Auto-compaction when ops exceed threshold
- [ ] Benchmark same metrics at 10M scale

**Deliverable:** ShardStore-native approach compared to baseline.

---

## Phase 6: Results Report + Recommendation (BLOCKED on Phases 3-5)

**Owner:** Edward

- [ ] Compile all experiment results into comparison table
- [ ] Identify winner (if any) that beats baseline on ALL THREE metrics
- [ ] Write recommendation with evidence
- [ ] Present to Justin for decision

**Deliverable:** `docs/design/silo-benchmark-results.md` — go/no-go recommendation.

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-29 | Defer original silo implementation | 107M validation failed: OOM, deadlock, double-write |
| 2026-03-29 | Benchmark-first approach | Justin: "prove it beats baseline or don't ship" |
| 2026-03-29 | 10M rows for fast iteration | Justin: iterate fast, validate at 107M only if promising |
| 2026-03-29 | Deterministic-offset is preferred | Justin's vision: zero index overhead, O(1) lookup |

---

## Status Updates

### 2026-03-29 19:30
- Phase 1: Baseline dump at 106M/108.9M rows, server alive at 39GB RSS. Ollie assigned to capture metrics on completion.
- Phase 2: Mark assigned to fix 3 bugs on data-silo branch.
- Phases 3-5: Blocked on baseline. Team assignments TBD.
- Implementation plan created, sent to Dakota for review.

### 2026-03-29 20:48
- **Phase 1 COMPLETE.** Ollie captured all baseline metrics:
  - Write: 108.9M rows in 268s = **405,894 rows/sec**, 40.3 GB RSS
  - Disk: **35 GB** docstore, 3.7 GB bitmaps, **245,435 shard files**
  - Doc sizes: p50=148KB, p95=161KB, mean=145KB (~444 docs/file) — **tight distribution**
  - Warm read: p50=83ms client / 10μs server (doc cache), cold first query: 2s (lazy load)
  - Results at `data/baseline-bench/results.md`
- **Doc size distribution** shows fixed-size slots are viable (~10% waste at p99)
- Phase 2: Mark actively fixing bugs (grepping code, reading silo module)
- Dakota review complete — 5 gaps addressed in experiment doc
- Scarlet's Gate 5 comparison: her images took ~11min vs our 4.5min (different hardware)
- Phases 3-5 now UNBLOCKED by baseline completion

### 2026-03-29 21:37
- **Phase 3 (Experiment 1) COMPLETE.** Ollie benchmarked fixed silo code at 10M:
  - Write: **909K rows/s (2.2x baseline)**, cold start 261ms (7.8x faster)
  - 33 files vs 227K, warm read identical (11μs vs 10μs)
  - +17% disk from residual dual-write path
- **Phase 4 (Experiment 2) COMPLETE.** Mark prototyped deterministic-offset:
  - Exp 2a (seek+write): 96K docs/s — 4x slower, killed by syscall overhead
  - Exp 2b (mmap): **WINNER** — single-thread 2.07M/s (5.1x), 32-thread **6.49M/s (16x)**
  - Reads: p50=5.6μs cold (beats baseline warm!), memory: **zero**
  - Disk: 27.4 GB at 107M (10% waste from 256-byte slots vs ~230-byte avg docs)
- Phase 5 (ShardStore-native) SKIPPED — deterministic-offset dominates
- **RECOMMENDATION:** Deterministic-offset + mmap beats baseline on ALL THREE metrics:
  - Writes: 16x faster | Reads: faster (cold!) | Memory: zero
  - Awaiting Justin's decision to proceed with production implementation
