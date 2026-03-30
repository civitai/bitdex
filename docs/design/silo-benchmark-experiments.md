---
status: DRAFT
created: 2026-03-29
author: Edward (team lead)
reviewer: Tom, Justin, team
---

# Data Silo Benchmark Experiments

> Before building further, we need to verify that any silo approach actually beats
> the current ShardStore doc persistence on writes, reads, AND memory.
> "If it's not faster, it's not worth doing." — Justin

## Context

The data silo project aimed to replace doc persistence with per-thread large files
to eliminate filesystem metadata overhead (210K small files → N large files).
Validation revealed implementation bugs (OOM from missing multi-value accumulation,
double-writes from ungated cfg paths) but also raised a fundamental question:
**does the silo approach actually outperform the current system?**

Justin clarified:
- Doc persistence is now ShardStore (snapshot + ops), NOT DocStore V2
- The silo vision is: deterministic slot locations, no index, snapshot+ops model
- Writes should go through the flush thread buffer, not direct rayon writes
- Auto-compaction when ops stack exceeds threshold (same as ShardStore)
- Goal: maximize read AND write throughput with zero additional memory overhead

## Baseline: Current DocStore V2 Persistence

**NOTE:** Code review of main (v1.0.99) confirms docs are stored via DocStore V2
(append-only tuple logs in hex-nested shard files), NOT ShardStore. ShardStore
handles bitmaps only. Justin's direction is to explore ShardStore-style
snapshot+ops for docs as a future approach.

**Known baseline numbers (from storage.md + code review):**
- Write throughput: **290K docs/sec** via BulkWriter
- Batch read latency: **21us** (DocStore V2 LIFO scan), **<1us** (DocCache hit)
- Shard files at 107M: **~205K** (SHARD_SHIFT=9, 512 docs/shard)
- Disk footprint at 100M: **~6GB** (uncompressed V2 format)
- DocCache: 1GB LRU DashMap (write-through from flush thread)

**Must verify with fresh measurements (Scarlet's code on main):**

| Metric | How to Measure |
|--------|---------------|
| Write throughput (images phase) | Time the images dump phase, count rows/s |
| Write throughput (resources phase) | Time resources dump, count rows/s |
| Read latency (cache miss) | GET /documents/{slot} with cold cache, p50/p95/p99 over 1000 random slots |
| Read latency (cache warm) | Same query repeated, measure cached path |
| Memory overhead for doc storage | RSS delta: server with bitmaps only vs server with bitmaps + docs |
| Disk usage at 107M | Total bytes in docstore shards |
| File count at 107M | Number of shard files created |

## Experiment 1: Index-Based Silos (Current Implementation, Bugs Fixed)

**What:** Fix the three bugs (double-write, multi-value accumulation, filter_only skip),
then re-run the same measurements.

**Hypothesis:** Write throughput improves (fewer files, sequential I/O) but memory
increases by ~1.6GB (DocIndex) + per-thread local index overhead.

| Metric | Expected vs Baseline |
|--------|---------------------|
| Write throughput | Faster (fewer file creates) |
| Read latency | Similar or faster (mmap vs shard file reads) |
| Memory overhead | WORSE (+1.4GB index minimum) |
| Disk usage | Similar (same data, different layout) |
| File count | Much less (28 + 1 vs 210K+) |

**Key question:** Is the write speedup worth the memory cost?

## Experiment 2: Deterministic-Offset Silos (Justin's Vision)

**What:** Fixed-size slots in a single large file. `offset = slot_id * slot_size`.
No index needed. mmap for reads. Snapshot + ops model like ShardStore.

**Design options to test:**

### 2a: Fixed-size slots (padded)
- Each slot gets `MAX_DOC_SIZE` bytes (e.g., 512 bytes)
- Pros: O(1) lookup, zero memory overhead, trivial implementation
- Cons: Wastes disk if docs vary widely in size
- Need to measure: what's the doc size distribution at 107M?

### 2b: Page-aligned variable slots
- Fixed-size "page" per slot (e.g., 256 bytes), overflow to a secondary file
- Pros: Good for common case, handles outliers
- Cons: Two-file reads for large docs

### 2c: Slot table + data region (thin indirection)
- Slot table: `slot_id * 8 bytes → (offset, length)` = 860MB at 107M
- Data region: variable-length docs packed contiguously
- Pros: Handles variable sizes efficiently
- Cons: Still needs ~860MB for the slot table (but less than 1.4GB index)

**Hypothesis:** 2a wins if doc sizes are uniform. 2c wins if they vary widely.

## Experiment 3: ShardStore-Style Silo (Snapshot + Ops)

**What:** Use the existing ShardStore infrastructure but with doc-specific
sharding strategy — fewer, larger shards. Images phase creates the snapshot.
Resources/tools/techniques are ops on top.

**Hypothesis:** Minimal code change (reuse ShardStore traits), proven architecture,
auto-compaction already works. May not be as fast as purpose-built silos but
has lowest risk.

## Measurements Required For Each Experiment

All measurements at 107M scale. 10M can be used for initial iteration but
**must be validated at 107M before shipping** — TLB pressure at scale changed
mmap reads from 7ns to 42ns (6x), which 10M would have missed.

1. **Bulk write throughput** — images phase: rows/s, wall clock time
2. **Bulk write throughput** — resources phase (with enrichment): rows/s
3. **Point read latency (cold)** — p50/p95/p99 over 1000 random slot reads
4. **Point read latency (warm)** — same after warmup
5. **Memory overhead** — RSS delta attributable to doc storage. Note: baseline
   includes 1GB DocCache (DashMap LRU). If silo uses mmap and eliminates DocCache,
   compare against baseline-minus-DocCache for fair comparison.
6. **Disk usage** — total bytes on disk for doc data
7. **File count** — number of files created
8. **Startup time** — time to mmap/load doc storage on server boot
9. **Compaction overhead** — time and memory for compaction at steady state
   (only applicable to Experiment 3 / ShardStore-native; Experiments 2a-2c don't compact)
10. **Concurrent read/write safety** — how reads behave during active writes.
    Current DocStore V2 uses append-only + LIFO which is inherently safe.
    Fixed-offset writes (Exp 2) need locking or CoW — measure any contention overhead.
11. **Enrichment overhead isolation** — measure enrichment chain time separately
    from raw storage I/O in the resources phase, so storage throughput is comparable.

## Pre-experiment: Doc Size Distribution

Before choosing between fixed-size and variable-size approaches, measure:
- Mean doc size at 107M (bytes)
- p50, p95, p99 doc sizes
- Max doc size
- Standard deviation

This determines whether fixed-size slots (Experiment 2a) are viable.

## Execution Plan

1. Measure baseline (Scarlet's main branch) — **first priority**
2. Measure doc size distribution — **needed before Experiment 2**
3. Fix bugs in current silo code, re-measure (Experiment 1)
4. Prototype deterministic-offset approach, measure (Experiment 2)
5. Evaluate ShardStore-native approach (Experiment 3)
6. Report results to Justin with recommendation

## Success Criteria

An approach ships only if it beats baseline on ALL THREE:
- Write throughput: faster than current
- Read latency: equal or better than current
- Memory: equal or less than current

If no approach meets all three, we keep the current ShardStore doc persistence.

## Team Review Requested

- **Tom (CTO):** Is this the right set of experiments? Missing anything?
- **Josh:** Any concerns about the deterministic-offset approaches from a Rust implementation perspective?
- **Scarlet's team:** Can you share exact baseline numbers from Gate 5 for the doc-specific metrics above?
- **Justin:** Does this capture your vision correctly?
