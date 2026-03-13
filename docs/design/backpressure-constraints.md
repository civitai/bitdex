# Backpressure Pipeline — Constraints & Targets

## Performance Target

- **300K+ docs/s** sustained insert throughput (end-to-end, on-disk docstore via redb)
- Current measured rate: ~57K docs/s at 5M scale, ~28K docs/s at 104M scale
- Gap: **5.3x** at 5M, **10.7x** at 104M
- Target wall time for 104M record full load: **5-7 minutes** (currently ~70 minutes)

---

## Identified Bottlenecks

### 1. Unbounded doc_batch / doc_buffer at Burst Pressure

At Burst pressure, the current design skips persistence to maximize bitmap throughput. This means document data accumulates in memory without being flushed to redb. At 104M records with ~500 bytes/doc serialized, this would consume **~50 GB RAM** if persistence is skipped entirely.

**Requirement**: Documents MUST be flushed to redb in batches at ALL pressure levels, including Burst. Batch sizes scale with pressure (see Batch Size Guidelines below), but persistence is never skipped.

### 2. accumulate() Per-Doc Cost

BulkAccumulator's `accumulate()` costs ~4.87us/doc in the flush thread context (HashMap lookups + Vec pushes for filter fields, sort bit decomposition, alive tracking). At 4.87us/doc, the ceiling is **~205K docs/s** from the accumulator alone.

**Requirement**: Accumulator path must achieve 300K+ docs/s in isolation. Options: reduce per-doc cost via pre-sized HashMaps, or move accumulation to writer threads (per-thread accumulators that submit batched `BulkFlushResult`s).

### 3. staging.clone() on Snapshot Publish

Each `ArcSwap::store()` publish clones the entire `InnerEngine`: O(num_fields * num_values) Arc refcount bumps on FilterField HashMaps. At 104M scale with ~800K distinct filter values, this costs **5-50ms per publish**. Worse, it bumps refcounts so that the next `Arc::make_mut()` deep-clones the HashMap.

**Requirement**: Reduce publish frequency under pressure (already handled by PressureState) AND reduce per-publish cost. The current loading mode fix demonstrates the approach: skip publishing during bulk inserts. The backpressure gradient replaces loading mode with the same behavior at Hot/Burst levels.

---

## Hard Constraints

These are non-negotiable for any backpressure implementation.

### Persistence

1. **Documents MUST be flushed to redb at ALL pressure levels.** Never skip persistence. Batch sizes scale with pressure, but every document reaches disk.
2. **RAM must stay bounded.** `doc_buffer` and `doc_batch` must not grow linearly with total docs inserted. Hard cap: 100K entries in the write-through buffer (~50 MB). If exceeded, writers block until the persist thread drains.
3. **No "loading mode" hacks.** The backpressure gradient (Steady/Warm/Hot/Burst) handles everything. `enter_loading_mode()` / `exit_loading_mode()` are removed entirely.

### Correctness

4. **All 449 existing tests must pass after every change.** No exceptions.
5. **BulkAccumulator must produce identical results to the diff_document path.** Verified via proptest with 10K+ iterations. Filter bitmaps, sort bit layers, alive bitmap, and deferred alive must be bit-identical between paths.
6. **Readers must get reasonably fresh snapshots.** Steady/Warm: publish every cycle. Hot: publish every 10th cycle. Burst: publish every 50th cycle. De-escalation from Burst forces a full publish + compaction.

### API Contract

7. **`put()` must remain ~100ns.** Channel send only. No mutex, no snapshot load, no doc clone, no accumulator lock in the caller's hot path.
8. **Query correctness is never compromised.** Pressure levels only gate optional maintenance steps (compaction, bound cache, cache invalidation). Mutations are always drained and applied. Bitmap state is always correct.

### Project Principles (from CLAUDE.md)

9. **Bitmaps are the index.** No Vecs for column storage, no sorted arrays, no forward maps. All filtering and sorting via roaring bitmap operations.
10. **Documents stored on disk.** redb docstore keyed by slot ID for upsert diffing. The persist thread owns redb writes, decoupled from the flush thread.
11. **No sorted data structures.** Sorting via bit-layer bitmap traversal only.

---

## Verification Escalation Path

Each phase must pass at the current scale before moving to the next. Do not skip scales.

### 1. Component Microbenchmark

Prove 300K+ docs/s for each bottleneck fix in isolation:
- `accumulate()` throughput: target 400K+ docs/s per thread
- Persist thread `put_batch()` throughput: target 300K+ docs/s with batched redb transactions
- Channel send throughput: verify ~100ns/send for `InputMessage::Put`
- Snapshot publish cost: measure and log per-publish latency at each pressure level

### 2. 1M Integration Benchmark

- **Throughput**: 300K+ docs/s end-to-end
- **Correctness**: all 20 query types return correct results after insert
- **Persistence**: all 1M documents present in redb docstore after drain
- **RSS**: bounded, no unbounded growth during insert phase
- **Pressure transitions**: verify escalation to Hot/Burst during sustained insert, return to Steady after completion

### 3. 5M Integration Benchmark

- **Throughput**: 300K+ docs/s sustained (not just peak)
- **Correctness**: all 20 query types correct
- **Persistence**: all 5M documents on disk
- **RSS**: bounded, proportional to bitmap memory (expect ~1.2 GB RSS at 5M)
- **Diff sizes**: verify filter diffs stay bounded during sustained Hot/Burst

### 4. 104M Full Benchmark

- Only run after smaller scales pass
- **Throughput**: 300K+ sustained = ~5-7 minute wall time
- **Correctness**: all 20 query types correct at full scale
- **Persistence**: all 104.6M documents on disk
- **RSS**: should not exceed ~15 GB (current: 14.51 GB + ~100-150 MB pipeline overhead)
- **Query latency**: no regression from current bound-cache-enabled numbers

---

## Batch Size Guidelines

Redb transaction batch sizes scale with pressure level to balance write throughput against persistence latency.

| Pressure Level | Docs per redb Transaction | Persist Interval (flush cycles) | Rationale |
|----------------|--------------------------|-------------------------------|-----------|
| Steady | 1K | Every cycle | Low latency, frequent small commits |
| Warm | 5K | Every 2nd cycle | Moderate batching, slightly reduced commit frequency |
| Hot | 50K | Every 5th cycle | Large batches, reduced redb overhead per doc |
| Burst | 100K | Every 10th cycle | Maximum batching, minimize redb transaction count |

At 300K docs/s with Burst batches of 100K: 3 redb transactions/second. Each transaction writes ~50 MB (100K docs * ~500 bytes). NVMe sequential write throughput (~3 GB/s) handles this with headroom.

---

## Pressure Level Behavior Summary

| Level | Channel Fill | Drain Budget | Publish | Compact | Bounds | Cache Inval | Bulk Accum |
|-------|-------------|-------------|---------|---------|--------|-------------|------------|
| Steady | 0-20% | 1,000/cycle | every cycle | every 50 cycles | yes | yes | off |
| Warm | 20-50% | 5,000/cycle | every cycle | every 100 cycles | yes | yes | off |
| Hot | 50-80% | 20,000/cycle | every 10th | skip | skip | skip | **active** |
| Burst | 80-100% | 100,000/cycle | every 50th | skip | skip | skip | **active** |

Escalation is immediate. De-escalation uses hysteresis (separate, lower thresholds) to prevent oscillation.

---

## Memory Budget (104M Bulk Load)

| Component | Steady State | Peak (Bulk Load) |
|-----------|-------------|-----------------|
| Bitmap memory | 6.51 GB | 6.51 GB |
| Trie cache | ~160 MB | ~160 MB |
| Bound cache | ~3.7 KB | ~3.7 KB |
| Persist queue | ~0 | ~20 MB (4 batches) |
| Write-through buffer | ~0 | ~50 MB (100K cap) |
| Bulk accumulator (per thread) | ~0 | ~24 MB x M threads |
| Parser-to-writer channel | ~0 | ~13 MB (16K entries) |
| **Total additional RSS** | **~0** | **~100-150 MB** |

Additional memory is negligible relative to the existing 14.51 GB RSS.

---

## Reference

- **Design doc**: `docs/design/design-backpressure-implementation.md` — Pipeline redesign with put() simplification
- **Auto-throttle design**: `docs/design/design-backpressure-autothrottle.md` — Unified 4-layer design with thread model
- **Pressure state machine**: `src/pressure.rs` — PressureState with hysteresis-based escalation/de-escalation
- **Bulk accumulator**: `src/bulk_accumulator.rs` — BulkAccumulator with accumulate/flush/apply
- **Project principles**: `CLAUDE.md` — Inviolable design principles
- **Benchmark data**: `docs/benchmarks/benchmark-comparison-loading-mode.md` — Current 104M numbers with loading mode + bound cache
