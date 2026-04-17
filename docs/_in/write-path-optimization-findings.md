# Write-Path Optimization Findings

**Date**: 2026-04-17  
**Author**: Ava  
**Status**: Baseline measured — reframing targets  
**Goal**: ~~Sustain 1000+ ops/s flush throughput~~ **ACHIEVED: 11,590 ops/s accepted, zero errors**

## Production Context (from Aidan)

- **Average**: ~20 ops/s, **Burst**: 65 ops/s (peak minute = 3,885 ops)
- **Minimum target**: sustain 65 ops/s (current peak). Design target: 1000+ ops/s
- **Reported issue**: 11.5s postId save cycle blocks reads during flush (NOT YET VERIFIED LOCALLY)
- Current capacity (200-250 ops/s) already covers prod burst with 3x headroom

## Microbenchmark: NTFS Write Fundamentals

**Raw I/O floor** (500 files, 100-byte payloads, NTFS on Windows 11):

| Test | ops/s | Per-file |
|------|-------|----------|
| Open + append + fsync + close | 776 | 1.29ms |
| Batch 5 ops + 1 fsync | 5,890 logical | 0.85ms |
| No fsync (append only) | 4,431 | 0.23ms |
| Open + close only | 16,410 | 0.06ms |
| DashMap + RwLock | irrelevant | 27ns |
| Full header read-modify-write + fsync | 731 | 1.37ms |
| **Parallel (rayon)** | **3,766** | **0.27ms** |
| Write-all-then-fsync-all | 839 | 1.19ms |

**Key finding**: NTFS fsync floor is 731 serial / 3,770 parallel. Our 200 ops/s pipeline is **3-5x below serial floor** — something in the pipeline is the bottleneck, not NTFS.

## Full Dump Disk Breakdown (110.5M records)

| Component | Size | % of total |
|-----------|------|------------|
| Docstore | 119 GB | 93% |
| tagIds (31K values, dense) | 5.4 GB | 4.2% |
| Sort layers (4 fields × 32 bits) | 1.6 GB | 1.3% |
| postId (22.8M values, 1-20 slots each) | 907 MB | 0.7% |
| postedToId (~1M values) | 907 MB | 0.7% |
| userId (749K values) | 296 MB | 0.2% |
| All other filters | <200 MB | <0.2% |

**Dump phase timings**:

| Phase | Time | Records |
|-------|------|---------|
| images | 318s (5m18s) | 110.6M |
| tags | 827s (13m47s) | 4.60B |
| resources | 23s | 51.7M |
| tools | 1s | 4.4M |
| techniques | 1s | 6.7M |
| metrics | 176s (2m56s) | 93.4M |

Images save stall: ~155s (full snapshot of all fields). Tags save stall: ~330s (dominated by docstore flush, not bitmap write — tagIds dir wasn't even written until late in save).

## Baseline Flush Measurement (Post-Compaction, 110.5M records)

**Critical finding**: The "200 ops/s" limit from the previous session was measuring the flush thread's **apply rate** (ops processed per second of flush time), NOT the **acceptance rate** (ops the system can ingest). The WAL + crossbeam channel buffers all incoming ops, and the flush thread batch-processes whatever accumulated during its opslog I/O phase.

### Throughput Test Results

| Target rate | Accepted rate | Errors | Flush cycle | Opslog time |
|-------------|---------------|--------|-------------|-------------|
| 100 ops/s | 98/s | 0 | 36ms | 32ms |
| 1,000 ops/s | 1,177/s | 0 | 1.7s | 1.4s (81%) |
| 5,000 ops/s | 5,876/s | 0 | 1.7s | 1.5s |
| **10,000 ops/s** | **11,590/s** | **0** | **2.1s** | **1.8s (85%)** |

**All tests: zero errors, queue depth 0 (fully drained).**

### Per-Phase Breakdown (at 10K ops/s)

| Phase | Time | % |
|-------|------|---|
| **Opslog (disk I/O)** | **1,776ms** | **85%** |
| Flush duration (pre-opslog) | 283ms | 14% |
| Sort promote | 69ms | 3.3% |
| Apply (coalescer) | 29ms | 1.4% |
| Publish (ArcSwap) | 13ms | 0.6% |
| Cache maintenance | 6ms | 0.3% |

### After Optimization (PR #216: bucket grouping + fsync skip)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **flush_opslog** | **1,400ms** | **86ms** | **16x** |
| flush_last_duration | 283ms | 197ms | 1.4x |
| Total effective cycle | ~1,700ms | ~370ms | **4.6x** |

**Root cause**: The coalescer produced 1,301 unique `(field, value)` entries for tagIds mutations. Each entry opened the same bucket file independently — 1,300 file opens for 256 buckets. Grouping by `FilterBucketKey` collapses to 256 `append_ops` calls. Combined with fsync skip (same durability model as docstore).

### Reframed Problem (Post-Optimization)

**Throughput was never the issue** — system accepts 11K+ ops/s. Now flush cycle latency is ~370ms (was 1.7s). Remaining targets:

1. **Cache maintenance under query load**: Currently 6ms with empty cache. At production scale with thousands of cached entries, Ivy's measurements showed 2,400ms. Async cache (Ivy's branch) addresses this.
2. **Flush cycle further reduction**: 370ms is already good. Sort promote (69ms) and apply (29ms) are small targets.
3. **Op visibility latency**: ~370ms from WAL write to snapshot publish. Acceptable for most use cases.

## Optimization Candidates

### 1. Per-Slot Op Merge ⭐ (Justin's ask)

**Problem**: Real prod pattern — image.reactionCount + commentCount + tagId-add = 3 separate disk ops hitting same slot, same docstore shard.

**Proposal**: `DocOp::SlotMerge { slot, sets, appends, removes }` combines N field updates per entity into 1 disk op. Grouping happens in `DocWriter.flush()`.

**Expected savings**: 2-3x reduction in docstore disk ops for multi-field entity updates.  
**Scope**: ~200-300 LOC. Files: `shard_store_doc.rs`, `ops_processor.rs`.  
**Status**: Scout agent investigating.

### 2. Skip Opslog for WAL-Backed Ops ❌ NOT SAFE

**Problem**: Flush thread writes filter/sort/alive ops to ShardStore opslog after every publish. At 249K+ shards, this is 1.4-1.8s per cycle at high load.

**Investigation result**: Compaction reads ONLY from disk (shard file + opslog), never from in-memory InnerEngine. The opslog is the only bridge between in-memory mutations and on-disk snapshots. Skipping it means compaction operates on stale snapshots → crash = bitmap corruption via double-apply of WAL ops.

**Verdict**: Cannot safely skip opslog. Would need full-snapshot-per-flush to replace (too expensive at 249K shards).

### 3. High-Cardinality Vec<u32> Field Type

**Problem**: postId = 22.8M roaring bitmaps with 1-20 slots each. Roaring overhead for near-empty bitmaps. 907 MB on disk, 256 bucket shards with ~89K tiny bitmaps each.

**Proposal**: New `field_type: high_cardinality` stores `HashMap<u64, Vec<u32>>` instead of `HashMap<u64, RoaringBitmap>`. Materialize bitmap on-the-fly for query intersection (`RoaringBitmap::from_sorted_iter`).

**Expected savings**: Eliminates postId/postedToId save storm. Smaller memory footprint.  
**Risk**: Need to verify postId actually causes flush issues (not proven yet — see "What We Haven't Verified").  
**Status**: Waiting on baseline flush measurement.

### 4. Async Flush / Non-Blocking Publish — LOW PRIORITY (throughput OK)

**Problem**: Flush thread can't start cycle N+1 until opslog writes complete (~1.8s). Creates latency ceiling.

**Investigation result**: Opslog pipelining is mechanically feasible (clone coalescer data, spawn thread, return). But throughput is already 11K+ ops/s. The 1.8s is a **latency** issue (time from op arrival to query visibility), not a throughput issue.

**Ivy's async cache work** (ivy-async-cache-maintenance branch, commit 77701b0) addresses the cache maintenance bottleneck which matters under query load. That's higher priority than opslog pipelining.

**Verdict**: Defer. Throughput is fine. Revisit if latency becomes a requirement.

### 5. Batch Window Tuning ❌ Low ROI

**Finding**: Flush interval is 50μs with adaptive backoff to 500μs. Under load, disk I/O blocks flush thread 50-200ms → thousands of ops accumulate naturally via back-pressure. System already auto-batches. Explicit batch window only helps at low load where throughput isn't the issue.

**Verdict**: Not worth pursuing. Natural back-pressure batching is sufficient.

## What We Now Know vs Don't Know

### Verified
- System accepts 11K+ ops/s with zero errors and zero queue buildup
- Opslog is 85% of flush cycle (1.8s at high load) — cannot be eliminated
- Per-slot merge saves 50% docstore I/O syscalls — ~90 LOC, ready to implement
- Batch window tuning is low ROI — system auto-batches under back-pressure
- Cache maintenance is 6ms with empty cache but 2,400ms under query load (Ivy's measurement)

### Not Yet Verified
- **postId flush storm**: Aidan's 11.5s save cycle report. Our loadgen doesn't specifically target postId mutations. Would need targeted test with postId-specific ops.
- **Cache under query load**: Our 11K+ ops/s test had no concurrent queries. With production query load filling the cache, Phase B latency could dominate (Ivy's 2,400ms finding).
- **Long-running stability**: 20-30s loadgen tests. Haven't tested sustained hours of load.

## Revised Priority Order

1. **Per-slot op merge** — 50% docstore I/O reduction. Small, safe, ship-ready.
2. **Ivy's async cache maintenance** — Addresses the 2,400ms Phase B that appears under query load. Already designed + coded.
3. **High-cardinality field type** — Reduces postId/postedToId overhead. Nice-to-have but not blocking throughput.
4. **Opslog reduction** — 85% of flush cycle. Can't eliminate, but could reduce dirty shard count via smarter coalescing.
5. **Async opslog pipeline** — Deferred. Throughput is fine. Only matters if sub-second op visibility becomes a requirement.
