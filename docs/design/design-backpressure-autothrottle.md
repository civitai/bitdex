# Unified Backpressure & Auto-Throttle Design

## 1. Executive Summary

Bitdex V2's current write path achieves 62K docs/s at 104M records, limited by a single-threaded flush loop that serializes bitmap mutation, snapshot publishing, and redb document persistence. This design unifies four complementary proposals into a single coherent system: **pressure-driven adaptive flushing** replaces the binary loading mode with smooth 4-level auto-throttling; a **bulk accumulator** bypasses per-document MutationOp overhead during high-pressure writes; a **dedicated persist thread** decouples redb I/O from the bitmap hot path; and a **parallel mmap+memchr ingestion pipeline** saturates the write path from the read side. Together, these layers target **300K docs/s sustained throughput**, ingesting 104M records in 5-7 minutes.

---

## 2. Architecture Overview

### 2.1 Thread Model

```
                         +--------------------------+
                         |    NDJSON Source File     |
                         |       (mmap, 59 GB)      |
                         +----+-----+-----+----+----+
                              |     |     |    |
                      chunk0  | ... |     |    | chunkN
                              v     v     v    v
                         +----------------------------+
                         |   Parser Threads (N = 6)   |  Stage 1: mmap + memchr + serde_json
                         |  Each: memchr line scan    |  Zero-copy byte slices
                         |  serde_json::from_slice    |  to_document()
                         +----------------------------+
                                      |
                          bounded crossbeam channel
                            (u32, Document) x 16K
                                      |
                              +-------+-------+
                              v               v
                         +----+----+    +-----+----+
                         | Writer  |    | Writer   |  Stage 2: 2-4 writer threads
                         | Thread  |    | Thread   |  call engine.put() or engine.put_bulk()
                         +---------+    +----------+
                              |               |
              +---------------+---------------+--------+
              | MutationOp channel (100K bounded)       |  Normal pressure (Level 0-1)
              | OR BulkAccumulator submit (Level 2-3)   |  High pressure
              +-------------------+--------------------+
                                  |
                                  v
                    +-----------------------------+
                    |       Flush Thread          |  Stage 3: drain + apply + publish
                    |  PressureState drives:      |
                    |  - publish cadence           |
                    |  - compaction frequency       |
                    |  - bound/cache maintenance    |
                    |  - bulk accumulator flush     |
                    +-----------------------------+
                         |                  |
              ArcSwap::store()        enqueue Vec<(u32, StoredDoc)>
              (snapshot publish)       (non-blocking try_send)
                         |                  |
                         v                  v
                    +---------+     +------------------+
                    | Readers |     |  Persist Thread  |  Stage 4: redb docstore writes
                    | (query) |     |  (dedicated)     |  Decoupled from flush hot path
                    +---------+     +------------------+
                                          |
                                    redb DocStore
                                          |
                    +------------------+  |
                    |  Merge Thread    |  |  Existing: periodic bitmap compaction
                    |  (5s interval)   |  |  Writes to separate BitmapStore redb
                    +------------------+  |
                           |              |
                    redb BitmapStore   redb DocStore
                    (separate DB)      (separate DB)
```

### 2.2 How the Four Proposals Fit Together

The proposals are **complementary layers**, not competing alternatives:

| Layer | Proposal | Role | Bottleneck Addressed |
|-------|----------|------|---------------------|
| **Read** | Parallel Ingestion (P4) | Saturate the write path from I/O side | JSON parsing (single-threaded serde_json) |
| **Write** | Bulk Accumulator (P2) | Eliminate per-doc MutationOp overhead | Channel transport + allocation (20 ops/doc) |
| **Flush** | Adaptive Backpressure (P1) | Auto-throttle publish/maintenance cadence | staging.clone() cascade, cache/bound maintenance |
| **Persist** | Hybrid Flush (P3) | Decouple redb I/O from flush hot path | redb write transaction blocking flush thread |

Each layer can be implemented and benchmarked independently. The combined system achieves multiplicative gains.

---

## 3. Unified Design

### 3.1 Pressure-Driven Flush Loop (P1 Core)

The channel depth (`rx.len() / capacity`) is the single control signal. Four pressure levels drive the cadence of all optional flush operations.

| Level | Fill Ratio | Name | Publish | Compact | Bounds | Cache Inval | Bulk Accum |
|-------|-----------|------|---------|---------|--------|-------------|------------|
| 0 | 0-10% | Idle | every cycle | every 50 cycles | yes | yes | off |
| 1 | 10-50% | Warm | every 4th | every 100 | yes | yes | off |
| 2 | 50-80% | Hot | every 16th | every 200 | skip | yes | **activate** |
| 3 | 80-100% | Critical | skip | on transition only | skip | skip | **activate** |

**Key behavioral rules:**

- **Escalation is immediate.** When channel depth crosses a threshold upward, the system escalates instantly. The channel is filling; do not wait.
- **De-escalation has hysteresis.** Require 2+ cycles below threshold before dropping a level. This prevents oscillation at boundaries.
- **De-escalation from Level 3 forces a full publish.** Same semantics as the current `exit_loading_mode()`: compact all filter diffs, invalidate all caches, publish snapshot.
- **Mutations are always drained and applied.** Pressure levels only gate the *optional* steps (publish, compact, maintain, invalidate). Correctness is never affected.

```rust
struct PressureState {
    level: u8,                    // 0-3
    cycles_since_publish: u64,
    cycles_since_compact: u64,
    level_entry_time: Instant,
}
```

**This replaces the binary `loading_mode` flag entirely.** See section 4.1 for the resolution.

### 3.2 Bulk Accumulator Activation via Pressure (P1 + P2 Integration)

The bulk accumulator activates when pressure reaches Level 2 or higher. This is the critical integration point between P1 and P2.

**Why Level 2 is the trigger (not Level 3):**

At Level 2 (50-80% fill), the channel is under significant pressure but not saturated. Switching to the bulk path here gives the system headroom to avoid ever reaching Level 3 (channel saturation, writers blocking on `send()`). If we wait until Level 3, writers are already blocking and the damage is done.

**How it works:**

1. Writer threads check a shared `AtomicU8` pressure level (updated by flush thread each cycle).
2. At Level 0-1: Normal path. Writers call `diff_document()`, generate MutationOps, send through channel.
3. At Level 2-3: Bulk path. Writers accumulate into per-thread `BulkAccumulator` buffers. When a buffer hits its threshold (50K docs), the writer submits the entire buffer to a separate `bulk_rx` channel that the flush thread drains with priority.

```rust
impl ConcurrentEngine {
    pub fn put(&self, id: u32, doc: &Document) -> Result<()> {
        let level = self.pressure_level.load(Ordering::Relaxed);
        if level >= 2 {
            self.put_bulk(id, doc)
        } else {
            self.put_normal(id, doc)
        }
    }
}
```

**The `BulkAccumulator` bypasses three costs:**
- No per-doc `diff_document()` call for fresh inserts (decomposes field values directly)
- No per-doc MutationOp allocation (~20 allocations saved per doc)
- No per-doc channel send (~20 sends saved per doc)

**Per-thread accumulators eliminate contention.** Each writer thread has its own `BulkAccumulator`. When full, it submits the buffer as a single `BulkBatch` message through a dedicated bounded channel (`bulk_tx` / `bulk_rx`). The flush thread drains `bulk_rx` with priority over the normal `MutationOp` channel.

**Flush thread bulk drain:**

```rust
// In flush loop, BEFORE normal channel drain:
while let Ok(bulk_batch) = bulk_rx.try_recv() {
    bulk_batch.flush_to_bitmaps(&mut staging.slots, &mut staging.filters, &mut staging.sorts);
    staging_dirty = true;
}
// Then normal MutationOp drain (for any Level 0-1 writes still in flight):
let bitmap_count = coalescer.prepare();
```

### 3.3 Persist Thread Integration (P1 + P3 Integration)

The persist thread decouples redb document writes from the flush hot path. It communicates with the flush thread through a small bounded channel of doc batch `Vec`s.

**Pipeline:**

```
Flush Thread                          Persist Thread
  drain + apply + publish               drain persist queue
  enqueue doc_batch via try_send  --->   docstore.put_batch(&batch)
  (non-blocking, ~1us)                  (blocking, 5-50ms)
```

**Pressure-aware doc batching:**

The flush thread accumulates documents from the `doc_rx` channel into a local `doc_batch` buffer. Under normal pressure (Level 0-1), it enqueues after every cycle. Under high pressure (Level 2-3), it accumulates larger batches before enqueuing, reducing the number of redb transactions:

```rust
let doc_enqueue_interval = match pressure.level {
    0 | 1 => 1,   // every cycle
    2 => 4,       // every 4th cycle
    _ => 16,      // every 16th cycle (Level 3)
};
if flush_cycle % doc_enqueue_interval == 0 && !doc_batch.is_empty() {
    let batch = std::mem::take(&mut doc_batch);
    let _ = persist_tx.try_send(batch); // non-blocking
}
```

**Backpressure handling:**

The persist channel is bounded at 4 batch slots. If `try_send` fails (queue full), the flush thread accumulates in an overflow buffer up to a hard limit (100K docs, ~50 MB). If the hard limit is reached, it blocks on `send()`. This is a safety valve, not a normal operating mode.

**Write-through doc buffer for upsert correctness:**

When the persist thread runs behind, the on-disk docstore is stale. Upserts that read the old doc for diffing would get a stale version. Solution: an `Arc<Mutex<HashMap<u32, StoredDoc>>>` write-through buffer shared between writer threads and the persist thread. Writers check this buffer before the docstore. The persist thread clears entries after successful `put_batch()`.

At peak, this buffer holds `persist_queue_depth * avg_batch_size` entries (~4K-40K docs, 2-20 MB). Negligible relative to 14.5 GB RSS.

### 3.4 Parallel Ingestion Pipeline (P4 Integration)

The ingestion pipeline feeds the combined write path from the read side.

**Architecture:**

1. **mmap + memchr**: Memory-map the NDJSON file. Compute N chunk boundaries by scanning for newlines at `file_size / N` offsets. Each parser thread gets an independent `&[u8]` slice.

2. **Parser threads (N = physical_cores - 3)**: Each thread scans its chunk with `memchr::memchr(b'\n', ...)` for SIMD-accelerated line detection. Parses each line with `serde_json::from_slice::<NdjsonRecord>()`, converts to `Document`, sends `(u32, Document)` through a bounded channel (16K capacity).

3. **Writer threads (M = 2-4)**: Receive `(u32, Document)` from the parser channel. Call `engine.put()`, which routes to the normal or bulk path based on pressure level.

4. **ID assignment**: In `--remap-ids` mode, each parser thread gets a pre-computed ID range based on estimated record count per chunk. Slight overallocation is benign (gaps hidden by alive bitmap). In Postgres ID mode, each record carries its own ID.

**Backpressure propagation:**

```
Parsers --> [bounded channel, 16K] --> Writers --> [MutationOp channel, 100K] --> Flush Thread
                                                   [BulkAccumulator, 50K/thread]
```

When the write path saturates, the writer-to-flush channel fills up. This triggers pressure escalation, which activates the bulk accumulator. If the bulk path's submit channel also fills, writers block on submit. This blocks their `recv()` from the parser channel, which causes parser threads to block on `send()`. Backpressure propagates end-to-end with no configuration.

### 3.5 VersionedBitmap Diff Management Across Layers

All layers write to the VersionedBitmap diff layer, not directly to base bitmaps:

| Layer | Write Target | Merge Frequency |
|-------|-------------|-----------------|
| Normal MutationOp path | Diff layer via `insert_bulk()` / `set_layer_bulk()` | Filter: periodic (50-200 cycles). Sort + alive: eager. |
| Bulk accumulator path | Diff layer via same methods | Same, but larger batches per call |
| Flush compaction | Merges diffs into bases | Pressure-driven cadence |
| De-escalation from Level 3 | Full diff merge | One-time on level transition |

**Diff size budget (from P1):** Every 20 flush cycles, scan filter fields and merge any VersionedBitmap whose diff `serialized_size()` exceeds the `merge_diff_threshold` (default 10K bytes). This prevents unbounded diff growth during sustained Level 1-2 operation. At Level 3, diffs accumulate unchecked but are bounded by the channel backpressure limiting input rate, and are fully merged on de-escalation.

**Sort diffs remain eagerly merged** (required by sort traversal's invariant). Alive diffs remain eagerly merged (required for correct `is_alive` checks). Only filter diffs benefit from deferred compaction.

---

## 4. Conflict Resolution

### 4.1 Loading Mode: Eliminate vs Keep (P1 vs P2)

**Conflict:** P2 proposes keeping `loading_mode` as the activation signal for the bulk accumulator. P1 proposes eliminating loading mode entirely, replacing it with automatic pressure levels.

**Resolution: Eliminate `loading_mode`. Use pressure levels as the sole signal.**

Rationale:
- Loading mode is a manual, binary switch that callers must remember to enter/exit. This is error-prone (the current `rebuild_from_docstore` has a stabilization polling loop after `exit_loading_mode()`).
- Pressure levels provide the same semantics automatically: Level 3 is functionally identical to loading mode (no publishing, no maintenance), and de-escalation from Level 3 provides the same force-publish/compact behavior as `exit_loading_mode()`.
- The bulk accumulator activates at Level 2+, which happens naturally during bulk loads when the channel fills up.

**Migration:**
- Remove `loading_mode: Arc<AtomicBool>` from `ConcurrentEngine`.
- Remove `enter_loading_mode()` / `exit_loading_mode()` public API.
- `rebuild_from_docstore()` no longer needs special loading/exit logic. Just call `put()` in a loop; pressure auto-throttles.
- Benchmark harness no longer needs loading mode calls.

**Safety net for callers who want to force bulk behavior from the start:**
Add a `hint_bulk_load()` method that artificially sets pressure to Level 2 for N seconds, giving the system a head start on activating the bulk path before the channel even fills. This is advisory, not mandatory.

### 4.2 Write-Through Doc Buffer: Needed? (P2 vs P3)

**Conflict:** P3 introduces a write-through doc buffer to handle stale docstore reads during async persist. P2's bulk accumulator bypasses the normal `put()` path for fresh inserts (no old doc read needed). Is the buffer still needed?

**Resolution: Yes, the write-through buffer is still needed, but only for upserts.**

During bulk loads, most documents are fresh inserts (slot not alive). The bulk accumulator handles these with `accumulate_insert()`, which never reads the docstore. No buffer needed.

However, some documents may be upserts (slot alive from a previous batch or from persistence restore). These require reading the old doc for diffing. If the persist thread is behind, the old doc may be stale on disk. The write-through buffer covers this case.

At Level 2+ (bulk mode active), upserts are rare (< 1% during initial load). The buffer stays small. At Level 0-1 (normal mode), the persist thread keeps up and the buffer is effectively empty. The buffer is a correctness safety net, not a performance-critical component.

### 4.3 Thread Count Unification

**Conflict:** The four proposals imply different thread models.

**Resolution: Unified thread model.**

| Thread(s) | Count | Source Proposal | Role |
|-----------|-------|----------------|------|
| Parser threads | N (default: cores - 3) | P4 | mmap + memchr + serde_json parsing |
| Writer threads | M (default: 2) | P4 | `engine.put()` or `engine.put_bulk()` |
| Flush thread | 1 | P1 (existing) | Drain + apply + publish, pressure controller |
| Persist thread | 1 | P3 (new) | Dedicated redb docstore writes |
| Merge thread | 1 | Existing | Periodic bitmap compaction to BitmapStore |

**Total: N + M + 3 threads.** On an 8-core system: 5 parsers + 2 writers + 3 background = 10 threads (OS schedules across 8 cores with time-slicing on the background threads, which are mostly idle). On a 16-core system: 11 parsers + 4 writers + 3 = 18 threads.

**For non-ingestion workloads (production query serving):** Parser and writer threads do not exist. Only the 3 background threads run. Writer calls to `put()` come from API handler threads (managed by the HTTP framework). Thread count is identical to today plus 1 (the persist thread).

---

## 5. Implementation Phases

Ordered by impact (highest throughput gain first). Each phase is independently testable and benchmarkable.

### Phase 1: Persist Thread Decoupling

**What:** Extract redb docstore writes from the flush loop into a dedicated persist thread.

**Changes:**
- New `PersistThread` struct owning `DocStore` and a bounded `crossbeam::Receiver<Vec<(u32, StoredDoc)>>`
- Flush loop sends doc batches via `try_send()` instead of calling `docstore.put_batch()` directly
- Shutdown sequence: signal persist thread after flush thread, drain remaining docs
- Write-through doc buffer (`Arc<Mutex<HashMap<u32, StoredDoc>>>`) for upsert correctness

**Files touched:** `src/concurrent_engine.rs` (flush loop refactor), new `src/persist_thread.rs`

**Why first:** This is the single largest bottleneck. Removing redb from the flush hot path cuts cycle time from 5-50ms to ~400us. Estimated standalone impact: **2-5x throughput improvement** (62K -> 120-300K docs/s).

### Phase 2: Pressure-Driven Adaptive Flush

**What:** Replace binary `loading_mode` with `PressureState` driving publish/compact/maintain cadence.

**Changes:**
- New `PressureState` struct with `update()`, `should_publish()`, `should_compact()`, `should_maintain_bounds()`, `should_invalidate_cache()`
- Replace `was_loading` / `staging_dirty` / `is_loading` logic with pressure-driven decisions
- Add `BackpressureConfig` to `Config` (thresholds, cadences)
- Remove `loading_mode`, `enter_loading_mode()`, `exit_loading_mode()`
- Add shared `AtomicU8` for writer-visible pressure level
- Diff size budget check every 20 cycles

**Files touched:** `src/concurrent_engine.rs`, `src/config.rs`, new `src/pressure.rs`

**Why second:** The persist thread (Phase 1) makes the flush loop fast enough that pressure-driven cadence becomes meaningful. Without Phase 1, the flush loop is redb-bound regardless of pressure level.

### Phase 3: Bulk Accumulator Path

**What:** Per-thread `BulkAccumulator` that bypasses MutationOp channel at pressure Level 2+.

**Changes:**
- New `BulkAccumulator` struct with `accumulate_insert()`, `accumulate_upsert()`, `flush_to_bitmaps()`
- `PerThreadAccumulator` wrapper with threshold-based submit
- Dedicated `bulk_tx` / `bulk_rx` channel (bounded, 8 slots)
- Flush thread drains `bulk_rx` with priority before normal channel
- `put()` routes to bulk path when `pressure_level >= 2`

**Files touched:** `src/concurrent_engine.rs`, new `src/bulk_accumulator.rs`

**Why third:** Requires Phase 2 (pressure levels) for activation. The bulk path eliminates per-doc MutationOp overhead, which becomes the bottleneck after Phase 1 removes redb. Estimated additional impact: **1.5-3x** on top of Phases 1-2.

### Phase 4: Parallel Ingestion Pipeline

**What:** mmap + memchr + parallel serde_json parsing in the benchmark harness.

**Changes:**
- Add `memmap2`, `memchr` to `Cargo.toml`
- New `parallel_stream_records()` function: mmap file, compute chunk boundaries, spawn parser threads
- Parser threads: memchr line scan + `serde_json::from_slice` + `to_document()` + send to bounded channel
- Writer threads: recv from channel + `engine.put()`
- ID range pre-computation for `--remap-ids` mode
- Replace current single-threaded ingestion in `src/bin/benchmark.rs`

**Files touched:** `src/bin/benchmark.rs`, new `src/ingestion.rs`, `Cargo.toml`

**Why fourth:** This unblocks the write path from being parsing-gated. Without Phases 1-3, parsing throughput just hits the 62K/s write ceiling. With Phases 1-3 raising the write ceiling to ~300K+/s, parallel parsing ensures the pipeline stays saturated.

### Phase 5: Polish and Optimization (Optional)

**What:** simd-json drop-in, binary cache (rkyv), allocation reduction.

**Changes:**
- Try `simd-json` as drop-in replacement for `serde_json` (benchmark delta)
- Pre-size HashMap in `to_document()` (trivial)
- Binary `.bitdex-cache` format for repeated benchmark loads (rkyv)

**Why last:** Marginal gains. Only worthwhile if Phase 4 shows parsing is still the bottleneck after all write path improvements. The binary cache is a quality-of-life feature for development benchmarking, not production.

---

## 6. Test Plan

### Phase 1: Persist Thread Decoupling

**Correctness tests:**
- Unit: persist thread drains batches and writes to docstore correctly
- Unit: write-through buffer returns latest version for recently-written slots
- Unit: upsert reads write-through buffer before docstore
- Integration: shutdown drains all remaining docs to disk
- Integration: crash simulation (kill between publish and persist) followed by `rebuild_from_docstore` produces consistent state
- Property test: concurrent puts + reads never see partial documents

**Benchmark (1M records, `images-full.ndjson` subset):**
- Metric: insert throughput (docs/s)
- Expected: 120-200K docs/s (up from 62K)
- Verify: query correctness on 20 query types after insert
- Verify: docstore contains all 1M documents after drain

### Phase 2: Pressure-Driven Adaptive Flush

**Correctness tests:**
- Unit: `PressureState::update()` escalation/de-escalation with hysteresis
- Unit: `should_publish()` returns correct cadence at each level
- Unit: de-escalation from Level 3 triggers force-publish
- Integration: bulk insert (1M records) naturally escalates to Level 2-3, de-escalates after completion
- Integration: queries during Level 2 see data that is at most 16 cycles stale
- Integration: `rebuild_from_docstore` works without `enter_loading_mode()` (pressure auto-activates)
- Regression: no `loading_mode` references in codebase after removal

**Benchmark (1M records):**
- Metric: insert throughput at each pressure level (log level transitions)
- Expected: comparable to Phase 1 (pressure auto-throttle should not degrade throughput)
- Verify: pressure reaches Level 2-3 during sustained insert, returns to Level 0 after
- Verify: diff sizes stay bounded (no bloat during sustained Level 2)

### Phase 3: Bulk Accumulator Path

**Correctness tests:**
- Unit: `BulkAccumulator::accumulate_insert()` correctly decomposes filter values and sort bit layers
- Unit: `flush_to_bitmaps()` produces identical bitmap state as equivalent MutationOp path
- Unit: upsert path (`accumulate_upsert`) correctly computes diffs
- Property test: for any sequence of documents, bulk path and normal path produce identical filter/sort bitmaps
- Integration: 1M insert at Level 2+, all 20 query types return correct results
- Integration: mixed insert+upsert workload at Level 2+ (verify diff correctness)

**Benchmark (1M records):**
- Metric: insert throughput (docs/s)
- Expected: 200-350K docs/s (bulk path eliminates per-doc channel overhead)
- Verify: accumulator memory stays under 25 MB per thread
- Verify: channel depth stays below Level 3 (bulk path provides enough headroom)

### Phase 4: Parallel Ingestion Pipeline

**Correctness tests:**
- Unit: chunk boundary computation produces valid line-aligned boundaries
- Unit: parser thread correctly handles edge cases (empty lines, last chunk without trailing newline, very long lines)
- Unit: ID range pre-computation covers all records with no gaps or overlaps
- Integration: parallel parse of 1M records produces identical document set as sequential parse
- Integration: full 104M record ingestion produces correct query results on all 20 types

**Benchmark (1M records, then 5M, then full 104M):**
- 1M: Expected 300K+ docs/s end-to-end
- 5M: Expected 300K+ docs/s sustained
- 104M: Expected 5-7 minute total wall time
- Metric: per-thread utilization (parser % parsing vs blocked, writer % put vs blocked, flush thread utilization)
- Metric: channel fullness at steady state (measure backpressure balance)
- Metric: peak RSS (should not exceed current 14.5 GB + pipeline overhead ~100 MB)

### Phase 5: Polish

**Benchmark only:**
- simd-json vs serde_json: measure MB/s per parser thread
- Binary cache: measure subsequent-load time vs NDJSON first-load time
- Allocation reduction: measure per-record processing time delta

---

## 7. Risk Analysis

### 7.1 High Risk: Pressure Oscillation at Threshold Boundaries

**What could go wrong:** If write rate sits exactly at a threshold boundary (e.g., 50% fill), pressure oscillates between Level 1 and Level 2 every few cycles. This causes the bulk accumulator to repeatedly activate and deactivate, potentially losing partially-filled buffers or creating inconsistent write paths.

**Mitigation:** De-escalation hysteresis (200us minimum at current level before dropping). Partial accumulator buffers are force-flushed on de-escalation, same as the current loading mode exit. Additionally, the bulk accumulator's threshold (50K docs) means it rarely has partial data to flush — most of the time it either submits full buffers or is empty.

**Fallback:** If oscillation proves problematic in benchmarks, add a "sticky" mode: once bulk path activates, require 10+ cycles below Level 1 before deactivating. This biases toward staying in bulk mode during sustained writes.

### 7.2 High Risk: Write-Through Buffer Memory Under Pathological Upsert Workloads

**What could go wrong:** If a workload is 100% upserts (every put is an update to an existing doc) and the persist thread falls behind, the write-through buffer grows to hold every recently-written document. At 300K docs/s with a 4-batch persist queue and 50ms redb commits, the buffer could hold ~60K docs (~30 MB). This is manageable, but pathological scenarios (redb compaction stall, 500ms commit) could push it to ~150K docs (~75 MB).

**Mitigation:** Hard cap on write-through buffer size (100K entries). When exceeded, block writers until persist thread drains. This is identical to P3's overflow buffer strategy.

**Fallback:** If upsert-heavy workloads cause persistent buffer growth, switch to epoch-based clearing: persist thread reports highest-persisted sequence number, flush thread discards all entries below that epoch. This is more complex but bounded.

### 7.3 Medium Risk: Bulk Accumulator Correctness for Upserts

**What could go wrong:** The bulk accumulator's `accumulate_upsert()` path must produce identical bitmap mutations as the normal `diff_document()` path. Any divergence causes silent data corruption (bitmap says a slot has a tag it doesn't, or vice versa).

**Mitigation:** Property-based testing. For any random sequence of documents (including upserts), assert that the bulk path and normal path produce bit-identical filter and sort bitmaps. Run this as a `proptest` with 10K+ iterations.

**Hardest part:** Ensuring the bulk accumulator handles multi-value field diffs correctly (e.g., tagIds changing from [1,2,3] to [2,3,4] must clear bit 1, set bit 4, leave 2 and 3 unchanged). The normal path does this via `diff_document()` which compares old and new field values. The bulk path for upserts must replicate this logic exactly.

### 7.4 Medium Risk: mmap on Windows

**What could go wrong:** The project runs on Windows (per environment). Memory-mapped I/O on Windows has different semantics: files cannot be deleted or truncated while mapped, and page faults may behave differently than Linux for very large files (59 GB exceeding physical RAM).

**Mitigation:** `memmap2` abstracts platform differences and is widely used on Windows. For the 59 GB file, Windows will page intelligently (same as Linux). The benchmark harness never deletes the source file while running.

**Fallback:** If mmap proves problematic on Windows, fall back to P4's Option B (reader thread with 64 MB buffer distributing line batches to parser threads via channel). This is only ~10% slower than mmap for sequential reads.

### 7.5 Low Risk: Persist Thread as New Bottleneck

**What could go wrong:** After decoupling persist from flush, the persist thread may fall permanently behind at 300K docs/s. Each doc is ~500 bytes serialized; at 300K/s that's ~150 MB/s of redb writes. NVMe can handle this, but redb's write serialization and compaction may not keep up.

**Mitigation:** Monitor `persist_queue_depth` metric. If it stays at max, options are: (a) skip docstore writes during bulk load (rebuild from source on restart), (b) switch to a faster KV store (fjall, sled), (c) batch larger (fewer transactions).

**Fallback:** During initial load, the docstore is optional (we have the source NDJSON). Add a `--skip-docstore` flag for benchmark-only runs. For production, the Postgres WAL is the source of truth; the docstore is a performance optimization for upsert diffing, not a durability requirement.

### 7.6 Low Risk: Removing Loading Mode Breaks External Callers

**What could go wrong:** External code that calls `enter_loading_mode()` / `exit_loading_mode()` breaks.

**Mitigation:** These methods are only called in two places: `rebuild_from_docstore()` (internal) and `src/bin/benchmark.rs` (internal). No external callers exist yet (V2 has no published API). Clean removal with grep verification.

---

## Appendix A: Expected Throughput at Each Phase

| Phase | Bottleneck Removed | Est. Throughput | Wall Time (104M) |
|-------|-------------------|----------------|-------------------|
| Current | (all) | 62K docs/s | ~28 min |
| Phase 1 (persist thread) | redb blocking flush | 120-200K docs/s | ~9-15 min |
| Phase 2 (pressure) | binary loading mode | ~same as P1 | ~same as P1 |
| Phase 3 (bulk accumulator) | per-doc MutationOp overhead | 200-350K docs/s | ~5-9 min |
| Phase 4 (parallel ingestion) | single-threaded parsing | **300K+ docs/s** | **5-7 min** |

Phase 2 alone does not improve throughput significantly; its value is correctness (smooth transitions) and enabling Phase 3 (bulk accumulator needs pressure levels for activation).

## Appendix B: Memory Budget

| Component | Steady State | Peak (104M bulk load) |
|-----------|-------------|----------------------|
| Bitmap memory | 6.5 GB | 6.5 GB |
| Trie cache | ~160 MB | ~160 MB |
| Bound cache | ~2 KB | ~2 KB |
| Docstore (on-disk, redb) | ~6 GB disk | ~6 GB disk |
| **New: Persist queue** | ~0 | ~20 MB (4 batches) |
| **New: Write-through buffer** | ~0 | ~20 MB (worst case) |
| **New: Bulk accumulator (per thread)** | ~0 | ~24 MB x M threads |
| **New: Parser-to-writer channel** | ~0 | ~13 MB (16K entries) |
| **New: mmap virtual** | 0 | 59 GB virtual, ~2 GB resident (OS-managed) |
| **Total additional RSS** | ~0 | **~100-150 MB** |

The additional memory is negligible relative to the existing 14.5 GB RSS.

## Appendix C: Configuration

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Channel fill ratio thresholds for pressure levels [warm, hot, critical].
    /// Default: [0.10, 0.50, 0.80]
    pub pressure_thresholds: [f64; 3],

    /// Publish cadence per pressure level [idle, warm, hot].
    /// Level 3 (critical) never publishes. Values are flush cycle counts.
    /// Default: [1, 4, 16]
    pub publish_cadence: [u64; 3],

    /// Compaction cadence per pressure level [idle, warm, hot].
    /// Level 3 compacts only on de-escalation.
    /// Default: [50, 100, 200]
    pub compact_cadence: [u64; 3],

    /// Maximum diff serialized bytes before forced compaction (per bitmap).
    /// Default: 10000
    pub diff_budget: usize,

    /// Flush threshold for per-thread bulk accumulators (docs per thread).
    /// Default: 50000
    pub bulk_flush_threshold: usize,

    /// Persist thread queue depth (number of doc batch slots).
    /// Default: 4
    pub persist_queue_depth: usize,

    /// Write-through buffer hard cap (entries).
    /// Default: 100000
    pub write_through_cap: usize,
}
```
