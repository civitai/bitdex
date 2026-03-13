# Backpressure Auto-Throttle — Implementation Design

## Problem Statement

We need 300K docs/s sustained insert throughput. Current e2e: ~33K docs/s.

We built a BulkAccumulator (545K docs/s in isolation) and a persist thread, but end-to-end throughput barely improved because the **per-doc `put()` overhead dominates**: mutex lock, snapshot check, doc clone, channel send — all per document. The components are fast but the pipeline between them is the bottleneck.

The correct solution was stated from the beginning: **auto-adjusting backpressure where `put()` just builds a backlog and the flush thread adapts based on pressure.**

## What We Have (Assets to Keep)

- **BulkAccumulator** (`src/bulk_accumulator.rs`): accumulate() + flush() + apply(). 545K docs/s in isolation. 8 tests, equivalence verified against diff_document path.
- **Persist thread** (in `src/concurrent_engine.rs`): Dedicated thread for redb writes with write-through doc buffer. Working, 3 tests.
- **Microbenchmarks** (`src/bin/write_microbench.rs`): 8 benchmarks covering individual components.

## What's Wrong (Pipeline Design)

Current `put()` for fresh inserts:
```
put(id, doc)
  → mark_in_flight(id)              // atomic
  → snapshot().is_alive(id)          // ArcSwap load + bitmap check
  → snapshot().was_ever_allocated(id) // ArcSwap load + bitmap check
  → bulk_accum.lock()               // MUTEX LOCK per doc
  → accumulate(id, doc)             // HashMap lookups + Vec pushes
  → bulk_accum.unlock()
  → StoredDoc { fields: doc.fields.clone() }  // HEAP ALLOC per doc
  → doc_tx.try_send()               // channel send per doc
  → clear_in_flight(id)             // atomic
```

Per-call cost: ~30μs. At 30μs/doc, ceiling is ~33K docs/s. The BulkAccumulator's 545K throughput is irrelevant because we never reach it — we're bottlenecked on the put() overhead.

## Correct Architecture

### Principle: `put()` is a fast append. Flush thread does all the real work.

```
put(id, doc)
  → channel.send((id, doc))  // THAT'S IT. ~100ns.
```

The flush thread drains the channel and auto-adjusts behavior based on backlog depth:

| Channel Depth | Behavior | Publish | Batch Strategy |
|--------------|----------|---------|----------------|
| < 1,000 (steady) | Per-op diff + apply | Every cycle | Small batches |
| 1,000 - 50,000 | Accumulator mode | Every 10th cycle | Batched bitmap build |
| > 50,000 (burst) | Full bulk mode | Every 100th cycle | Large bulk bitmap build |

### Key Design Points

1. **Single input channel**: `put()` sends `(u32, Document)` pairs into a bounded channel. The Document itself (not a clone) goes in. The channel IS the backlog.

2. **Flush thread reads pressure**: `channel.len()` tells it how much work is pending. This drives all behavior.

3. **Steady state (< 1K pending)**: Flush thread drains mutations, calls diff_document(), applies via WriteBatch, publishes every cycle. Same as today's per-op path. Low latency, readers see fresh data.

4. **Under pressure (1K - 50K pending)**: Flush thread drains a large batch, routes fresh inserts through BulkAccumulator, upserts through diff_document(). Publishes every Nth cycle. Larger redb transactions.

5. **Burst mode (> 50K pending)**: Flush thread drains everything, all fresh inserts go through BulkAccumulator, publishes rarely, maximum bitmap building efficiency. Nobody cares about query freshness during a backfill.

6. **No separate alive check in put()**: The flush thread knows if a slot is alive because it owns the staging engine. Fresh vs upsert routing happens in the flush thread, not the caller.

7. **Doc persistence**: The flush thread sends doc batches to the persist thread. Under pressure, it sends larger batches less frequently. The persist thread writes to redb independently.

8. **No loading mode**: The backpressure gradient replaces loading mode entirely. A 104M record backfill naturally puts the channel at max depth → burst mode. Steady-state traffic keeps it low.

### Backpressure Config

```yaml
backpressure:
  low_watermark: 1000      # below this, steady state
  high_watermark: 50000    # above this, full burst mode
  publish_interval_steady: 1      # publish every cycle
  publish_interval_burst: 100     # publish every 100th cycle
  redb_batch_steady: 1000         # docs per redb transaction
  redb_batch_burst: 50000         # docs per redb transaction under load
  merge_interval_steady: 5000ms   # merge thread compaction interval
  merge_interval_burst: 2000ms    # MORE frequent under load (prevent diff bloat)
```

### Channel Type Change

Current: Two separate channels
- `Sender<MutationOp>` for bitmap mutations (after diff_document in put())
- `Sender<(u32, StoredDoc)>` for doc persistence

New: Single channel
- `Sender<(u32, Document)>` carrying the raw document + slot ID
- Flush thread does ALL processing: alive check, fresh vs upsert routing, diff or accumulate, bitmap apply, doc persistence routing

This moves ALL CPU work off the writer threads onto the flush thread. Writers become pure producers with ~100ns send time.

### Interaction with VersionedBitmap Diffs

Under backpressure, diffs accumulate between publishes. This is fine:
- Readers apply diffs against small result sets, not full bitmaps
- Merge thread merges more frequently under pressure (not less) to prevent diff bloat
- During backfill, diffs essentially become the bases since building from scratch

### What This Means for put()

```rust
pub fn put(&self, id: u32, doc: &Document) -> Result<()> {
    self.input_tx.send((id, doc.clone())).map_err(|_| {
        BitdexError::CapacityExceeded("input channel disconnected".to_string())
    })?;
    Ok(())
}
```

That's it. No in_flight tracking (flush thread handles it). No snapshot check (flush thread owns staging). No accumulator lock (flush thread owns it). No doc_tx (flush thread routes to persist).

### Expected Throughput

- `put()`: ~100ns/call → **10M docs/s** producer ceiling
- BulkAccumulator (flush thread): 545K docs/s in isolation
- Persist thread: 328K docs/s with batching
- **System ceiling: ~500K docs/s** (flush thread limited)
- With multiple flush workers: potentially higher

## Microbenchmark Gaps

We benched components in isolation but missed:
1. **Flush loop cycle cost**: Per-cycle overhead of locking caches, invalidating, publishing. How much time does the "bookkeeping" take vs actual bitmap work?
2. **put() per-call overhead**: Mutex lock + snapshot load + doc clone + channel send. We never measured this.
3. **Channel throughput with Document payloads**: We measured MutationOp channel throughput, but Documents are much larger (HashMap with 12 fields including Vecs).
4. **Accumulator under realistic flush patterns**: We measured accumulator at 100K docs in one shot, but never at the batch sizes the flush thread actually processes (2-3 docs per cycle).
5. **Snapshot publish frequency impact**: How much does reducing publish frequency help throughput? What's the cost of skipping publishes?

## Action Items

1. Build missing microbenchmarks (gaps above)
2. Redesign put() → flush pipeline per this doc
3. Implement pressure-driven flush with auto-adjusting behavior
4. Remove loading mode
5. Benchmark at 1M and 5M
