# Janitor: Reader-Triggered Compaction — DONE

> Readers are the janitors. When `get_v2()` scans a shard and sees too many stale tuples, it hands the buffer to a background worker for compaction. No polling, no dirty tracker, no scanning. The system only cleans what it reads, and it only reads what matters.

## Core Insight

The LIFO read path already knows exactly how dirty a shard is. During the reverse scan, it counts total tuples and unique tuples. The difference is stale data. If stale exceeds a threshold, the reader fires off a compaction request. The compaction worker already has the file contents in memory (the reader hands over the buffer) so it skips the disk read entirely.

This eliminates:
- Dirty bit protocol (no header writes on the upsert path)
- In-memory dirty tracker (no Vec, no BitVec, no atomic counters)
- Persistence file (nothing to save/load on shutdown/boot)
- Polling loop (no interval, no scan, no sleep)
- Boot-time scan (nothing to scan)

## How It Works

### Read Path (zero added latency)

```rust
fn get_v2(&self, slot_id: u32) -> Result<Option<StoredDoc>> {
    let data = fs::read(&shard_path)?;
    let (doc, total_tuples, unique_tuples) = lifo_scan(&data, slot_id);

    // If stale tuples exceed threshold, request compaction
    let stale = total_tuples - unique_tuples;
    if stale > 0 && stale * 100 / total_tuples > COMPACT_THRESHOLD_PCT {
        let _ = self.compact_tx.try_send((shard_id, data));  // non-blocking
    }

    Ok(doc)
}
```

The `try_send` is non-blocking. If the channel is full (worker is busy), the request is silently dropped. The next reader to hit this shard will re-detect the dirtiness and try again. Cost to the reader: ~10 ns for the try_send plus ownership transfer of the Vec (pointer move, zero copy).

### Compaction Worker (single background thread)

```rust
fn compaction_worker(compact_rx: Receiver<(u32, Vec<u8>)>, docstore: Arc<DocStore>) {
    while let Ok((shard_id, data)) = compact_rx.recv() {
        compact_shard_from_buffer(&docstore, shard_id, &data);
    }
}
```

The worker receives the shard buffer that the reader already loaded. It runs the zero-copy compaction (forward scan for offsets, reverse dedup, write winners directly from the source buffer). No second disk read. 1-2 ms per shard.

### Dedup: Multiple Readers, Same Shard

If two readers hit the same dirty shard simultaneously, both may try to send a compaction request. This is fine:

1. The bounded channel (capacity ~32) naturally deduplicates by backpressure. If the channel is full, `try_send` drops the request silently.
2. If both get through, the second compaction is a no-op. `compact_shard` exits early when `winners == total` (no stale tuples — the first compaction already cleaned it).

No locking, no coordination, no "compaction in progress" flags needed.

## Compaction: Zero-Copy Single Pass

The compaction algorithm (already implemented in `compact_shard()`):

1. Forward scan source buffer to build offset index: `Vec<(slot, field_idx, byte_offset, tuple_len)>`. No data copying — just byte positions.
2. Reverse iterate offsets for LIFO dedup via HashSet. Collect winning indices.
3. Write winners directly from source buffer to new file via `&data[start..start+len]`.
4. Correct `num_tuples` in header from the start (no second file open).
5. Atomic rename over the original. No fsync (crash-safe via rename).
6. Early exit if nothing to compact (`winners == total`).

### Measured Performance (Benchmark J2)

| Dirty % | Read p50 (dirty) | Read p50 (clean) | Degradation | Compact time |
|---------|------------------|-------------------|-------------|-------------|
| 10% | 187 us | 159 us | 1.18x | 1.1 ms |
| 30% | 151 us | 122 us | 1.24x | 1.2 ms |
| 50% | 222 us | 139 us | 1.60x | 2.2 ms |
| 70% | 278 us | 172 us | 1.62x | 1.3 ms |
| 90% | 647 us | 113 us | 5.73x | 1.4 ms |

Compaction is 1-2 ms regardless of dirtiness (I/O-dominated, not CPU). Read degradation is mild until 50%+ dirty.

## Threshold

Configurable, default 30% (`COMPACT_THRESHOLD_PCT = 30`). At 30% dirty, reads cost only 1.24x clean. Below that, compaction isn't worth the I/O. Above that, the reader triggers cleanup automatically.

The threshold is computed from data the reader already has: `stale * 100 / total > 30`. No external state needed.

## Lifecycle

### Startup

Nothing to do. No dirty tracker to load, no scan to run. The first read to each dirty shard will discover and trigger compaction naturally. Self-healing.

### Shutdown

Drain the compaction channel (process any pending requests). The worker thread exits when the sender is dropped. No state to persist.

### Loading Mode

During bulk inserts (`enter_loading_mode()`), the docstore is write-only — no reads happen. The compaction channel stays open but receives no requests. After `exit_loading_mode()`, the first queries naturally discover any dirty shards from the load. No special pause/resume logic needed.

### Crash Recovery

If the process crashes mid-compaction, the atomic rename ensures the old file is intact. The .tmp file is orphaned and can be cleaned up on next startup (or ignored — it wastes a few KB of disk). No dirty state is lost because there is no dirty state — readers rediscover dirtiness on every read.

## Bound Cache Cleanup

BoundStore tombstone cleanup remains a separate concern. Tombstoned entries accumulate in `meta.bin` and shard files on disk. A periodic sweep (e.g., on the compaction worker thread during idle periods, or on a timer) should:

1. Scan bound cache meta.bin for tombstoned entries
2. Delete tombstoned shard files from disk
3. Rewrite meta.bin without tombstoned entries

This can share the compaction worker thread — when the compaction channel is empty, the worker runs a bound cache cleanup pass. Or it can be a separate low-priority timer. Either way, it's independent of the docstore compaction design.

## Implementation Checklist

1. Add `compact_tx: Option<crossbeam::Sender<(u32, Vec<u8>)>>` to DocStore
2. Modify `get_v2()` to count total vs unique tuples during LIFO scan
3. Add threshold check and `try_send` after scan
4. Add `compact_shard_from_buffer(shard_id, data)` that takes a pre-read buffer
5. Spawn compaction worker thread in ConcurrentEngine (alongside flush/merge)
6. Worker drains channel, calls `compact_shard_from_buffer` for each
7. On shutdown: drop sender, join worker thread

## What This Design Gives Up

**Shards that are dirty but never read won't be compacted.** They waste disk space. This is acceptable because:
- If nobody reads a shard, nobody cares about its read latency
- Disk space is cheap; NVMe bandwidth for reads is the bottleneck
- The shard will be compacted the moment anyone reads it

**No prioritization.** Shards are compacted in the order readers discover them, not dirtiest-first. In practice, the hottest shards (most reads) get compacted first, which is the right priority anyway — clean the things people use most.
