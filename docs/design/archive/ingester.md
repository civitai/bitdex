# Ingester: Unified Tuple Ingestion Core

> Every data path — HTTP put, NDJSON loader, CSV loader, outbox sync — reduces to the same operation: take `(slot, field, value)` tuples and route them to two sinks: bitmap mutations and docstore appends. The Ingester extracts this shared core so all paths use the same code. Two implementations: LiveIngester for real-time puts (bitmap ops via coalescer channel) and BulkIngester for loading (bitmap ops via thread-local BitmapAccum).

## The Primitive

```rust
fn ingest(&mut self, slot: u32, field_idx: u16, value: &[u8]) {
    self.bitmap_sink.emit(slot, field_idx, value);  // bitmap mutation
    self.doc_sink.append(slot, field_idx, value);    // V2 shard append
}
```

The doc sink is always `append_tuple` — identical for live and bulk. Only the bitmap routing differs.

## LiveIngester (HTTP puts, outbox sync)

Bitmap ops go to the coalescer channel as `MutationOp`s. The flush thread batches and applies them to staging. Diff still needed for bitmaps (compute which bits to flip based on old vs new doc). Doc write is pure append — no diff needed, LIFO handles dedup.

```
HTTP request → json_to_fields() → diff_document() → MutationOps → coalescer
                                 → append_tuple() per field → V2 shard
```

**Change from V1:** Replace `put_batch()` (read-decompress-merge-compress-write) with `append_tuple()` per field. Eliminates the docstore write bottleneck on the put path.

## BulkIngester (CSV loader, NDJSON loader)

Bitmap ops accumulate in thread-local `BitmapAccum` via rayon fold+reduce. No diff needed (fresh insert, all fields are new). Doc write is append — same as live.

```
CSV row → parse_fields() → bitmap_accum.insert(slot, field, value)
                          → append_tuple() per field → V2 shard
```

## Assumptions to Validate

**I1: V2 append latency on put hot path.** Measure single-doc upsert with V2 append vs V1 read-modify-write under concurrent HTTP load (100+ upserts/sec). Expected: 10-100x faster per upsert since no shard read/decompress.

```
Bench I1: 8 threads, 1000 upserts each, V1 vs V2 doc write
  Measured: V2 p50=103 us, p99=865 us, 41,241 ops/s concurrent
           V1 p50=22,214 us, p99=1,049,078 us, 54 ops/s concurrent
  Result:  V2 is 215x faster at p50, 764x higher throughput
```

**I4: Diff-then-append split.** Verify that diff_document can run for bitmaps only while the doc path does pure append. The diff reads the old doc from disk — with V2, that read is a tuple scan (4 us). Check that the diff output doesn't depend on the doc write format.

## Implementation Status: PARTIAL

Steps 1-3 are done. Steps 4-6 are deferred — the trait exists but isn't wired into production code yet.

### Done (src/ingester.rs)

1. `BitmapSink` trait with `filter_insert/remove`, `sort_set/clear`, `alive_insert/remove`, `flush`
2. `CoalescerSink` — buffers `MutationOp`s and sends to coalescer channel
3. `AccumSink` — inserts directly into `BitmapAccum` (bulk loading path)
4. `DocSink` — wraps `Arc<Mutex<DocStore>>` for V2 tuple appends
5. `Ingester<B: BitmapSink>` — generic struct holding bitmap sink + optional doc sink
6. 5 tests: RecordingSink, bitmap-only, AccumSink, DocSink, full pipeline

### Not yet wired (deferred)

- `put()` in concurrent_engine.rs still uses manual `diff_document()` → `sender.send_batch()` → `doc_tx.send()`
- `put_bulk_into()` still uses manual thread-local HashMaps
- PG loader and NDJSON loader still use old paths
- No duplicate code has been deleted yet

### Why deferred

The `put()` path is the riskiest refactor — `diff_document()` returns `Vec<MutationOp>` rather than calling `BitmapSink` methods directly. To wire it, either refactor `diff_document` to accept a `&mut BitmapSink` (large change, touches mutation.rs), or iterate the Vec and dispatch to sink methods (same as today, just indirected). Neither is hard, but both touch the core write path and need careful testing.

The trait adds the most value when we have more ingestion paths (outbox sync, webhook, streaming pipeline). With just `put` and `put_bulk` as callers today, the abstraction is ready but not urgent.

### When to revisit

Wire the ingester when:
- Adding a new ingestion source (outbox sync, streaming)
- Refactoring the PG loader to use V2 docstore directly
- Unifying the doc write path (replace `doc_tx` channel with `DocSink.append_tuple`)
