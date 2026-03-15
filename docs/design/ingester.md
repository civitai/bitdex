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

## Implementation

1. Define `BitmapSink` trait with `emit(slot, field_idx, value)` — two impls: `CoalescerSink` (sends MutationOps) and `AccumSink` (inserts into BitmapAccum)
2. Define `DocSink` with `append(slot, field_idx, value)` — single impl using `append_tuple`
3. `Ingester<B: BitmapSink>` struct holds both sinks
4. Refactor `put()` in concurrent_engine.rs to use `Ingester<CoalescerSink>`
5. Refactor loader to use `Ingester<AccumSink>`
6. Delete duplicate decomposition code from loader.rs and scatter_gather.rs
