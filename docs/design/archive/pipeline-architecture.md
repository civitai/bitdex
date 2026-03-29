# Pipeline Architecture: Connected Pools with Backpressure

> The system is a chain of processing stages connected by bounded channels. Workers sit at the top of the pipe and get pulled to wherever backpressure is highest. Bottlenecks are math — measure each stage's throughput, identify the constraint, shift workers.

## The Pipeline

```
Input Sources          Ingestion Core         Bitmap Engine         Storage
─────────────         ──────────────         ─────────────         ───────
HTTP put     ─┐
NDJSON file  ─┤→ [Parse Pool] →→ [Ingest Pool] →→ [Coalescer] →→ [Flush Thread]
CSV file     ─┤                       │                                │
Outbox poll  ─┘                       │                                │
                                      ↓                                ↓
                              [V2 DocStore]                    [BitmapFs + BoundStore]
                              (append tuples)                  (bitmap snapshots)
                                      │                                │
                                      ↓                                ↓
                               [Janitor Pool]                  [Merge Thread]
                              (compact shards)                 (compact bitmaps)
```

## Stages

| Stage | Work | Throughput | Bottleneck when |
|-------|------|-----------|----------------|
| Parse | Deserialize input (JSON, CSV, msgpack) → typed fields | ~300K rows/s per thread | I/O-bound on large files |
| Ingest | Decompose fields → bitmap ops + docstore tuples | ~500K tuples/s per thread | CPU-bound on bitmap insert |
| Coalescer | Batch bitmap ops, apply to staging | 1 thread, ~1M ops/s | Backs up under write storms |
| Flush | Publish staging snapshot via ArcSwap | 1 thread, ~10 publishes/s | Trivial — snapshot swap is ns |
| V2 DocStore | Append tuples to shard files | ~512 MB/s aggregate | Disk I/O saturation |
| Janitor | Compact dirty shards, clean tombstones | ~240 shards/s per thread | Background, never on critical path |
| Merge | Compact VersionedBitmap diff layers | 1 thread | Background |

## Backpressure

Each channel between stages has a bounded capacity. When a channel fills, the upstream stage blocks. This naturally shifts work to wherever the bottleneck is:

- **Parse blocks** when ingest channel full → workers shift to ingestion
- **Ingest blocks** when coalescer channel full → workers shift to bitmap apply
- **DocStore append** never blocks (always-appendable, per-shard mutex <1% contention)

## Worker Elasticity (future)

Current: fixed thread pools per stage. Future: unified worker pool where workers morph based on backpressure.

```
Worker loop:
  1. Check write channel (highest priority — clear backlog)
  2. Check ingest channel
  3. Check parse channel
  4. If all empty, sleep or steal
```

Early in a load, most workers parse (no ingest backlog yet). As ingest produces work, workers shift to bitmap building. Late in a load, workers shift to compaction cleanup. The system finds equilibrium.

## Assumption to Validate

**P5: Worker pool elasticity vs fixed pools.** Benchmark unified pool (workers morph) against fixed pools (dedicated threads per stage). Measure: throughput, latency variance, idle time. The dynamic pipeline benchmark (28 threads, 107M records) showed 55% of time in docstore writes with fixed pools — V2 docstore eliminates this bottleneck, potentially making fixed pools adequate.

## Measured Stage Costs (from session benchmarks)

| Operation | Latency | Source |
|-----------|---------|--------|
| CSV row parse (tag) | 70 ns | 14.4M rows/s scatter rate |
| Bitmap insert (HashMap) | 300 ns | 3.3M inserts/s with rayon |
| V2 tuple append | 103 us/op (3 tuples) | I1 bench, 41K ops/s at 8 threads |
| V1 shard read-modify-write | 22 ms p50 | I1 bench, 54 ops/s at 8 threads |
| V2 LIFO doc read | 4 us | in-memory, single doc |
| V2 concurrent doc read (8T) | 21 us/doc | NVMe, 100 docs spread |
| Shard compaction | 10-18 ms | J2 bench, I/O-dominated |
| Bitmap snapshot save | ~10 s | BitmapFs at 107M |
