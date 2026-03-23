# Snapshot & Benchmark Replay System

> Capture production traffic and database state at a point in time, then replay it locally to reproduce and fix performance problems. The goal is a reproducible benchmark: capture once, replay many times, immediately see if a code change helps.

**Status**: PROPOSED
**Source**: Voice memo 2026-03-23, plan doc `docs/bitdex-snapshot-system-plan.md`
**Depends on**: Unified ShardStore + generation model (see `docs/design/docstore-v3-oplog.md`)

---

## Problem

Production showed inconsistent performance:
- Average latency very low when cache hit correctly — handled massive bursts.
- Periodic **6-second stalls** where the server appeared dead.
- Request pile-up filled the thread queue, causing total stall cascades.
- Worse with 2 pods than 1 (possible memory/thread contention on shared node).

We need to capture the exact traffic + database state from a production window so we can replay it locally, measure, change code, and immediately see if things improve.

---

## How It Works (Generation Model)

The snapshot system is built on top of the ShardStore generation model. A capture creates two generations that bracket the recording window.

```
Time ──────────────────────────────────────────────►

     │◄── Gen N (pre-capture state) ──►│◄── Gen N+1 (writes during capture) ──►│◄── Gen N+2 ──►
     │                                  │                                       │
  capture start                    capture stop                            normal ops resume
  (gen switch after                (gen switch after
   next flush cycle)                next flush cycle)
```

**On capture start**: Flag flush thread → after next flush completes, bump generation counter. Gen N becomes the frozen pre-capture state. All new writes go to Gen N+1. Traffic recording begins.

**During capture**: Reads unaffected (in-memory snapshot via ArcSwap). Writes append ops to Gen N+1 shard files. Traffic log records every request + timing + response.

**On capture stop**: Flag flush thread → after next flush completes, bump generation again. Gen N+1 is now frozen (contains all writes during capture). Traffic recording stops. Prometheus metrics scraped.

**Result**: Two frozen generations (Gen N = starting state, Gen N+1 = mutations during capture) plus a traffic log and metrics.

---

## Snapshot Package

After capture, the system produces a self-contained package:

```
snapshots/{session_id}/
  gen_n/                        ← pre-capture state (ShardStore generation dir)
    docstore/                   ← doc shard files (hex-bucketed, snapshot + ops format)
    bitmaps/
      filters/                  ← filter bitmap shard files (field-bucketed .fpack)
      sorts/                    ← sort layer files (.sort per field)
      alive.roar                ← alive bitmap
    schema.bin                  ← field dictionary, types, defaults
    meta.json                   ← slot counter, generation ID, field configs
  gen_n1/                       ← writes during capture (same structure)
    docstore/                   ← only shards that received writes
    bitmaps/                    ← only bitmaps that changed
  traffic.caplog                ← request/response log (msgpack tuples)
  metrics_start.prom            ← Prometheus scrape at capture start
  metrics_stop.prom             ← Prometheus scrape at capture stop
  manifest.json                 ← session metadata, timestamps, record counts
```

### Capture Is Zero-Cost

Capture itself does no compaction, no compression, no heavy I/O. It's just two generation switches (start and stop) — atomic counter bumps that happen after the next flush cycle. The captured generations sit on disk in their normal shard file format alongside the traffic log and Prometheus scrapes.

This means you can capture multiple snapshots without paying any cost beyond the generation switches. Only when you *request a package* does the heavy work happen.

### Packaging (On Demand)

`POST /debug/snapshot/{session_id}/package` creates a background task that:

1. **Compacts Gen N** — flattens all prior generations' ops into Gen N snapshots so it represents the complete state at capture start. Old pre-N generations can be deleted after. Gen N+1 stays as-is (just ops from the capture window).
2. **Streams tar.zst** — walks the actual BitDex data directory in-place (no copy to staging dir) and streams shard files + traffic log + metrics into a compressed archive. Written to `snapshots/{session_id}.snapshot.tar.zst`.
3. **Reports status** — `GET /debug/snapshot/{session_id}/status` returns progress (`compacting`, `compressing`, `ready`, `failed`).

**What the package includes:**
- **Gen N**: Full compacted state — entire docstore (~60GB) + all bitmaps (~7GB). Starting point for replay.
- **Gen N+1**: Ops that happened during capture (~10MB). Both the writes to replay and comparison data.
- **traffic.caplog**: Request/response log.
- **metrics_start.prom / metrics_stop.prom**: Prometheus snapshots.
- **manifest.json**: Session metadata.

### Download

`GET /debug/snapshot/{session_id}/download` streams the packaged `.snapshot.tar.zst` directly from the server. This avoids needing to SSH into the K8s pod or set up a kubectl port-forward to extract files.

The download is a streaming HTTP response — the server reads from the tar.zst file on disk and pipes it to the client. No buffering the full file in memory. Cost is just disk read I/O on the NVMe (~60GB at ~2GB/s = ~30 seconds of sequential read). Query handling is unaffected since axum/tokio async I/O means the download is just another event loop task.

### Production Workflow

For the current state (before performance tuning):

```
1. Enable shadow mode              → BitDex receives real traffic
2. POST /debug/capture/start       → zero-cost gen switch, recording begins
3. Wait N minutes
4. POST /debug/capture/stop        → another gen switch, recording stops
5. Disable shadow mode             → no more traffic pressure
6. POST /debug/snapshot/.../package → compaction + compression (server is idle)
7. GET /debug/snapshot/.../download → stream the package
```

Steps 6-7 happen with the server idle (shadow mode off), so compaction and I/O don't compete with query serving. Once performance is tuned, these could run live — compaction and streaming are background I/O that shouldn't interfere with in-memory query serving (queries hit ArcSwap snapshots, not disk).

**Architecture boundary**: Snapshot *capture* and *packaging* live in the BitDex server binary (production features). Snapshot *replay* is a separate test harness binary (`bitdex-replay`) — it doesn't ship to production. The replay harness emits its own metrics and comparison reports.

---

## Traffic Capture

### Trigger

- `POST /debug/capture/start` with `{ "duration_seconds": 300 }` — records for N seconds, then auto-stops.
- `POST /debug/capture/stop` — manual stop.
- `GET /debug/capture/status` — current capture state and stats.

### What Gets Recorded

| Field | Why |
|-------|-----|
| `arrived_at` (ns timestamp) | Replay timing |
| `method` + `path` + `query_string` | Reconstruct the request |
| `body` (if POST/PATCH) | Full request payload |
| `response_status` | Comparison baseline |
| `response_body` (configurable) | Result comparison |
| `responded_at` (ns timestamp) | Latency baseline |
| `trace_id` (if present) | Link to Tempo traces |

### Format

Append-only binary log (msgpack tuples, same pattern as ShardStore ops). One file per session: `traffic.caplog`. Compressed with zstd *after* capture completes.

### Size Estimate

~500 req/s × 5 min = 150K requests × ~2KB avg = ~300MB uncompressed, ~60MB compressed.

---

## Local Replay

### Loading a Snapshot

```bash
# Start BitDex pointing at the snapshot
bitdex-server --snapshot snapshots/{session_id}.snapshot.tar.zst --port 3001

# What happens:
# 1. Decompress to .snapshot-data/{session_id}/
# 2. Load Gen N shard files into ShardStore (same startup path as production)
# 3. Apply Gen N+1 ops on top (reach capture-start state)
# 4. Server ready for replay
```

The decompressed data stays at `.snapshot-data/{session_id}/` so subsequent replays skip decompression. `--snapshot` with an already-decompressed directory skips straight to load.

### Running a Replay

```bash
# Replay traffic against the loaded snapshot
bitdex-replay --url http://localhost:3001 \
  --caplog .snapshot-data/{session_id}/traffic.caplog \
  --mode realtime \
  --output replay_results/
```

### Replay Modes

| Mode | Description | Use case |
|------|-------------|----------|
| `realtime` | Replay at original timestamps | Most realistic — reproduces concurrency patterns |
| `saturate` | Fire all requests ASAP | Stress test, find throughput ceiling |
| `sequential` | One request at a time | Isolate per-query performance |
| `stepped` | Pause between requests | Attach profiler, collect flamegraphs |

### Development Loop

The intended workflow for iterating on performance:

```
1. Start server from snapshot:  bitdex-server --snapshot {file}.tar.zst --port 3001
2. Clear trace state:           curl -X POST localhost:3001/debug/traces/clear
3. Run replay:                  bitdex-replay --url localhost:3001 --caplog ... --mode realtime
4. Review results:              replay_results/{run_id}/summary.json  (p50/p95/p99 comparison)
5. Make code change
6. Repeat from 1
```

Each iteration is: load snapshot → replay → compare → change code → repeat. The replay harness handles the comparison output. Ollie should validate this workflow early in implementation to make sure it covers the diagnostic needs.

### Comparison Output

```
replay_results/{run_id}/
  summary.json                  ← p50/p95/p99, original vs replay, delta %
  per_request.csv               ← request_id, original_ms, replay_ms, status_match, result_match
  gen_comparison/               ← optional: Gen N+1 diff (see below)
  flamegraph.svg                ← optional, if --profile flag used
```

### Generation Comparison (Correctness Check)

After replay, the local engine has its own Gen N+1 (writes that happened during replay). This can be compared against the captured Gen N+1:

- **Ops comparison**: Do the same shard ops appear in both? If not, a write was lost or duplicated.
- **Snapshot comparison**: After compacting both, are the shard snapshots identical? If not, there's a state divergence.
- **Hot-swap check**: Query the local engine at its Gen N+1 state and compare results against the captured Gen N+1 state. Differences indicate correctness regressions.

This is powerful — it's not just timing comparison, it's full state verification.

### Local Capture (For Development)

The snapshot system also runs locally for dev benchmarking:

```bash
# Start server with capture enabled
bitdex-server --port 3001 --data-dir ./data

# Start capture (records traffic + metrics, no shard data needed since it's local)
curl -X POST localhost:3001/debug/capture/start -d '{"duration_seconds": 60}'

# ... run your workload ...

# Stop capture
curl -X POST localhost:3001/debug/capture/stop

# Package metrics-only (shard data is already local)
curl -X POST localhost:3001/debug/snapshot/{session_id}/package?mode=metrics_only
```

The `metrics_only` package gives you the traffic log + Prometheus metrics for comparison without duplicating the shard data you already have. Useful for A/B testing code changes locally.

---

## Implementation Order

With the ShardStore generation model as the foundation, the snapshot system is significantly simplified:

| Phase | What | Effort | Depends on |
|-------|------|--------|------------|
| S1 | Traffic capture middleware + caplog format | Small | Nothing |
| S2 | Generation pin (flag flush thread, bump gen counter) | Small | ShardStore generation support |
| S3 | Prometheus metrics scrape at start/stop | Small | S2 |
| S4 | Snapshot packaging (compact + tar.zst) | Medium | S2 |
| S5 | `--snapshot` server startup mode (decompress + load) | Medium | S4 |
| S6 | Replay harness (load caplog, schedule, fire, compare) | Medium | S1, S5 |
| S7 | Generation comparison (ops diff, state verification) | Medium | S5, S6 |
| S8 | Local capture mode (metrics_only) | Small | S1, S3 |

**Critical path**: ShardStore generation support → S2 → S4 → S5 → S6. Traffic capture (S1) is independent and can start immediately.

**Parallel tracks**:
- **Ollie**: S1 (traffic capture) + S6 (replay harness) — can develop against current engine
- **Adam**: ShardStore + S2 (gen pin) + S4 (packaging) + S5 (snapshot load)
- **Aiden**: S3 (Prometheus scrape) + S8 (local capture) + production metrics additions

---

## Testing Requirements

### E2E Tests

- Capture start/stop lifecycle (clean start, duration-based auto-stop, manual stop)
- Traffic log completeness (every request during window appears in caplog)
- Generation boundary correctness:
  - Writes in-flight at capture start land in correct generation
  - Flush thread mid-cycle when flag set completes cleanly
  - Multiple rapid start/stop → generation counter monotonic
- Snapshot packaging round-trip (capture → package → decompress → load → query)
- Replay produces identical results to captured responses (correctness)
- Replay timing comparison output is sane (p50/p95/p99 present, deltas calculated)

### Microbenchmarks (Ollie)

- Generation switch overhead (flush thread flag check + counter bump)
- Snapshot packaging throughput (compaction + tar.zst at 105M)
- Replay scheduling accuracy (request fired within 1ms of original timing)

---

## Resolved Design Questions

All open questions from the initial design have been resolved:

1. **Bitmap snapshot during capture** — With ops-log architecture, gen switch is just "flush pending ops, bump gen counter." Milliseconds, not seconds.
2. **Gen 0 size** — Full state at 105M is ~60GB. `metrics_only` mode avoids transfer for local captures.
3. **DocStore during capture** — Reads go through in-memory snapshot. Gen split is disk-only.
4. **Pod identity** — Target single pod. Capture endpoint is per-pod.
5. **Prometheus metrics** — Scraped at start and stop, included in snapshot package.
6. **Gen N+1 as comparison data** — Keep both gens in the package. Gen N+1 ops can be compared against local replay's Gen N+1 for correctness verification.
