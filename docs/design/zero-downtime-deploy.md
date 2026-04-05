---
status: PROPOSED
created: 2026-04-04
author: Fredrick (design) + Justin (direction, multi-node insight)
---

# Zero-Downtime Rolling Deploys

> File-lock writer election on a shared PVC. New pod mmaps shared silo files for
> instant startup, serves reads immediately, promotes to read-write when the old
> pod exits. ~200 lines of Rust. No external coordination.

---

## Problem

BitDex runs as a single replica. Every deploy has a downtime window while the new
pod loads data (22+ seconds for lazy bitmap loading at 107M, longer for a full
dump restore). The API layer falls back to Postgres during this window, but PG
queries are slower and miss BitDex-specific sort behavior.

## Solution: Shared-PVC Rolling Deploy

Two pods briefly coexist during a rolling update, sharing the same data directory
via a single PVC. The V3 mmap architecture makes this natural — both pods mmap
the same silo files, and the Linux kernel shares the physical pages. No data
duplication, no double memory footprint.

A POSIX file lock (`flock`) on the shared PVC elects the single writer. No K8s
API calls, no external coordination service, no network dependencies.

### Scope

This design covers **same-node rolling deploys only** — two pods on the same
node sharing a ReadWriteOnce PVC. It does not cover multi-node HA (see
[Multi-Node Model](#multi-node-model-future) below for that direction).

---

## Architecture

### Startup Mode Selection

On startup, the binary attempts an exclusive file lock:

```rust
enum ServerMode { ReadWrite, ReadOnly }

fn acquire_writer_lock(data_dir: &Path) -> io::Result<(ServerMode, File)> {
    let f = File::create(data_dir.join("writer.lock"))?;
    match f.try_lock_exclusive() {
        Ok(()) => Ok((ServerMode::ReadWrite, f)),
        Err(_) => Ok((ServerMode::ReadOnly, f)),
    }
}
```

**ReadWrite mode** (lock acquired): Full operation — mutation thread, ops polling,
compaction, time bucket refresh. This is today's behavior.

**ReadOnly mode** (lock held by another pod): Serve queries from mmap'd silo
files. No mutation thread, no ops polling, no compaction. A background thread
retries the lock every second for promotion.

### Read-Only Serving

The read-only pod:

1. **mmaps all silo files** — index table, data shards, bitmap shards. Pages are
   already hot in the kernel page cache from the writer pod. Startup is
   sub-second (no loading, no deserialization).

2. **Tails the ops log** — the shared PVC contains the ops log file. The
   read-only pod watches it (inotify or poll) and replays new entries into its
   own in-memory ops HashMap. Staleness window: milliseconds.

3. **Serves queries normally** — mmap reads + in-memory ops snapshot, same as
   the writer pod's read path. Callers cannot distinguish which pod served them.

What it does NOT run:
- Mutation thread (no silo writes)
- Ops poller (no PG connection for BitdexOps)
- Compaction
- Time bucket refresh
- Cache persistence writes

### Writer Promotion

A background thread retries the lock:

```rust
fn lock_watcher(lock_file: &File, promote_tx: Sender<()>) {
    loop {
        thread::sleep(Duration::from_secs(1));
        if lock_file.try_lock_exclusive().is_ok() {
            let _ = promote_tx.send(());
            return;
        }
    }
}
```

On promotion:
1. Start mutation thread (drain ops channel, write to silos)
2. Start ops poller (connect to PG, poll BitdexOps)
3. Start compaction scheduler
4. Log: `"Promoted to read-write mode"`

### Sync Sidecar Behavior

The `bitdex-sync` sidecar runs in both pods. It does not need to know about
writer election — the engine's HTTP endpoints handle it:

- **Read-only mode:** Write endpoints (`POST /ops`, `PUT /dumps`) return **503
  Service Unavailable**. The sidecar's existing retry/backoff logic handles this
  naturally — it sees "my local engine isn't accepting writes" and retries.

- **After promotion:** Write endpoints start returning 200. The sidecar resumes
  normal operation automatically.

```
Read-only pod:
  bitdex-sync: POST /ops → 503 → backoff, retry 1s
  bitdex-sync: POST /ops → 503 → backoff, retry 1s
  ...pod promotes to read-write...
  bitdex-sync: POST /ops → 200 → normal polling resumes
```

No sidecar code changes. No lock-file awareness. No new endpoints. The sidecar
only cares that its local engine eventually accepts writes — it doesn't need to
know why it was waiting.

**Cursor safety:** The sidecar reads its cursor from the engine on startup
(`GET /cursors/{name}`). In read-only mode this endpoint works fine (cursors are
on the shared PVC). The sidecar knows exactly where to resume once writes are
accepted.

### Graceful Shutdown

On SIGTERM (K8s pod termination):
1. Stop accepting new HTTP requests
2. Drain in-flight requests (respect `terminationGracePeriodSeconds`)
3. Stop mutation thread, flush pending ops
4. Close file handles (kernel releases flock automatically on exit)

---

## Rolling Deploy Sequence

```
t=0   Pod A (v1.0.X):   read-write, holds writer.lock
      K8s creates Pod B (v1.0.X+1)

t=1s  Pod B:             flock fails → starts read-only
      Pod B:             mmaps shared silos (pages already hot) → sub-second
      Pod B:             tails ops log → catches up

t=2s  Pod B:             readiness probe passes
      K8s:               shifts traffic to include Pod B
      K8s:               sends SIGTERM to Pod A

t=3s  Pod A:             drains in-flight, stops mutation thread, exits
      Kernel:            releases writer.lock

t=4s  Pod B:             lock watcher acquires writer.lock
      Pod B:             promotes to read-write (starts mutation thread + ops poller)

      Zero downtime. Both pods existed for ~3 seconds.
```

---

## K8s Configuration

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bitdex
spec:
  replicas: 1
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1        # allow 2 pods during rollout
      maxUnavailable: 0   # never drop below 1 ready pod
  template:
    spec:
      terminationGracePeriodSeconds: 30
      affinity:
        podAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: bitdex
              topologyKey: kubernetes.io/hostname  # same node
      containers:
        - name: bitdex
          volumeMounts:
            - name: data
              mountPath: /data
          readinessProbe:
            httpGet:
              path: /health
              port: 3000
            initialDelaySeconds: 1
            periodSeconds: 1
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: bitdex-data
```

### PVC

No changes needed. ReadWriteOnce (RWO) allows multiple pods on the same node.
The `podAffinity` rule ensures both pods land on the same node during rollout.

---

## What This Does NOT Solve

**Node failure.** If the node dies, both pods die. This is acceptable per design
principle #9 (single process, single node). The API layer's Postgres fallback
handles the gap during node recovery.

**Long-running dual serving.** This is designed for the brief rollout overlap
window (seconds), not permanent multi-replica serving. The single-writer model
remains.

---

## Multi-Node Model (Future)

If BitDex ever needs pods on different nodes (true HA, not just zero-downtime
deploys), the shared-PVC approach does not apply. mmap over network storage
(NFS, CephFS) has fundamentally different performance characteristics — page
faults become network round trips, and the nanosecond read path that makes this
whole design work becomes millisecond latency.

The multi-node model is fully independent instances:

```
Node A                          Node B
┌──────────────┐               ┌──────────────┐
│ BitDex Pod   │               │ BitDex Pod   │
│ Own PVC      │               │ Own PVC      │
│ Own silos    │               │ Own silos    │
│ Own mutation │               │ Own mutation │
│   thread     │               │   thread     │
└──────┬───────┘               └──────┬───────┘
       │                              │
       └──────── Both poll ───────────┘
                BitdexOps table
```

Each instance:
- Has its own PVC and data directory
- Independently polls the BitdexOps table from Postgres
- Independently runs its own dump pipeline on startup
- Independently applies ops and runs compaction
- Is a fully self-contained BitDex server

No coordination needed between instances. They converge to the same state because
they consume the same ops stream from the same source of truth (Postgres). Minor
transient divergence (one pod applies an op milliseconds before the other) is
acceptable for the query workload.

This is the simpler model conceptually — just N independent copies — but it costs
N times the storage and memory. The shared-PVC approach exists specifically to
avoid that cost when both pods are on the same node anyway.

---

## Implementation Estimate

| Component | Lines | Notes |
|-----------|-------|-------|
| `ServerMode` enum + lock acquisition | ~30 | Startup path |
| Read-only serving mode | ~80 | Skip mutation thread, ops poller, compaction |
| Ops log tailing (read-only freshness) | ~60 | inotify/poll + replay into HashMap |
| Lock watcher + promotion | ~30 | Background thread, channel signal |
| Graceful shutdown handler | ~20 | SIGTERM drain (may already exist) |
| **Total** | **~220** | |

K8s config: add `podAffinity` stanza + adjust `strategy` in the deployment
manifest. ~15 lines of YAML.

---

## Dependencies

- **V3 mmap architecture** — this design assumes silo files are mmap'd. With V2's
  in-memory `Arc<RoaringBitmap>` model, a read-only pod would need to deserialize
  all bitmaps independently (defeating the shared-pages benefit).
- **Ops log on shared PVC** — the read-only pod tails this for freshness. Already
  the case in V3's design.
- **`/health` endpoint** — needs to return ready once mmaps are established,
  before writer promotion. May need a small tweak if current health check
  requires the mutation thread to be running.
