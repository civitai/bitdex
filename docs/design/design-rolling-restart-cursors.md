---
status: IMPLEMENTED (Phases 1-3), APPROVED (Phase 4 — K8s deployment)
created: 2026-03-10
updated: 2026-03-13
---

# Design: Rolling Restarts via Named Cursors

## Problem

BitDex currently runs as a single instance with a single pg-sync sidecar. The sidecar polls the `BitdexOutbox` table, pushes mutations to BitDex over HTTP, and deletes outbox rows after successful delivery. This means:

- No redundancy — if BitDex restarts, queries fail until it's back
- No way to do zero-downtime deployments
- Outbox rows are deleted after one consumer processes them, so a second replica can't catch up

We need rolling restarts with 2+ replicas so there's always at least one pod serving traffic.

## Design Goals

1. Zero-downtime rolling restarts for BitDex
2. Each replica is fully self-contained (no inter-replica coordination)
3. BitDex remains a general-purpose engine — no opinions about what cursors mean
4. Outbox stays naturally bounded without manual cleanup
5. Minimal code changes

## Architecture

```
                ┌──────────────────────┐
                │      Postgres        │
                │  ┌────────────────┐  │
                │  │ BitdexOutbox   │  │  ← triggers on Image, Tags, etc.
                │  └───────┬────────┘  │
                │  ┌───────┴────────┐  │
                │  │ bitdex_cursors │  │  ← updated by sidecars, trigger cleans outbox
                │  └────────────────┘  │
                └───────┬──────┬───────┘
                        │      │
           ┌────────────┘      └────────────┐
    ┌──────┴───────────┐           ┌────────┴─────────┐
    │  Pod: bitdex-0   │           │  Pod: bitdex-1   │
    │                  │           │                  │
    │  ┌────────────┐  │           │  ┌────────────┐  │
    │  │  pg-sync   │──┼───read────┤  │  pg-sync   │  │
    │  │ (sidecar)  │  │  cursor   │  │ (sidecar)  │  │
    │  └─────┬──────┘  │           │  └─────┬──────┘  │
    │        │ HTTP     │           │        │ HTTP     │
    │  ┌─────┴──────┐  │           │  ┌─────┴──────┐  │
    │  │   BitDex   │  │           │  │   BitDex   │  │
    │  │  (engine)  │  │           │  │  (engine)  │  │
    │  └─────┬──────┘  │           │  └─────┬──────┘  │
    │        │          │           │        │          │
    │  [PVC: data/]     │           │  [PVC: data/]     │
    │   bitmaps/        │           │   bitmaps/        │
    │   docs/           │           │   docs/           │
    │   cursors/        │           │   cursors/        │
    └──────────────────┘           └──────────────────┘
```

Each pod is independent. No pod knows about any other pod. The only shared state is Postgres.

## Named Cursors — BitDex Side

### Concept

A cursor is an opaque named string that BitDex persists to disk alongside its data. BitDex doesn't know what a cursor represents — it just stores it. The caller chooses the name and value.

This keeps BitDex general-purpose. A pg-sync sidecar might use `cursor: { name: "pg-sync-0", value: "48291537" }` (an outbox ID). Someone else's pipeline might use `cursor: { name: "kafka-ingest", value: "partition-3:offset-9281" }`. BitDex doesn't care.

### API Changes

**Upsert — accepts optional cursor**

```
POST /api/indexes/{name}/documents/upsert

{
  "documents": [...],
  "cursor": {                    ← optional
    "name": "pg-sync-0",
    "value": "48291537"
  }
}
```

**Delete — accepts optional cursor**

```
DELETE /api/indexes/{name}/documents

{
  "ids": [123, 456],
  "cursor": {                    ← optional
    "name": "pg-sync-0",
    "value": "48291538"
  }
}
```

**Read cursor**

```
GET /api/indexes/{name}/cursors/{cursor_name}

→ { "name": "pg-sync-0", "value": "48291537" }
```

**List cursors**

```
GET /api/indexes/{name}/cursors

→ [{ "name": "pg-sync-0", "value": "48291537" }]
```

### Storage

Cursors are stored as plain text files in the data directory:

```
data/civitai/cursors/pg-sync-0     ← contains "48291537"
data/civitai/cursors/pg-sync-1     ← contains "48291201"
```

### Persistence Semantics

Cursors are held in memory (a `HashMap<String, String>`) and written to disk **by the merge thread at checkpoint time**, alongside bitmap and docstore snapshots. This is critical:

- The cursor on disk always reflects what's actually persisted
- If BitDex crashes between receiving a mutation and the next checkpoint, the cursor hasn't advanced
- On restart, the sidecar reads the cursor via the GET endpoint and replays from that point
- Replay is safe because upsert is idempotent — applying the same document twice is a no-op

The merge thread already runs every 5 seconds (configurable). Adding cursor file writes is trivial — it's a single small file write per cursor.

### Concurrency

- Mutations arrive via HTTP → write coalescer channel → flush thread applies to staging → merge thread checkpoints to disk
- The cursor value travels the same path: HTTP request stores it in a pending slot, flush thread picks it up, merge thread writes it
- Between the HTTP response and the checkpoint, the cursor is in memory but not yet on disk. If BitDex crashes in that window, the sidecar replays those mutations. This is correct — the data wasn't persisted either.

### Implementation in concurrent_engine.rs

The cursor flows through the existing mutation pipeline:

1. **HTTP handler** receives upsert/delete with optional cursor
2. If cursor present, sends a `MutationOp::CursorSet { name, value }` through the crossbeam channel (same channel as filter/sort mutations)
3. **Flush thread** picks it up during `coalescer.prepare()`, stores latest value per cursor name in staging
4. **Merge thread** at checkpoint: writes each cursor to `data/{index}/cursors/{name}`

On startup, `ConcurrentEngine` loads all cursor files from the cursors directory into the in-memory map.

## Sidecar Changes — pg-sync Side

### Boot Sequence

1. Sidecar starts, waits for BitDex to be healthy (`GET /health`)
2. Reads its cursor: `GET /api/indexes/civitai/cursors/pg-sync-0`
   - If found: resume from that value
   - If not found (fresh instance): start from 0 (full catch-up from outbox, or trigger bulk load)
3. Begins polling

### Poll Loop (outbox_poller.rs)

Current flow:
```
SELECT ... FROM BitdexOutbox ORDER BY id DESC LIMIT batch
→ dedupe → enrich → push to BitDex
→ DELETE FROM BitdexOutbox WHERE id <= max_processed_id
```

New flow:
```
SELECT ... FROM BitdexOutbox WHERE id > $cursor ORDER BY id ASC LIMIT batch
→ dedupe → enrich → push to BitDex with cursor: { name, value: max_id }
→ report cursor to Postgres
```

Key changes:
- **Cursor-based polling**: `WHERE id > $cursor ORDER BY id ASC` instead of `ORDER BY id DESC` + delete
- **Cursor included in mutations**: every upsert/delete call includes the cursor so BitDex persists it
- **No outbox deletion by sidecar**: the sidecar never deletes outbox rows directly

### Cursor Reporting to Postgres

After each successful push to BitDex, the sidecar also updates Postgres:

```sql
INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (replica_id)
DO UPDATE SET last_outbox_id = $2, updated_at = now();
```

This serves two purposes:
1. Enables automatic outbox cleanup via trigger
2. Provides visibility into replica state

Note: the sidecar reports the cursor it *sent* to BitDex, not what's on disk yet. This is fine — the PG cursor is for cleanup and observability. The authoritative cursor for replay is always the one on disk in BitDex.

### Dedupe Fix

Current: "DELETE wins over UPSERT" within a batch. This is wrong — it should be **highest outbox ID wins per entity**. If the outbox has `id=100 DELETE entity=5` then `id=101 UPSERT entity=5`, the UPSERT is newer and should win.

New rule: for each entity_id in a batch, keep only the event with the highest outbox id.

## Automatic Outbox Cleanup — Postgres Side

### Schema

```sql
CREATE TABLE bitdex_cursors (
    replica_id TEXT PRIMARY KEY,
    last_outbox_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Trigger

```sql
CREATE OR REPLACE FUNCTION cleanup_bitdex_outbox() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM "BitdexOutbox"
    WHERE id < (SELECT MIN(last_outbox_id) FROM bitdex_cursors);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cleanup_bitdex_outbox
    AFTER INSERT OR UPDATE ON bitdex_cursors
    FOR EACH ROW
    EXECUTE FUNCTION cleanup_bitdex_outbox();
```

Every time a sidecar reports its cursor, the trigger fires and deletes outbox rows that all replicas have consumed. The outbox naturally stays bounded to the gap between the slowest replica and the newest event.

### BIGINT Exhaustion

Not a concern. `BIGSERIAL` uses `BIGINT` (2^63 = 9,223,372,036,854,775,807). At 1,000 events/second sustained, exhaustion would take 292 billion years.

### Decommissioned Replicas

If a replica is permanently removed, its cursor row in `bitdex_cursors` will block cleanup (it holds the MIN). Operator must delete the row:

```sql
DELETE FROM bitdex_cursors WHERE replica_id = 'pg-sync-bitdex-1';
```

The `replica_id` is derived from the StatefulSet pod name (e.g., `bitdex-0`, `bitdex-1`), prefixed with `pg-sync-`. StatefulSet guarantees stable, persistent pod names across restarts, updates, and rollbacks — `bitdex-0` is always `bitdex-0`. This makes pod names a reliable replica identifier without needing a separate config element.

This is an explicit operational action, not an automatic timeout. Automatic timeouts risk deleting rows that a slow-but-alive replica still needs.

For observability, alert if any replica's `updated_at` is older than a threshold (e.g., 10 minutes), which indicates something is wrong.

## Kubernetes Deployment

### StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: bitdex
spec:
  replicas: 2
  serviceName: bitdex
  podManagementPolicy: Parallel
  template:
    spec:
      containers:
        - name: bitdex
          image: ghcr.io/civitai/bitdex:latest
          ports:
            - containerPort: 3000
          livenessProbe:
            httpGet:
              path: /health
              port: 3000
          readinessProbe:
            httpGet:
              path: /ready        # lag-aware, see below
              port: 3000
          volumeMounts:
            - name: data
              mountPath: /data
        - name: pg-sync
          image: ghcr.io/civitai/bitdex-pg-sync:latest
          env:
            - name: BITDEX_REPLICA_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name   # "bitdex-0", "bitdex-1"
            - name: BITDEX_URL
              value: "http://localhost:3000"
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 50Gi
```

### PodDisruptionBudget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: bitdex
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: bitdex
```

### Readiness Probe

The `/ready` endpoint must be lag-aware. A restarted pod that loaded its snapshot but hasn't caught up with the outbox should not receive traffic.

BitDex exposes:
- `GET /health` → 200 if process is alive (liveness)
- `GET /ready` → 200 only if the engine is loaded AND the most recently applied cursor is within an acceptable lag of the outbox head

The sidecar can set a "caught up" flag (e.g., via a local file or a BitDex admin endpoint) once it observes that its polling returned zero new rows. BitDex's `/ready` checks this flag.

Simpler alternative: `/ready` returns 200 once BitDex has successfully processed at least one batch from the sidecar after startup. This avoids needing to know the outbox head.

### Rolling Restart Flow

1. K8s begins rolling update, respects PDB (maxUnavailable: 1)
2. `bitdex-1` receives SIGTERM → graceful shutdown (stop accepting requests, flush pending mutations, checkpoint to disk)
3. Traffic routes only to `bitdex-0` (still ready)
4. New `bitdex-1` starts, PVC still attached with latest checkpoint
5. BitDex loads snapshot from disk (instant — lazy bitmap loading)
6. pg-sync sidecar starts, reads cursor from `GET /cursors/pg-sync-bitdex-1` → resumes
7. Sidecar catches up (small delta — just mutations since last checkpoint)
8. `/ready` returns 200 → K8s routes traffic to `bitdex-1`
9. K8s proceeds to restart `bitdex-0`, same process

### First Load (Fresh Pod, No PVC Data)

The first load has a critical ordering requirement: the cursor must be seeded **before** the bulk load starts, so that any mutations occurring during the (potentially long) bulk load are captured in the outbox.

1. `pg-sync load` runs setup (creates outbox table + triggers + cursor table)
2. Snapshots current outbox head: `SELECT MAX(id) FROM "BitdexOutbox"` → e.g. 50,000,000
3. Registers cursor in engine AND in PG: `cursor = 50000000`
4. Enters loading mode, runs bulk load from PG (no sidecar polling during this)
5. Bulk load completes, exits loading mode, checkpoints (cursor persisted to disk with bitmap snapshot)
6. `pg-sync sync` starts — sidecar reads cursor from BitDex (50M), starts polling from there
7. Catches up on mutations that occurred during bulk load (outbox rows 50M+)
8. Readiness probe passes → traffic routed to pod

The sidecar does NOT poll during bulk load. The `load` and `sync` commands are sequential — either as separate init container + sidecar, or as a startup script that runs `load` then `sync`.

## Edge Cases

| Scenario | Behavior |
|---|---|
| **Normal rolling restart** | Pod loads from PVC, sidecar catches up from cursor, ready in seconds |
| **PVC lost** | Cursor file gone → sidecar starts from 0 → replays full outbox. If outbox has been cleaned past 0, needs bulk load first |
| **Sidecar crashes, BitDex stays up** | K8s restarts sidecar container. Reads cursor from BitDex, resumes. BitDex serves stale but doesn't go down |
| **BitDex crashes, sidecar stays up** | Health gate detects BitDex is down, pauses PG polling (no wasted work). When BitDex restarts from checkpoint, health returns 200, sidecar resumes from its in-memory cursor. Cursor on disk is authoritative |
| **Both pods restart** | PDB prevents this in normal operation. If it happens (node failure), both catch up independently from their PVC cursors |
| **Replica decommissioned** | Operator deletes cursor row from `bitdex_cursors`. Cleanup trigger resumes |
| **Outbox grows during long restart** | Bounded by write volume. At typical rates, minutes of outbox accumulation is negligible. Alert if `bitdex_cursors.updated_at` is stale |
| **Sidecar pushes batch, BitDex crashes before checkpoint** | Cursor on disk hasn't advanced. Sidecar re-reads old cursor on BitDex restart, replays batch. Idempotent — no duplicates |

## Observability

### Prometheus Metrics (BitDex)

- `bitdex_cursor_value{cursor="pg-sync-0"}` — current in-memory cursor value (gauge)
- `bitdex_cursor_disk_value{cursor="pg-sync-0"}` — last checkpointed cursor value (gauge)

### Prometheus Metrics (Sidecar)

- `bitdex_sidecar_outbox_head` — latest outbox ID seen in poll (gauge)
- `bitdex_sidecar_cursor_value` — current cursor value (gauge)
- `bitdex_sidecar_lag` — `outbox_head - cursor_value` (gauge)
- `bitdex_sidecar_poll_batch_size` — rows fetched per poll (histogram)
- `bitdex_sidecar_push_duration_seconds` — time to push batch to BitDex (histogram)

### Grafana Alerts

- Sidecar lag > 10,000 for 5 minutes → something is stuck
- `bitdex_cursors.updated_at` older than 10 minutes → replica may be dead
- Inter-replica cursor drift > threshold → one replica falling behind

### Postgres Visibility

```sql
-- See all replica states
SELECT * FROM bitdex_cursors;

-- Current outbox depth (how far behind the slowest replica is)
SELECT MAX(o.id) - MIN(c.last_outbox_id) AS outbox_depth
FROM "BitdexOutbox" o, bitdex_cursors c;
```

## Implementation Plan

### Phase 1: Named Cursors in BitDex (engine changes)

1. Add `cursors: HashMap<String, String>` to `InnerEngine`
2. Add `MutationOp::CursorSet { name: String, value: String }` variant
3. Flush thread: store latest cursor values in staging
4. Merge thread: write cursor files to `data/{index}/cursors/` at checkpoint
5. Startup: load cursor files from disk into HashMap
6. HTTP: accept optional `cursor` field on upsert/delete requests
7. HTTP: add `GET /cursors/{name}` and `GET /cursors` endpoints
8. Tests: cursor persistence across restart, concurrent cursor updates, missing cursor returns 404

### Phase 2: Sidecar Cursor-Based Polling

1. Add `replica_id` to `PgSyncConfig` (from env var)
2. On boot: read cursor from BitDex `GET /cursors/{replica_id}`
3. Change poll query to `WHERE id > $cursor ORDER BY id ASC`
4. Include cursor in upsert/delete payloads
5. Report cursor to `bitdex_cursors` table after each batch
6. Fix dedupe: highest outbox ID per entity wins (not "DELETE always wins")
7. Remove outbox row deletion from poller
8. Tests: cursor resume after restart, dedupe ordering

### Phase 3: Postgres Cleanup

1. Add `bitdex_cursors` table to setup SQL
2. Add cleanup trigger on `bitdex_cursors`
3. Tests: verify outbox rows cleaned after both cursors advance

### Phase 4: Kubernetes + Readiness

1. Add `/ready` endpoint (lag-aware)
2. Graceful shutdown: flush + checkpoint on SIGTERM
3. StatefulSet + PVC + PDB manifests
4. Integration test: simulate rolling restart

## Summary

| Component | Change |
|---|---|
| **BitDex engine** | Accept and persist named cursors at checkpoint time |
| **BitDex HTTP API** | Optional `cursor` on upsert/delete, GET cursor endpoints |
| **pg-sync sidecar** | Cursor-based polling, report cursor to PG, no outbox deletion |
| **Postgres** | `bitdex_cursors` table + cleanup trigger |
| **Kubernetes** | StatefulSet, PVC per pod, PDB, lag-aware readiness |

Total new code estimate: ~200-300 lines in BitDex, ~100 lines in pg-sync, ~20 lines SQL.

## Implementation Status

Phases 1-3 are **COMPLETE** on branch `worktree-rolling-restart-cursors`:

- Named cursors in `ConcurrentEngine` (in-memory HashMap, persisted by merge thread at checkpoint)
- `BitmapFs` cursor read/write (atomic file writes in `data/{index}/cursors/`)
- HTTP API: optional `cursor` on upsert/delete, `GET /cursors/{name}`, `GET /cursors`
- Outbox poller: cursor-based polling, highest-outbox-ID-wins dedupe, cursor reporting to PG
- PG setup SQL: `bitdex_cursors` table + auto-cleanup trigger
- `pg-sync load`: seeds cursor at outbox head before bulk load
- Config: `replica_id` field + `BITDEX_REPLICA_ID` env var override
- 389 tests passing, clean compile

Phase 4 (Kubernetes + readiness) is the remaining work.

## Kubernetes Handoff — What the K8s Agent Needs to Know

### Current State in talos-infra

The BitDex deployment lives at `clusters/production/apps/bitdex/deployment.yaml`. Today it is:

- A single-replica **Deployment** with `strategy: Recreate`
- One 200Gi PVC on **OpenEBS hostpath** NVMe (`openebs-hostpath-bitdex-nvme`)
- Pinned to a single node: `talos-fq9-f3k` (dedicated NVMe at `/var/mnt/bitdex-nvme`)
- Init container + sidecar pattern already **stubbed but commented out** in the YAML
- Image: `ghcr.io/civitai/bitdex:latest`
- Port 3000, exposed via Traefik IngressRoute to `bitdex.civitai.com`
- Secrets (DB/ClickHouse creds) SOPS-encrypted in `clusters/production/apps/bitdex/secrets/`

### Changes Required

#### 1. Deployment → StatefulSet

Convert the Deployment to a StatefulSet for stable pod names (`bitdex-0`, `bitdex-1`) and per-pod PVCs via `volumeClaimTemplates`.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: bitdex
  namespace: bitdex
spec:
  serviceName: bitdex
  replicas: 2
  podManagementPolicy: Parallel
```

The pod names (`bitdex-0`, `bitdex-1`) become the replica identifiers used for cursor tracking.

#### 2. Storage — The Biggest Blocker

Current storage is OpenEBS **hostpath** on a single NVMe node. Hostpath PVCs are node-local and cannot float between nodes. For two replicas on different nodes, there are two options:

**Option A: Add a second NVMe node** (recommended — keeps NVMe performance)
- Provision a second node with NVMe storage
- Label it for BitDex scheduling (e.g., `role: bitdex`)
- Create a second OpenEBS hostpath StorageClass or expand the existing one
- Use pod anti-affinity to spread replicas across both nodes

**Option B: Switch to replicated block storage** (Linstor or Longhorn)
- Allows pods to float between nodes
- Trades NVMe-direct performance for flexibility
- Probably unnecessary given BitDex's I/O pattern (bulk write at load, mostly reads after)

The recommended path is Option A — BitDex benefits from NVMe for the docstore and bitmap persistence. Two dedicated NVMe nodes, one pod each.

#### 3. Pod Anti-Affinity

Ensure replicas land on different nodes. There's an existing pattern in the redis deployment:

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: bitdex
        topologyKey: kubernetes.io/hostname
```

Replace the current single-node `nodeAffinity` with a role-based affinity + anti-affinity:

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: role
              operator: In
              values: [bitdex]
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: bitdex
        topologyKey: kubernetes.io/hostname
```

#### 4. PodDisruptionBudget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: bitdex
  namespace: bitdex
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: bitdex
```

#### 5. Container Configuration

Three containers per pod:

**Init container** — runs `pg-sync load` (only needed on fresh PVC, skips if data exists):
```yaml
initContainers:
  - name: bitdex-bulk-load
    image: ghcr.io/civitai/bitdex:latest
    command: ["bitdex-pg-sync", "load", "--config", "/etc/sync/sync.toml"]
    env:
      - name: BITDEX_REPLICA_ID
        valueFrom:
          fieldRef:
            fieldPath: metadata.name
    volumeMounts:
      - name: bitdex-data
        mountPath: /data
      - name: sync-config
        mountPath: /etc/sync
      - name: index-config
        mountPath: /etc/bitdex/index
```

The load command should be conditional — skip if data already exists on the PVC. Either:
- Check for the existence of `data/indexes/civitai/bitmaps/system/alive.roar` in the entrypoint script
- Or wrap in a shell: `sh -c "[ -f /data/indexes/civitai/bitmaps/system/alive.roar ] && echo 'Data exists, skipping load' || bitdex-pg-sync load --config /etc/sync/sync.toml"`

**Main container** — BitDex server:
```yaml
containers:
  - name: bitdex
    image: ghcr.io/civitai/bitdex:latest
    command: ["bitdex-server", "--data-dir", "/data", "--port", "3000"]
    ports:
      - containerPort: 3000
    startupProbe:
      httpGet:
        path: /api/health
        port: 3000
      initialDelaySeconds: 5
      periodSeconds: 5
      failureThreshold: 60    # 5 min tolerance for lazy loading
    livenessProbe:
      httpGet:
        path: /api/health
        port: 3000
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /api/health
        port: 3000
      periodSeconds: 5
```

**Sidecar** — pg-sync poller:
```yaml
  - name: bitdex-pg-sync
    image: ghcr.io/civitai/bitdex:latest
    command: ["bitdex-pg-sync", "sync", "--config", "/etc/sync/sync.toml"]
    env:
      - name: BITDEX_REPLICA_ID
        valueFrom:
          fieldRef:
            fieldPath: metadata.name    # "bitdex-0" or "bitdex-1"
      - name: BITDEX_URL
        value: "http://localhost:3000"
      - name: DATABASE_URL
        valueFrom:
          secretKeyRef:
            name: bitdex-db-connection
            key: DATABASE_URL
```

The sidecar waits for BitDex to be healthy before polling. On boot, the outbox poller loops on `GET /api/health` until it returns 200, then reads the cursor. During steady-state, every poll cycle checks health first — if BitDex is unreachable, the PG fetch is skipped entirely to avoid wasted work. The metrics poller has the same health gate.

On K8s 1.28+, native sidecars (`restartPolicy: Always` in `initContainers`) guarantee ordering. Check cluster K8s version — the current cluster runs v1.33-1.35 so this is available.

#### 6. sync.toml ConfigMap Update

The `bitdex-sync-config` ConfigMap needs the `replica_id` field. Since it's overridden by the `BITDEX_REPLICA_ID` env var, the ConfigMap just needs the default:

```toml
replica_id = "default"
```

The env var from `metadata.name` will override this at runtime.

#### 7. Readiness — Current Limitation

The design calls for a lag-aware `/ready` endpoint, but this is **not yet implemented** in the BitDex server code. For initial deployment, use `/api/health` for readiness (same as today). The lag-aware readiness probe is a follow-up task.

In practice, the `startupProbe` with 5-minute tolerance handles the cold-start case. The gap is: after a rolling restart, a pod could briefly serve slightly stale data (seconds behind) before the sidecar catches up. This is acceptable for the initial rollout.

#### 8. ClickHouse Metrics Poller

No cursor needed for ClickHouse. The metrics poller queries for "entities with activity since timestamp X" and fetches all-time totals. It's idempotent — both replicas can independently poll ClickHouse and push the same metric updates. No coordination required.

#### 9. Database Setup

The `bitdex_cursors` table and cleanup trigger are created automatically by `pg-sync load` (which runs `setup` internally) or `pg-sync setup`. The init container's `load` command will create everything needed on first run.

If running setup separately:
```bash
bitdex-pg-sync setup --config /etc/sync/sync.toml
```

### Deployment Sequence

1. **Provision second NVMe node** and label both nodes `role: bitdex`
2. **Create StorageClass** (or expand existing) for both NVMe nodes
3. **Apply StatefulSet + PDB + Service** manifests via git → FluxCD
4. Pod `bitdex-0` starts:
   - Init container checks for existing data → runs `pg-sync load` if fresh
   - Load seeds cursor at current outbox head, bulk loads from PG, saves snapshot
   - BitDex server starts, loads from snapshot (instant)
   - Sidecar starts, reads cursor, begins polling, catches up
5. Pod `bitdex-1` starts in parallel (same sequence)
6. Both pods become ready → Service routes traffic to both
7. Rolling updates now work: PDB ensures one pod stays up while the other restarts

### Rollback Plan

If rolling restarts cause issues, revert to single-replica by setting `replicas: 1` in the StatefulSet. The cursor infrastructure is backwards-compatible — a single replica with cursors works identically to before, just with the outbox cleanup being cursor-based instead of delete-based.
