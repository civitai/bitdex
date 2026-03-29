# BitDex Deployment Handoff

This document tells you everything you need to deploy BitDex to production, run the initial data load, verify the system is working, and troubleshoot if it isn't. Written for an agent picking this up cold.

## Repositories

| Repo | Path on this machine | What it contains |
|------|---------------------|-----------------|
| **bitdex-v2** | `C:\Dev\Repos\open-source\bitdex-v2` | Rust source code — the engine, server, pg-sync binaries, integration tests |
| **talos-infra** | `C:\Dev\Repos\work\civitai\talos-infra` | Kubernetes manifests for all Civitai infrastructure. BitDex lives at `clusters/production/apps/bitdex/` |

## What gets deployed

One Docker image (`ghcr.io/civitai/bitdex:latest`) contains two binaries:

- **`bitdex-server`** — the query server (HTTP API on port 3000, Prometheus metrics on port 9090)
- **`bitdex-pg-sync`** — the Postgres sync tool with two subcommands:
  - `load` — one-shot bulk loader that streams all data from Postgres via COPY, builds bitmaps + docstore on disk
  - `sync` — long-running sidecar that polls the Postgres outbox for changes and pushes them to the server

## K8s Architecture (talos-infra)

All manifests are in `clusters/production/apps/bitdex/`. FluxCD auto-applies everything in `kustomization.yaml`.

| File | Managed by FluxCD? | Purpose |
|------|-------------------|---------|
| `deployment.yaml` | Yes | Namespace, StorageClass, ConfigMaps (sync.toml, config.json), StatefulSet (server + sidecar), Services, PDB, IngressRoute, PodMonitor |
| `bulk-load-job.yaml` | Yes | Suspended CronJob — template for bulk loading. Never auto-runs. |
| `kustomization.yaml` | — | Lists resources for FluxCD |
| `secrets/` | Yes | SOPS-encrypted DATABASE_URL, ClickHouse creds, GHCR pull secret |
| `README.md` | — | Ops documentation for the DevOps team |

**PR #20** (https://github.com/civitai/talos-infra/pull/20) adds the sidecar, CronJob, PodMonitor, ConfigMaps, and README. It needs to be merged before deployment. Zach (ZacxDev) is assigned as reviewer.

### Node

Everything runs on `talos-fq9-f3k` — a dedicated node with NVMe storage. The PVC (`data-bitdex-0`, 200Gi) is backed by `openebs-hostpath-bitdex-nvme` on `/var/mnt/bitdex-nvme`.

## Deployment Sequence

### Step 1: Merge the bitdex-v2 code changes

The COPY-based bulk loader, progress endpoint, and related changes need to be on the `main` branch. Key files changed:

- `src/pg_sync/copy_queries.rs` — COPY SQL + CSV parser (new)
- `src/pg_sync/copy_streams.rs` — per-table COPY stream processing (new)
- `src/pg_sync/progress.rs` — load progress HTTP endpoint (new)
- `src/pg_sync/bulk_loader.rs` — `run_bulk_load_copy()` function (modified)
- `src/pg_sync/config.rs` — `progress_port` config field (modified)
- `src/pg_sync/mod.rs` — new module exports (modified)
- `src/bin/pg_sync.rs` — progress server wiring (modified)
- `src/pg_sync/bitdex_client.rs` — HTTP timeouts for health gate (modified)
- `docker/Dockerfile` — removed stale MALLOC_CONF, bumped Rust to 1.88 (modified)
- `docker/Dockerfile.simd` — removed stale MALLOC_CONF (modified)

Check current status:
```bash
cd C:\Dev\Repos\open-source\bitdex-v2
git status
git log --oneline -5
```

All unit tests should pass:
```bash
cargo test --features pg-sync
```

Integration tests (requires Docker):
```bash
cd tests/integration
bash run.sh        # outbox flow test (12 tests)
bash run-bulk.sh   # bulk load → sync handoff test (11 tests)
```

### Step 2: Build and push the Docker image

The production Dockerfile is `docker/Dockerfile`. It targets the EPYC 4585PX (Zen 5) CPU with AVX-512 optimizations.

```bash
cd C:\Dev\Repos\open-source\bitdex-v2

# Build
docker build -t ghcr.io/civitai/bitdex:latest -f docker/Dockerfile .

# Push to GHCR
docker push ghcr.io/civitai/bitdex:latest
```

Or if there's a CI pipeline, merge to `main` and let it build. Check GHCR for the image:
```bash
gh api orgs/civitai/packages/container/bitdex/versions --jq '.[0] | {id, created_at: .metadata.container.tags}'
```

### Step 3: Merge the talos-infra PR

```bash
cd C:\Dev\Repos\work\civitai\talos-infra
gh pr view 20
gh pr merge 20 --squash  # or wait for Zach's approval
```

FluxCD will reconcile within ~1 minute. Monitor:
```bash
flux reconcile kustomization production
flux logs --follow
```

### Step 4: Verify the StatefulSet comes up

```bash
kubectl get pods -n bitdex -o wide
kubectl describe pod -n bitdex bitdex-0
```

The server container should become ready (startup probe passes). The sidecar will start polling but won't find data yet — that's expected. Check sidecar logs:
```bash
kubectl logs -n bitdex bitdex-0 -c pg-sync --tail=20
```

You might see "get_cursor returned 404" — this is normal before the bulk load creates the cursor.

### Step 5: Run the bulk load

This is the big one. The bulk loader streams all data from Postgres via `COPY TO STDOUT`, builds the bitmap index and docstore on the NVMe PVC. Takes ~10-15 minutes for 105M records.

```bash
# Trigger the load from the suspended CronJob template
kubectl create job -n bitdex --from=cronjob/bitdex-bulk-load bitdex-bulk-load-run-1
```

### Step 6: Monitor the bulk load

**Logs:**
```bash
kubectl logs -n bitdex job/bitdex-bulk-load-run-1 -f
```

Key log lines to watch for:
- `Connected to Postgres (pool_size=10)` — PG connection ok
- `Setting up BitdexOutbox table and triggers...` — creating outbox infrastructure
- `Seeded cursor 'pg-sync-bitdex-0' at outbox head 12345` — cursor position saved
- `Starting bulk load...` — COPY streams beginning
- `Bulk load complete: 105000000 records in 612.3s (171432/s)` — success

**Progress endpoint:**
```bash
# In one terminal:
kubectl port-forward -n bitdex job/bitdex-bulk-load-run-1 9091:9091

# In another:
watch -n5 'curl -s localhost:9091/status | jq .'
```

The progress response shows per-stream row counts and rates:
```json
{
  "phase": "streaming",
  "elapsed_secs": 127.4,
  "streams": {
    "images":     { "rows": 52000000, "rate": 408000, "done": false },
    "tags":       { "rows": 310000000, "rate": 2430000, "done": false },
    "tools":      { "rows": 10000000, "rate": 78500, "done": true },
    "techniques": { "rows": 5000000, "rate": 39200, "done": true },
    "resources":  { "rows": 95000000, "rate": 745000, "done": false }
  },
  "streams_done": 2
}
```

Phases in order: `setup` → `streaming` → `cleanup` → `applying` → `finalizing` → `saving` → `done`.

Tags is the bottleneck (~1.3B rows). The load is done when the job completes:
```bash
kubectl wait --for=condition=complete job/bitdex-bulk-load-run-1 -n bitdex --timeout=30m
```

### Step 7: Restart the server to pick up the snapshot

```bash
kubectl rollout restart statefulset/bitdex -n bitdex
kubectl rollout status statefulset/bitdex -n bitdex
```

The server lazy-loads bitmaps on first query (startup is <1s, first query may take a few seconds as fields load from disk).

### Step 8: Verify the system is working

**Check document count:**
```bash
curl -s https://bitdex.civitai.com/api/indexes/civitai/query \
  -H 'Content-Type: application/json' \
  -d '{"filters":[],"limit":1}' | jq '.total_matched'
```

Should return ~105M.

**Check cursor exists (seeded by loader):**
```bash
kubectl exec -n bitdex bitdex-0 -c bitdex -- \
  curl -s localhost:3000/api/indexes/civitai/cursors/pg-sync-bitdex-0
```

Should return a JSON object with a `value` field (the outbox ID where sync will start).

**Check sidecar is polling:**
```bash
kubectl logs -n bitdex bitdex-0 -c pg-sync --tail=20
```

Should show periodic poll cycles. If data is changing in Postgres, you'll see upsert batches being pushed.

**Check Prometheus metrics are being scraped:**
```bash
kubectl get podmonitor -n bitdex
# In Grafana: query for bitdex_documents_total
```

**Run a real query:**
```bash
curl -s https://bitdex.civitai.com/api/indexes/civitai/query \
  -H 'Content-Type: application/json' \
  -d '{"filters":[{"field":"nsfwLevel","op":"Eq","value":1}],"sort":{"field":"sortAt","direction":"Desc"},"limit":20}' | jq '.ids[:5]'
```

### Step 9: Verify cursor advancement (sync is working)

Wait a minute, then check the cursor again:
```bash
kubectl exec -n bitdex bitdex-0 -c bitdex -- \
  curl -s localhost:3000/api/indexes/civitai/cursors/pg-sync-bitdex-0
```

If data is flowing through the Postgres outbox, the cursor value should increase over time. You can also check the outbox table directly:
```bash
# Requires psql access to the PG replica
# The outbox should stay small — rows are cleaned up after all cursors pass them
SELECT COUNT(*) FROM "BitdexOutbox";
SELECT MAX(id) FROM "BitdexOutbox";
SELECT * FROM bitdex_cursors;
```

## How the System Works

### Bulk Load (one-time)

1. `pg-sync load` creates the `BitdexOutbox` table and triggers on Image, Post, Tags, Tools, Techniques, Resources, ModelVersion, Model tables
2. All triggers use `ENABLE ALWAYS` so they fire on the logical replica (Debezium sets `session_replication_role = replica` which skips normal triggers)
3. The loader snapshots the current max outbox ID — this becomes the cursor starting position
4. Five parallel COPY streams read all data from Postgres:
   - Image + Post JOIN (105M rows)
   - Tags ordered by tagId (1.3B rows — the bottleneck)
   - Tools ordered by toolId
   - Techniques ordered by techniqueId
   - Resources + ModelVersion + Model JOIN (200M rows)
5. Each stream builds roaring bitmaps (filter + sort) and writes document data to the arena
6. After all streams complete, an alive-cleanup pass ANDs enrichment bitmaps against the alive bitmap (strips orphan bits from tags referencing deleted images)
7. Bitmaps are merged, applied to the engine staging, and published
8. Docstore shards are finalized to disk
9. Bitmap snapshot is saved to NVMe
10. The cursor is persisted both in PG (`bitdex_cursors` table) and in the BitDex engine

### Steady-State Sync (continuous)

The sidecar (`pg-sync sync`) runs two concurrent loops:

**Outbox poller (every 2s):**
1. Health check: `GET /api/health` on localhost:3000 (3s timeout). If the server is down, skip this cycle.
2. Read the current cursor position from BitDex
3. Query `BitdexOutbox` for rows after the cursor (up to 5000 per batch)
4. For each batch: fetch full document data from PG, push upserts/deletes to BitDex via HTTP
5. Advance the cursor in both BitDex and PG
6. PG cleanup trigger fires: deletes outbox rows below the minimum cursor across all replicas

**Metrics poller (every 60s):**
1. Query ClickHouse for aggregated engagement metrics (reactionCount, commentCount, collectedCount)
2. Push updated sort field values to BitDex

### Cursor Continuity (the critical handoff)

The bulk loader seeds the cursor at the outbox head BEFORE streaming data. Any changes that happen during the load create outbox entries AFTER this position. When the sidecar starts, it reads the cursor and begins polling from that exact position. No data is missed, no data is re-processed.

## Troubleshooting

### Bulk load OOMKilled
The load peaks at ~25GB. The Job requests 28Gi with a 32Gi limit. If it's still OOMing, the dataset may have grown. Increase the limit in `bulk-load-job.yaml`.

### Bulk load fails with "Setup failed"
The loader can't create outbox triggers. Check that `DATABASE_URL` points to a PG instance where the user has CREATE TABLE/TRIGGER permissions. On the logical replica, this requires superuser or replication role.

### Sidecar says "BitDex is unreachable"
This is the health gate working as designed. The sidecar pauses polling when the server is down (e.g., during a restart). It will resume automatically when the server comes back. Check that the server container is healthy:
```bash
kubectl logs -n bitdex bitdex-0 -c bitdex --tail=20
```

### Sidecar says "get_cursor returned 404"
The cursor doesn't exist in BitDex. This means the bulk load hasn't run yet, or the server restarted without restoring the snapshot. Run the bulk load first.

### Cursor not advancing
Check: (1) the outbox has rows (`SELECT COUNT(*) FROM "BitdexOutbox"`), (2) the sidecar is running and healthy, (3) the triggers exist (`SELECT tgname FROM pg_trigger WHERE tgname LIKE 'bitdex%'`).

### Data appears stale / missing recent changes
The outbox poll interval is 2 seconds. If changes aren't appearing after 10+ seconds: check sidecar logs, verify the cursor is advancing, verify the trigger for the relevant table is `ENABLE ALWAYS` (not just `ENABLE`).

### Server slow on first query after restart
This is normal. Bitmaps are lazy-loaded from disk on first query. The first query touching `tagIds` takes ~6-7 seconds (31K values, 79% of bitmap memory). Subsequent queries are fast.

## Resource Budgets

| Component | CPU | Memory | Disk |
|-----------|-----|--------|------|
| Server (steady state) | 8 cores | ~14.5 GB RSS at 105M records | ~15 GB on NVMe |
| Sidecar | 500m | ~512 MB | None (talks to server via HTTP) |
| Bulk load (peak) | 8 cores | ~25 GB peak during COPY streams | Writes to server's NVMe PVC |

## Config Files to Know

- **sync.toml** (ConfigMap `bitdex-sync-config`): controls PG connection, poll intervals, batch sizes, Bitdex URL. Env vars override values at runtime (DATABASE_URL, CLICKHOUSE_URL, etc.)
- **config.json** (ConfigMap `bitdex-index-config`): defines the index schema — filter fields, sort fields, data schema with field mappings and string maps. Changes here require a full reload.
- **Secrets** (SOPS encrypted in `secrets/`): DATABASE_URL, CLICKHOUSE_URL/USERNAME/PASSWORD, GHCR pull creds.

## Integration Tests

Before deploying, you can verify locally with Docker:

```bash
cd C:\Dev\Repos\open-source\bitdex-v2\tests\integration

# Test 1: Outbox flow (sidecar sync, cursor advancement, restart recovery, deletes)
bash run.sh

# Test 2: Bulk load → sync handoff (load seeds cursor, sync resumes, no gap)
bash run-bulk.sh
```

Both should show all tests passing. The bulk load test (`run-bulk.sh`) is the one that validates the exact production flow: loader runs → server restores → sidecar picks up cursor → new inserts flow through → cursor advances.
