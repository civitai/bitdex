# PG Dump & Bulk Load Operations Guide

How to dump fresh data from Postgres, get it onto the BitDex PVC, and run a bulk load. This is the canonical reference for the full reload cycle.

## Prerequisites

- `kubectl` configured for the production cluster
- Access to `bitdex` namespace
- The `bitdex` DB user on Postgres (79.127.232.241:5432) with `statement_timeout=0`
- ClickHouse credentials in `bitdex-secrets` K8s secret

## Overview

The bulk load process has three phases:

1. **Dump** — COPY tables from Postgres to CSV files on the PVC
2. **Load** — Build bitmaps + docstore from CSVs (the `bitdex-pg-sync load` command)
3. **Sync** — Start the server, pg-sync sidecar catches up from the outbox cursor

The loader checks for `.done` marker files alongside each CSV. If the marker exists, it skips the download. To force a fresh dump, delete the markers.

---

## Phase 1: Dump Data from Postgres

### Option A: Let the bulk load job dump automatically

The `bitdex-pg-sync load` binary has built-in COPY streams that dump all tables directly from PG. This is the simplest path but requires:
- The `DATABASE_URL` env var set (provided by `bitdex-secrets`)
- `statement_timeout=0` on the DB user (the `bitdex` user has this)
- No `.done` markers in `/data/indexes/civitai/load_stage/` for the tables you want refreshed

To force a fresh dump, delete the `.done` markers before running the bulk load:

```bash
# From inside a pod with PVC access:
rm /data/indexes/civitai/load_stage/*.done
```

### Option B: Manual psql dump (from the PG replica pod)

Use this when the bulk loader's COPY stream hits issues (statement timeouts, connection drops).

```bash
# Exec into the PG replica pod
kubectl exec -it cnpg-cluster-nvme0-1 -n cnpg-database -- bash

# Set no timeout for the session
export PGPASSWORD='<password>'  # from bitdex-secrets DATABASE_URL

# Create dump directory
mkdir -p /var/lib/postgresql/data/bitdex_dump && cd /var/lib/postgresql/data/bitdex_dump

# SET CURSOR FIRST — capture the outbox high-water mark BEFORE dumping
psql -U bitdex -d civitai -c "SELECT MAX(id) FROM \"BitdexOutbox\"" > cursor.txt
# Record this number — it becomes the sync starting cursor

# Dump all 8 tables (run in order, biggest last)
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, poi, type::text FROM \"Model\") TO STDOUT WITH CSV HEADER" > models.csv
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, \"baseModel\", \"modelId\" FROM \"ModelVersion\") TO STDOUT WITH CSV HEADER" > model_versions.csv
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"imageId\", \"toolId\" FROM \"ImageTool\") TO STDOUT WITH CSV HEADER" > tools.csv
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"imageId\", \"techniqueId\" FROM \"ImageTechnique\") TO STDOUT WITH CSV HEADER" > techniques.csv
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, extract(epoch from \"publishedAt\")::bigint, availability::text, \"modelVersionId\" FROM \"Post\" WHERE \"publishedAt\" IS NOT NULL) TO STDOUT WITH CSV HEADER" > posts.csv
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"imageId\", \"modelVersionId\", detected FROM \"ImageResourceNew\") TO STDOUT WITH CSV HEADER" > resources.csv
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, url, \"nsfwLevel\", hash, flags, type::text, \"userId\", \"blockedFor\", extract(epoch from \"scannedAt\")::bigint, extract(epoch from \"createdAt\")::bigint, \"postId\" FROM \"Image\") TO STDOUT WITH CSV" > images.csv
psql -U bitdex -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"tagId\", \"imageId\" FROM \"TagsOnImageDetails\" WHERE disabled = false) TO STDOUT WITH CSV" > tags.csv
```

**Important:** The Image and Tag queries do NOT include `HEADER` — the bulk loader's CSV parser expects raw data, not headers. The enrichment tables (models, posts, etc.) use `HEADER` because they're loaded into HashMaps.

### Setting the cursor

The cursor tells the pg-sync sidecar where to start catching up from. **Always capture this BEFORE the PG dump starts** — otherwise you'll miss changes that happened during the dump.

```bash
# Get the current outbox high-water mark
psql -U bitdex -d civitai -c "SELECT MAX(id) FROM \"BitdexOutbox\""
# Example output: 52754132

# Save it
echo "52754132" > cursor.txt
```

The cursor value goes into the `bitdex_cursors` table after the bulk load, or can be set via the BitDex API after the server starts.

---

## Phase 2: Transfer Files to the BitDex PVC

### File locations

The bulk loader expects CSVs at `/data/indexes/civitai/load_stage/` on the BitDex PVC.

| File | Rows | Size | Notes |
|------|------|------|-------|
| tags.csv | 4.48B | ~63 GB | Biggest file, tagId+imageId pairs |
| images.csv | 107.5M | 14 GB | Core image table |
| resources.csv | 41.2M | 783 MB | ImageResourceNew |
| posts.csv | 22.8M | 610 MB | Post enrichment |
| techniques.csv | 6.4M | 71 MB | ImageTechnique |
| tools.csv | 4.1M | 50 MB | ImageTool |
| model_versions.csv | 1.0M | 24 MB | ModelVersion enrichment |
| models.csv | 822K | 12 MB | Model enrichment |
| metrics.csv | 91M | 1.3 GB | ClickHouse metrics (fetched separately) |
| cursor.txt | 1 | 11 B | Outbox cursor high-water mark |

### Setting up a download server on the PVC

Since `kubectl cp` doesn't work (no `tar` in the bitdex container) and PodSecurity rejects ad-hoc pods, the proven approach is a Python HTTP server running inside the bulk load job pod, or a dedicated download pod.

```bash
# Scale down bitdex first (PVC is RWO)
kubectl scale statefulset/bitdex -n bitdex --replicas=0
kubectl wait --for=delete pod/bitdex-0 -n bitdex --timeout=60s

# Suspend FluxCD to prevent pod pruning
kubectl patch kustomization bitdex -n flux-system --type merge -p '{"spec":{"suspend":true}}'
kubectl patch kustomization production -n flux-system --type merge -p '{"spec":{"suspend":true}}'

# Create a download server pod (uses python3 http.server)
cat <<'YAML' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: bitdex-download
  namespace: bitdex
spec:
  nodeName: talos-fq9-f3k
  containers:
  - name: server
    image: python:3.11-slim
    command: ["python3", "-m", "http.server", "8080", "--directory", "/data/indexes/civitai/load_stage"]
    ports:
    - containerPort: 8080
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data-bitdex-0
  restartPolicy: Never
YAML

# Expose it via a Service + IngressRoute for external access
# (or use kubectl port-forward for local access)
kubectl port-forward -n bitdex bitdex-download 8080:8080 &

# Download files locally
curl -o images.csv http://localhost:8080/images.csv
```

### Pod-to-pod transfer (fastest)

For transferring from the PG replica pod to the PVC, use Python HTTP server on the source pod and wget on the destination:

```bash
# On the PG pod (source):
cd /var/lib/postgresql/data/bitdex_dump && python3 -m http.server 8080

# On the bitdex pod (destination):
wget -r -np -nH --cut-dirs=0 http://<pg-pod-ip>:8080/ -P /data/indexes/civitai/load_stage/
```

Transfer rate: ~240 MB/s over the pod network.

---

## Phase 3: ClickHouse Metrics

The ClickHouse metrics (reactionCount, commentCount, collectedCount) are fetched separately. The bulk loader handles this automatically if `CLICKHOUSE_URL`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD` env vars are set.

### Manual fetch

```bash
curl -o /data/indexes/civitai/load_stage/metrics.csv \
  --user "$CLICKHOUSE_USERNAME:$CLICKHOUSE_PASSWORD" \
  "$CLICKHOUSE_URL/" \
  --data-binary "SELECT
    entityId,
    sumIf(total, metricType IN ('ReactionLike','ReactionHeart','ReactionLaugh','ReactionCry')) as reactionCount,
    sumIf(total, metricType = 'Comment') as commentCount,
    sumIf(total, metricType = 'Collection') as collectedCount
  FROM entityMetricDailyAgg
  WHERE entityType = 'Image'
  GROUP BY entityId
  FORMAT TSV"
```

Output: ~91M rows, ~1.3 GB TSV (tab-separated, no header). Credentials are in the `bitdex-secrets` K8s secret.

---

## Phase 4: Run the Bulk Load

### Using the CronJob (standard)

```bash
# Ensure bitdex is scaled down
kubectl scale statefulset/bitdex -n bitdex --replicas=0
kubectl wait --for=delete pod/bitdex-0 -n bitdex --timeout=60s

# Trigger the bulk load
kubectl create job -n bitdex --from=cronjob/bitdex-bulk-load bitdex-bulk-load-v<N>

# Monitor progress
kubectl logs -f -n bitdex -l job-name=bitdex-bulk-load-v<N>
```

The CronJob uses `ghcr.io/civitai/bitdex:latest`. Verify it points to the correct version:

```bash
kubectl get cronjob -n bitdex bitdex-bulk-load -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].image}'
```

### Expected timeline

| Step | Duration | Notes |
|------|----------|-------|
| Tags CSV (4.5B rows) | ~200s | Biggest step, builds tagIds filter bitmaps |
| Images CSV (108M rows) | ~80s | Core image fields + docstore tuples |
| Resources CSV | ~15s | modelVersionIds filter bitmaps |
| Posts CSV | ~10s | Post enrichment |
| Tools CSV | ~2s | toolIds filter bitmaps |
| Techniques CSV | ~1s | techniqueIds filter bitmaps |
| Metrics CSV | ~30s | ClickHouse sort bitmaps |
| Bitmap save | ~30s | Streaming save to BitmapFs |
| **Total** | **~6-7 min** | 107.6M records at ~278K/s |

### After completion

```bash
# Scale bitdex back up
kubectl scale statefulset/bitdex -n bitdex --replicas=1

# Wait for ready
kubectl wait --for=condition=Ready pod/bitdex-0 -n bitdex --timeout=300s

# Verify record count
kubectl logs -n bitdex bitdex-0 -c bitdex --tail=5
# Should show: Restored index 'civitai' from disk (107616479 records)

# Check sync is working
kubectl logs -n bitdex bitdex-0 -c pg-sync --tail=10
# Should show: upserted N documents, cursor advancing
```

---

## Phase 5: Unsuspend FluxCD

If you suspended FluxCD during the process:

```bash
kubectl patch kustomization production -n flux-system --type merge -p '{"spec":{"suspend":false}}'
kubectl patch kustomization bitdex -n flux-system --type merge -p '{"spec":{"suspend":false}}'
```

---

## Codebase Reference

The dump and load logic lives in these files:

| File | Purpose |
|------|---------|
| `src/pg_sync/copy_queries.rs` | COPY TO STDOUT queries for all 8 tables, CSV parsers |
| `src/pg_sync/copy_streams.rs` | Rayon parallel bitmap construction from COPY streams |
| `src/pg_sync/bulk_loader.rs` | Full bulk load orchestrator (download, build, save) |
| `src/pg_sync/single_pass.rs` | Single-pass V2 CSV loader (mmap'd files, no PG needed) |
| `src/pg_sync/config.rs` | PgSyncConfig with `DATABASE_URL` env var support |
| `src/bin/pg_sync.rs` | Binary entry point (`bitdex-pg-sync load`) |

---

## Troubleshooting

### Statement timeout kills COPY streams
The `bitdex` PG user should have `statement_timeout=0`. Verify:
```sql
SELECT usename, useconfig FROM pg_user WHERE usename = 'bitdex';
```
If missing, ask a DBA to: `ALTER USER bitdex SET statement_timeout = 0;`

### PodSecurity rejects ad-hoc pods
The bitdex namespace has restricted PodSecurity. The bulk load CronJob and StatefulSet pass because they're in the FluxCD manifests. Ad-hoc pods (busybox, curl) get silently rejected. Workarounds:
- Suspend FluxCD, scale down bitdex, use a pod that mounts the PVC
- Use the bulk load job itself (it has PVC access)
- Use `kubectl port-forward` from a running pod

### Bulk load uses stale CSVs
Delete the `.done` markers to force re-download:
```bash
# From inside a pod with PVC access:
rm /data/indexes/civitai/load_stage/*.done
```

### FluxCD prunes pods
Suspend both kustomizations before creating ad-hoc pods:
```bash
kubectl patch kustomization production -n flux-system --type merge -p '{"spec":{"suspend":true}}'
kubectl patch kustomization bitdex -n flux-system --type merge -p '{"spec":{"suspend":true}}'
```
Always unsuspend when done.

### Data loss on restart
If the bulk load data disappears after a pod restart, check if FluxCD redeployed or if the PVC path changed. The data lives at `/data/indexes/civitai/` on the `data-bitdex-0` PVC (200Gi, openebs-hostpath on talos-k8s-worker-3).
