# Bulk Load Handoff

All 8 tables are dumped as CSV files on the Postgres server's local NVMe. The BitDex code (v1.0.11) can read these files and build bitmaps locally with zero PG dependency.

## What's done

Server-side `COPY TO FILE` completed for all tables. Files are at:

```
Pod:  cnpg-cluster-nvme0-1  (namespace: cnpg-database)
Path: /var/lib/postgresql/data/bitdex_dump/
```

| File | Rows | Size |
|------|------|------|
| tags.csv | 4.48B | ~62 GB |
| images.csv | 107.5M | 14 GB |
| posts.csv | 22.8M | 610 MB |
| resources.csv | 41.2M | 777 MB |
| techniques.csv | 6.4M | 71 MB |
| tools.csv | 4.1M | 50 MB |
| model_versions.csv | 1.0M | 24 MB |
| models.csv | 822K | 12 MB |

## What needs to happen

### Step 1: Transfer files to BitDex PVC

The bulk loader reads from `/data/indexes/civitai/load_stage/`. The bitdex server must be scaled to 0 first (memory contention with Meilisearch on the same node).

```bash
# Scale down server
kubectl scale statefulset/bitdex -n bitdex --replicas=0
kubectl wait --for=delete pod/bitdex-0 -n bitdex --timeout=120s

# Create a transfer pod that mounts the bitdex PVC
kubectl run bitdex-transfer -n bitdex --image=busybox \
  --overrides='{"spec":{"containers":[{"name":"transfer","image":"busybox","command":["sleep","3600"],"volumeMounts":[{"name":"data","mountPath":"/data"}]}],"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"data-bitdex-0"}}],"nodeSelector":{"kubernetes.io/hostname":"talos-fq9-f3k"}}}' \
  --restart=Never

# Wait for it
kubectl wait --for=condition=ready pod/bitdex-transfer -n bitdex --timeout=60s

# Create the staging directory
kubectl exec -n bitdex bitdex-transfer -- mkdir -p /data/indexes/civitai/load_stage
```

Transfer each file. Start with the small ones, then the big ones. Use `MSYS_NO_PATHCONV=1` on Windows/Git Bash to prevent path mangling:

```bash
# Small files first (seconds each)
for f in models.csv model_versions.csv tools.csv techniques.csv posts.csv resources.csv; do
  echo "Copying $f..."
  kubectl cp cnpg-database/cnpg-cluster-nvme0-1:/var/lib/postgresql/data/bitdex_dump/$f \
    bitdex/bitdex-transfer:/data/indexes/civitai/load_stage/$f
  kubectl exec -n bitdex bitdex-transfer -- sh -c "echo ok > /data/indexes/civitai/load_stage/$f.done"
done

# Images (14 GB — a few minutes)
kubectl cp cnpg-database/cnpg-cluster-nvme0-1:/var/lib/postgresql/data/bitdex_dump/images.csv \
  bitdex/bitdex-transfer:/data/indexes/civitai/load_stage/images.csv
kubectl exec -n bitdex bitdex-transfer -- sh -c "echo ok > /data/indexes/civitai/load_stage/images.csv.done"

# Tags (62 GB — will take a while, network transfer)
kubectl cp cnpg-database/cnpg-cluster-nvme0-1:/var/lib/postgresql/data/bitdex_dump/tags.csv \
  bitdex/bitdex-transfer:/data/indexes/civitai/load_stage/tags.csv
kubectl exec -n bitdex bitdex-transfer -- sh -c "echo ok > /data/indexes/civitai/load_stage/tags.csv.done"
```

Alternative: if both pods are on the same node or have a shared volume, use a direct copy instead of kubectl cp (avoids network).

```bash
# Clean up transfer pod when done
kubectl delete pod -n bitdex bitdex-transfer
```

### Step 2: Wipe old data (if any partial loads exist)

```bash
kubectl run bitdex-cleanup -n bitdex --image=busybox \
  --overrides='...' --restart=Never \
  -- sh -c "rm -rf /data/indexes/civitai/bitmap_* /data/indexes/civitai/docs /data/indexes/civitai/slot_arena.bin && echo cleaned"
```

Keep the `load_stage/` directory — that's our data.

### Step 3: Run the bulk load job

```bash
kubectl create job -n bitdex --from=cronjob/bitdex-bulk-load bitdex-bulk-load-final
```

The loader will:
1. See `.done` markers in `load_stage/` — skip all PG downloads
2. Load enrichment tables (posts, MV, models) from CSV into HashMaps
3. Build bitmaps from images.csv, tags.csv, tools.csv, techniques.csv, resources.csv
4. Clean orphan bitmaps, merge, apply to engine, write docstore, save snapshot
5. Clean up `load_stage/` on success

Monitor:
```bash
kubectl logs -n bitdex job/bitdex-bulk-load-final -f
```

### Step 4: Restart server + verify

```bash
kubectl scale statefulset/bitdex -n bitdex --replicas=1
kubectl rollout status statefulset/bitdex -n bitdex

# Check document count
curl -s https://bitdex.civitai.com/api/indexes/civitai/query \
  -H 'Content-Type: application/json' \
  -d '{"filters":[],"limit":1}' | jq '.total_matched'
```

## Cleanup

After successful load, delete the dump files from the PG server:

```bash
MSYS_NO_PATHCONV=1 kubectl exec -n cnpg-database cnpg-cluster-nvme0-1 -- \
  rm -rf /var/lib/postgresql/data/bitdex_dump
```

## Known issues

- **Slot arena**: 512 bytes/slot x 124M = 60GB mmap file. Created during Phase 2, cleaned up after. PVC needs ~76GB staging + 60GB arena + 15GB final = ~151GB. Fits in 200Gi but tight.
- **Memory**: Bulk load job requests 28Gi. Server requests 32Gi. Can't run simultaneously on the node (Meilisearch takes 80Gi). Server must be scaled to 0 during load.
- **kubectl cp for 62GB tags.csv**: This streams over the K8s API server. Could be slow. If both pods are on the same node, a shared hostPath mount would be faster.
- **Statement timeout**: v1.0.7+ sets `statement_timeout=0` on all pg-sync connections. The PG server itself has a 5-minute timeout for superuser sessions.

## Architecture (for reference)

The bulk loader has two phases:
- **Phase 1 (Download)**: Streams tables from PG to CSV files via COPY TO STDOUT. Resumable per-table with `.done` markers. **Skipped if files already exist** (our case).
- **Phase 2 (Build)**: Reads local CSV files, builds roaring bitmaps, writes docstore. No PG connection needed. Pure CPU + local disk I/O.

The server-side dump approach (what we did) bypasses Phase 1 entirely by pre-populating the staging directory from `kubectl exec` + `COPY TO FILE` inside the PG pod.
