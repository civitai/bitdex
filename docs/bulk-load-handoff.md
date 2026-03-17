# Bulk Load & Reload Guide

Production bulk load procedure for BitDex. Covers fresh loads, reloads after code changes, cursor management, and known pitfalls.

## Prerequisites

- **Flux must be suspended** before any scale-to-0. Flux reconciles replicas back to the declared count within seconds, fighting manual scaling.
- **Both PVCs** (data-bitdex-0, data-bitdex-1) need separate load jobs — the CronJob template only targets data-bitdex-0.
- **CSVs on PVCs** in `/data/indexes/civitai/load_stage/` with `.done` markers.

```bash
# Suspend Flux (MUST be first)
kubectl patch kustomization bitdex -n flux-system \
  --type=json -p='[{"op":"add","path":"/spec/suspend","value":true}]'

# Verify
kubectl get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}'
# Should print: true
```

## Full Reload Procedure

Use when code changes affect how the bulk loader writes data (docstore field names, conversions, bitmap logic).

### Step 1: Scale down

```bash
kubectl scale statefulset/bitdex -n bitdex --replicas=0
kubectl wait --for=delete pod/bitdex-0 pod/bitdex-1 -n bitdex --timeout=120s
```

### Step 2: Wipe old data (keep CSVs)

```bash
for i in 0 1; do
  kubectl run wipe-$i -n bitdex --image=busybox \
    --overrides="{\"spec\":{\"containers\":[{\"name\":\"w\",\"image\":\"busybox\",\"command\":[\"sh\",\"-c\",\"rm -rf /data/indexes/civitai/bitmaps /data/indexes/civitai/docs /data/indexes/civitai/bounds /data/indexes/civitai/slot_arena.bin /data/indexes/civitai/snapshot.meta && echo wiped-$i\"],\"volumeMounts\":[{\"name\":\"d\",\"mountPath\":\"/data\"}]}],\"volumes\":[{\"name\":\"d\",\"persistentVolumeClaim\":{\"claimName\":\"data-bitdex-$i\"}}],\"nodeSelector\":{\"kubernetes.io/hostname\":\"talos-fq9-f3k\"}}}" \
    --restart=Never
done
# Wait + verify + cleanup
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/wipe-0 pod/wipe-1 -n bitdex --timeout=60s
kubectl logs -n bitdex wipe-0; kubectl logs -n bitdex wipe-1
kubectl delete pod -n bitdex wipe-0 wipe-1
```

**Do NOT wipe config.json.** The bulk load job creates its own from the K8s configmap. If you wipe it, the server will fail to start with "EOF while parsing a value."

### Step 3: Update CronJob image (if new release)

```bash
kubectl set image cronjob/bitdex-bulk-load -n bitdex "*=ghcr.io/civitai/bitdex:<version>"
```

### Step 4: Run bulk load jobs

```bash
# Job for PVC 0 (from CronJob template)
kubectl create job -n bitdex reload-0 --from=cronjob/bitdex-bulk-load

# Job for PVC 1 (modified to use data-bitdex-1)
kubectl get cronjob -n bitdex bitdex-bulk-load -o json | \
  node -e "
const c=[];process.stdin.on('data',d=>c.push(d));process.stdin.on('end',()=>{
  const cj=JSON.parse(Buffer.concat(c));
  const spec=cj.spec.jobTemplate.spec;
  for(const v of spec.template.spec.volumes){
    if(v.persistentVolumeClaim&&v.persistentVolumeClaim.claimName==='data-bitdex-0')
      v.persistentVolumeClaim.claimName='data-bitdex-1';
  }
  for(const c of spec.template.spec.containers){
    for(const e of(c.env||[])){
      if(e.name==='BITDEX_REPLICA_ID')e.value='bitdex-1';
    }
  }
  process.stdout.write(JSON.stringify({apiVersion:'batch/v1',kind:'Job',
    metadata:{name:'reload-1',namespace:'bitdex'},spec}));
})" | kubectl apply -f -
```

Monitor:
```bash
kubectl get jobs -n bitdex -w
kubectl logs -n bitdex job/reload-0 --tail=5  # progress
```

Typical load time: ~8 min for 107M images (tags ~6 min, images ~2 min, rest < 1 min).

The loader includes collectionIds as a step (v1.0.56+). It downloads `collection_items.csv` from PG and builds collectionIds bitmaps alongside the other filter fields. **The pg-sync sidecar does NOT do backfills** — all bitmap building happens in the loader.

### Step 5: Reset cursors

The bulk load seeds cursors at the current outbox head. **This is wrong** — the CSVs are from an earlier point in time. You must reset cursors to the outbox ID from when the CSVs were dumped.

```bash
# Find the correct cursor value
kubectl exec -n bitdex <any-transfer-pod> -- cat /data/indexes/civitai/load_stage/cursor.txt
# e.g. 52905554

# Reset on both PVCs
for i in 0 1; do
  kubectl run cr-$i -n bitdex --image=busybox \
    --overrides="{\"spec\":{\"containers\":[{\"name\":\"c\",\"image\":\"busybox\",\"command\":[\"sh\",\"-c\",\"mkdir -p /data/indexes/civitai/bitmaps/cursors && echo -n 52905554 > /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-$i && cat /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-$i\"],\"volumeMounts\":[{\"name\":\"d\",\"mountPath\":\"/data\"}]}],\"volumes\":[{\"name\":\"d\",\"persistentVolumeClaim\":{\"claimName\":\"data-bitdex-$i\"}}],\"nodeSelector\":{\"kubernetes.io/hostname\":\"talos-fq9-f3k\"}}}" \
    --restart=Never
done
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/cr-0 pod/cr-1 -n bitdex --timeout=30s
kubectl delete pod -n bitdex cr-0 cr-1

# Reset in PG (both replicas)
MSYS_NO_PATHCONV=1 kubectl exec -n cnpg-database cnpg-cluster-nvme0-1 -- \
  psql -U postgres -d civitai -c \
  "UPDATE bitdex_cursors SET last_outbox_id = 52905554 WHERE replica_id IN ('pg-sync-bitdex-0', 'pg-sync-bitdex-1');"
```

### Step 6: Clean up jobs + scale up

```bash
kubectl delete job -n bitdex reload-0 reload-1
kubectl set image statefulset/bitdex -n bitdex \
  bitdex=ghcr.io/civitai/bitdex:<version> \
  pg-sync=ghcr.io/civitai/bitdex:<version>
kubectl scale statefulset/bitdex -n bitdex --replicas=2
kubectl rollout status statefulset/bitdex -n bitdex --timeout=300s
```

### Step 7: Verify

```bash
# Port forward and check a known image
kubectl port-forward -n bitdex bitdex-0 3099:3000 &
curl -s 'http://localhost:3099/api/indexes/civitai/query?format=compact' \
  -H 'Content-Type: application/json' \
  -d '{"filter":{"isPublished":true},"sort":"publishedAt","limit":3,"include_docs":true}'
# publishedAt should be nonzero for published images

# Check pg-sync is catching up
kubectl logs -n bitdex bitdex-0 -c pg-sync --tail=20 | grep -v "slot not found"
# Should see: "processed N changes (cursor=...)"
```

### Step 8: Unsuspend Flux

```bash
kubectl patch kustomization bitdex -n flux-system \
  --type=json -p='[{"op":"replace","path":"/spec/suspend","value":false}]'
```

## Pitfalls (learned the hard way)

### Flux reconciliation
Flux will restore replicas=2 within seconds of a scale-to-0. Always suspend Flux before scaling down. The `kubectl scale` command succeeds but gets immediately overwritten.

### Config.json overwrites
The bulk load job copies config.json from the K8s configmap into the index directory. If you write your own config.json to the PVC before the load, the load overwrites it. If you write it after the load via a stdin pipe that fails silently, you get an empty file and the server can't start.

**Rule:** Let the bulk load create config.json. Keep the K8s configmap (`bitdex-index-config`) as the source of truth — any new fields (like collectionIds) must be added there.

### PVC contention (same node)
Both StatefulSet pods and bulk load jobs run on talos-fq9-f3k. PVCs are ReadWriteOnce but K8s allows multiple pods on the same node to mount the same PVC. This means the server and bulk loader can run simultaneously on the same PVC, causing data corruption. Always ensure the server pod is fully terminated before starting the load job.

### Cursor drift
If server pods start briefly before you reset cursors, the pg-sync sidecar reads the bulk-load-seeded cursor (current outbox head) and starts polling from there. When you later reset the cursor file, the running pg-sync has already cached the old value in memory. You must reset cursors while pods are down.

### Three docstore write paths
The docstore is written by three separate code paths that must stay in sync:
1. **`single_pass.rs`** — production bulk loader (CSV → bitmaps + docstore)
2. **`bulk_loader.rs`** — older bulk loader (not used in production)
3. **`row_assembler.rs`** — outbox poller (PG → PATCH/PUT)

Field names, conversions (ms_to_seconds), and types (exists_boolean) must match across all three. The `format_document()` serving path looks up fields by target name first, then source name, and only applies ms_to_seconds when found by source name.

### Source vs target field names
- **Source name** (e.g. `publishedAtUnix`): raw value from PG, may be in milliseconds
- **Target name** (e.g. `publishedAt`): converted value, in seconds
- The bulk loader should store under **target names** with conversions applied
- The outbox row_assembler stores under **target names** (via schema mapping)
- `format_document()` looks up by target first, source second

## CSV Files

Located on each PVC at `/data/indexes/civitai/load_stage/`:

| File | Rows | Size | Source |
|------|------|------|--------|
| tags.csv | 4.49B | ~63 GB | PG TagsOnImageNew |
| images.csv | 107.8M | 14 GB | PG Image |
| posts.csv | 22.8M | 610 MB | PG Post |
| resources.csv | 41.2M | 777 MB | PG ImageResourceNew |
| techniques.csv | 6.4M | 71 MB | PG ImageTechnique |
| tools.csv | 4.1M | 50 MB | PG ImageTool |
| model_versions.csv | 1.0M | 24 MB | PG ModelVersion |
| models.csv | 822K | 12 MB | PG Model |
| metrics.csv | ~107M | 1.3 GB | ClickHouse |
| cursor.txt | 1 | 10 B | Outbox ID at dump time |

## Deploy Skill

For automated deployments, use the deploy skill CLI:
```bash
node .claude/skills/deploy/cli.mjs <command>
```

Commands: `release`, `watch-build`, `rollout`, `status`, `scale`, `cursor-reset`, `wipe`, `pg-sync-health`, `pg-sync-logs`, `resources`
