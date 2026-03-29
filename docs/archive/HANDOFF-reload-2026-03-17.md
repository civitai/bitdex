# Reload Handoff — 2026-03-17

## What's Done

- **v1.0.57 deployed** — includes all fixes: publishedAt doc serving, PATCH→PUT fallback, CollectionItem i32, filter_sync skip non-alive, single_pass publishedAt target name, auto-backfill removed from pg-sync, PUT /cursors admin endpoint
- **Docker image:** `ghcr.io/civitai/bitdex:1.0.57`
- **Safety cursor** set in PG (`safety-hold` with `last_outbox_id=0`) — prevents outbox cleanup
- **collectionIds** added to K8s configmap (`bitdex-index-config`) by Arabella
- **Deploy skill** built at `.claude/skills/deploy/cli.mjs` and reload script at `.claude/skills/deploy/reload.mjs` (reload script needs quoting fixes)
- **Bulk load guide** updated at `docs/bulk-load-handoff.md`
- **Flux suspend** pushed by Arabella (commit `9932a721` on trunk) but Flux git source controller is stuck on old commit `01389418`. Needs manual intervention.

## What's NOT Done

The system needs a **full reload from fresh CSVs**. The current data on the PVCs is from old CSVs (Mar 15). We need fresh dumps to close the data gap.

## Blocking Issue: Flux

Flux keeps bringing pods back to 2 replicas. Arabella pushed `suspend: true` to `clusters/production/flux-system/apps/bitdex/bitdex.yaml` but the Flux git source controller hasn't pulled the new commit. Tried `kubectl annotate gitrepository` to force reconcile — didn't work.

**Fix options:**
1. Restart the Flux source-controller pod: `kubectl rollout restart deployment/source-controller -n flux-system`
2. Or manually edit the git source to bump the interval
3. Or just scale the source-controller to 0 and back to 1

Once Flux pulls commit `9932a721`, it will suspend the bitdex kustomization and stop fighting scale-to-0.

## Reload Steps (once Flux is suspended)

### 1. Verify Flux suspended + scale to 0
```bash
kubectl get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}'
# Must show: true

kubectl scale statefulset/bitdex -n bitdex --replicas=0
kubectl delete pod bitdex-0 bitdex-1 -n bitdex --force --grace-period=0
# Wait until: kubectl get pods -n bitdex shows 0 bitdex pods
# Verify it STAYS at 0 for 30+ seconds
```

### 2. Wipe both PVCs
Mount busybox on each PVC and delete:
- `bitmaps/`, `docs/`, `bounds/`, `slot_arena.bin`, `snapshot.meta`
- `load_stage/*` (old CSVs)

Keep `config.json` and empty `load_stage/` directory.

### 3. Dump fresh CSVs on PG pod
Remote into `cnpg-cluster-nvme0-1` in `cnpg-database` namespace. The exact COPY queries from the codebase (`src/pg_sync/copy_queries.rs`):

```sql
SET statement_timeout = 0;

-- images (11 cols)
COPY (SELECT id, url, "nsfwLevel", hash, flags, type::text, "userId", "blockedFor",
      extract(epoch from "scannedAt")::bigint, extract(epoch from "createdAt")::bigint, "postId"
      FROM "Image")
TO '/var/lib/postgresql/data/bitdex_dump/images.csv' CSV;

-- posts (4 cols)
COPY (SELECT id, extract(epoch from "publishedAt")::bigint, availability::text, "modelVersionId"
      FROM "Post")
TO '/var/lib/postgresql/data/bitdex_dump/posts.csv' CSV;

-- tags
COPY (SELECT "tagId", "imageId" FROM "TagsOnImageDetails" WHERE disabled = false)
TO '/var/lib/postgresql/data/bitdex_dump/tags.csv' CSV;

-- tools
COPY (SELECT "toolId", "imageId" FROM "ImageTool")
TO '/var/lib/postgresql/data/bitdex_dump/tools.csv' CSV;

-- techniques
COPY (SELECT "techniqueId", "imageId" FROM "ImageTechnique")
TO '/var/lib/postgresql/data/bitdex_dump/techniques.csv' CSV;

-- resources
COPY (SELECT "imageId", "modelVersionId", detected FROM "ImageResourceNew")
TO '/var/lib/postgresql/data/bitdex_dump/resources.csv' CSV;

-- model_versions
COPY (SELECT id, "baseModel", "modelId" FROM "ModelVersion")
TO '/var/lib/postgresql/data/bitdex_dump/model_versions.csv' CSV;

-- models
COPY (SELECT id, poi, type::text FROM "Model")
TO '/var/lib/postgresql/data/bitdex_dump/models.csv' CSV;

-- collection_items
COPY (SELECT "collectionId", "imageId" FROM "CollectionItem"
      WHERE "imageId" IS NOT NULL AND status = 'ACCEPTED')
TO '/var/lib/postgresql/data/bitdex_dump/collection_items.csv' CSV;
```

**AFTER all dumps complete**, record the outbox head:
```sql
SELECT MAX(id) FROM "BitdexOutbox";
-- Save this number as cursor.txt
```
Write it: `echo -n <VALUE> > /var/lib/postgresql/data/bitdex_dump/cursor.txt`

### 4. Transfer CSVs to both PVCs
There should be a download server running on the PG pod from previous sessions. Copy all CSV files + cursor.txt to `/data/indexes/civitai/load_stage/` on both PVCs. Create `.done` markers for each CSV.

### 5. Run bulk load jobs
```bash
kubectl set image cronjob/bitdex-bulk-load -n bitdex "*=ghcr.io/civitai/bitdex:1.0.57"

# Job for PVC-0
kubectl create job -n bitdex reload-0 --from=cronjob/bitdex-bulk-load

# Job for PVC-1 (modify PVC claim + replica ID)
# See the node script pattern in .claude/skills/deploy/reload.mjs step5_load()
```

Wait for both to complete (~10 min). Verify `collectionIds` fpack files exist on both PVCs.

### 6. Reset cursors (CRITICAL)
**Pods MUST be at 0 for this.** The bulk loader seeds cursors at the current outbox head, NOT at the CSV dump time. We must overwrite them.

Mount busybox on both PVCs:
```bash
# Read the correct cursor from load_stage/cursor.txt
cat /data/indexes/civitai/load_stage/cursor.txt

# Write to cursor files
echo -n <VALUE> > /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-0  # on PVC-0
echo -n <VALUE> > /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-1  # on PVC-1

# Verify
cat /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-0
cat /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-1
```

Also update PG:
```sql
UPDATE bitdex_cursors SET last_outbox_id = <VALUE>
WHERE replica_id IN ('pg-sync-bitdex-0', 'pg-sync-bitdex-1');
```

### 7. Scale up + verify
```bash
kubectl scale statefulset/bitdex -n bitdex --replicas=2
kubectl rollout status statefulset/bitdex -n bitdex --timeout=300s

# Verify cursors
kubectl logs -n bitdex bitdex-0 -c pg-sync | grep starting_cursor
kubectl logs -n bitdex bitdex-1 -c pg-sync | grep starting_cursor
# Both should show the cursor value from step 6

# Verify sync is processing (wait 60s, check cursor advanced)
# Verify publishedAt is correct:
# Port-forward + query image 58189320 — publishedAt should be ~1487255090
```

### 8. Cleanup
```bash
# Remove safety cursor from PG
DELETE FROM bitdex_cursors WHERE replica_id = 'safety-hold';

# Tell Arabella to remove suspend:true from talos-infra
# Unsuspend Flux (after Arabella pushes):
kubectl patch kustomization bitdex -n flux-system --type=json \
  -p='[{"op":"replace","path":"/spec/suspend","value":false}]'

# Delete dump files from PG pod
kubectl exec -n cnpg-database cnpg-cluster-nvme0-1 -- rm -rf /var/lib/postgresql/data/bitdex_dump

# Take down the download server on PG pod if still running
```

### 9. Notify
- **Donovan:** re-enable shadow mode on model-share
- **Justin:** system live, verified, sync catching up — message via Discord
- **Adam:** monitoring can resume

## Key Pitfalls (from this session)

1. **Flux fights scale-to-0** — must be suspended via git push, not kubectl patch (Flux reconciles from git and resets patches)
2. **Bulk loader seeds wrong cursor** — seeds at current outbox head, not CSV dump time. MUST overwrite after load completes.
3. **Engine checkpoint overwrites cursor files** — the merge thread writes the in-memory cursor to disk periodically. Must write cursor files while NO server pods are running.
4. **Outbox cleanup deletes old rows** — use safety-hold cursor to prevent cleanup while reloading
5. **PVC contention** — pods and jobs on same node can both mount ReadWriteOnce PVCs. Ensure no server pods running during load jobs.
6. **Config.json** — the bulk loader creates it from the K8s configmap. Don't manually overwrite it.

## Files Created This Session

- `.claude/skills/deploy/cli.mjs` — deploy CLI (status, rollout, pg-sync-health, etc.)
- `.claude/skills/deploy/skill.md` — deploy skill documentation
- `.claude/skills/deploy/reload.mjs` — reload script (needs quoting fixes for pgExec)
- `docs/bulk-load-handoff.md` — updated bulk load guide
- `docs/reload-checklist-2026-03-17.md` — detailed checklist
- `tests/e2e/e2e-schema-mapping.mjs` — E2E schema mapping test

## Releases This Session

v1.0.48 → v1.0.49 → v1.0.50 → v1.0.51 → v1.0.52 → v1.0.53 → v1.0.54 → v1.0.55 → v1.0.56 → v1.0.57
