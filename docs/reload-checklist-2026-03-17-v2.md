# Full Reload Checklist — 2026-03-17 v2

Complete reload procedure for both BitDex replicas. Dumps fresh CSVs from PG once, copies to both PVCs, loads both, then resets cursors to a known safe point.

---

## Phase 0: Pre-flight

- [ ] **Verify Flux is suspended** in the `bitdex` namespace (check with Arabella / talos-infra)
  ```bash
  kubectl get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}'
  # Must print: true
  ```
  If not suspended:
  ```bash
  kubectl patch kustomization bitdex -n flux-system \
    --type=json -p='[{"op":"replace","path":"/spec/suspend","value":true}]'
  ```
  Wait 30s and verify no pods come back after scaling down.

---

## Phase 1: Safety cursor + scale down

1. **Set a safety cursor in PG** to prevent outbox cleanup from deleting rows we need:
   - [ ] SSH into PG:
     ```bash
     MSYS_NO_PATHCONV=1 kubectl exec -it -n cnpg-database cnpg-cluster-nvme0-1 -- psql -U postgres -d civitai
     ```
   - [ ] Insert safety cursor:
     ```sql
     INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at)
     VALUES ('safety-hold', 0, now())
     ON CONFLICT (replica_id)
     DO UPDATE SET last_outbox_id = 0, updated_at = now();
     ```
   - [ ] Record the current outbox head (write this down — `CURSOR_VALUE`):
     ```sql
     SELECT MAX(id) FROM "BitdexOutbox";
     ```
   - [ ] **CURSOR_VALUE = ____________**

2. **Scale both pods to 0:**
   - [ ] Scale down:
     ```bash
     kubectl scale statefulset/bitdex -n bitdex --replicas=0
     ```
   - [ ] Wait for termination:
     ```bash
     kubectl wait --for=delete pod/bitdex-0 pod/bitdex-1 -n bitdex --timeout=120s
     ```
   - [ ] Verify no pods running:
     ```bash
     kubectl get pods -n bitdex
     ```

---

## Phase 2: Clean both PVCs

3. **Mount busybox on both PVCs and remove CSVs + old data:**
   - [ ] For each PVC (i = 0, 1):
     ```bash
     for i in 0 1; do
       kubectl run clean-$i -n bitdex --image=busybox \
         --overrides='{"spec":{"containers":[{"name":"c","image":"busybox","command":["sh","-c","rm -rf /data/indexes/civitai/bitmaps /data/indexes/civitai/docs /data/indexes/civitai/bounds /data/indexes/civitai/slot_arena.bin /data/indexes/civitai/snapshot.meta && rm -rf /data/indexes/civitai/load_stage/* && echo wiped-'$i'"],"volumeMounts":[{"name":"d","mountPath":"/data"}]}],"volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"data-bitdex-'$i'"}}],"nodeSelector":{"kubernetes.io/hostname":"talos-fq9-f3k"}}}' \
         --restart=Never
     done
     ```
   - [ ] Wait + verify:
     ```bash
     kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/clean-0 pod/clean-1 -n bitdex --timeout=60s
     kubectl logs -n bitdex clean-0
     kubectl logs -n bitdex clean-1
     ```
   - [ ] Clean up pods:
     ```bash
     kubectl delete pod -n bitdex clean-0 clean-1
     ```

---

## Phase 3: Dump fresh CSVs from Postgres

4. **Exec into the PG replica pod:**
   - [ ] Connect:
     ```bash
     MSYS_NO_PATHCONV=1 kubectl exec -it -n cnpg-database cnpg-cluster-nvme0-1 -- bash
     ```
   - [ ] Create dump directory:
     ```bash
     mkdir -p /var/lib/postgresql/data/bitdex_dump && cd /var/lib/postgresql/data/bitdex_dump
     ```

5. **Dump all tables** (run in order, smallest first — biggest last):
   - [ ] Models (~12 MB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, poi, type::text FROM \"Model\") TO STDOUT WITH CSV" > models.csv
     ```
   - [ ] Model versions (~24 MB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, \"baseModel\", \"modelId\" FROM \"ModelVersion\") TO STDOUT WITH CSV" > model_versions.csv
     ```
   - [ ] Tools (~50 MB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"toolId\", \"imageId\" FROM \"ImageTool\") TO STDOUT WITH CSV" > tools.csv
     ```
   - [ ] Techniques (~71 MB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"techniqueId\", \"imageId\" FROM \"ImageTechnique\") TO STDOUT WITH CSV" > techniques.csv
     ```
   - [ ] Collection items (~2 GB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"collectionId\", \"imageId\" FROM \"CollectionItem\" WHERE \"imageId\" IS NOT NULL AND status = 'ACCEPTED') TO STDOUT WITH CSV" > collection_items.csv
     ```
   - [ ] Posts (~610 MB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, extract(epoch from \"publishedAt\")::bigint, availability::text, \"modelVersionId\" FROM \"Post\") TO STDOUT WITH CSV" > posts.csv
     ```
   - [ ] Resources (~777 MB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"imageId\", \"modelVersionId\", detected FROM \"ImageResourceNew\") TO STDOUT WITH CSV" > resources.csv
     ```
   - [ ] Images (~14 GB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT id, url, \"nsfwLevel\", hash, flags, type::text, \"userId\", \"blockedFor\", extract(epoch from \"scannedAt\")::bigint, extract(epoch from \"createdAt\")::bigint, \"postId\" FROM \"Image\") TO STDOUT WITH CSV" > images.csv
     ```
   - [ ] Tags (~63 GB):
     ```bash
     psql -U postgres -d civitai -c "SET statement_timeout = 0; COPY (SELECT \"tagId\", \"imageId\" FROM \"TagsOnImageDetails\" WHERE disabled = false) TO STDOUT WITH CSV" > tags.csv
     ```

6. **Save the cursor value** (the `CURSOR_VALUE` from Phase 1):
   - [ ] Write cursor file:
     ```bash
     echo -n <CURSOR_VALUE> > /var/lib/postgresql/data/bitdex_dump/cursor.txt
     ```
   - [ ] Verify:
     ```bash
     cat /var/lib/postgresql/data/bitdex_dump/cursor.txt
     ls -lh /var/lib/postgresql/data/bitdex_dump/
     ```

---

## Phase 4: Copy CSVs to both PVCs

7. **Start a download server on the PG pod** (if not already running):
   - [ ] In the PG pod:
     ```bash
     cd /var/lib/postgresql/data/bitdex_dump && python3 -m http.server 8080 &
     ```
   - [ ] Get the PG pod IP:
     ```bash
     kubectl get pod -n cnpg-database cnpg-cluster-nvme0-1 -o jsonpath='{.status.podIP}'
     ```
   - [ ] **PG_POD_IP = ____________**

8. **Mount busybox on both PVCs and download CSVs:**
   - [ ] Mount pods:
     ```bash
     for i in 0 1; do
       kubectl run xfer-$i -n bitdex --image=busybox \
         --overrides='{"spec":{"containers":[{"name":"x","image":"busybox","command":["sh","-c","mkdir -p /data/indexes/civitai/load_stage && sleep 7200"],"volumeMounts":[{"name":"d","mountPath":"/data"}]}],"volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"data-bitdex-'$i'"}}],"nodeSelector":{"kubernetes.io/hostname":"talos-fq9-f3k"}}}' \
         --restart=Never
     done
     ```
   - [ ] Wait for pods:
     ```bash
     kubectl wait --for=condition=Ready pod/xfer-0 pod/xfer-1 -n bitdex --timeout=60s
     ```
   - [ ] Download CSVs to each PVC (busybox has wget):
     ```bash
     PG_IP=<PG_POD_IP>
     for i in 0 1; do
       for f in models.csv model_versions.csv tools.csv techniques.csv collection_items.csv posts.csv resources.csv images.csv tags.csv cursor.txt; do
         echo "Downloading $f to PVC-$i..."
         kubectl exec -n bitdex xfer-$i -- wget -q -O /data/indexes/civitai/load_stage/$f http://$PG_IP:8080/$f
       done
     done
     ```
   - [ ] Create `.done` markers:
     ```bash
     for i in 0 1; do
       for f in models.csv model_versions.csv tools.csv techniques.csv collection_items.csv posts.csv resources.csv images.csv tags.csv; do
         kubectl exec -n bitdex xfer-$i -- sh -c "echo ok > /data/indexes/civitai/load_stage/$f.done"
       done
     done
     ```
   - [ ] Verify files on both PVCs:
     ```bash
     for i in 0 1; do
       echo "=== PVC-$i ==="
       kubectl exec -n bitdex xfer-$i -- ls -lh /data/indexes/civitai/load_stage/
     done
     ```
   - [ ] Clean up transfer pods:
     ```bash
     kubectl delete pod -n bitdex xfer-0 xfer-1 --force --grace-period=0
     ```

---

## Phase 5: Run both PG loaders

9. **Start bulk load jobs on both PVCs:**
   - [ ] Delete any old jobs:
     ```bash
     kubectl delete job -n bitdex fresh-load-0 fresh-load-1 2>/dev/null
     ```
   - [ ] Create job for PVC-0:
     ```bash
     kubectl create job -n bitdex fresh-load-0 --from=cronjob/bitdex-bulk-load
     ```
   - [ ] Create job for PVC-1 (modified PVC claim):
     ```bash
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
         metadata:{name:'fresh-load-1',namespace:'bitdex'},spec}));
     })" | kubectl apply -f -
     ```
   - [ ] Monitor progress (~10 min):
     ```bash
     kubectl get jobs -n bitdex -w
     kubectl logs -n bitdex -f job/fresh-load-0
     kubectl logs -n bitdex -f job/fresh-load-1
     ```
   - [ ] Wait for completion:
     ```bash
     kubectl wait --for=condition=Complete job/fresh-load-0 job/fresh-load-1 -n bitdex --timeout=900s
     ```

---

## Phase 6: Post-load cursor reset (safety measure)

After loading completes, both loaders will have set their own cursors. We override them with the known-safe cursor value from Phase 1 to ensure no gap.

10. **Scale to 0** (if load jobs haven't already terminated the pods):
    - [ ] Verify no bitdex server pods running:
      ```bash
      kubectl get pods -n bitdex | grep -E '^bitdex-[01]'
      ```

11. **Mount busybox on both PVCs and reset cursors:**
    - [ ] Mount:
      ```bash
      for i in 0 1; do
        kubectl run cs-$i -n bitdex --image=busybox \
          --overrides='{"spec":{"containers":[{"name":"c","image":"busybox","command":["sh","-c","mkdir -p /data/indexes/civitai/bitmaps/cursors && echo -n <CURSOR_VALUE> > /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-'$i' && echo set-'$i' && cat /data/indexes/civitai/bitmaps/cursors/pg-sync-bitdex-'$i'"],"volumeMounts":[{"name":"d","mountPath":"/data"}]}],"volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"data-bitdex-'$i'"}}],"nodeSelector":{"kubernetes.io/hostname":"talos-fq9-f3k"}}}' \
          --restart=Never
      done
      ```
    - [ ] Verify:
      ```bash
      kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/cs-0 pod/cs-1 -n bitdex --timeout=30s
      kubectl logs -n bitdex cs-0
      kubectl logs -n bitdex cs-1
      ```
    - [ ] Both should print the CURSOR_VALUE.
    - [ ] Clean up:
      ```bash
      kubectl delete pod -n bitdex cs-0 cs-1
      ```

12. **Update PG cursors to match:**
    - [ ] Set both replica cursors in PG:
      ```sql
      UPDATE bitdex_cursors
      SET last_outbox_id = <CURSOR_VALUE>, updated_at = now()
      WHERE replica_id IN ('pg-sync-bitdex-0', 'pg-sync-bitdex-1');
      ```
    - [ ] Verify:
      ```sql
      SELECT * FROM bitdex_cursors;
      ```

---

## Phase 7: Start + verify

13. **Scale to 2:**
    - [ ] Start pods:
      ```bash
      kubectl scale statefulset/bitdex -n bitdex --replicas=2
      kubectl rollout status statefulset/bitdex -n bitdex --timeout=300s
      ```

14. **Verify cursors started correctly:**
    - [ ] Check both pg-sync sidecars:
      ```bash
      kubectl logs -n bitdex bitdex-0 -c pg-sync | grep starting_cursor
      kubectl logs -n bitdex bitdex-1 -c pg-sync | grep starting_cursor
      ```
    - [ ] Both should show the CURSOR_VALUE from Phase 1.

15. **Verify sync is processing:**
    - [ ] Wait 30s, check cursor has advanced:
      ```bash
      sleep 30
      kubectl logs -n bitdex bitdex-0 -c pg-sync --tail=10 | grep cursor
      kubectl logs -n bitdex bitdex-1 -c pg-sync --tail=10 | grep cursor
      ```
    - [ ] Wait another 30s, check it advanced further.
    - [ ] Confirm rate is realistic (~5K entries per 2s batch).

16. **Verify data correctness:**
    - [ ] Port forward and query:
      ```bash
      kubectl port-forward -n bitdex bitdex-0 3099:3000 &
      curl -s 'http://localhost:3099/api/indexes/civitai/query?format=compact' \
        -H 'Content-Type: application/json' \
        -d '{"filter":{"isPublished":true},"sort":"publishedAt","limit":3,"include_docs":true}'
      ```
    - [ ] publishedAt should be nonzero for published images
    - [ ] No errors in pg-sync logs (transient connection resets during startup are normal)

---

## Phase 8: Cleanup + notify

17. **DO NOT remove safety cursor yet.** The safety cursor stays until Justin explicitly gives the go-ahead. It protects outbox rows from cleanup and costs nothing to keep.
    - [ ] **WAIT for Justin's explicit approval** before running:
      ```sql
      -- ONLY run this when Justin says to:
      DELETE FROM bitdex_cursors WHERE replica_id = 'safety-hold';
      ```

18. **Unsuspend Flux** (coordinate with Arabella):
    - [ ] Unsuspend:
      ```bash
      kubectl patch kustomization bitdex -n flux-system \
        --type=json -p='[{"op":"replace","path":"/spec/suspend","value":false}]'
      ```

19. **Clean up dump files from PG pod:**
    - [ ] Delete dump dir:
      ```bash
      MSYS_NO_PATHCONV=1 kubectl exec -n cnpg-database cnpg-cluster-nvme0-1 -- rm -rf /var/lib/postgresql/data/bitdex_dump
      ```

20. **Clean up old jobs + lingering pods:**
    - [ ] Delete jobs:
      ```bash
      kubectl delete job -n bitdex fresh-load-0 fresh-load-1 2>/dev/null
      ```
    - [ ] Delete any lingering helper pods:
      ```bash
      for p in clean xfer cs verify wipe cr; do
        kubectl delete pod -n bitdex ${p}-0 ${p}-1 --force --grace-period=0 2>/dev/null
      done
      ```

21. **Notify team:**
    - [ ] Message Donovan: system is live, re-enable shadow mode comparison
    - [ ] Message Justin on Discord: reload complete, steady state confirmed
    - [ ] Stop the download server on PG pod (kill the python3 process)

---

## Notes

- **DO NOT remove the safety cursor** (`safety-hold`) without Justin's explicit go-ahead. It prevents the PG cleanup trigger from deleting outbox rows. The trigger deletes rows where `id < MIN(last_outbox_id)` across all cursors. With `safety-hold=0`, nothing gets deleted. It costs nothing to keep and provides a recovery safety net.
- **CURSOR_VALUE** should be captured BEFORE the CSV dump starts. The CSVs reflect the database state at that point, so the sync sidecar must start catching up from there.
- The bulk loader seeds its own cursor at the current outbox head, which is WRONG — it's ahead of where the CSVs were dumped. That's why Phase 6 overrides it.
- Both PVCs are on the same node (`talos-fq9-f3k`), so busybox pods can mount either one. Use `nodeSelector` to ensure scheduling.
- The download server approach (`python3 -m http.server`) is the fastest way to copy files between pods on the same cluster. Transfer rate ~240 MB/s.
- **Do NOT wipe config.json** — the bulk load job creates it from the K8s configmap.
- ClickHouse metrics are handled automatically by the bulk loader if env vars are set in `bitdex-secrets`.
