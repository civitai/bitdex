# Full Reload Checklist — 2026-03-17

## Prerequisites
- [ ] Flux suspended: `kubectl patch kustomization bitdex -n flux-system --type=json -p='[{"op":"replace","path":"/spec/suspend","value":true}]'`
- [ ] Both pods scaled to 0 and fully terminated

## Phase 1: Set safety cursor + clear old data

1. **Set a safety cursor in PG** so outbox cleanup doesn't purge rows we need:
   - [ ] Create a fake cursor entry with a very low ID to prevent cleanup:
     ```sql
     INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at)
     VALUES ('safety-hold', 0, now())
     ON CONFLICT (replica_id)
     DO UPDATE SET last_outbox_id = 0, updated_at = now();
     ```
   - [ ] Record the current outbox head for reference:
     ```sql
     SELECT MAX(id) FROM "BitdexOutbox";
     ```

2. **Clear old CSVs from both PVCs:**
   - [ ] Mount busybox on PVC-0, delete load_stage/*
   - [ ] Mount busybox on PVC-1, delete load_stage/*
   - [ ] Also wipe bitmaps/docs/bounds/snapshot.meta on both PVCs

3. **Clear cursors on both PVCs:**
   - [ ] Delete `bitmaps/cursors/pg-sync-bitdex-0` on PVC-0
   - [ ] Delete `bitmaps/cursors/pg-sync-bitdex-1` on PVC-1

## Phase 2: Dump fresh CSVs from PG

4. **SSH into PG pod and run COPY TO FILE for all tables:**
   - [ ] `kubectl exec -n cnpg-database cnpg-cluster-nvme0-1 -- psql -U postgres -d civitai`
   - [ ] Dump all tables to `/var/lib/postgresql/data/bitdex_dump/`
   - [ ] Record the outbox head AFTER the dump completes (this is our target cursor)
   - [ ] Save cursor value to a file: `echo -n <CURSOR> > /var/lib/postgresql/data/bitdex_dump/cursor.txt`

5. **Transfer CSVs to both PVCs:**
   - [ ] Use kubectl cp or a transfer pod to copy from PG pod to PVC-0 load_stage/
   - [ ] Copy same files to PVC-1 load_stage/
   - [ ] Create .done markers for each file on both PVCs
   - [ ] Copy cursor.txt to both PVCs

## Phase 3: Run bulk load

6. **Update CronJob image to v1.0.57:**
   - [ ] `kubectl set image cronjob/bitdex-bulk-load -n bitdex "*=ghcr.io/civitai/bitdex:1.0.57"`

7. **Create load jobs with unique names:**
   - [ ] Job for PVC-0: `kubectl create job -n bitdex load-final-0 --from=cronjob/bitdex-bulk-load`
   - [ ] Job for PVC-1: (modified to use data-bitdex-1)
   - [ ] Wait for both to complete (~10 min)
   - [ ] Verify collectionIds fpacks exist on both PVCs

## Phase 4: Reset cursors

8. **Stop everything — scale to 0, wait for full termination**

9. **Mount busybox on both PVCs:**
   - [ ] Read cursor.txt from load_stage (the value we saved in step 4)
   - [ ] Write that value to `bitmaps/cursors/pg-sync-bitdex-0` on PVC-0
   - [ ] Write that value to `bitmaps/cursors/pg-sync-bitdex-1` on PVC-1
   - [ ] Verify both files contain the correct value

10. **Update PG cursors:**
    - [ ] `UPDATE bitdex_cursors SET last_outbox_id = <CURSOR> WHERE replica_id IN ('pg-sync-bitdex-0', 'pg-sync-bitdex-1');`

## Phase 5: Bring up + verify

11. **Scale to 2, wait for rollout**

12. **Verify cursors started correctly:**
    - [ ] `kubectl logs -n bitdex bitdex-0 -c pg-sync | grep starting_cursor`
    - [ ] `kubectl logs -n bitdex bitdex-1 -c pg-sync | grep starting_cursor`
    - [ ] Both should show the cursor value from step 4

13. **Verify sync is processing:**
    - [ ] Wait 30s, check cursor has advanced
    - [ ] Wait another 30s, check it advanced further
    - [ ] Confirm rate is realistic (~5K entries per 2s batch)

14. **Verify data correctness:**
    - [ ] publishedAt is correct (port-forward + query)
    - [ ] collectionIds fpacks exist
    - [ ] No errors in pg-sync logs (except transient connection resets during startup)

## Phase 6: Cleanup + notify

15. **Remove safety cursor from PG:**
    - [ ] `DELETE FROM bitdex_cursors WHERE replica_id = 'safety-hold';`

16. **Unsuspend Flux:**
    - [ ] `kubectl patch kustomization bitdex -n flux-system --type=json -p='[{"op":"replace","path":"/spec/suspend","value":false}]'`

17. **Notify team:**
    - [ ] Message Donovan: re-enable shadow mode
    - [ ] Message Justin via Discord: system live + verified
    - [ ] Message Adam: monitoring can resume

## Notes
- The safety cursor (`safety-hold` with last_outbox_id=0) prevents the cleanup trigger from deleting outbox rows. Remove it AFTER sync is caught up.
- Fresh CSVs ensure no data gap — the cursor matches exactly when the CSVs were dumped.
- The bulk load seeds its own cursor at the outbox head, but we override it in Phase 4 with the value from when CSVs were actually dumped.
