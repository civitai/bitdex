# BitDex Deploy: Soft Nuke vs Hard Nuke

When a code change invalidates on-disk bitmap or docstore layout, or when steady-state ops have produced incorrect bitmaps that incremental repair won't fix, the pod has to start from a known-clean state. Two recovery paths exist, distinguished by **how much state is wiped and re-derived**.

This is the canonical reference. Pre-flight, commands, what each preserves, when to choose which, and how to recover from a botched run.

> **Sync v2 changed the hard-nuke flow.** The old 9-step `reload.mjs` orchestration manually drove dump → transfer → load → cursor-reset. As of bitdex-sync v2, the sidecar's `run_setup_v2` + boot dump pipeline (`src/bin/pg_sync.rs:282`) does all of that autonomously when the pod comes up against a clean PVC. The host-side orchestration is now a thin wrapper: scale down → reset PG state → wipe PVC → scale up → wait. See §Hard nuke below.

---

## TL;DR

| | Soft Nuke | Hard Nuke |
|---|---|---|
| **Wipes bitmaps + docstore + bounds + meta on PVC** | yes | yes |
| **Wipes CSVs in `load_stage`** | no | yes |
| **Drops `bitdex_*` triggers + truncates `BitdexOps` + `bitdex_cursors`** | no | yes |
| **Re-dumps from Postgres** | no | yes (driven by bitdex-sync sidecar) |
| **Time to recover** | 5–15 min (load only) | 60–90 min (dump + transfer + load via sidecar) |
| **Use when** | layout/format change, in-mem schema migration, recover from corrupt bitmaps but CSVs still match prod | Postgres schema/trigger config changed, steady-state ops produced incorrect bitmaps, cursor too far behind for catch-up, fully fresh slate for benchmarking |

---

## Layout reminder

Production runs a single-replica StatefulSet on `talos-wjh-tgy` with `data-bitdex-0` (RWO, openebs-hostpath, locally pinned). PVC mounted at `/data` inside the pod. Inside `/data/indexes/civitai/`:

```
bitmaps/        roaring index shards     ← always wiped
docs/           DocStore V2 / silos       ← always wiped
bounds/         bound cache shards        ← always wiped
slot_arena.bin  slot ID arena             ← always wiped
snapshot.meta   gen pin metadata          ← always wiped
load_stage/     CSV dump output            ← KEPT in soft, WIPED in hard
  ├ images.csv
  ├ tags.csv
  ├ resources.csv
  └ ...
```

PG-side state (only relevant to hard nuke):
- `bitdex_*` triggers + their functions on `Image`, `Post`, `ImageResourceNew`, `ModelVersion`, `Model`, etc.
- `BitdexOps` outbox table (rows accumulated from triggers; consumed by `bitdex-sync` ops poller).
- `bitdex_cursors` table — replica row-id checkpoints.

> **Note:** the older two-replica layout (`data-bitdex-1`) is documented in archived runbooks. Only `data-bitdex-0` is bound and used in current prod. References to `bitdex-1` / `data-bitdex-1` in older docs are stale.

---

## Pre-flight (both flavors)

Before doing anything destructive:

1. **Confirm shadow is OFF (durable, via flipt-state git repo)**
   ```bash
   node .claude/skills/flipt/flipt.mjs shadow off
   node .claude/skills/flipt/flipt.mjs get bitdex-image-search   # confirm
   ```
   The `bitdex-image-search` flag in `civitai-app/default/features.yaml` must be `enabled: false`. Otherwise mirrored prod traffic from model-share will hit the wiped/loading pod and emit divergence storms.

2. **Suspend Flux (durable, via talos-infra git repo — not `kubectl patch`)**
   The live `flux suspend kustomization bitdex` (or `kubectl patch`) does NOT stick. Flux reconciles the live Kustomization back to whatever is in git within ~5 min. Edit `clusters/production/flux-system/apps/bitdex/bitdex.yaml`:
   ```yaml
   spec:
     suspend: true
   ```
   Commit + push, then verify:
   ```bash
   kubectl --context civit-datapacket get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}'
   # must print: true
   ```

3. **Verify pod is on the expected node**
   ```bash
   kubectl --context civit-datapacket get pv $(kubectl --context civit-datapacket -n bitdex get pvc data-bitdex-0 -o jsonpath='{.spec.volumeName}') -o jsonpath='{.spec.nodeAffinity}'
   ```
   Should reference `talos-wjh-tgy`. PVC is hard-pinned via NodeAffinity; the pod can't move without PVC migration.

4. **Note current image tag** for awareness (the rollback-to-old-image path is mostly theoretical post-wipe — see Caution).

5. **Confirm pre-flight skill output:**
   ```bash
   node .claude/skills/deploy/reload.mjs preflight
   ```

---

## Soft nuke

Wipes bitmaps + docstore + bounds + slot arena + snapshot meta. **Keeps `load_stage/*.csv`.** The pod boots, finds CSVs in `load_stage`, runs the bitdex-sync boot pipeline against them, skips the PG dump phase (CSVs already present), and bulk-loads.

```bash
node .claude/skills/deploy/cli.mjs wipe
```

What that runs internally (ephemeral pod, hostpath mount on PVC 0):

```bash
rm -rf /data/indexes/civitai/bitmaps \
       /data/indexes/civitai/docs \
       /data/indexes/civitai/bounds \
       /data/indexes/civitai/slot_arena.bin \
       /data/indexes/civitai/snapshot.meta
```

Then:

1. **Scale back up:**
   ```bash
   kubectl --context civit-datapacket -n bitdex scale statefulset bitdex --replicas=1
   ```
2. **Pod boots → bitdex-sync sidecar runs `run_boot_sequence` → finds existing CSVs in `load_stage` → loads bitmaps + docstore from scratch.** ~5–15 min.
3. **Watch logs:**
   ```bash
   kubectl --context civit-datapacket -n bitdex logs -f bitdex-0 -c pg-sync
   kubectl --context civit-datapacket -n bitdex logs -f bitdex-0 -c bitdex
   ```
4. **Resume Flux** by reverting the talos-infra `suspend: true` commit when the load looks healthy.

When to use:
- Bitmap shard format changed but the CSV schema didn't
- DocStore layout migration (e.g. V2 → V3 silos)
- Corrupt `snapshot.meta` or partial flush you can't recover
- You want to re-test bulk-load timing without re-dumping

When NOT to use:
- Postgres schema added/removed columns since the last dump (CSVs are stale)
- The trigger config in `config/sync-civitai.yaml` changed (PG-side state needs rebuild)
- Steady-state ops produced incorrect bitmaps (see v1.0.201 example below)

---

## Hard nuke

Wipes everything soft nuke wipes, **plus** `load_stage/*.csv`, **plus** PG-side state (triggers + BitdexOps + bitdex_cursors). Then bitdex-sync's autonomous boot drives re-install of triggers, fresh dump, transfer, and load.

The orchestration is now 6 steps. Each step is verifiable on its own. **Run them sequentially** — don't pipe them.

```bash
node .claude/skills/deploy/reload.mjs preflight     # verify shadow OFF, Flux suspended
node .claude/skills/deploy/reload.mjs suspend       # scale StatefulSet to 0
node .claude/skills/deploy/reload.mjs nuke-pg       # drop triggers + truncate BitdexOps/bitdex_cursors
node .claude/skills/deploy/reload.mjs wipe          # wipe bitmaps/docs/bounds/load_stage on PVC
node .claude/skills/deploy/reload.mjs start         # scale up — bitdex-sync drives the rest
node .claude/skills/deploy/reload.mjs monitor       # tail pg-sync logs until load completes
```

| # | Step | What it does |
|---|------|---|
| 1 | `preflight` | Verify shadow OFF, Flux suspended, note current image. Read-only — does not mutate cluster state. |
| 2 | `suspend` | Scale StatefulSet to 0, force-delete `bitdex-0` pod, verify pod count = 0. |
| 3 | `nuke-pg` | Run `sql/nuke-pg-state.sql` (pass 1, lock_timeout=5s) + `sql/nuke-pg-state-retry.sql` (pass 2, retry up to 8x) against the PG primary writer. Drops every `bitdex_*` trigger + function, truncates `BitdexOps` + `bitdex_cursors`. |
| 4 | `wipe` | Mount PVC via ephemeral busybox; `rm -rf` bitmaps/docs/bounds/slot_arena.bin/snapshot.meta + `load_stage/*`. |
| 5 | `start` | `scale --replicas=1`. The bitdex-sync sidecar runs `run_boot_sequence` autonomously: setup_v2 (re-installs triggers from sync config + creates `BitdexOps`/`bitdex_cursors`) → captures pre-dump cursor → streams CSVs from PG → registers each phase via `PUT /dumps` + `POST /dumps/{name}/loaded` → polls completion → seeds cursor → transitions to ops poller. |
| 6 | `monitor` | `kubectl logs -f bitdex-0 -c pg-sync`. Detach with Ctrl-C; load continues regardless. Re-run `monitor` to reattach. |

### Why no manual cursor-reset / dump / transfer / load steps?

bitdex-sync's boot sequence handles them:
- **Setup_v2** (`src/pg_sync/queries.rs:200-280`): generates expected trigger set from `config/sync-civitai.yaml` via `trigger_gen::generate_trigger_sql`, lists existing `bitdex_*` triggers (none — we just dropped them), drops "stale" ones (none), creates each fresh.
- **Pre-dump cursor capture** (`src/bin/pg_sync.rs:333`): reads `MAX(id) FROM "BitdexOps"` (will be 0 — we just truncated) and stashes it as the seed value.
- **Per-phase streaming** (`run_streaming_pipeline`): downloads CSV, registers dump, signals loaded, polls completion. Overlaps next phase's download with current phase's processing.
- **Cursor seed** (`upsert_cursor`): writes `pg-sync-bitdex-0` row at the captured pre-dump cursor.
- **Pollers** (ops_poller + metrics_poller): start consuming `BitdexOps` and ClickHouse metrics.

### PG-side reset details

The DO-loop in `nuke-pg-state.sql` uses `SET lock_timeout = '5s'` and per-statement `BEGIN ... EXCEPTION` blocks. Hot tables (`Image`, `ImageResourceNew`, `Post`, `ModelVersion`) hold AccessShareLock from civitai app traffic and frequently lose the deadlock race against `DROP TRIGGER`'s required AccessExclusiveLock. The script accepts those failures and surfaces a "remaining_triggers" count.

If pass 1 leaves stragglers, the retry script (`nuke-pg-state-retry.sql`) bumps `lock_timeout` to 10s and retries up to 8 times per trigger with `pg_sleep(2)` between attempts. **If 4 or fewer triggers remain after the retry pass, that's fine** — bitdex-sync's setup_v2 will drop them on boot since trigger-name hashes match the sync config (existing triggers are re-installed cleanly via `DROP IF EXISTS` + `CREATE`).

The `reload.mjs nuke-pg` step automates both passes and reports the final count. Direct CLI access:

```bash
node .claude/skills/deploy/cli.mjs nuke-pg
# or run the SQL files directly:
kubectl --context civit-datapacket exec -i -n cnpg-database cnpg-cluster-nvme0-3 -c postgres -- \
  psql -U postgres -d civitai < .claude/skills/deploy/sql/nuke-pg-state.sql
```

The writer pod name (`cnpg-cluster-nvme0-3` at time of writing) can shift across CNPG failovers. Verify before running:
```bash
kubectl --context civit-datapacket get pod -n cnpg-database -l role=primary
```
Override via `BITDEX_PG_WRITER_POD` env var if needed.

---

## Post-load (manual)

After `monitor` reports "All dump phases complete" and `alive_count` is climbing toward the expected total:

1. **Trigger docstore compaction** — the dump leaves docs uncompacted; the doc cache hit rate climbs once compact completes:
   ```bash
   kubectl --context civit-datapacket -n bitdex port-forward pod/bitdex-0 4099:3000 &
   curl -s -X POST -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" -H "Content-Type: application/json" \
     -d '{"targets":["docs"]}' \
     http://localhost:4099/api/indexes/civitai/compact
   # Returns 202 Accepted with task_id. Poll progress:
   curl -s -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
     http://localhost:4099/api/tasks/<task_id>
   ```
   Real example: docstore compact at 107M records ran 5m40s, scanned 249,663 shards, compacted 212,090, skipped 37,573 clean.

2. **Re-enable trace knobs** (don't persist across restarts):
   ```bash
   kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- bash -c '
     curl -sS -X PATCH -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" -H "Content-Type: application/json" \
       -d "{\"enable_traces\":true,\"trace_min_us\":500000,\"trace_buffer_size\":500}" \
       http://localhost:3000/api/indexes/civitai/config'
   ```

3. **Run smoke tests.** For the v1.0.201 deploy these were Archer's three queries from `docs/_in/correctness-handoff-2026-05-01.md`:
   - `nsfwLevel + Gte sortAtUnix` — must return rows
   - `nsfwLevel + type=image + Gte sortAtUnix` — must return matching count (was 0 pre-fix)
   - `nsfwLevel + isPublished=true + Gte sortAtUnix` — must return matching count (was 0 pre-fix)

   Plus a doc spot-check (the broken-bit example, doc 129087101):
   ```bash
   curl https://bitdex.civitai.com/api/indexes/civitai/query \
     -H "Content-Type: application/json" \
     -d '{"filters":[{"Eq":["id",{"Integer":129087101}]},{"Eq":["isPublished",{"Bool":true}]}],"limit":1}'
   ```
   Should return `{"ids":[129087101],...}`.

4. **Re-enable shadow flag** via flipt-state once smoke tests pass:
   ```bash
   node .claude/skills/deploy/flipt/flipt.mjs shadow on
   ```

5. **Resume Flux** by reverting the `suspend: true` commit in talos-infra `clusters/production/flux-system/apps/bitdex/bitdex.yaml`.

---

## Sample monitor commands during load

```bash
# Pod / sidecar phase
kubectl --context civit-datapacket -n bitdex logs -f bitdex-0 -c pg-sync --tail=200

# Server-side dump phase progress
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  curl -s -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
  http://localhost:3000/api/indexes/civitai/tasks

# Alive count / stats
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  curl -s -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
  http://localhost:3000/api/indexes/civitai/stats | grep -E 'alive_count|filter_field_count|sort_field_count'

# RSS + headroom
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  curl -s -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" http://localhost:3000/debug/memory

# Disk usage in load_stage
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- du -sh /data/indexes/civitai/load_stage
```

---

## Recovering from a botched run

### Pod won't come up after wipe

Confirm the wipe target was correct (no `bitmaps`/`docs`/`bounds` left):
```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- ls -la /data/indexes/civitai/
```

If pod is crashlooping with "snapshot inconsistency" or "missing alive bitmap": the wipe was partial. Scale to 0, re-run `reload.mjs wipe`, scale back up.

### Hard nuke stuck on dump phase

Most common: PG `statement_timeout` killing a long COPY. The sidecar uses the `bitdex` PG user (configured via the `bitdex-secrets` K8s secret), which has `statement_timeout=0`. If the COPY still hangs, check:
- Active queries in PG: `SELECT pid, query, state FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%COPY%';`
- Replica lag: COPY can be killed if running on a hot-standby that diverges.

If a phase fails partway, the sidecar retries on next pod restart but does NOT auto-retry within a single boot. Bounce the pod (`kubectl delete pod bitdex-0`) and watch the sidecar pick up where it left off.

### Triggers won't drop

If the retry pass leaves >4 triggers, manual escalation:
```bash
kubectl --context civit-datapacket exec -n cnpg-database cnpg-cluster-nvme0-3 -c postgres -- \
  psql -U postgres -d civitai -c "
    SELECT pg_blocking_pids(pid), pid, query, state
    FROM pg_stat_activity
    WHERE pid = ANY(pg_blocking_pids((SELECT pid FROM pg_stat_activity WHERE query LIKE 'DROP TRIGGER%' LIMIT 1)));"
```
This shows which civitai app queries are blocking the drop. Either wait for them to clear, kill them (last resort), or accept the surviving triggers — bitdex-sync setup_v2 will reconcile on boot since trigger-name hashes match.

### Cursor seed wrong after restart

If the seeded cursor is too low, the ops poller reprocesses old ops harmlessly (LIFO dedup). If too high, ops are lost. To force a re-seed at a specific value:
```bash
kubectl --context civit-datapacket exec -n cnpg-database cnpg-cluster-nvme0-3 -c postgres -- \
  psql -U postgres -d civitai -c "
    UPDATE bitdex_cursors SET last_outbox_id = <value>, updated_at = now()
    WHERE replica_id = 'pg-sync-bitdex-0';"
```
Then bounce the pod.

### "Pod stuck Pending" (not nuke-related but easy to confuse)

Check node memory; another tenant may have packed onto the node:
```bash
kubectl --context civit-datapacket describe node talos-wjh-tgy | grep -A 20 "Allocated resources"
```

---

## Caution

- **Never** wipe while shadow is ON. Mirrored prod traffic will see hard errors.
- **Never** skip the suspend step. Without `suspend: true` in talos-infra git, Flux will reconcile your scale-to-0 back to scale-to-1 within ~5 min.
- **Verify the writer pod** before running `nuke-pg`. CNPG failovers shift the primary; targeting a replica fails the truncates.
- **Rollback after wipe is a re-run, not a revert.** If v1.0.201 misbehaves post-nuke, rolling back to v1.0.200 means re-running the hard nuke against v1.0.200 — the bug recurs because the broken state is gone. Only worth doing if v1.0.201 is fundamentally broken (panic loop, OOM).

---

## Related

- `.claude/skills/deploy/reload.mjs` — orchestrated 6-step hard nuke
- `.claude/skills/deploy/cli.mjs` — `wipe`, `nuke-pg`, `cursor-read`, `cleanup`, `csv-dump-*`, `metrics-now`
- `.claude/skills/deploy/sql/nuke-pg-state.sql` + `nuke-pg-state-retry.sql` — bundled SQL assets
- `config/sync-civitai.yaml` — source of truth for dump phases + trigger configs
- `src/bin/pg_sync.rs` — sidecar autonomous boot sequence (read this if the boot phase is doing something unexpected)
- `docs/guide/prod-ops.md` — single-page reference for the deploy/observability seat
- `docs/guide/deploy-monitoring-handoff.md` — long-form mental model + every cli.mjs command
- `docs/design/trigger-deployment-process.md` — how PG triggers + outbox cursors interact
