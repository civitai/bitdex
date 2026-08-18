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

Production runs a **two-replica** StatefulSet. Each pod has its own RWO `openebs-hostpath` PVC, hard-pinned to one node by PV `nodeAffinity` — a pod cannot move without a PVC migration:

| Pod | PVC | Node | Role |
|---|---|---|---|
| `bitdex-0` | `data-bitdex-0` | `talos-wjh-tgy` | HAProxy **active** — serves all query traffic |
| `bitdex-1` | `data-bitdex-1` | `talos-48r-b3a` | warm **failover only** |

Verify a replica's node before wiping it:

```bash
kubectl --context civit-datapacket get pv \
  $(kubectl --context civit-datapacket -n bitdex get pvc data-bitdex-1 -o jsonpath='{.spec.volumeName}') \
  -o jsonpath='{.spec.nodeAffinity}'
```

PVC mounted at `/data` inside the pod. Inside `/data/indexes/civitai/`:

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

> **This doc used to say prod was single-replica.** It was wrong, and the tooling was written to match. Both PVCs are bound and both pods run. Anything that scales the StatefulSet or wipes "the PVC" without naming a replica is a bug.

---

## The cursor-pin trap — read this before wiping anything

**Rebuilding both pods concurrently destroys the second one.**

`cleanup_bitdex_ops` deletes every `BitdexOps` row below `MIN(last_outbox_id)` across all rows in `bitdex_cursors`. A freshly-wiped pod has **no cursor row yet** — it seeds one only *after* its dump finishes, which is 60–90 min later. So while it rebuilds, `MIN` is the *other* pod's cursor, and the healthy pod's normal forward progress trims away exactly the ops the rebuilding pod will need when it finally starts polling.

Observed 2026-08-18: bitdex-1 came up at cursor 5,502 against an ops table that started at 171,697 — `ALERT — hole above id 5502 exceeds 100000 ids` — and had to be rebuilt a second time.

**Fix: pin the cursor row of every replica you are not rebuilding first, to the current head of the ops table, before you wipe anything.**

```bash
node .claude/skills/deploy/reload.mjs pin-cursor --replica=1
```

which is:

```sql
INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at)
VALUES ('pg-sync-bitdex-1', (SELECT max(id) FROM "BitdexOps"), now())
ON CONFLICT (replica_id) DO UPDATE
  SET last_outbox_id = excluded.last_outbox_id, updated_at = now();
```

The pin holds the retention floor down until that pod has caught up on its own. The ops table grows while the pin is held — that is the point, and it drains once the pod catches up. Check it:

```sql
SELECT replica_id, last_outbox_id FROM bitdex_cursors ORDER BY replica_id;
SELECT min(id), max(id), count(*) FROM "BitdexOps";
```

`min(id)` should sit at or below the lowest cursor.

**And rebuild the pods sequentially, never concurrently.** Rebuild bitdex-0 (the pod serving traffic) first, verify it, then bitdex-1.

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

3. **Verify each pod is on the expected node**
   ```bash
   for r in 0 1; do
     kubectl --context civit-datapacket get pv \
       $(kubectl --context civit-datapacket -n bitdex get pvc data-bitdex-$r -o jsonpath='{.spec.volumeName}') \
       -o jsonpath="{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}{'\n'}"
   done
   ```
   Expect `talos-wjh-tgy` then `talos-48r-b3a`. PVCs are hard-pinned via NodeAffinity; a pod can't move without a PVC migration.

3b. **Check node headroom before you scale down.** Each pod requests 8 CPU / 32Gi. The moment a pod terminates, other tenants can take the freed capacity, and the pod cannot reschedule anywhere else — its PVC pins it. Observed 2026-08-18: Tekton build pods took bitdex-1's slot on `talos-48r-b3a` and it sat `Pending` for over an hour.
   ```bash
   kubectl --context civit-datapacket describe node talos-48r-b3a | sed -n '/Allocated resources/,/Events/p'
   ```
   If the node is tight, reserve the capacity *before* scaling down (for `talos-48r-b3a`, that meant a required `nodeAffinity` excluding it from the Tekton build pool — talos-infra #1096). Note that already-running PipelineRuns keep spawning task pods with the podTemplate captured at *their* creation, so an exclusion only takes full effect once in-flight builds drain.

3c. **Pin the cursor of every replica you are not rebuilding first** — see §The cursor-pin trap.
   ```bash
   node .claude/skills/deploy/reload.mjs pin-cursor --replica=1
   ```

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

**This wipes BOTH PVCs** — it loops replicas 0 and 1 — so it is a full outage for the duration of the reload, not a rolling one. That is safe from the cursor-pin trap (§above): a soft nuke keeps `bitdex_cursors` intact and no pod is advancing, so the retention floor holds. It is *not* safe for availability. To reload one replica at a time, wipe its PVC alone — but note `reload.mjs wipe --replica=N` is the **hard** wipe (`rm -rf /data/*`, CSVs included), so for a per-replica *soft* wipe run the `rm -rf` below against that replica's PVC yourself.

What `cli.mjs wipe` runs internally, per PVC (ephemeral pod, hostpath mount):

```bash
rm -rf /data/indexes/civitai/bitmaps \
       /data/indexes/civitai/docs \
       /data/indexes/civitai/bounds \
       /data/indexes/civitai/slot_arena.bin \
       /data/indexes/civitai/snapshot.meta
```

Then:

1. **Scale back up (both replicas):**
   ```bash
   kubectl --context civit-datapacket -n bitdex scale statefulset bitdex --replicas=2
   ```
2. **Pod boots → bitdex-sync sidecar runs `run_boot_sequence` → finds existing CSVs in `load_stage` → loads bitmaps + docstore from scratch.** ~5–15 min.
3. **Watch logs** (per replica):
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

The orchestration is now 6 steps plus a pin. Each step is verifiable on its own. **Run them sequentially** — don't pipe them. Steps that touch one pod's disk or cursor require `--replica=N`; there is no default.

```bash
node .claude/skills/deploy/reload.mjs preflight              # verify shadow OFF, Flux suspended
node .claude/skills/deploy/reload.mjs pin-cursor --replica=1 # hold the retention floor for the pod you rebuild SECOND
node .claude/skills/deploy/reload.mjs suspend                # scale StatefulSet to 0 (fleet-wide)
node .claude/skills/deploy/reload.mjs nuke-pg                # drop triggers + truncate BitdexOps/bitdex_cursors
node .claude/skills/deploy/reload.mjs wipe --replica=0       # wipe ONE replica's PVC
node .claude/skills/deploy/reload.mjs start --replicas=2     # scale up — bitdex-sync drives the rest
node .claude/skills/deploy/reload.mjs monitor --replica=0    # tail that pod's pg-sync logs
```

Then, only once replica 0 is verified serving, wipe and rebuild replica 1 the same way.

> **On a full hard nuke**, `nuke-pg` truncates `bitdex_cursors`, so the pin is gone and the ops table restarts from empty — no retention floor to protect. The pin matters when you rebuild **one** pod against a live ops table, which is the common case and the one that bit us.

| # | Step | What it does |
|---|------|---|
| 1 | `preflight` | Verify shadow OFF, Flux suspended, note current image. Read-only — does not mutate cluster state. |
| — | `pin-cursor` | `--replica=N` required. Upserts that replica's `bitdex_cursors` row to `MAX(BitdexOps.id)` so `cleanup_bitdex_ops` cannot trim ops it will need. See §The cursor-pin trap. |
| 2 | `suspend` | Scale StatefulSet to 0, force-delete **every** `bitdex-N` pod, verify no `bitdex-N` pod remains. (The old check grepped `^bitdex-0` only and reported "Pods: 0" while `bitdex-1` was still Terminating.) |
| 3 | `nuke-pg` | Run `sql/nuke-pg-state.sql` (pass 1, lock_timeout=5s) + `sql/nuke-pg-state-retry.sql` (pass 2, retry up to 8x) against the PG primary writer. Drops every `bitdex_*` trigger + function, truncates `BitdexOps` + `bitdex_cursors`. |
| 4 | `wipe` | `--replica=N` required. Mounts **that replica's** PVC via an ephemeral busybox pinned to that replica's node; `rm -rf /data/*` (full PVC wipe). The `init-config` init container restores `config.yaml` + `ui-config.yaml` from the configmap and recreates `/data/{indexes/civitai,wal,indexes/civitai/load_stage}` on next pod boot, so the wipe leaves no stale shards, WAL bytes, or unknown future files. |
| 5 | `start` | `scale --replicas=N` (`--replicas=` flag, default 2). Warns if any pod stays `Pending` — its PVC pins it to one node, so it waits for headroom *there*. The bitdex-sync sidecar runs `run_boot_sequence` autonomously: setup_v2 (re-installs triggers from sync config + creates `BitdexOps`/`bitdex_cursors`) → captures pre-dump cursor → streams CSVs from PG → registers each phase via `PUT /dumps` + `POST /dumps/{name}/loaded` → polls completion → seeds cursor → transitions to ops poller. |
| 6 | `monitor` | `--replica=N` required. `kubectl logs -f bitdex-N -c pg-sync`. Detach with Ctrl-C; load continues regardless. Re-run `monitor` to reattach. |

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
kubectl --context civit-datapacket exec -i -n cnpg-database cnpg-cluster-nvme0-5 -c postgres -- \
  psql -U postgres -d civitai < .claude/skills/deploy/sql/nuke-pg-state.sql
```

The writer pod name (`cnpg-cluster-nvme0-5` at time of writing — it has been `nvme0-3` and `nvme0-2` before) shifts across CNPG failovers. Verify before running:
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

4. **Verify scheduled-publish health (post-#339).** Read stored documents, not bitmaps — `postId` is `per_value_lazy`, so `postId eq X` returns a partial set and is not a membership instrument. Take a sample of images PG says were scheduled-then-published, plus a control sample of immediately-published ones, and read each doc:

   ```bash
   curl -s -X POST http://127.0.0.1:4099/api/indexes/civitai/document \
     -H 'content-type: application/json' \
     -d '{"slot_id":<imageId>,"fields":["publishedAt","isPublished","sortAt","postId"]}'
   ```

   The stuck signature is `publishedAt == 0` on a doc PG says published in the past — no repair op can heal it, because arming requires a *future* timestamp. Expect **0** in both samples. Pre-#339 the scheduled group ran ~1.2%; the control group was always 0, which is what makes it a control.

5. **Re-enable shadow flag** via flipt-state once smoke tests pass:
   ```bash
   node .claude/skills/deploy/flipt/flipt.mjs shadow on
   ```

6. **Resume Flux** by reverting the `suspend: true` commit in talos-infra `clusters/production/flux-system/apps/bitdex/bitdex.yaml`. **Nothing bitdex-side reconciles until you do** — image pins, config, replica count all sit frozen. Verify:
   ```bash
   kubectl --context civit-datapacket get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}'
   # must print: false (or be unset)
   ```

7. **Undo any temporary capacity reservation** you made in pre-flight step 3b (e.g. revert the Tekton node exclusion), so the spare compute goes back to its normal tenant.

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
kubectl --context civit-datapacket exec -n cnpg-database cnpg-cluster-nvme0-5 -c postgres -- \
  psql -U postgres -d civitai -c "
    SELECT pg_blocking_pids(pid), pid, query, state
    FROM pg_stat_activity
    WHERE pid = ANY(pg_blocking_pids((SELECT pid FROM pg_stat_activity WHERE query LIKE 'DROP TRIGGER%' LIMIT 1)));"
```
This shows which civitai app queries are blocking the drop. Either wait for them to clear, kill them (last resort), or accept the surviving triggers — bitdex-sync setup_v2 will reconcile on boot since trigger-name hashes match.

### Cursor seed wrong after restart

If the seeded cursor is too low, the ops poller reprocesses old ops harmlessly (LIFO dedup). If too high, ops are lost. To force a re-seed at a specific value:
```bash
kubectl --context civit-datapacket exec -n cnpg-database cnpg-cluster-nvme0-5 -c postgres -- \
  psql -U postgres -d civitai -c "
    UPDATE bitdex_cursors SET last_outbox_id = <value>, updated_at = now()
    WHERE replica_id = 'pg-sync-bitdex-0';"
```
Then bounce the pod.

### Ops-poller reports a hole above the cursor

```
ALERT — hole above id 5502 exceeds 100000 ids
```

The pod's cursor is below `MIN(BitdexOps.id)` — the ops it needs were deleted by `cleanup_bitdex_ops` while it was rebuilding. This is the cursor-pin trap; see §The cursor-pin trap. There is no incremental recovery: the pod has a gap in its op stream and must be rebuilt again, this time with its cursor pinned first.

```sql
SELECT replica_id, last_outbox_id FROM bitdex_cursors ORDER BY replica_id;
SELECT min(id), max(id) FROM "BitdexOps";
```

### "Pod stuck Pending"

Each pod requests 8 CPU / 32Gi and its PVC pins it to exactly one node, so "Pending" always means *that node* lacks headroom — there is no second placement to fall back to.

```bash
kubectl --context civit-datapacket describe pod bitdex-1 -n bitdex | tail -20
kubectl --context civit-datapacket describe node talos-48r-b3a | sed -n '/Allocated resources/,/Events/p'
```

To see who took the capacity:
```bash
kubectl --context civit-datapacket get pods -A --field-selector spec.nodeName=talos-48r-b3a
```

`talos-48r-b3a` is shared with the Tekton build pool, which is the usual culprit. Excluding it from that pool (talos-infra #1096) stops *new* PipelineRuns from landing, but in-flight PipelineRuns keep spawning task pods using the podTemplate captured when they were created — so expect a drain delay, not an immediate eviction. This is why pre-flight step 3b reserves capacity *before* the pod goes down.

---

## Caution

- **Never** wipe while shadow is ON. Mirrored prod traffic will see hard errors.
- **Never** skip the suspend step. Without `suspend: true` in talos-infra git, Flux will reconcile your scale-to-0 back to scale-to-1 within ~5 min.
- **Verify the writer pod** before running `nuke-pg`. CNPG failovers shift the primary; targeting a replica fails the truncates.
- **Never rebuild both replicas concurrently.** See §The cursor-pin trap — the healthy pod's progress deletes the rebuilding pod's ops.
- **Never scale down without checking node headroom first.** The PVC pins each pod to one node; if another tenant takes the freed capacity while the pod is down, it cannot reschedule anywhere.
- **Rollback after wipe is a re-run, not a revert.** If v1.0.201 misbehaves post-nuke, rolling back to v1.0.200 means re-running the hard nuke against v1.0.200 — the bug recurs because the broken state is gone. Only worth doing if v1.0.201 is fundamentally broken (panic loop, OOM).

---

## Related

- `.claude/skills/deploy/reload.mjs` — orchestrated hard nuke; replica-aware (`--replica=N`), plus `pin-cursor`
- `.claude/skills/deploy/cli.mjs` — `wipe` (soft, **both** PVCs), `nuke-pg`, `cursor-read`, `cursor-set` (writes both replica rows), `cleanup`, `csv-dump-*`, `metrics-now`
- `.claude/skills/deploy/sql/nuke-pg-state.sql` + `nuke-pg-state-retry.sql` — bundled SQL assets
- `config/sync-civitai.yaml` — source of truth for dump phases + trigger configs
- `src/bin/pg_sync.rs` — sidecar autonomous boot sequence (read this if the boot phase is doing something unexpected)
- `docs/guide/prod-ops.md` — single-page reference for the deploy/observability seat
- `docs/guide/deploy-monitoring-handoff.md` — long-form mental model + every cli.mjs command
- `docs/design/trigger-deployment-process.md` — how PG triggers + outbox cursors interact
