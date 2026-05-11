# BitDex Prod Ops — Critical Functions

Single-page reference for the deploy/observability seat. Terse on purpose. Links to deeper docs where they exist; inline procedure where they don't.

If you just took the seat: read top to bottom once, then keep this open.

---

## 1. Constants

| | |
|---|---|
| K8s context | `civit-datapacket` |
| Namespace | `bitdex` |
| StatefulSet / Pod | `bitdex` / `bitdex-0` |
| Containers | `bitdex` (server, port 3000), `pg-sync` (sidecar) |
| Isolated health port | `3001` (separate runtime, never wedges) |
| PVCs | `data-bitdex-0`, `data-bitdex-1` |
| Current node | `talos-wjh-tgy` (was `talos-fq9-f3k`, then `talos-48r-b3a`) |
| GHCR | `ghcr.io/civitai/bitdex:<tag>` (no `v` prefix on image tag) |
| PG replica | `cnpg-cluster-nvme0-1` in `cnpg-database` ns |
| Manifest | `talos-infra` repo → `clusters/production/apps/bitdex/deployment.yaml` |
| Flux Kustomization | `bitdex` in `flux-system` ns |
| Sync config | `config/sync-civitai.yaml` |

---

## 2. Cluster access + auth

```bash
kubectl --context civit-datapacket -n bitdex get pods
```

`https://bitdex.civitai.com/...` only resolves to nginx download proxy. **Admin endpoints need port-forward or in-pod curl.**

```bash
# In-pod curl (preferred for one-shot diagnostics):
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'curl -s http://localhost:3000/metrics | grep <pattern>'

# Port-forward for browser/repeated calls:
kubectl --context civit-datapacket -n bitdex port-forward pod/bitdex-0 4099:3000
```

Admin bearer token:
```bash
kubectl --context civit-datapacket -n bitdex get secret bitdex-secrets \
  -o jsonpath='{.data.BITDEX_ADMIN_TOKEN}' | base64 -d
```

Same secret carries `DATABASE_URL` and `CLICKHOUSE_*`.

---

## 3. Pull metrics — three modes

### Mode A: deploy skill (pre-canned PromQL)
```bash
node .claude/skills/deploy/cli.mjs metrics-now            # 5-min: QPS, p50/95/99, cache hit
node .claude/skills/deploy/cli.mjs metrics-trend [window]
node .claude/skills/deploy/cli.mjs metrics-query '<promql>'
```

### Mode B: direct curl `/metrics` (raw Prom text)
Best for grepping specific families during incident triage:
```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'curl -s http://localhost:3000/metrics | grep -E "^bitdex_(flush|cache|wal_ops)_"'
```

### Mode C: local Prom container (for time-series of local server)
See `docs/_in/local-prom-howto.md`. Standard scrape every 5s, UI on `localhost:9090`.

---

## 4. Build + release

`.github/workflows/docker.yml` triggers on **tag push only**. NO auto-deploy.

```bash
# 1. Bump Cargo.toml manually (release.mjs broken on -jemalloc tags, task #116)
sed -i 's/version = "1.0.NNN-jemalloc"/version = "1.0.NN1-jemalloc"/' Cargo.toml
cargo update -p bitdex-v2

# 2. Commit + tag + push
git add Cargo.toml Cargo.lock
git commit -m "release: v1.0.NN1-jemalloc"
git tag v1.0.NN1-jemalloc
git push origin main
git push origin v1.0.NN1-jemalloc      # push tag separately to avoid stale-collision

# 3. Watch build (~9 min)
node .claude/skills/deploy/cli.mjs watch-build
```

**Tag prefix gotcha:** git tag = `v1.0.N-jemalloc`, image tag = `1.0.N-jemalloc` (semver action strips the `v`). Manifest must use no-`v` form or pull 404s.

**Imagepolicy regex gotcha:** Flux ImagePolicy filter is `^[0-9]+\.[0-9]+\.[0-9]+$` — won't match `-jemalloc` suffix. When you commit a `-jemalloc` image to `deployment.yaml`, **remove the `# {"$imagepolicy": "flux-system:bitdex"}` marker** on the image line. Otherwise automation rewrites your image back to a numeric tag within a reconcile.

---

## 5. Deploy

### Canonical: talos-infra commit
```bash
# Edit clusters/production/apps/bitdex/deployment.yaml — change image: line
git -C /path/to/talos-infra add clusters/production/apps/bitdex/deployment.yaml
git -C /path/to/talos-infra commit -m "bitdex: bump to v1.0.NN1-jemalloc"
git -C /path/to/talos-infra push
flux --context civit-datapacket reconcile kustomization bitdex
kubectl --context civit-datapacket -n bitdex get pods -w
```

Pod ready ~10s for relay mode, ~2 min for server cold start with cache load.

### Skill convenience
```bash
node .claude/skills/deploy/cli.mjs rollout <version>     # set image + wait for rollout
node .claude/skills/deploy/cli.mjs deploy <version>      # rollout + health checks + emit rollback cmd
node .claude/skills/deploy/cli.mjs rollback <version>
node .claude/skills/deploy/cli.mjs status
```

`kubectl set image` works in a pinch but **gets stomped within a reconcile** if imagepolicy marker present. Only safe with Flux suspended.

---

## 6. Flux suspend/resume

Live `kubectl patch` of `suspend` field does **NOT** stick — Flux reconciles back from git within ~5 min. **The only durable lever is git.**

```bash
# Read state
flux --context civit-datapacket get kustomization bitdex

# Live (transient — DON'T rely on this)
flux --context civit-datapacket suspend kustomization bitdex
flux --context civit-datapacket resume kustomization bitdex
flux --context civit-datapacket reconcile kustomization bitdex
```

Durable suspend/resume = edit `clusters/production/flux-system/apps/bitdex/bitdex.yaml`:
```yaml
spec:
  suspend: true   # or false
```
Commit + push.

---

## 7. Flipt shadow toggle

`bitdex-image-search` flag in `flipt-state` repo gates shadow-mode mirroring on Civitai side. API toggle gets reverted by GitOps within ~30s — **only the repo edit sticks.**

```bash
node .claude/skills/flipt/flipt.mjs shadow on        # writes flipt-state, push, verify
node .claude/skills/flipt/flipt.mjs shadow off
node .claude/skills/flipt/flipt.mjs get bitdex-image-search   # confirm state
```

`shadow primary` is blocked by skill (Justin only).

**Pre-flip relay precondition:** shadow MUST be OFF before flipping `BITDEX_MODE=relay`. Otherwise comparator runs against relay's `tee_mode:true` stub → divergence storm.

---

## 8. Relay-mode flip (no canonical runbook before now — fills the gap)

Use `BITDEX_MODE=relay` to detach pod from bitmap engine and stream `/events/queries` + `/events/ops` SSE channels for local consumption (Donovan's path).

### Pre-flip checklist
1. Shadow OFF (§7). Verify via `flipt get`.
2. PG-sync caught up (`bitdex_cursors.pg-sync-bitdex-0` near `MAX(id) FROM "BitdexOps"`).
3. Capture cursor snapshot — needed for replay on flip-back.

### Cursor snapshot (required)
```bash
# 1. Read PG ops cursor
kubectl --context civit-datapacket -n cnpg-database exec cnpg-cluster-nvme0-1 -- \
  psql "$DATABASE_URL" -t -c \
  "SELECT last_outbox_id FROM bitdex_cursors WHERE replica_id='pg-sync-bitdex-0';"
# Note this value: <PRE_FLIP_CURSOR>

# 2. Read WAL byte-offset (pre-flip)
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'curl -s -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
    http://localhost:3000/api/indexes/civitai/cursors/wal-reader'

# 3. Pin safety-hold cursor — prevents outbox cleanup during the window
kubectl --context civit-datapacket -n cnpg-database exec cnpg-cluster-nvme0-1 -- \
  psql "$DATABASE_URL" -c "
    INSERT INTO bitdex_cursors (replica_id, last_outbox_id, updated_at)
    VALUES ('safety-hold', <PRE_FLIP_CURSOR>, now())
    ON CONFLICT (replica_id) DO UPDATE
      SET last_outbox_id = EXCLUDED.last_outbox_id, updated_at = now();
  "
```

**Always use `BitdexOps` table (V2), NOT `BitdexOutbox` (V1 legacy frozen).** Mistake here can replay 25M ops on top of fresh state.

### Flip TO relay
Edit `clusters/production/apps/bitdex/deployment.yaml`:
```yaml
env:
  - name: BITDEX_MODE
    value: relay
  - name: BITDEX_RELAY_CONFIG
    value: |
      <relay yaml — see config/relay-civitai.yaml>
```
Commit + push. Flux reconciles → pod restarts in relay mode (~10s).

### Flip BACK to server
```bash
# 1. Edit deployment.yaml — REMOVE BITDEX_MODE=relay + BITDEX_RELAY_CONFIG env entries
git -C /path/to/talos-infra commit -m "bitdex: relay-mode flip back"
git -C /path/to/talos-infra push
flux --context civit-datapacket reconcile kustomization bitdex

# 2. Reset PG ops cursor to pre-flip value (engine replays the relay window)
kubectl --context civit-datapacket -n cnpg-database exec cnpg-cluster-nvme0-1 -- \
  psql "$DATABASE_URL" -c "
    UPDATE bitdex_cursors
    SET last_outbox_id = <PRE_FLIP_CURSOR>, updated_at = now()
    WHERE replica_id = 'pg-sync-bitdex-0';
  "

# 3. Drop safety-hold once replay catches up
kubectl --context civit-datapacket -n cnpg-database exec cnpg-cluster-nvme0-1 -- \
  psql "$DATABASE_URL" -c "DELETE FROM bitdex_cursors WHERE replica_id='safety-hold';"
```

WAL byte-offset cursor does NOT need manual reset — engine resumes from MetaStore on boot. LIFO dedup absorbs re-delivered ops.

### Validation
- `bitdex_cursors.pg-sync-bitdex-0` advances toward current `MAX(BitdexOps.id)`
- `bitdex_wal_ops_processed_total` rate bursts during catchup, settles to ~10–20 ops/s
- No restart_count increment

Reference: `docs/_in/relay-cursor-snapshot-2026-04-29.md` (concrete instance).

---

## 9. Cursor management

```bash
node .claude/skills/deploy/cli.mjs cursor-read         # both PVCs
node .claude/skills/deploy/cli.mjs cursor-reset <val>  # both PVCs + PG (pods at 0)
node .claude/skills/deploy/cli.mjs cursor-csv          # load_stage/cursor.txt
```

Two cursors, distinct purposes:
| Cursor | Owner | Used by |
|---|---|---|
| `bitdex_cursors.pg-sync-bitdex-0` (row id) | PG | ops_poller in pg-sync sidecar |
| `wal-reader` byte-offset | MetaStore on PVC | WAL reader thread in bitdex |
| `safety-hold` (row id) | PG | outbox-cleanup floor (set during sensitive windows) |

---

## 10. Nukes

Soft (keep CSVs) vs hard (re-dump from PG): **see `docs/guide/deploy-nukes.md`.**

Path B "fresh nuke": wipe `/data/wal` + `bitmaps` + `docs` + `load_stage`. **Forgetting `/data/wal` mixes pre-window WAL into post-window state** — ruins clean-canary signal. Always wipe WAL when isolating a deploy variable.

### 10b. Rolling redump (per-pod, no PG nuke)

When a code fix invalidates on-disk bitmap state (e.g. fresh-insert OR-accumulation, sortAt drift) but PG state is fine, use rolling redump instead of `reload.mjs`. Each pod self-wipes and re-runs its boot dump pipeline; the other pod keeps serving.

```bash
node .claude/skills/deploy/redump.mjs all
```

What happens per pod (orchestrator handles both sequentially):
1. `POST /api/indexes/civitai/redump` (admin-gated, returns 202 immediately)
2. Server removes `.ready` → k8s readiness probe flips to 503 → pod drops from Service
3. Server drains 30 s for in-flight queries
4. Server calls sidecar `POST http://127.0.0.1:9192/internal/restart` → sidecar deletes its row from `bitdex_cursors` (keyed by `replica_id`, set per-pod from `metadata.name`) and exits
5. Server wipes `shardstore/`, `docstore/`, `bitmaps/`, `cursors/`, `system/`, `load_stage/`, `wal/`, `captures/`, `dumps.json`, `.ready` — configs are configmap-mounted so they survive defensively
6. Server exits → k8s restarts both containers → sidecar boot sees empty PVC + missing dump registry → re-runs full dump pipeline

`kubectl wait --for=condition=ready` between pods. Typical end-to-end: ~5–15 min per pod at 107M scale.

**Use redump when:** you need to clear corrupt bitmap state (stuck-bit OR-accumulation, stale sortAt, etc.) without PG-side changes.

**Use `reload.mjs` when:** you need to drop PG triggers + `BitdexOps` + `bitdex_cursors`, or you're shipping a sync-v2 schema/trigger change. Redump leaves PG untouched.

**Caveats:**
- Cross-pod dump-snapshot race (handoff Bug B) is **not** addressed by redump — each pod captures its own pre-dump cursor at its own PG snapshot time, so a small steady-state delta between pods can still emerge for ops that arrive between pod-0's redump and pod-1's redump. Separate fix track (`pg_export_snapshot()`).
- If the sidecar `/internal/restart` call fails (network, sidecar down), redump logs a warning and continues — the server exit alone will eventually take the sidecar down via shared pod lifecycle, but the cursor row may linger until next sidecar boot.

---

## 11. Compaction

Manual kick via admin endpoint:
```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'curl -s -X POST -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
    http://localhost:3000/admin/compact'
```

Single-threaded shard scan + rewrite. Runs ~5 min on full Civitai dataset (~250K shards). Visible only via:
- `bitdex_compaction_skipped_total` (low signal)
- `bitdex_flush_compact_nanos` (per-flush in-line cost — ~zero in steady state, only fires for shards crossing op-count threshold)
- Logs: grep `"compaction (started|complete)"` (intermittent only)

**Compaction is NOT the steady-state CPU floor.** See §13.

---

## 12. Diagnostic primitives

### Decode `[flush-slow]` log lines
```
[flush-slow] total=Xms ops=N | apply=Yms promote=Zms cache=Ams compact=Bms tb=Cms publish=Dms post_apply_total=Ems
```
- **apply** — write changed filter/sort buckets per op. Steady ~1.7 ms/op; spike 13–20 ms/op for tiny batches.
- **compact** — in-flush shard compact. Almost always 0.0ms; non-zero only when shard hits op-count threshold mid-flush.
- **promote** — deferred-alive wall-clock promotions.
- **cache** — unified cache live maintenance.
- **publish** — ArcSwap snapshot publish.
- **tb** — time-bucket refresh.

### Per-thread CPU (right way)
**Cumulative `utime` ≠ instantaneous CPU.** Sample twice over a window:
```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'cat /proc/1/task/*/stat | awk "{print \$1, \$2, \$14, \$15}"' > /tmp/t0
sleep 10
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  sh -c 'cat /proc/1/task/*/stat | awk "{print \$1, \$2, \$14, \$15}"' > /tmp/t1
# Diff utime+stime per tid, divide by 10s × 100 ticks/s = % per thread
```
Compaction = 1–2 hot threads. Fan-out + tokio workers = uniform 24-way spread (~35% each = 8.4c).

### Heap-vs-pagecache disambiguation
```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- \
  cat /proc/1/smaps_rollup
```
- `Pss_File` — page-cache contribution
- `Pss_Anon` — heap contribution
Apr 30 baseline: Pss_File=8MB, Pss_Anon=31GB → heap dominates, file-cache theory ruled out. RSS oscillating 21–27GB with `Pss_Anon` not climbing = NOT a leak (jemalloc decay tuning working).

### jemalloc decay tuning
Env on container:
```yaml
- name: _RJEM_MALLOC_CONF
  value: "dirty_decay_ms:5000,muzzy_decay_ms:10000"
```
Zero-decay (`0,0`) caused unbounded RSS ratchet OOM at 110GiB on shadow-ON canary (v1.0.177 post-mortem).

---

## 13. Pre-flight checklist (any deploy/canary)

1. **Shadow state** — `flipt get bitdex-image-search` matches expectation
2. **Cursors caught up** — pg-sync row-id near `MAX(BitdexOps.id)`, lag <100
3. **Flux state** — suspended for hand-orchestrated changes; resumed for Flux-driven
4. **Restart count** — `kubectl get pod bitdex-0` shows expected count (any unexplained restart = investigate first)
5. **Image confirmed on GHCR** — `docker manifest inspect` before manifest push
6. **Memory headroom on node** — `kubectl describe node <node> | grep -A 20 Allocated`
7. **PVC bound to expected node** — openebs-hostpath WaitForFirstConsumer can pin to wrong node on fresh PVC

---

## 14. Abort criteria

Pull the lever (flip shadow OFF, suspend Flux, scale down) on any of:
- `restart_count` increments unexpectedly
- RSS climbs >1 GB/min sustained pre-flip (likely OOM trajectory)
- `bitdex_wal_ops_processed_total` rate spikes 10× baseline (retry storm)
- p99 query >2× pre-canary baseline for 60s+ window
- pg-sync cursor goes backwards or stalls >2 min

Recovery: shadow OFF first, then assess. v1.0.177 OOM auto-recovered in ~80s on StatefulSet replacement; no data loss because PVC + cursor strategy held.

---

## 15. Mission targets (current)

P50 <2ms, P95 <350ms, P99 <1s in shadow mode. 0 shed events. Beat v1.0.157 baseline. Few queries >1s, each with clear root-cause.

---

## 16. Known gotchas (pointer index)

| Topic | Where |
|---|---|
| `v` prefix on git tag vs image tag | §4 |
| Imagepolicy regex `-jemalloc` mismatch | §4 |
| `release.mjs` regex bug (task #116) | §4 |
| `kubectl patch suspend` doesn't stick | §6 |
| `kubectl set image` stomped by automation | §5 |
| Cumulative utime ≠ instantaneous CPU | §12 |
| `BitdexOutbox` (V1) vs `BitdexOps` (V2) | §8 |
| Path B nuke must wipe `/data/wal` | §10 |
| `BITDEX_MODE` flip needs shadow OFF first | §7, §8 |
| Memory scanner runtime PATCH not wired (task #28) | — |

---

## 17. Deeper references

- `docs/guide/deploy-monitoring-handoff.md` — long-form mental model + every cli.mjs command
- `docs/guide/deploy-nukes.md` — soft vs hard nuke
- `docs/_in/local-prom-howto.md` — local Prom setup
- `docs/_in/relay-consumer-howto.md` — Donovan-side SSE consumer
- `docs/_in/relay-cursor-snapshot-2026-04-29.md` — concrete relay-flip instance
- `docs/_in/shadow-flip-oom-postmortem-2026-04-29.md` — v1.0.177 OOM lessons
- `.claude/skills/deploy/skill.md` — every CLI command + skill internals
- `.claude/skills/flipt/SKILL.md` — flag toggle helpers
