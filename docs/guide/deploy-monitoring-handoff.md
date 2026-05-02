# BitDex Deploy + Monitoring — Handoff

Aidan's accumulated knowledge from running prod deploys, monitoring, and incident response on BitDex. Successor agent should read end-to-end before touching the cluster, then keep this doc current as new gotchas surface.

---

## 1. The 60-second mental model

- **Single-pod StatefulSet** (`bitdex-0`) on a single node (`talos-wjh-tgy` at time of writing — the pod has migrated nodes a few times: `talos-fq9-f3k` → `talos-48r-b3a` → `talos-wjh-tgy`. Always verify with `kubectl get pv $(kubectl -n bitdex get pvc data-bitdex-0 -o jsonpath='{.spec.volumeName}') -o jsonpath='{.spec.nodeAffinity}'`) with a locally-bound NVMe PVC (`data-bitdex-0`). `data-bitdex-1` exists in archived runbooks for the old two-replica layout but is not in use.
- **Two containers per pod:** `bitdex` (Rust server, port 3000 main + 3001 isolated health) and `pg-sync` (sidecar that polls `BitdexOps` table in PG and POSTs ops to BitDex).
- **GitOps:** talos-infra repo → Flux Kustomization reconciles `clusters/production/apps/bitdex/deployment.yaml` into the cluster.
- **Image tags:** `ghcr.io/civitai/bitdex:<version>` published by `.github/workflows/docker.yml` on every `v*` tag push.
- **Releases:** semver-prerelease style — `1.0.NNN-jemalloc`. The git tag is `v1.0.NNN-jemalloc`, but the **image tag is `1.0.NNN-jemalloc` (no `v` prefix)** — see §6.
- **Active mode:** controlled by `BITDEX_MODE` env var. `server` = full bitmap engine. `relay` = HTTP→SSE relay (no bitmap work; mirrors traffic to `/events/queries` and `/events/ops`).
- **Shadow comparator** (Civitai side, model-share repo): mirrors prod query traffic to BitDex when the Flipt flag `bitdex-image-search` is `enabled: true` with rule routing to the `shadow` variant.

---

## 2. The deploy skill

`node .claude/skills/deploy/cli.mjs <command> [args]` — JSON to stdout, status to stderr.

### Most-used commands

```bash
# Status / health
status                     # image, pod ready, restart count
health                     # pods, pg-sync cursor
resources                  # kubectl top
memory                     # top + /debug/memory endpoint
disk                       # PVC usage
metrics-now                # 5-min QPS + percentiles (Prom)
metrics-trend [window]     # time series
metrics-query '<promql>'   # arbitrary query

# Logs
server-logs [pod] [lines]
pg-sync-logs [pod] [lines]
pg-sync-health

# Releases / deploys
release                    # bump Cargo.toml + tag + push + trigger build  ⚠ see §7
watch-build [run-id]       # block until Docker build completes
watch-build-notify [run-id] [--notify <agent>]
build-status               # latest builds

rollout <version>          # kubectl set image + wait for rollout
deploy <version>            # rollout + health checks + rollback cmd
rollback <version>
status

# Cursors (mostly relevant during nukes)
cursor-reset <value>
cursor-read
cursor-csv

# Config (runtime PATCH; reverted on restart unless persisted via env)
config-read
config-patch '{"key":"value"}'

# Bulk reload (hard nuke — see docs/guide/deploy-nukes.md)
node .claude/skills/deploy/reload.mjs <step>
wipe                       # soft nuke: bitmaps/docs/bounds only

# CSV dump pipeline (config-driven from config/sync-civitai.yaml)
csv-dump-tables
csv-dump [tables] [--gzip]
csv-dump-progress
csv-dump-cleanup
csv-full-pipeline [tables] [--notify <agent>] [--skip-dump]
csv-serve / csv-serve-stop
csv-download [tables] [--token <token>] [--chunks 16]
csv-verify <dir>

# Tunnels
tunnel pg [start|stop|status]      # localhost:5432
tunnel bitdex [start|stop|status]  # localhost:3099
```

### Skill-internal constants

| | |
|---|---|
| Namespace | `bitdex` |
| StatefulSet | `bitdex` |
| Containers | `bitdex` (server), `pg-sync` (sidecar) |
| PVCs | `data-bitdex-0` (`data-bitdex-1` exists but unused in current single-replica layout) |
| Node | `talos-wjh-tgy` (verify before assuming — see §10) |
| GHCR | `ghcr.io/civitai/bitdex` |
| K8s context | `civit-datapacket` |
| PG replica | `cnpg-cluster-nvme0-1` in `cnpg-database` ns |
| Sync config | `config/sync-civitai.yaml` |

---

## 3. Cluster access

```bash
# Default kubectl context for prod
kubectl --context civit-datapacket -n bitdex get pods

# Note: skill commands all use this context implicitly.
```

**Cloudflare DNS does NOT route `/api/*` to the BitDex pod.** `https://bitdex.civitai.com/...` only resolves to `bitdex-dl` (nginx download proxy in the same namespace). Any admin call (`/api/indexes/...`, `/metrics`, `/debug/queries/stream`, etc.) needs a port-forward:

```bash
kubectl --context civit-datapacket -n bitdex port-forward pod/bitdex-0 4099:3000
# Then curl localhost:4099/api/indexes/civitai/...
```

Port 3001 is the isolated health listener — separate `std::thread` + `new_current_thread` tokio runtime since v1.0.156. If `localhost:4101 → 3001` responds but `4099 → 3000` doesn't, the main listener is wedged.

---

## 4. Admin auth

Bearer token from secret `bitdex-secrets` (key `BITDEX_ADMIN_TOKEN`):

```bash
kubectl --context civit-datapacket -n bitdex get secret bitdex-secrets \
  -o jsonpath='{.data.BITDEX_ADMIN_TOKEN}' | base64 -d
```

Same secret carries `DATABASE_URL`, `CLICKHOUSE_*`. Relay reuses the BitDex admin token.

The skill `.env` files automatically read this for some commands. The deploy skill picks up `BITDEX_ADMIN_TOKEN` from env.

---

## 5. Image build pipeline

`.github/workflows/docker.yml`:

```yaml
on:
  push:
    tags: ['v*']
  workflow_dispatch:
```

Triggers on **tag push only**. NO auto-deploy hook. Workflow:

1. `docker/metadata-action@v5` extracts `type=semver,pattern={{version}}` → strips the `v` prefix.
2. Builds + pushes to `ghcr.io/civitai/bitdex:<image-tag>`.

So when you push git tag `v1.0.165-jemalloc`:
- Image gets tag `1.0.165-jemalloc` (no `v`)
- Also `latest` if `startsWith(github.ref, 'refs/tags/v')`

Build time: ~9 min full LTO with jemalloc.

**Watch a build:**

```bash
gh run list --workflow docker.yml --limit 3 --json databaseId,headBranch,status
gh run watch <run-id> --exit-status
# or skill version:
node .claude/skills/deploy/cli.mjs watch-build [run-id]
```

---

## 6. Image tag gotchas

### ⚠ `v` prefix mismatch (Tom's catch on relay V1)

Git tags are `v1.0.NNN-jemalloc`. Image tags published by `docker.yml` are `1.0.NNN-jemalloc` (semver action strips the `v`). When you write `image: ghcr.io/civitai/bitdex:v1.0.172-jemalloc` in the manifest, K8s gets `404 Not Found` on pull. Use the no-`v` form.

### ⚠ Image automation regex doesn't match `-jemalloc` suffix

Talos-infra has Flux ImagePolicy + ImageUpdateAutomation watching the `bitdex` image. The default policy filter is `^[0-9]+\.[0-9]+\.[0-9]+$` — won't match `1.0.172-jemalloc` (has `-jemalloc` suffix).

Two consequences:
1. Auto-bump to a new perf tag won't happen — manual edit of `deployment.yaml` is required.
2. If you set the image manually via `kubectl set image`, automation rewrites `deployment.yaml` back to whatever DOES match the regex (e.g. an old `1.0.221`). Flux applies → your image gets stomped within a reconcile.

**Fix:** when committing the relay or perf image to talos-infra, **remove the `# {"$imagepolicy": "flux-system:bitdex"}` marker** on the image line so automation skips it. Re-add when you want auto-bump back.

### ⚠ release.mjs regex bug (Aidan, Apr 18)

`.claude/skills/deploy/cli.mjs` `release()` function has regex `/version\s*=\s*"(\d+\.\d+\.)(\d+)"/` that picks the FIRST `\d+\.\d+\.\d+` triple in `Cargo.toml`. With `-jemalloc` suffix, the package version no longer matches → falls through to e.g. `serde_yaml = "0.9.34"` and bumps to `0.9.35`. Botched once already (commit `418c080 release: v0.9.35`, fixed forward with `7cbcc6d`). Filed as task #116. Until fixed, **do releases manually for `-jemalloc` tags:**

```bash
# Edit Cargo.toml package version manually
cargo update -p bitdex-v2
git add Cargo.toml Cargo.lock
git commit -m "release: v1.0.NNN-jemalloc"
git tag v1.0.NNN-jemalloc
git push origin main
git push origin v1.0.NNN-jemalloc
```

### ⚠ Push tag separately when remote tag collisions

Memory rule: **when `git push --tags` fails on a stale collision, push main first without `--tags`, then push the tag separately.** Some orphan tags from earlier sessions exist (e.g. `v1.0.153-jemalloc` points at a Ava force-push superseded commit). They don't break Docker builds but break `--tags` pushes.

---

## 7. Releasing a new version

Manual process (works around release.mjs bug):

```bash
# 1. Bump Cargo.toml manually
sed -i 's/version = "1.0.165-jemalloc"/version = "1.0.166-jemalloc"/' Cargo.toml

# 2. Refresh Cargo.lock
cargo update -p bitdex-v2

# 3. Commit, tag, push
git add Cargo.toml Cargo.lock
git commit -m "release: v1.0.166-jemalloc

<one-line description of what's new>

Co-Authored-By: <model> <noreply@anthropic.com>"
git tag v1.0.166-jemalloc
git push origin main
git push origin v1.0.166-jemalloc

# 4. Wait for build (~9 min)
gh run watch <run-id> --exit-status

# 5. Confirm GHCR has the image (usually auto-published by build)
docker manifest inspect ghcr.io/civitai/bitdex:1.0.166-jemalloc
# OR
gh api /orgs/civitai/packages/container/bitdex/versions --jq '.[0:5][].metadata.container.tags'
```

Now image is on GHCR. To **deploy**, see §8.

---

## 8. Deploying — talos-infra commit path (canonical)

Manifest lives in `talos-infra` repo (path varies by machine — common location `C:/Dev/Repos/work/talos-infra/clusters/production/apps/bitdex/deployment.yaml`). Tom owns this repo for cluster apply.

### Edit the manifest

```yaml
# clusters/production/apps/bitdex/deployment.yaml
spec:
  template:
    spec:
      containers:
        - name: bitdex
          # Keep this line WITHOUT the imagepolicy marker for jemalloc tags:
          image: ghcr.io/civitai/bitdex:1.0.166-jemalloc
          # If you want auto-bump back (post-jemalloc), restore the marker:
          # image: ghcr.io/civitai/bitdex:1.0.221 # {"$imagepolicy": "flux-system:bitdex"}
```

### Commit + push + reconcile

```bash
git -C /path/to/talos-infra add clusters/production/apps/bitdex/deployment.yaml
git -C /path/to/talos-infra commit -m "bitdex: bump to v1.0.166-jemalloc"
git -C /path/to/talos-infra push
flux --context civit-datapacket reconcile kustomization bitdex
```

### Watch rollout

```bash
kubectl --context civit-datapacket -n bitdex get pods -w
# Pod restarts (~10s for relay, ~2 min for server cold start with cache load)
# Both containers must report ready 1/1 + 1/1 = 2/2
```

### kubectl set image fallback (transient only)

Works in a pinch but **gets stomped by automation** within a reconcile if imagepolicy marker is present. Use only when Flux Kustomization is suspended OR for instant testing before a proper commit:

```bash
kubectl --context civit-datapacket -n bitdex set image \
  statefulset/bitdex bitdex=ghcr.io/civitai/bitdex:1.0.166-jemalloc
```

---

## 9. Flux Kustomization state

**Canonical state of `bitdex` Kustomization is `suspend: false` in talos-infra** (`clusters/production/flux-system/apps/bitdex/bitdex.yaml`). The earlier "suspended since 2026-04-12" note is stale — the current default is unsuspended.

### ⚠ Live `kubectl patch` / `flux suspend` does NOT stick

Patching the live Kustomization directly (via `kubectl patch` or `flux suspend kustomization bitdex`) sets `spec.suspend: true` only on the in-cluster object. Flux reconciles the Kustomization back from git within ~5 min, overriding the patch. **The only durable lever is a git commit** to talos-infra.

```bash
# Read current state (in-cluster — may be transient)
flux --context civit-datapacket get kustomization bitdex

# Durable suspend/resume = git commit
# Edit clusters/production/flux-system/apps/bitdex/bitdex.yaml:
#   spec:
#     suspend: true   # or false
# Commit + push, then force a flux-system reconcile so the change picks up:
kubectl --context civit-datapacket -n flux-system annotate gitrepository flux-system \
  reconcile.fluxcd.io/requestedAt=$(date +%s) --overwrite
kubectl --context civit-datapacket -n flux-system annotate kustomization bitdex \
  reconcile.fluxcd.io/requestedAt=$(date +%s) --overwrite

# Verify the change took:
kubectl --context civit-datapacket get kustomization bitdex -n flux-system -o jsonpath='{.spec.suspend}'
```

The transient API path is still useful for *short* windows (under ~5 min) when you need a freeze right now and a git commit is mid-prep:

```bash
# Transient — DON'T rely past one reconcile interval
flux --context civit-datapacket suspend kustomization bitdex
flux --context civit-datapacket resume kustomization bitdex
flux --context civit-datapacket reconcile kustomization bitdex
```

---

## 10. Node memory budget — DON'T blow this

The bitdex pod runs on whichever node holds `data-bitdex-0`'s PV at any given time. Pod has migrated across nodes through PVC re-binds (most recently to `talos-wjh-tgy`). The numbers below are illustrative of the contention pattern; **always verify with `kubectl describe node <current-node>` before bumping memory requests**, since both the node identity and tenant mix shift over time.

```
Example state (snapshot, may be stale):
Node mem requests: ~111 GiB / 126 GiB (88%)
Top tenants:
  cnpg-database/buzz-db-1                    80 GiB
  civitai-feeds/feeds-meilisearch-2-0        16 GiB
  civitai-signals/civitai-signals-silo        6 GiB
  spine-controller/spine-controller-mp        4 GiB
  traefik                                     2 GiB
```

Bitdex pod was at `requests.memory: 24Gi` for a long time, then **lowered to 14Gi for the relay window** (relay process uses ~200 MiB; the request is reserved-but-idle to guarantee flip-back). When `civitai-signals-silo` later packed onto the node, even 14Gi couldn't fit — Scarlet authorized **emergency drop to 10Gi** (Apr 25 ~09:00 UTC).

### Pre-bump checklist (before raising memory request)

```bash
# Find the current node
kubectl --context civit-datapacket get pv $(kubectl --context civit-datapacket -n bitdex get pvc data-bitdex-0 -o jsonpath='{.spec.volumeName}') -o jsonpath='{.spec.nodeAffinity}'

# Check available headroom
kubectl --context civit-datapacket describe node <current-node> | grep -A 20 "Allocated resources"

# Compute: allocatable_mem - sum(requests.memory across all pods on node)
# If headroom < requested bump → pod will go Pending. Evict-or-wait first.
```

**The aggressive-mode lower (1Gi during relay) is opt-in only** per the relay runbook. Default-safe is 14Gi reserved-but-idle so flip-back doesn't get evicted.

### PVC affinity is hard-pinned

`openebs-hostpath-bitdex-nvme` PVCs use NodeAffinity to pin to a specific node (current state: only `data-bitdex-0` is bound — `data-bitdex-1` exists in older two-replica manifests but is not in use). **You cannot move the bitdex pod to another node without migrating the PVC** — major surgery.

---

## 11. PodSecurity + filesystem permissions

PodSecurity policy is `restricted:latest`. Manifest must include:

```yaml
spec:
  template:
    spec:
      securityContext:
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        fsGroupChangePolicy: OnRootMismatch
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: bitdex
          securityContext:
            runAsUser: 65532
            runAsGroup: 65632
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
            seccompProfile:
              type: RuntimeDefault
        - name: pg-sync
          # ...same securityContext block...
```

### ⚠ openebs-hostpath does NOT honor fsGroup

Discovered Apr 25 by Tom. Even with `fsGroup: 65532` set, kubelet doesn't recursively chown the PVC mount. Pod starts but pg-sync fails to write WAL/cursor files.

**Fix:** add a `fix-perms` initContainer:

```yaml
initContainers:
  - name: fix-perms
    image: busybox:1.36
    command: ['sh', '-c', 'chown -R 65532:65532 /data && chmod -R u+rwX /data']
    securityContext:
      runAsUser: 0
      runAsNonRoot: false
      capabilities:
        add: ["CHOWN"]
    volumeMounts:
      - name: data
        mountPath: /data
```

(Reference: Tom's commit `1c1b7ff0` on talos-infra.)

### ⚠ PodSecurity warnings are noisy but not fatal

Every `kubectl apply` / `kubectl patch` emits a long warning about `runAsNonRoot/allowPrivilegeEscalation/etc.` even when those fields ARE set. The warning is informational; the apply succeeds. Ignore unless `kubectl get pod` shows `CreateContainerConfigError`.

---

## 12. Concurrency cap (env-var-persistent since v1.0.164)

Server has a `max_query_concurrency` runtime config. **Default is 0 (unlimited).** Without a cap, prod traffic can stack 970+ in-flight queries → tokio runtime saturation → P50 climbs to 4-5s.

Persisted via env var — survives pod restart:

```yaml
env:
  - name: BITDEX_MAX_QUERY_CONCURRENCY
    value: "32"
```

Confirm in pod:

```bash
kubectl --context civit-datapacket -n bitdex exec bitdex-0 -c bitdex -- sh -c 'echo $BITDEX_MAX_QUERY_CONCURRENCY'
```

When cap is hit, excess queries get HTTP 503. `bitdex_queries_rejected_total` counter rises. That's the cap doing its job, not a regression.

---

## 13. Relay V1 — `BITDEX_MODE`

Same image, two modes:

```yaml
env:
  - name: BITDEX_MODE
    value: "server"   # default — full bitmap engine
    # or
  - name: BITDEX_MODE
    value: "relay"    # HTTP→SSE relay, no bitmap work
```

Probe shape changes between modes:
- `server`: `GET /api/health` returns `ok` (plain text)
- `relay`: `GET /api/health` returns `{"status":"ok","mode":"relay"}` (JSON with mode field)

Use that response shape as your "is this actually relay?" probe — env var alone tells you nothing if image is pre-relay (predates v1.0.172).

### Pre-flip checklist (excerpt)

See `docs/guide/relay-flip-runbook.md` for full version.

- Shadow OFF (`bitdex-image-search` flipt flag disabled) ← was the original V1 design; later relaxed once shadow comparator was confirmed harmless during relay
- Local rig connected to `/events/queries` BEFORE flip (events between flip + subscribe are lost)
- Pre-flip cursor recorded for both pg-sync replicas (needed for WAL replay reseed on flip-back)
- Memory request stays at 14Gi (default-safe) unless explicitly opting into aggressive 1Gi mode

### Flip-back is NOT idempotent

PG-sync advanced its cursor during the relay window; BitDex WAL received nothing. Mandatory reseed:

- **Path A (default, 5–10 min):** Reset PG-sync cursor to pre-flip value. WAL reader replays. Only works if `(relay window) < (WAL retention)`.
- **Path B (60–90 min):** Full hard nuke via `reload.mjs`. Use when WAL retention exceeded or replay verification fails.

Canary checklist on flip-back:
1. Both pods Running + Ready
2. WAL ops processed counter advancing, gap to PG cursor < one batch (~5K)
3. Shadow comparator divergence ≤ pre-window baseline (5-min sample)
4. Smoke query returns expected count + recent `sortAt`

---

## 14. Flipt skill — shadow toggle

`.claude/skills/flipt/flipt.mjs` — copied from model-share + extended.

```bash
# Shadow ON / OFF (GitOps path; commits to flipt-state, pushes, verifies via API)
node .claude/skills/flipt/flipt.mjs shadow on
node .claude/skills/flipt/flipt.mjs shadow off

# PRIMARY is BLOCKED — Justin only (skill exits with explicit error)
node .claude/skills/flipt/flipt.mjs shadow primary  # refused

# Read state
node .claude/skills/flipt/flipt.mjs get bitdex-image-search [--json]
node .claude/skills/flipt/flipt.mjs list
```

### How `shadow on/off` works

1. Verify `flipt-state` repo (`C:/Dev/Repos/work/flipt-state` by default) is clean + on `main`
2. Pull latest
3. Patch `enabled:` for `bitdex-image-search` block in `civitai-app/default/features.yaml`
4. Commit + push (no PR; flipt-state pushes direct to main)
5. Sleep ~35s for Flipt to re-sync from git
6. Verify via API that the change took effect

**API write path doesn't work** — token in bundled `.env` is read-only fetcher token. Writes return 405 Method Not Allowed. GitOps is the only working write path.

The flag is a `VARIANT_FLAG_TYPE` with three variants:
- `off` (default)
- `shadow` — distribution rule routes 100% to this when enabled
- `primary` — guarded; never flip via skill

`enabled: false` = comparator path inactive. `enabled: true` = mirror traffic to BitDex shadow path.

---

## 15. Soft nuke vs hard nuke

See `docs/guide/deploy-nukes.md` for full procedure (rewritten 2026-05-01 for the sync-v2 autonomous boot flow).

| | Soft Nuke | Hard Nuke |
|---|---|---|
| Wipes `bitmaps/`, `docs/`, `bounds/`, `slot_arena.bin`, `snapshot.meta` | yes | yes |
| Wipes `load_stage/*.csv` | no | yes |
| Drops `bitdex_*` triggers + truncates `BitdexOps` + `bitdex_cursors` | no | yes |
| Re-dumps from PG | no | yes (driven autonomously by bitdex-sync sidecar on boot) |
| Recovery time | 5–15 min | 60–90 min |
| Command | `cli.mjs wipe` | `reload.mjs <step>` (6 steps: preflight, suspend, nuke-pg, wipe, start, monitor) |

Pre-flight for both:
1. Confirm shadow OFF — durable via flipt-state git commit (not the API)
2. Suspend Flux for bitdex — **durable via talos-infra git commit** to `clusters/production/flux-system/apps/bitdex/bitdex.yaml` `suspend: true`. Live `flux suspend` / `kubectl patch` does NOT stick (see §9).
3. Scale StatefulSet to 0
4. Wait for pod gone
5. Note current image tag

Hard nuke replaces the old 9-step `dump → transfer → load → cursor-reset` orchestration. The `bitdex-sync` sidecar's `run_setup_v2` + boot dump pipeline (`src/bin/pg_sync.rs`) does all of that autonomously when the pod comes up against a clean PVC + clean PG state.

---

## 16. Common probes / cookbook

```bash
# Are pods up?
kubectl --context civit-datapacket -n bitdex get pods

# Is StatefulSet what we expect?
kubectl --context civit-datapacket -n bitdex get sts/bitdex \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="bitdex")].image}|{.spec.replicas}{"\n"}'

# What's pod-0's actual running image + restarts?
kubectl --context civit-datapacket -n bitdex get pod bitdex-0 \
  -o jsonpath='{.spec.containers[0].image}|ready={.status.containerStatuses[0].ready}|restarts={.status.containerStatuses[0].restartCount}{"\n"}'

# Pod resource usage
kubectl --context civit-datapacket -n bitdex top pod bitdex-0 --containers

# Why is pod Pending?
kubectl --context civit-datapacket -n bitdex describe pod bitdex-0 | grep -A 5 Events

# Tail logs
kubectl --context civit-datapacket -n bitdex logs bitdex-0 -c bitdex --tail=100
kubectl --context civit-datapacket -n bitdex logs -f bitdex-0 -c bitdex   # follow

# Port-forward + probe (port 4099 is convention; pick another if busy)
kubectl --context civit-datapacket -n bitdex port-forward pod/bitdex-0 4099:3000 &
sleep 3
curl -s http://localhost:4099/api/health
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:4099/api/indexes/civitai/config
curl -s http://localhost:4099/metrics | grep -E '^bitdex_query_duration'
```

### MSYS path mangling on Git Bash for Windows

```bash
# This rewrites /data/... to "C:/Program Files/Git/data/..." → 404
kubectl exec bitdex-0 -c bitdex -- ls /data/indexes/

# Workaround: prefix with MSYS_NO_PATHCONV=1
MSYS_NO_PATHCONV=1 kubectl exec bitdex-0 -c bitdex -- ls /data/indexes/
```

### Port-forward port conflicts

If `4099` is busy (lingering from another session), use any other port:
```bash
kubectl --context civit-datapacket -n bitdex port-forward pod/bitdex-0 5099:3000 &
```

`/tmp/pf.log` is a useful conventional location for port-forward stderr — captures bind errors when the port is already in use.

### Watching a long-running build in background

The skill's `watch-build` and Bash's `run_in_background` both block until done. Pattern I use:

```bash
gh run watch <id> --exit-status > /tmp/build.log 2>&1 &
# Continue with other work; check /tmp/build.log when notification fires
```

---

## 17. Compaction endpoint

Bitmap and docstore compaction can be triggered manually (also runs autonomously):

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"targets":["docs"]}' \
  http://localhost:4099/api/indexes/civitai/compact
# Returns 202 Accepted with task_id

# Check task progress
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:4099/api/tasks/<task_id>
```

Targets: `bitmaps`, `docs`, or omit `targets` for both. Workers default 4, max 32.

Real example: docstore compact at 107M records ran 5m40s, scanned 249,663 shards, compacted 212,090, skipped 37,573 clean. Doc cache hit rate climbs after compaction completes.

⚠ **Pod restart kills compact mid-flight** — task state is in-memory. Don't roll deploys while compact is running unless you don't care about the partial work.

---

## 18. Metrics + Prometheus

`/metrics` endpoint scrapes ~50ms+ at scale; don't poll faster than every 5s.

### Useful PromQL queries

```bash
node .claude/skills/deploy/cli.mjs metrics-query '<query>'
```

Examples:

```promql
# P50/P95/P99 query latency over 5 min
histogram_quantile(0.50, sum(rate(bitdex_query_duration_seconds_bucket[5m])) by (le))
histogram_quantile(0.95, sum(rate(bitdex_query_duration_seconds_bucket[5m])) by (le))
histogram_quantile(0.99, sum(rate(bitdex_query_duration_seconds_bucket[5m])) by (le))

# QPS
rate(bitdex_query_duration_seconds_count[1m])

# Doc cache hit ratio
rate(bitdex_doc_cache_hit_total[5m]) / (rate(bitdex_doc_cache_hit_total[5m]) + rate(bitdex_doc_cache_miss_total[5m]))

# Memory
bitdex_jemalloc_resident_bytes / 1024 / 1024 / 1024

# Concurrency cap rejections
rate(bitdex_queries_rejected_total[5m])

# Flush stalls
bitdex_flush_iter (heartbeat metric — should keep climbing)

# Sort merge slow events
rate(sort_merge_slow_total[1m])
```

### Local Prom doesn't scrape prod

`metrics-query` reads from local Prom (`host.docker.internal:3002`, scrapes only `bitdex-local` job). For prod numbers you have to:
- Use Grafana directly (Civitai's instance — URL not in this doc; ask Justin or check `reference_grafana_monitoring.md` in memory).
- Or stand up a Prom federation that scrapes prod.

To compute deltas yourself, snapshot `/metrics` to two files at T1 and T2, diff the bucket counts:

```bash
# T1
curl -s http://localhost:4099/metrics | grep -E '^bitdex_query_duration_seconds_(bucket|count|sum)' > /tmp/m1.txt
sleep 600
# T2
curl -s http://localhost:4099/metrics | grep -E '^bitdex_query_duration_seconds_(bucket|count|sum)' > /tmp/m2.txt

# Compute P50 delta from cumulative buckets:
# - Find half of (count_T2 - count_T1)
# - Find which le bucket crosses that
# - Linear-interpolate within the bucket
```

---

## 19. Steady-state baselines (last known)

| Version | RSS (GiB) | P50 (ms) | P95 (ms) | P99 (ms) | QPS | Notes |
|---|---|---|---|---|---|---|
| v1.0.85 | 14.5 | 0.23 | 15 | 36 | 89 | Pre-jemalloc baseline. |
| v1.0.97 | 28-32 | 0.5 | 50 | 200 | 60 | Cache leak + jemalloc retention. |
| v1.0.150 (clean PVC) | 8 | 0.225 | n/a | 470 (cold) | n/a | Best P99 in 40 releases. Clean PVC + shadow 100%. |
| v1.0.164 + cap=32 | 7.5-13 | 21-65 | 494-865 | 3030-3840 | 64-87 | Steady state under shadow load. Concurrency cap binding. |

The post-jemalloc swap (v1.0.156-159) cut RSS by 48% from glibc baseline.

---

## 20. Outstanding tasks (Aidan's queue, hand off to next agent)

(Internal numbering — hub TaskList may differ.)

- **#105** — Design local ops-replay rig. Donovan has partial; see `docs/_in/local-replay-rig-howto.md`.
- **#106** — Capture slow-query traces for Justin. Use `/api/traces` endpoint + tempo skill.
- **#108** — Fix test compile rot on v1.0.150-era main. ~18 unresolved imports in tests/examples (`bitdex_v2::docstore`, `process_csv_dump_direct`, `single_pass`, `FilterIndex::get_field_mut`). Pre-existing; CI red on every PR. Low priority but blocks clean CI.
- **#116** — Fix release.mjs regex for `-jemalloc` suffix (see §6 above).
- **#124** — Ship v1.0.165+ flush breakdown logging (Ava's commit `3151c6b`). Already shipped post-handoff as part of v1.0.165 stack.
- **#127** — Run relay canary steps 7-8 on v1.0.172+ once running in actual relay mode (Tom drove this Apr 25).

---

## 21. Incident pattern recognition

Things that have bitten us multiple times:

### Pod stuck Pending
- Almost always node memory request oversubscription. See §10.
- Local PVC pin → can't move. Lower pod request or evict tenant.

### Pod CrashLoopBackOff
- Check exit code in `kubectl describe pod`:
  - `Exit 137` = SIGKILL = OOM. Memory limit hit. Investigate `bitdex_jemalloc_*` metrics.
  - `Exit 1` from pg-sync = crash on startup. Recently was relay returning empty 200 to `/dumps` (fixed in v1.0.173 via PR #230).
  - `Exit 0` = clean shutdown, then restarted by readiness probe failure. Check `/api/health`.

### Pod CPU pegged 8/8
- Often a flush stall — high-cardinality field (`postId` 3290 values × 138 buckets = 65s save cycle). Logs show `save: filter postId in 65894ms`. Steady-state P50 is unaffected; tail latency spikes during the stall window.

### P50 climbing under load
- First check: is `BITDEX_MAX_QUERY_CONCURRENCY` set in pod env? Without it, no backpressure → tokio saturation → P50 → 4-5s.
- Second check: `bitdex_queries_rejected_total` rate. Non-zero = cap is biting, working as designed.

### Build fails / tag collision
- `git push --tags` rejected on stale tag. Push main first without `--tags`, then push the new tag separately. Orphan tags are functionally harmless (point at superseded commits) but break `--tags` pushes.

### Image won't pull
- Most likely the `v` prefix mismatch. Image tag is `1.0.NNN-jemalloc` (no v). See §6.
- Or: push to GHCR not yet replicated. Wait 30s + retry.

### After deploy: pod is on wrong (older) image
- Image automation overrode your manifest commit. The `# {"$imagepolicy": "flux-system:bitdex"}` marker on the image line. Remove it for `-jemalloc` tags. See §6.

---

## 22. Useful files / paths

```
.claude/skills/
├ deploy/
│  ├ cli.mjs          # main deploy CLI
│  ├ reload.mjs       # 6-step hard nuke orchestration (sync-v2 autonomous boot)
│  ├ skill.md         # entry point
│  ├ sql/             # bundled SQL assets
│  │  ├ nuke-pg-state.sql        # drop bitdex_* triggers + truncate ops/cursors
│  │  └ nuke-pg-state-retry.sql  # retry pass for hot-table deadlock losses
│  └ lib/
│     ├ kubectl.mjs   # K8s helpers
│     ├ prometheus.mjs
│     ├ csv-dump.mjs
│     └ sync-config.mjs
├ flipt/
│  ├ flipt.mjs        # flipt CLI + shadow on/off helpers
│  ├ SKILL.md
│  ├ .env.example
│  └ .env             # bundled read-only fetcher token
└ ...

docs/guide/
├ deploy-nukes.md          # soft vs hard nuke procedures
├ relay-flip-runbook.md    # relay window lifecycle (Jack's, on feat/relay → main)
├ deploy-monitoring-handoff.md  # this doc
└ ...

docs/_in/  (drafts / WIP)
├ relay-system-design.md   # Jack's V3 design (relay)
├ relay-deploy-patch.md    # K8s manifest patch for relay
├ measurement-plan-2026-04-24.md  # Donovan's perf plan
├ post-150-fast-forward-audit.md
└ v150-jemalloc-slow-queries.md

config/
├ sync-civitai.yaml       # source of truth for COPY queries (CSV dump)
└ ...

deploy/configs/
├ civitai-index.yaml      # bitdex config (filter_fields, sort_fields, cache, time_buckets)
├ sync-local-fulldump.toml
└ ...

# External (not in this repo)
C:/Dev/Repos/work/flipt-state/   # GitOps repo for Flipt feature flags
C:/Dev/Repos/work/talos-infra/   # GitOps repo for K8s manifests (Tom owns)
C:/Dev/Repos/work/model-share/   # Civitai web app — shadow comparator lives here
```

---

## 23. People + handoff context

- **Justin** — final authority on PRIMARY mode flips, hard infra changes that displace tenants. Currently has standing rule: "PRIMARY only Justin." Otherwise grants broad autonomy ("don't wait for me").
- **Scarlet** — coordinator. Final gate for relay V1 ship + perf PR merges. Owns hub TaskList. Mail her per-step on canary work.
- **Tom** — cluster apply auth + talos-infra owner. Knows openebs-hostpath quirks. Drives image bumps + Flux operations.
- **Donovan** — perf engineer. Owns the local rig + measurement plan. Inherited Ava's perf seat.
- **Jack** — relay V1 design + implementation. Owns `feat/relay` branch (merged to main as PR #229).
- **Ava** — parked. Original sort/prefilter/SSE work shipped in v1.0.158-164 era.

### Communication norms

- Voice via `mcp__hub-channel__speak` — every response. Justin hears via TTS.
- Status via `mcp__hub-channel__update_status` — keeps statusline / dashboard accurate.
- Mail per milestone for any multi-step deploy. **Don't go silent during a canary** — Scarlet will reassign if you don't surface signal within 20-30 min.
- TaskList for medium-life tracking. Not the same as hub-side IDs.

---

## 24. Knowledge gaps / known unknowns

Things I never fully closed out:

1. **Why prod runs single-replica, not the design's two.** PVC `data-bitdex-1` exists and is bound but has been unused for weeks. Manifest patches and runbooks reference both replicas. Real cluster: only `bitdex-0` runs. Unclear whether scale-to-2 is intentional next step or vestigial design.
2. **Aggressive-mode flip-back path.** Lower memory request to 1Gi during relay window (vs default 14Gi reserved-but-idle). Unproven in practice — only the default-safe path has been exercised. Risk: scheduler backfills the node with other tenants, then on flip-back the 14Gi bump goes Pending forever.
3. **Compaction during steady state.** Manual compact endpoint works, but autonomous compaction triggers + frequency haven't been measured against doc cache hit rate. May benefit from tuning (auto-compact when miss rate exceeds threshold).
4. **WAL retention vs relay window.** "WAL replay reseed" works only if relay window < WAL retention. Actual WAL retention setting in PG isn't documented here — confirm with Tom or DBA before a long relay window.
5. **Talos-infra repo location** isn't in this repo. Path varies per machine. Successor agent should ask Tom for the canonical path on their setup.

---

## 25. When in doubt

- Read `kubectl describe pod bitdex-0` — it's verbose but the events section usually has the answer.
- `kubectl logs --previous` shows the prior container's logs after a crash.
- Check `flux get kustomization bitdex` to confirm whether your manifest commit has been reconciled.
- Use the `fix-session` skill if your own session breaks (orphan tool_results, oversize transcript).
- Ask Tom before doing anything cluster-side that displaces other workloads.
- Default to GitOps over `kubectl set` for anything that needs to persist.
- Default to mail per step over silence during a coordinated deploy.
