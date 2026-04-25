# BitDex Relay Flip Runbook

**Owner:** Aidan (deploy authority)  
**Design ref:** `docs/_in/relay-system-design.md` (V3)  
**Manifest patch:** `docs/_in/relay-deploy-patch.md`

This runbook covers the full relay window lifecycle: pre-flip preconditions, flip commands, what to watch, mandatory reseed on flip-back, and canary verification before declaring the window closed.

---

## Overview

`BITDEX_MODE=relay` swaps the bitmap engine for a pure HTTP→SSE relay. The pod still accepts all traffic on port 3000, passes K8s probes (`GET /api/health`), and PG-sync continues advancing its cursor — but no bitmap work happens. Local rigs subscribe to `GET /events/{channel}` and consume the relayed ops/queries.

**The relay window is a controlled experiment. Flip-back is not idempotent without a reseed step.**

---

## Pre-flip Preconditions Checklist

Complete ALL of these before issuing the flip command. If any item is not met, do not flip.

- [ ] **Shadow-mode comparator OFF.** Per Justin's standing rule for model-share ("shadow ON only, not primary") + V1 runbook constraint, the shadow-mode comparator (`src/server/bitdex/compare.ts` in model-share) must be **disabled** for the duration of the relay window. With shadow off, the comparator never sees the `tee_mode:true` stub, so no divergence alerts fire. Confirm via the model-share feature flag dashboard that shadow comparison is disabled before flipping. (V2 alongside-mode will need a model-share `tee_mode` skip path so comparator + relay can coexist; tracked as `docs/_in/v2-model-share-tee-skip-spec.md` if drafted.)
- [ ] **Local rig connected BEFORE flip.** Subscribe to `https://bitdex.civitai.com/events/queries` and `/events/ops` with a valid bearer token before issuing `kubectl set env`. Events emitted between the flip and your subscribe are lost (no replay). Confirm the SSE stream is receiving events.
- [ ] **Capture decision made.** If the relay window data must be guaranteed durable (e.g. for offline replay or verifier), enable capture in the relay config by mounting a ConfigMap with `capture.enabled: true`. If iteration-only with no replay need, capture can stay off — but accept that SSE lag = data loss.
- [ ] **WAL retention confirmed > planned relay window.** The cheap reseed path on flip-back (WAL replay) only works if the relay window duration is less than the WAL retention window. Confirm WAL retention from PG before flipping. If the window might exceed retention, plan for full bulk reload from the start.
- [ ] **Relay image confirmed built and deployed.** The running image must include the `feat/relay` code (mode dispatch in `src/bin/server.rs`, `src/relay/` module). Verify: `node .claude/skills/deploy/cli.mjs status` shows the expected image tag.
- [ ] **Record the current PG-sync cursor value.** Before flip, capture the cursor position for the running pod. This is the value you reset to for WAL replay on flip-back.
  ```bash
  kubectl --context civit-datapacket exec -n bitdex bitdex-0 -c bitdex -- \
    curl -s localhost:3000/api/indexes/civitai/cursors/pg-sync-bitdex-0
  ```
  Record the cursor value in your ops notes. You need this for the WAL replay path on flip-back. (V1 = single replica; if scaled to two later, also capture `pg-sync-bitdex-1`.)
- [ ] **Resource requests NOT lowered (default-safe mode).** `requests.memory: 14Gi` must remain in the StatefulSet during the relay window. Do NOT lower it unless you are explicitly opting into aggressive mode (see §Resource Budget below). The scheduling trap on flip-back will cause a production outage.
- [ ] **Operator acknowledges PG-sync divergence invariant.** During the relay window, PG-sync advances its cursor but the BitDex WAL receives nothing. Flip-back requires a mandatory reseed before BitDex is authoritative. Confirm this is understood and the reseed plan is selected (WAL replay vs full bulk reload).
- [ ] **Single replica (V1).** Production runs one pod (`bitdex-0`); `data-bitdex-1` PVC exists but is unused. The `kubectl set env statefulset/...` command flips the single pod via rolling restart. If V1 ever scales to two replicas (Justin coordinates), revisit this checklist for the multi-replica invariant: both flip together, never split modes across replicas.
- [ ] **Flux Kustomization for `bitdex` already suspended.** Tom suspended on Apr 12; documented in `clusters/production/apps/bitdex/README.md`. Verify with `flux get kustomization bitdex`. Resume only after the canary checks pass on flip-back, not before.
- [ ] **StatefulSet may not currently exist in cluster.** As of Apr 25, `kubectl get sts -n bitdex` returned `No resources found` (PVCs intact and Bound). The manifest patch is a re-create on first apply, not a patch on an existing object. Confirm before apply via `kubectl get sts -n bitdex`; if absent, the apply seeds from scratch.
- [ ] **PG-sync sidecar UID 65532 PVC permission — Tom cleared (2026-04-25).** Mitigation in place: pod-template `securityContext.fsGroup: 65532` + `fsGroupChangePolicy: OnRootMismatch`. Kubelet chowns `data-bitdex-0` to GID 65532 on first mount only; subsequent mounts are no-ops. Manifest patch top-of-file notes the cleared gate. Tom re-validates the patched manifest before apply.

---

## Flip Command

```bash
# Flip the single pod to relay mode.
# Changes the StatefulSet template and triggers a rolling pod restart.
kubectl --context civit-datapacket set env statefulset/bitdex \
  BITDEX_MODE=relay -n bitdex
```

Note: V1 production is a single replica (`bitdex-0`). If the StatefulSet does not exist in the cluster (see precondition checklist), the apply path is `flux get kustomization bitdex` → resume → push talos-infra → reconcile re-creates from manifest. `kubectl set env` against a non-existent StatefulSet returns an error; either (a) apply the manifest patch first to seed the StatefulSet, then `set env`, or (b) include `BITDEX_MODE` directly in the manifest patch and skip `set env`.

**Note on Flux:** Flux CD manages this StatefulSet and will revert `kubectl` changes within 30s–5min unless the env change is also committed to talos-infra. For a temporary relay window, you have a choice:

- **Ephemeral (temporary, Flux reverts when reconciled):** Use the `kubectl set env` command above. Flux will eventually revert it. Before Flux reconciles, scale your window accordingly or suspend Flux.
- **Persistent (talos-infra commit):** Commit `BITDEX_MODE=relay` in the manifest to talos-infra. Flux applies it and holds it. Flip back by committing `BITDEX_MODE=server`.

For relay windows, the talos-infra commit path is safer — Flux reversion mid-window would silently restart the server in bitmap mode while your rig is still subscribed, which is confusing. Coordinate with Arabella if you need Flux suspended or a talos-infra commit.

**Verify the flip:**

```bash
# Watch pods restart
kubectl --context civit-datapacket get pods -n bitdex -w

# After the pod is Running+Ready, check health response includes mode=relay
curl -s https://bitdex.civitai.com/api/health | jq .
# Expected: {"status":"ok","mode":"relay"}
```

---

## Observability During the Relay Window

### Metrics to watch

| Metric | Type | What it tells you |
|---|---|---|
| `relay_emit_total{channel,route}` | counter | Events being emitted per channel. Should be non-zero and growing if traffic is flowing. |
| `relay_drops_total{channel,reason}` | counter | Lag-induced drops. Zero is ideal; non-zero means your local rig is lagging. |
| `relay_sse_subscribers{channel}` | gauge | Active SSE connections. Must be >0 for emit gate to fire (unless capture is on). |
| `relay_sse_lagged_events_total{channel}` | counter | Cumulative lag events. Rising = your subscriber is falling behind. |
| `relay_request_duration_seconds{route}` | histogram | Handler latency. Should stay sub-millisecond; relay does no bitmap work. |
| `relay_capture_disk_bytes` | gauge | Capture dir usage if capture is enabled. Watch against `max_total_bytes`. |

Query in Grafana or via the deploy CLI:

```bash
node .claude/skills/deploy/cli.mjs metrics-query 'relay_emit_total'
node .claude/skills/deploy/cli.mjs metrics-query 'relay_drops_total'
node .claude/skills/deploy/cli.mjs metrics-query 'relay_sse_subscribers'
```

### Pod health

```bash
node .claude/skills/deploy/cli.mjs status
node .claude/skills/deploy/cli.mjs pg-sync-health
```

RSS should be well under 1 GiB in relay mode (no bitmap engine). If RSS climbs toward 14 GiB, something is wrong — relay should be ~200 MiB steady-state.

### PG-sync cursor advancement

PG-sync continues polling and advancing its cursor during the relay window. Confirm it is running normally:

```bash
node .claude/skills/deploy/cli.mjs pg-sync-logs bitdex-0 50
# If V1 ever scales to two replicas, also: pg-sync-logs bitdex-1 50
```

The sidecar should show periodic poll cycles. Cursor advancement is expected and is what creates the reseed requirement on flip-back.

---

## Flip-back Command

```bash
# Return the pod to server mode.
kubectl --context civit-datapacket set env statefulset/bitdex \
  BITDEX_MODE=server -n bitdex
```

If you used the talos-infra commit path to flip in, revert the commit and push.

**Wait for the pod to be Running+Ready before proceeding to the reseed step.**

```bash
kubectl --context civit-datapacket rollout status statefulset/bitdex -n bitdex
```

---

## Mandatory Reseed on Flip-back

**This step is not optional.** PG-sync advanced its cursor during the relay window. The BitDex server WAL received none of those ops. Without a reseed, BitDex serves silently stale or missing data.

Choose the cheapest applicable path:

### Path A — WAL Replay (default, 5–10 min)

Use this when: `(relay window duration) < (WAL retention window)`.

1. Stop the pg-sync sidecar temporarily (scale PG-sync out, or patch sidecar env to stop polling — Aidan's call on the cleanest mechanism without disrupting the StatefulSet).
2. Reset the PG-sync cursor to the pre-flip-in value you recorded in the preconditions step:
   ```bash
   # Replace <cursor_value_before_flip> with the recorded cursor
   curl -X PUT https://bitdex.civitai.com/api/indexes/civitai/cursors/pg-sync-bitdex-0 \
     -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"value": <cursor_value_before_flip>}'
   # If V1 ever scales to two replicas, repeat for pg-sync-bitdex-1
   ```
3. Restart pg-sync (or re-enable polling). WAL reader replays from the reset cursor. Monitor with `pg-sync-logs` until the cursor advances past the flip-back point and stabilizes.
4. WAL replay is complete when `bitdex_wal_ops_processed_total` metric matches the current PG-sync cursor (or is within a small batch tolerance).

### Path B — Full Bulk Reload (fallback, 60–90 min)

Use this when: WAL retention is insufficient, WAL is corrupt, or WAL replay verification fails.

Full reload wipes both PVCs and re-ingests from PG. Use the reload script:

```bash
node .claude/skills/deploy/reload.mjs suspend     # Suspend Flux, scale to 0
node .claude/skills/deploy/reload.mjs wipe        # Wipe bitmaps/docs/cursors on both PVCs
node .claude/skills/deploy/reload.mjs dump        # Dump fresh CSVs from PG
node .claude/skills/deploy/reload.mjs transfer    # Copy CSVs to both PVCs
node .claude/skills/deploy/reload.mjs load        # Run bulk load on both PVCs
node .claude/skills/deploy/reload.mjs cursor-reset # Write cursor files, update PG
node .claude/skills/deploy/reload.mjs start       # Scale to 2, verify
node .claude/skills/deploy/reload.mjs resume      # Unsuspend Flux
```

See `docs/HANDOFF.md §Bulk Reload` and the reload script's inline help for details. The critical pitfall: the bulk loader seeds the cursor at the current outbox head, not CSV dump time. `cursor-reset` step handles this.

**The deploy skill reload path covers the full wipe-and-reload procedure.** No new skill is needed for relay flip-back. Path A (WAL replay) requires only cursor reset via the HTTP admin API, which the deploy CLI's `config` commands can handle.

---

## Re-enabling Shadow Comparison After Flip-back

Once flip-back + reseed + canary checks all pass, shadow-mode comparison can be re-enabled. **Do not** re-enable shadow before the canary checks pass — the comparator would run against potentially stale BitDex output and produce divergence noise that obscures the actual reseed status.

Re-enable steps:
1. Confirm all four canary checks below pass.
2. Flip the model-share shadow-comparison feature flag back ON via the model-share dashboard.
3. Watch divergence rate for 5 minutes. If rate is at or below the pre-window baseline, leave shadow ON. If higher, flip shadow back OFF and investigate (likely an incomplete reseed).

V2 alongside-mode will require a model-share `compare.ts` skip path so shadow can stay ON during a relay window — tracked separately. V1 just toggles shadow OFF for the window.

---

## Flip-back Canary Checklist

Run these checks after flip-back + reseed completes. Do not declare the window closed or remove the Meili fallback until all four pass.

**If any check fails: do not advertise BitDex as authoritative. Keep Meili as fallback. Investigate root cause and re-reseed if needed.**

1. **Both pods Running + Ready**

   ```bash
   kubectl --context civit-datapacket get pods -n bitdex
   ```

   `bitdex-0` must show `Running` status and `1/1` or `2/2` ready containers (server + pg-sync sidecar both healthy). V1 = single replica.

2. **WAL reader caught up — ops processed matches PG cursor**

   Query Prometheus:

   ```bash
   node .claude/skills/deploy/cli.mjs metrics-query 'bitdex_wal_ops_processed_total'
   node .claude/skills/deploy/cli.mjs pg-sync-health
   ```

   The `bitdex_wal_ops_processed_total` counter should be advancing. Cross-check the current PG-sync cursor (from `pg-sync-health`) against the BitDex WAL position. If the gap is more than one poll batch (~5000 ops) and is not shrinking, WAL replay is stalled — investigate pg-sync logs.

   For a clean pass: the gap is zero or within one batch, and both metrics are stable (not diverging).

3. **Shadow comparator divergence rate at or below pre-window baseline (5-min sample)**

   In Grafana, check the shadow-mode divergence metric over a 5-minute window since flip-back. The divergence rate must be at or below what it was before the relay window started.

   Metric (confirm exact name with Ivanna/Arabella — likely `bitdex_shadow_divergence_total` or similar on the model-share side):

   ```bash
   node .claude/skills/deploy/cli.mjs metrics-query 'bitdex_shadow_divergence_total[5m]'
   ```

   If divergence is higher than the pre-window baseline, BitDex is still serving stale data. Do not remove Meili fallback.

4. **Smoke query returns expected count + correct `sortAt` ordering**

   Run a known-good safety-prefix query and verify result count and ordering:

   ```bash
   curl -s https://bitdex.civitai.com/api/indexes/civitai/query \
     -H "Content-Type: application/json" \
     -d '{"filters":[{"field":"nsfwLevel","op":"lte","value":1}],"sort_field":"sortAt","sort_desc":true,"limit":10}' \
   | jq '{total_matched: .total_matched, first_sortAt: .documents[0].sortAt}'
   ```

   Check:
   - `total_matched` is in the expected range (~50–90M for nsfwLevel ≤ 1 at current dataset size). A significantly lower value suggests missing data from the relay window.
   - `documents[0].sortAt` is a recent timestamp (within the last few days), confirming sort ordering is correct and `sortAt` data is present. A very old or zero `sortAt` indicates a data regression.

   If either check fails, the reseed is incomplete. Re-run the reseed (Path B if Path A was used and failed).

---

## Rollback Steps if Mid-window Issues Arise

If something goes wrong during the relay window (relay crashes, SSE stream breaks, pod OOM):

1. **Flip back immediately:**
   ```bash
   kubectl --context civit-datapacket set env statefulset/bitdex BITDEX_MODE=server -n bitdex
   ```

2. **Wait for pods Running+Ready**, then execute the mandatory reseed (above). Do not skip this step even if the relay window was short.

3. **Check relay logs for the failure cause:**
   ```bash
   kubectl --context civit-datapacket logs -n bitdex bitdex-0 -c bitdex --tail=100 --since=10m
   ```

4. **Run the flip-back canary checklist** before resuming normal operations.

---

## Resource Budget

### Default mode (safe flip-back, no scheduling trap)

Keep `requests.memory: 14Gi` throughout the relay window. The relay process uses ~200 MiB, leaving ~13.8 GiB reserved-but-idle on the node. This is the cost of guaranteed flip-back without eviction risk.

**Do NOT lower memory requests during the relay window** unless opting into aggressive mode below.

### Aggressive mode (opt-in, resource-savings)

Only choose this when the freed capacity is genuinely needed by other workloads AND the eviction-on-flip-back risk is explicitly accepted.

**Opt-in procedure (two separate operator actions):**

1. Flip to relay: `kubectl set env statefulset/bitdex BITDEX_MODE=relay -n bitdex`
2. Lower requests (explicit "I accept eviction risk" gate):
   ```bash
   kubectl --context civit-datapacket patch statefulset bitdex -n bitdex \
     --type=json \
     -p='[{"op":"replace","path":"/spec/template/spec/containers/0/resources/requests/memory","value":"1Gi"}]'
   ```

**Flip-back from aggressive mode:**

1. Revert resources first:
   ```bash
   kubectl --context civit-datapacket patch statefulset bitdex -n bitdex \
     --type=json \
     -p='[{"op":"replace","path":"/spec/template/spec/containers/0/resources/requests/memory","value":"14Gi"}]'
   ```
2. Wait for the scheduler to find 14 GiB capacity (may take time if node was backfilled by other pods).
3. Then flip `BITDEX_MODE=server`.
4. Execute mandatory reseed.
5. Run canary checklist.

If the node was backfilled and 14 GiB is unavailable, the pod goes `Pending`. Options: evict a lower-priority workload, drain the node temporarily, or wait for natural pod turnover.

---

## Known Pitfalls

- **Flux reversion:** Flux reconciles every 30s–5min. A `kubectl set env` flip will be reverted unless you also commit to talos-infra or suspend Flux. Mid-window Flux reversion puts the pod back in server mode while your rig is still subscribed — data your rig received is not in BitDex. Coordinate with Arabella.
- **Mixed-mode replicas (V1: not applicable, single replica).** If V1 scales to two replicas later, never let one replica be in server mode while the other is in relay mode. The Service would round-robin, producing inconsistent responses (real query results from one pod, relay stubs from the other). The `kubectl set env statefulset/bitdex` command changes the pod template and triggers rolling restart of all replicas together — verify all pods have restarted before consuming from the SSE stream.
- **BITDEX_ADMIN_TOKEN required:** The relay refuses to start if the admin token is unset and any route requires bearer auth (SSE egress always does). Confirm the `bitdex-secrets` secret is populated. If the pod is crash-looping on relay mode, check logs for `admin bearer required`.
- **PVC contention:** The relay pod does not access the PVC (no bitmap engine), so PVC contention is not an issue during the relay window itself. It becomes relevant again on flip-back when the server restarts and loads from disk.
- **SSE subscriber required for emit gate:** If no SSE subscriber is connected and capture is disabled, route handlers skip the emit step entirely (`receiver_count() == 0` gate). Events are not buffered — they are dropped. Your local rig must be subscribed before the flip completes.
