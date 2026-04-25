# Relay V1 — K8s Manifest Patch

**For review only. Apply gates on Aidan / Justin sign-off / Tom executing.**

> **✅ Tom-cleared PVC permission gate (2026-04-25):** existing `data-bitdex-0`
> PVC has `/data/wal/` and `/data/indexes/` as 755 root-owned from prior
> deploys; UID 65532 can't write without intervention. Mitigation chosen
> (option b from prior list): pod-template `securityContext.fsGroup:
> 65532` + `fsGroupChangePolicy: OnRootMismatch`. Kubelet chowns the
> volume to GID 65532 on first mount only; subsequent mounts are no-ops.
> Manifest below includes the fsGroup block. Tom standing by to
> re-validate the patched manifest before apply.
>
> **PVC adoption invariants (do not change on re-create):**
> - `volumeClaimTemplates[0].metadata.name: data`
> - `spec.storageClassName: openebs-hostpath-bitdex-nvme`
> - `spec.accessModes: [ReadWriteOnce]`
> - `spec.resources.requests.storage: 200Gi` (downsizing fails adoption — K8s
>   requires existing PVC ≥ requested; growing is OK with a manual edit
>   but breaks immutability of the spec field on most StatefulSets)
>
> Changing any of these breaks adoption of the existing
> `data-bitdex-0` PVC and forces a new PVC + lost data.

Target file in talos-infra: `clusters/production/apps/bitdex/deployment.yaml`

> **Important — re-create, not patch.** As of 2026-04-25 Aidan reports
> `kubectl get sts -n bitdex` returns `No resources found`; the
> StatefulSet is not currently in the cluster (PVCs intact). The first
> apply of this manifest **re-creates** the StatefulSet from scratch
> rather than patching an existing object. Confirm cluster state before
> apply.
>
> **Flux is suspended.** Tom suspended the `bitdex` Kustomization on
> Apr 12 (per `clusters/production/apps/bitdex/README.md`). Verify with
> `flux get kustomization bitdex`. Resume only after the canary checks
> in the flip runbook pass on flip-back.
>
> **Single replica.** V1 production runs one pod (`bitdex-0`).
> `data-bitdex-1` PVC exists but is unused. Manifest below assumes
> `replicas: 1`.

---

## Changes Summary

1. Add `BITDEX_MODE` env var (default `server`) to the `bitdex` container — relay toggle.
2. Add `BITDEX_RELAY_CONFIG` env var (optional override) to the `bitdex` container.
3. Add `BITDEX_ADMIN_TOKEN` secret-ref to the `bitdex` container — relay reuses the existing secret.
4. Add PodSecurity block (pod-level + per-container) to fix Tom's flagged warnings.
5. Add `fsGroup: 65532` + `fsGroupChangePolicy: OnRootMismatch` at pod-template level so kubelet chowns the existing `data-bitdex-0` PVC on first mount (Tom-cleared mitigation).
6. Keep `requests.memory: 14Gi` — scheduling-trap mitigation per Gemini review.
7. Preserve `volumeClaimTemplates` invariants (name `data`, storageClass `openebs-hostpath-bitdex-nvme`, accessMode `ReadWriteOnce`) so the existing PVC is adopted, not replaced.

---

## Unified Diff

Apply against the StatefulSet spec in `clusters/production/apps/bitdex/deployment.yaml`.
Surrounding context lines included for safe patch application.

```diff
--- a/clusters/production/apps/bitdex/deployment.yaml
+++ b/clusters/production/apps/bitdex/deployment.yaml
@@ -1,6 +1,8 @@
 # StatefulSet for BitDex server + pg-sync sidecar
 # Managed by Flux CD — do not edit directly; push to talos-infra
 # Node affinity: talos-fq9-f3k (NVMe storage)
+# Relay toggle: set BITDEX_MODE=relay to enter relay mode (no bitmap engine).
+# See docs/guide/relay-flip-runbook.md for the full flip + reseed procedure.
 ---
 apiVersion: apps/v1
 kind: StatefulSet
@@ ... @@
 spec:
   template:
     spec:
+      # PodSecurity — Tom's flagged warnings, same-commit as relay rollout.
+      # fsGroup + OnRootMismatch chowns the PVC to GID 65532 on first
+      # mount (existing /data/wal /data/indexes are 755 root-owned).
+      # Subsequent mounts skip the chown, so cost is one-time.
+      securityContext:
+        runAsNonRoot: true
+        fsGroup: 65532
+        fsGroupChangePolicy: OnRootMismatch
       containers:
         - name: bitdex
+          securityContext:
+            runAsNonRoot: true
+            runAsUser: 65532
+            runAsGroup: 65532
+            allowPrivilegeEscalation: false
+            capabilities:
+              drop: ["ALL"]
+            seccompProfile:
+              type: RuntimeDefault
           env:
+            # ── Relay toggle ──────────────────────────────────────────────
+            # Default: server (existing behavior, no change).
+            # Set to "relay" via:
+            #   kubectl set env statefulset/bitdex BITDEX_MODE=relay -n bitdex
+            # Flip BOTH replicas together — never mix modes across replicas.
+            - name: BITDEX_MODE
+              value: "server"
+            # Optional: override default relay config path
+            # (/etc/bitdex/relay-config.yaml is shipped in the image).
+            # Mount a ConfigMap here if you need a custom relay config.
+            # Leave unset to use the image default.
+            - name: BITDEX_RELAY_CONFIG
+              value: "/etc/bitdex/relay-config.yaml"
+            # Admin token — relay reuses the same secret as the server.
+            # Required: relay refuses to start if token is unset and any
+            # route has auth: bearer (SSE egress always requires bearer).
+            - name: BITDEX_ADMIN_TOKEN
+              valueFrom:
+                secretKeyRef:
+                  name: bitdex-secrets
+                  key: BITDEX_ADMIN_TOKEN
           # ... (existing env vars below, unchanged) ...
           resources:
             requests:
               # KEEP AT 14Gi — scheduling-trap mitigation.
               # Lowering during relay window lets scheduler backfill the node;
               # flip-back to server mode then goes Pending indefinitely.
               # See docs/guide/relay-flip-runbook.md §Resource Budget.
               # Aggressive mode (opt-in only): see runbook §Aggressive Mode.
               memory: "14Gi"
             limits:
               memory: "32Gi"
         - name: pg-sync
+          securityContext:
+            runAsNonRoot: true
+            runAsUser: 65532
+            runAsGroup: 65532
+            allowPrivilegeEscalation: false
+            capabilities:
+              drop: ["ALL"]
+            seccompProfile:
+              type: RuntimeDefault
           # ... (existing pg-sync config unchanged) ...
```

---

## Applying This Patch

This is a review artifact. When ready to apply:

1. Clone / pull talos-infra.
2. Apply the diff to `clusters/production/apps/bitdex/deployment.yaml`.
3. Commit with a message like `feat(bitdex): relay toggle env + PodSecurity housekeeping`.
4. Push to talos-infra. Flux reconciles within ~1 minute.
5. Verify pods are running: `node .claude/skills/deploy/cli.mjs status`

**Do NOT use `kubectl apply` directly** — Flux will revert it within 30s-5min. The talos-infra git push is the canonical channel.

---

## Notes for Aidan

- The `BITDEX_ADMIN_TOKEN` secret-ref uses `name: bitdex-secrets` / `key: BITDEX_ADMIN_TOKEN` — confirmed from `kubectl.mjs` and measurement-plan doc (secret is in `bitdex` namespace).
- If your actual StatefulSet manifest uses a different secret name for the admin token (check the live manifest), update the `secretKeyRef.name` accordingly. The key referenced in `server.rs:250` is `BITDEX_ADMIN_TOKEN` env var — that part is correct regardless.
- PodSecurity on `pg-sync` sidecar: confirm UID 65532 is compatible with the pg-sync binary's file access requirements (DATABASE_URL, sync config mount). If the sidecar writes to `/data`, confirm PVC volume is group-writable or owned by 65532.
- The `BITDEX_RELAY_CONFIG` explicit value matches the image default path (confirmed in `src/bin/server.rs:288` and the Dockerfile COPY destination). Setting it explicitly makes the config path auditable in the manifest without adding config drift.
