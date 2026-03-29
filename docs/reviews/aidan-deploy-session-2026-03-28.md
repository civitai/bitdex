# Session Review: aidan — PG-Sync V2 Deploy Prep, CSV Management, Deploy Skill

**Session:** 8a94e17e-a73a-4849-884a-7e19305ede5d
**Agent:** aidan (aidan-phase2.5-metrics)
**Date:** 2026-03-28 (session spans ~4:41 PM through ~10:55 AM the next morning)
**Reviewer:** Conversation Reviewer (spawned by Dakota)

---

## Key Decisions

- **Decision:** Build a `tunnel` command into the deploy skill for self-service PG port-forward.
  **Rationale:** Scarlet's team needed access to the CNPG replica to deploy V2 triggers and validate trigger output. Aidan had previously been setting this up manually via `kubectl port-forward`. Justin approved making it self-service.
  **Impact:** Any agent can now run `node .claude/skills/deploy/cli.mjs tunnel pg` and get `postgresql://bitdex@localhost:5432/civitai` without Aidan's involvement.

- **Decision:** Use a transfer pod (running on the same node as BitDex) to dump CSVs directly to the PVC rather than going through a local machine.
  **Rationale:** kubectl exec pipes are unreliable for binary (gzip) streams over ~1 GB due to websocket framing corruption. The transfer pod writes to the NVMe PVC at line rate (no double-hop through a dev machine).
  **Impact:** This is the canonical approach for the `csv-dump` deploy skill command. Aidan discovered the pipe approach the hard way (multiple corrupted files) before landing on the transfer pod pattern.

- **Decision:** COPY queries for CSV dumps must use separate `-c` flags for `SET statement_timeout = 0` and the `COPY` command — never combined in a single `-c` string.
  **Rationale:** When combined, psql outputs `SET\n` as the first line before the CSV data. This pollutes every CSV (the dump processor sees `SET` as the first row). Worse, when piped to gzip, the `SET\n` output before the gzip stream corrupts the gzip file header entirely — making the gz file unreadable.
  **Impact:** All 9 initial CSVs dumped during this session were affected. The images.csv.gz was discovered corrupt only when trying to verify/decompress it. The fix is to pass `-c "SET statement_timeout = 0" -c "COPY ..."` separately.

- **Decision:** V2 CSV dump SQL must come from `docs/design/sync-config-civitai.yaml` (`dump_phases[].copy_query` fields), not from the hardcoded `src/pg_sync/copy_queries.rs`.
  **Rationale:** The V2 design principle requires config-driven field mappings. `copy_queries.rs` is V1 hardcoded SQL that will diverge from the actual schema. Scarlet confirmed only the tags table required a different query for V2 (`TagsOnImageNew` with 3 columns including `attributes`).
  **Impact:** The `COPY_TABLES` array in `csv-dump` is currently marked as a TODO — it needs to be updated to parse the sync config YAML. The 8 non-tags CSVs from this session were confirmed correct by Scarlet.

- **Decision:** K8s startup probe `failureThreshold` should be raised from 60 (5 min) to 180 (15 min) for V2 fresh loads.
  **Rationale:** PR #78 fixed the root cause (O(n^2) BoundStore eviction storm), but V2 fresh loads start with a larger BoundStore and may run close to the 5-min limit. 15 min provides safety margin.
  **Impact:** Arabella (talos-infra) was assigned the one-line change. Without this, V2 fresh loads could crash-loop during startup even though the server is healthy.

---

## Performance Findings

- **Finding:** Transfer pod CSV dump (NVMe-direct) vs kubectl pipe timing comparison.
  **Numbers:** images.csv.gz (7.4 GB): ~14 min via transfer pod. tags.csv.gz (17-21 GB): ~38 min V1 / ~53 min V2 (extra `attributes` column). Small tables (<344 MB): under 1 min each. Total 9-table dump: ~53 min.
  **Context:** Transfer pod runs on `talos-fq9-f3k` (same node as BitDex PVC `data-bitdex-0`). PG service DNS: `cnpg-cluster-nvme0-rw.cnpg-database.svc`.

- **Finding:** kubectl exec binary streaming is unreliable for files larger than ~1 GB.
  **Numbers:** Two separate images.csv.gz downloads (7.4 GB) both produced corrupt output. Text streaming (gunzip on pod side, raw CSV pipe) works but runs at ~200 MB/min — 14 GB takes ~70 min.
  **Context:** Applies to any kubectl exec pipe over the websocket connection. Only affects downloads to a local machine. Pod-to-PVC writes (within the cluster) are not affected.

- **Finding:** Production RSS was at 28.4 GB (87% of 32 GB limit) at session start and held flat throughout.
  **Numbers:** 24.6 → 25.3 → 28.4 GB observed across session. RSS-aware eviction (PR #83) + jemalloc `dirty_decay_ms:0` held it stable.
  **Context:** The combination of both fixes was required — neither alone was sufficient to prevent OOM. Before, the pod OOMed within 90 min. After both PRs, it has been stable for days.

- **Finding:** V2 tags table has grown since the V1 dump.
  **Numbers:** V1 dump: 17 GB compressed (63 GB uncompressed). V2 dump (with `attributes` column): 21 GB compressed. The table is larger than expected and growing at ~250-360 MB/min write rate during dump.
  **Context:** The `TagsOnImageNew` table includes a third column (`attributes`) not present in the V1 query. This column adds ~4 GB compressed (~24% increase).

---

## Gotchas Discovered

- **Gotcha:** `SET statement_timeout = 0; COPY ...` in a single psql `-c` flag corrupts gzip output.
  **Root cause:** psql emits `SET\n` to stdout before the COPY data. When piped to `gzip`, the first 4 bytes of the gz stream are `SET\n` instead of the gzip magic bytes. The file appears to write successfully but fails validation.
  **Prevention:** Always use separate `-c` flags: `psql -c "SET statement_timeout = 0" -c "COPY ... TO STDOUT"`. The `SET` output then goes to stderr. The deploy skill `csv-dump` command has this fix applied; older manually-created PVC files do not.

- **Gotcha:** The `postgres` user on the CNPG pod has a 5-minute statement_timeout.
  **Root cause:** Default timeout is set cluster-wide. The `civitai` user has a 120s timeout (discovered earlier). The `postgres` user was assumed unlimited but is also capped at 5 min.
  **Prevention:** Always prepend `SET statement_timeout = 0` to any long-running COPY query. Small tables (<344 MB) complete within 5 min; images (~14 GB) and tags (~63 GB) do not.

- **Gotcha:** Peer auth only works on the CNPG pod itself. Password auth is required for network connections (transfer pod → PG service).
  **Root cause:** PostgreSQL peer auth requires a matching OS user. The transfer pod uses the postgres image which has the `postgres` OS user, but connecting over TCP requires password auth via the `bitdex` secret.
  **Prevention:** Use `kubectl get secret -n bitdex bitdex -o jsonpath='{.data.DATABASE_URL}' | base64 -d` to retrieve the DATABASE_URL (includes password). Shell quoting of the password can be tricky — pass it as an environment variable, not inline in the command.

- **Gotcha:** `kubectl cp` cannot copy files cross-namespace.
  **Root cause:** kubectl cp only works within the same namespace. Copying from `cnpg-database/cnpg-cluster-nvme0-1` to `bitdex/bitdex-0` requires a different approach.
  **Prevention:** The correct pattern is the transfer pod: spin up a pod in the target namespace (`bitdex`) that mounts the target PVC and connects to PG via service DNS. No cross-namespace copy needed.

- **Gotcha:** Local `data/load_stage/` contained 81 GB of old uncompressed V1 CSVs from the March 14 scatter-gather run, not the new compressed V2 files.
  **Root cause:** The new compressed downloads (7-character filenames like `images.csv.gz`) landed in the current worktree's `data/load_stage/`, which was already populated with the old V1 data. The old data was not cleaned up before the new dump.
  **Prevention:** Before running a fresh dump, delete or move the old `data/load_stage/` contents. Check disk usage with `df -h` before starting. The PVC had 1.5 TB free but the local dev machine had only 123 GB free at one point (94% full).

- **Gotcha:** Old `data/indexes/` directory on the dev machine can grow to hundreds of GB from test benchmark runs.
  **Root cause:** Benchmark runs write bitmap data to `data/indexes/` and are rarely cleaned up.
  **Prevention:** Before any disk-intensive operation, check `data/indexes/`, `data/captures/`, `data/debug/`, and any `test_*/` directories. Rust debug builds (`target/debug/`) also accumulate. `captures/` (30 GB) and `debug/` (37 GB) were safe to delete.

- **Gotcha:** `kubectl exec` background tasks have a local connection timeout but the remote process continues independently.
  **Root cause:** The task notification system kills the local connection after ~10 min, but the remote `kubectl exec` process keeps running if the pod-side process is long-lived (e.g., if the script ends with `sleep 3600`). An exit code 137 from the background task can mean OOM kill OR just that the local connection was dropped after `sleep 3600` expired.
  **Prevention:** Check the actual file size on the pod before concluding a task failed. A growing file with a recent timestamp means the process is still running.

- **Gotcha:** BoundStore shard loading can push past the startup probe timeout on a fresh V2 load.
  **Root cause:** The startup probe allowed 5 min (60 failures × 5s period). BoundStore with 265K entries loads in ~24s, but during an eviction storm (pre-PR #78) the server thread was blocked entirely, stacking up past the deadline.
  **Prevention:** PR #78 fixed the eviction storm. The failureThreshold should still be raised to 180 (15 min) as a safety margin. The `/api/health` endpoint returns 200 as soon as the HTTP server starts, not waiting for bitmap loading — so the probe passes during normal startup regardless.

---

## Design Changes

- **Changed:** CSV dump approach from manual ad-hoc psql commands to a deploy skill `csv-dump` command.
  **Reason:** Justin explicitly asked Aidan to make all routine manual operations self-service. The transfer pod pattern was proven during this session.
  **Old approach:** Aidan ran psql commands manually via kubectl exec against the PG pod, piped output to files on the PG pod's `/run/` directory, then tried to kubectl cp or pipe them across.

- **Changed:** CSV dump SQL source from `src/pg_sync/copy_queries.rs` (V1 hardcoded) to `docs/design/sync-config-civitai.yaml` (`dump_phases[].copy_query` fields).
  **Reason:** V2 design principle requires config-driven everything. The hardcoded V1 queries diverge from the actual table schemas as the pipeline evolves.
  **Old approach:** `copy_queries.rs` contained hardcoded COPY SQL for all tables. Aidan used these for the initial dump during this session, which required a re-dump of tags after Scarlet pointed out the query was wrong.

---

## Undocumented Knowledge

- **PG connection details:** DATABASE_URL from the `bitdex` K8s secret in the `bitdex` namespace points to the CNPG internal cluster (`cnpg-cluster-nvme0-rw`). The external "DP-6228 replica" mentioned in earlier project memory is likely the upstream primary that CNPG replicates from, not a directly addressable host.

- **Transfer pod pattern for PVC operations:** Spin up a temporary pod in the `bitdex` namespace with the PVC mounted, run operations, then delete it. Uses `nodeSelector: kubernetes.io/hostname: talos-fq9-f3k` to pin to the node where the PVC lives. The `postgres` image is used when psql is needed. The pod lifecycle pattern is: create → wait for Succeeded → read logs → delete.

- **V2 tags query:** Uses `TagsOnImageNew` table with 3 columns (imageId, tagId, attributes), not the V1 2-column version. The correct COPY query is in `docs/design/sync-config-civitai.yaml` under `dump_phases[].copy_query`. Tags CSV is ~21 GB compressed (up from 17 GB V1) because of the attributes column.

- **tags.csv is filter_only and skipped for silo validation:** Per Edward's plan, tags are marked `filter_only` in the sync config and bypass the silo write path entirely. The 80 GB decompressed tags CSV is not needed for data silo validation. This is why Aidan skipped downloading it locally for that workstream.

- **PVC disk health after this session:** 265 GB free on the PVC after cleanup (was 123 GB at worst point). Freed: 30 GB captures, 37 GB debug builds, 109 GB Lucy's test data, 81 GB old V1 uncompressed CSVs. Safe to delete: captures/, debug/, test_*/. Check with team before deleting: indexes/ (active worktree data), any agent's active test directories.

- **Memory stability combo (production):** Two PRs together solved OOM. PR #83 (RSS-aware eviction, reads pod memory limit) + jemalloc `dirty_decay_ms:0` (freed pages returned immediately). Neither alone was sufficient. Production has been stable at ~28.4 GB for days (87% of 32 GB limit). Sync cursor advancing normally at ~205 changes/cycle.

- **Self-service commands Aidan added to the deploy skill during this session:**
  - `tunnel pg` / `tunnel bitdex` — port-forward to PG (localhost:5432) or BitDex pod 0 (localhost:3099)
  - `memory` — detailed RSS + `/debug/memory` endpoint read
  - `disk` — PVC usage breakdown via `kubectlExec`
  - `cleanup <captures|load_stage|legacy|bounds>` — targeted PVC cleanup
  - `config-patch` — runtime config changes
  - `health` — rich pod status (CPU/mem/restarts/cursor)
  - `csv-dump` — transfer pod lifecycle for all 9 COPY operations (TODO: update SQL from sync config YAML)

- **pg-sync sidecar health signals to watch:** Aidan monitors `cursor=<N>` in logs (should advance), `processed N changes` (should be nonzero), and error/failed counts (should be 0). The deploy skill `pg-sync-health` command extracts these from pod logs automatically.

- **Why peer auth fails from transfer pods:** The CNPG pod uses a PostgreSQL `pg_hba.conf` that grants peer auth on the socket only for the `postgres` OS user. Connecting over TCP (even to localhost) requires password auth. The `bitdex` K8s secret has the credentials. Shell quoting the password inline in kubectl exec commands is error-prone — pass it via `PGPASSWORD` env var or embed it in the connection string.

- **Nate's `download_from_sync_config` in pg-sync binary:** The right long-term architecture for CSV dumps. The pg-sync binary runs inside the cluster, reads the sync config, and dumps directly to the PVC without any kubectl pipe hacks. Aidan flagged this but it was not built during this session — it was deferred to when the pg-sync binary takes over.

---

## Regression Risks

- If the startup probe `failureThreshold` is not raised before a V2 fresh load, the pod will crash-loop during BoundStore loading even though the server is healthy. The probe should be at 180 (15 min), not 60 (5 min).

- If `csv-dump` COPY queries are not updated from `docs/design/sync-config-civitai.yaml` before the next V2 bulk load, the tags CSV will be generated with the V1 schema (2 columns, no `attributes`), and the dump processor will fail or produce incorrect tag data.

- If `SET statement_timeout = 0` and `COPY ... TO STDOUT` are combined in a single psql `-c` flag, every output file will be prefixed with `SET\n` and any gzip output will be corrupt. This must be two separate `-c` flags.

---

## Recommended Memory Entries

1. **aidan_deploy_skill_commands** — The deploy skill (`node .claude/skills/deploy/cli.mjs`) has commands for: `release`, `watch-build`, `build-status`, `rollout`, `deploy`, `status`, `scale`, `cursor-reset`, `cursor-read`, `cursor-csv`, `config-read`, `config-update`, `reload`, `wipe`, `pg-sync-health`, `pg-sync-logs`, `server-logs`, `resources`, `tunnel pg/bitdex`, `memory`, `disk`, `cleanup`, `config-patch`, `health`, `csv-dump`. Aidan built this skill incrementally across several sessions.

2. **csv_dump_gotchas** — Critical: (1) use separate `-c` flags for SET and COPY or gzip output is corrupt; (2) postgres user has 5-min statement_timeout; (3) kubectl exec binary pipes corrupt files >1 GB; (4) peer auth fails from transfer pods (use DATABASE_URL from bitdex secret); (5) V2 tags query uses TagsOnImageNew (3 cols), V1 uses 2 cols — source of truth is docs/design/sync-config-civitai.yaml.

3. **transfer_pod_pattern** — To copy data to the BitDex PVC: spin up a pod in the `bitdex` namespace with `data-bitdex-0` mounted, run psql COPY with separate -c flags writing directly to `/data/indexes/civitai/load_stage/`. Pin with `nodeSelector: kubernetes.io/hostname: talos-fq9-f3k`. Use `postgres` image for psql. Authenticate via DATABASE_URL from the `bitdex` K8s secret.

4. **v2_csv_sizes** — Compressed sizes for V2 9-table dump: model_versions 6.7 MB, models 3.2 MB, tools 17 MB, techniques 23 MB, posts 169 MB, resources 204 MB, collections 344 MB, images 7.4 GB (14 GB uncompressed), tags 21 GB (80+ GB uncompressed). Total ~25 GB compressed. Tags takes ~38-53 min to dump; images ~14 min.

5. **startup_probe_threshold** — K8s startup probe failureThreshold should be 180 (15 min), not the default 60 (5 min). PR #78 fixed the OOM eviction storm root cause, but the higher threshold is needed as a safety margin for V2 fresh loads. Arabella owns this change in talos-infra.
