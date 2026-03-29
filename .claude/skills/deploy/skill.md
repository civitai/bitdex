---
name: deploy
description: Release, build, and deploy BitDex to production K8s. Manage cursors, configs, bulk reloads, CSV dumps, and monitor pg-sync health. Use when cutting releases, deploying new versions, resetting cursors, updating configs, dumping CSVs, or monitoring the production pipeline.
user-invocable: false
---

# BitDex Production Deploy

CLI for release pipeline, K8s deployment, cursor management, CSV dumps, and monitoring.

**CLI:** `node .claude/skills/deploy/cli.mjs <command> [args]`

All commands output JSON to stdout. Status/progress messages go to stderr.

## Architecture

The CLI is modular:
- `cli.mjs` — command router
- `lib/kubectl.mjs` — K8s helpers, transfer pods, port-forward
- `lib/prometheus.mjs` — PromQL queries, metrics
- `lib/sync-config.mjs` — V2 sync config YAML parser
- `lib/csv-dump.mjs` — Config-driven CSV dump with progress tracking

## Release Pipeline

```bash
# Cut a release: bump Cargo.toml, commit, tag, push, trigger Docker build
node .claude/skills/deploy/cli.mjs release

# Watch a Docker build until completion (blocks)
node .claude/skills/deploy/cli.mjs watch-build [--run-id <id>]

# Watch build + notify an agent via mailbox on completion
node .claude/skills/deploy/cli.mjs watch-build-notify [run-id] [--notify <agent>]

# Check latest build status
node .claude/skills/deploy/cli.mjs build-status
```

## Deployment

```bash
# Rolling update to a specific version (no downtime)
node .claude/skills/deploy/cli.mjs rollout <version>

# Full deploy: rollout + health checks (pg-sync, pods, memory) + rollback command
node .claude/skills/deploy/cli.mjs deploy <version>

# Rollback to a previous version
node .claude/skills/deploy/cli.mjs rollback <version>

# Check current deployed version and pod status
node .claude/skills/deploy/cli.mjs status
```

`deploy` stores the previous version and outputs a rollback command in its JSON response. Health checks verify pg-sync cursor advancement, pod readiness, and memory usage.

## Cursor Management

```bash
# Reset cursors on both PVCs + PG to a specific value (pods must be at 0)
node .claude/skills/deploy/cli.mjs cursor-reset <value>

# Read current cursor values from both PVCs
node .claude/skills/deploy/cli.mjs cursor-read

# Get the CSV dump cursor value (from load_stage/cursor.txt)
node .claude/skills/deploy/cli.mjs cursor-csv
```

## Config Management

```bash
# Read current config.json from a pod
node .claude/skills/deploy/cli.mjs config-read

# Live-patch config via admin API (no restart needed)
node .claude/skills/deploy/cli.mjs config-patch '{"key": "value"}'
```

## CSV Dump (V2 — Config-Driven)

Reads COPY queries from `config/sync-civitai.yaml`. Uses the transfer pod pattern (not kubectl pipe, which corrupts >1GB). Outputs raw CSV by default for V2 dump processor.

```bash
# List available tables with expected sizes
node .claude/skills/deploy/cli.mjs csv-dump-tables

# Dump all tables to PVC
node .claude/skills/deploy/cli.mjs csv-dump

# Dump specific tables only
node .claude/skills/deploy/cli.mjs csv-dump tags,images

# Dump with gzip compression (V1 compat)
node .claude/skills/deploy/cli.mjs csv-dump --gzip

# Poll dump progress (file sizes + completion %)
node .claude/skills/deploy/cli.mjs csv-dump-progress

# Clean up transfer pod after dump completes
node .claude/skills/deploy/cli.mjs csv-dump-cleanup
```

**Safety:** Uses `psql -q` (quiet mode) to suppress the `SET` prefix that psql outputs to stdout. Separate `-c` flags alone do NOT fix this — `-q` is required. Verifies output files for corruption after each table.

### CSV Full Pipeline

End-to-end orchestration: dump on PVC → serve via HTTP → download locally → verify.

```bash
# Full pipeline: dump → serve → download → verify → cleanup
node .claude/skills/deploy/cli.mjs csv-full-pipeline [tables] [--notify <agent>] [--output <dir>] [--skip-dump]

# Serve CSVs from PVC via HTTP (runs ephemeral pod with python3 -m http.server)
node .claude/skills/deploy/cli.mjs csv-serve

# Stop the CSV server pod
node .claude/skills/deploy/cli.mjs csv-serve-stop

# Download CSVs — prefers ingress HTTPS, falls back to port-forward
# Auto multi-part for files >1GB (8 parallel Range requests)
node .claude/skills/deploy/cli.mjs csv-download [tables] [--token <token>] [--chunks 16] [--output <dir>]

# Verify downloaded CSV integrity (row counts, headers)
node .claude/skills/deploy/cli.mjs csv-verify <dir>

# Watch a download in progress (poll file size growth)
node .claude/skills/deploy/cli.mjs csv-download-watch <filename> [--poll <seconds>] [--notify <agent>]
```

**Download methods (tried in order):**
1. **Ingress** (preferred) — `https://bitdex.civitai.com/downloads/<file>` with Bearer token (`BITDEX_DL_TOKEN` env or `--token`). HTTP/2, resume support. Files >1GB use multi-part parallel download (N chunks via Range headers, default 8, configurable with `--chunks`).
2. **Port-forward** (fallback) — csv-serve pod if ingress unavailable.

`csv-full-pipeline` is the preferred way to get CSVs locally — it handles the full lifecycle including cleanup. Use `--skip-dump` to download existing PVC CSVs without re-dumping.

## Bulk Reload

```bash
# Full reload orchestration (see reload.mjs for 9-step process)
node .claude/skills/deploy/reload.mjs <step>

# Wipe bitmap/docstore data on both PVCs (keeps CSVs)
node .claude/skills/deploy/cli.mjs wipe
```

## Monitoring

```bash
# Pod health: status, readiness, restarts, memory, pg-sync cursor
node .claude/skills/deploy/cli.mjs health

# PG-sync health: cursor, errors, processing rate
node .claude/skills/deploy/cli.mjs pg-sync-health

# Tail logs
node .claude/skills/deploy/cli.mjs pg-sync-logs [pod] [lines]
node .claude/skills/deploy/cli.mjs server-logs [pod] [lines]

# Resource usage (kubectl top)
node .claude/skills/deploy/cli.mjs resources

# Memory: kubectl top + /debug/memory endpoint
node .claude/skills/deploy/cli.mjs memory

# Disk: PVC usage and directory breakdown
node .claude/skills/deploy/cli.mjs disk
```

## Prometheus Metrics

```bash
# 5-minute window: QPS, latency percentiles, cache hit rate
node .claude/skills/deploy/cli.mjs metrics-now

# Time series trend (QPS + p95 latency)
node .claude/skills/deploy/cli.mjs metrics-trend [window]

# Arbitrary PromQL query
node .claude/skills/deploy/cli.mjs metrics-query <promql>
```

## Tunnels

```bash
# PostgreSQL tunnel on localhost:5432
node .claude/skills/deploy/cli.mjs tunnel pg [start|stop|status]

# BitDex server tunnel on localhost:3099
node .claude/skills/deploy/cli.mjs tunnel bitdex [start|stop|status]
```

## Snapshots

```bash
node .claude/skills/deploy/cli.mjs snapshot-status <session_id>
node .claude/skills/deploy/cli.mjs snapshot-download <session_id> [--output <path>]
```

## Cleanup

```bash
node .claude/skills/deploy/cli.mjs cleanup <captures|load_stage|legacy|bounds>
node .claude/skills/deploy/cli.mjs scale <replicas>
```

## Constants

- **Namespace:** bitdex
- **StatefulSet:** bitdex
- **Containers:** bitdex (server), pg-sync (sidecar)
- **PVCs:** data-bitdex-0, data-bitdex-1
- **Node:** talos-fq9-f3k
- **GHCR:** ghcr.io/civitai/bitdex
- **K8s Context:** civit-datapacket
- **PG replica:** cnpg-cluster-nvme0-1 in cnpg-database namespace
- **Sync config:** config/sync-civitai.yaml (source of truth for dump queries)
