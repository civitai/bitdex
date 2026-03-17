---
name: deploy
description: Release, build, and deploy BitDex to production K8s. Manage cursors, configs, bulk reloads, and monitor pg-sync health. Use when cutting releases, deploying new versions, resetting cursors, updating configs, or monitoring the production pipeline.
user-invocable: false
---

# BitDex Production Deploy

CLI for release pipeline, K8s deployment, cursor management, and monitoring.

**CLI:** `node .claude/skills/deploy/cli.mjs <command> [args]`

All commands output JSON to stdout. Status/progress messages go to stderr.

## Release Pipeline

```bash
# Cut a release: bump Cargo.toml, commit, tag, push, trigger Docker build
node .claude/skills/deploy/cli.mjs release

# Watch a Docker build until completion (blocks)
node .claude/skills/deploy/cli.mjs watch-build [--run-id <id>]

# Check latest build status
node .claude/skills/deploy/cli.mjs build-status
```

`release` is the full pipeline: reads current version from Cargo.toml, bumps patch, commits, tags, pushes, triggers Docker build via `gh workflow run`, and returns the run ID. Use `watch-build` to block until the image is ready.

## Deployment

```bash
# Rolling update to a specific version (no downtime)
node .claude/skills/deploy/cli.mjs rollout <version>

# Full deploy: rollout + wait for ready + verify pg-sync health
node .claude/skills/deploy/cli.mjs deploy <version>

# Check current deployed version and pod status
node .claude/skills/deploy/cli.mjs status
```

`rollout` updates both container images (bitdex + pg-sync) and waits for the rollout to complete. `deploy` adds health verification on top.

## Cursor Management

```bash
# Reset cursors on both PVCs + PG to a specific value
# Requires pods to be scaled down first
node .claude/skills/deploy/cli.mjs cursor-reset <value>

# Read current cursor values from both PVCs
node .claude/skills/deploy/cli.mjs cursor-read

# Get the CSV dump cursor value (from load_stage/cursor.txt)
node .claude/skills/deploy/cli.mjs cursor-csv
```

Cursor reset handles:
1. Creating transfer pods on both PVCs
2. Writing cursor files to `bitmaps/cursors/pg-sync-bitdex-{0,1}`
3. Updating `bitdex_cursors` table in PG
4. Cleaning up transfer pods

**Important:** Pods must be scaled to 0 before resetting cursors, otherwise the running pg-sync will overwrite them.

## Config Management

```bash
# Read current config.json from a pod
node .claude/skills/deploy/cli.mjs config-read

# Update config.json on both PVCs (takes a JSON patch)
# Requires pods to be scaled down first
node .claude/skills/deploy/cli.mjs config-update <json-file>
```

## Bulk Reload

```bash
# Full reload: scale down, wipe, load from CSVs, reset cursors, scale up
node .claude/skills/deploy/cli.mjs reload [--cursor <value>]

# Wipe bitmap/docstore data on both PVCs (keeps CSVs)
node .claude/skills/deploy/cli.mjs wipe
```

## Monitoring

```bash
# Check pg-sync health: cursor position, errors, processing rate
node .claude/skills/deploy/cli.mjs pg-sync-health

# Tail pg-sync logs (filtered for progress, errors)
node .claude/skills/deploy/cli.mjs pg-sync-logs [--pod 0|1] [--lines 20]

# Check server logs
node .claude/skills/deploy/cli.mjs server-logs [--pod 0|1] [--lines 20]

# Resource usage
node .claude/skills/deploy/cli.mjs resources
```

## Constants

- **Namespace:** bitdex
- **StatefulSet:** bitdex
- **Containers:** bitdex (server), pg-sync (sidecar)
- **PVCs:** data-bitdex-0, data-bitdex-1
- **Node:** talos-fq9-f3k
- **GHCR:** ghcr.io/civitai/bitdex
- **Docker workflow:** docker.yml
- **PG replica:** cnpg-cluster-nvme0-1 in cnpg-database namespace
- **Cursor files:** bitmaps/cursors/pg-sync-bitdex-{0,1}
- **Config:** /data/indexes/civitai/config.json
- **CSV stage:** /data/indexes/civitai/load_stage/
