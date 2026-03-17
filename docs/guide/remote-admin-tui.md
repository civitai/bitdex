# BitDex Remote Admin TUI

A terminal dashboard for monitoring and managing a running BitDex server. Works with any BitDex instance -- local, remote, or production via port-forward.

## Quick Start

```bash
# Local server
node .claude/skills/dev-server/cli.mjs dash --remote 127.0.0.1:3001

# Production (via kubectl port-forward)
kubectl port-forward svc/bitdex 3000:80 -n bitdex
node .claude/skills/dev-server/cli.mjs dash --remote 127.0.0.1:3000

# Or use the just shortcut
just dev-remote 127.0.0.1:3001
```

## Admin Token

Admin actions (config changes, cache control, snapshots) require a Bearer token. The TUI loads it automatically from `.claude/skills/dev-server/.env`.

**Current production token:**

```
uct7hZhiWtjynKkmQ5wYB4hZ-C0HI7OCAG8eMxQ37P0
```

To set up, create `.claude/skills/dev-server/.env`:

```
BITDEX_ADMIN_TOKEN=uct7hZhiWtjynKkmQ5wYB4hZ-C0HI7OCAG8eMxQ37P0
```

Or pass it directly:

```bash
node .claude/skills/dev-server/cli.mjs dash --remote 127.0.0.1:3000 --token uct7hZhiWtjynKkmQ5wYB4hZ-C0HI7OCAG8eMxQ37P0
```

Or set the environment variable:

```bash
export BITDEX_ADMIN_TOKEN=uct7hZhiWtjynKkmQ5wYB4hZ-C0HI7OCAG8eMxQ37P0
```

**Note:** Read-only operations (stats, traces, queries) work without a token. The token is only needed for mutating actions.

## What You See

The TUI has three views, switched with keyboard shortcuts:

### Default View

Shows a summary of the server state:

- **Stats** -- record count, slot count, bitmap memory (MB), flush cycle
- **Cache** -- entry count, memory usage, hit/miss rate
- **Fields** -- count of eager vs lazy filter and sort fields
- **Recent Traces** -- table of last N query traces (result count, total time, filter time, sort time, cache hit/miss)

### Explain Panel (`e` key)

Detailed view of individual query traces. Navigate with arrow keys.

For each trace:
- Cache hit/miss status
- Total, plan, filter, sort timing
- Result count
- Clause-by-clause breakdown: field, operator, cardinality, accumulator, selectivity delta, eval/and timing, execution mode
- Sort detail: field, direction, input/output counts, time

### Config Panel (`c` key)

Lists all filter and sort fields with their `eager_load` state. Use this to control which bitmaps are loaded into memory.

- Arrow keys to navigate fields
- **Enter** to toggle `eager_load` on/off (sends PATCH to server immediately)
- Cache actions available in this panel:
  - `C` -- Clear in-memory cache
  - `P` -- Purge persistent cache (disk)
  - `W` -- Warm cache
  - `S` -- Save bitmap snapshot to disk

## All Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `e` | Toggle explain panel (query trace detail) |
| `c` | Toggle config panel (field settings, cache controls) |
| `r` | Force refresh stats and traces |
| `q` | Quit |
| Arrow keys | Navigate traces (explain) or fields (config) |
| `Enter` | Toggle eager_load on selected field (config panel) |
| `C` | Clear cache (config panel) |
| `P` | Purge persistent cache (config panel) |
| `W` | Warm cache (config panel) |
| `S` | Save snapshot (config panel) |

## CLI Commands (Non-Interactive)

For scripting or agent use, the bitdex CLI provides the same capabilities as JSON output:

```bash
SKILL="node .claude/skills/bitdex/cli.mjs"

# Stats
$SKILL stats

# Recent traces
$SKILL traces --last 20

# Clear cache (uses admin token from .env)
$SKILL cache-clear

# For production, set BITDEX_URL first
export BITDEX_URL=http://127.0.0.1:3000
$SKILL traces --last 50
```

## Authentication Architecture

- **Public endpoints** (no token): health, stats, queries, traces, index listing, tasks, cursors, metrics
- **Admin endpoints** (token required): config patch, cache clear/purge/warm, snapshot, rebuild, upsert, delete, field add/remove, index create/delete
- **Internal requests** (no `X-Forwarded-For` header) bypass auth -- this allows the pg-sync sidecar and localhost connections to work without a token
- **External requests** (through Traefik/reverse proxy) must include the token as `Authorization: Bearer <token>`

The token is configured via:
1. `BITDEX_ADMIN_TOKEN` environment variable (recommended for deployments)
2. `admin_token` in `bitdex.toml`
3. If neither is set, admin endpoints return `403 Forbidden` (fail-safe)

## Traces

Traces must be enabled on the server with `--enable-traces` (off by default). Traces are stored in an in-memory ring buffer (1000 entries, ~1MB). No disk I/O.

Each trace captures:
- Timestamp, index name
- Total/plan/filter/sort timing (microseconds)
- Cache hit/miss
- Per-clause detail: field, operator, values, cardinality, accumulator before/after, eval and AND timing, execution mode (owned/ref)
- Sort detail: field, direction, input/output counts, time

## Connecting to Production

```bash
# 1. Start port-forward
kubectl port-forward svc/bitdex 3000:80 -n bitdex

# 2. Launch TUI
node .claude/skills/dev-server/cli.mjs dash --remote 127.0.0.1:3000

# 3. Or use CLI for one-off checks
export BITDEX_URL=http://127.0.0.1:3000
node .claude/skills/bitdex/cli.mjs stats
node .claude/skills/bitdex/cli.mjs traces --last 20
```

The TUI polls stats every 2 seconds. Traces are fetched on demand when you press `e`.
