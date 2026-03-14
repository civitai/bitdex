---
name: dev-server
description: Manage BitDex server instances, coordinate builds, tests, and datasets across multiple agents/worktrees. Use when you need to start a server, run E2E tests, check logs, or see if another agent is using resources.
user-invocable: true
---

# BitDex Dev-Server

Coordinates server instances, builds, tests, and datasets via a background daemon. The daemon auto-starts on first CLI call.

**CLI:** `node .claude/skills/dev-server/cli.mjs <command>`

All commands output JSON to stdout. Status messages go to stderr.

## Starting a Server

```bash
# Ensure a server is running (idempotent — won't start a duplicate)
node .claude/skills/dev-server/cli.mjs start

# Start a NEW instance (when you need your own, e.g. different data dir)
node .claude/skills/dev-server/cli.mjs new --name my-test --port 3005 --data-dir .test-data/my-work
```

`start` is **idempotent**: if a server is already running, it returns `{ "already_running": true, "instances": [...] }`. Use this as your default — only use `new` when you need a separate instance with its own port/data.

## Checking Status

```bash
node .claude/skills/dev-server/cli.mjs status
```

Returns JSON with all instances, datasets, build lock, and E2E lock state. Check this before claiming resources.

## Viewing Logs

```bash
# Logs from the first running instance (no ID needed)
node .claude/skills/dev-server/cli.mjs logs

# Logs from a specific instance
node .claude/skills/dev-server/cli.mjs logs srv-3005

# Last 20 lines, or incremental polling
node .claude/skills/dev-server/cli.mjs logs --tail 20
node .claude/skills/dev-server/cli.mjs logs --since 42
```

No ID required — defaults to the first running instance. Pass an ID when multiple instances are running.

## Stopping

```bash
# Stop the first running instance (no ID needed)
node .claude/skills/dev-server/cli.mjs stop

# Stop a specific instance
node .claude/skills/dev-server/cli.mjs stop srv-3005
```

Stopping preserves the data directory. The dataset stays registered.

## Building (with Lock Coordination)

```bash
# Default: build bitdex-server with fast profile
node .claude/skills/dev-server/cli.mjs build

# Build a specific target
node .claude/skills/dev-server/cli.mjs build --target bitdex-loadtest
node .claude/skills/dev-server/cli.mjs build --target bitdex-benchmark
```

The daemon runs cargo and captures all output. The `build` command polls and streams logs automatically. If another agent is already building, running `build` will watch their build output. If you request a new build while one is in progress, the old one gets killed and yours starts. Lock auto-releases if the holder dies or after 10 minutes.

## Running E2E Tests (with Lock)

```bash
node .claude/skills/dev-server/cli.mjs test-e2e
```

Acquires E2E lock (only one run at a time — shared port 3100), runs the suite with `--skip-build`, releases. Build separately via `build` if needed.

## Rules

1. **Use `start` (not `new`) by default.** It's idempotent — checks if something is running first.
2. **Never wipe a data directory** that shows records in `datasets`. The 105M dataset takes 5-10 minutes to load.
3. **Use `build` for cargo builds** — coordinates the build lock across agents.
4. **Use `test-e2e` for E2E tests** — coordinates the E2E lock (shared port 3100 + data dir).
5. **Check `status` before claiming resources** — see what other agents are using.
6. **Don't stop another agent's instance** unless the user explicitly asks.
7. **Stop your instance when done** — the daemon detects dead processes, but explicit cleanup is better.

## All Commands

| Command | Description | Output |
|---------|-------------|--------|
| `start` | Ensure server is running (idempotent) | JSON |
| `start --new` | Force a new instance even if one exists | JSON |
| `new [--name N] [--port N] [--data-dir D]` | Spin up an additional instance | JSON |
| `stop [id\|port]` | Stop instance (defaults to first running) | JSON |
| `status` | Full overview: instances, datasets, locks | JSON |
| `logs [id\|port] [--tail N] [--since N]` | View logs (defaults to first running) | JSON |
| `datasets` | List known data directories | JSON |
| `reserve-port` | Reserve next available port | JSON |
| `build [--target T] [--profile P]` | Coordinated cargo build | JSON |
| `traces [id\|port] [--last N]` | Fetch recent query traces (default last=5) | JSON |
| `test-e2e [--port N]` | Coordinated E2E test run | JSON |
| `shutdown` | Kill all instances + daemon | JSON |

## Build Targets

| Target | Profile | Notes |
|--------|---------|-------|
| `bitdex-server` (default) | `fast` | Dev build, thin LTO |
| `bitdex-loadtest` | `fast` | Dev build |
| `bitdex-benchmark` | `release` | Always full optimization |

## How It Works

- **Daemon** runs on `127.0.0.1:9851`, auto-starts on first CLI call
- **Shadow copy**: server runs from `.active.exe` so cargo builds don't lock the running binary
- **Port range**: 3001-3099 for instances, 3100 reserved for E2E
- **Heartbeat**: every 10s, detects dead processes, auto-releases stale locks
- **State**: persisted to `.claude/skills/dev-server/state/` (gitignored)
- **Datasets**: tracked but never auto-deleted — stopping preserves data
