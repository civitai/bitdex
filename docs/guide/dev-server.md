# Dev-Server: Daemon, TUI & Operations

## Overview

The dev-server is a Node.js daemon + CLI + TUI that coordinates BitDex development. It manages server instances, builds, E2E tests, datasets, and query traces across multiple agents and worktrees.

```
just dev          # Start server (idempotent)
just dev-dash     # TUI dashboard
just dev-build    # Coordinated build
just dev-stop     # Stop server
just dev-kill     # Force-kill all bitdex-server processes
just dev-restart  # Restart daemon (picks up new Node code)
just dev-shutdown # Kill everything
```

---

## Daemon Architecture

The daemon listens on `127.0.0.1:9851`. It's a single Node.js process with an HTTP server and a 10-second heartbeat timer.

### State Model (all in-memory, no disk persistence)

| State | Description |
|-------|-------------|
| **Instances** | Running bitdex-server processes: id, pid, port, dataDir, status, dataset, recordCount, stats |
| **Datasets** | Known data directories (discovered when instances report index stats) |
| **Build lock** | Who's building, what target, log buffer, cargo process handle |
| **E2E lock** | Who's running E2E tests |
| **SSE clients** | Connected event stream consumers |
| **Log ring buffers** | Per-instance, 1000 lines max, in-memory only |

### Server Instance Lifecycle

1. **Start**: `POST /instances` — allocates port (3001-3099), creates shadow copy of binary (`.active.exe`), spawns process, pipes stdout/stderr to log ring buffer
2. **Health check**: Async — polls `/api/health` on the instance port for up to 60s, updates status to `running` when ready, queries `/api/indexes` for stats
3. **Running**: Heartbeat checks PID liveness every 10s via `process.kill(pid, 0)`, polls `/api/indexes/{name}/stats` for bitmap/cache metrics
4. **Stop**: `DELETE /instances/:id` — kills process, cleans shadow copy, preserves data directory
5. **Build restart**: After successful `cargo build`, daemon auto-stops running instances, copies fresh binary to `.active`, restarts on same port+dataDir

### Shadow Copy System

The server binary runs from a `.active.exe` copy, not the original. This means `cargo build` can overwrite `target/fast/bitdex-server.exe` while the server is running — the locked file is `.active.exe`, not the build output.

On restart: kill process → wait 1.5s → delete old `.active` → copy fresh binary → spawn new `.active`.

### Build Pipeline

Builds run through the daemon, not directly via cargo:

1. `POST /build/run` starts a cargo build, captures stdout/stderr into a log buffer
2. Any agent can watch via `GET /build/logs?since=N` or the TUI
3. On success, running instances auto-restart with the new binary
4. On failure, instances keep running the old binary

### SSE Event Stream

`GET /events` opens a Server-Sent Events connection:

- `event: status` — full status JSON, sent on connect + every heartbeat (10s)
- `event: log` — individual log entry with `instanceId`, sent instantly as logs arrive

The TUI connects via SSE instead of polling. Logs appear in real-time. Render is throttled to ~10fps via a dirty flag + timer to prevent input lag under high event load. During text input mode, only the input line is redrawn.

Agents can consume the SSE stream programmatically via the `follow` command:

```bash
node .claude/skills/dev-server/cli.mjs follow              # All events as JSON lines
node .claude/skills/dev-server/cli.mjs follow --type log    # Logs only
node .claude/skills/dev-server/cli.mjs follow srv-3001      # Filter by instance
```

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Daemon health check |
| GET | `/events` | SSE event stream |
| GET | `/status` | Full status (instances, datasets, locks) |
| POST | `/instances` | Start a server instance |
| DELETE | `/instances/:id` | Stop an instance |
| GET | `/instances/:id/logs` | Instance logs (polling fallback) |
| GET | `/datasets` | Known data directories |
| POST | `/build/run` | Start a build |
| GET | `/build/status` | Build lock state |
| GET | `/build/logs` | Build output logs |
| POST | `/e2e/acquire` | Acquire E2E test lock |
| POST | `/e2e/release` | Release E2E test lock |
| POST | `/force-kill` | Kill all bitdex-server processes |
| POST | `/stop-all` | Stop all managed instances |
| POST | `/shutdown` | Stop everything + exit daemon |

---

## TUI Dashboard

The TUI renders in an alternate screen buffer using ANSI escape codes. It connects to the daemon via SSE for real-time updates.

### Layout

```
 BitDex Dev Server │ up 2h13m │ PID 12345
─────────────────────────────────────────────
 Instances
  ● srv-3001  3001  running  civitai  105.3M  data  2h
─────────────────────────────────────────────
 Stats                          Locks
  Bitmaps: 913.5 MB             Build: FREE
  Cache: 1 entries (45K, 75%)   E2E: FREE
─────────────────────────────────────────────
  ◈ build: idle
── logs: srv-3001 ───────────────────────────
  14:30:05  WHERE nsfwLevel IN [1, 2] AND ...
  14:30:05    → 4.0K results  total=15μs  plan=0  filter=0  sort=0  cache
─────────────────────────────────────────────
  ↑↓ scroll  1-9 select  info  build  new  stop  restart  kill  Kill force  quit
```

### Hotkeys

| Key | Action |
|-----|--------|
| `q` / `Ctrl+C` | Quit (daemon keeps running) |
| `1-9` | Select instance (resets log view) |
| `↑↓` | Scroll logs (3 lines) |
| `PgUp/PgDn` | Scroll logs (20 lines) |
| `Home/End` | Jump to oldest/newest logs |
| `e` | Enter explain mode (fetch traces, pause logs, navigate with arrows) |
| `i` | Toggle expanded stats panel |
| `b` | Trigger build |
| `n` | Start new instance (prompts for data dir) |
| `s` | Stop selected/first instance |
| `r` | Restart selected/first instance |
| `k` | Stop all instances |
| `K` | Force-kill all bitdex-server processes |

### Explain Mode

Press `e` to enter explain mode:
- Fetches last 50 traces from the server
- Logs pause
- `←→` or `↑↓` navigate between traces
- Shows clause table: ord, field, op, card, accumulator cascade, delta%, timing, mode
- `e` again to exit and resume live logs

### Log Formatting

- Query lines: SQL keywords colored (magenta), fields (blue), sort direction (yellow)
- Result lines: green arrow, white count, adaptive time units, cache tag
- Server events: green checkmark, cyan values
- Daemon messages: blue dot

---

## Operations

### Starting from scratch

```bash
just dev          # Auto-starts daemon, starts server on port 3001 with ./data
just dev-dash     # Connect TUI dashboard
```

### Building after code changes

```bash
just dev-build    # Acquires lock, runs cargo build, auto-restarts server
```

Or press `b` in the TUI.

### When things go wrong

```bash
just dev-kill     # Force-kill all bitdex-server processes (including orphans)
just dev          # Start fresh
```

### Restarting the daemon (after daemon code changes)

```bash
just dev-restart  # Stops daemon, auto-starts new one
```

### Multiple instances

```bash
just dev-new --port 3005 --data-dir .test-data/experiment
```

Or press `n` in the TUI and enter a data directory path.

### Query traces

Traces are written to `{data_dir}/traces.jsonl` — one JSON line per query with per-clause timing, cardinality cascade, cache hit detection, and doc fetch timing.

```bash
node .claude/skills/dev-server/cli.mjs traces --last 5
```

Or press `e` in the TUI to browse traces visually.

---

## Troubleshooting

**Dashboard hangs on connect**: Kill the daemon (`just dev-shutdown` or `curl -X POST http://127.0.0.1:9851/shutdown`) and let it auto-start fresh via `just dev`.

**Port already in use**: Another bitdex-server process is running. Use `just dev-kill` to force-kill all instances, then `just dev`.

**Build fails with "Access is denied"**: The `.active.exe` shadow copy is locked. Stop the server first (`just dev-stop`), then build.

**Daemon not responding blips**: Fixed by SSE. If still occurring, the daemon may be running old code — use `just dev-restart`.
