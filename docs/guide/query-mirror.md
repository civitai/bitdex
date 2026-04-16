# Query Mirror — Live SSE Stream

Mirror production query traffic to a local BitDex instance in real time. Useful for benchmarking local changes against prod query patterns without operating on historical cap-logs.

## How it works

When `BITDEX_QUERY_STREAM=1` is set, the server creates a `tokio::sync::broadcast` channel (capacity 10 000). Every query that reaches `handle_query` is teed into that channel via a non-blocking `send`. If the channel is full, the oldest buffered event is discarded — the query path never blocks.

`GET /debug/queries/stream` (admin-gated) exposes the channel as an SSE stream. Each event is a JSON object:

```json
{
  "ts_ms": 1714000000000,
  "index": "civitai",
  "body": { "filters": [...], "sort": "reactionCount", "limit": 20 }
}
```

## Enabling

```bash
# Start server with stream enabled
BITDEX_QUERY_STREAM=1 BITDEX_ADMIN_TOKEN=secret \
  cargo run --release --features server --bin bitdex-server -- --port 3001

# Or in prod — set env var in the K8s deployment manifest
```

When enabled, the server logs:

```
Query stream enabled (BITDEX_QUERY_STREAM=1) — GET /debug/queries/stream
```

When `BITDEX_QUERY_STREAM` is unset (the default), the broadcast channel is not created and the check in `handle_query` is a single `Option::is_none` branch — zero overhead.

## Manual stream verification (curl)

```bash
curl -N \
  -H "Authorization: Bearer $BITDEX_ADMIN_TOKEN" \
  "https://bitdex.civitai.com/debug/queries/stream?index=civitai"
```

Each SSE event looks like:

```
data: {"ts_ms":1714000123456,"index":"civitai","body":{...}}

data: {"ts_ms":1714000123789,"index":"civitai","body":{...}}
```

## Query mirror client

`scripts/query-mirror.mjs` connects to the prod SSE stream and replays each query body against a local server.

### Prerequisites

- Node.js 18+ (for native `fetch` with streaming body support)
- `BITDEX_ADMIN_TOKEN` env var set to the prod admin token
- Prod server running with `BITDEX_QUERY_STREAM=1`

### Usage

```bash
# Mirror all queries from prod to local (default concurrency: 4)
BITDEX_ADMIN_TOKEN=secret node scripts/query-mirror.mjs

# Mirror only the civitai index, higher concurrency
BITDEX_ADMIN_TOKEN=secret node scripts/query-mirror.mjs \
  --source https://bitdex.civitai.com \
  --target http://localhost:3001 \
  --index civitai \
  --concurrency 8
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | `BITDEX_PROD_URL` env or `https://bitdex.civitai.com` | Source SSE server |
| `--target` | `http://localhost:3001` | Local server to replay against |
| `--index`  | (all indexes) | Only mirror queries for this index |
| `--concurrency` | `4` | Max in-flight POSTs to local server |

### Behavior

- If the local server falls behind (concurrency saturated), events are **dropped** with a counter. The prod stream is never backpressured.
- Progress is logged every 1 000 received events: `received / processed / dropped / in_flight / lag`.
- `SIGINT` (Ctrl+C) prints a final summary and exits cleanly.

## Notes

- No persistence — events are live-only. Missed events are gone.
- No replay timing — queries fire as fast as events arrive (modulo concurrency cap).
- Auth is Bearer-only on both ends (admin token for SSE source, no auth required for local target).
- The SSE route is in the `admin_routes` group — requires `BITDEX_ADMIN_TOKEN` to be configured on the source server.
