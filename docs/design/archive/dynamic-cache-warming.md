# Design: Dynamic Cache Warming

## Problem

BitDex's unified cache eliminates cold miss latency (40-70ms → 12μs) for repeated queries. But the first query for any new filter+sort combination pays the full cold miss cost. After a server restart, the persisted cache only contains entries from the previous session — new query patterns always hit cold.

In production, Civitai's image browsing generates a predictable set of query patterns: ~50 common filter combinations × 4-5 sort fields. Pre-warming these on startup would eliminate cold misses entirely for typical traffic.

## Endpoint

```
POST /api/indexes/{name}/warm
```

Accepts a list of queries and runs each to seed the cache. Reports per-query timing. See `docs/guide/api.md` for full API documentation.

## Design: Automatic Warming Agent

An external agent (running alongside BitDex, not embedded) should:

### 1. Observe query traffic

Listen to the query trace stream (JSONL file or `/traces` endpoint) and collect:
- Filter clause combinations (canonicalized)
- Sort field + direction
- Frequency counts per 5-minute window
- p50/p95 latency per pattern

### 2. Identify warming candidates

A query pattern is a warming candidate if:
- It appears 5+ times in a 5-minute window (frequent)
- Its first occurrence was a cache miss (>1ms, indicating no persisted cache entry)
- It's not already in the current cache (check `/stats` endpoint)

### 3. Build warming manifest

On server startup or on demand, produce a JSON file of queries to warm:

```json
{
  "queries": [
    {"filters": [...], "sort": {"field": "reactionCount", "direction": "Desc"}},
    {"filters": [...], "sort": {"field": "commentCount", "direction": "Desc"}}
  ]
}
```

Sources for the manifest:
- **Historical traffic analysis**: Parse trace logs from the last N hours, extract top-K patterns
- **Static configuration**: Known critical queries (e.g., homepage, default browse)
- **Shadow mode traffic**: Replay the most common comparison queries

### 4. Warming lifecycle

```
Server starts → loads persisted cache shards (~337μs)
             → eager loads bitmap fields (~1.4s)
             → starts accepting traffic
             → warming agent hits /warm endpoint with manifest
             → cache is fully hot for common patterns
```

The warming agent can run as:
- **Init container** in K8s: runs before the readiness probe passes
- **Sidecar**: continuously monitors traffic and warms new patterns
- **Cron job**: refreshes the warming manifest daily from trace analysis

### 5. Warming manifest generation

The agent should output a `warm-manifest.json` that gets stored alongside the index config. On startup, the server could optionally auto-warm from this file.

Suggested location: `{data_dir}/indexes/{name}/warm-manifest.json`

### 6. Integration with the `/warm` endpoint

The warming agent calls:
```bash
curl -X POST http://localhost:3000/api/indexes/civitai/warm \
  -H 'Content-Type: application/json' \
  -d @warm-manifest.json
```

Response tells the agent which queries were already cached (from persistence) and which needed cold computation:
```json
{
  "warmed": 12,
  "already_cached": 38,
  "total_elapsed_ms": 840
}
```

### 7. Metrics

The warming agent should track:
- Warming latency per query pattern
- Cache hit rate improvement after warming
- Manifest staleness (how often patterns change)

These can be pushed to Prometheus via the existing `/metrics` endpoint or logged to the trace JSONL.

## Future: Embedded Warming

If the external agent proves too complex, an embedded version could:
- Record query patterns in a ring buffer during operation
- On shutdown, save the top-N patterns to `warm-manifest.json`
- On startup, auto-warm from the manifest before accepting traffic

This is simpler but less flexible than the external agent approach.

## Dependencies

- `POST /api/indexes/{name}/warm` endpoint (this PR)
- Query trace collection (`/traces` endpoint, already implemented)
- Cache persistence (`BoundStore`, already implemented)
