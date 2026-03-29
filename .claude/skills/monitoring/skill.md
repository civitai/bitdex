---
name: monitoring
description: Monitor BitDex health — cache hit rates, memory trends, sync lag, query performance, alerts, and runtime config. Agent-friendly structured output from Prometheus metrics.
---

# BitDex Monitoring

Check BitDex operational health with structured, agent-friendly output. Parses Prometheus `/metrics` into actionable summaries.

## When to Use

- Checking system health before/after deploys
- Investigating performance issues
- Monitoring cache effectiveness
- Checking sync pipeline lag
- Verifying runtime config
- Running threshold-based alert checks

## Commands

```bash
CLI=".claude/skills/monitoring/cli.mjs"

# At-a-glance health summary
node $CLI overview [--prod]

# Cache health (unified cache)
node $CLI cache-health [--prod]

# Document cache stats
node $CLI doc-cache [--prod]

# Memory breakdown (RSS, bitmaps, caches)
node $CLI memory [--prod]

# Query latency and concurrency
node $CLI query-perf [--prod]

# Sync pipeline health (V1 + V2)
node $CLI sync-health [--prod]

# Current runtime config values
node $CLI config [--prod]

# Threshold-based alert checks
node $CLI alerts [--prod]

# Cache persistence (BoundStore)
node $CLI boundstore [--prod]

# Query backpressure stats
node $CLI backpressure [--prod]
```

## Flags

- `--prod` — Query production BitDex (default: local on port 3001)

## Output Format

All commands output JSON for easy parsing by agents. Example `alerts` output:

```json
{
  "source": "production",
  "total_checks": 6,
  "firing": 1,
  "status": "WARNING",
  "checks": [
    {
      "name": "RSS Memory",
      "value": "28.40 GB (88.8% of 32.00 GB)",
      "status": "CRITICAL",
      "thresholds": "warn: 80%, crit: 87%"
    }
  ]
}
```

## Alert Thresholds

| Check | Warning | Critical |
|-------|---------|----------|
| RSS Memory | >80% of pod limit | >87% of pod limit |
| Unified Cache Hit Rate | <80% | <50% |
| Doc Cache Hit Rate | <80% | <50% |
| Rejected Queries | any rejection | — |
| Pending Lazy Loads | any pending | — |
| Sync V2 Lag | >1K rows | >10K rows |

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `BITDEX_URL` | Local server URL (default: http://localhost:3001) |
| `BITDEX_PROD_URL` | Production URL (default: https://bitdex.civitai.com) |
| `BITDEX_ADMIN_TOKEN` | Bearer token for authenticated endpoints |
