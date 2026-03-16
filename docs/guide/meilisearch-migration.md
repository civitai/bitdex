# Meilisearch to BitDex Migration

This document tracks the migration from Meilisearch to BitDex as the primary image search engine. It covers the readiness assessment process, known differences between the engines, and the tools used to validate correctness.

## Readiness Assessment

We determine switchover readiness by running a comparison test that hits both engines with identical queries and measures result similarity.

### Running the Readiness Report

```bash
node tests/e2e/e2e-compare.mjs --url https://civitai.com --limit 20 --delay 1000
```

The report has three tiers:

**Tier 1: Apples-to-Apples (must pass)**
Queries where both engines should return the same results: anonymous access, timestamp sorts (sortAt), standard filters (type, hasMeta, nsfwLevel). These use no period filters or metrics sorts — areas where the engines intentionally differ. Threshold: jaccard >= 0.8 for each test.

**Tier 2: Known-Divergent (informational)**
Queries where the engines intentionally differ: period filters (BitDex uses time buckets, Meili uses ms timestamps), metrics sorts (reactionCount/collectedCount may have enrichment lag), access control (BitDex defers private/blocked to post-filter). Tracked for trends but not pass/fail.

**Tier 3: Field Validation (must pass)**
For overlapping IDs from Tier 1, compares actual field values (nsfwLevel, type) to catch data corruption independent of sort/filter differences.

### Interpreting Results

- **Tier 1 all passing + Tier 3 passing = READY** for switchover
- Tier 1 failures on `Newest` variants may indicate data freshness gap (BitDex indexes faster than Meili) rather than correctness bugs
- Tier 2 low Jaccard is expected and acceptable

### Results Over Time

Each run appends to `tests/e2e-compare-results.jsonl` for historical tracking. Compare runs to watch for regressions.

## Known Differences

These are intentional design choices, not bugs:

| Area | Meilisearch | BitDex | Why |
|------|-------------|--------|-----|
| Period filters | `sortAtUnix > ms_timestamp` (60s snap) | Time bucket pre-computed bitmaps (seconds) | BitDex optimizes period queries via pre-computed buckets |
| isPublished | `publishedAtUnix <= now` filter | `deferred_alive` bitmap (future publishedAt held invisible) | BitDex uses bitmap-native approach |
| Access control | Always filters private/blocked inline | Defers to post-filter for logged-in users | Avoids per-user cache busting in BitDex |
| Metrics freshness | Backfilled historical metrics | ClickHouse enrichment on sync (may lag) | BitDex metrics arrive via outbox poller |
| Data freshness | Meili indexing pipeline | PG outbox poller (real-time) | BitDex may have newer documents |

## Shadow Mode Metrics (Prometheus)

The model-share shadow mode comparison system records these Prometheus metrics on every production query:

| Metric | Type | Labels | What it tells you |
|--------|------|--------|-------------------|
| `bitdex_shadow_result_overlap` | Histogram | (none) | Jaccard similarity distribution |
| `bitdex_shadow_query_duration_seconds` | Histogram | `source` | Latency comparison |
| `bitdex_shadow_total_matched_diff` | Histogram | (none) | Count divergence |
| `bitdex_shadow_order_match_total` | Counter | `matched` | How often order matches exactly |
| `bitdex_shadow_errors_total` | Counter | `type` | Error categorization |

### Gap: Per-Pattern Divergence Tracking

**Current limitation:** These metrics are aggregate — you can see overall Jaccard distribution but can't tell which sort/filter patterns are diverging most.

**Needed on model-share side:**
Add `sort` and `query_class` labels to `bitdex_shadow_result_overlap` so Grafana can show "MostReactions overlap is low" vs "Newest overlap is high". Proposed labels:

- `sort`: `Newest`, `Oldest`, `MostReactions`, `MostComments`, `MostCollected`
- `query_class`: `simple` (no period/tags), `period` (has period filter), `filtered` (has tags/type/meta), `complex` (period + filters)

This enables a Grafana dashboard panel showing overlap by query pattern, immediately highlighting where BitDex is most different.

## Changes Needed on Model-Share

1. **Add labels to shadow metrics** — `sort` and `query_class` on `bitdex_shadow_result_overlap` and `bitdex_shadow_order_match_total`
2. **Pass sort/filter info to `compareBitdexResults()`** — extend `ComparisonInput` with sort name and filter classification
3. **Add to Grafana dashboard** — panel showing overlap distribution by sort and query_class

## Debugging Tools

### Skip-Cache Flag

BitDex supports `?skip_cache=true` on queries to bypass the unified cache entirely. Use this to isolate whether divergence comes from cache maintenance or underlying data:

```bash
# Direct BitDex query with cache bypass
curl -X POST 'https://bitdex.civitai.com/api/indexes/civitai/query?format=compact&skip_cache=true' \
  -H 'Content-Type: application/json' \
  -d '{"filter":{"nsfwLevel":1},"sort":"-sortAt","limit":20}'
```

### Smoke Tests

Production smoke tests validate BitDex independently (not compared to Meili):

```bash
WEBHOOK_TOKEN=letsgethookie node tests/e2e/e2e-smoke-production.mjs --url https://bitdex.civitai.com
```

See `docs/guide/smoke-tests.md` for details.

## Switchover Plan

1. Shadow mode metrics show sustained high overlap (Tier 1 Jaccard >= 0.8)
2. Per-pattern tracking confirms no unknown broken filter combinations
3. Readiness report passes (`tests/e2e/e2e-compare.mjs` exits 0)
4. Flip BitDex to primary in model-share, Meili to fallback
5. Monitor Grafana dashboard for regression in query latency, error rates
6. After 1 week stable: decommission Meili shadow path
