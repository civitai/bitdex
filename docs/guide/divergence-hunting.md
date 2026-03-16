# Divergence Hunting Playbook

How to find and investigate where BitDex results differ from Meilisearch. Follow this when asked to "check if BitDex matches Meili" or "find where we're different."

## Quick Start

```bash
# Run the readiness report (hits civitai.com comparison endpoint)
node tests/e2e/e2e-compare.mjs --url https://civitai.com --limit 20 --delay 1000

# Verbose mode shows which IDs differ
node tests/e2e/e2e-compare.mjs --url https://civitai.com --limit 20 --delay 1000 --verbose
```

The report categorizes results into three tiers:
- **Tier 1** (must match): queries where both engines should agree
- **Tier 2** (informational): queries with known intentional differences
- **Tier 3** (field validation): data corruption checks on overlapping IDs

## Understanding the Results

### Tier 1 failures: is it freshness or a real bug?

The most common Tier 1 failure is `Newest` sort variants showing jaccard=0. Check whether this is a data freshness gap or a real bug:

1. **Compare IDs**: if BitDex IDs are higher (more recent) than Meili IDs, it's freshness — BitDex indexes faster than Meili.
2. **Check Oldest sort**: if `Oldest` has jaccard=1.0, the filter logic and sort bitmaps are correct. The divergence is only at the head of the sort where freshness matters.
3. **Check MostComments**: if it matches (jaccard=1.0), all-time stable queries agree. The issue is limited to recency-sensitive queries.

**Freshness gap is NOT a bug.** It means BitDex is more current than Meili.

### Tier 2 known differences

These are intentional and expected:

| Pattern | Why it differs |
|---------|----------------|
| Period filters (Day/Week/Month) | BitDex uses time bucket bitmaps (seconds), Meili uses ms timestamp filters |
| MostReactions / MostCollected sorts | Metrics may have enrichment lag from ClickHouse poller |
| Access control (logged-in users) | BitDex defers private/blocked to post-filter to avoid per-user cache busting |
| isPublished | BitDex uses deferred_alive bitmap, Meili uses publishedAtUnix timestamp |

## Drilling Into a Specific Mismatch

### Step 1: Use the compare endpoint directly

```bash
# Spot-check a specific filter combo
curl -s 'https://civitai.com/api/internal/bitdex-compare?sort=Newest&browsingLevel=1&limit=20' | jq .

# Add filters to narrow down
curl -s 'https://civitai.com/api/internal/bitdex-compare?sort=Newest&browsingLevel=1&types=video&limit=20' | jq .

# With tags
curl -s 'https://civitai.com/api/internal/bitdex-compare?sort=Newest&browsingLevel=1&tags=12345&limit=20' | jq .
```

The response includes:
- `comparison.jaccard` — 0-1 similarity score
- `comparison.only_in_meili` — IDs Meili returned but BitDex didn't
- `comparison.only_in_bitdex` — IDs BitDex returned but Meili didn't
- `meili.sample` — field values from Meili results for inspection

### Step 2: Check if it's a cache issue

Use `skip_cache=true` on a direct BitDex query to bypass the unified cache:

```bash
# Query BitDex directly, bypassing cache
curl -s -X POST 'https://bitdex.civitai.com/api/indexes/civitai/query?format=compact&skip_cache=true' \
  -H 'Content-Type: application/json' \
  -d '{"filter":{"nsfwLevel":1},"sort":"-sortAt","limit":20}'

# Same query with cache (default)
curl -s -X POST 'https://bitdex.civitai.com/api/indexes/civitai/query?format=compact' \
  -H 'Content-Type: application/json' \
  -d '{"filter":{"nsfwLevel":1},"sort":"-sortAt","limit":20}'
```

If results differ between cached and uncached: cache maintenance is the problem.
If results are the same: the underlying data/bitmaps are the issue.

### Step 3: Check specific IDs

For IDs that appear "only in Meili" or "only in BitDex", verify them:

```bash
# Check if a Meili-only ID exists in BitDex
curl -s 'https://bitdex.civitai.com/api/indexes/civitai/documents/12345678' | jq .

# Query BitDex for that specific ID
curl -s -X POST 'https://bitdex.civitai.com/api/indexes/civitai/query?format=compact' \
  -H 'Content-Type: application/json' \
  -d '{"filter":{"id":12345678},"sort":"-sortAt","limit":1,"include_docs":true}'
```

This tells you:
- Does the document exist in BitDex at all?
- Does it have the right field values (nsfwLevel, type, isPublished)?
- Is it alive (in the index) or deleted?

### Step 4: Check Prometheus metrics

After model-share deploys with per-pattern labels, query Grafana:

```promql
# Average Jaccard by sort type
avg by (sort) (bitdex_shadow_result_overlap)

# Low-overlap queries (< 0.5) by sort and query class
histogram_quantile(0.5, sum by (sort, query_class, le) (rate(bitdex_shadow_result_overlap_bucket[5m])))

# Order match rate by sort
sum by (sort) (rate(bitdex_shadow_order_match_total{matched="true"}[5m]))
/ sum by (sort) (rate(bitdex_shadow_order_match_total[5m]))
```

Labels:
- `sort`: Newest, Oldest, MostReactions, MostComments, MostCollected
- `query_class`: simple, filtered, period, complex

## Common Root Causes

### 1. Data freshness gap
**Symptom:** Newest jaccard=0, Oldest jaccard=1.0
**Cause:** BitDex indexes via PG outbox in real-time; Meili has its own pipeline
**Fix:** Not a bug. BitDex is more current. Will resolve itself at switchover.

### 2. Metrics enrichment lag
**Symptom:** MostReactions/MostCollected return different IDs; BitDex IDs are newer
**Cause:** ClickHouse metrics poller hasn't enriched recent documents yet
**Fix:** Check pg-sync logs for metrics poller activity. Verify reactionCount in BitDex docs.

### 3. Cache corruption
**Symptom:** Cached and uncached results differ (use skip_cache to test)
**Cause:** Cache maintenance (flush thread) injecting stale data
**Fix:** Purge cache via `DELETE /cache/persistent`, investigate flush thread

### 4. Sort bitmap staleness
**Symptom:** Sort by a field returns wrong order; doc values don't match sort position
**Cause:** Sort bitmaps not updated on upsert (diff saw None→Some(0) as no-op)
**Fix:** Check that outbox poller includes metric fields; may need bulk reload

### 5. Filter logic mismatch in model-share
**Symptom:** Same sort, different IDs, not a freshness issue
**Cause:** BitDex filter builder in `image.service.ts` doesn't match Meili filter logic
**Fix:** Compare `getImagesFromBitdexPreFilter` vs `getImagesFromSearchPreFilter`

## Adding New Test Cases

To add a new filter combination to the matrix, edit `tests/e2e/e2e-compare.mjs`:

```javascript
// Add to TIER1 (should match) or TIER2 (known-divergent)
const TIER1 = [
  // ...existing...
  { name: 'your-test', params: { sort: 'Newest', browsingLevel: '1', tags: '12345' }, description: 'Tag filter test' },
];
```

## Files

| File | Purpose |
|------|---------|
| `tests/e2e/e2e-compare.mjs` | Readiness report (run this) |
| `tests/e2e-compare-results.jsonl` | Historical results |
| `docs/guide/meilisearch-migration.md` | Migration overview |
| model-share: `src/server/bitdex/compare.ts` | Prometheus metrics with per-pattern labels |
| model-share: `src/pages/api/internal/bitdex-compare.ts` | Comparison endpoint |
| model-share: `src/server/services/image.service.ts` | Both filter builders (Meili + BitDex) |
