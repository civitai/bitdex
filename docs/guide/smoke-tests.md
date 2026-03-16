# Production Smoke Tests

`tests/e2e/e2e-smoke-production.mjs` validates that a BitDex production instance returns correct results for all query patterns used by model-share.

## Running

```bash
# Against production (via external URL)
WEBHOOK_TOKEN=letsgethookie node tests/e2e/e2e-smoke-production.mjs --url https://bitdex.civitai.com

# Against a specific pod (via port-forward)
kubectl port-forward -n bitdex bitdex-1 3002:3000 &
node tests/e2e/e2e-smoke-production.mjs --url http://localhost:3002

# With delay between queries (useful during lazy loading warmup)
node tests/e2e/e2e-smoke-production.mjs --url http://localhost:3002 --delay 500

# Verbose output (shows query details)
node tests/e2e/e2e-smoke-production.mjs --url https://bitdex.civitai.com --verbose
```

## What It Tests

| Group | Tests | What it validates |
|-------|-------|-------------------|
| 0. Health | 1 | Index exists with >1M records |
| 1. Main Feed | 3 | nsfwLevel filter, sortAt sort order, isPublished |
| 2. Sort Variants | 6 | All 5 sort fields + value ordering |
| 3. Filter Types | 8 | userId, type, hasMeta, onSite, tagIds, modelVersionIds, availability |
| 4. Exclusions | 9 | NOT filters, boolean filters, baseModel, toolIds, techniqueIds, postedToId |
| 5. Period Filters | 2 | 24h and 7d time ranges via sortAtUnix |
| 6. Combined | 2 | Multi-filter + sort combos |
| 7. Pagination | 2 | Cursor pagination for sortAt and reactionCount |
| 8. Doc Content | 2 | Required and metrics fields present |
| 9. Metrics | 2 | reactionCount sort matches doc values, no zeroes in top 20 |
| 10. isPublished | 2 | Published count < total, unfiltered may include unpublished |
| 11. Edge Cases | 4 | Empty filter, limit 1/100, large offset |
| 12. Data Comparison | 3 | Record count vs PG estimate, max ID gap, realistic metrics |

**Total: ~47 tests**, 3 may skip on fresh bulk loads (enrichment fields).

## Expected Skips

**tagIds, toolIds, techniqueIds** may skip on freshly bulk-loaded replicas. These multi-value fields use idle eviction with per-value lazy loading. On a fresh replica, the docs returned by `reactionCount` sort may not have these fields loaded in the document yet.

This is normal and does not indicate a bug:
- The bitmaps are loaded (filtering works)
- The docs just don't have the arrays populated until the field values are queried
- On a warmed-up replica with active sync, these tests pass

**Data comparison** tests skip without `WEBHOOK_TOKEN`. The token authenticates against `civitai.com/api/internal/bitdex-stats` which returns `imageCountEstimate` and `maxImageId` from PG.

## Data Comparison

The stats endpoint replaces direct PG/CH queries. Set `WEBHOOK_TOKEN` env var or `--stats-token`:

```bash
WEBHOOK_TOKEN=letsgethookie node tests/e2e/e2e-smoke-production.mjs --url https://bitdex.civitai.com
```

Tests:
1. **Record count within 10% of PG estimate** — BitDex alive_count vs PG imageCountEstimate
2. **Max ID gap < 1M** — BitDex highest ID vs PG maxImageId (measures sync lag)
3. **Top reactionCount > 10K** — sanity check that metrics are populated

## Enrichment Field Strategy

Newly published images go through an ingestion pipeline: image → post → tags → resources → tools → techniques → metrics. Tags and other enrichment fields may take minutes to hours to appear.

The tests handle this by:
- Using `reactionCount` sort (returns established, older images with enrichment)
- Falling back to `offset: 200` on `sortAt` sort (skips past very recent images)
- Gracefully skipping if no docs with the field are found

## When to Run

- After deploying a new BitDex version
- After a bulk load on a new replica
- As part of the recurring health check loop
- Before enabling a replica for production traffic
