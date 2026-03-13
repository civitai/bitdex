# BitDex Shadow Mode — Integration Plan

## Goal

Run BitDex alongside Meilisearch in production without serving from it. BitDex receives the same queries and updates, results are compared and logged, but Meilisearch remains the source of truth. Rollout controlled via Flipt with three segments: `off`, `shadow`, `primary`.

---

## Current Architecture (Meilisearch)

### Search Path
- `getImagesFromSearch()` in `image.service.ts:1913` is the entry point
- Flipt `FEED_POST_FILTER` flag selects between `getImagesFromSearchPreFilter` and `getImagesFromSearchPostFilter`
- Both build Meilisearch filter strings + sort params, call `metricsSearchClient.index(METRICS_SEARCH_INDEX).getDocuments()`
- Results go through post-filtering (NSFW, existence check, blocked content), then metrics enrichment via ClickHouse
- Sort options: `sortAt` (desc/asc), `reactionCount` (desc), `commentCount` (desc), `collectedCount` (desc)

### Index Schema (metrics-images.search-index.ts)
- **Filterable**: `id`, `sortAtUnix`, `modelVersionIds`, `modelVersionIdsManual`, `postedToId`, `baseModel`, `type`, `hasMeta`, `onSite`, `toolIds`, `techniqueIds`, `tagIds`, `userId`, `nsfwLevel`, `combinedNsfwLevel`, `postId`, `publishedAtUnix`, `existedAtUnix`, `flags.promptNsfw`, `remixOfId`, `availability`, `poi`, `minor`, `blockedFor`
- **Sortable**: `id`, `sortAt`, `reactionCount`, `commentCount`, `collectedCount`
- **Ranking**: `['sort']` (sort-only, no relevance)

### Update Path
- `imagesMetricsDetailsSearchIndex` processor in `metrics-images.search-index.ts`
- Pulls from Postgres + ClickHouse in batched steps (images → metrics → tags → tools/techniques → model versions)
- Pushes to Meilisearch via `updateDocs()` in batches of 100K
- Runs continuously, processing new + updated images since last run

### Flipt Pattern
- `FliptSingleton.getInstance()` returns a `FliptClient`
- `evaluateBoolean()` for on/off flags, `evaluateVariant()` for multi-segment (we need variant)
- `getFliptVariant()` helper already exists in `flipt/client.ts`
- Entity ID is `currentUserId?.toString() || 'anonymous'`

---

## BitDex HTTP API (already built)

The BitDex server (`src/bin/server.rs`) already has:
- `POST /api/indexes/{name}/query` — accepts `BitdexQuery` (filters + sort + limit + cursor)
- `POST /api/indexes/{name}/load` — bulk NDJSON loading
- `GET /api/indexes/{name}/stats` — alive count, memory stats

Queries use typed enums: `{"Eq": ["tagIds", {"Integer": 4}]}`, sort: `{"field": "reactionCount", "direction": "Desc"}`

**What's missing for shadow mode**:
1. A real-time update endpoint (`PUT /api/indexes/{name}/documents`) for incremental updates
2. Query translation from Meilisearch filter strings to BitDex `FilterClause` AST

---

## Plan

### Phase 1: BitDex Server Additions

#### 1a. Document upsert endpoint
`PUT /api/indexes/{name}/documents` — accepts batch of documents in the same schema as NDJSON loading.

```
POST /api/indexes/{name}/documents/upsert
Content-Type: application/json

{
  "documents": [
    {"id": 123, "nsfwLevel": 1, "tagIds": [4, 10], "reactionCount": 42, ...}
  ]
}
```

The server already has `engine.put(slot_id, &document)` — this just needs an HTTP wrapper that converts JSON documents through the existing `DataSchema` → `Document` pipeline.

#### 1b. Bulk delete endpoint
`POST /api/indexes/{name}/documents/delete` — accepts list of IDs to delete.

```json
{"ids": [123, 456]}
```

Maps to `engine.delete(slot_id)` for each.

> **Q for Justin**: Do we need deletes for shadow mode, or just upserts? Meilisearch's `updateDocs` does upsert-only — I don't see a delete path in the current indexer. If images get deleted, do they just stay in the search index until the next full reindex?
@justin: Yes, we do. Let's make it `DELETE /api/indexes/{name}/documents`

---

### Phase 2: TypeScript Client for BitDex

New file: `src/server/bitdex/client.ts`

A lightweight HTTP client that:
1. Translates Meilisearch-style filter strings → BitDex `FilterClause[]` JSON
2. Translates sort params → BitDex `SortClause`
3. Sends queries to BitDex's HTTP API
4. Translates results back to the same `ImageMetricsSearchIndexRecord` shape

#### 2a. Filter translation

Meilisearch filter strings like:
```
nsfwLevel IN [1,2] AND tagIds IN [4,10] AND sortAtUnix > 1709251200000 AND hasMeta = true
```

Need to become BitDex `FilterClause[]`:
```json
[
  {"In": ["nsfwLevel", [{"Integer": 1}, {"Integer": 2}]]},
  {"In": ["tagIds", [{"Integer": 4}, {"Integer": 10}]]},
  {"Gt": ["sortAtUnix", {"Integer": 1709251200000}]},
  {"Eq": ["hasMeta", {"Bool": true}]}
]
```

This is a string parser for the Meilisearch filter syntax subset we actually use. Looking at the codebase, the filters are built programmatically via `makeMeiliImageSearchFilter()`, so we know the exact patterns:
- `field = value`
- `field != value`
- `field IN [v1, v2]`
- `field NOT IN [v1, v2]`
- `field > value`, `field >= value`, `field < value`, `field <= value`
- `field NOT EXISTS`, `field IS NULL`
- `NOT (...)` groups
- `AND` / `OR` combinators with parens

> **Q for Justin**: Some Meilisearch filters use fields BitDex doesn't index yet: `baseModel` (string), `postedToId`, `postId`, `publishedAtUnix`, `existedAtUnix`, `combinedNsfwLevel`, `flags.promptNsfw`, `remixOfId`, `availability`, `poi`, `minor`, `blockedFor`, `modelVersionIdsManual`. Two options:
>
> **Option A**: Add all filterable fields to BitDex config (more fields = more memory, but complete parity).
> **Option B**: Only translate the fields BitDex knows about, ignore the rest (BitDex returns a superset, post-filter in TS handles the rest — which is what already happens for some fields).
>
> I'd lean toward **Option B** for the initial shadow, then add fields as needed based on comparison data. Thoughts?
@justin: Option A. But double check if we need `existedAtUnix`, `combinedNsfwLevel`, `flags.promptNsfw`, `remixOfId`. BaseModel should be a low cardinality string u16.

#### 2b. Sort translation

Straightforward mapping:
| Meilisearch | BitDex |
|---|---|
| `sortAt:desc` | `{"field": "sortAt", "direction": "Desc"}` |
| `reactionCount:desc` | `{"field": "reactionCount", "direction": "Desc"}` |
| `commentCount:desc` | `{"field": "commentCount", "direction": "Desc"}` |
| `collectedCount:desc` | `{"field": "collectedCount", "direction": "Desc"}` |

> **Q for Justin**: BitDex currently only has `reactionCount` and `sortAt` as sort fields. Do we need `commentCount` and `collectedCount` too? They're used by `MostComments` and `MostCollected` sort options. Easy to add (just more sort layer bitmaps, ~64MB each at 105M).
@justin: Yes

#### 2c. Result translation

BitDex returns `{ids: [i64], total_matched, cursor, elapsed_us}`. Shadow mode needs to:
1. Take the IDs
2. Fetch full documents from BitDex's docstore (`POST /api/indexes/{name}/documents` with `slot_ids`)
3. OR (better for shadow comparison): just compare the **ID ordering** between Meilisearch and BitDex results

For shadow mode, comparing ID sets is sufficient — we don't need to reconstruct full `ImageMetricsSearchIndexRecord` objects.
@justin: I think we should have BitDex return more than just the ids... How is our current front-end doing it? Is it doing two requests? Maybe it's a query param to get just the IDs (which I assume is faster). For shadow mode it can just be ID comparison. We'd want to track differences via prom metrics

---

### Phase 3: Flipt Integration

New flag: `BITDEX_IMAGE_SEARCH` (variant flag, not boolean)

Segments:
| Variant | Behavior |
|---|---|
| `off` | Meilisearch only (default, no BitDex involvement) |
| `shadow` | Meilisearch serves response + BitDex query fires async in background. Results compared and logged. |
| `primary` | BitDex serves response. Meilisearch still queried in background for comparison logging. |

Add to `FLIPT_FEATURE_FLAGS` enum:
```typescript
BITDEX_IMAGE_SEARCH = 'bitdex-image-search',
```

#### Integration point: `getImagesFromSearch()`

```typescript
export async function getImagesFromSearch(input: ImageSearchInput) {
  // Existing Flipt logic for pre/post filter selection...
  let searchFn = getImagesFromSearchPreFilter;
  // ...

  const bitdexMode = await getFliptVariant(
    FLIPT_FEATURE_FLAGS.BITDEX_IMAGE_SEARCH,
    input.currentUserId?.toString() || 'anonymous'
  );

  if (bitdexMode === 'primary') {
    // BitDex serves, Meilisearch shadows
    const [bitdexResult, meiliResult] = await Promise.allSettled([
      getImagesFromBitdex(input),
      searchFn(input),
    ]);
    compareAndLog(bitdexResult, meiliResult, input);
    if (bitdexResult.status === 'fulfilled') return bitdexResult.value;
    // Fallback to Meilisearch if BitDex fails
    if (meiliResult.status === 'fulfilled') return meiliResult.value;
    throw bitdexResult.reason;
  }

  if (bitdexMode === 'shadow') {
    // Meilisearch serves, BitDex shadows (fire-and-forget)
    const meiliResult = await searchFn(input);
    getImagesFromBitdex(input)
      .then(bitdexResult => compareAndLog(bitdexResult, meiliResult, input))
      .catch(err => logBitdexError(err, input));
    return meiliResult;
  }

  // 'off' or unknown — Meilisearch only
  return searchFn(input);
}
```

> **Q for Justin**: For `shadow` mode, should the BitDex query be truly fire-and-forget (no await, doesn't block response), or should we `await` both with a timeout so we always get comparison data? Fire-and-forget is safer for latency but we might miss comparison data under load.
@justin: Fire and forget, we should be measuring the response time in buckets or something via prom though since we'll care about that. Oh, you covered all the prom metrics below, great.

---

### Phase 4: Comparison Logging

New file: `src/server/bitdex/compare.ts`

For each shadow query, log:
```typescript
{
  type: 'bitdex-shadow-comparison',
  // Timing
  meili_elapsed_ms: number,
  bitdex_elapsed_ms: number,
  // Result comparison
  meili_total: number,
  bitdex_total: number,
  meili_ids: number[],     // first page
  bitdex_ids: number[],    // first page
  overlap_count: number,   // IDs present in both
  order_match: boolean,    // same ordering?
  jaccard_similarity: number, // overlap / union
  // Query context
  sort: string,
  filter_count: number,
  limit: number,
  user_id: number | null,
}
```

Log to Axiom via existing `logToAxiom()`. This gives us:
- Latency comparison (is BitDex faster?)
- Result accuracy (do we return the same images?)
- Coverage gaps (which queries produce different results and why?)

@justin: We don't want to use axiom as much as we can. Can we focus on prom instead?

---

### Phase 5: Update Pipeline

The existing `imagesMetricsDetailsSearchIndex` processor pushes documents to Meilisearch. We need BitDex to receive the same updates.

#### Option: Dual-write in the push step

In `metrics-images.search-index.ts`, the `pushData` function:

```typescript
pushData: async ({ indexName }, data) => {
  if (data.length > 0) {
    await updateDocs({ indexName, documents: data, ... });

    // Shadow: also push to BitDex
    if (bitdexEnabled) {
      await pushToBitdex(data).catch(err => {
        console.error('BitDex push failed:', err);
      });
    }
  }
},
```

`pushToBitdex()` would call the upsert endpoint from Phase 1a, mapping the `ImageMetricsSearchIndexRecord` to BitDex's document format.

> **Q for Justin**: Should the BitDex update be gated behind its own Flipt flag, or just always-on once deployed? If BitDex isn't receiving queries (flag = `off`), the updates are wasted work. But if we gate updates too, there's a cold-start delay when enabling shadow mode. I'd suggest: updates always flow once BitDex is deployed, queries are gated by Flipt.
@justin: Updates always flow, the gate is only for the image.service.ts file for fetching requests. However the updates should fail silently so they don't block meilisearch updates

---

### Phase 6: Local Development

1. Run BitDex server locally: `cargo run --release --bin server --features server`
2. Create index via API with the Civitai schema
3. Load initial data from NDJSON (or connect to the update pipeline)
4. Point `BITDEX_URL` env var at `http://localhost:3000`
5. Set Flipt flag to `shadow` for your user ID
6. Browse the site — every image search query gets shadowed to BitDex
7. Check Axiom for comparison logs

---

### Phase 7: Kubernetes Deployment

#### BitDex pod
- Single-container deployment (single process, single node per CLAUDE.md)
- Resource request: 16GB RAM, 4 CPU (105M records = ~14.5GB RSS)
- Persistent volume for bitmap snapshot + docstore (~10GB)
- Startup: loads from bitmap snapshot in <1s (lazy loading), ready to serve immediately
- Health check: `GET /api/health`
- No horizontal scaling needed (single-node design)

#### ConfigMap / Secrets
- `BITDEX_URL`: internal service URL (`http://bitdex:3000`)
- Index config and data schema baked into a ConfigMap, applied via init container or startup script

#### Initial data load
- Option A: Load from NDJSON in an init container (5-6 min for 105M)
- Option B: Pre-bake the bitmap snapshot into the persistent volume (instant startup)
- Option C: Bootstrap from the update pipeline (slow — hours for 105M via incremental updates)

> **Q for Justin**: For k8s, do we want to pre-bake the NDJSON into the image/PV, or have an init step that pulls it? The NDJSON is 95GB which is unwieldy. We could also add a "backfill from Meilisearch" mode that pulls documents from the existing Meilisearch index.
@justin: We'll tackle this later, but basically we'll upload the NDJSON and load from disk/PV.

#### Rollout sequence
1. Deploy BitDex pod + PV, load data, verify health
2. Deploy model-share with BitDex client code + Flipt flag (flag = `off`)
3. Enable `shadow` for internal users → verify comparison logs look sane
4. Enable `shadow` for 1% → 10% → 100% of traffic
5. Analyze comparison data, fix any discrepancies
6. Enable `primary` for internal users → dogfood
7. Enable `primary` for 1% → 10% → 100%

---

## Files to Create/Modify

| File | Change |
|---|---|
| `bitdex-v2/src/server.rs` | Add `POST /documents/upsert` and `POST /documents/delete` endpoints |
| `model-share/src/server/bitdex/client.ts` | **New** — BitDex HTTP client with filter translation |
| `model-share/src/server/bitdex/compare.ts` | **New** — Shadow comparison logging |
| `model-share/src/server/bitdex/filter-translator.ts` | **New** — Meilisearch filter string → BitDex FilterClause[] |
| `model-share/src/server/flipt/client.ts` | Add `BITDEX_IMAGE_SEARCH` flag |
| `model-share/src/server/services/image.service.ts` | Modify `getImagesFromSearch()` to add BitDex shadow/primary path |
| `model-share/src/server/search-index/metrics-images.search-index.ts` | Dual-write in `pushData` |
| `model-share/src/env/server.ts` | Add `BITDEX_URL` env var |
| k8s manifests (TBD) | Deployment, Service, PVC, ConfigMap for BitDex |

---

## Open Questions Summary

1. **Deletes**: Does the current indexer ever delete from Meilisearch, or just upserts?
2. **Field coverage**: Option A (add all fields to BitDex) vs Option B (subset + post-filter)?
3. **Sort fields**: Add `commentCount` and `collectedCount` sort fields to BitDex?
4. **Shadow blocking**: Fire-and-forget vs awaited-with-timeout for shadow queries?
5. **Update gating**: Always push updates to BitDex once deployed, or gate behind Flipt?
6. **K8s data loading**: Pre-bake NDJSON, init container, or backfill from Meilisearch?
