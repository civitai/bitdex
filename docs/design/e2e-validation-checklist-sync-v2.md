---
status: ACTIVE
updated: 2026-03-28
---

# Sync V2 End-to-End Validation Checklist

> **Owner:** QA Engineer | **Created:** 2026-03-28
> **Purpose:** Comprehensive pre-production validation for the Sync V2 pipeline.
> **Standard:** Every item is "done with proof" or "explicitly deferred with reason." No silent skips.

**Prerequisites:**
- BitDex server running: `cargo run --release --features server,pg-sync --bin bitdex-server -- --port 3001 --data-dir ./data --config deploy/configs/civitai-index.json`
- CSV files in `data/load_stage/` (tags.csv, images.csv, posts.csv, resources.csv, model_versions.csv, models.csv, tools.csv, techniques.csv, metrics.tsv)
- Existing validation scripts: `tools/validate-dump-processor.mjs`, `tools/e2e-phase2-validation.mjs`, `tools/e2e-gate3-triggers.mjs`

---

## A. Dump Pipeline (Bulk Load)

### A.1 Phase-Level Verification

For each phase, verify the dump request completes and row counts match source CSVs.

**How to get expected counts:**
```bash
# Count CSV rows (subtract 1 if header present; these CSVs have no header from COPY)
wc -l data/load_stage/tags.csv
wc -l data/load_stage/images.csv
wc -l data/load_stage/resources.csv
wc -l data/load_stage/tools.csv
wc -l data/load_stage/techniques.csv
wc -l data/load_stage/metrics.tsv
```

| # | Check | Command / Method | Pass Criteria | Status |
|---|-------|-----------------|---------------|--------|
| A.1.1 | Tags phase completes | `PUT /api/indexes/civitai/dumps` with tags dump body (see `tools/validate-dump-processor.mjs` DUMP_REQUESTS[0]) | HTTP 200, progress reports complete, no errors | |
| A.1.2 | Tags row count | After tags dump, query `tagIds eq {known_tag_id}` for a high-frequency tag. Cross-reference: `grep -c ",{known_tag_id}," tags.csv` minus disabled rows | Counts match within tolerance of disabled-tag filter | |
| A.1.3 | Tags disabled filter | Pick a tag with `(attributes >> 10) & 1 = 1`. Query `tagIds eq {disabled_tag}`. | Returns 0 results | |
| A.1.4 | Images phase completes | PUT dumps with images body (DUMP_REQUESTS[1]) | HTTP 200, no errors | |
| A.1.5 | Images alive count | `GET /api/indexes/civitai/stats` → `alive_count` | Matches `wc -l images.csv` | |
| A.1.6 | Resources phase completes | PUT dumps with resources body (DUMP_REQUESTS[2]) | HTTP 200, no errors | |
| A.1.7 | Resources: modelVersionIds populated | `curl -s http://localhost:3001/api/indexes/civitai/query -d '{"filters":[{"In":["modelVersionIds",[{"Integer":12345}]]}],"limit":5}'` (use a known MV ID from resources.csv) | Returns non-empty ids array | |
| A.1.8 | Resources: modelVersionIdsManual | Query `modelVersionIdsManual eq {mv_id}` where that MV had `detected=false` in resources.csv | Returns the correct imageIds | |
| A.1.9 | Tools phase completes | PUT dumps with tools body | HTTP 200 | |
| A.1.10 | Tools: toolIds populated | Query `toolIds eq {known_tool_id}` | Count matches `grep -c "{tool_id}" tools.csv` | |
| A.1.11 | Techniques phase completes | PUT dumps with techniques body | HTTP 200 | |
| A.1.12 | Techniques: techniqueIds populated | Query `techniqueIds eq {known_technique_id}` | Count matches CSV | |
| A.1.13 | Metrics phase completes | PUT dumps with metrics body (TSV format) | HTTP 200 | |
| A.1.14 | Metrics: reactionCount sort | `curl -s http://localhost:3001/api/indexes/civitai/query -d '{"filters":[],"sort":{"field":"reactionCount","direction":"desc"},"limit":10}'` | Top 10 IDs match top 10 from metrics.tsv sorted by reactionCount desc | |

### A.2 Field Mapping Verification

| # | Check | Command | Pass Criteria | Status |
|---|-------|---------|---------------|--------|
| A.2.1 | nsfwLevel filter | `curl -s http://localhost:3001/api/indexes/civitai/query -d '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]}],"limit":1}'` | Returns results; count matches `awk -F, '{if($3==1)c++}END{print c}' images.csv` | |
| A.2.2 | type filter (LCS) | `curl -s http://localhost:3001/api/indexes/civitai/query -d '{"filters":[{"Eq":["type",{"String":"image"}]}],"limit":1}'` | Returns results; LCS dictionary encodes "image" correctly | |
| A.2.3 | userId filter | Query `userId eq {known_user_id}` | Returns correct image IDs for that user | |
| A.2.4 | postId filter | Query `postId eq {known_post_id}` | Returns images belonging to that post | |
| A.2.5 | blockedFor filter (LCS) | Query `blockedFor eq "TOS"` (or whatever values exist) | Returns correct count | |
| A.2.6 | availability filter (LCS, enriched) | Query `availability eq "Public"` | Returns images whose post has availability=Public | |
| A.2.7 | baseModel filter (enriched, nested) | Query `baseModel eq "SDXL 1.0"` | Only returns images linked to Checkpoint-type models with SDXL baseModel | |
| A.2.8 | postedToId filter (enriched, lookup_key) | Query `postedToId eq {post_id}` | Same results as `postId eq {post_id}` (they should match since postedToId = the Post ID looked up via postId) | |
| A.2.9 | remixOfId filter | Query `remixOfId eq {known_remix_id}` | Returns correct images | |

### A.3 Boolean / Computed Field Verification

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| A.3.1 | hasMeta (computed from flags) | Query `hasMeta eq true`. Pick 5 result IDs, look up their `flags` in images.csv. Verify `(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0` for each. | All 5 satisfy the expression | |
| A.3.2 | hasMeta false case | Pick an image where `(flags >> 2) & 1 == 1` (mature override clears hasMeta). Verify it is NOT in `hasMeta eq true` results. | Confirmed absent | |
| A.3.3 | onSite (computed) | Query `onSite eq true`. Spot-check 5 IDs against `(flags >> 14) & 1 == 1`. | All match | |
| A.3.4 | minor (computed) | Query `minor eq true`. Spot-check against `(flags >> 3) & 1 == 1`. | All match | |
| A.3.5 | poi (computed, dual source) | Query `poi eq true`. Verify includes images where either `(flags >> 4) & 1 == 1` OR the linked Model has `poi=true` (via resources enrichment, Checkpoint filter). | Both sources contribute | |
| A.3.6 | isPublished (exists_boolean) | Query `isPublished eq true`. Spot-check: these images' posts have non-null publishedAt in posts.csv. | Match | |
| A.3.7 | isPublished false | Query `isPublished eq false`. These images either have no post match OR their post's publishedAt is null. | Match | |
| A.3.8 | isRemix (exists_boolean) | Query `isRemix eq true`. Verify these images have non-null remixOfId. | Match | |

### A.4 Deferred Alive

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| A.4.1 | Future publishedAt excluded | Find an image whose post has `publishedAt` in the future (or insert one). Query without filters. | Image NOT in results | |
| A.4.2 | Past publishedAt included | Same query, image with past publishedAt. | Image IS in results | |
| A.4.3 | Null publishedAt handling | Image with no post match (thus null publishedAt). Should still be alive if `sets_alive=true` from images phase. Verify behavior matches design. | Documented behavior confirmed | |

### A.5 Sort Field Verification

| # | Check | Command | Pass Criteria | Status |
|---|-------|---------|---------------|--------|
| A.5.1 | sortAt correctness | Query `sort=sortAt desc limit=10`. For each result, compute expected sortAt = GREATEST(existedAt, publishedAt) where existedAt = max(scannedAtSecs, createdAtSecs) from images.csv and publishedAt from posts.csv. **Note:** sortAtUnix with ms_to_seconds conversion means `sortAtUnix / 1000` should equal sortAt. | Top 10 in correct order | |
| A.5.2 | sortAt: ms_to_seconds | Verify sortAt values are in seconds (10-digit epoch), NOT milliseconds (13-digit). | All values < 2^32, reasonable epoch range | |
| A.5.3 | reactionCount sort | `sort=reactionCount desc limit=10`. Cross-reference with metrics.tsv. | Order matches | |
| A.5.4 | commentCount sort | Same for commentCount. | Order matches | |
| A.5.5 | collectedCount sort | Same for collectedCount. | Order matches | |
| A.5.6 | publishedAt sort | `sort=publishedAt desc limit=10`. Cross-reference with posts.csv. | Order matches | |
| A.5.7 | id sort | `sort=id desc limit=10`. Should return highest slot IDs. | Matches highest IDs in images.csv | |
| A.5.8 | Ascending sort | `sort=reactionCount asc limit=10`. | Returns lowest values (likely many 0s) | |

### A.6 RSS and Performance

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| A.6.1 | 107M load RSS | Monitor `bitdex_rss_bytes` or `/proc/PID/status` VmRSS during full load | Peak < 15 GB | |
| A.6.2 | Load time | Wall-clock time for all 6 phases | < 15 minutes total | |
| A.6.3 | Per-phase memory drop | RSS after tags phase save+drop should be lower than peak during tags processing | Confirmed via metrics | |
| A.6.4 | Crash recovery (V1.7) | Kill server mid-resources phase (`kill -9`). Restart. Verify tags and images phases are preserved (query works for those fields). Resources phase can be re-run. | No data corruption, completed phases intact | |

---

## B. Document Verification

### B.1 doc_only Fields Present

```bash
# Get a document for a known slot
curl -s http://localhost:3001/api/indexes/civitai/documents/{slot_id}
```

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| B.1.1 | url present in doc | GET document for 5 random alive slots | `url` field present, matches images.csv | |
| B.1.2 | hash present in doc | Same 5 slots | `hash` field present, matches images.csv | |
| B.1.3 | width present in doc | Same 5 slots | `width` field present, integer, matches images.csv | |
| B.1.4 | height present in doc | Same 5 slots | `height` field present, integer, matches images.csv | |
| B.1.5 | needsReview in doc | Find a slot where needsReview is non-null in source data | Field present with correct value | |
| B.1.6 | acceptableMinor in doc | Check doc for a slot | Field present (boolean), defaults to false | |
| B.1.7 | index in doc | Check doc for a slot | Field present (integer), defaults to 0 | |

### B.2 filter_only Fields NOT in Docstore

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| B.2.1 | toolIds NOT in doc | GET document for a slot that has toolIds in bitmaps | `toolIds` key absent from document JSON | |
| B.2.2 | techniqueIds NOT in doc | Same | `techniqueIds` key absent | |
| B.2.3 | modelVersionIdsManual NOT in doc | Same | `modelVersionIdsManual` key absent | |

### B.3 Multi-value Fields IN Docstore

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| B.3.1 | tagIds in doc | GET document for slot with known tags | `tagIds` present as integer array, values match tags.csv rows for that imageId (minus disabled) | |
| B.3.2 | modelVersionIds in doc | GET document for slot with known resources | `modelVersionIds` present as integer array | |

### B.4 LCS String Fields

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| B.4.1 | type returns text | GET document, check `type` field | Returns string like "image", "video", "audio" — not a numeric dictionary ID | |
| B.4.2 | baseModel returns text | Check `baseModel` field | Returns string like "SDXL 1.0", "SD 1.5" — not numeric | |
| B.4.3 | availability returns text | Check `availability` field | Returns string like "Public", "Private" — not numeric | |
| B.4.4 | blockedFor returns text | Find slot with non-null blockedFor | Returns string value — not numeric | |
| B.4.5 | LCS survives restart | Restart server, query `type eq "image"`, verify results unchanged | Same count and IDs | |

### B.5 Query with include_docs

```bash
curl -s http://localhost:3001/api/indexes/civitai/query -d '{
  "filters": [{"Eq": ["nsfwLevel", {"Integer": 1}]}],
  "sort": {"field": "sortAt", "direction": "desc"},
  "limit": 5,
  "include_docs": true
}'
```

| # | Check | Pass Criteria | Status |
|---|-------|---------------|--------|
| B.5.1 | All 30 data_schema fields present | Response includes all non-null fields from civitai-index.json data_schema for each document | |
| B.5.2 | Field values match source CSV | Spot-check 5 docs: nsfwLevel, userId, postId, type, url, hash, width, height, reactionCount, sortAt all match source data | |
| B.5.3 | Enriched fields present | publishedAt, availability, baseModel, postedToId present where applicable | |
| B.5.4 | Computed booleans correct | hasMeta, onSite, minor, poi, isPublished, isRemix match computed expressions | |

---

## C. Query Correctness

### C.1 Filter Operators

```bash
# Template — replace FILTER with each test case
curl -s http://localhost:3001/api/indexes/civitai/query -d '{
  "filters": [FILTER],
  "limit": 10,
  "skip_cache": true
}'
```

| # | Check | Filter | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| C.1.1 | Eq single_value | `{"Eq":["nsfwLevel",{"Integer":1}]}` | Returns results; count matches CSV grep | |
| C.1.2 | NotEq single_value | `{"NotEq":["nsfwLevel",{"Integer":1}]}` | Returns all alive minus nsfwLevel=1 | |
| C.1.3 | In single_value | `{"In":["nsfwLevel",[{"Integer":1},{"Integer":2}]]}` | Union of nsfwLevel=1 and nsfwLevel=2 | |
| C.1.4 | NotIn single_value | `{"NotIn":["nsfwLevel",[{"Integer":1},{"Integer":2}]]}` | All alive minus 1 and 2 | |
| C.1.5 | Eq boolean | `{"Eq":["hasMeta",{"Boolean":true}]}` | Matches computed hasMeta | |
| C.1.6 | Eq boolean false | `{"Eq":["hasMeta",{"Boolean":false}]}` | Complement of hasMeta=true within alive | |
| C.1.7 | In multi_value | `{"In":["tagIds",[{"Integer":42}]]}` | All images with tag 42 | |
| C.1.8 | NotIn multi_value | `{"NotIn":["tagIds",[{"Integer":42}]]}` | All alive images WITHOUT tag 42 | |
| C.1.9 | Eq multi_value | `{"Eq":["tagIds",{"Integer":42}]}` | Same as In with single value | |
| C.1.10 | Range sort field | `{"Range":["reactionCount",{"gte":{"Integer":100},"lt":{"Integer":200}}]}` | Only images with 100 <= reactionCount < 200 | |
| C.1.11 | Eq LCS string | `{"Eq":["type",{"String":"image"}]}` | Matches type=image | |
| C.1.12 | In LCS string | `{"In":["type",[{"String":"image"},{"String":"video"}]]}` | Union of both types | |
| C.1.13 | Multiple filters (AND) | `[{"Eq":["nsfwLevel",{"Integer":1}]},{"Eq":["hasMeta",{"Boolean":true}]}]` | Intersection of both conditions | |
| C.1.14 | Empty result | `{"Eq":["nsfwLevel",{"Integer":999999}]}` | Returns `{"ids":[],"total":0}` | |

### C.2 Sort Queries

| # | Check | Sort Config | Pass Criteria | Status |
|---|-------|-------------|---------------|--------|
| C.2.1 | sortAt desc | `{"field":"sortAt","direction":"desc"}` | Descending order verified for first 10 | |
| C.2.2 | sortAt asc | `{"field":"sortAt","direction":"asc"}` | Ascending order | |
| C.2.3 | reactionCount desc | `{"field":"reactionCount","direction":"desc"}` | Descending order | |
| C.2.4 | commentCount desc | `{"field":"commentCount","direction":"desc"}` | Descending order | |
| C.2.5 | collectedCount desc | `{"field":"collectedCount","direction":"desc"}` | Descending order | |
| C.2.6 | publishedAt desc | `{"field":"publishedAt","direction":"desc"}` | Descending order | |
| C.2.7 | id desc | `{"field":"id","direction":"desc"}` | Descending order | |
| C.2.8 | Filter + sort | Filter `nsfwLevel eq 1` + sort `reactionCount desc` | Filtered set in correct sort order | |

### C.3 Pagination

```bash
# Page 1
curl -s http://localhost:3001/api/indexes/civitai/query -d '{
  "filters": [{"Eq":["nsfwLevel",{"Integer":1}]}],
  "sort": {"field":"sortAt","direction":"desc"},
  "limit": 5
}'
# Page 2 — use cursor from page 1 response
curl -s http://localhost:3001/api/indexes/civitai/query -d '{
  "filters": [{"Eq":["nsfwLevel",{"Integer":1}]}],
  "sort": {"field":"sortAt","direction":"desc"},
  "limit": 5,
  "cursor": "{cursor_from_page_1}"
}'
```

| # | Check | Pass Criteria | Status |
|---|-------|---------------|--------|
| C.3.1 | Cursor returned | First page response includes `cursor` field | |
| C.3.2 | Page 2 continues | Page 2 IDs do not overlap with page 1 IDs | |
| C.3.3 | Sort order maintained | Page 2 sort values all <= last page 1 sort value (desc) | |
| C.3.4 | Deep pagination (page 20) | 20 sequential pages with no duplicate IDs across all pages | |
| C.3.5 | Exhaustion | Eventually returns empty ids with no cursor | |

---

## D. Ops Pipeline (Steady-State)

**Reference script:** `tools/e2e-phase2-validation.mjs` (covers V2.1-V2.11)

### D.1 Single Document Ops

```bash
# Upsert entity 500000001 (beyond existing range to avoid collision)
curl -s http://localhost:3001/api/indexes/civitai/ops -d '{
  "ops": [{
    "entity_id": 500000001,
    "creates_slot": true,
    "ops": [
      {"op": "set", "field": "nsfwLevel", "value": 8},
      {"op": "set", "field": "userId", "value": 12345},
      {"op": "set", "field": "type", "value": "image"},
      {"op": "set", "field": "reactionCount", "value": 77},
      {"op": "set", "field": "url", "value": "https://example.com/test.jpg"},
      {"op": "set", "field": "width", "value": 1024},
      {"op": "set", "field": "height", "value": 768}
    ]
  }],
  "meta": {"source": "validation", "cursor": 1}
}'
```

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| D.1.1 | POST /ops accepted | HTTP response | 200 OK | |
| D.1.2 | Bitmap updated | Wait 500ms, query `nsfwLevel eq 8` | 500000001 in results | |
| D.1.3 | Sort updated | Query `sort=reactionCount desc limit=100` | 500000001 appears if reactionCount=77 is in top range, or query with filter to confirm sort value | |
| D.1.4 | Docstore updated | `GET /api/indexes/civitai/documents/500000001` | url, width, height present with correct values | |
| D.1.5 | doc_only fields via ops | Verify `url` stored in doc but NOT as a bitmap | No filter field named "url" in stats | |

### D.2 Partial Update (PATCH semantics)

```bash
# Update only reactionCount, leave everything else
curl -s http://localhost:3001/api/indexes/civitai/ops -d '{
  "ops": [{
    "entity_id": 500000001,
    "creates_slot": false,
    "ops": [
      {"op": "set", "field": "reactionCount", "value": 999}
    ]
  }],
  "meta": {"source": "validation", "cursor": 2}
}'
```

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| D.2.1 | Updated field changed | Wait 500ms, query entity doc | reactionCount = 999 | |
| D.2.2 | Untouched fields preserved | Same doc query | nsfwLevel still 8, userId still 12345, url still present | |
| D.2.3 | Bitmap for old value cleared | Query `reactionCount` sort — entity should reflect new value | |

### D.3 Multi-Value Ops

```bash
# Add tags
curl -s http://localhost:3001/api/indexes/civitai/ops -d '{
  "ops": [{
    "entity_id": 500000001,
    "creates_slot": false,
    "ops": [
      {"op": "add", "field": "tagIds", "value": 100},
      {"op": "add", "field": "tagIds", "value": 200},
      {"op": "add", "field": "tagIds", "value": 300}
    ]
  }],
  "meta": {"source": "validation", "cursor": 3}
}'
```

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| D.3.1 | Add reflected | Query `tagIds eq 100` | 500000001 in results | |
| D.3.2 | Multiple adds | Query `tagIds in [100, 200]` | 500000001 in results | |
| D.3.3 | Remove op | Send `{"op":"remove","field":"tagIds","value":100}`, wait, query `tagIds eq 100` | 500000001 NOT in results | |
| D.3.4 | Remaining tags intact | Query `tagIds eq 200` | 500000001 still in results | |
| D.3.5 | Docstore array updated | GET document | tagIds array reflects adds/removes | |

### D.4 Delete

```bash
curl -s http://localhost:3001/api/indexes/civitai/ops -d '{
  "ops": [{
    "entity_id": 500000001,
    "creates_slot": false,
    "ops": [{"op": "delete"}]
  }],
  "meta": {"source": "validation", "cursor": 4}
}'
```

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| D.4.1 | Alive cleared | Wait 500ms, query any filter that previously matched | 500000001 NOT in results | |
| D.4.2 | Filter bits cleared (clean delete) | Query `nsfwLevel eq 8` | 500000001 absent (not just hidden by alive) | |
| D.4.3 | Sort bits cleared | Query `sort=reactionCount desc` with no filters | 500000001 not in any page | |
| D.4.4 | tagIds cleared | Query `tagIds eq 200` | 500000001 absent | |

### D.5 WAL Persistence and Recovery

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| D.5.1 | WAL cursor persisted | Insert 10 ops with cursors 10-19. Kill server (`kill -9`). Restart. | Server resumes from cursor >= 10, no duplicate processing | |
| D.5.2 | Ops replayed on restart | After restart, query for entities created by ops 10-19 | All present in query results | |
| D.5.3 | No duplicate application | Check stats — alive count should not double-count replayed ops | Alive count matches expected | |
| D.5.4 | LIFO dedup | Send duplicate ops in same batch (same entity_id, different values). Last one wins. | Final state matches last op in batch | |

### D.6 Edge Cases

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| D.6.1 | Ops on non-alive slot | Send set op for entity 999999999 (never created) without creates_slot | Op silently dropped, no error | |
| D.6.2 | queryOpSet fan-out | Send `queryOpSet` targeting `postId eq {id}` with a set op — should fan out to all images with that postId | All matching images updated | |
| D.6.3 | queryOpSet large fan-out | Fan-out to 1000+ slots | All slots updated, no timeout | |
| D.6.4 | Empty ops array | POST ops with empty ops array | 200 OK, no crash | |
| D.6.5 | Unknown field in op | Op referencing a field not in config | Graceful error or skip, no panic | |

---

## E. PG Trigger Pipeline

**Prerequisite:** PG tunnel access established (Aidan). BitdexOps table created on replica.

**Reference script:** `tools/e2e-gate3-triggers.mjs`

### E.1 Infrastructure

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| E.1.1 | PG tunnel access | `psql` connection via tunnel | Can query Image table | |
| E.1.2 | BitdexOps table exists | `SELECT count(*) FROM "BitdexOps"` | Table exists, query succeeds | |
| E.1.3 | Trigger SQL generated | `bitdex-sync pg --generate-triggers --config sync-config-civitai.yaml` | SQL output for all 7 trigger tables | |
| E.1.4 | Triggers deployed | `SELECT tgname FROM pg_trigger WHERE tgname LIKE 'bitdex_%'` | All expected triggers present | |

### E.2 Direct Entity Triggers

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| E.2.1 | Image INSERT | Insert test row into Image table. Check BitdexOps. | Row with `creates_slot=true`, all tracked fields as set ops | |
| E.2.2 | Image UPDATE — tracked field | Update `nsfwLevel` on test image. Check BitdexOps. | Two entries: remove (old value) + set (new value) | |
| E.2.3 | Image UPDATE — untracked field | Update a column NOT in track_fields. Check BitdexOps. | No new BitdexOps entry | |
| E.2.4 | Image UPDATE — flags change | Update `flags` column. Check BitdexOps. | Computed fields (hasMeta, onSite, minor, poi) correctly recomputed in ops | |
| E.2.5 | Image DELETE | Delete test image. Check BitdexOps. | Delete op generated | |
| E.2.6 | Tag INSERT | Insert row into TagsOnImageNew. Check BitdexOps. | `add` op for tagIds | |
| E.2.7 | Tag INSERT — disabled | Insert tag with `(attributes >> 10) & 1 = 1`. Check BitdexOps. | No op generated (filtered out) | |
| E.2.8 | Tag DELETE | Delete tag row. Check BitdexOps. | `remove` op for tagIds | |
| E.2.9 | ImageTool INSERT | Insert into ImageTool. | `add` op for toolIds | |
| E.2.10 | ImageTechnique INSERT | Insert into ImageTechnique. | `add` op for techniqueIds | |
| E.2.11 | ImageResourceNew INSERT | Insert resource row. | `add` op for modelVersionIds | |
| E.2.12 | ImageResourceNew INSERT — detected=false | Insert with detected=false. | Both `add modelVersionIds` AND `add modelVersionIdsManual` ops | |
| E.2.13 | ImageResourceNew DELETE | Delete resource row. | `remove` ops for modelVersionIds (and modelVersionIdsManual if was manual) | |

### E.3 Fan-Out Triggers

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| E.3.1 | Post UPDATE — publishedAt | Update Post.publishedAt. Check BitdexOps. | queryOpSet with `postId eq {post_id}`, set op for publishedAt (epoch seconds) | |
| E.3.2 | Post UPDATE — publishedAt null | Set Post.publishedAt to NULL. | queryOpSet with remove op for publishedAt (not set with null) | |
| E.3.3 | Post UPDATE — publishedAt null→value | Set publishedAt from NULL to a timestamp. | queryOpSet with set op | |
| E.3.4 | Post UPDATE — availability | Update Post.availability. | queryOpSet with set op, value is text cast | |
| E.3.5 | ModelVersion UPDATE — baseModel | Update MV.baseModel. Check BitdexOps. | queryOpSet with `modelVersionIds eq {mv_id}`, Checkpoint filter applied | |
| E.3.6 | ModelVersion — non-Checkpoint model | Update MV where Model.type != 'Checkpoint'. | No ops generated (JOIN filter) | |
| E.3.7 | Model UPDATE — poi | Update Model.poi. Check BitdexOps. | MV IDs resolved via subquery, queryOpSet with `modelVersionIds in [mv_ids]` | |

### E.4 Ops Poller Integration

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| E.4.1 | ops_poller reads BitdexOps | Start `bitdex-sync pg` with ops polling. Generate trigger events. | Poller reads rows and POSTs to BitDex /ops endpoint | |
| E.4.2 | Cursor advances | Check `bitdex_cursors` table in PG. | Cursor position advances after successful POST | |
| E.4.3 | BitDex receives ops | After poller delivers, query BitDex for changed entities. | Changes reflected in query results | |
| E.4.4 | At least 100 ops per trigger type | Wait for organic traffic or generate test data. | 100+ ops from each of the 7 trigger types verified | |

---

## F. Config Verification

### F.1 Index Config vs PG Schema

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| F.1.1 | All data_schema fields have PG source | For each of the 30 fields in civitai-index.json data_schema, verify the `source` column exists in the sync config's COPY query or computed_fields | No orphaned fields | |
| F.1.2 | filter_only alignment | Fields with `filter_only: true` in data_schema (modelVersionIdsManual, toolIds, techniqueIds) are NOT in docstore writes | Confirmed in code review | |
| F.1.3 | doc_only alignment | Fields with `doc_only: true` (url, hash, width, height, needsReview, acceptableMinor, index) have NO filter/sort bitmaps | Confirmed no bitmap allocation | |
| F.1.4 | Sort field bit widths | All sort fields have `bits: 32` in config. Verify no values exceed 2^32. | No truncation issues | |
| F.1.5 | ms_to_seconds fields | `sortAtUnix` has `ms_to_seconds: true`. Verify conversion produces seconds (10-digit), not ms (13-digit). | Values in reasonable epoch range (1.6B - 1.8B for 2020-2027) | |
| F.1.6 | Fallback fields | `nsfwLevel` has `fallback: "combinedNsfwLevel"`. Verify fallback logic works when primary source is null. | Falls back correctly | |
| F.1.7 | sortAt fallback | `sortAtUnix` has `fallback: "sortAt"`. If sortAtUnix missing, falls back to sortAt field. | Confirmed | |

### F.2 Sync Config vs PG Tables

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| F.2.1 | COPY query columns exist | For each dump phase, run the COPY query with `LIMIT 1` against PG. | No column-not-found errors | |
| F.2.2 | tags COPY columns | `"tagId", "imageId", "attributes"` exist in TagsOnImageNew | Confirmed | |
| F.2.3 | images COPY columns | `id, url, "nsfwLevel", hash, flags, type, "userId", "blockedFor", scannedAt, createdAt, "postId", width, height` exist in Image | Confirmed | |
| F.2.4 | resources COPY columns | `"imageId", "modelVersionId", detected` exist in ImageResourceNew | Confirmed | |
| F.2.5 | tools COPY columns | `"toolId", "imageId"` exist in ImageTool | Confirmed | |
| F.2.6 | techniques COPY columns | `"techniqueId", "imageId"` exist in ImageTechnique | Confirmed | |
| F.2.7 | posts enrichment columns | `id, publishedAt, availability, "modelVersionId"` exist in Post | Confirmed | |
| F.2.8 | model_versions enrichment columns | `id, "baseModel", "modelId"` exist in ModelVersion | Confirmed | |
| F.2.9 | models enrichment columns | `id, poi, type` exist in Model | Confirmed | |
| F.2.10 | ClickHouse metrics query | `SELECT imageId, reactionCount, commentCount, collectedCount FROM image_metrics` runs | Confirmed | |

### F.3 Trigger SQL vs Sync Config

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| F.3.1 | Image trigger tracks all fields | Generated trigger SQL monitors: nsfwLevel, type, userId, postId, blockedFor, url, hash, width, height, flags | All present | |
| F.3.2 | Image trigger computed fields | Trigger computes: hasMeta, onSite, minor, poi from flags expressions | Expressions match sync config | |
| F.3.3 | TagsOnImageNew trigger | Monitors tagId, applies disabled-tag filter on attributes | Filter matches `(attributes >> 10) & 1 = 0` | |
| F.3.4 | Post fan-out trigger | queryOpSet uses `postId eq {id}`, tracks publishedAt (with epoch extraction) and availability (with text cast) | Matches sync config | |
| F.3.5 | ModelVersion fan-out trigger | queryOpSet uses `modelVersionIds eq {id}`, JOIN on Model, filter Checkpoint | Matches sync config | |
| F.3.6 | Model fan-out trigger | Resolves MV IDs via subquery, queryOpSet uses `modelVersionIds in [{ids}]`, tracks poi | Matches sync config | |
| F.3.7 | Trigger naming convention | All triggers named `bitdex_{table}_{hash8}` | Confirmed | |
| F.3.8 | Stale trigger cleanup | If config changes, old triggers with different hash are DROPped | 3.4 reconciliation logic verified | |

---

## G. Integration Sanity Checks

These verify the full pipeline end-to-end, not individual components.

| # | Check | Method | Pass Criteria | Status |
|---|-------|--------|---------------|--------|
| G.1 | Dump + query + ops roundtrip | Load via dump, query to confirm, modify via ops, re-query | All three stages produce correct results | |
| G.2 | Server restart preserves state | Load data, restart server, run same queries | Identical results before and after restart | |
| G.3 | LCS dictionary persistence | Load data with LCS fields (type, baseModel, availability, blockedFor), restart, query | String queries still work, same dictionary encoding | |
| G.4 | Time bucket correctness | After load, `GET /api/indexes/civitai/time-buckets` | Buckets populated with correct counts for 24h/7d/30d/1y windows | |
| G.5 | Stats endpoint | `GET /api/indexes/civitai/stats` | alive_count, field counts, sort field info all populated | |
| G.6 | Concurrent dump + ops | Start a dump phase while ops are flowing | No deadlock, no data corruption, ops applied after dump completes | |
| G.7 | Loading mode behavior | During dump, verify snapshot not published per-flush (loading mode active) | No Arc clone cascade, single publish on exit | |

---

## H. Production Readiness Cross-Checks

These map to the existing `production-readiness-checklist.md` gates but verify the gaps identified on 2026-03-28.

| # | Check | Gate | Pass Criteria | Status |
|---|-------|------|---------------|--------|
| H.1 | Gate 1 CLEAR with real data | V1.1-V1.9 | All pass against 107M production CSVs, not crafted data | |
| H.2 | Gate 2 CLEAR | V2.1-V2.11 | All pass (reference: e2e-phase2-validation.mjs) | |
| H.3 | Gate 3 CLEAR with real PG | V2.5.1-V2.5.8 | Triggers deployed to PG replica, real ops verified | |
| H.4 | Gate 4 complete | 3.4-3.6 | Boot sequence, trigger reconciliation, config hash detection implemented | |
| H.5 | Gate 5 CLEAR with real PG | All items | bitdex-sync runs against real Postgres, real CSVs loaded | |
| H.6 | existedAt sort field | Config | `existedAt` added to civitai-index.json, `sortAt = GREATEST(existedAt, publishedAt)` computed correctly | |
| H.7 | Justin approval | Process | Justin has personally reviewed and approved the sync-v2 PR | |
| H.8 | Memory budget | Ops | Full load at 107M stays under 28 GB RSS (leaves headroom in 32 GB pod) | |

---

## Appendix: Quick-Reference Commands

### Start Server
```bash
RAYON_NUM_THREADS=28 cargo run --release --features server,pg-sync --bin bitdex-server -- \
  --port 3001 --data-dir ./data --config deploy/configs/civitai-index.json
```

### Run Existing Validation Suites
```bash
# Phase 1 (dump) — requires CSVs in data/load_stage/
node tools/validate-dump-processor.mjs

# Phase 2 (ops) — uses crafted data against test index
BITDEX_URL=http://localhost:3001 node tools/e2e-phase2-validation.mjs

# Gate 3 (triggers) — requires PG tunnel
node tools/e2e-gate3-triggers.mjs

# Gate 5 (integration) — requires PG tunnel + CSVs
node tools/e2e-gate5-integration.mjs
```

### Spot-Check a Document
```bash
# Get alive count, pick a random slot
ALIVE=$(curl -s http://localhost:3001/api/indexes/civitai/stats | jq '.alive_count')
SLOT=$(curl -s http://localhost:3001/api/indexes/civitai/query -d '{"filters":[],"sort":{"field":"id","direction":"desc"},"limit":1}' | jq '.ids[0]')
curl -s http://localhost:3001/api/indexes/civitai/documents/$SLOT | jq .
```

### Verify Filter vs Doc Separation
```bash
# Should have field data:
curl -s http://localhost:3001/api/indexes/civitai/documents/$SLOT | jq 'has("url", "hash", "width", "height")'
# Should NOT have filter_only fields:
curl -s http://localhost:3001/api/indexes/civitai/documents/$SLOT | jq 'has("toolIds", "techniqueIds", "modelVersionIdsManual")'
# Expected: true then false
```

### Count Comparison Template
```bash
# CSV count (no header in COPY output)
CSV_COUNT=$(wc -l < data/load_stage/images.csv)
# BitDex alive count
BDX_COUNT=$(curl -s http://localhost:3001/api/indexes/civitai/stats | jq '.alive_count')
echo "CSV: $CSV_COUNT, BitDex: $BDX_COUNT, Match: $([ $CSV_COUNT -eq $BDX_COUNT ] && echo YES || echo NO)"
```

---

## Checklist Summary

| Section | Total Items | Blocking | Notes |
|---------|------------|----------|-------|
| A. Dump Pipeline | 30 | All | Core data loading correctness |
| B. Document Verification | 19 | All | Docstore field presence and accuracy |
| C. Query Correctness | 27 | All | Filter, sort, pagination |
| D. Ops Pipeline | 25 | All | Steady-state mutation path |
| E. PG Trigger Pipeline | 24 | E.1-E.3 blocking, E.4 nice-to-have at scale | Requires PG tunnel (blocker) |
| F. Config Verification | 18 | All | Config alignment prevents silent bugs |
| G. Integration Sanity | 7 | G.1-G.3 blocking | Full pipeline coherence |
| H. Production Cross-Checks | 8 | All | Gate model compliance |
| **Total** | **158** | | |

**Minimum for production deploy:** All blocking items in A-D, F, G.1-G.3, H.1-H.8 pass. Gate 3 (section E) requires PG tunnel access — if blocked, document as explicit deferral with mitigation plan.
