# BitDex V2 — API Reference

Base URL: `http://<host>:<port>` (default port 3000)

All endpoints accept and return JSON. Content-Type: `application/json`.

---

## Server CLI

```bash
cargo run --release --features server --bin server -- [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--port <N>` | `3000` | HTTP listen port |
| `--data-dir <PATH>` | `./data` | Root directory for index storage (config, docstore, bitmaps) |
| `--rebuild` | off | Rebuild all bitmap indexes from docstore before serving (see below) |

### `--rebuild` — Full Bitmap Rebuild on Boot

Deletes existing bitmap files, rebuilds every filter and sort index from the on-disk docstore using the current config, persists the result via `save_and_unload()`, then starts the HTTP listener.

**Use cases:**
- **Config changes** — added/removed fields, changed field types, updated sort encoding
- **Corruption recovery** — bitmap files damaged or out of sync with docstore
- **Fresh deployment** — docstore populated by pg-sync or NDJSON loader, bitmaps not yet built

**What happens:**
1. Server loads the index config and docstore (normal restore path)
2. Existing bitmaps directory is deleted and recreated
3. `build_all_from_docstore()` reads every docstore shard using packed decode, builds all filter + sort bitmaps via channel-based merge (rayon workers → bounded channel → merge thread)
4. `save_and_unload()` persists bitmaps to disk via zero-copy `fused_cow()`, then unloads them from memory
5. Server starts listening — first queries trigger lazy bitmap loading from the freshly written files

**Performance at 105M records** (Justin's dev machine, NVMe):
- Build phase: 98–120s (~1M docs/s)
- Persist phase: 37–49s
- Total: ~2.5 min, 20–22 GB peak RSS
- Disk footprint: ~8 GB (7.2 GB filter + 866 MB sort + 15 MB system)

See `docs/benchmarks/performance-baseline.md` for full baselines and regression thresholds.

**Example:**
```bash
cargo run --release --features server --bin server -- --rebuild --port 3001 --data-dir ./data
```

### Normal Boot (no `--rebuild`)

Restores config and docstore from `--data-dir`. Bitmaps load lazily from disk on first query per field. Startup completes in <1s at 105M records.

---

## Index Management

### Create Index

```
POST /api/indexes
```

Creates a new index with the given configuration and data schema. Only one index is supported at a time.

**Request body:**

```json
{
  "name": "civitai-images",
  "config": {
    "filter_fields": [
      { "name": "nsfwLevel", "field_type": "single_value" },
      { "name": "tagIds", "field_type": "multi_value" }
    ],
    "sort_fields": [
      { "name": "reactionCount", "source_type": "u32", "bits": 32, "encoding": "identity" }
    ]
  },
  "data_schema": {
    "id_field": "id",
    "fields": [
      { "source": "nsfwLevel", "target": "nsfwLevel", "value_type": "integer" },
      { "source": "tags", "target": "tagIds", "value_type": "integer_array" },
      { "source": "stats.reactionCountAllTime", "target": "reactionCount", "value_type": "integer" },
      { "source": "url", "target": "url", "value_type": "string", "doc_only": true }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Alphanumeric, underscore, or hyphen. Max 64 chars. |
| `config` | object | Engine configuration (filter fields, sort fields, cache, etc.) |
| `data_schema` | object | Maps source JSON fields to engine fields. See [Data Schema](#data-schema). |

**Response:** `201 Created`

```json
{ "name": "civitai-images", "status": "created" }
```

**Errors:**
- `400` — Invalid name or config
- `409` — An index already exists

---

### List Indexes

```
GET /api/indexes
```

**Response:** `200 OK`

```json
{
  "indexes": [
    { "name": "civitai-images", "alive_count": 105300000 }
  ]
}
```

---

### Get Index

```
GET /api/indexes/{name}
```

**Response:** `200 OK`

```json
{
  "name": "civitai-images",
  "config": { ... },
  "data_schema": { ... },
  "stats": {
    "alive_count": 105300000,
    "slot_count": 105300001
  }
}
```

**Errors:** `404` — Index not found

---

### Delete Index

```
DELETE /api/indexes/{name}
```

Deletes the index and removes its directory from disk.

**Response:** `200 OK`

```json
{ "status": "deleted" }
```

**Errors:**
- `404` — Index not found
- `409` — Cannot delete while a task is running

---

## Data Loading

### Load NDJSON

```
POST /api/indexes/{name}/load
```

Loads documents from a newline-delimited JSON file on disk. Runs asynchronously — returns immediately with `202 Accepted` and a task ID. Poll the task system for progress.

**Request body:**

```json
{
  "path": "/data/images-full-v2.ndjson",
  "limit": null,
  "threads": 4,
  "chunk_size": 500000,
  "docstore_batch_size": 100000
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | **required** | Absolute path to NDJSON file on the server's filesystem |
| `limit` | integer \| null | `null` | Max records to load. `null` = load all. |
| `threads` | integer | `cores/2` clamped 4-8 | Worker threads (rayon manages parallelism internally) |
| `chunk_size` | integer | `500000` | Records per processing chunk |
| `docstore_batch_size` | integer | `100000` | Documents per docstore write batch |

**Response:** `202 Accepted`

```json
{ "task_id": 1 }
```

**Errors:**
- `400` — File not found
- `404` — Index not found
- `409` — Another task is already running (returns the active task info)

---

## Querying

### Query

```
POST /api/indexes/{name}/query
```

Execute a filter + sort query and return ordered document IDs.

**Request body:**

```json
{
  "filters": [
    ["Eq", ["nsfwLevel", { "Integer": 1 }]],
    ["In", ["tagIds", [{ "Integer": 42 }, { "Integer": 99 }]]]
  ],
  "sort": {
    "field": "reactionCount",
    "direction": "Desc"
  },
  "limit": 20,
  "cursor": null,
  "offset": null,
  "include_docs": false
}
```

#### Query Parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `filters` | array | `[]` | Array of filter clauses (see [Filter Clauses](#filter-clauses)) |
| `sort` | object \| null | `null` | Sort specification: `{ "field": "...", "direction": "Asc" \| "Desc" }` |
| `limit` | integer | **required** | Max results to return |
| `cursor` | object \| null | `null` | Keyset pagination cursor from a previous response |
| `offset` | integer \| null | `null` | Skip first N results (offset pagination) |
| `include_docs` | bool \| string[] | `false` | Document field selection (see [Field Selection](#field-selection)) |

#### Field Selection

The `include_docs` parameter controls which document fields are returned alongside IDs:

| Value | Behavior |
|-------|----------|
| `false` (default) | No documents returned — IDs only |
| `true` | All document fields returned, with schema defaults for missing fields |
| `["*"]` | Same as `true` |
| `["url", "nsfwLevel"]` | Only the named fields (plus `id`) |

When documents are returned, schema-defined fields that were absent at write time are filled with their type's default value:

| Field Type | Default |
|------------|---------|
| `integer` / `mapped_string` | `0` |
| `boolean` / `exists_boolean` | `false` |
| `string` | `""` |
| `integer_array` | `[]` |

**Response:** `200 OK`

```json
{
  "ids": [948271, 831044, 720193],
  "cursor": { "sort_value": 15842, "slot_id": 720193 },
  "total_matched": 3847291,
  "elapsed_us": 1423
}
```

With `"include_docs": true`:

```json
{
  "ids": [948271, 831044],
  "cursor": { "sort_value": 15842, "slot_id": 831044 },
  "total_matched": 3847291,
  "elapsed_us": 2105,
  "documents": [
    {
      "id": 948271,
      "nsfwLevel": 1,
      "reactionCount": 28401,
      "url": "abc123-guid",
      "tagIds": [42, 99, 301],
      "type": 0,
      "blockedFor": 0
    },
    {
      "id": 831044,
      "nsfwLevel": 1,
      "reactionCount": 15842,
      "url": "def456-guid",
      "tagIds": [42],
      "type": 0,
      "blockedFor": 0
    }
  ]
}
```

With `"include_docs": ["url", "reactionCount"]`:

```json
{
  "ids": [948271],
  "documents": [
    {
      "id": 948271,
      "url": "abc123-guid",
      "reactionCount": 28401
    }
  ]
}
```

**Errors:** `400` — Invalid query, `404` — Index not found

#### Filter Clauses

Filters are JSON arrays with the operator as the first element:

| Operator | Format | Description |
|----------|--------|-------------|
| `Eq` | `["Eq", ["field", value]]` | Field equals value |
| `NotEq` | `["NotEq", ["field", value]]` | Field does not equal value |
| `In` | `["In", ["field", [value, ...]]]` | Field matches any value in list |
| `NotIn` | `["NotIn", ["field", [value, ...]]]` | Field matches none of the values |
| `Gt` | `["Gt", ["field", value]]` | Field greater than value |
| `Lt` | `["Lt", ["field", value]]` | Field less than value |
| `Gte` | `["Gte", ["field", value]]` | Field greater than or equal |
| `Lte` | `["Lte", ["field", value]]` | Field less than or equal |
| `Not` | `["Not", clause]` | Logical negation of a clause |
| `And` | `["And", [clause, ...]]` | All clauses must match |
| `Or` | `["Or", [clause, ...]]` | At least one clause must match |

**Values** are typed:

```json
{ "Integer": 42 }
{ "Float": 3.14 }
{ "Bool": true }
{ "String": "tos" }
```

#### Cursor Pagination

To paginate through results, pass the `cursor` from the previous response:

```json
{
  "filters": [...],
  "sort": { "field": "reactionCount", "direction": "Desc" },
  "limit": 20,
  "cursor": { "sort_value": 15842, "slot_id": 720193 }
}
```

The cursor encodes the last seen sort value and slot ID for efficient keyset pagination. Results resume from exactly where the previous page ended.

#### Offset Pagination

For compatibility with offset-based pagination:

```json
{
  "filters": [...],
  "sort": { "field": "reactionCount", "direction": "Desc" },
  "limit": 20,
  "offset": 40
}
```

When both `cursor` and `offset` are set, `cursor` takes precedence.

---

## Documents

### Get Document

```
POST /api/indexes/{name}/document
```

Retrieve a single document by slot ID.

**Request body:**

```json
{
  "slot_id": 948271,
  "fields": true
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `slot_id` | integer | **required** | The document's slot ID (= Postgres ID) |
| `fields` | bool \| string[] | all fields | Field selection (same format as `include_docs`) |

**Response:** `200 OK`

```json
{
  "id": 948271,
  "nsfwLevel": 1,
  "reactionCount": 28401,
  "url": "abc123-guid",
  "tagIds": [42, 99, 301]
}
```

With `"fields": ["url"]`:

```json
{
  "id": 948271,
  "url": "abc123-guid"
}
```

**Errors:** `404` — Index or document not found

---

### Get Documents (Batch)

```
POST /api/indexes/{name}/documents
```

Retrieve multiple documents by slot ID.

**Request body:**

```json
{
  "slot_ids": [948271, 831044, 720193],
  "fields": ["url", "nsfwLevel"]
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `slot_ids` | integer[] | **required** | Array of slot IDs |
| `fields` | bool \| string[] | all fields | Field selection |

**Response:** `200 OK`

```json
{
  "documents": [
    { "id": 948271, "url": "abc123-guid", "nsfwLevel": 1 },
    { "id": 831044, "url": "def456-guid", "nsfwLevel": 1 },
    { "id": 720193 }
  ]
}
```

Documents that don't exist on disk return just `{ "id": <slot_id> }`.

---

## Mutations

### Upsert Documents

```
POST /api/indexes/{name}/documents/upsert
```

Insert or update documents. Documents are matched by the ID field defined in the data schema.

**Request body:**

```json
{
  "documents": [
    {
      "id": 948271,
      "nsfwLevel": 2,
      "stats": { "reactionCountAllTime": 29000 },
      "tags": [42, 99, 301, 500],
      "url": "abc123-guid"
    }
  ]
}
```

Documents use the **source field names** from the data schema (not target names). The schema's `source` → `target` mapping is applied during upsert.

**Response:** `200 OK`

```json
{ "upserted": 1 }
```

With partial failures:

```json
{ "upserted": 3, "errors": ["doc[4]: missing id field"] }
```

---

### Delete Documents

```
DELETE /api/indexes/{name}/documents
```

Delete documents by slot ID. Performs clean deletes (clears all filter/sort bitmap bits before clearing the alive bit).

**Request body:**

```json
{
  "ids": [948271, 831044]
}
```

**Response:** `200 OK`

```json
{ "deleted": 2 }
```

---

## Operations

### Stats

```
GET /api/indexes/{name}/stats
```

Returns index statistics and unified cache state.

**Response:** `200 OK`

```json
{
  "alive_count": 105300000,
  "slot_count": 105300001,
  "unified_cache_entries": 42,
  "unified_cache_hits": 18394,
  "unified_cache_misses": 291,
  "unified_cache_bytes": 524288,
  "unified_cache_meta_entries": 6,
  "unified_cache_meta_bytes": 180,
  "unified_cache_entry_details": [
    {
      "sort_field": "reactionCount",
      "direction": "Desc",
      "filter_count": 2,
      "cardinality": 8000,
      "capacity": 8000,
      "max_capacity": 16000,
      "has_more": true,
      "min_tracked_value": 142
    }
  ]
}
```

---

### Clear Cache

```
DELETE /api/indexes/{name}/cache
```

Clears all unified cache entries. Cache will rebuild on subsequent queries.

**Response:** `200 OK`

```json
{ "cleared": true }
```

---

### Save Snapshot

```
POST /api/indexes/{name}/snapshot
```

Persist current bitmap state to disk. Blocks until save is complete (synchronous).

**Request body:** None

**Response:** `200 OK`

```json
{ "status": "saved", "elapsed_secs": 37.2 }
```

**Errors:**
- `404` — Index not found
- `500` — Snapshot save failed

---

### Rebuild Fields

```
POST /api/indexes/{name}/rebuild
```

Reconstructs sort and/or filter bitmaps from the on-disk document store. Runs asynchronously — returns a task ID. Uses the same `build_all_from_docstore()` pipeline as the `--rebuild` CLI flag, but supports selective field rebuilds.

**Request body:**

```json
{
  "sort_fields": ["reactionCount"],
  "filter_fields": ["blockedFor"],
  "save_snapshot": true
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sort_fields` | string[] \| null | `null` | Sort fields to rebuild. `null` = skip sorts (unless filter_fields is also null, then rebuild all). |
| `filter_fields` | string[] \| null | `null` | Filter fields to rebuild. `null` = skip filters (unless sort_fields is also null, then rebuild all). |
| `save_snapshot` | boolean | `true` | Save bitmap snapshot to disk after rebuild |

If both `sort_fields` and `filter_fields` are null/omitted, ALL fields are rebuilt.

**Response:** `202 Accepted`

```json
{ "task_id": 2 }
```

**Errors:**
- `400` — Unknown field name
- `404` — Index not found
- `409` — Another task is already running (returns the active task info)

---

### Add Fields

```
POST /api/indexes/{name}/fields
```

Hot-add new filter or sort fields to a running index by scanning the docstore. The index remains queryable during the operation. Returns a task ID immediately.

**Request body:**

```json
{
  "filter_fields": [
    { "name": "type", "field_type": "single_value" }
  ],
  "sort_fields": [
    { "name": "commentCount", "source_type": "u32", "bits": 32, "encoding": "identity" }
  ],
  "save_snapshot": true
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `filter_fields` | object[] | `[]` | New filter field configs to add |
| `sort_fields` | object[] | `[]` | New sort field configs to add |
| `save_snapshot` | boolean | `true` | Persist bitmaps to disk after building |

Field names must exist in the data schema and must not already be indexed. The operation scans all alive docstore shards via rayon fold+reduce, building bitmaps for only the new fields.

**Performance at 105M:** ~48s for a filter field, ~55s for a sort field. I/O-dominated — adding multiple fields in one request costs roughly the same as adding one.

**Response:** `202 Accepted`

```json
{ "task_id": 3 }
```

**Errors:**
- `400` — Field already exists, not in schema, or no fields specified
- `404` — Index not found
- `409` — Another task is already running

---

### Remove Fields

```
DELETE /api/indexes/{name}/fields
```

Remove filter or sort fields from a running index. Removes the in-memory bitmaps immediately. Orphaned bitmap files on disk are left in place (overwritten on next `save_snapshot` or ignored on boot).

**Request body:**

```json
{
  "filter_fields": ["type"],
  "sort_fields": ["commentCount"],
  "save_snapshot": true
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `filter_fields` | string[] | `[]` | Filter fields to remove |
| `sort_fields` | string[] | `[]` | Sort fields to remove |
| `save_snapshot` | boolean | `true` | Persist bitmaps to disk after removal |

Field names must currently exist in the index config. The data schema is NOT modified — raw data remains in the docstore and can be re-indexed later via Add Fields.

**Response:** `202 Accepted`

```json
{ "task_id": 4 }
```

**Errors:**
- `400` — Field not found in config, or no fields specified
- `404` — Index not found
- `409` — Another task is already running

---

## Task System

All long-running operations (load, rebuild, add_fields, remove_fields) return a task ID immediately. At most one task runs at a time per index. Completed and failed tasks are kept in a history ring (max 20 entries).

### List Tasks

```
GET /api/indexes/{name}/tasks
```

Returns the active task (if any) and recent task history for the index.

**Response:** `200 OK`

```json
{
  "active": {
    "id": 3,
    "task_type": "add_fields",
    "status": "running",
    "progress": { "records_processed": 52000000 },
    "elapsed_secs": 24.5,
    "result": null,
    "error": null
  },
  "history": [
    {
      "id": 2,
      "task_type": "rebuild",
      "status": "complete",
      "progress": { "records_processed": 105300000 },
      "elapsed_secs": 98.2,
      "result": "Rebuilt 3 filter fields, 2 sort fields",
      "error": null
    },
    {
      "id": 1,
      "task_type": "load",
      "status": "complete",
      "progress": { "records_processed": 105300000 },
      "elapsed_secs": 320.1,
      "result": "Loaded 105300000 records",
      "error": null
    }
  ]
}
```

---

### Get Task

```
GET /api/tasks/{task_id}
```

Look up a specific task by ID. Checks the active task first, then searches history.

**Response:** `200 OK`

```json
{
  "id": 3,
  "task_type": "add_fields",
  "status": "running",
  "progress": { "records_processed": 52000000 },
  "elapsed_secs": 24.5,
  "result": null,
  "error": null
}
```

**Task types:** `load`, `rebuild`, `add_fields`, `remove_fields`

**Task statuses:** `running`, `saving`, `complete`, `error`

**Errors:** `404` — Task not found

---

## Named Cursors

Named cursors store opaque string values (typically Postgres LSNs or timestamps) that persist across server restarts. Used by pg-sync and other CDC consumers to track replication position.

### List Cursors

```
GET /api/indexes/{name}/cursors
```

**Response:** `200 OK`

```json
{
  "cursors": [
    { "name": "pg-sync", "value": "0/1A3B4C0" },
    { "name": "cdc-checkpoint", "value": "2026-03-13T12:00:00Z" }
  ]
}
```

Returns an empty array if no cursors are set.

**Errors:** `404` — Index not found

---

### Get Cursor

```
GET /api/indexes/{name}/cursors/{cursor_name}
```

**Response:** `200 OK`

```json
{ "name": "pg-sync", "value": "0/1A3B4C0" }
```

**Errors:**
- `404` — Index or cursor not found

---

## Health & Metrics

### Health Check

```
GET /api/health
```

**Response:** `200 OK` — `"ok"`

---

### Prometheus Metrics

```
GET /metrics
```

Returns Prometheus-formatted metrics for scraping. Includes:

- `bitdex_query_total` — Query count by index (counter)
- `bitdex_query_duration_seconds` — Query latency histogram by index
- `bitdex_upsert_total` — Upsert count by index (counter)
- `bitdex_delete_total` — Delete count by index (counter)
- `bitdex_alive_documents` — Current alive document count by index (gauge)
- `bitdex_slot_high_water` — Slot counter high-water mark by index (gauge)
- `bitdex_cache_entries` — Unified cache entry count by index (gauge)
- `bitdex_cache_bytes` — Unified cache memory by index (gauge)
- `bitdex_cache_hits_total` — Cache hit count by index (gauge)
- `bitdex_cache_misses_total` — Cache miss count by index (gauge)
- `bitdex_slot_bitmap_bytes` — Alive bitmap memory by index (gauge)
- `bitdex_filter_bitmap_bytes` — Per-field filter bitmap memory (gauge, labeled by field)
- `bitdex_filter_bitmap_count` — Per-field filter bitmap count (gauge, labeled by field)
- `bitdex_sort_bitmap_bytes` — Per-field sort layer memory (gauge, labeled by field)

---

## Data Schema

The data schema maps source JSON fields to engine fields. Each mapping specifies how to interpret the source value.

```json
{
  "id_field": "id",
  "fields": [
    {
      "source": "nsfwLevel",
      "target": "nsfwLevel",
      "value_type": "integer"
    },
    {
      "source": "blockedFor",
      "target": "blockedFor",
      "value_type": "mapped_string",
      "string_map": { "tos": 1, "moderated": 2, "CSAM": 3, "AiNotVerified": 4 },
      "case_sensitive": false
    },
    {
      "source": "url",
      "target": "url",
      "value_type": "string",
      "doc_only": true
    }
  ]
}
```

### Field Mapping Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source` | string | **required** | Source field name in the raw JSON document |
| `target` | string | **required** | Target field name in the engine |
| `value_type` | string | **required** | One of: `integer`, `boolean`, `string`, `mapped_string`, `integer_array`, `exists_boolean` |
| `fallback` | string \| null | `null` | Fallback source field if primary is missing |
| `string_map` | object \| null | `null` | For `mapped_string`: maps string values to integer IDs |
| `doc_only` | boolean | `false` | If true, stored in docstore only (not bitmap-indexed) |
| `truncate_u32` | boolean | `false` | Cast value to u32 before storing (for timestamps that exceed u32::MAX) |
| `case_sensitive` | boolean | `false` | For `mapped_string`: whether matching is case-sensitive |

### Value Types

| Type | Storage | Description |
|------|---------|-------------|
| `integer` | Bitmap-indexed | Numeric value → filter/sort bitmaps |
| `boolean` | Bitmap-indexed | Boolean → one bitmap per boolean value |
| `string` | Docstore only | String stored for retrieval, not indexed |
| `mapped_string` | Bitmap-indexed | String mapped to integer via `string_map` |
| `integer_array` | Bitmap-indexed | Array of integers → one bitmap per distinct value |
| `exists_boolean` | Bitmap-indexed | `true` if source field exists and is non-null, `false` otherwise |
