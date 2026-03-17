# Unified Ingestion Pipeline

**Status:** Proposed
**Date:** 2026-03-17
**Supersedes:** [declarative-sync.md](declarative-sync.md), [pipeline-architecture.md](pipeline-architecture.md) (partially)

## Executive Summary

One pipeline, three entry points. All data flows through the same engine internals — load, put, and patch differ only in their input adapter and optimization traits, not in their bitmap-building or storage logic.

```
                         ┌─────────────────────────────────┐
  POST /load             │                                 │
  (CSV, NDJSON, SQL)  ──→│   Input Adapter                 │
                         │   (parse + batch)               │
  POST /documents/upsert │                                 │──→ BitmapFs
  (JSON document)     ──→│   Ingestion Core                │──→ DocStore
                         │   (field mapping + bitmap ops)  │──→ ArcSwap
  PATCH /documents/patch │                                 │
  (partial JSON)      ──→│   Storage Writer                │
                         │   (bulk or incremental)         │
                         └─────────────────────────────────┘
```

**What this replaces:**
- The standalone `pg-sync load` binary (bulk CSV loader)
- The `single_pass` module running as a separate K8s job
- The auto-backfill system in pg-sync sidecar
- Three separate code paths that independently decide field names, conversions, and storage format

**What we reuse (already built):**
- `process_tags_csv`, `process_multi_value_csv`, `process_collection_items_csv` — CSV adapters with mmap+rayon
- `save_filter_field_to_disk`, `save_sort_field_to_disk` — atomic BitmapFs writers
- `reload_existence_set` — live signal for lazy loading to pick up new data
- `Ingester<B: BitmapSink>` trait — routes mutations to bitmap sink + doc sink
- `AccumSink` — bulk bitmap accumulator (skip diffing, assume fresh inserts)
- `CoalescerSink` — online mutation batching (diff old vs new, send to flush thread)
- `format_document` — doc serving with source/target mapping + ms_to_seconds
- `json_to_document_with_dicts` — JSON→Document with schema field mapping
- Field mapping config (`data_schema.fields`) — source→target, ms_to_seconds, filter_only, doc_only

**What's new:**
- `POST /load` endpoint with format/source config
- Input adapter trait (CSV, NDJSON, future: SQL)
- Bulk write mode: build bitmap accumulator → atomic BitmapFs write → existence set reload
- Unified field mapping: one function that all paths call for name resolution + type conversion

---

## The Problem

We have three ingestion paths that independently handle field names, type conversions, and storage:

| Path | Entry | Field naming | Conversion | Storage |
|------|-------|-------------|------------|---------|
| single_pass (loader) | CLI binary | Hardcoded source names | Manual per-field | Direct to BitmapFs |
| put/upsert (HTTP) | POST endpoint | Schema-driven target names | json_to_document_with_dicts | Mutation channel → flush |
| patch (HTTP) | PATCH endpoint | Schema-driven target names | diff_document_partial | Mutation channel → flush |

This caused:
- `publishedAt=0` — loader stored under source name, serving looked up target name
- `isPublished` as integer — loader didn't apply exists_boolean conversion
- double-divide bug — ms_to_seconds applied on both ingest and serve
- INT4/i64 type mismatch — loader used wrong Rust types for PG columns
- OOM crash loop — backfill in wrong process with wrong resource limits

Root cause: three paths, no shared code for the critical field mapping step.

---

## Design: One Pipeline, Three Modes

### The Pipeline

Every ingestion — whether bulk load, single document, or partial update — flows through:

1. **Input Adapter** — parses the source format into typed field values
2. **Field Mapper** — resolves source→target names, applies ms_to_seconds, exists_boolean, etc.
3. **Bitmap Builder** — inserts into filter/sort bitmaps (bulk or incremental)
4. **Storage Writer** — writes to BitmapFs + DocStore (bulk or incremental)
5. **Publish** — makes new data visible (ArcSwap for incremental, existence set reload for bulk)

### Three Modes

| Mode | When | Adapter | Bitmap Builder | Writer | Publish |
|------|------|---------|---------------|--------|---------|
| **Bulk** | POST /load | CSV/NDJSON mmap+rayon | AccumSink (no diff) | Direct BitmapFs write | Existence set reload |
| **Upsert** | POST /upsert | JSON document | CoalescerSink (diff old vs new) | Mutation channel → flush | ArcSwap publish |
| **Patch** | PATCH /patch | Partial JSON | CoalescerSink (partial diff) | Mutation channel → flush | ArcSwap publish |

The critical difference: **bulk mode skips diffing and writes directly to disk**. It assumes fresh inserts with no existing data for the field. This is 100x faster than streaming through the mutation channel.

### The Shared Field Mapper

The single function that all three paths call:

```rust
fn map_field(
    source_name: &str,
    raw_value: &serde_json::Value,
    schema: &DataSchema,
) -> Option<(String, FieldValue)> {
    let mapping = schema.fields.iter().find(|m| m.source == source_name)?;

    // Apply type conversion
    let value = match mapping.value_type {
        ExistsBoolean => FieldValue::Single(Value::Bool(!raw_value.is_null())),
        Integer if mapping.should_convert_ms() => {
            let ms = raw_value.as_i64()?;
            FieldValue::Single(Value::Integer(ms / 1000))
        }
        // ... other types
    };

    // Return under TARGET name
    Some((mapping.target.clone(), value))
}
```

This eliminates the divergence. Loader, upsert, and patch all produce identical field names and values because they call the same function.

---

## POST /load Endpoint

```
POST /api/indexes/{name}/load
Content-Type: application/json

{
  "format": "csv",           // "csv" | "ndjson"
  "source": "/data/load_stage/collection_items.csv",
  "field": "collectionIds",  // optional: specific field (for filter_only)
  "columns": ["collectionId", "imageId"]  // optional: CSV column mapping
}
```

**Response:** Task ID (async, like hot-add fields)

```json
{ "task_id": 42 }
```

**Progress:** `GET /api/tasks/42` returns rows processed, ETA, phase.

### How it works internally

1. Endpoint validates config, spawns blocking task
2. Task mmaps the file, determines adapter from format
3. Adapter produces `HashMap<u64, RoaringBitmap>` for filter fields (or sort layer maps for sort fields)
4. Engine writes to BitmapFs atomically
5. Engine reloads existence set
6. Task completes, sets cursor if provided

### For full reloads (all fields)

```
POST /api/indexes/{name}/load
{
  "format": "csv",
  "sources": [
    { "file": "tags.csv", "field": "tagIds", "columns": ["tagId", "imageId"] },
    { "file": "tools.csv", "field": "toolIds", "columns": ["toolId", "imageId"] },
    { "file": "collection_items.csv", "field": "collectionIds", "columns": ["collectionId", "imageId"] },
    { "file": "images.csv", "type": "entity", "columns": ["id", "url", "nsfwLevel", ...] }
  ]
}
```

Processes sequentially, largest first. Each field's bitmaps are written and freed before the next starts (memory-bounded).

---

## What Gets Deprecated

| Current | Replaced by | Status |
|---------|-------------|--------|
| `pg-sync load` CLI binary | `POST /load` on running server | Deprecated |
| `single_pass::run_single_pass_v2()` | Load endpoint calling same CSV processors | Code reused, entry point changes |
| `pg-sync backfill` subcommand | Already removed | Done |
| Auto-backfill on sync startup | Already removed | Done |
| Three separate field mapping paths | Shared `map_field()` function | New |
| Standalone loader K8s Job | Not needed — server does it | Deprecated |

**What stays:**
- `pg-sync sync` (outbox poller + metrics poller) — steady-state changes
- CSV adapters (mmap+rayon parsers) — reused by load endpoint
- BitmapFs atomic writes — reused
- All existing tests + fixtures

---

## Implementation Plan

### Phase 1: Shared field mapper (prevents future bugs)

Extract `map_field()` from the three paths. All paths call it. Test with fixture data.

**Files:** New `src/field_mapper.rs`, changes to `src/loader.rs`, `src/pg_sync/single_pass.rs`, `src/pg_sync/row_assembler.rs`
**Effort:** 1 day
**Risk:** Low — refactor, no behavior change

### Phase 2: POST /load endpoint for filter_only fields

Start simple: CSV → multi_value filter bitmap → BitmapFs → existence reload.
Handles collectionIds, tagIds, toolIds, techniqueIds.

**Files:** `src/server.rs` (endpoint), reuses existing CSV processors
**Effort:** 1 day
**Risk:** Low — wraps existing proven code

### Phase 3: POST /load for entity fields (images)

Harder: entity CSV → scalar filter + sort + alive + docstore.
Needs enrichment joins (posts, resources, models) which currently live in single_pass.

**Files:** `src/server.rs`, new entity adapter
**Effort:** 2 days
**Risk:** Medium — enrichment logic is complex

### Phase 4: Deprecate pg-sync load

Once POST /load handles all field types, the standalone loader binary is unnecessary.
pg-sync becomes sync-only (outbox + metrics pollers).

**Effort:** Config changes only
**Risk:** Low

---

## Open Questions

1. **Enrichment data for entity loads:** Images need Post/Resource/Model enrichment. Currently these are loaded as HashMaps from CSVs. Should the load endpoint accept multiple files in one request, or should enrichment be pre-joined?

2. **Alive bitmap on bulk load:** Bulk filter_only loads don't touch alive. But entity loads need to set alive bits. How does this merge with existing alive state on a live server?

3. **Memory budget:** The load endpoint runs inside the server process. Should it have a configurable memory limit? Or rely on the task system's backpressure?

4. **NDJSON adapter:** The current NDJSON loader (load_ndjson in server.rs) streams through put(). Should POST /load with NDJSON use the same path, or the bulk AccumSink path?
