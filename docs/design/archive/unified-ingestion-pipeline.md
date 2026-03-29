# Unified Ingestion Pipeline

**Status:** Proposed
**Date:** 2026-03-17
**Supersedes:** [declarative-sync.md](declarative-sync.md), [pipeline-architecture.md](pipeline-architecture.md) (partially)

## Executive Summary

Everything is a stream of bit tuples: `(field, value, slot, insert|remove)`. Whether the source is a 63GB CSV, a single JSON document, or a partial PATCH — the output is always the same: set or clear a bit in a bitmap. Load, put, and patch are one pipeline with different input adapters and batching strategies.

```
  CSV file ──→ mmap+rayon adapter ──┐
                                    │
  JSON doc ──→ json adapter ────────┤──→ Bit Tuples ──→ Bitmap Engine ──→ BitmapFs
                                    │    (field,val,    (bulk accum     + DocStore
  PATCH    ──→ diff adapter ────────┘     slot,op)       or coalescer)  + ArcSwap
```

**Three days of work. Net negative lines of code. Eliminates every class of bug we hit today.**

### What we already have (reuse directly)

| Component | File | What it does |
|-----------|------|-------------|
| CSV multi-value adapter | `process_tags_csv`, `process_multi_value_csv`, `process_collection_items_csv` | mmap+rayon → `HashMap<u64, RoaringBitmap>` |
| CSV entity adapter | `process_images_csv` | mmap+rayon → filter maps + sort maps + alive + docstore |
| BitmapFs writers | `save_filter_field_to_disk`, `save_sort_field_to_disk` | Atomic fpack/sort file writes |
| Existence set reload | `reload_existence_set` | ArcSwap update for lazy loading |
| Bulk bitmap sink | `AccumSink` | Collects into HashMap, no diffing |
| Online bitmap sink | `CoalescerSink` | Batches ops, sends to flush thread |
| Schema field config | `data_schema.fields` | source→target, ms_to_seconds, filter_only, doc_only |
| Doc serving | `format_document` | Stored fields → JSON response |

### What we delete (~400 lines)

- Hardcoded `append_int!("publishedAtUnix", ...)` in single_pass (~20 per-field macro calls)
- Manual `published_at_ms / 1000` conversions
- Manual `isPublished` boolean logic
- `json_to_document_with_dicts` duplicate conversion logic
- Standalone `pg-sync load` binary entry point
- Auto-backfill system (already removed)

### What we add (~200 lines)

- `src/field_mapper.rs` — shared field mapping function (~100 lines)
- `POST /load` endpoint handler (~80 lines)
- Load task progress tracking (~20 lines)

---

## The Core Idea

### Everything is a bit tuple

The fundamental operation across all paths:

```rust
// This is ALL the engine does. Everything else is how you produce these.
struct BitTuple {
    field: Arc<str>,    // "nsfwLevel", "tagIds", "collectionIds"
    value: u64,         // the bitmap key
    slot: u32,          // the document/image ID
    op: Op,             // Insert or Remove
}
```

CSV adapter produces millions of these per second via rayon. JSON adapter produces a handful per HTTP request. PATCH adapter produces them by diffing old vs new. The engine doesn't care where they came from.

### Two batching strategies

| Strategy | When | How |
|----------|------|-----|
| **Accumulate** | Bulk load (no existing data) | Collect into `HashMap<u64, RoaringBitmap>`, write to BitmapFs once, reload existence set |
| **Coalesce** | Online mutations (live data) | Send through crossbeam channel, flush thread applies to staging, ArcSwap publish |

The accumulate path is 100x faster because it skips: per-row diffing, channel overhead, snapshot cloning, and the ArcSwap publish cycle. It writes complete bitmaps directly to disk.

### The shared field mapper

One function. All paths call it. This is the fix for every bug we hit today.

```rust
/// Map a raw source field to its target representation.
/// Called by: CSV bulk loader, JSON upsert, PATCH, format_document (reverse).
pub fn map_raw_to_target(
    source_name: &str,
    raw_value: &RawValue,
    schema: &DataSchema,
) -> Option<MappedField> {
    let mapping = schema.field_by_source(source_name)?;

    let value = match mapping.value_type {
        ExistsBoolean => {
            // Non-null source → true, null/missing → false
            FieldValue::Single(Value::Bool(raw_value.is_present()))
        }
        Integer if mapping.should_convert_ms() => {
            // Milliseconds → seconds
            let ms = raw_value.as_i64()?;
            FieldValue::Single(Value::Integer(ms / 1000))
        }
        MappedString => {
            // String → dictionary integer
            let s = raw_value.as_str()?;
            let key = mapping.string_map.get(s)?;
            FieldValue::Single(Value::Integer(*key))
        }
        IntegerArray => {
            // Array of ints → Multi
            let arr = raw_value.as_array()?;
            FieldValue::Multi(arr.iter().filter_map(|v| v.as_i64()).map(Value::Integer).collect())
        }
        // Integer, Boolean, String — pass through
        _ => raw_value.to_field_value()?,
    };

    Some(MappedField {
        target_name: mapping.target.clone(),
        value,
        filter_only: mapping.filter_only,
        doc_only: mapping.doc_only,
    })
}
```

**What this eliminates:**
- Source vs target name bugs (always returns target name)
- ms_to_seconds inconsistency (applied once, here)
- exists_boolean as integer (always bool)
- Duplicate conversion logic across paths

**What `format_document` becomes:**
```rust
// Serving is trivial when ingest is correct
fn format_document(doc: &StoredDoc, schema: &DataSchema) -> Value {
    // Everything stored under target name, already converted.
    // Just read and return.
    let mut fields = Map::new();
    for mapping in &schema.fields {
        if let Some(fv) = doc.fields.get(&mapping.target) {
            fields.insert(mapping.target.clone(), field_value_to_json(fv));
        } else {
            fields.insert(mapping.target.clone(), default_json_for_field(mapping));
        }
    }
    Value::Object(fields)
}
```

No source-name fallback. No ms_to_seconds on serve. No from_source flag. Because ingest got it right.

---

## POST /load Endpoint

### Single field (filter_only)
```
POST /api/indexes/{name}/load
{
  "format": "csv",
  "source": "/data/load_stage/collection_items.csv",
  "field": "collectionIds",
  "columns": ["collectionId", "imageId"]
}
```

### Full reload (all fields)
```
POST /api/indexes/{name}/load
{
  "format": "csv",
  "sources": [
    { "file": "tags.csv", "field": "tagIds", "columns": ["tagId", "imageId"] },
    { "file": "tools.csv", "field": "toolIds", "columns": ["toolId", "imageId"] },
    { "file": "techniques.csv", "field": "techniqueIds", "columns": ["techniqueId", "imageId"] },
    { "file": "collection_items.csv", "field": "collectionIds", "columns": ["collectionId", "imageId"] },
    { "file": "images.csv", "type": "entity" }
  ]
}
```

**Response:** `{ "task_id": 42 }` — async, poll `GET /api/tasks/42` for progress.

### Internal flow

1. Validate config, spawn `tokio::task::spawn_blocking`
2. For each source:
   a. mmap the file
   b. Rayon parallel parse → bit tuples → bitmap accumulator
   c. Write to BitmapFs atomically (fpack files)
   d. Free the accumulator (memory bounded per-field)
3. Reload existence sets for all loaded fields
4. Set completion cursor if provided
5. Mark task complete

---

## How Each Path Uses the Pipeline

### Bulk Load (POST /load)

```
CSV file
  → mmap + rayon split into chunks
  → each thread: parse lines → map_raw_to_target() → insert into local HashMap<u64, RoaringBitmap>
  → merge thread-local maps with bitor_assign
  → save_filter_field_to_disk() (atomic fpack write)
  → reload_existence_set()
```

No diffing. No channel. No flush thread. Direct to disk.

### Upsert (POST /documents/upsert)

```
JSON body
  → for each field: map_raw_to_target()
  → produces Document with target names + converted values
  → diff_document(old_doc, new_doc) → bit tuples (insert/remove)
  → send to coalescer channel
  → flush thread applies to staging → ArcSwap publish
  → write doc to DocStore
```

Uses the diff because there might be existing data to update.

### Patch (PATCH /documents/patch)

```
Partial JSON body
  → for each provided field: map_raw_to_target()
  → diff only provided fields against old doc
  → bit tuples for changed fields only
  → send to coalescer channel
  → flush thread applies + publishes
  → merge into stored doc
```

Same as upsert but only touches provided fields.

### Steady-state sync (outbox poller)

```
PG outbox event
  → fetch full doc from PG
  → row_assembler builds JSON (using map_raw_to_target internally)
  → PATCH to BitDex HTTP endpoint
  → flows through the Patch path above
```

The outbox poller doesn't change — it just calls PATCH. But now PATCH uses the shared mapper.

---

## What Gets Deprecated

| Current | Replaced by |
|---------|-------------|
| `pg-sync load` CLI binary | `POST /load` endpoint |
| `single_pass` hardcoded field logic | Shared `map_raw_to_target()` |
| Standalone loader K8s Job | Not needed — server does it |
| `json_to_document_with_dicts` field conversion | Shared `map_raw_to_target()` |
| `format_document` source-name fallback + from_source flag | Not needed — all stored under target name |
| Auto-backfill (already removed) | `POST /load` on running server |

**What stays unchanged:**
- `pg-sync sync` (outbox + metrics pollers)
- CSV mmap+rayon parsers (reused by load endpoint)
- BitmapFs (atomic fpack/sort writes)
- Flush thread + coalescer (online path)
- ArcSwap snapshot publish (online path)
- DocStore V2 (append-only tuple logs)
- All existing tests + fixtures

---

## Rough Implementation Guide

### Day 1: Shared Field Mapper

**Goal:** One function that all paths call. Delete hardcoded conversion logic.

**Step 1: Create `src/field_mapper.rs`** (~100 lines)

```rust
pub struct MappedField {
    pub target_name: String,
    pub value: FieldValue,
    pub filter_only: bool,
    pub doc_only: bool,
}

pub fn map_raw_to_target(source: &str, raw: &RawValue, schema: &DataSchema) -> Option<MappedField>
pub fn map_csv_pair(value_id: i64, slot_id: i64, field_name: &str) -> BitTuple  // for multi-value CSVs
```

- Handle: Integer, Boolean, String, MappedString, LCS, IntegerArray, ExistsBoolean
- Apply: ms_to_seconds, string_map lookup, null→default
- Always return target name

**Step 2: Refactor `single_pass.rs` `process_images_csv`** (delete ~400 lines)

Current: 20+ hardcoded `append_int!("publishedAtUnix", published_at_ms)` calls.
New: Loop over `schema.fields`, call `map_raw_to_target()` for each, produce bit tuples.

```rust
// Before (per field, hardcoded):
append_int!("publishedAtUnix", published_at_ms);
append_int!("sortAtUnix", sort_at_secs as i64 * 1000);

// After (schema-driven, one loop):
for mapping in &schema.fields {
    if let Some(mapped) = map_raw_to_target(&mapping.source, &raw_fields, schema) {
        if !mapped.doc_only {
            // produce bit tuple for bitmap
        }
        if !mapped.filter_only {
            // append docstore tuple under mapped.target_name
        }
    }
}
```

**Step 3: Refactor `json_to_document_with_dicts`** in `loader.rs`

Replace the per-field `convert_field_with_dict` logic with `map_raw_to_target()`.

**Step 4: Simplify `format_document`** in `server.rs`

Remove `from_source` flag, source-name fallback, ms_to_seconds on serve.
All docs now stored under target names with conversions applied.

**Step 5: Tests**

- Existing fixture tests verify the refactor didn't break parsing
- Add test: `map_raw_to_target` produces identical output for same input regardless of path
- Run full suite: `cargo test --lib --features server,pg-sync`

**Files touched:** New `src/field_mapper.rs`, modify `src/pg_sync/single_pass.rs`, `src/loader.rs`, `src/server.rs`
**Lines added:** ~100. **Lines deleted:** ~400. **Net: -300.**

---

### Day 2: POST /load Endpoint

**Goal:** Load CSV data into a running server via HTTP.

**Step 1: Add endpoint** in `src/server.rs` (~80 lines)

```rust
#[derive(Deserialize)]
struct LoadRequest {
    format: String,                    // "csv"
    source: Option<String>,            // single file path
    sources: Option<Vec<LoadSource>>,  // multi-file
    field: Option<String>,             // for single multi-value field
    columns: Option<Vec<String>>,      // CSV column mapping
}

#[derive(Deserialize)]
struct LoadSource {
    file: String,
    field: Option<String>,             // multi-value field name
    #[serde(rename = "type")]
    source_type: Option<String>,       // "entity" for image rows
    columns: Option<Vec<String>>,
}
```

Route: `POST /api/indexes/{name}/load` (admin, behind auth layer)

**Step 2: Handler spawns blocking task**

```rust
async fn handle_load(...) -> impl IntoResponse {
    // Validate request
    // Spawn blocking task
    let (task_id, progress) = tasks.try_start(TaskType::Load)?;
    tokio::task::spawn_blocking(move || {
        for source in sources {
            match source.field {
                Some(field) => {
                    // Multi-value CSV: reuse process_multi_value_csv / process_collection_items_csv
                    let bitmaps = process_csv(&source.file, &field)?;
                    save_filter_field_to_disk(&bitmap_fs, &field, &bitmaps)?;
                    engine.reload_existence_set(&field)?;
                }
                None => {
                    // Entity CSV: reuse process_images_csv (refactored in Day 1)
                    // This is Phase 3 work
                }
            }
        }
        tasks.set_complete(task_id, ...);
    });
    (StatusCode::ACCEPTED, Json(json!({"task_id": task_id})))
}
```

**Step 3: Generic multi-value CSV processor**

The existing processors (`process_tags_csv`, `process_collection_items_csv`) are nearly identical. Extract a generic version:

```rust
fn process_two_column_csv(
    path: &Path,
    field_name: &str,
) -> Result<HashMap<u64, RoaringBitmap>, String>
```

Takes any 2-column CSV (valueId, slotId), returns bitmap map. All existing multi-value processors delegate to this.

**Step 4: Test with fixture CSVs**

- E2E test: start server, POST /load with collection_items.csv fixture, query collectionIds, verify results
- E2E test: POST /load with tags.csv fixture, query tagIds, verify results

**Files touched:** `src/server.rs` (endpoint), new generic CSV processor
**Lines added:** ~80 endpoint + ~40 generic processor. **Lines deleted:** ~60 (deduplicate processors).

---

### Day 3: Clean Up + Deprecate

**Goal:** Remove the standalone loader, update docs, finalize.

**Step 1: Mark `pg-sync load` as deprecated**

Add deprecation warning:
```rust
Commands::Load => {
    eprintln!("WARNING: pg-sync load is deprecated. Use POST /api/indexes/{name}/load instead.");
    // ... existing code still works
}
```

Don't remove yet — keep as fallback until POST /load is proven in production.

**Step 2: Entity CSV support (Phase 3 start)**

Wire `process_images_csv` (now using shared mapper from Day 1) to the load endpoint.
This requires enrichment maps (posts, resources, models) to also be loaded from CSVs in the same request.

The `sources` array handles this:
```json
{
  "sources": [
    { "file": "posts.csv", "type": "enrichment", "enriches": "images" },
    { "file": "resources.csv", "type": "enrichment", "enriches": "images" },
    { "file": "images.csv", "type": "entity" }
  ]
}
```

Enrichment CSVs loaded into HashMaps first, then entity CSV processed with enrichment available.

**Step 3: Update documentation**

- Update `docs/guide/pg-dump-and-bulk-load.md` — new load endpoint instructions
- Update `docs/guide/api.md` — document POST /load
- Archive `docs/design/declarative-sync.md` — superseded

**Step 4: Run full test suite + fixture integration**

```bash
cargo test --lib --features server,pg-sync  # all unit + fixture tests
cargo test --features pg-sync --test bulk_load_fixture_test  # pipeline integration
node tests/e2e/run-e2e.mjs  # all E2E suites
node tests/e2e/e2e-filter-sync.mjs  # filter-sync + write path
```

---

## After Day 3: What the System Looks Like

```
BitDex Server
├── POST /load          → bulk CSV/NDJSON → BitmapFs (new)
├── POST /upsert        → JSON doc → coalescer → flush (existing)
├── PATCH /patch         → partial JSON → coalescer → flush (existing)
├── POST /filter-sync    → bitmap replace → coalescer (existing)
├── DELETE               → clean delete → coalescer (existing)
└── All share: map_raw_to_target() for field mapping (new)

pg-sync sidecar
├── sync → outbox poller → PATCH to server (existing)
├── sync → metrics poller → PATCH to server (existing)
└── load → DEPRECATED (use POST /load instead)
```

No standalone loader binary. No separate K8s job. No auto-backfill. No three-path divergence. One pipeline, one field mapper, one source of truth.

---

## Open Questions

1. **Enrichment for entity loads:** Should enrichment CSVs (posts, resources, models) be loaded via separate POST /load calls that build in-memory maps? Or as part of a single multi-source request?

2. **Alive bitmap on live bulk load:** For entity loads on a running server, how do new alive bits merge with existing alive state? The accumulate path builds a separate alive bitmap — does it OR into the existing one, or replace it?

3. **Memory budget:** Server process now does bulk loads. Should the load task have a memory limit? Or is the per-field sequential processing (build → write → free) sufficient?

4. **When to fully remove pg-sync load:** After how many successful POST /load cycles in production? One release cycle? One month?
