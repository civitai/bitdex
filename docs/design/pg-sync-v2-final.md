---
status: ACTIVE
updated: 2026-03-28
---

# BitDex Sync V2 — Final Design

> Distilled from the [working design doc](pg-sync-v2.md) (Justin + Adam, 2026-03-25).

## Problem

The V1 outbox poller is 80M rows behind and can never catch up. Each cycle polls 5,000 rows from `BitdexOutbox`, then runs 5 enrichment queries per batch (images + tags + tools + techniques + resources) to assemble full JSON documents before PATCHing to BitDex. At ~2,500 changes/s with enrichment as the bottleneck, steady-state write volume exceeds processing capacity.

## Solution

Replace the "notify then re-fetch" pattern with **self-contained ops**. PG triggers encode the exact delta (old value, new value, field name) directly into a JSONB ops array. No enrichment queries, no full-document assembly. BitDex applies ops as direct bitmap mutations.

---

## Architecture

```
PG trigger fires
  → INSERT one row into BitdexOps (entity_id, JSONB ops array)
  → pg-sync polls BitdexOps, deduplicates, POSTs batch to BitDex
  → BitDex /ops endpoint appends to local WAL file, returns 200
  → WAL reader thread processes ops → bitmap mutations via coalescer
```

### BitdexOps Table

```sql
CREATE TABLE IF NOT EXISTS "BitdexOps" (
    id          BIGSERIAL PRIMARY KEY,
    entity_id   BIGINT NOT NULL,
    ops         JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_bitdex_ops_id ON "BitdexOps" (id);
```

Each row contains a JSONB array of ops. Triggers include both old and new values so BitDex can update bitmaps without reading the docstore.

### Op Types

| Op | Example | Bitmap Action |
|----|---------|---------------|
| `set` | `{"op":"set","field":"nsfwLevel","value":16}` | Set bit in value bitmap |
| `remove` | `{"op":"remove","field":"nsfwLevel","value":8}` | Clear bit from value bitmap |
| `add` | `{"op":"add","field":"tagIds","value":42}` | Set bit in multi-value bitmap |
| `delete` | `{"op":"delete"}` | Clear all filter/sort bits + alive bit |
| `queryOpSet` | See [Fan-Out](#fan-out-via-queryopset) | Query-resolved bulk bitmap ops |

**No `full` op type.** INSERTs emit individual `set` ops for each field (all additive, no `remove` since there's no prior state). One format for everything.

### Op Examples

**Image UPDATE** (nsfwLevel 8→16, type stays same):
```json
[
  {"op": "remove", "field": "nsfwLevel", "value": 8},
  {"op": "set", "field": "nsfwLevel", "value": 16}
]
```

**Image INSERT** (new image):
```json
[
  {"op": "set", "field": "nsfwLevel", "value": 1},
  {"op": "set", "field": "type", "value": "image"},
  {"op": "set", "field": "userId", "value": 12345},
  {"op": "set", "field": "sortAt", "value": 1711234567}
]
```

**Tag added:**
```json
[{"op": "add", "field": "tagIds", "value": 42}]
```

**Image deleted:**
```json
[{"op": "delete"}]
```

---

## Fan-Out via queryOpSet

Fan-out tables (ModelVersion, Post, Model) don't produce per-image ops in the trigger. Instead, they emit a single `queryOpSet` op that tells BitDex to resolve affected slots from its own bitmaps.

**ModelVersion baseModel change:**
```json
[{"op": "queryOpSet", "query": "modelVersionIds eq 456", "ops": [
  {"op": "remove", "field": "baseModel", "value": "SD 1.5"},
  {"op": "set", "field": "baseModel", "value": "SDXL"}
]}]
```

BitDex looks up the `modelVersionIds=456` bitmap, gets all affected slots, applies two bulk bitmap operations (`andnot` old + `or` new). A 15M-image fan-out completes in microseconds — no per-image ops, no PG queries.

**Model POI change** (needs MV ids from PG first):
```json
[{"op": "queryOpSet", "query": "modelVersionIds in [101, 102, 103]", "ops": [
  {"op": "set", "field": "poi", "value": true}
]}]
```

The trigger uses `jsonb_agg` to collect MV ids: `SELECT jsonb_agg(id) FROM ModelVersion WHERE modelId = NEW.id`. BitDex ORs the MV bitmaps together, then applies the ops.

**Post publishedAt change:**
```json
[{"op": "queryOpSet", "query": "postId eq 789", "ops": [
  {"op": "remove", "field": "publishedAt", "value": 1711000000},
  {"op": "set", "field": "publishedAt", "value": 1711234567}
]}]
```

### Fan-Out Scale (measured 2026-03-25)

| Metric | Value |
|--------|-------|
| ImageResourceNew rows | ~375M |
| Top ModelVersion (290640) | ~15.1M images |
| Top 5 ModelVersions | 18.6% of all rows |
| p50 images/MV | 1 |
| p90 images/MV | 5 |
| p99 images/MV | 53 |

The distribution is extremely heavy-tailed. 99% of fan-outs are trivial. The queryOpSet approach handles even the 15M-image worst case as two bitmap operations.

---

## Trigger Configuration (YAML)

pg-sync generates trigger SQL from a declarative YAML config. Two table types:

### Direct Tables (slot = PG column)

```yaml
sync_sources:
  - table: Image
    slot_field: id
    track_fields: [nsfwLevel, type, userId, postId, minor, poi, hideMeta, meta, blockedFor]
    on_delete: delete_slot

  - table: TagsOnImageNew
    slot_field: imageId
    field: tagIds
    value_field: tagId

  - table: ImageTool
    slot_field: imageId
    field: toolIds
    value_field: toolId

  - table: ImageTechnique
    slot_field: imageId
    field: techniqueIds
    value_field: techniqueId

  - table: CollectionItem
    slot_field: imageId
    field: collectionIds
    value_field: collectionId
    filter: "status = 'ACCEPTED' AND \"imageId\" IS NOT NULL"
```

- `slot_field`: PG column that maps to the BitDex slot ID
- `track_fields`: Scalar columns — trigger emits `remove`/`set` pairs using `IS DISTINCT FROM`
- `field` + `value_field`: Multi-value join tables — INSERT = `add`, DELETE = `remove`
- `on_delete`: `delete_slot` emits a `{"op":"delete"}` op

### Fan-Out Tables (slots resolved by BitDex query)

```yaml
  - table: ModelVersion
    query: "modelVersionIds eq {id}"
    track_fields: [baseModel]

  - table: Post
    query: "postId eq {id}"
    track_fields: [publishedAt, availability]

  - table: Model
    query: "modelVersionIds in {modelVersionIds}"
    query_source: "SELECT jsonb_agg(id) as \"modelVersionIds\" FROM \"ModelVersion\" WHERE \"modelId\" = {id}"
    track_fields: [poi]
```

- `query`: BitDex query template. `{column}` placeholders are substituted from `NEW` columns.
- `query_source`: Optional PG subquery for values not on the triggering table. Returns named columns that feed into `query` placeholders.
- No `slot_field` — slots come from the BitDex query result.

### Trigger Reconciliation

Trigger naming: `bitdex_{table}_{hash8}` where `hash8` is the first 8 chars of SHA256 of the function body. On startup, pg-sync:

1. Generates trigger SQL from config
2. Queries `pg_trigger WHERE tgname LIKE 'bitdex_%'`
3. Hash matches → skip. Hash differs → `CREATE OR REPLACE`. Table not in config → `DROP TRIGGER`.

Config is the source of truth. pg-sync reconciles PG state to match.

---

## WAL-Backed Ops Endpoint

### Ingestion

`POST /api/indexes/{name}/ops` receives ops from pg-sync, appends to a local WAL file, returns 200. Zero processing on the HTTP path — just fsync and acknowledge.

```json
{
  "ops": [
    {"entity_id": 123, "ops": [{"op": "add", "field": "tagIds", "value": 42}]},
    {"entity_id": 456, "ops": [{"op": "set", "field": "nsfwLevel", "value": 16}]}
  ],
  "meta": {
    "source": "pg-sync-default",
    "cursor": 420000000,
    "max_id": 500000000,
    "lag_rows": 80000000
  }
}
```

No cursor management — pg-sync owns its cursor in PG (`bitdex_cursors` table). The `meta` field carries lag metrics for Prometheus exposition.

### WAL Processing

A dedicated reader thread tails the WAL file, reads batches, deduplicates, and submits mutations to the coalescer.

- Append-only files, one per generation: `ops_000001.wal`, `ops_000002.wal`, ...
- Reader maintains a persisted byte-offset cursor
- Size-based rotation (e.g., 100MB), old generations deleted after processing
- Format: `[4-byte len][entity_id: i64][ops: JSONB bytes][CRC32]` — same pattern as ShardStore/BucketDiffLog
- Crash recovery: resume from persisted cursor in current generation

### Op Deduplication

Two-layer dedup using a shared `dedup_ops()` helper:

1. **pg-sync side**: LIFO dedup per `(entity_id, field)` + add/remove cancellation. Reduces batch before sending.
2. **WAL reader side**: Same dedup on WAL batch. Catches cross-poll duplicates.

`full` ops are decomposed into individual `set` ops by pg-sync before dedup — `full` is not a special case in the processing pipeline.

BitDex skips ops for fields not in its index config. Stale triggers that emit ops for removed fields are harmless.

---

## Observability

### Prometheus Metrics

Unified `bitdex_sync_*` namespace with `source` label:

```
bitdex_sync_cursor_position{source="pg-sync-default"} 420000000
bitdex_sync_max_id{source="pg-sync-default"} 500000000
bitdex_sync_lag_rows{source="pg-sync-default"} 80000000
bitdex_sync_cycle_duration_seconds{source="pg-sync-default"} 0.05
bitdex_sync_cycle_rows{source="pg-sync-default"} 4850
bitdex_sync_wal_pending_bytes 1048576
bitdex_sync_wal_generation 3
```

### Lag Endpoint

`GET /api/internal/sync-lag` — returns latest `meta` from each sync source.

Metrics are bundled with the ops payload — no separate reporting call.

---

## Deployment

### Binary

Rename `bitdex-pg-sync` → `bitdex-sync` with subcommands:
- `bitdex-sync pg --config sync.toml` — PG ops poller
- `bitdex-sync ch --config sync.toml` — ClickHouse metrics poller
- `bitdex-sync all --config sync.toml` — both (default for K8s sidecar)

Single sidecar container, concurrent tokio tasks.

### ClickHouse

Stays separate and simple. Polls CH for aggregate counts (reactionCount, commentCount, collectedCount), pushes to BitDex ops endpoint. Not config-driven — the CH query is domain-specific.

### Migration Plan

1. Build V2: BitdexOps table, YAML-driven triggers, ops poller, WAL endpoint, queryOpSet, dump pipeline
2. Boot pod — pg-sync auto-detects empty BitDex, runs table dumps, transitions to steady-state
3. Done. No manual intervention. V1 code stays in repo, unused.

No incremental migration, no shadow mode, no V1 fixes. No manual pod teardown/reload dance.

---

## Unified Load Pipeline

### Responsibility Split

**pg-sync (sidecar)** is a thin data mover:
- `COPY FROM` PG → write CSV to shared volume
- Signal BitDex that a CSV is ready (`POST /dumps/{name}/loaded`)
- Poll BitdexOps outbox → `POST /ops` batches (steady-state)
- Manage cursor in PG (`bitdex_cursors` table)

**BitDex (server)** owns all processing:
- On dump signal: read CSV → parse → ops → AccumSink → bitmap accumulation (direct path, ~367K images/s)
- On `/ops` POST: append to WAL → WAL reader → CoalescerSink → coalescer channel (steady-state)
- YAML sync config awareness: field mapping, value conversion, bit decomposition
- All indexing logic: BitmapSink trait, FieldMeta, value_to_bitmap_key, value_to_sort_u32

pg-sync never generates ops, never touches bitmaps, never writes WAL. The sync config (`sync.yaml`) is read by both: pg-sync uses it for `COPY` column selection and trigger generation, BitDex uses it for CSV→ops field mapping.

### Boot Sequence

```
K8s starts pod (BitDex server + bitdex-sync sidecar)
  → bitdex-sync waits for BitDex health check
  → Capture max(BitdexOps.id) as pre_dump_cursor
  → GET /api/indexes/{name}/dumps — check dump history
  → For each sync_source not yet dumped:
      1. PUT /api/indexes/{name}/dumps — register dump
      2. COPY table from PG → write CSV to shared volume
      3. POST /api/indexes/{name}/dumps/{name}/loaded — "CSV is ready"
      4. BitDex reads CSV directly, parses → AccumSink → bitmaps
      5. BitDex saves bitmaps to ShardStore, unloads from memory
  → Seed cursor at pre_dump_cursor (not current max — catches dump-window ops)
  → Transition to steady-state ops polling
  → K8s readiness probe flips to 200, traffic starts routing
```

No manual intervention. No WAL for dumps. No serialization overhead. Just boot and it works.

### Dump Endpoints

```
GET  /api/indexes/{name}/dumps                    — list dump history
PUT  /api/indexes/{name}/dumps                    — register new dump → task ID
POST /api/indexes/{name}/dumps/{name}/loaded      — signal dump file complete
DELETE /api/indexes/{name}/dumps/{name}            — remove from history
DELETE /api/indexes/{name}/dumps                   — clear all history
GET  /api/tasks/{task_id}                          — poll dump processing status (existing)
```

### Dump Identity and Change Detection

Dump names include a config hash: `Image-a1b2c3d4`. pg-sync constructs the name from the table name + hash of that table's YAML config. If the config changes (add a field to `track_fields`), the hash changes, the name doesn't match existing dumps, and pg-sync auto-re-dumps. BitDex treats dump names as opaque strings.

### Table Ordering

No JOINs on large tables. Each table dumps flat.

1. **Image** — flat COPY. Produces `existedAt` via `GREATEST(scannedAt, createdAt)` expression in `track_fields`.
2. **TagsOnImageNew, ImageTool, ImageTechnique, CollectionItem, ImageResourceNew** — flat COPYs, can run in parallel.
3. **Post** — flat COPY (id, publishedAt, availability). Depends on Image being loaded first. Uses `queryOpSet "postId eq {id}"` to set fields on image slots.
4. **ModelVersion** — flat COPY (small table, <1M rows, JOINs fine). Sets baseModel via `queryOpSet`.
5. **ClickHouse metrics** — separate dump via ch-sync.

### Dump Processing Mode

Dump processing bypasses the WAL, coalescer, and flush thread entirely. BitDex reads the CSV directly and processes via `AccumSink` → `BitmapAccum` → `apply_accum()`:

1. CSV rows parsed in-process (`parse_image_row`, `parse_tag_row`, etc.)
2. Each row → ops → `BitmapSink::filter_insert()` / `sort_set()` / `alive_insert()`
3. `AccumSink` inserts directly into `BitmapAccum` (HashMap-backed bitmap accumulator)
4. After all rows: `engine.apply_accum(&accum)` merges bitmaps into staging via OR
5. Save bitmaps to ShardStore, unload from memory
6. Lazy load on first query (existing `ensure_fields_loaded()` path)

This matches the single-pass loader's throughput: **367K images/s at 1M scale** (vs 345K/s single-pass baseline). No serialization, no WAL I/O, no channel overhead.

The `creates_slot` flag on `EntityOps` controls alive bit management:
- Image table CSVs: `creates_slot: true` → sets alive bit
- Join table CSVs (tags, tools): `creates_slot: false` → only adds filter bitmaps

Peak memory: one table's bitmaps at a time. K8s readiness probe returns 503 during dumps (health probe stays 200). Traffic routes only after all dumps complete.

### Prerequisite: Computed Sort Fields

`sortAt = GREATEST(existedAt, publishedAt)` requires BitDex to compute sort values from multiple source fields. `existedAt` comes from Image dumps, `publishedAt` comes from Post dumps — they arrive at different times. BitDex must recompute `sortAt` whenever either source changes.

This is a separate feature tracked in [computed-sort-fields.md](computed-sort-fields.md).

---

## Throughput

| | V1 | V2 (measured) |
|---|---|---|
| Enrichment queries | 5 per batch | 0 |
| Dump throughput (images) | ~70K/s (single-pass) | **367K/s** (direct AccumSink) |
| Dump throughput (tags) | — | **2.6M/s** (direct AccumSink) |
| Steady-state throughput | ~2,500 changes/s | 2,700 ops/s (CoalescerSink) |
| Fan-out cost (15M images) | 15M enrichment queries | 2 bitmap ops |
| WAL-backed dump (if needed) | — | 41K ops/s |

Dump mode at 367K images/s processes 107M images in ~4.9 minutes (image table only).
Steady-state 2,700 ops/s provides 1.1x headroom over peak traffic (~2,500 changes/s).
The WAL path exists for steady-state durability; dumps skip it entirely for throughput.

---

## Design Review Findings (2026-03-25)

Architectural review identified 17 issues. Resolutions agreed with Justin:

### Cursor Gap (Critical — C1)

PG triggers fire into `BitdexOps` while dumps run. If we seed the cursor at `max(BitdexOps.id)` AFTER dumps, ops generated during the dump window are skipped.

**Resolution:** Capture `max(BitdexOps.id)` BEFORE starting dumps. Seed cursor at that pre-dump value. pg-sync re-processes some overlapping ops (idempotent — set/remove are self-correcting). Updated boot sequence:

```
→ Capture max(BitdexOps.id) as pre_dump_cursor
→ Run dumps...
→ Seed cursor at pre_dump_cursor (not current max)
→ Start steady-state polling — catches all ops from dump window
```

### queryOpSet Race (Critical — C2)

Between bitmap lookup and op application, concurrent mutations could change the resolved slot set. A new image gaining MV 456 during a baseModel cascade could be missed.

**Resolution:** Snapshot-level isolation is acceptable. The next steady-state trigger on the missed image corrects the state. The consistency window is bounded by the poll interval (~2s). Document this as eventual consistency, not serializability.

### Delete Ops + Docstore Read (High — H1)

Delete ops carry no old values, so BitDex must read the docstore to know which bitmaps to clear (clean delete principle).

**Resolution:** Deletes are infrequent — docstore read is acceptable for this case. This is the one op type that requires a docstore read. Doc cache makes it <1μs in the common case. The trigger can't easily emit all field values from `OLD` because multi-value fields (tags, tools) come from join tables, not the Image row.

### WAL Partial Records (High — H3)

Crash mid-write leaves truncated WAL record.

**Resolution:** `POST /ops` returns 200 only after all records are written and fsynced. If crash happens before response, pg-sync doesn't advance its cursor and resends the batch. LIFO dedup on the WAL reader handles re-delivered ops. For dump WAL files, same approach: pg-sync only calls `/loaded` after the full file is written.

### Alive Bit Management (Medium — M1)

No op type explicitly sets the alive bit for new slots.

**Resolution:** The Image table config gets a new property: `sets_alive: true`. Only the table marked `sets_alive` triggers alive bit setting on first `set` op for a non-alive slot. This prevents tags/tools from accidentally creating alive entries for non-existent images. Other tables' ops on non-alive slots are silently dropped.

```yaml
- table: Image
  slot_field: id
  sets_alive: true  # only this table can create new alive slots
  track_fields: [...]
```

### Dump Ordering Dependency (Medium — M4)

ImageResourceNew must complete before ModelVersion dump starts (MV queryOpSet needs `modelVersionIds` bitmaps).

**Resolution:** Explicit dump phases:
1. Image
2. ImageResourceNew + tags + tools + techniques + collections (parallel)
3. Post + ModelVersion (parallel, both depend on step 2)
4. ClickHouse metrics

### Docstore Writes for V2 Ops (Medium — M5)

Each op must also write to the docstore (not just bitmaps) for document serving and computed field lookups.

**Resolution:** Each op appends to the docstore via V2 tuple format: `DocSink.append(slot_id, field_idx, value)`. For `queryOpSet`, each affected slot gets a docstore write per field. Slot ID is always available from `entity_id` (direct ops) or from the query result set (queryOpSet).

### `meta` Field Write Amplification (Low — L5)

**Non-issue.** `hasMeta` and `onSite` are already precomputed as bit flags on the Image table (`flags` column — bit 13 = hasPrompt, bit 14 = madeOnSite, bit 2 = hideMeta). The COPY loader reads these directly via `CopyImageRow.has_meta()` and `.on_site()`. No raw `meta` JSONB tracking needed — `hasMeta` and `onSite` are plain boolean fields in `track_fields`, derived from flag bit changes.

### queryOpSet entity_id Dedup (Low — L2)

Multiple queryOpSets with `entity_id=0` would incorrectly deduplicate.

**Resolution:** Use the source entity's ID (ModelVersion ID, Post ID) as `entity_id`. Dedup logic treats `queryOpSet` ops separately — dedup by `(entity_id, query)` not `(entity_id, field)`.

---

## Files That Change

| File | Change |
|------|--------|
| `src/pg_sync/queries.rs` | BitdexOps table SQL, `poll_ops_from_cursor()` |
| `src/pg_sync/ops_poller.rs` | **New** — V2 poller with dedup |
| `src/pg_sync/op_dedup.rs` | **New** — shared dedup helper |
| `src/pg_sync/trigger_gen.rs` | **New** — YAML config → trigger SQL generator |
| `src/pg_sync/dump.rs` | **New** — table dump pipeline (COPY → WAL writer) |
| `src/pg_sync/config.rs` | V2 config fields, YAML sync_sources, dump config |
| `src/bin/pg_sync.rs` | Rename to bitdex-sync, add subcommands |
| `src/server.rs` | `POST /ops` (WAL-backed), `GET /sync-lag`, dump endpoints |
| `src/ops_wal.rs` | **New** — WAL writer + reader thread (ops + dumps) |
| `src/pg_sync/bitdex_client.rs` | `post_ops()`, dump registration |
| `src/metrics.rs` | `bitdex_sync_*` Prometheus gauges |
