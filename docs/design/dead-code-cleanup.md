# Dead Code Cleanup

**Created:** 2026-03-31 (data fix sprint)
**Context:** ~3 hours spent debugging/fixing code paths that production never executes. PRs #107 and #108 modified dead code. This doc inventories what to clean up.

## Production Dump Flow (the ACTUAL path)

```
bitdex-sync (pg_sync.rs)
  → run_streaming_pipeline()
    → download_phase_csvs() [PG COPY to disk]
    → PUT /api/indexes/{name}/dumps [register with server]
    → POST /dumps/{name}/loaded [signal CSV ready]
    → server: dump_processor.rs processes CSV directly
      → parallel rayon parse → filter/sort bitmaps + docstore writes
      → save_phase_to_disk() → ShardStore
```

**NOT** csv_ops → WAL → ops_processor. That path exists in code but is never called.

## Dead Code Inventory

### 1. `src/pg_sync/csv_ops.rs` (entire module)

**Status:** DEAD. Zero callers outside the module.

Functions defined but never called:
- `images_csv_to_wal()` — converts image CSV rows to WAL ops
- `tags_csv_to_wal()` — converts tag CSV to WAL ops
- `tools_csv_to_wal()` — converts tools CSV to WAL ops
- `techniques_csv_to_wal()` — converts techniques CSV to WAL ops
- `multi_value_csv_to_wal()` — generic multi-value CSV→WAL
- `image_row_to_ops()` / `image_row_to_ops_pub()` — row→ops conversion

**Why it exists:** Written as an alternative dump path (CSV→ops→WAL→ops_processor) but never wired into the streaming pipeline. The streaming pipeline uses dump_processor.rs directly instead.

**Action:** Delete entire module. Remove `pub mod csv_ops;` from `src/pg_sync/mod.rs`.

### 2. `src/ops_processor.rs::process_wal_dump()` (line ~1259)

**Status:** DEAD. Zero callers outside ops_processor.rs.

Reads a WAL file and applies ops via `apply_ops_batch_dump()` to a BitmapAccum. Was the intended consumer of csv_ops output, but the dump pipeline uses dump_processor.rs instead.

**Action:** Delete function. If needed for future WAL replay, re-implement when actually needed.

### 3. `src/ops_processor.rs::apply_ops_batch_dump()` (line ~1241)

**Status:** DEAD. Only caller is `process_wal_dump()` (also dead).

Variant of `apply_ops_batch()` that writes to a BitmapAccum instead of a BitmapSink.

**Action:** Delete function.

## Why This Matters

During the 2026-03-31 data fix sprint:

- **3+ hours** spent auditing the "metrics pipeline end-to-end" including csv_ops.rs code paths that never execute
- **PRs #107/#108** modified ops_processor code (the WAL/ops path) to fix issues that only manifest through a code path production doesn't use
- Team members (Scarlet, Josh) traced through csv_ops.rs believing it was the production dump path, leading to incorrect diagnosis of sortAt computation

Dead code that looks like production code is actively dangerous — it wastes investigation time and leads to fixes in the wrong place.

### 4. Dead modules in `src/` (lib.rs exports)

**`src/field_handler.rs`** — DEAD. Exported in lib.rs but zero imports anywhere. Trait + implementations for field operation validation. Abandoned feature.

**`src/preset.rs`** — DEAD. Exported in lib.rs but zero imports. Config overlay system (`load_preset`, `apply_preset`). Only internal tests call it.

**`src/shard_store_migrate.rs`** — DEAD. Zero external callers. V2→V3 docstore migration code that was never wired in.

**`src/shard_store_doc.rs`** — DEAD (transitively). Only imported by `field_handler.rs` and `shard_store_migrate.rs`, both dead.

### 5. Dead functions in `src/pg_sync/copy_queries.rs`

Parse functions only called by dead modules (csv_ops.rs, dead ops_processor enrichment functions):
- `parse_image_row` — only caller is csv_ops.rs (dead)
- `parse_tag_row` — only caller is csv_ops.rs (dead)
- `parse_tool_row` — only caller is csv_ops.rs (dead)
- `parse_technique_row` — only caller is csv_ops.rs (dead)
- `parse_resource_row` — only caller is csv_ops.rs (dead)
- `parse_post_row` — callers are csv_ops.rs (dead) + ops_processor load_posts_enrichment (dead private fn)
- `parse_model_version_row` — callers are csv_ops.rs (dead) + ops_processor (dead private fn)
- `parse_model_row` — callers are csv_ops.rs (dead) + ops_processor (dead private fn)
- `parse_metric_row` — zero callers
- `parse_collection_item_row` — zero callers

The `copy_*` functions (copy_images, copy_posts, etc.) are LIVE — used by bulk_loader for PG COPY.

### 6. Dead functions in `src/loader.rs`

- `json_to_document()` — zero callers (superseded by `json_to_document_with_dicts`)
- `apply_computed_sort_fields()` — zero callers
- `convert_field_with_dict()` — zero callers

### 7. Dead functions in `src/pg_sync/bulk_loader.rs`

- `download_from_sync_config()` — zero callers outside module

### 8. Nearly-dead: `src/pg_sync/backfill.rs`

Entry points are dead:
- `auto_backfill()` — zero callers
- `needs_backfill()` — zero callers
- `mark_backfilled()` — zero callers

Only `process_collection_items_csv()` is called (from integration tests).

### 9. Nearly-dead: `src/ingester.rs`

- `Ingester<B>` struct — zero callers. Callers use `CoalescerSink`/`AccumSink` directly.
- `DocSink` — only used in ingester's own tests, never in production.
- `BitmapSink` trait and `CoalescerSink`/`AccumSink` are LIVE.

## Cleanup Plan

| Item | Action | Risk |
|------|--------|------|
| `src/pg_sync/csv_ops.rs` | Delete module + mod.rs entry | None — zero callers |
| `src/field_handler.rs` | Delete module + lib.rs entry | None — zero imports |
| `src/preset.rs` | Delete module + lib.rs entry | None — zero imports |
| `src/shard_store_migrate.rs` | Delete module + lib.rs entry | None — zero callers |
| `src/shard_store_doc.rs` | Delete module + lib.rs entry | None — only dead callers |
| `process_wal_dump()` | Delete | None — zero callers |
| `apply_ops_batch_dump()` | Delete | None — only caller is process_wal_dump |
| `parse_*_row` functions | Delete all 10 parse functions | None — only dead callers |
| `ops_processor` enrichment fns | Delete load_posts/mv/model_enrichment | None — private, zero callers |
| `loader.rs` dead fns | Delete json_to_document, apply_computed_sort_fields, convert_field_with_dict | None |
| `bulk_loader::download_from_sync_config` | Delete | None — zero callers |
| `backfill.rs` entry points | Delete auto_backfill, needs_backfill, mark_backfilled | None — zero callers |
| `ingester.rs` Ingester struct | Delete wrapper; keep BitmapSink + sinks | Low — callers already bypass it |

**Estimated total:** ~2,000-3,000 lines of dead code across 6 modules and ~20 functions.

## Related: Code That IS Production

For reference, the code paths that ARE live in production:

- **Dump path:** `dump_processor.rs` (CSV → bitmaps + docstore)
- **Steady-state ops:** `ops_processor.rs::apply_ops_batch()` (WAL reader → bitmap mutations)
- **Metrics poller:** `metrics_poller.rs` (ClickHouse → Op::Set → /ops endpoint)
- **Ops endpoint:** `server.rs::handle_ops()` (POST /ops → WAL append)
- **Server handlers:** All 51 route handlers are wired and live
- **All 6 binary targets:** Live and registered in Cargo.toml
- **bitmap_fs.rs:** Still heavily used (82 refs in concurrent_engine) despite "legacy" label
- **unified_cache.rs:** Live (82 refs in concurrent_engine)
