# Dead Code Removal Plan

Produced 2026-04-04 via LSP path tracing of the four live paths (dump, ops, query, janitor) and cross-referencing all symbol references.

**Methodology:** Five parallel agents traced every function/struct/trait reachable from each entry point using `rust-analyzer` workspace-symbols + references. Findings were cross-validated with grep to catch cross-binary references the LSP can't see.

---

## Summary

| Category | Est. Lines | Risk |
|----------|-----------|------|
| Entire dead files (slot_arena, progress, loader, copy_queries) | ~3,350 | Low |
| Dead V1 code in live files (queries, bulk_loader) | ~800 | Low |
| Dead superseded code (HashMap enrichment/expression paths) | ~500 | Low |
| Test-only production structs (MutationEngine, AccumSink) | ~600 | Medium |
| Dead engine methods + stub endpoints | ~300 | Low |
| Dead binary (rebuild_bench) | ~200 | Low |
| **Total** | **~5,750** | |

---

## Tier 1: Entire Dead Files (delete completely)

### `src/sync/slot_arena.rs` (1,016 lines) -- CERTAIN DEAD

V1 arena-based slot storage. Zero external references in any production code. The only `use` is inside `scalars_to_json` in `bulk_loader.rs`, which is itself dead. Deploy scripts reference `slot_arena.bin` (the data file, not this code) -- those references are also stale.

**Action:** Delete file. Remove `pub mod slot_arena;` from `src/sync/mod.rs`.

### `src/sync/progress.rs` (113 lines) -- CERTAIN DEAD

Progress server for V1 bulk loader. Zero callers in any `src/` file. Only references: `examples/load_from_csv.rs` and `tests/bulk_load_fixture_test.rs`.

**Action:** Delete file. Remove `pub mod progress;` from `src/sync/mod.rs`. Update example/test if they still compile (they likely reference other V1 code too).

---

## Tier 2: Heavily Dead Files (>50% dead code inside)

### `src/sync/queries.rs` (787 lines, ~500 dead)

**ALIVE (setup + cursor ops, called from pg_sync binary):**
- `run_setup()`, `run_setup_v2()`, `get_max_ops_id()`, `get_max_outbox_id()`, `upsert_cursor()`, `SETUP_SQL`, `SETUP_V2_SQL`, `find_trigger_table()`, `check_triggers_exist()`

**DEAD (V1 fetch functions, zero production callers):**
- 11 structs: `ImageRow`, `TagRow`, `ToolRow`, `TechniqueRow`, `ResourceRow`, `OutboxRow`, `MetricRow`, `CollectionItemRow`, `StreamTagRow`, `StreamResourceRow`, `StreamCollectionRow`
- 17 functions: `get_max_image_id`, `fetch_images_by_range`, `fetch_images_by_ids`, `fetch_tags`, `fetch_tools`, `fetch_techniques`, `fetch_resources`, `fetch_collections`, `poll_outbox_from_cursor`, `get_max_tag_id`, `fetch_tags_by_tag_range`, `get_max_tool_id`, `fetch_tools_by_tool_range`, `get_max_technique_id`, `fetch_techniques_by_technique_range`, `fetch_resources_by_range`, `get_max_collection_id`, `fetch_collections_by_range`

**Action:** Delete all V1 row structs and fetch functions. Keep setup/cursor functions.

### `src/sync/bulk_loader.rs` (810 lines, ~300 dead)

**ALIVE (CSV download, called from pg_sync binary):**
- `download_phase_csvs()`, `download_all_tables()`, `download_table()`, `download_copy_query()`, `download_enrichment_csvs()`, `download_from_sync_config()`, `download_single_table()`, `download_metrics_from_clickhouse()`, `clear_done_markers()`, `TableDownload`, `TABLES`

**DEAD (V1 data structs + helpers, zero production callers):**
- `ImageScalars` struct -- hardcoded V1 image row
- `ResourceEnrichment` struct -- hardcoded V1 enrichment
- `BulkLoadStats` struct -- never used outside tests
- `FINALIZE_CHUNK_SIZE` constant
- `finalize_from_bitmaps()` -- stub, returns `Err`
- `scalars_to_json()` -- V1 docstore serializer (uses dead `slot_arena` imports)
- `cleanup_orphan_bitmaps()` -- V1 bitmap cleanup

**Action:** Delete V1 structs, `scalars_to_json`, `cleanup_orphan_bitmaps`, `finalize_from_bitmaps`, `BulkLoadStats`, `FINALIZE_CHUNK_SIZE`.

### `src/sync/dump_enrichment.rs` (1,263 lines, ~300 dead)

HashMap-based enrichment paths superseded by indexed variants.

**DEAD:**
- `LookupRow::to_csv_row()` -- never called
- `LookupRow::iter_columns()` -- never called
- `LookupRow::get()` -- only called from dead `enrich_from_lookup`
- `EnrichmentTable::get()` -- returns LookupRow ref, dead
- `EnrichmentTable::child()` -- never called
- `EnrichmentTable::enrich()` -- HashMap-based, replaced by `enrich_indexed_into()`
- `EnrichmentTable::enrich_indexed()` -- allocating variant, replaced by `enrich_indexed_into()`
- `EnrichmentTable::enrich_from_lookup()` -- explicitly marked "Legacy", dead
- `EnrichmentManager::enrich_row()` -- HashMap-based, replaced by `enrich_row_indexed_into()`
- `EnrichmentManager::enrich_row_indexed()` -- allocating variant, replaced by `enrich_row_indexed_into()`
- `EnrichmentManager::clear()`, `drop_table()`, `total_memory()` -- never called
- `resolve_dictionary_value()`, `resolve_expr_to_bitmap_key()` -- never called
- `DictionarySet` struct + all methods -- never used from dump path

**Action:** Delete all listed items. The "into" variants and `MmapIndex` stay.

### `src/sync/dump_expression.rs` (1,065 lines, ~200 dead)

HashMap-based eval path superseded by indexed evaluators.

**DEAD:**
- `EvalContext` struct -- HashMap-based context, only used in tests
- `Expr::eval()` -- HashMap-based evaluator, only test callers
- `FilterExpression::eval()` -- HashMap-based, only test callers
- `ComputedFieldDef::eval()` -- HashMap-based, only test callers
- `build_column_index()` -- utility, dump processor builds its own
- `ExprValue::as_str_value()` -- never called

**Action:** Delete HashMap-based eval methods. Keep `eval_indexed()` variants and all parser/tokenizer code.

### `src/sync/dump_processor.rs` (2,973 lines, ~50 dead)

Mostly alive. Small dead spots:

**DEAD:**
- `ParsedRow::to_csv_row()` -- replaced by `fill_indexed_fields()`
- `ParsedRow::to_indexed_fields()` -- replaced by `fill_indexed_fields()`

**Action:** Delete the two dead methods.

---

## Tier 3: Dead Code in Core Engine Files

### `src/mutation.rs` (1,663 lines, ~500 dead)

**DEAD production code (test-only infrastructure):**
- `MutationEngine` struct (~250 lines, L712-960) -- only used in test modules of `mutation.rs`, `executor.rs`, and `planner.rs`. Never called from any production path (all writes go through MutationOp → flush thread, not MutationEngine).
  - `MutationEngine::new()`, `put()`, `patch()`, `delete()`, `allocate_slot()`
- `PatchPayload` struct (L99) -- only used by `MutationEngine::patch()` + tests
- `PatchField` struct (L105) -- only used by `PatchPayload`
- `diff_document_partial()` (L336) -- only called from `MutationEngine::put()`
- `diff_patch()` (L456) -- only called from `MutationEngine::patch()`

**Note:** `collect_filter_insert_ops()` is NOT dead -- it's called from `diff_document()` which is used by the flush thread for deferred alive activation. `MutationEngine` is purely test scaffolding.

**Action:** Move `MutationEngine`, `PatchPayload`, `PatchField`, `diff_document_partial`, `diff_patch` into `#[cfg(test)]` block or a `test_helpers` module. This preserves test coverage without polluting the production build. Alternatively, restructure tests to not need MutationEngine (use the actual mutation channel path).

### `src/ops_processor.rs` (2,135 lines, ~150 dead)

**DEAD:**
- `document_to_ops()` (L219) -- only test references. Was for PUT→WAL decomposition, never shipped.
- `qvalue_to_json()` -- helper for `document_to_ops`, zero callers
- `OpsProcessorConfig` struct -- zero callers (ops processor config comes from server/WAL setup, not this struct)

**Action:** Delete `document_to_ops`, `qvalue_to_json`, `OpsProcessorConfig`. Delete associated tests.

### `src/engine/concurrent_engine.rs` (1,167 lines, ~80 dead)

**DEAD:**
- `compact_docstore()` (L781) -- zero callers. Janitor compacts via silo directly. `compact_all()` exists but doesn't call this.
- `persist_dirty_dictionaries()` -- zero callers
- `docstore_schema_version()` -- zero callers
- `indexed_field_names()` -- zero callers

**Action:** Delete all four methods.

### `src/engine/sort.rs` (992 lines, ~60 dead)

**DEAD:**
- `SortField::layer_bases()` -- zero callers
- `SortField::layer_bases_fused()` -- zero callers
- `SortField::slots_in_range()` -- test-only callers

**Action:** Delete `layer_bases` and `layer_bases_fused`. Move `slots_in_range` to `#[cfg(test)]` or delete.

### `src/engine/executor.rs` (1,543 lines, ~30 dead)

**DEAD:**
- `QueryExecutor::slot_matches_filters()` -- zero production callers (was post-validation, removed)
- Builder methods: `with_string_maps()`, `with_case_sensitive_fields()`, `with_dictionaries()`, `with_bitmap_silo()`, `with_time_buckets()` -- bypassed by `new_full()`, only used in tests

**Action:** Move builder methods to `#[cfg(test)]`. Delete `slot_matches_filters`.

### `src/engine/filter.rs` (553 lines, ~20 dead)

**DEAD:**
- `FilterField::distinct_count()` -- zero callers

**Action:** Delete.

---

## Tier 4: Dead Sync Module Code

### `src/sync/ingester.rs` (226 lines, ~100 dead)

**ALIVE:** `BitmapSink` trait, `CoalescerSink` struct
**DEAD:** `AccumSink` struct + impl (~100 lines) -- V1 bulk loading sink. Only callers are a test in the same file and `examples/validate_ops_pipeline.rs`.

**Action:** Move `AccumSink` to `#[cfg(test)]` or delete entirely. Update example.

### `src/sync/loader.rs` (1,855 lines) -- DELETE

`load_ndjson()` is called from the server's `POST /load` handler. `BitmapAccum` is used by `bulk_loader.rs` (dead path) and `AccumSink` in `ingester.rs` (dead). Justin confirmed NDJSON loading is no longer needed.

**Action:** Delete entire file. Remove `POST /load` handler + route from `server.rs`. Remove `pub mod loader;` from `src/sync/mod.rs`. Remove `LoadRequest` struct from server.

### `src/sync/copy_queries.rs` (364 lines) -- DELETE

Called from `bulk_loader::download_table()` which is called from `pg_sync::download_all_tables()`. This is the V1 CSV download path. V2 uses config-driven `download_from_sync_config()` instead. Justin confirmed V1 download path is no longer needed.

**Action:** Delete entire file. Remove `download_all_tables()`, `download_table()`, `download_single_table()`, and `TABLES` from `bulk_loader.rs`. Remove `pub mod copy_queries;` from `src/sync/mod.rs`. Remove the fallback call in `pg_sync.rs`.

---

## Tier 5: Stub HTTP Endpoints

### `src/server.rs` -- NOT_IMPLEMENTED stubs

These endpoints exist but return 501:
- `handle_patch_documents()` (L2748) -- "PATCH is not implemented, use PUT"
- `handle_filter_sync()` (L2764) -- "filter_sync is not implemented, use PUT"
- `FilterSyncRequest` struct, `UpsertRequest` usage in PATCH

**Action:** Consider removing these stubs and their route registrations. They add dead weight.

---

## Tier 6: Binaries

### `src/bin/rebuild_bench.rs` -- DELETE

Justin confirmed this is no longer needed.

**Action:** Delete file. Remove `[[bin]]` entry from `Cargo.toml`.

### `src/bin/replay.rs` -- KEEP

Traffic replay tool for the capture system. Capture endpoints (`/debug/capture/start`, `/stop`, `/status`) and the CaptureManager middleware are still fully wired in `server.rs`. `capture.rs` is alive.

### `src/bin/benchmark.rs` -- KEEP

Active benchmark suite.

### `src/bin/loadtest.rs` -- KEEP

Active load testing tool.

---

## Ownership & Execution Plan

Two agents working in parallel. Arabella works in a **worktree** (isolated branch, opens PR).
Lucy works on **main** (her changes are in-flight, different files).

### Arabella (worktree → PR) — Pure deletion, ~5,200 lines

Execute in this order. `cargo build --features server,pg-sync` and `cargo test` after each step.

1. **Tier 1: Entire dead files** (~3,550 lines)
   - Delete `src/sync/slot_arena.rs`, `src/sync/progress.rs`, `src/sync/loader.rs`, `src/sync/copy_queries.rs`
   - Delete `src/bin/rebuild_bench.rs`, remove `[[bin]]` from Cargo.toml
   - Remove `pub mod slot_arena;`, `pub mod progress;`, `pub mod loader;`, `pub mod copy_queries;` from `src/sync/mod.rs`
   - Remove `POST /load` handler + `LoadRequest` struct from `server.rs`
   - Remove `download_all_tables()`, `download_table()`, `download_single_table()`, `TABLES` from `bulk_loader.rs`

2. **Tier 3: Dead engine methods** (~80 lines)
   - Delete `compact_docstore()`, `persist_dirty_dictionaries()`, `docstore_schema_version()`, `indexed_field_names()` from `concurrent_engine.rs`
   - Delete `SortField::layer_bases()`, `layer_bases_fused()` from `sort.rs`. Move `slots_in_range` to `#[cfg(test)]`.
   - Delete `QueryExecutor::slot_matches_filters()` from `executor.rs`. Move builder methods to `#[cfg(test)]`.
   - Delete `FilterField::distinct_count()` from `filter.rs`

3. **Tier 2: V1 dead code in sync/** (~800 lines)
   - Delete 11 V1 row structs + 17 fetch functions from `queries.rs`
   - Delete `ImageScalars`, `ResourceEnrichment`, `BulkLoadStats`, `FINALIZE_CHUNK_SIZE`, `finalize_from_bitmaps()`, `scalars_to_json()`, `cleanup_orphan_bitmaps()` from `bulk_loader.rs`
   - Delete `ParsedRow::to_csv_row()`, `ParsedRow::to_indexed_fields()` from `dump_processor.rs`

4. **Tier 2: Dead enrichment/expression paths** (~500 lines)
   - Delete all dead methods from `dump_enrichment.rs`: `LookupRow::to_csv_row()`, `iter_columns()`, `get()`, `EnrichmentTable::get()`, `child()`, `enrich()`, `enrich_indexed()`, `enrich_from_lookup()`, `EnrichmentManager::enrich_row()`, `enrich_row_indexed()`, `clear()`, `drop_table()`, `total_memory()`, `resolve_dictionary_value()`, `resolve_expr_to_bitmap_key()`, `DictionarySet` struct
   - Delete HashMap-based eval methods from `dump_expression.rs`: `EvalContext`, `Expr::eval()`, `FilterExpression::eval()`, `ComputedFieldDef::eval()`, `build_column_index()`, `ExprValue::as_str_value()`

5. **Tier 3: ops_processor dead code** (~150 lines)
   - Delete `document_to_ops()`, `qvalue_to_json()`, `OpsProcessorConfig` from `ops_processor.rs`

6. **Tier 4: Test-only production structs** (~100 lines)
   - Move `AccumSink` in `ingester.rs` to `#[cfg(test)]` or delete
   - Move `MutationEngine`, `PatchPayload`, `PatchField`, `diff_document_partial`, `diff_patch` in `mutation.rs` to `#[cfg(test)]`

7. **Tier 5: Stub endpoints** (~50 lines)
   - Remove `handle_patch_documents()`, `handle_filter_sync()`, `FilterSyncRequest`, and their route registrations from `server.rs`

### Lucy (main) — Targeted edits, different files

Lucy handles items that Arabella's plan does NOT cover, or that touch files Lucy is actively modifying:

- **dump-timing feature flag** (in progress) — `dump_processor.rs`, `Cargo.toml`
- **streaming_merge=true test** — new test in `dump_processor.rs`
- **Guard EnrichmentTable::get() for Mmap** — `dump_enrichment.rs` (Arabella deletes the HashMap `get()` but Lucy guards the Mmap path)
- **Dead CacheStats/CacheEntryDetail stubs** — `concurrent_engine.rs` (Arabella deletes other dead methods but not these; Lucy handles the rename + Prometheus cleanup)
- **Rename `clear_unified_cache()` → `clear_cache()`** — `concurrent_engine.rs`, `server.rs`, `benchmark.rs`, `dump_processor.rs` via Rust LSP rename
- **Rename `UnifiedKey` → `CacheKey`** — `cache_silo.rs`, `query.rs` via Rust LSP rename
- **Arc-wrap cache entry bitmaps** — `query.rs`, `cache_silo.rs`
- **Reuse intermediate lookup_fields buffer** — `dump_enrichment.rs`
- **MADV_RANDOM for enrichment lookups** — `dump_enrichment.rs`
- **200M key cap warning** — `dump_enrichment.rs`

### Merge Strategy

1. Arabella opens PR from worktree branch
2. Scarlet sends verification sub-agent to review PR
3. If clean, merge Arabella's PR first (pure deletions, low conflict risk)
4. Lucy rebases if needed (unlikely — different files mostly)
5. Lucy's changes committed on main after Arabella's PR merges

**After each tier:** Run `cargo build --features server,pg-sync` and `cargo test` to verify nothing breaks.

---

## Files NOT Dead (confirmed alive by path tracing)

These were suspected but confirmed alive:

| File | Why alive |
|------|-----------|
| `src/bucket_diff_log.rs` | Used by flush thread + query cache diff tracking |
| `src/silos/bitmap_silo.rs` | Used by query path (ops-on-read), janitor, dump |
| `src/silos/cache_silo.rs` | Used by query path (cache), janitor (compact) |
| `src/silos/cache.rs` | Used by query path (canonicalization) |
| `src/silos/doc_format.rs` | Used everywhere (StoredDoc, PackedValue) |
| `src/silos/doc_silo_adapter.rs` | Used by ops path, query path, janitor |
| `src/capture.rs` | Used by server endpoints (capture start/stop) |
| `src/time_buckets.rs` | Used by flush thread + query path |
| `src/dictionary.rs` | Used by query planner + dump path |
| `src/capture.rs` | Active debug tool — capture endpoints wired in server |
| `src/bin/replay.rs` | Traffic replay for capture system — still active |
