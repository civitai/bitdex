# Null Bitmap Implementation Plan

**Status:** In Progress
**Date:** 2026-04-01
**PR:** #118 (fix/nullable-filter-fields)

## Overview

Add null value support to nullable filter fields using a reserved sentinel key (`u64::MAX`) in the existing value bitmap infrastructure. This enables `IS NULL` / `IS NOT NULL` queries as first-class operations with zero changes to persistence, coalescing, flush, snapshot, eviction, or compaction.

## Design Decisions

1. **`u64::MAX` sentinel key in existing value bitmap HashMap** — reuses the entire existing pipeline (ShardStore, WriteBatch, flush thread, ArcSwap snapshots, eviction, compaction, cache invalidation). No new data structures needed.
2. **`IsNull` / `IsNotNull` query operators** — explicit operators. Parsers also rewrite `Eq(field, null)` → `IsNull(field)` for convenience.
3. **Nullable defined in data_schema** (`FieldMapping.nullable`) — not on FilterFieldConfig.
4. **Docstore: omission = null for nullable fields** — consistent with existing default field behavior. If `nullable: true`, default is null unless overridden.
5. **NotEq/NotIn on nullable fields excludes nulls** — SQL standard. `NotEq("blockedFor", "CSAM")` → `alive - eq_bitmap - null_bitmap`.
6. **Deferred alive: null bitmaps NOT set for deferred slots** — consistent with existing no-bitmap-state-until-activation.
7. **No multi-value null support** — multi-value fields use empty array for "no values", not null.
8. **No schema migration** — assume fresh state. Adding nullable to an existing field requires reindex.
9. **`Eq(field, null)` rewriting** — parsers rewrite to `IsNull(field)`. `In(field, ["A", null])` → `Or(In(field, ["A"]), IsNull(field))`.

## Constant

```rust
/// Reserved bitmap key for null values on nullable filter fields.
/// u64::MAX is safe because PG IDs are i32/i64 (max ~9.2e18) and
/// dictionary string IDs auto-increment from 0.
pub const NULL_BITMAP_KEY: u64 = u64::MAX;
```

## Nullable Fields (Civitai)

| Field | Type | Reason |
|-------|------|--------|
| blockedFor | low_cardinality_string | Not blocked = null |
| baseModel | low_cardinality_string | No resource/base model |
| availability | low_cardinality_string | No post = no availability |
| postId | integer | Image not in a post |
| postedToId | integer | Derived from Post |
| remixOfId | integer | Not a remix |

## Implementation Checklist

### Phase 1: Ops Processing (core behavior)

- [ ] **1.1 Define `NULL_BITMAP_KEY` constant**
  - Add `pub const NULL_BITMAP_KEY: u64 = u64::MAX;` to appropriate module

- [ ] **1.2 Update `process_set_op`** (`src/ops_processor.rs`)
  - When value is null AND field is nullable → `sink.filter_insert(field, NULL_BITMAP_KEY, slot)`
  - When value is non-null AND field is nullable → `sink.filter_remove(field, NULL_BITMAP_KEY, slot)` (clear null bit when setting a real value)
  - Null detection MUST happen on raw `JsonValue` BEFORE `json_to_qvalue()`

- [ ] **1.3 Update `process_remove_op`**
  - When value is null AND field is nullable → `sink.filter_remove(field, NULL_BITMAP_KEY, slot)` (removing a null = clearing the null bit)

- [ ] **1.4 Dump processor** (`src/dump_processor.rs`)
  - When `row.is_null(column)` and field is nullable → `sink.filter_insert(field, NULL_BITMAP_KEY, slot)`
  - Enrichment miss on nullable field → filter_insert with NULL_BITMAP_KEY
  - Computed fields evaluating to null → filter_insert with NULL_BITMAP_KEY

- [ ] **1.5 Fresh insert with missing nullable fields**
  - `document_to_ops` / `put_inner`: for fresh inserts, nullable fields absent from new doc → emit filter_insert with NULL_BITMAP_KEY

- [ ] **1.6 Delete path**
  - `remove_from_all()` already clears all bitmaps including the NULL_BITMAP_KEY entry — no change needed

### Phase 2: Query Support

- [ ] **2.1 Query AST** (`src/query.rs`)
  - Add `IsNull(String)` variant to FilterClause
  - Add `IsNotNull(String)` variant to FilterClause

- [ ] **2.2 Executor** (`src/executor.rs`)
  - `IsNull(field)` → look up bitmap at key `NULL_BITMAP_KEY`, return clone
  - `IsNotNull(field)` → `alive & !bitmap[NULL_BITMAP_KEY]`
  - Non-nullable field: IsNull → empty bitmap, IsNotNull → alive
  - Update `NotEq` → `alive - eq_bitmap - null_bitmap` (exclude nulls, SQL standard)
  - Update `NotIn` → `alive - union_bitmap - null_bitmap`
  - Add IsNull/IsNotNull to `try_and_by_ref` fast path
  - Add IsNull/IsNotNull to `evaluate_clause_with_candidates`

- [ ] **2.3 Query parsers**
  - **Bitdex** (`src/parser/json.rs`): `{ "IsNull": "fieldName" }` / `{ "IsNotNull": "fieldName" }`
  - **Compact** (`src/parser/compact.rs`): `{ "fieldName": { "$exists": false } }` / `{ "$exists": true }`
  - **Meilisearch** (`src/parser/meilisearch.rs`): `fieldName IS NULL` / `fieldName IS NOT NULL`
  - **All parsers**: rewrite `Eq(field, null)` → `IsNull(field)`

- [ ] **2.4 Planner** (`src/planner.rs`)
  - `estimate_cardinality` for `IsNull` → bitmap len at NULL_BITMAP_KEY (or alive/10 estimate)
  - `estimate_cardinality` for `IsNotNull` → alive_count - null bitmap len

### Phase 3: Tests

- [ ] **3.1 ops_processor unit tests**
  - null set → filter_insert with NULL_BITMAP_KEY
  - non-null set on nullable field → filter_remove with NULL_BITMAP_KEY
  - transition null→value→null round-trip
  - Remove old + Set null → old bitmap removed + null key inserted

- [ ] **3.2 Executor tests**
  - IsNull returns correct slots
  - IsNotNull returns alive minus null slots
  - IsNull on non-nullable field → empty
  - NotEq on nullable field excludes null slots

- [ ] **3.3 Parser tests**
  - Each parser parses IsNull/IsNotNull syntax
  - Eq(field, null) rewrites to IsNull

- [ ] **3.4 Integration test**
  - Start server, create index, POST ops with null values
  - Query IsNull → correct results
  - Query IsNotNull → correct results
  - Transition value→null→value → results update
  - Delete slot with null → cleared

## Edge Cases

1. **Set value then null**: clear value bitmap via Remove, set NULL_BITMAP_KEY via Set null
2. **Set null then value**: clear NULL_BITMAP_KEY, set value bitmap
3. **Delete on null slot**: `remove_from_all` clears all bitmaps including NULL_BITMAP_KEY — works automatically
4. **Dump null CSV values**: filter_insert with NULL_BITMAP_KEY
5. **Enrichment miss**: nullable target fields get NULL_BITMAP_KEY insert
6. **queryOpSet with null**: fan-out calls process_set_op which handles null — works transitively
7. **Deferred alive**: no null bits until activation (consistent with no-bitmap-state policy)
8. **Loading mode**: null bitmaps accumulate like any other value bitmap
9. **Compaction/eviction/persistence**: all automatic via existing infrastructure
10. **Cache invalidation**: NULL_BITMAP_KEY mutations appear in mutated_filter_fields — automatic
11. **IsNotNull**: `alive & !null_bitmap` — must use alive bitmap
12. **Non-nullable IsNull**: return empty (field has no NULL_BITMAP_KEY entry)
13. **NotEq/NotIn**: must subtract null bitmap to exclude nulls (SQL standard)
14. **Eq(field, null) rewriting**: parsers convert to IsNull
15. **json_to_qvalue ordering**: null detection on raw JsonValue before conversion (json_to_qvalue maps null to 0)
16. **Fresh insert missing nullable field**: must emit NULL_BITMAP_KEY insert for absent nullable fields

## What Does NOT Change

- FilterField struct
- BitmapSink trait
- MutationOp / WriteBatch
- ShardStore persistence
- Compaction
- Flush thread
- Snapshot clone / ArcSwap
- Eviction / lazy loading
- Cache invalidation logic
- bitmap_bytes / bitmap_count metrics
