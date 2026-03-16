# Filter-Only Fields

**Status:** Implemented (branch `feature/collection-ids-filter-only`)
**Date:** 2026-03-16

## Problem

BitDex has `doc_only` fields: stored in the docstore but not bitmap-indexed. Used for display data (URLs, hashes, dimensions) that clients need in results but never filter or sort by.

The inverse case had no support: fields that should be bitmap-indexed but **not** stored in the docstore. This arises when membership data comes from a separate table rather than the document itself. Storing it in the docstore would duplicate data that the engine doesn't own and can't keep consistent through the normal document path.

## Solution

### `filter_only` on FieldMapping

A new boolean flag on `FieldMapping` in `src/config.rs`:

```json
{
  "source": "collectionIds",
  "target": "collectionIds",
  "value_type": "integer_array",
  "filter_only": true,
  "default": []
}
```

**Behavior:**

| Flag | Bitmap-indexed | Stored in docstore |
|------|:-:|:-:|
| neither | yes | yes |
| `doc_only: true` | no | yes |
| `filter_only: true` | yes | no |

`doc_only` and `filter_only` are mutually exclusive. Schema validation rejects both set on the same field.

### Where `filter_only` gates

1. **`json_to_document_with_dicts()`** (`src/loader.rs`): skips filter_only fields. The `Document` struct passed to `engine.put()` or `engine.patch_document()` never contains filter_only fields. This means no docstore writes and no diffing through the normal document path.

2. **`json_to_stored_doc()`** (test helper): same skip.

3. **`extract_bitmaps_with_dicts()`**: unchanged. Only `doc_only` fields are skipped from bitmap extraction. A filter_only field present in the JSON still gets bitmap-indexed during bulk loading.

### The PATCH problem

Since filter_only fields are excluded from the Document, the normal PUT/PATCH path cannot update their bitmaps. The document diff compares old vs. new Document fields — if the field isn't there, nothing happens. This is by design: the normal document path doesn't own this data.

### Filter-Sync Endpoint

A dedicated endpoint handles filter_only multi-value field updates:

```
POST /api/indexes/{name}/documents/filter-sync
{
  "field": "collectionIds",
  "documents": [
    { "id": 12345, "values": [100, 200, 300] },
    { "id": 67890, "values": [200] }
  ]
}
```

**Response:** `{"synced": 2}` or `{"synced": 1, "errors": [...]}`

### Engine Method: `sync_filter_values()`

`ConcurrentEngine::sync_filter_values(slot, field_name, new_values)`:

1. Verify the slot is alive (returns `SlotNotFound` if not).
2. Snapshot the current engine state.
3. Scan all **loaded** bitmaps for the named field. For each bitmap that contains this slot, record the value as an "old value."
4. Diff old values vs. new values:
   - Values in old but not new: `FilterRemove` ops.
   - Values in new but not old: `FilterInsert` ops.
   - Values in both: no-op.
5. Send ops to the coalescer channel.
6. No docstore involvement.

**Limitation:** The scan only sees loaded bitmaps. If a bitmap for value X is not loaded (lazy-load, evicted), and the slot was previously in that bitmap, the remove won't happen until that bitmap is loaded. In practice this is acceptable because:
- Bulk load sets the initial state correctly.
- Steady-state sync always provides the full current value set from PG.
- Lazy-loaded bitmaps will reflect the correct state from their on-disk snapshot.

### Config Addition

In `FilterFieldConfig` (existing struct, no changes needed):

```json
{ "name": "collectionIds", "field_type": "multi_value" }
```

The field behaves identically to `tagIds` or `modelVersionIds` for queries. Lazy per-value loading and existence sets work the same way.

## Files Changed

| File | Change |
|------|--------|
| `src/config.rs` | `filter_only: bool` on FieldMapping, validation |
| `src/loader.rs` | Skip filter_only in document conversion, 4 tests |
| `src/concurrent_engine.rs` | `sync_filter_values()`, 3 tests |
| `src/server.rs` | `/documents/filter-sync` endpoint + request types |

## Tests

- `test_filter_only_excluded_from_document` — verifies filter_only fields don't appear in Document or StoredDoc
- `test_filter_only_still_indexed_in_bitmaps` — verifies bitmap extraction still works for filter_only fields
- `test_filter_only_and_doc_only_mutually_exclusive` — schema validation rejects both flags
- `test_sync_filter_values_add_and_remove` — replace [100,200] with [200,300], verify 100 removed, 300 added, 200 kept
- `test_sync_filter_values_clear_all` — sync to empty array removes all memberships
- `test_sync_filter_values_slot_not_found` — error on non-existent slot
