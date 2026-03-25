# Computed Sort Fields

> Prerequisite for PG-Sync V2 unified load pipeline. See [pg-sync-v2-final.md](pg-sync-v2-final.md).

## Problem

`sortAt` is defined as `GREATEST(existedAt, publishedAt)` where:
- `existedAt` = `GREATEST(scannedAt, createdAt)` — computed at PG level from the Image table
- `publishedAt` — comes from the Post table via `queryOpSet`

These arrive as separate ops at different times (Image dump first, Post dump second, then independent steady-state triggers). BitDex needs to compute the final `sortAt` value whenever either source changes.

Currently, sort fields are simple name→value mappings with no computation support.

## Current State

**`SortFieldConfig`** (`src/config.rs`):
```rust
pub struct SortFieldConfig {
    pub name: String,
    pub source_type: String,  // "uint32", "int64"
    pub encoding: String,     // "linear"
    pub bits: u8,             // 32
    pub eager_load: bool,
}
```

No field references, no expressions, no dependencies.

**Mutation path** (`src/mutation.rs:diff_document()`):
- Reads sort field value directly from the Document
- Decomposes to bit layers via XOR diff
- No mechanism to compute a value from other fields

**Loader** (`src/loader.rs:json_to_document()`):
- Extracts and maps fields from JSON via `FieldMapping`
- No computation step

## Proposed Design

### Config Extension

```rust
pub struct SortFieldConfig {
    pub name: String,
    pub source_type: String,
    pub encoding: String,
    pub bits: u8,
    pub eager_load: bool,
    // NEW:
    pub computed: Option<ComputedField>,
}

pub struct ComputedField {
    pub op: ComputedOp,
    pub source_fields: Vec<String>,  // field names to read from document
}

pub enum ComputedOp {
    Greatest,  // max(field1, field2, ...)
    Least,     // min(field1, field2, ...)
}
```

Index config example:
```json
{
  "name": "sortAt",
  "source_type": "uint32",
  "bits": 32,
  "eager_load": true,
  "computed": {
    "op": "greatest",
    "source_fields": ["existedAt", "publishedAt"]
  }
}
```

### Mutation Path Changes

In `diff_document()`, when processing a computed sort field:

1. Check if any of the source fields changed in this mutation
2. If yes: read current values of ALL source fields from the document (new doc for changed fields, old doc for unchanged fields)
3. Apply the computation (e.g., `max(existedAt, publishedAt)`)
4. Use the computed value for the sort layer XOR diff

```rust
// Pseudocode for computed sort field handling
if let Some(computed) = &sort_config.computed {
    let source_changed = computed.source_fields.iter()
        .any(|f| new_doc.has_field(f) && old_doc.get(f) != new_doc.get(f));

    if source_changed {
        let values: Vec<u32> = computed.source_fields.iter()
            .map(|f| new_doc.get(f).or(old_doc.get(f)).unwrap_or(0))
            .collect();

        let new_value = match computed.op {
            ComputedOp::Greatest => values.into_iter().max().unwrap_or(0),
            ComputedOp::Least => values.into_iter().min().unwrap_or(0),
        };

        let old_value = /* same but using old_doc values */;
        // XOR diff old_value vs new_value, emit sort layer ops
    }
}
```

### Loader Changes

In `json_to_document()`, after extracting all regular fields, compute derived fields:

```rust
for sort_field in &config.sort_fields {
    if let Some(computed) = &sort_field.computed {
        let values: Vec<u32> = computed.source_fields.iter()
            .map(|f| doc.get_u32(f).unwrap_or(0))
            .collect();
        let result = match computed.op {
            ComputedOp::Greatest => values.into_iter().max().unwrap_or(0),
            ComputedOp::Least => values.into_iter().min().unwrap_or(0),
        };
        doc.set_sort(sort_field.name.clone(), result);
    }
}
```

### Source Fields as Sort Fields

The source fields (`existedAt`, `publishedAt`) must also be sort fields so their values are stored and accessible during mutation. They don't need `eager_load` — they can be lazy-loaded or even just stored in the docstore. The key is that when `publishedAt` changes via a Post `queryOpSet`, BitDex can read the current `existedAt` from the doc and compute the new `sortAt`.

Config:
```json
"sort_fields": [
  {"name": "existedAt", "source_type": "uint32", "bits": 32, "eager_load": false},
  {"name": "publishedAt", "source_type": "uint32", "bits": 32, "eager_load": false},
  {"name": "sortAt", "source_type": "uint32", "bits": 32, "eager_load": true,
   "computed": {"op": "greatest", "source_fields": ["existedAt", "publishedAt"]}}
]
```

Only `sortAt` needs to be eagerly loaded — the source fields are just stored for computation.

**Alternative:** Store source field values in the docstore only (not as sort fields). This avoids 2 extra sets of 32 bit-layer bitmaps. On mutation, read from docstore to compute. Tradeoff: docstore read on every mutation that touches a source field vs 64 extra bitmaps in memory.

## Gotchas and Performance Considerations

**Source field value lookup:** When `publishedAt` changes, we need the current value of `existedAt` to compute `sortAt`. Two options:
1. **Docstore read** (<1μs via doc cache) — read the stored document, extract the field value. Fast for single-slot mutations.
2. **Sort bitmap reconstruction** — iterate 32 bit-layer bitmaps, check each bit for the slot. 32 `contains()` calls to reconstruct one u32. Slower than docstore for single lookups.

**Recommendation:** Use docstore reads for steady-state single-slot mutations. For bulk operations (`queryOpSet` touching many slots), batch the computation. **Microbench required** against real data at 105M scale to validate.

**Bulk queryOpSet + computed fields:** When a Post's `publishedAt` changes via `queryOpSet "postId eq 789"`, potentially thousands of image slots need `sortAt` recomputed. Each needs its `existedAt` value read from the docstore. With doc cache this should be fast, but at 10K+ slots per queryOpSet it could add up. Profile this path.

**During dumps:** Not a concern — during initial load, the dump processor sets source fields sequentially (Image dump sets `existedAt`, then Post dump sets `publishedAt`). After the Post dump, a full recomputation pass over all slots sets `sortAt` correctly. This is a one-time bulk operation, not per-mutation.

**Sort bitmap memory:** Source fields (`existedAt`, `publishedAt`) as sort fields add 2 × 32 = 64 extra bit-layer bitmaps. At 105M slots, each layer is ~13MB (roaring-compressed), so ~832MB total. If memory is a concern, store source values in docstore only (not as sort bitmaps) and compute on mutation. Tradeoff: docstore read on every mutation vs 832MB memory.

## Scope

- **Start with `Greatest`** as the only computed op — it's the only one we need now
- Generalize later if needed (Least, Sum, etc.)
- Validation: source fields must exist in sort or filter config (or docstore-only fields)
- Property-based tests: computed value equals expected result across put/patch/delete cycles
- **Microbench:** Compare docstore read vs sort bitmap reconstruction for source value lookup at 105M scale

## Files That Change

| File | Change |
|------|--------|
| `src/config.rs` | Add `ComputedField`, `ComputedOp` to `SortFieldConfig` |
| `src/mutation.rs` | Computed value logic in `diff_document()` |
| `src/loader.rs` | Computed value logic in `json_to_document()` |
| `data/indexes/civitai/config.json` | Add `existedAt`, `publishedAt` as source sort fields, `sortAt` as computed |
