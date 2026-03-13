# BitDex Index Configuration Schema

An index config is a JSON file with three top-level sections: **name**, **config** (engine configuration), and **data_schema** (NDJSON loading rules).

```
POST /api/indexes
```

```json
{
  "name": "civitai",
  "config": { ... },
  "data_schema": { ... }
}
```

---

## `name`

**Type:** `string`

Unique identifier for the index. Used in API paths (`/api/indexes/{name}/query`) and as the directory name under `data_dir/indexes/`.

---

## `config`

Engine configuration. Controls what fields are indexed, how queries are sorted, and tuning parameters.

### `config.filter_fields`

**Type:** `FilterFieldConfig[]`

Each entry creates bitmap indexes for one field. Every distinct value gets its own roaring bitmap.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `name` | `string` | yes | — | Field name. Must match a `target` in `data_schema.fields`. Must be unique across all filter fields. |
| `field_type` | `enum` | yes | — | `"single_value"`, `"multi_value"`, or `"boolean"` |
| `storage` | `enum` | no | `"snapshot"` | `"snapshot"` (always in memory) or `"cached"` (loaded on demand from disk via moka cache) |
| `behaviors` | `object \| null` | no | `null` | Time-related behaviors (see below) |

**Field types:**

- **`single_value`** — One integer value per document (e.g., `nsfwLevel`, `userId`, `type`). Creates one bitmap per distinct value.
- **`multi_value`** — Array of integers per document (e.g., `tagIds`, `modelVersionIds`). Creates one bitmap per distinct value; a document appears in all bitmaps for its values.
- **`boolean`** — True/false. Creates two bitmaps: value `1` (true) and value `0` (false). This allows both `Eq` and `NotEq` queries on boolean fields.

**`behaviors` (optional):**

For timestamp fields only.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `deferred_alive` | `bool` | `false` | If true, documents with future values in this field are hidden (alive bit deferred) until the timestamp arrives. Not valid on `boolean` fields. |
| `range_buckets` | `BucketConfig[]` | `[]` | Pre-computed time range bitmaps (e.g., "all docs from last 24h"). |

**`BucketConfig`:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | `string` | Identifier used in cache keys (e.g., `"24h"`, `"7d"`, `"30d"`). Must be unique within the field. |
| `duration_secs` | `u64` | Bucket duration in seconds (e.g., `86400` for 24h). Must be > 0. |
| `refresh_interval_secs` | `u64` | How often to rebuild this bucket's bitmap, in seconds. Must be > 0. |

### `config.sort_fields`

**Type:** `SortFieldConfig[]`

Each entry creates N bitmap layers (one per bit position) for sort-by-bitmap-traversal. Queries can sort by any configured sort field in ascending or descending order.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `name` | `string` | yes | — | Field name. Must match a `target` in `data_schema.fields`. Must be unique. |
| `source_type` | `string` | no | `"uint32"` | Source type. Currently only `"uint32"` is supported. |
| `encoding` | `string` | no | `"linear"` | `"linear"` = full precision bit layers. `"log"` is reserved for future use. |
| `bits` | `u8` | no | `32` | Number of bitmap layers. Must be 1–64. A `uint32` field uses 32 layers. |

### `config.max_page_size`

**Type:** `usize` — **Default:** `100`

Hard cap on the `limit` parameter in queries. Clients cannot request more results than this per page.

### `config.cache`

Trie cache and bound cache settings.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `max_entries` | `usize` | `10000` | Maximum cached filter result bitmaps. |
| `decay_rate` | `f64` | `0.95` | Exponential decay for hit stats. Must be in `(0.0, 1.0]`. |
| `bound_target_size` | `usize` | `10000` | Target cardinality for bound cache entries (approximate top-K for sort acceleration). |
| `bound_max_size` | `usize` | `20000` | Max bound size before triggering rebuild. |
| `bound_max_count` | `usize` | `100` | Maximum bound cache entries before LRU eviction. |

### `config.storage`

Bitmap persistence settings.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `bitmap_path` | `string \| null` | `null` | Path to redb file for bitmap persistence. `null` = memory-only. When using the server, this is auto-set to `{index_dir}/bitmaps.redb`. |
| `tier2_cache_size_mb` | `u64` | `1024` | Memory budget for the Tier 2 moka cache (for `"cached"` storage mode fields). |
| `merge_diff_threshold` | `usize` | `10000` | Compact filter diffs after this many set+clear operations. |
| `pending_drain_cap` | `usize` | `100` | Max Tier 2 bitmaps to drain from pending buffer per merge cycle. |

### Operational tuning (usually left at defaults)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `autovac_interval_secs` | `u64` | `3600` | Autovacuum interval (seconds). *Config-only — autovac thread not yet implemented (Phase 4).* |
| `merge_interval_ms` | `u64` | `5000` | Versioned bitmap merge interval (milliseconds). |
| `prometheus_port` | `u16` | `9090` | Prometheus metrics port. *Config-only — metrics endpoint not yet wired up (Phase 4).* |
| `flush_interval_us` | `u64` | `100` | Background flush thread interval (microseconds). |
| `channel_capacity` | `usize` | `100000` | Bounded channel capacity for the write coalescer. |

---

## `data_schema`

Describes how raw NDJSON records are mapped to engine documents during bulk loading. The loader reads each JSON line, applies these mapping rules, and produces documents with the correct field names and types for the engine.

### `data_schema.id_field`

**Type:** `string`

Name of the JSON field containing the document's unique integer ID. This value becomes the bitmap slot ID.

### `data_schema.fields`

**Type:** `FieldMapping[]`

Each entry describes one field mapping from raw JSON to engine document.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `source` | `string` | yes | — | Field name in the raw JSON record. |
| `target` | `string` | yes | — | Field name in the engine document. Must match a `filter_fields` or `sort_fields` name (unless `doc_only`). |
| `value_type` | `enum` | yes | — | How to interpret the value (see below). |
| `fallback` | `string \| null` | no | `null` | Fallback source field if the primary is missing or null. |
| `string_map` | `object \| null` | no | `null` | For `mapped_string`: maps string values to integer IDs. |
| `doc_only` | `bool` | no | `false` | If `true`, stored in docstore only — not bitmap-indexed. Used for display fields like `url`, `hash`, `width`. |
| `truncate_u32` | `bool` | no | `false` | Cast the value to `u32` before storing. Useful for Unix timestamps that may exceed `u32::MAX`. |

**Value types:**

| Type | Input JSON | Output | Notes |
|------|-----------|--------|-------|
| `integer` | number | `Value::Integer(i64)` | Direct numeric passthrough. |
| `boolean` | bool | `Value::Bool` | Direct boolean passthrough. |
| `string` | string | `Value::String` | Typically used with `doc_only: true` since strings can't be bitmap-indexed. |
| `mapped_string` | string | `Value::Integer(i64)` | Looks up the string in `string_map` to get an integer. Unknown strings default to `0`. |
| `integer_array` | number[] | `FieldValue::Multi(Vec<Integer>)` | Each element becomes a separate bitmap entry. Used for `tagIds`, `modelVersionIds`, etc. |
| `exists_boolean` | any | `Value::Bool` | `true` if the source field exists and is non-null, `false` otherwise. Useful for computed booleans like `isPublished` (derived from `publishedAtUnix` existing). |

---

## Field relationship diagram

A field typically appears in all three sections:

```
data_schema.fields[].target ──→ config.filter_fields[].name  (bitmap-indexed for filtering)
                              ──→ config.sort_fields[].name   (bit-layer decomposed for sorting)
```

**Exception:** `doc_only: true` fields exist only in `data_schema` — they're stored in the document store for retrieval but never bitmap-indexed.

**Exception:** `exists_boolean` computed fields have a different `source` than `target`. For example, `source: "publishedAtUnix"` → `target: "isPublished"` checks whether the timestamp field exists and stores a boolean.

---

## Complete example

See [`data/indexes/civitai/config.json`](../data/indexes/civitai/config.json) for a production config indexing 105M Civitai image records with:
- 21 filter fields (9 single_value, 7 boolean, 5 multi_value)
- 6 sort fields (reactionCount, sortAt, commentCount, collectedCount, publishedAt, id)
- 32 field mappings including 6 doc-only fields, 3 mapped_string fields, and 3 exists_boolean computed fields

---

## Validation rules

The engine validates configs on load. Errors are returned as 400s from the create index API.

- `max_page_size` must be > 0
- `cache.decay_rate` must be in (0.0, 1.0]
- No duplicate filter field names
- No duplicate sort field names
- No empty field names
- Sort field `bits` must be 1–64
- `deferred_alive` is not allowed on `boolean` filter fields
- Bucket names must be unique within a field
- Bucket names must not be empty
- Bucket `duration_secs` and `refresh_interval_secs` must be > 0
