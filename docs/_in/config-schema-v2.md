# BitDex Index Configuration Schema

An index config is a JSON file with three top-level sections: **name**, **config** (engine configuration), and **data_schema** (field mapping and document storage rules).

```
POST /api/indexes
```

```json
{
  \"name\": \"civitai\",
  \"config\": { ... },
  \"data_schema\": { ... }
}
```

---

## `name`

**Type:** `string`

Unique identifier for the index. Used in API paths (`/api/indexes/{name}/query`) and as the directory name under `data_dir/indexes/`.

---

## Storage Architecture

BitDex uses a fully filesystem-based storage model. There is no embedded database engine. All data lives as files on disk, memory-mapped by the OS for zero-copy reads. The OS page cache automatically manages what stays in physical RAM based on access patterns — no application-level cache management is needed for source bitmaps.

**Directory layout:**

```
{data_dir}/indexes/{name}/
  bitmaps/
    filter/
      {field_name}/
        {value_id}.roar       # one roaring bitmap per distinct value
    sort/
      {field_name}/
        bit{00..31}.roar      # one file per bit layer
    system/
      alive.roar              # active document slots
      clean.roar              # recycled slots available for reuse
    cache/
      filters/
        {hash}.roar           # computed filter result bitmaps
      bounds/
        {hash}.roar           # computed top-K sort acceleration bitmaps
      meta/
        cache_stats.bin       # hit counts, generation counters
  docs/
    {xx}/
      {yy}.bin                # msgpack+zstd documents, sharded by slot ID hex
  dictionaries/
    {field_name}.dict         # auto-built string→integer mappings
  meta/
    slot_counter              # atomic monotonic counter
    field_definitions.yaml    # resolved schema with defaults
    wal/                      # write-ahead log for crash recovery
      {sequence}.wal
  schema/
    v{n}.yaml                 # schema version history for doc decoding
```

Bitmap files are written atomically: write to `{name}.tmp`, fsync, rename over the existing file. Rename is atomic on all POSIX filesystems — readers always see either the old or new complete version, never a partial write.

---

## `config`

Engine configuration. Controls what fields are indexed, how queries are sorted, document storage behavior, and tuning parameters.

### `config.filter_fields`

**Type:** `FilterFieldConfig[]`

Each entry creates bitmap indexes for one field. Every distinct value gets its own roaring bitmap stored as a separate file.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `name` | `string` | yes | — | Field name. Must match a `target` in `data_schema.fields`. Must be unique. |
| `field_type` | `enum` | yes | — | `\"single_value\"`, `\"multi_value\"`, or `\"boolean\"` |
| `behaviors` | `object \\| null` | no | `null` | Time-related behaviors (see below) |

**Field types:**

- **`single_value`** — One integer value per document (e.g., `nsfwLevel`, `userId`). Creates one bitmap per distinct value.
- **`multi_value`** — Array of integers per document (e.g., `tagIds`, `modelVersionIds`). Creates one bitmap per distinct value; a document appears in all bitmaps for its values.
- **`boolean`** — True/false. Creates two bitmaps: value `1` (true) and value `0` (false). Allows both `Eq` and `NotEq` queries.

Note: The `storage: snapshot/cached` tier split has been removed. All source bitmaps are memory-mapped files. The OS page cache manages which bitmaps are in physical RAM transparently. Hot bitmaps stay warm automatically; cold bitmaps are loaded on demand via page faults at NVMe speed (~0.5-1ms).

**`behaviors` (optional):**

For timestamp fields only.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `deferred_alive` | `bool` | `false` | If true, documents with future values in this field are hidden (alive bit not set) until the timestamp arrives. A background timer checks pending documents and flips their alive bit when the time passes. Not valid on `boolean` fields. |
| `range_buckets` | `BucketConfig[]` | `[]` | Pre-computed time range bitmaps. Bucket names are discrete cache keys — queries snapping to a bucket name avoid cache key fragmentation from rolling timestamps. |

**`BucketConfig`:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | `string` | Identifier used in cache keys (e.g., `\"24h\"`, `\"7d\"`, `\"30d\"`). Must be unique within the field. |
| `duration_secs` | `u64` | Bucket duration in seconds. Must be > 0. |
| `refresh_interval_secs` | `u64` | How often to rebuild this bucket's bitmap. Must be > 0. |

Bucket bitmaps are stored in `bitmaps/filter/{field_name}/bucket_{name}.roar` and refreshed in place on their interval. Cache entries keyed by bucket name automatically see the refreshed bitmap on next access — no invalidation needed.

---

### `config.sort_fields`

**Type:** `SortFieldConfig[]`

Each entry creates N bitmap layers for bit-layer sort traversal. Each layer is stored as a separate file (`bit00.roar` through `bit31.roar`). Sort traversal ANDs the working set against layers from most to least significant bit, narrowing the candidate set at each step without materializing a full sorted copy.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `name` | `string` | yes | — | Field name. Must match a `target` in `data_schema.fields`. |
| `source_type` | `string` | no | `\"uint32\"` | Source numeric type. Determines default bit depth. Supported: `\"uint8\"`, `\"uint16\"`, `\"uint32\"`, `\"uint64\"`. |
| `encoding` | `string` | no | `\"linear\"` | `\"linear\"` = raw bit layers (full precision). `\"log\"` = log2-encoded layers (reduces bit depth for power-law fields like reaction counts, spreads value distribution evenly across layers). |
| `bits` | `u8` | no | full bit width of source_type | Number of bitmap layers. Must be 1–64. Reducing bits reduces precision but improves sort performance by requiring fewer layer intersections. Start with the default and reduce only if benchmarks show sort latency is a bottleneck. |

**Encoding guidance:**

- `\"linear\"` for fields with uniform or unpredictable distributions (dates, IDs).
- `\"log\"` for power-law fields where most values cluster near zero (reaction counts, comment counts). Log encoding ensures every bit layer meaningfully narrows the working set instead of the top layers being nearly empty.

---

### `config.max_page_size`

**Type:** `usize` — **Default:** `100`

Hard cap on the `limit` parameter in queries.

---

### `config.cache`

Controls the trie filter cache and bound cache system. Both cache types store computed result bitmaps on disk alongside source bitmaps. Persisting caches to disk means the system is warm immediately after restart — no cold start penalty.

**Cache lifecycle:**
- **Filter caches**: created when a filter combination is queried `filter_promotion_threshold` times. Maintained live — writes use the meta-index to identify relevant caches and update bits directly without scanning all caches.
- **Bound caches**: created immediately on first query of any filter+sort combination (`bound_promotion_threshold: 0`). Maintained live — when a document's sort value crosses the bound's minimum threshold, its bit is set. Bits are never removed; the bound grows slowly over time.
- **Eviction**: a periodic GC pass checks hit rates (exponential decay). Caches below `gc_min_hit_rate` are deleted from disk. Memory is managed by the OS page cache — no application-level memory budget needed.
- **Bloat control**: when a bound's cardinality exceeds `bound_max_size`, it is rebuilt from a fresh sort traversal at the next query, producing a tight set of `bound_target_size` candidates.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `filter_promotion_threshold` | `usize` | `10` | Queries before a filter combination gets a cached bitmap. |
| `bound_promotion_threshold` | `usize` | `0` | Queries before a filter+sort combination gets a bound bitmap. Default 0 = immediate. Bound bitmaps are small and the OS manages memory. |
| `bound_target_size` | `usize` | `10000` | Target cardinality when building or rebuilding a bound bitmap. |
| `bound_max_size` | `usize` | `20000` | Cardinality at which a bound is flagged for rebuild. |
| `gc_interval_secs` | `u64` | `3600` | How often to scan for and delete unused cache bitmaps. |
| `gc_min_hit_rate` | `f64` | `0.01` | Hit rate below which a cache entry is deleted. Uses exponential decay — recent hits count more than old ones. |
| `decay_rate` | `f64` | `0.95` | Exponential decay multiplier applied to hit counts each GC cycle. Must be in `(0.0, 1.0]`. |

---

### `config.storage`

Filesystem storage and write pipeline settings.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `data_dir` | `string` | `\"./data\"` | Root directory for all index data. |
| `doc_store` | `DocStoreConfig` | see below | Document storage settings. |
| `wal_sync` | `enum` | `\"fsync\"` | WAL durability. `\"fsync\"` = flush every commit. `\"async\"` = background flush (faster, small data loss window on crash). |

**`DocStoreConfig`:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `enabled` | `bool` | `true` | Store full documents alongside bitmaps. When enabled, PATCH operations use stored docs to diff old vs new values without requiring callers to supply old values. When disabled, Bitdex returns IDs only and callers manage document storage externally. |
| `format` | `enum` | `\"msgpack\"` | Serialization format. `\"msgpack\"` (recommended) or `\"json\"`. |
| `compression` | `enum` | `\"zstd\"` | Compression. `\"zstd\"` (recommended) or `\"none\"`. |
| `shard_depth` | `u8` | `2` | Directory sharding depth by hex slot ID. `2` = `/docs/1B/82.bin`. Prevents large flat directories. |

---

### `config.backpressure`

The write pipeline automatically adjusts batch sizes, publish frequency, and flush behavior based on mutation channel depth. No manual mode switching — the system detects bulk load vs steady state and tunes itself.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `low_watermark` | `usize` | `1000` | Below this channel depth: steady state behavior. Small batches, frequent ArcSwap publishes, per-cycle redb commits. |
| `high_watermark` | `usize` | `50000` | Above this channel depth: burst mode. Large batches, infrequent publishes, large filesystem write batches. Between watermarks, behavior interpolates. |
| `publish_interval_steady_ms` | `u64` | `100` | ArcSwap publish interval in steady state. |
| `publish_interval_burst_ms` | `u64` | `1000` | ArcSwap publish interval under burst load. |
| `merge_interval_steady_ms` | `u64` | `5000` | Diff merge + disk write interval in steady state. |
| `merge_interval_burst_ms` | `u64` | `2000` | Diff merge interval under burst load. More frequent merges prevent diff bloat during bulk ingestion. |
| `batch_size_steady` | `usize` | `1000` | Mutations drained per flush cycle in steady state. |
| `batch_size_burst` | `usize` | `50000` | Mutations drained per flush cycle under burst load. |

---

### Operational tuning

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `autovac_interval_secs` | `u64` | `3600` | Autovacuum interval. Autovac finds dead slots (not alive, not clean), clears their bits from all bitmaps, and marks them as clean for slot recycling. *Phase 4.* |
| `prometheus_port` | `u16` | `9090` | Prometheus metrics port. *Phase 4.* |
| `flush_interval_us` | `u64` | `100` | Flush thread wakeup interval. |
| `channel_capacity` | `usize` | `100000` | Bounded write coalescer channel capacity. |

---

## `data_schema`

Describes how raw NDJSON records map to engine documents during bulk loading, and how documents are encoded for storage. These rules apply to both bulk backfill and real-time ingestion.

### `data_schema.id_field`

**Type:** `string`

Name of the JSON field containing the document's unique integer ID. This value is used directly as the bitmap slot ID. Documents are stored at `docs/{hex_shard}/{slot_id}.bin`.

### `data_schema.schema_version`

**Type:** `u8` — **Default:** `1`

Current schema version. Increment when changing field defaults. Old documents encoded with previous schema versions are decoded using the historical schema stored in `meta/schema/v{n}.yaml`. Docs are lazily migrated to the current schema version on next write. This allows default values to change over time without requiring a full reindex.

### `data_schema.fields`

**Type:** `FieldMapping[]`

Each entry maps one field from raw JSON to the engine document.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `source` | `string` | yes | — | Field name in raw JSON. |
| `target` | `string` | yes | — | Field name in engine document. Must match a filter or sort field name unless `doc_only: true`. |
| `value_type` | `enum` | yes | — | How to interpret and store the value (see below). |
| `default` | `any \\| null` | no | `null` | Default value. If the document's value equals the default, the field is omitted from the stored document to save space. On read, missing fields are reconstructed from the schema default. |
| `storage_type` | `string` | no | inferred | Explicit storage type hint: `\"u8\"`, `\"u16\"`, `\"u32\"`, `\"u64\"`, `\"i64\"`, `\"bool\"`. Overrides the inferred type to reduce storage size where you know the value range. |
| `fallback` | `string \\| null` | no | `null` | Fallback source field if primary is missing or null. |
| `doc_only` | `bool` | no | `false` | Stored in docstore only — not bitmap-indexed. Used for display fields like `url`, `hash`, `width`, `height`. |
| `truncate_u32` | `bool` | no | `false` | Cast value to `u32` before storing. Useful for millisecond timestamps being stored as second-precision. |

**Value types:**

| Type | Input JSON | Output | Notes |
|------|-----------|--------|-------|
| `integer` | number | `Value::Integer` | Direct numeric passthrough. Use `storage_type` to control byte width. |
| `boolean` | bool | `Value::Bool` | Direct boolean. |
| `string` | string | `Value::String` | Typically `doc_only: true` — strings can't be bitmap-indexed. |
| `low_cardinality_string` | string | `Value::Integer` | Engine auto-builds a string→integer dictionary per field. New values are assigned the next available ID. Use `cardinality` to control storage width. |
| `integer_array` | number[] | `FieldValue::Multi` | Each element becomes a separate bitmap entry. Used for `tagIds`, `modelVersionIds`, etc. Default `[]` elides the field when empty. |
| `exists_boolean` | any | `Value::Bool` | `true` if field exists and is non-null, `false` otherwise. Useful for computed fields like `isPublished`. |

**`low_cardinality_string` settings:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `cardinality` | `string` | `\"u8\"` | Storage width for dictionary IDs. `\"u8\"` = max 255 values (ID 0 reserved for null). `\"u16\"` = max 65535 values. `\"u32\"` = max ~4B values. Choose based on expected distinct value count. Engine emits an error if a new value would exceed the cardinality limit — operator must promote the field type and trigger a reindex. |

Dictionaries are stored in `dictionaries/{field_name}.dict` and loaded fully into memory on startup. They are small (hundreds of entries at most for truly low-cardinality fields).

---

## Document encoding

Documents are stored with default elision and schema versioning to minimize storage size.

**Encoding rules:**
1. Fields equal to their schema default are omitted entirely.
2. Low cardinality strings are stored as their dictionary integer ID, not the string.
3. Timestamps are stored as `u32` unix seconds (not milliseconds) unless `storage_type` specifies otherwise.
4. The schema version byte is prepended to every stored document.
5. The resulting struct is serialized as msgpack then compressed with zstd.

**Example:** For the following document and schema defaults:

```json
{
  \"commentCount\": 0,       // default 0 → elided
  \"collectedCount\": 0,     // default 0 → elided
  \"hideMeta\": false,       // default false → elided
  \"needsReview\": null,     // default null → elided
  \"onSite\": false,         // default false → elided
  \"toolIds\": [],           // default [] → elided
  \"techniqueIds\": [],      // default [] → elided
  \"minor\": false,          // default false → elided
  \"blockedFor\": null,      // default null → elided
  \"remixOfId\": null,       // default null → elided
  \"poi\": false,            // default false → elided
  \"acceptableMinor\": false,// default false → elided
  \"hasPositivePrompt\": true,// default true → elided
  \"availability\": \"Public\",// default \"Public\" → elided
  \"type\": \"image\"          // low_cardinality_string → stored as u8 ID
}
```

Roughly 12-14 fields are elided from a typical document, reducing storage by 30-40% before compression. Combined with msgpack serialization and zstd compression, expected storage is approximately 100-150 bytes per document versus ~600 bytes in raw NDJSON.

**Estimated total storage for 104M documents:**
- Documents: ~104M × 130 bytes avg = ~13GB
- Bitmaps: ~7-9GB
- **Total: ~20-22GB** (vs 61GB raw NDJSON, vs 120GB in redb with JSON)

---

## Field relationship diagram

```
data_schema.fields[].target ──→ config.filter_fields[].name  (bitmap-indexed for filtering)
                              ──→ config.sort_fields[].name   (bit-layer decomposed for sorting)
                              ──→ doc_only: true              (stored in docstore only, not indexed)
```

Computed fields (`exists_boolean`) have a different `source` than `target`:
```
source: \"publishedAtUnix\" → target: \"isPublished\"   (exists_boolean → bool filter field)
```

---

## Validation rules

- `max_page_size` must be > 0
- `cache.decay_rate` must be in `(0.0, 1.0]`
- `cache.gc_min_hit_rate` must be in `(0.0, 1.0]`
- No duplicate filter field names
- No duplicate sort field names
- No empty field names
- Sort field `bits` must be 1–64
- `deferred_alive` is not allowed on `boolean` filter fields
- Bucket names must be unique within a field
- Bucket `duration_secs` and `refresh_interval_secs` must be > 0
- `low_cardinality_string` fields must have `cardinality` of `\"u8\"`, `\"u16\"`, or `\"u32\"`
- `schema_version` must be >= 1
- `backpressure.low_watermark` must be < `high_watermark`
- `shard_depth` must be 1–4
- Sort field `encoding: \"log\"` is accepted but reserved — has no effect until Phase D

---

## Complete example

See [`data/indexes/civitai/config.json`](../data/indexes/civitai/config.json) for a production config indexing 105M Civitai image records with:
- 21 filter fields (9 single_value, 7 boolean, 5 multi_value)
- 6 sort fields (reactionCount, sortAt, commentCount, collectedCount, publishedAt, id)
- 32 field mappings including 6 doc-only fields, 3 low_cardinality_string fields, and 3 exists_boolean computed fields
- Default elision covering ~13 fields per document
- Backpressure tuned for 104M record backfill in under 10 minutes
