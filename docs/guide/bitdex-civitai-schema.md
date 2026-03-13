# Civitai Image Search - Full BitDex Schema

This document maps ALL Meilisearch filterable/sortable fields from the Civitai metrics-images search index to BitDex field definitions.

Source: `model-share/src/server/search-index/metrics-images.search-index.ts`

---

## 1. Field Usage Analysis (Questioned Fields)

### `existedAtUnix`
- **Indexed?** Yes - set to `new Date().getTime()` during indexing (timestamp of when the record was indexed)
- **Used in queries?** NO - the filter code is commented out in both `getImagesByCategory` and the main image query:
  ```ts
  //   filters.push(makeMeiliImageSearchFilter('existedAtUnix', `>= ${lastExistedAt}`));
  ```
- **Verdict:** Dead code. Was intended for incremental index reads but never activated. **Include anyway** for future use - it's a low-cost single_value field.

### `combinedNsfwLevel`
- **Indexed?** Yes - computed as `nsfwLevelLocked ? nsfwLevel : Math.max(nsfwLevel, aiNsfwLevel)`
- **Used in queries?** YES - actively used. The query builder selects between `nsfwLevel` and `combinedNsfwLevel` based on a flag:
  ```ts
  const nsfwLevelField = useCombinedNsfwLevel ? 'combinedNsfwLevel' : 'nsfwLevel';
  ```
- **Verdict:** Required. Same value space as `nsfwLevel` (bitmask: 1, 2, 4, 8, 16, 32).

### `flags.promptNsfw`
- **Indexed?** Yes - nested under `flags: { promptNsfw: boolean }` in the indexed document
- **Used in queries?** NO - declared as filterable in Meilisearch but no query code uses it as a filter
- **Verdict:** Include as boolean for future use. Note: in BitDex this becomes a flat field `promptNsfw` (no nested objects). The filter translator must map `flags.promptNsfw` to `promptNsfw`.

### `remixOfId`
- **Indexed?** Yes - extracted from `i."meta"->'extra'->'remixOfId'` in the SQL query
- **Used in queries?** YES - actively used in three ways:
  ```ts
  makeMeiliImageSearchFilter('remixOfId', `= ${remixOfId}`)    // exact match
  makeMeiliImageSearchFilter('remixOfId', '>= 0')              // "is a remix"
  makeMeiliImageSearchFilter('remixOfId', 'NOT EXISTS')        // "is not a remix"
  ```
- **Verdict:** Required. Note the `NOT EXISTS` / `>= 0` pattern - this needs special handling. Use `ExistsBoolean` for an `isRemix` helper field, plus `single_value` for exact ID filtering.

---

## 2. All Filter Fields

| # | Meilisearch Field | BitDex Field Name | BitDex Type | Value Type | Notes |
|---|---|---|---|---|---|
| 1 | `nsfwLevel` | `nsfwLevel` | `single_value` | Integer | Bitmask: 1 (PG), 2 (PG13), 4 (R), 8 (X), 16 (XXX), 32 (Blocked). ~6 distinct values. |
| 2 | `combinedNsfwLevel` | _(remapped to `nsfwLevel`)_ | — | — | **RETIRED.** Filter translator maps `combinedNsfwLevel` → `nsfwLevel` at query time. Not a separate BitDex field. |
| 3 | `userId` | `userId` | `single_value` | Integer | High cardinality (~millions of users). Sparse lookups ~34us. |
| 4 | `postId` | `postId` | `single_value` | Integer | High cardinality (one per post). |
| 5 | `postedToId` | `postedToId` | `single_value` | Integer | ModelVersion ID the image was posted to. Nullable. |
| 6 | `type` | `type` | `single_value` | MappedString | MediaType enum: "image"=1, "video"=2, "audio"=3. 3 values. |
| 7 | `baseModel` | `baseModel` | `single_value` | MappedString | Free-form string from Checkpoint model versions. ~30-50 distinct values. See string_map below. |
| 8 | `availability` | `availability` | `single_value` | MappedString | Enum: "Public"=1, "Unsearchable"=2, "Private"=3, "EarlyAccess"=4. |
| 9 | `blockedFor` | `blockedFor` | `single_value` | MappedString | BlockedReason enum: "tos"=1, "moderated"=2, "CSAM"=3, "AiNotVerified"=4. Nullable. |
| 10 | `remixOfId` | `remixOfId` | `single_value` | Integer | Nullable. The source image ID for remixes. |
| 11 | `sortAtUnix` | `sortAtUnix` | `single_value` | Integer | Unix timestamp (milliseconds). Used for time range filters. `truncate_u32: true` needed (ms timestamps exceed u32). |
| 12 | `publishedAtUnix` | `publishedAtUnix` | `single_value` | Integer | Unix timestamp (milliseconds). Nullable. `truncate_u32: true`. |
| 13 | `existedAtUnix` | `existedAtUnix` | `single_value` | Integer | Unix timestamp (milliseconds). Currently unused in queries but indexed. `truncate_u32: true`. |
| 14 | `hasMeta` | `hasMeta` | `boolean` | Boolean | True if image has non-null, non-hidden metadata. |
| 15 | `onSite` | `onSite` | `boolean` | Boolean | True if generated on Civitai. |
| 16 | `poi` | `poi` | `boolean` | Boolean | Person of interest flag. |
| 17 | `minor` | `minor` | `boolean` | Boolean | Minor flag. |
| 18 | `flags.promptNsfw` | _(dropped)_ | — | — | **EXCLUDED.** Never used in queries. Filter translator silently drops `promptNsfw` filters. |
| 19 | `tagIds` | `tagIds` | `multi_value` | IntegerArray | Array of tag IDs. HIGH CARDINALITY (~31K distinct values). 79-80% of filter bitmap memory. |
| 20 | `modelVersionIds` | `modelVersionIds` | `multi_value` | IntegerArray | Auto-detected model version IDs. |
| 21 | `modelVersionIdsManual` | `modelVersionIdsManual` | `multi_value` | IntegerArray | Manually tagged model version IDs. |
| 22 | `toolIds` | `toolIds` | `multi_value` | IntegerArray | Tool IDs used to create the image. |
| 23 | `techniqueIds` | `techniqueIds` | `multi_value` | IntegerArray | Technique IDs used. |
| 24 | `id` | (slot=id) | N/A | N/A | BitDex slot IS the Postgres ID. Filtering by ID is done via alive bitmap. Not a separate filter field. |

**Total: 21 filter fields** (excluding `id`, retired `combinedNsfwLevel`, and excluded `promptNsfw`)

---

## 3. All Sort Fields

| # | Meilisearch Field | BitDex Field Name | Bits | Source Type | Notes |
|---|---|---|---|---|---|
| 1 | `reactionCount` | `reactionCount` | 32 | uint32 | Sum of like, heart, laugh, cry reactions. |
| 2 | `sortAt` | `sortAt` | 32 | uint32 | `sortAtUnix` in the data, mapped to `sortAt`. Unix timestamp seconds (truncated from ms). |
| 3 | `commentCount` | `commentCount` | 32 | uint32 | Number of comments. |
| 4 | `collectedCount` | `collectedCount` | 32 | uint32 | Number of collections containing this image. |
| 5 | `id` | `id` | 32 | uint32 | Sortable in Meilisearch. BitDex uses slot=id so default slot ordering handles this, but we include it explicitly for sort-by-slot queries. |

---

## 4. `baseModel` String Map

The `baseModel` field is a free-form string from `ModelVersion.baseModel` (aggregated via `string_agg` for Checkpoint models). Known values:

```json
{
  "": 0,
  "SD 1.4": 1,
  "SD 1.5": 2,
  "SD 1.5 LCM": 3,
  "SD 1.5 Hyper": 4,
  "SD 2.0": 5,
  "SD 2.0 768": 6,
  "SD 2.1": 7,
  "SD 2.1 768": 8,
  "SD 2.1 Unclip": 9,
  "SD 3": 10,
  "SD 3.5": 11,
  "SD 3.5 Large": 12,
  "SD 3.5 Large Turbo": 13,
  "SD 3.5 Medium": 14,
  "SDXL 0.9": 15,
  "SDXL 1.0": 16,
  "SDXL 1.0 LCM": 17,
  "SDXL Distilled": 18,
  "SDXL Hyper": 19,
  "SDXL Lightning": 20,
  "SDXL Turbo": 21,
  "Pony": 22,
  "Flux.1 D": 23,
  "Flux.1 S": 24,
  "Flux.1 D2": 25,
  "AuraFlow": 26,
  "SVD": 27,
  "SVD XT": 28,
  "Stable Cascade": 29,
  "PixArt a": 30,
  "PixArt E": 31,
  "Hunyuan 1": 32,
  "Hunyuan 2": 33,
  "Lumina": 34,
  "Kolors": 35,
  "Mochi": 36,
  "LTX Video": 37,
  "CogVideoX": 38,
  "Illustrious": 39,
  "NoobAI": 40,
  "Other": 99
}
```

> **Note:** This map will need updating as new base models are added. Unknown values should map to 99 ("Other") as a fallback. The map can be extended without rebuilding the index.

---

## 5. Full DataSchema JSON

```json
{
  "id_field": "id",
  "fields": [
    {"source": "nsfwLevel", "target": "nsfwLevel", "value_type": "integer"},
    {"source": "userId", "target": "userId", "value_type": "integer"},
    {"source": "postId", "target": "postId", "value_type": "integer"},
    {"source": "postedToId", "target": "postedToId", "value_type": "integer"},
    {
      "source": "type",
      "target": "type",
      "value_type": "mapped_string",
      "string_map": {"image": 1, "video": 2, "audio": 3}
    },
    {
      "source": "baseModel",
      "target": "baseModel",
      "value_type": "mapped_string",
      "string_map": {
        "": 0, "SD 1.4": 1, "SD 1.5": 2, "SD 1.5 LCM": 3, "SD 1.5 Hyper": 4,
        "SD 2.0": 5, "SD 2.0 768": 6, "SD 2.1": 7, "SD 2.1 768": 8, "SD 2.1 Unclip": 9,
        "SD 3": 10, "SD 3.5": 11, "SD 3.5 Large": 12, "SD 3.5 Large Turbo": 13, "SD 3.5 Medium": 14,
        "SDXL 0.9": 15, "SDXL 1.0": 16, "SDXL 1.0 LCM": 17, "SDXL Distilled": 18,
        "SDXL Hyper": 19, "SDXL Lightning": 20, "SDXL Turbo": 21,
        "Pony": 22, "Flux.1 D": 23, "Flux.1 S": 24, "Flux.1 D2": 25,
        "AuraFlow": 26, "SVD": 27, "SVD XT": 28, "Stable Cascade": 29,
        "PixArt a": 30, "PixArt E": 31, "Hunyuan 1": 32, "Hunyuan 2": 33,
        "Lumina": 34, "Kolors": 35, "Mochi": 36, "LTX Video": 37, "CogVideoX": 38,
        "Illustrious": 39, "NoobAI": 40, "Other": 99
      }
    },
    {
      "source": "availability",
      "target": "availability",
      "value_type": "mapped_string",
      "string_map": {"Public": 1, "Unsearchable": 2, "Private": 3, "EarlyAccess": 4}
    },
    {
      "source": "blockedFor",
      "target": "blockedFor",
      "value_type": "mapped_string",
      "string_map": {"tos": 1, "moderated": 2, "CSAM": 3, "AiNotVerified": 4}
    },
    {"source": "remixOfId", "target": "remixOfId", "value_type": "integer"},
    {"source": "sortAtUnix", "target": "sortAtUnix", "value_type": "integer", "truncate_u32": true},
    {"source": "publishedAtUnix", "target": "publishedAtUnix", "value_type": "integer", "truncate_u32": true},
    {"source": "existedAtUnix", "target": "existedAtUnix", "value_type": "integer", "truncate_u32": true},
    {"source": "hasMeta", "target": "hasMeta", "value_type": "boolean"},
    {"source": "onSite", "target": "onSite", "value_type": "boolean"},
    {"source": "poi", "target": "poi", "value_type": "boolean"},
    {"source": "minor", "target": "minor", "value_type": "boolean"},
    {"source": "tagIds", "target": "tagIds", "value_type": "integer_array"},
    {"source": "modelVersionIds", "target": "modelVersionIds", "value_type": "integer_array"},
    {"source": "modelVersionIdsManual", "target": "modelVersionIdsManual", "value_type": "integer_array"},
    {"source": "toolIds", "target": "toolIds", "value_type": "integer_array"},
    {"source": "techniqueIds", "target": "techniqueIds", "value_type": "integer_array"},
    {"source": "reactionCount", "target": "reactionCount", "value_type": "integer"},
    {"source": "commentCount", "target": "commentCount", "value_type": "integer"},
    {"source": "collectedCount", "target": "collectedCount", "value_type": "integer"},
    {"source": "sortAtUnix", "target": "sortAt", "value_type": "integer", "truncate_u32": true},
    {"source": "url", "target": "url", "value_type": "string", "doc_only": true},
    {"source": "hash", "target": "hash", "value_type": "string", "doc_only": true},
    {"source": "width", "target": "width", "value_type": "integer", "doc_only": true},
    {"source": "height", "target": "height", "value_type": "integer", "doc_only": true}
  ]
}
```

### Notes on DataSchema

- **`combinedNsfwLevel`**: Retired — filter translator remaps `combinedNsfwLevel` queries to `nsfwLevel` at query time. Not indexed in BitDex.
- **`promptNsfw`**: Excluded — never used in queries. Filter translator silently drops `promptNsfw` / `flags.promptNsfw` filters.
- **`sortAt` vs `sortAtUnix`**: The source field is `sortAtUnix` (milliseconds). Two mappings exist: one for the filter field `sortAtUnix` and one for the sort field `sortAt` (both truncated to u32 seconds).
- **`truncate_u32`**: Millisecond timestamps exceed u32::MAX. The loader truncates by dividing by 1000 (seconds) then clamping to u32.
- **`doc_only` fields**: `url`, `hash`, `width`, `height` are stored in the docstore for serving alongside query results but are not bitmap-indexed.
- **`id`**: Not in the field list because BitDex slot = Postgres ID (no mapping needed).

---

## 6. Full Config JSON

```json
{
  "max_page_size": 100,
  "autovac_interval_secs": 3600,
  "merge_interval_ms": 5000,
  "flush_interval_us": 100,
  "channel_capacity": 100000,
  "cache": {
    "max_entries": 10000,
    "decay_rate": 0.95,
    "bound_target_size": 10000,
    "bound_max_size": 20000,
    "bound_max_count": 100
  },
  "filter_fields": [
    {"name": "nsfwLevel", "field_type": "single_value"},
    {"name": "userId", "field_type": "single_value"},
    {"name": "postId", "field_type": "single_value"},
    {"name": "postedToId", "field_type": "single_value"},
    {"name": "type", "field_type": "single_value"},
    {"name": "baseModel", "field_type": "single_value"},
    {"name": "availability", "field_type": "single_value"},
    {"name": "blockedFor", "field_type": "single_value"},
    {"name": "remixOfId", "field_type": "single_value"},
    {"name": "sortAtUnix", "field_type": "single_value"},
    {"name": "publishedAtUnix", "field_type": "single_value"},
    {"name": "existedAtUnix", "field_type": "single_value"},
    {"name": "hasMeta", "field_type": "boolean"},
    {"name": "onSite", "field_type": "boolean"},
    {"name": "poi", "field_type": "boolean"},
    {"name": "minor", "field_type": "boolean"},
    {"name": "tagIds", "field_type": "multi_value"},
    {"name": "modelVersionIds", "field_type": "multi_value"},
    {"name": "modelVersionIdsManual", "field_type": "multi_value"},
    {"name": "toolIds", "field_type": "multi_value"},
    {"name": "techniqueIds", "field_type": "multi_value"}
  ],
  "sort_fields": [
    {"name": "reactionCount", "source_type": "uint32", "encoding": "linear", "bits": 32},
    {"name": "sortAt", "source_type": "uint32", "encoding": "linear", "bits": 32},
    {"name": "commentCount", "source_type": "uint32", "encoding": "linear", "bits": 32},
    {"name": "collectedCount", "source_type": "uint32", "encoding": "linear", "bits": 32},
    {"name": "id", "source_type": "uint32", "encoding": "linear", "bits": 32}
  ],
  "storage": {
    "bitmap_path": "./data/bitmaps"
  }
}
```

---

## 7. Fields Added vs Current Benchmark Config

The benchmark (`src/bin/benchmark.rs:civitai_config()`) currently has 11 filter fields and 5 sort fields. This schema adds:

**New filter fields (12 additions):**
- `combinedNsfwLevel` - actively used for NSFW filtering
- `postId` - high cardinality single_value
- `postedToId` - model version the image was posted to
- `baseModel` - mapped string, ~40 values
- `availability` - mapped string, 4 values
- `blockedFor` - mapped string, 4 values
- `remixOfId` - nullable integer
- `sortAtUnix` - timestamp for range filtering
- `publishedAtUnix` - timestamp, nullable
- `existedAtUnix` - timestamp (currently unused in queries)
- `promptNsfw` - boolean (currently unused in queries)
- `modelVersionIdsManual` - multi_value integer array

**Sort fields:** No changes (already has all 5).

---

## 8. Memory Impact Estimate

Most new filter fields are low-cardinality (booleans, mapped strings with <10 values, timestamps). The high-cardinality additions are:
- `postId`: ~100M distinct values. Each gets a tiny bitmap (1-few bits). Roaring stores these efficiently as sparse bitmaps. Estimate: ~400 MB at 105M records.
- `userId`: Already included. ~1-5M distinct values.
- `postedToId`: Similar to postId but nullable. Many images share a postedToId. Estimate: ~100-200 MB.
- `remixOfId`: Very sparse (most images are not remixes). Estimate: <50 MB.
- `modelVersionIdsManual`: Much smaller than `modelVersionIds`. Estimate: <100 MB.

**Total estimated memory increase:** ~600-800 MB over current 6.5 GB baseline. New total: ~7.1-7.3 GB bitmap memory at 105M records.
