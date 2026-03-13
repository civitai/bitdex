# Schema Design Guide

Practical guidance for designing BitDex index schemas. Covers field type selection, computed fields, and performance trade-offs.

For the full config reference (all properties, types, defaults), see [config-schema.md](config-schema.md).

---

## Field type selection

### Filter fields

| Type | Use when | Bitmap cost | Query cost |
|------|----------|-------------|------------|
| `boolean` | Two-state (yes/no) | 2 bitmaps total | O(1) — single bitmap lookup |
| `single_value` | One value per document | 1 bitmap per distinct value | O(1) for `eq`/`not_eq`, O(distinct values) for range ops |
| `multi_value` | Array of values per document | 1 bitmap per distinct value | O(1) per value lookup |

**Choose `boolean`** when the field is truly two-state. Boolean fields always have exactly 2 bitmaps (keys `0` and `1`) regardless of document count.

**Choose `single_value`** for scalar fields (integers, mapped strings). Each distinct value gets one bitmap. Low-cardinality fields (e.g., `nsfwLevel` with ~30 values) are cheap. High-cardinality fields (e.g., `userId` with millions of values) cost more memory but `eq` lookups remain O(1).

**Choose `multi_value`** for array fields (e.g., `tagIds`). Same bitmap-per-value cost as `single_value`, but a document can appear in many bitmaps. Consider idle eviction (`eviction` config) for high-cardinality multi-value fields to control memory.

### Range queries on filter fields

Range operators (`gt`, `gte`, `lt`, `lte`) scan every bitmap in the field and union those matching the predicate. This is O(distinct values) — fast for low-cardinality fields, expensive for high-cardinality ones.

For timestamp range filters, use **time buckets** (`config.time_buckets`) to pre-compute common ranges (24h, 7d, 30d). The executor snaps range queries to the nearest bucket when within tolerance, turning an O(n) scan into an O(1) bitmap lookup.

---

## Computed fields

A **computed field** is a boolean or derived field that doesn't exist in the source data but is computed during ingestion. The `exists_boolean` value type is the most common example.

### When to use computed boolean fields

Use `exists_boolean` to create a boolean from a nullable field when:

1. **The source field is high-cardinality** and clients need to filter on existence (not a specific value)
2. **The "is null" / "is not null" check is a common query pattern**

**Why this matters**: Without a computed boolean, clients must express "field is not null" as a range query like `gte(field, 1)`. Range queries scan every distinct value bitmap in the field and union them — O(distinct values). A boolean lookup is always O(1).

The unified cache mitigates this on repeated queries (same filter+sort combination hits cache), but every **new filter combination** pays the full range scan cost. For a field with 500K+ distinct values, that's the difference between microseconds and hundreds of milliseconds on cache miss.

### Decision framework

| Source field cardinality | Clients filter on existence? | Recommendation |
|---|---|---|
| Low (< 100 distinct values) | Yes | Skip the boolean — range scan is cheap enough |
| Medium (100–10K) | Yes | Optional — boolean saves a few ms per cache miss |
| High (10K+ distinct values) | Yes | **Use a computed boolean** |
| Any | No | Don't add it — unused bitmaps waste memory |

### Example: `isRemix` from `remixOfId`

`remixOfId` is a foreign key to parent images — potentially millions of distinct values. Clients commonly filter "remixes only" or "non-remixes only."

Without `isRemix`, the client must do:
```
remixOfId gte 1        → scans all remixOfId bitmaps, unions matches
NOT(remixOfId gte 1)   → same scan, then complement against alive
```

With `isRemix`:
```
isRemix eq true    → single bitmap lookup
isRemix eq false   → single bitmap lookup
```

The schema mapping:
```json
{
  "source": "remixOfId",
  "target": "isRemix",
  "value_type": "exists_boolean"
}
```

Cost: one extra boolean field (2 bitmaps, negligible memory). Benefit: O(1) existence checks regardless of `remixOfId` cardinality, on every cache miss.

### Example: `isPublished` from `publishedAtUnix`

Same pattern — `publishedAtUnix` is a timestamp with millions of distinct values. Filtering "published only" is extremely common. The `exists_boolean` makes this a single bitmap lookup instead of scanning every distinct timestamp.

### When NOT to use computed fields

- **The source field is low-cardinality.** If `type` has 5 distinct values, `type gte 1` scans 5 bitmaps — fast enough without a boolean.
- **Nobody queries on existence.** Don't pre-compute what nobody uses.
- **An `eq` check on a specific value suffices.** If clients only ever check `status eq 3` (not "status exists"), you don't need a boolean.
- **The field is already boolean.** `hasMeta` is natively boolean in the source data — no computed field needed.

---

## Sort fields

Every sort field creates N bitmaps (one per bit layer). A 32-bit field = 32 bitmaps. These are used for MSB-to-LSB bitmap traversal to find top-K results.

**Only add sort fields that clients actually sort by.** Each one costs 32 bitmaps across the full document space. At 105M documents, a 32-bit sort field is ~100-200MB depending on value distribution.

**Use `truncate_u32`** for timestamps stored as milliseconds that exceed `u32::MAX`. The field mapping handles the cast:
```json
{ "source": "sortAtUnix", "target": "sortAt", "value_type": "integer", "ms_to_seconds": true }
```

---

## Doc-only fields

Fields with `doc_only: true` are stored in the document store but never bitmap-indexed. Use for display data (URLs, hashes, dimensions) that clients need in results but never filter or sort by.

Doc-only fields have zero bitmap memory cost. They only cost disk space in the docstore (zstd-compressed msgpack).

```json
{ "source": "url", "target": "url", "value_type": "string", "doc_only": true }
```

---

## Memory planning

Rough per-field memory costs at 105M documents:

| Field type | Memory per field | Notes |
|---|---|---|
| Boolean filter | ~25 MB | 2 bitmaps, roaring-compressed |
| Low-cardinality single_value (< 100 values) | ~50-200 MB | Depends on value distribution |
| High-cardinality single_value (100K+ values) | ~500 MB–2 GB | Each value bitmap is sparse |
| Multi-value (e.g., tagIds, 31K values) | ~5 GB | Dominates memory; consider idle eviction |
| Sort field (32-bit) | ~100-200 MB | 32 bitmaps |
| Doc-only | 0 MB (RAM) | Disk only |

See [benchmark-report.md](../benchmarks/benchmark-report.md) for measured values at 5M/50M/100M/105M scale.
