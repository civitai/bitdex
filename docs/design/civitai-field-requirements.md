---
status: APPROVED
created: 2026-03-28
source: Donovan (model-share), documented by Tom (CTO) via Dakota (Doc Keeper)
---

# Civitai Field Requirements

> Ground truth for what BitDex must serve for the Civitai images feed. Every filter, sort, and document field required by the website, with types and usage patterns.

---

## Filter Fields (22)

These fields must have bitmap indexes for query filtering.

| Field | Type | Operators | Source | Notes |
|-------|------|-----------|--------|-------|
| `nsfwLevel` | int | In | images.csv | Content rating levels |
| `availability` | string (LCS) | NotEq | Post enrichment | Derived from Post table |
| `blockedFor` | string (LCS) | NotIn, In | images.csv | Geo/legal blocking |
| `isPublished` | bool | Eq | Post enrichment | Computed: `publishedAt != null` |
| `postId` | int | Eq, In, NotEq | images.csv | Post grouping |
| `userId` | int | Eq, NotIn | images.csv | Creator filtering |
| `type` | string (LCS) | In | images.csv | image, video, audio |
| `tagIds` | int[] | In, NotIn | tags.csv | **Must be in docstore** (post-query merge filter) |
| `toolIds` | int[] | In | tools.csv | Can be filter_only |
| `techniqueIds` | int[] | In | techniques.csv | Can be filter_only |
| `baseModel` | string (LCS) | In | Resource enrichment | Only from Checkpoint model types |
| `modelVersionIds` | int[] | In | resources.csv | **Must be in docstore** (post-query merge filter) |
| `modelVersionIdsManual` | int[] | In | resources.csv | `detected=false` resources. Can be filter_only |
| `postedToId` | int | Eq, Or | Post enrichment | `Post.modelVersionId` — the model version this post is attached to (nullable) |
| `hasMeta` | bool | Eq | Computed | `(flags >> 13) & 1 == 1 && (flags >> 2) & 1 == 0` |
| `onSite` | bool | Eq | images.csv | |
| `poi` | bool | Eq, Not | Model enrichment | Person of interest flag |
| `minor` | bool | Eq, Not | images.csv | |
| `isRemix` | bool | Eq | images.csv | |
| `remixOfId` | int | Eq | images.csv | |
| `collectionIds` | int[] | — | **Deferred** | Falls back to PG query. Not in BitDex. |
| `sortAtUnix` | int | Gte | Computed | Time bucket filtering |

### filter_only Decision (Critical for Data Silos)

| Field | filter_only? | Why |
|-------|-------------|-----|
| `tagIds` | **NO** | Used in post-query merge filter — website reads tagIds from doc response |
| `modelVersionIds` | **NO** | Used in post-query merge filter — website reads from doc response |
| `toolIds` | YES | Only used for bitmap filtering, never read from documents |
| `techniqueIds` | YES | Only used for bitmap filtering, never read from documents |
| `modelVersionIdsManual` | YES | Only used for bitmap filtering, never read from documents |

**Impact:** tagIds in docstore = 5.4B rows at 3-5M/s instead of 55M/s (skipped). This is the primary motivation for the data silo architecture — per-thread large files eliminate the filesystem metadata bottleneck that makes multi-value docstore writes so expensive.

---

## Sort Fields (5)

These fields use bit-layer bitmap decomposition for MSB-to-LSB top-N retrieval.

| Field | Type | UI Sort Option | Notes |
|-------|------|---------------|-------|
| `sortAt` | u32 (seconds) | "Newest" (default) | Computed: `GREATEST(existedAt, publishedAt)` |
| `reactionCount` | u32 | "Most Reactions" | From ClickHouse metrics |
| `commentCount` | u32 | "Most Comments" | From ClickHouse metrics |
| `collectedCount` | u32 | "Most Collected" | From ClickHouse metrics |
| `tippedAmountCount` | u32 | "Most Tipped" | From ClickHouse metrics. Low priority — skippable. |

### Computed Sort Fields

| Field | Expression | Source Fields |
|-------|-----------|---------------|
| `existedAt` | `GREATEST(scannedAt, createdAt)` | images.csv |
| `sortAt` | `GREATEST(existedAt, publishedAt)` | existedAt (computed) + publishedAt (Post enrichment) |

---

## Document Fields (30)

These fields must be returned in document responses for image card rendering.

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `id` | i64 | Slot ID | = Postgres image ID |
| `url` | string | images.csv | Civitai CDN image URL |
| `hash` | string | images.csv | BlurHash for placeholder |
| `nsfwLevel` | int | images.csv | |
| `userId` | int | images.csv | |
| `type` | string | images.csv | |
| `availability` | string | Post enrichment | |
| `baseModel` | string | Resource enrichment | |
| `postId` | int | images.csv | |
| `postedToId` | int | Post enrichment | |
| `remixOfId` | int | images.csv | |
| `hasMeta` | bool | Computed | |
| `onSite` | bool | images.csv | |
| `poi` | bool | Model enrichment | |
| `minor` | bool | images.csv | |
| `width` | int | images.csv | Image dimensions |
| `height` | int | images.csv | Image dimensions |
| `needsReview` | string | images.csv | Moderation status |
| `reactionCount` | int | ClickHouse metrics | |
| `commentCount` | int | ClickHouse metrics | |
| `collectedCount` | int | ClickHouse metrics | |
| `sortAt` | int | Computed | |
| `publishedAt` | int | Post enrichment | |
| `tagIds` | int[] | tags.csv | **Required in doc response** |
| `modelVersionIds` | int[] | resources.csv | **Required in doc response** |
| `toolIds` | int[] | tools.csv | |
| `techniqueIds` | int[] | techniques.csv | |
| `blockedFor` | string | images.csv | |
| `index` | int | images.csv | Position within post |
| `acceptableMinor` | bool | images.csv | |

---

## Fields That Fall Back to PG

These fields are NOT in BitDex. The model-share API falls back to Postgres for them:

| Field | Why Not in BitDex |
|-------|------------------|
| `reactions` | Per-user reaction state — too dynamic, per-viewer |
| `collections` | `collectionIds` was deferred — complex join table |
| `prioritizedUserIds` | Per-request dynamic list |
| `stats` | Aggregated stats — served from ClickHouse or PG |

---

## Enrichment Chain Summary

How fields are populated during dump processing:

```
images.csv (direct fields)
    ↓ enrich with posts.csv (join on postId)
    │   → publishedAt, availability, isPublished, postedToId
    │
    ↓ enrich with resources.csv (join on imageId → modelVersionId)
    │   ↓ enrich with model_versions.csv (join on modelVersionId)
    │   │   ↓ enrich with models.csv (join on modelId)
    │   │       → baseModel (only if model.type == "Checkpoint")
    │   │       → poi (from model)
    │   → modelVersionIds, modelVersionIdsManual (detected=false)
    │
tags.csv (multi-value, join on imageId → tagIds)
tools.csv (multi-value, join on imageId → toolIds)
techniques.csv (multi-value, join on imageId → techniqueIds)
metrics.tsv (ClickHouse → reactionCount, commentCount, collectedCount)
```

---

## Why This Matters for Data Silos

The `tagIds` and `modelVersionIds` fields create a tension:
- They MUST be in docstore (website reads them from document responses)
- But writing them to docstore during bulk load is devastatingly slow (5.4B tag rows at 3-5M/s vs 55M/s with filter_only)

The data silo architecture (`docs/design/data-silo-architecture.md`) solves this: per-thread large files with zero contention achieve 20.1M rows/s, making it feasible to include tagIds in docstore without the 10x write penalty.
