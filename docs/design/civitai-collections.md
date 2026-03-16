# Civitai Collections in BitDex

**Status:** Implemented (branch `feature/collection-ids-filter-only`)
**Date:** 2026-03-16
**Depends on:** [filter-only-fields.md](filter-only-fields.md)

## Problem

Civitai users organize images into collections. The gallery needs to filter images by collection membership: "show me all images in collection 12345, sorted by reactionCount." This is a many-to-many relationship stored in the `CollectionItem` table, separate from the `Image` table.

**Scale:** 14M collections, 157M collection items. Most collections have fewer than 100 items; rare outliers reach 44K+.

## Data Model

### Postgres Schema

```
CollectionItem
  collectionId  INT       -- FK to Collection
  imageId       INT NULL  -- FK to Image (nullable: collections can hold articles, posts, models, tags)
  status        ENUM      -- ACCEPTED, PENDING, REVIEW, REJECTED
  ...
```

Key constraints:
- `imageId` is nullable — collections can reference non-image entities.
- Only `status = 'ACCEPTED'` items should appear in BitDex.
- One image can belong to many collections; one collection can hold many images.

### BitDex Config

```json
// filter_fields
{ "name": "collectionIds", "field_type": "multi_value" }

// data_schema fields
{
  "source": "collectionIds",
  "target": "collectionIds",
  "value_type": "integer_array",
  "filter_only": true,
  "default": []
}
```

`filter_only: true` means collectionIds are bitmap-indexed but not stored in the docstore. The membership data comes from the CollectionItem table, not the Image row. Storing it in the docstore would waste space and create a consistency problem (two sources of truth).

## Query Example

```json
POST /api/indexes/civitai/query
{
  "filter": { "AND": [
    { "Eq": { "field": "collectionIds", "value": 12345 } },
    { "Eq": { "field": "nsfwLevel", "value": 1 } }
  ]},
  "sort": { "field": "reactionCount", "direction": "Desc" },
  "limit": 50
}
```

This intersects the collectionId=12345 bitmap with nsfwLevel=1, then does bit-layer sort traversal on reactionCount. Same pattern as any other multi-value filter.

Lazy per-value loading ensures only queried collection bitmaps are loaded into memory. The existence set provides instant rejection (<22us) for non-existent collection IDs.

## Postgres Trigger

A dedicated trigger function fires on CollectionItem INSERT and DELETE:

```sql
CREATE OR REPLACE FUNCTION bitdex_collection_notify() RETURNS trigger AS $$
DECLARE
  _image_id BIGINT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    _image_id := OLD."imageId";
  ELSIF TG_OP = 'UPDATE' THEN
    -- Only fire when accepted-ness changes (REVIEW→ACCEPTED or ACCEPTED→REJECTED)
    IF (OLD.status = 'ACCEPTED') = (NEW.status = 'ACCEPTED') THEN
      RETURN NEW;
    END IF;
    _image_id := NEW."imageId";
  ELSE
    _image_id := NEW."imageId";
  END IF;
  -- Only fire for image collections (imageId is nullable)
  IF _image_id IS NOT NULL THEN
    INSERT INTO "BitdexOutbox" (entity_type, entity_id, event)
      VALUES ('Image', _image_id, 'UPSERT');
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER bitdex_collection_trg
  AFTER INSERT OR UPDATE OR DELETE ON "CollectionItem"
  FOR EACH ROW EXECUTE FUNCTION bitdex_collection_notify();
ALTER TABLE "CollectionItem" ENABLE ALWAYS TRIGGER bitdex_collection_trg;
```

**Design decisions:**
- Separate function (not reusing `bitdex_image_notify`) because of the NULL imageId guard. The generic function would insert NULL entity_ids into the outbox.
- `ENABLE ALWAYS` ensures the trigger fires on CDC-replicated rows (Debezium sets `session_replication_role = replica`).
- Fires on INSERT, UPDATE, and DELETE. UPDATE fires only when the `status` column changes (covers PENDING->ACCEPTED and ACCEPTED->REJECTED transitions). The enrichment query filters on `status = 'ACCEPTED'`, so the filter-sync call always sends the correct current set.
- The trigger emits entity_type='Image' so the existing outbox poller picks it up in the normal UPSERT flow.

## Outbox Poller: Steady-State Sync

When a CollectionItem is inserted or deleted, the trigger fires and the outbox poller picks up the imageId. The existing flow handles the normal image fields (tags, tools, etc.). Collections are handled separately via the filter-sync endpoint.

### Enrichment Query

```sql
SELECT "imageId", "collectionId"
FROM "CollectionItem"
WHERE "imageId" = ANY($1)
  AND status = 'ACCEPTED'
```

This runs in parallel with the existing enrichment queries (tags, tools, techniques, resources) via `tokio::try_join!`.

### Filter-Sync Call

After the normal PATCH for image fields, the poller calls:

```
POST /api/indexes/civitai/documents/filter-sync
{
  "field": "collectionIds",
  "documents": [
    { "id": 12345, "values": [100, 200] },
    { "id": 67890, "values": [] }
  ]
}
```

The poller builds the full current value set for each imageId from the enrichment query. Images with no ACCEPTED collections get an empty array, which clears all their collection memberships. The engine diffs internally against the bitmap state.

This handles both adds and removes correctly:
- Image added to collection 300: enrichment returns [existing..., 300], engine adds 300 bitmap bit.
- Image removed from collection 200: enrichment returns [everything except 200], engine removes 200 bitmap bit.

## Bulk Load

### Streaming Query

For initial deployment, the bulk loader streams the CollectionItem table ordered by collectionId for bitmap-optimal insertion (all images for one collectionId together):

```sql
SELECT "collectionId", "imageId"
FROM "CollectionItem"
WHERE "collectionId" >= $1 AND "collectionId" < $2
  AND "imageId" IS NOT NULL
  AND status = 'ACCEPTED'
ORDER BY "collectionId", "imageId"
```

Range iteration uses:

```sql
SELECT COALESCE(MAX("collectionId")::int8, 0)
FROM "CollectionItem"
WHERE "imageId" IS NOT NULL
```

### Bitmap Building

Same pattern as tags: iterate rows, group by collectionId, bulk-insert slot IDs into the bitmap accumulator. The ordered-by-collectionId scan ensures each bitmap is built contiguously, which is optimal for roaring bitmap compression.

## Performance Considerations

**Memory:** 14M collections but most are small (<100 items). With lazy per-value loading, only queried collections enter memory. The existence set (HashSet of all known collectionId values) is ~112MB for 14M entries — loaded from .fpack headers on startup.

**Query latency:** Single collection lookup is a bitmap load (if not cached) + intersection with other filters. Small collections (<100 items) will be sub-millisecond. Large outliers (44K items) are still fast — roaring bitmaps handle this efficiently.

**Bulk load:** 157M rows at ~16 bytes/row = ~2.5GB streaming. With range-based iteration (same pattern as tags), this parallelizes well.

**Trigger overhead:** One INSERT into BitdexOutbox per CollectionItem INSERT/DELETE. Negligible compared to the actual CollectionItem write.

## Files Changed

| File | Change |
|------|--------|
| `deploy/configs/civitai-index.json` | collectionIds filter + data_schema entry |
| `src/pg_sync/queries.rs` | Trigger SQL, CollectionItemRow, fetch_collections, StreamCollectionRow, bulk load queries |
| `src/pg_sync/bitdex_client.rs` | `filter_sync()` method |
| `src/pg_sync/outbox_poller.rs` | Collection enrichment + filter-sync call |

## Visibility Model

Collection items have three visibility tiers (confirmed by Donovan):

| Viewer | Sees | Source |
|--------|------|--------|
| Anonymous | ACCEPTED items only | BitDex |
| Authenticated user | ACCEPTED + own REVIEW items | BitDex + small PG query |
| Owner/Manager | ALL items (ACCEPTED, REVIEW, REJECTED) | PG only |

**BitDex handles the happy path:** ACCEPTED items only. This covers anonymous users and most authenticated queries. The per-user pending items (own REVIEW submissions) are a small PG query: `WHERE addedById = :userId AND status = 'REVIEW'`. The model-share API layer merges these results. Owner/manager review pages stay entirely on PG.

**Why not store all statuses:** Storing REVIEW/REJECTED items would require per-user bitmap logic (`In(collectionIds_review, [id]) AND Eq(addedById, userId)`) which breaks query cacheability. The few pending items per user are better served by a direct PG query.

**Contests and permissions:** Contest collections use special hash-based ordering (per-hour randomization) — handled by the model-share API, not BitDex. Collection read permissions are checked before the query reaches BitDex.

## Index Coverage (confirmed by Donovan)

CollectionItem has three relevant partial indexes (all with `WHERE imageId IS NOT NULL`):

| Index | Columns | Use |
|-------|---------|-----|
| `CollectionItem_imageId_lookup` | btree(imageId) INCLUDE(id, collectionId, status, tagId) | Enrichment query — index-only scan |
| `CollectionItem_image_idx` | UNIQUE btree(collectionId, imageId) | Bulk load range scan |
| `CollectionItem_collectionId_status_covered` | btree(collectionId, status, createdAt DESC) INCLUDE(id, imageId, ...) | Alternative for status-filtered range scans |

The enrichment query (`WHERE imageId = ANY($1) AND status = 'ACCEPTED'`) hits `CollectionItem_imageId_lookup` as an index-only scan — collectionId and status are INCLUDEd columns. No table heap access needed.

## Future Optimization: Inverse Index for High-Cardinality Fields

`sync_filter_values` scans all loaded bitmaps for the field to find a slot's current values. For collectionIds this is fine (most images in 0-3 collections, few hundred loaded bitmaps). For tagIds-scale fields (31K+ loaded values), this scan would be expensive. A slot→values inverse index could eliminate the scan. Not needed for collections but worth adding if filter-sync is used for higher-cardinality fields. (Flagged by Adam.)

## Open Questions

1. **Eviction config:** Should collectionIds have idle eviction like tagIds? Given 14M distinct values and lazy loading, eviction would help memory. Not added yet — can be configured post-deploy via PATCH /config.
