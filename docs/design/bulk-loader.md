# Bulk Loader

The bulk loader bootstraps a BitDex index from Postgres. It runs as `bitdex-pg-sync load` — a headless process that reads PG tables, builds bitmaps and a docstore, saves everything to disk, then exits. The running BitDex server picks up the data on its next restart.

## Execution Paths

Three loader implementations exist, selected by config:

| Path | Entry | Strategy | Status |
|------|-------|----------|--------|
| `run_bulk_load` | In-process streaming | Range-batched PG queries → SlotArena mmap | Legacy |
| `run_bulk_load_copy` | CSV two-phase | COPY download → HashMap scalars → bitmap reconstruction | Current production |
| `run_single_pass_v2` | Single-pass CSV | One pass per CSV, stream-save bitmaps after each | Experimental |

Production uses `run_bulk_load_copy`. The rest of this document describes that path.

## Pipeline Overview

```
PG Tables ──COPY──► CSV files on disk
                        │
                        ├─ posts.csv ──► post_map HashMap (enrichment)
                        ├─ model_versions.csv ──► mv_map HashMap
                        ├─ models.csv ──► model_map HashMap
                        │
                        ├─ images.csv ──► ImageScalars HashMap + filter/sort BitmapAccum
                        ├─ tags.csv ──► tagIds BitmapAccum (ordered by tagId)
                        ├─ resources.csv ──► modelVersionIds + baseModel BitmapAccum
                        ├─ tools.csv ──► toolIds BitmapAccum
                        └─ techniques.csv ──► techniqueIds BitmapAccum
                                                    │
                                            merge 5 accumulators
                                                    │
                                    ┌───────────────┴───────────────┐
                                    │                               │
                            extract multi-value           apply filter/sort
                            bitmaps (tags, tools,         bitmaps to staging
                            techniques, mvs)              engine
                                    │                               │
                            finalize_from_bitmaps:                  │
                            reconstruct arrays from       publish via ArcSwap
                            bitmaps → JSON → docstore
                                    │
                                save_snapshot
                                    │
                                  exit
```

## Phase 1: CSV Download

Downloads 8 PG tables via `COPY TO STDOUT` to local CSV files. Each table gets a `.done` marker on completion for resumability. Downloads run sequentially to avoid overwhelming the PG replica.

| Table | Typical Size | Purpose |
|-------|-------------|---------|
| images | 14 GB | Primary records (107M rows) |
| tags | 63 GB | TagsOnImageDetails (5.4B rows) |
| resources | 777 MB | ImageResourceNew (model versions per image) |
| posts | 610 MB | Post metadata (publishedAt, availability) |
| tools | 50 MB | ImageTool associations |
| techniques | 71 MB | ImageTechnique associations |
| model_versions | 24 MB | ModelVersion lookups (baseModel) |
| models | 12 MB | Model lookups (poi, type) |

**Total download:** ~80 GB, ~6 minutes at 44 MB/s sustained.

## Phase 2: Enrichment Lookup Build

Loads three small CSVs into HashMaps for O(1) enrichment during image processing:

- **post_map:** `HashMap<post_id, (published_at_secs, availability, model_version_id)>`
- **mv_map:** `HashMap<mv_id, (base_model_str, model_id)>`
- **model_map:** `HashMap<model_id, (poi, type_str)>`

These stay in memory throughout Phase 3. ~2 minutes, ~1 GB RAM.

## Phase 3: Bitmap Building

### Core Data Structure: BitmapAccum

```rust
struct BitmapAccum {
    filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
    sort_maps: HashMap<String, HashMap<u8, RoaringBitmap>>,
    alive: RoaringBitmap,
}
```

- **filter_maps:** Per field, per distinct value → roaring bitmap of slots with that value.
- **sort_maps:** Per field, per bit position → roaring bitmap of slots with that bit set. A u32 sort field = 32 bitmaps.
- **alive:** All active document slots.

### Image Stream

Reads images.csv row by row. For each image:

1. Store compact scalars in `HashMap<u32, ImageScalars>` (~80 bytes/image, 8.5 GB for 107M)
2. Enrich from post_map (publishedAt, availability, postedToId)
3. Compute derived booleans (hasMeta, onSite, isPublished, isRemix)
4. Set alive bit
5. Insert into filter bitmaps (nsfwLevel, userId, type, etc.)
6. Decompose sort values into per-bit bitmaps (sortAt → 32 bitmaps)

String fields (type, availability, blockedFor, baseModel) stored as u8 enums to save memory.

### Tag Stream

Tags CSV is **ordered by tagId**, not imageId. This is critical for performance:

- All images for tagId=1 arrive together → one bulk insert into `RoaringBitmap`
- Versus per-image ordering: 107M separate insertions → 360x slower

Processing: accumulate image IDs for current tagId, flush batch into bitmap on tagId change.

### Resource, Tool, Technique Streams

Same ordered-by-value pattern as tags. Resource stream additionally enriches baseModel and resource POI from the mv/model lookup maps.

### Merge

Five BitmapAccums (images, tags, tools, techniques, resources) merge into one:

```rust
for (key, bm) in other.filter_maps[field] {
    self.filter_maps[field][key] |= bm;
}
```

Multi-value bitmaps (tagIds, toolIds, etc.) are extracted before application — they're needed for docstore finalization but shouldn't go through the engine's mutation path.

## Phase 4: Docstore Finalization

Reconstructs complete JSON documents from scalars + bitmaps, writes to docstore.

Processes alive slots in 65K-block chunks (aligned to roaring bitmap containers, parallelized via rayon):

1. For each chunk, iterate all value bitmaps to reconstruct multi-value arrays:
   ```
   for (tag_id, bm) in &tag_bitmaps:
       for slot in bm.range(chunk_start..chunk_end):
           chunk_tags[slot - chunk_start].push(tag_id)
   ```
2. For each alive slot in chunk:
   - Look up ImageScalars from HashMap
   - Combine with reconstructed arrays (tagIds, toolIds, etc.)
   - Assemble JSON matching the data schema
   - Encode: msgpack + zstd compression
   - Append tuple to docstore shard file

**Output:** ~160 GB docstore (107M images × ~1.5 KB encoded per doc).

## Phase 5: Apply and Save

1. Apply remaining filter/sort bitmaps to a cloned engine staging snapshot
2. Publish staging atomically via `ArcSwap::store()`
3. `save_snapshot()` persists all bitmaps to BitmapFs on disk
4. Exit

The server's next startup loads the snapshot from disk. The pg-sync sidecar resumes outbox polling from the seeded cursor (set at the outbox head during Phase 1).

## Relation to Ingester

The bulk loader is an early version of the ingester system (`src/ingester.rs`). Both share the same core pattern:

- Parse source data → extract filter/sort field values
- Insert into roaring bitmaps (one per distinct value per field)
- Decompose sort values into per-bit bitmaps
- Write documents to docstore

The ingester generalizes this with the `BitmapSink` trait (`CoalescerSink`, `AccumSink`, `DocSink`) for pluggable downstream consumers. The bulk loader predates this abstraction and builds bitmaps directly.

## Key Performance Characteristics

| Metric | Value |
|--------|-------|
| Total wall time | ~28 min (107M images) |
| Download phase | ~6 min (80 GB at 44 MB/s) |
| Bitmap build | ~10 min |
| Docstore finalization | ~10 min |
| Snapshot save | ~2 min |
| Peak RSS | ~20 GB |
| Scalars memory | ~8.5 GB (107M × 80B) |

## K8s Deployment

Runs as a K8s Job created from a suspended CronJob template:

```bash
kubectl create job -n bitdex --from=cronjob/bitdex-bulk-load bitdex-bulk-load-run-1
```

Or as a standalone Job targeting a specific replica's PVC:

```yaml
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data-bitdex-1  # Target replica's PVC
env:
  - name: BITDEX_REPLICA_ID
    value: "bitdex-1"           # Cursor namespace
```

The Job mounts the same PVC as the StatefulSet pod (OpenEBS hostpath allows multi-pod access on the same node). Progress is available via HTTP on port 9091.

## Files

| File | Role |
|------|------|
| `src/pg_sync/mod.rs` | Entry point, three load subcommands |
| `src/pg_sync/bulk_loader.rs` | CSV-based loader, arena-free finalization |
| `src/pg_sync/copy_streams.rs` | COPY-based streaming + rayon parallel bitmap building |
| `src/pg_sync/copy_queries.rs` | COPY query generation, CSV row parsing |
| `src/pg_sync/row_assembler.rs` | JSON document assembly from ImageRow + enrichment |
| `src/pg_sync/slot_arena.rs` | Memory-mapped slot storage (legacy path) |
| `src/pg_sync/progress.rs` | Shared progress state + HTTP status endpoint |
| `src/pg_sync/config.rs` | PgSyncConfig loading |
