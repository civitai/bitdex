# PG Loader OOM Analysis

## The Problem

The `bitdex-pg-sync load` command runs out of memory at 48 GB RAM when loading ~107M records from Postgres CSVs. The bitmaps themselves only need ~6.5 GB (proven locally), so something in the loading pipeline is consuming ~35+ GB on top of that.

## Root Cause

The arena-free bulk loader (`run_bulk_load_copy` in `src/pg_sync/bulk_loader.rs`) stores a per-image scalar struct in a `HashMap<u32, ImageScalars>` for all 107M images. The code estimates this at ~8.5 GB (107M x 80 bytes), but the actual cost is **~25 GB**:

| Component | Per-entry | 107M total |
|---|---|---|
| ImageScalars struct | 80 B | 8.6 GB |
| HashMap overhead (hashbrown, 87% fill) | ~22 B | 2.4 GB |
| URL strings (heap-allocated `Box<str>`, ~60 chars + allocator header) | ~76 B | 8.1 GB |
| Hash strings (heap-allocated `Box<str>`, ~32 chars + allocator header) | ~48 B | 5.1 GB |
| **Total** | **~226 B** | **~24.2 GB** |

On top of that:
- Tag bitmaps (31K distinct values): **~5.1 GB**
- Other filter + sort bitmaps: **~2.5 GB**
- Post/MV/Model lookup HashMaps (never dropped, retained entire function): **~1.5 GB**
- Engine staging clone during apply: **~7 GB**
- **Peak total: ~40 GB** before docstore finalization even starts

The HashMap exists because the current pipeline processes all tables first (images, tags, tools, techniques, resources), stores everything in memory, then does a single finalization pass to write the docstore. URL and hash strings are doc-only fields (they don't affect bitmaps) but are held in memory for the entire load just so they can be written to the docstore at the end.

## What We Have (Current Pipeline)

```
1. Download all PG tables to local CSV files
2. Load enrichment lookups (Post, ModelVersion, Model) into HashMaps
3. Process images.csv    → scalar filter/sort bitmaps + store ALL scalars in HashMap (25 GB)
4. Process tags.csv      → tagIds filter bitmaps (~5 GB)
5. Process tools.csv     → toolIds filter bitmaps
6. Process techniques.csv → techniqueIds filter bitmaps
7. Process resources.csv → modelVersionIds + baseModel filter bitmaps
8. Merge all bitmap accumulators
9. Apply bitmaps to engine staging (two clone+apply cycles)
10. Finalize: iterate HashMap + reconstruct multi-value from bitmaps → docstore
11. Apply multi-value bitmaps to engine
12. Save snapshot
```

Everything is in memory simultaneously at step 10. No backpressure anywhere.

## What We Need To Do (Proposed Pipeline)

Flip the order: build enrichment bitmaps first, process images last as a streaming pass that writes directly to the docstore.

```
1. Download all PG tables to local CSV files
2. Load enrichment lookups (Post, ModelVersion, Model) into HashMaps
3. Process tags.csv       → tagIds filter bitmaps
4. Process tools.csv      → toolIds filter bitmaps
5. Process techniques.csv → techniqueIds filter bitmaps
6. Process resources.csv  → modelVersionIds + baseModel filter bitmaps
7. Drop enrichment lookup HashMaps (no longer needed)
8. Merge enrichment bitmap accumulators, apply to engine staging
9. Stream images.csv → for each row:
   a. Build scalar filter/sort bitmaps + alive bit (same as now)
   b. Pull scalars (url, hash, etc.) directly from CSV line
   c. Reconstruct multi-value fields for this image from bitmaps
   d. Write doc to docstore immediately (with backpressure)
   e. Scalars leave memory as soon as the doc is written
10. Apply image scalar bitmaps to engine staging
11. Save snapshot
```

**Memory at peak (step 9):** bitmaps (~8 GB) + streaming CSV buffer (~4 MB) + docstore write batch (bounded) = **~10 GB**. Well within 48 GB.

## The Open Problem: Multi-Value Reverse Lookup

Step 9c requires answering: "which tagIds/toolIds/etc. does image X have?" The bitmaps are indexed by value (tag 1234 → bitmap of image slots), not by slot (image 5678 → list of tag IDs). Reconstructing per-image lists from value-indexed bitmaps requires scanning all value bitmaps per image, which is O(distinct_values) per image — too slow for tagIds with 31K+ values.

### Options

**A. Per-slot scratch file (recommended)**
During enrichment CSV processing (steps 3-6), write `(slot, field, value_id)` tuples to a temporary sorted file on disk. During the image streaming pass (step 9), read the scratch file in slot order alongside the image CSV. Disk I/O is sequential and fast on NVMe. Memory cost: just a read buffer.

**B. In-memory reverse map**
During enrichment CSV processing, also build `HashMap<u32, Vec<u32>>` mapping slot → value IDs per multi-value field. This is essentially what we're trying to avoid — at ~8 tags/image average, 107M images x 8 x 4 bytes = 3.4 GB for tags alone. But it's still far less than the current 25 GB HashMap.

**C. Two-pass image processing**
Pass 1: stream images.csv to build scalar bitmaps only (no scalars stored).
Pass 2: stream images.csv again, reconstruct multi-value from bitmaps using chunked iteration (current `finalize_from_bitmaps` approach processes 65K-slot chunks, iterating all value bitmaps per chunk). This works but doubles image CSV read time.

**D. Don't reconstruct — defer docstore population**
Build all bitmaps, save snapshot, start serving queries (bitmaps are all that's needed for filtering/sorting). Populate docstore lazily on first document request, or via a background pass after the index is live. This gets the server online fastest but means document content isn't available immediately.

## Constraints

- Pod memory limit: 48 GB (already bumped from lower)
- Dataset: ~107M images, growing
- Bitmaps (steady-state memory): ~6.5 GB filter + ~1 GB sort = ~7.5 GB
- Docstore is on-disk (NVMe), not in memory
- tagIds has 31K+ distinct values, ~79% of filter bitmap memory
- The loader binary uses `rpmalloc` allocator (thread-local caches may inflate RSS beyond actual usage)
- CSV files are already downloaded to local disk before bitmap processing begins
