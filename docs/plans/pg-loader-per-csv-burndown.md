# Plan: Per-CSV Burn-Down Loader

**Status:** Reviewed (Opus + Gemini + Sonnet) — see Review Findings below
**Supersedes:** `pg-loader-scatter-gather.md` (v1, tested at 107M, 28 min / 20.4 GB peak)
**Goal:** Load 107M records in <15 min, peak memory <8 GB. No intermediate staging files.

> **Review verdict:** The per-CSV direction is correct but three assumptions are wrong:
> docstore merge-write takes 20-50 min (not 6s), tag bitmaps take 23 min (not 7),
> and images must run before tags for the alive bitmap. See Review Findings section.

---

## Core Idea

Process one CSV at a time, largest first. Each CSV streams through rayon workers that produce bit tuples: `(slot_id, field, value)`. Each tuple routes to two destinations — the bitmap index and the docstore — then leaves memory. When a CSV is fully processed, save its bitmaps to disk and drop them. Start the next CSV with clean memory.

No scratch shards. No gather phase. Each CSV IS its own scatter and gather.

---

## Why This Is Better Than Scatter-Gather

The scatter-gather v1 (tested tonight) has three problems:

1. **Sequential phases waste time.** Scatter takes 16 min writing 50 GB of scratch data. Gather takes 6 min reading it back. That's 22 min of I/O for data that already existed in the CSVs.

2. **Memory accumulates.** During gather, all shard bitmaps queue in an unbounded channel while the merge thread catches up. Peak RSS hit 20.4 GB despite a 7.5 GB steady-state bitmap footprint.

3. **Scratch files are redundant.** The CSVs already contain all the data. Writing it to scratch shards just reorganizes it by slot range — but the per-CSV approach achieves the same effect by processing each CSV completely before moving on.

The per-CSV burn-down eliminates scratch files, eliminates the gather phase, and caps memory at the cost of one CSV's bitmaps at a time.

---

## Processing Order

Largest first. Each step builds bitmaps + writes docstore fields, saves bitmaps, drops memory.

| Step | CSV | Size | Rows | Bitmap memory | Cumulative docstore |
|------|-----|------|------|---------------|---------------------|
| 0 | posts.csv, model_versions.csv, models.csv | 646 MB | ~24M | 0 (enrichment lookups only: ~1.3 GB) | 0 |
| 1 | tags.csv | 63 GB | 4.5B | ~5.1 GB (tagIds: 31K distinct values) | tagIds field written |
| 2 | images.csv | 14 GB | 107M | ~2.5 GB (scalar filters + sort layers + alive) | scalar fields written |
| 3 | resources.csv | 777 MB | ~10M | ~0.5 GB (modelVersionIds + baseModel) | resource fields written |
| 4 | tools.csv | 50 MB | ~1M | <50 MB (toolIds) | tool fields written |
| 5 | techniques.csv | 71 MB | ~1M | <50 MB (techniqueIds) | technique fields written |

**Peak memory: Step 1** — enrichment lookups (1.3 GB) + tagIds bitmaps (5.1 GB) + rayon worker buffers = ~7 GB. After step 1 completes, tagIds bitmaps save to disk and drop. Step 2 starts at ~1.3 GB.

### Step 0: Load Enrichment Lookups

Load Post, ModelVersion, Model CSVs into HashMaps for foreign key resolution during image processing. These are small (646 MB on disk, ~1.3 GB in memory) and stay alive until images.csv is processed.

### Step 1: Tags (dominant workload)

Stream tags.csv with the block reader (14.4M rows/s measured). Rayon fold+reduce across 8 MB blocks:
- Each worker: parse `(tag_id, image_id)` pairs, insert into thread-local `HashMap<u64, RoaringBitmap>` for tagIds, accumulate docstore tuples.
- Reduce: merge thread-local bitmaps with bitmap OR.
- Main thread: apply merged block to engine staging, write docstore field entries.

After all blocks processed: save tagIds bitmaps to BitmapFs. Drop tagIds bitmaps from memory. Drop enrichment lookups if not needed by later steps (they are — images needs post_map).

### Steps 2-5: Remaining CSVs

Same pattern. Images.csv is the heaviest remaining (107M rows, 14 GB), producing scalar filter bitmaps (nsfwLevel, userId, type, etc.), sort layer bit stacks (sortAt, reactionCount, etc.), and docstore scalar fields (url, hash). Resources adds modelVersionIds and baseModel. Tools and techniques are tiny.

---

## Docstore Format Change: Field-Append Shards

The current docstore writes complete msgpack documents per slot. Each `write_shard_file` call completely rewrites the shard. This blocks incremental field appends — you can't write tags first and images later to the same slot.

**Required change:** support field-level merge on write.

### Option A: Read-Merge-Write (minimal change)

Add a `write_batch_merge` method to BulkWriter that:
1. Reads the existing shard.
2. For each slot in the new batch, decodes the existing doc (if any), merges the new fields into it, re-encodes.
3. Writes the merged shard.

Cost: one read + decompress + merge + compress + write per shard per CSV pass. With 209K shards (107M / 512) and 5 CSV passes, that's ~1M shard read-write cycles. At measured shard I/O rates (~880K/s), this adds ~6 seconds total. Acceptable.

Field merge is straightforward: the msgpack encodes `Vec<(u16, PackedValue)>`. Merge = collect field pairs from both old and new, deduplicate by field index (new wins), re-encode.

**This is the recommended approach.** Smallest code change, no format migration, and the cost is negligible at measured I/O rates.

### Option B: Tuple-Append Format (larger change, future)

Change the shard format to store raw `(slot_id, field_idx, value)` tuples instead of pre-assembled documents. Append-only writes during load. Assembly on read. This matches the bit tuple model described in the architecture doc.

Benefits: zero read-modify-write during load, simpler write path, natural alignment with the tuple ingestion model. Costs: format migration, read-path change, potential read performance regression for single-doc fetches (scan instead of index lookup).

This is the right long-term direction but not needed for the immediate loader fix.

---

## Backing Numbers

All numbers from the 2026-03-14 test session against 107M Civitai images.

### Bitmap Memory Per Field

| Field | Distinct values | Memory | % of total |
|-------|----------------|--------|------------|
| tagIds | 31,248 | ~5.1 GB | 79% |
| userId | 748,787 | ~0.6 GB | 9% |
| modelVersionIds | 325,514 | ~0.4 GB | 6% |
| Sort layers (6 fields x 32 bits) | 192 bitmaps | ~1.0 GB | — |
| All other filters | ~20 bitmaps | ~0.4 GB | 6% |
| **Total** | | **~7.5 GB** | |

### I/O Throughput (measured)

| Operation | Rate | Source |
|-----------|------|--------|
| Tag CSV block read (zero-copy) | 14.4M rows/s | scatter_tags_io with block reader |
| Image CSV parse + enrich | 725K rows/s | scatter_images_io |
| DocStore BulkWriter | 290K docs/s | BulkWriter::write_batch_encoded |
| DocStore fresh write | 327K docs/s | write_batch_fresh (no read-modify-write) |
| NDJSON full load (all fields) | 320K docs/s | loader::load_ndjson (5m29s @ 105M) |
| Shard read (NVMe) | 880K/s | DocStore::get benchmarks |

### Projected Per-CSV Timing

| Step | CSV | Bitmap build | DocStore write | Save bitmaps | Total |
|------|-----|-------------|----------------|--------------|-------|
| 0 | Enrichment lookups | — | — | — | ~20s |
| 1 | tags.csv (4.5B rows) | ~5 min (rayon, 14.4M/s read, bitmap is bottleneck) | ~2 min (merge-write 209K shards) | ~10s (tagIds to BitmapFs) | ~7 min |
| 2 | images.csv (107M rows) | ~2.5 min (725K/s parse + bitmap) | ~1 min (merge-write) | ~30s (scalar + sort bitmaps) | ~4 min |
| 3 | resources.csv (10M rows) | ~15s | ~15s | ~5s | ~35s |
| 4 | tools.csv (1M rows) | ~2s | ~2s | <1s | ~5s |
| 5 | techniques.csv (1M rows) | ~2s | ~2s | <1s | ~5s |
| | | | | **Total** | **~12 min** |

**Projected peak memory: ~7 GB** (Step 1: 1.3 GB enrichment + 5.1 GB tagIds + 0.5 GB rayon buffers).

---

## Memory Budget

| Phase | Component | Memory |
|-------|-----------|--------|
| Step 0 | Enrichment lookups (post/mv/model) | ~1.3 GB |
| Step 1 | tagIds bitmaps (31K values) | ~5.1 GB |
| Step 1 | Rayon worker buffers | ~0.5 GB |
| Step 1 | **Peak** | **~7 GB** |
| Step 2 | Scalar filter + sort bitmaps | ~2.5 GB |
| Step 2 | Enrichment lookups (still alive) | ~1.3 GB |
| Step 2 | **Peak** | **~4 GB** |
| Steps 3-5 | Small bitmaps | < 1 GB |
| Bitmap save | BitmapFs write (streaming) | negligible |

After all steps: bitmaps on disk, docstore on disk, memory ~0. Server lazy-loads bitmaps on first query.

---

## Open Questions

1. **Tag bitmap build rate.** We measured 14.4M rows/s for pure I/O scatter (no bitmap work). With rayon fold+reduce building tagIds bitmaps, the rate was 3.3M/s (degrading). With 4.5B rows, that's ~23 min for tags alone — worse than scatter-gather. The NDJSON loader achieved 320K docs/s processing ALL fields per document because documents are independent and cache-friendly. Tags are different: each row touches a different tag_id bitmap, causing random HashMap access across 31K entries. **Need to benchmark:** can we match the NDJSON pattern by processing tags in rayon fold+reduce blocks large enough for each worker to build a substantial thread-local HashMap before merging?

2. **Read-merge-write cost at scale.** The docstore merge-write path reads, decompresses, merges, compresses, and writes each shard. With 5 CSV passes over 209K shards, that's ~1M cycles. At measured rates this should take ~6 seconds, but we haven't benchmarked the merge path specifically. Compression ratios may vary as documents grow from partial (tags only) to complete (all fields).

3. **Enrichment lookup lifetime.** Post_map (~1.3 GB) is needed during images.csv processing. If we process tags first, post_map stays in memory during the tag phase (~5 min). Should we process images before tags? That would reduce peak memory during the tag phase but lose the "biggest first" ordering.

4. **Bitmap save between steps.** Saving tagIds bitmaps (31K values, ~5.1 GB) to BitmapFs takes ~10s based on snapshot save timing. During this save, the bitmaps are still in memory. Can we stream the save (write each bitmap file and drop it) to reduce the peak?

---

## Implementation Order

1. **Add `write_batch_merge` to DocStore** — read existing shard, decode, merge new fields, encode, write. Unit test with partial documents.
2. **Process one CSV end-to-end** — pick tags.csv, stream with rayon, build tagIds bitmaps, write tagIds to docstore via merge, save bitmaps, drop. Measure memory + time.
3. **Wire remaining CSVs** — images (with enrichment), resources, tools, techniques. Each follows the same pattern.
4. **Test at 107M scale** — full pipeline, measure peak RSS, total time, docstore correctness.
5. **Optimize tag bitmap build** — if 3.3M/s is the bottleneck, experiment with larger rayon block sizes, pre-allocated thread-local HashMaps, or the concurrent DashMap approach.
6. **Remove scatter-gather code** — once per-CSV burn-down is validated, delete scratch.rs, scatter_gather.rs, and the scatter-gather plan doc.

---

## Comparison

| | HashMap (OOM'd) | Scatter-Gather v1 | Per-CSV Burn-Down |
|---|---|---|---|
| Peak RSS | 40+ GB (OOM) | 20.4 GB | ~7 GB (projected) |
| Total time | N/A | 28 min | ~12 min (projected) |
| Intermediate disk | 0 | 50 GB scratch | 0 |
| Docstore format change | No | No | Yes (field merge) |
| Code complexity | Low | Medium | Medium |

---

## Review Findings (2026-03-14, Opus + Gemini + Sonnet)

Three models reviewed independently. Strong consensus on three issues:

### Issue 1: Docstore merge-write costs 20-50 min, not 6 seconds

The full cycle per shard is: read → decompress → decode msgpack → merge fields → encode → compress → write. At ~2 ms/shard × 209K shards × 5 passes = **35-50 minutes** (Opus), **~20 minutes** (Sonnet). My 880K/s estimate was for hot NVMe reads, not the full cycle.

**Fix (consensus):** Write complete documents once, not incrementally. Build all bitmaps first (streaming to BitmapFs per CSV), then single-pass docstore population reconstructing multi-value fields from the saved bitmaps. ONE write per slot.

### Issue 2: Tag bitmap build at 3.3M/s means 23 min, not 7

The 4.5B rows at measured rayon rates = 23 min. Cache thrashing across 31K tag bitmaps (~5 GB working set) causes the 4x degradation from pure I/O.

**Optimizations proposed:**
- **Gemini:** Replace HashMap with dense `Vec<RoaringBitmap>[32K]` + partition workers by tag_id range (no merge phase).
- **Opus:** Pre-sort tags.csv by tag_id for cache locality, or accept 23 min.
- **Sonnet:** Larger rayon blocks (64-128 MB) to amortize HashMap access.

### Issue 3: images.csv must run BEFORE tags (alive bitmap dependency)

The alive bitmap (which slots exist) is built from images.csv. Without it, tags creates bitmaps for orphaned slots. All three reviewers flagged this as critical.

**Correct processing order:**
1. Enrichment lookups (posts, model_versions, models)
2. images.csv → alive bitmap + scalar filter/sort bitmaps. Save to BitmapFs. Drop.
3. tags.csv → build tagIds bitmaps, filter against alive. Save. Drop.
4. resources, tools, techniques.

**Peak shifts to Step 3:** enrichment (1.3 GB) + tagIds (5.1 GB) + alive bitmap (13 MB) = ~6.5 GB.

### Recommended Architecture (Opus Hybrid)

**Phase 1: Build ALL bitmaps per CSV, streaming saves to disk**
- Process images first (alive + scalars), save bitmaps, drop
- Process tags (tagIds), save bitmaps, drop
- Process remaining CSVs similarly
- Peak: ~6-7 GB (one CSV's bitmaps at a time)

**Phase 2: Single-pass docstore population**
- Load alive bitmap from BitmapFs (~13 MB)
- Stream images.csv one more time
- For each alive slot: reconstruct multi-value fields from disk-backed bitmaps (existing 65K-chunk pattern from `finalize_from_bitmaps`)
- Write complete documents (ONE write per slot, no merge)
- Peak: ~1 GB

**Projected total:** ~30 min (tags dominate), peak ~7 GB. Time parity with scatter-gather v1 but 3x less memory. Tag optimization (dense Vec + partitioned workers) could bring tags to ~10 min.
