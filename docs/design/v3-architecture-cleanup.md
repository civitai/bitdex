# V3 Architecture Cleanup — Kill V2 Machinery

**Status:** IMPLEMENTED (small-scale validated, pending full-scale 107M dump)  
**Date:** 2026-04-04 (design), 2026-04-05 (implementation complete)  
**Author:** Scarlet (team lead), Lucy (FieldRegistry + key encoding), Justin (architecture direction)  
**PRs:** #129-146 (16 PRs merged to main)

## Problem

BitDex's V3 architecture (BitmapSilo, frozen bitmaps, ops-on-read) was partially implemented alongside the existing V2 machinery (in-memory FilterIndex/SortIndex, flush thread, VersionedBitmap diffs). The result is a hybrid system that:

1. **Dual-writes everything** — mutations go to both BitmapSilo ops log AND in-memory indexes via the flush thread
2. **Dual-reads on some paths** — filter EQ uses BitmapSilo primary, but range scans and sorts still depend on in-memory state
3. **Wastes memory** — the full in-memory FilterIndex + SortIndex duplicates what's already in the mmap'd BitmapSilo
4. **Cache live maintenance is broken** — stale_fields are collected in the flush thread but discarded without invalidating anything (`flush.rs:159`)
5. **BitmapSilo index doesn't use the mmap HashIndex** — puts a heap HashMap + string manifest on top of DataSilo's already-mmap'd hash table

## What's Already V3 (Keep)

- `get_effective_bitmap()` in executor.rs uses BitmapSilo as primary path for filter EQ/IN queries
- `get_filter_with_ops()` / `get_sort_layer_with_ops()` — frozen base + ops-on-read
- `build_frozen_sort_layers()` — reads sort layers from BitmapSilo for unloaded fields
- `FrozenRoaringBitmap` zero-copy mmap reads from DataSilo
- DataSilo's `HashIndex` — mmap'd open-addressed hash table (u64 key → offset/length in data file)
- CacheSilo — already uses u32 hash keys directly into DataSilo, no manifest

## What's Broken

### 1. Cache Live Maintenance is Dead

**File:** `src/engine/flush.rs:97-159`

The flush thread collects `stale_fields` (which fields had mutations this cycle) but then clears the vector without doing anything:

```rust
stale_fields.clear();  // line 159 — collected field names thrown away
```

No cache entries are invalidated. No CacheSilo entries are deleted. Stale cache entries persist until overwritten by a future miss on the same key hash. This means:

- A document gets upserted changing its `nsfwLevel` from 1 to 5
- The cache entry for `nsfwLevel=1, sortAt desc` still contains that slot
- Queries see stale results until the cache entry happens to get re-seeded

**Fix:** Cache live maintenance must be restored. The old UnifiedCache had working live maintenance: `maintain_slot_insert()` / `maintain_slot_remove()` for mutation tracking, a meta-index mapping (field, value) → cache entries, and generational invalidation for time bucket staleness. All of this was lost in the CacheSilo rewrite. The V3 model should use ops-on-read for cache entries (same pattern as bitmap ops), with the janitor compacting cache ops into the data file. Epoch-based staleness is acceptable as a fallback ONLY if live maintenance proves infeasible in the silo model — but Justin's strong preference is live maintenance since it's what makes queries fast.

### 2. BitmapSilo Uses Heap HashMap Instead of mmap HashIndex

**File:** `src/silos/bitmap_silo.rs:38-49`

BitmapSilo stores a `HashMap<String, u32>` (`name_to_key`) that maps string names like `"filter:nsfwLevel:1"` to DataSilo integer keys. This is loaded from a JSON manifest file on startup.

Every query:
1. Allocates a string via `format!("filter:{}:{}", field, value)` 
2. Acquires a `RwLock::read()` on the HashMap
3. Looks up the string to get a u32 key
4. Passes that u32 to `DataSilo.get(key)` which does the actual mmap HashIndex lookup

The DataSilo's `HashIndex` already maps u64 keys to (offset, length) in the data file via mmap. We should encode (field, value) directly as a u64 and go straight to the mmap index. No heap HashMap, no string formatting, no RwLock, no manifest file.

### 3. Sort Traversal Requires In-Memory SortIndex

**File:** `src/engine/executor.rs:824-827`

`sort_and_paginate()` calls `self.sorts.get_field()` which must return an in-memory `SortField`. If the field isn't loaded in memory, the query fails. BitmapSilo frozen layers are only used as supplements for individual unloaded bit-layers, not as the primary sort source.

### 4. Range Scans Depend on In-Memory Key Enumeration

**File:** `src/engine/executor.rs:731-752`

`range_scan()` iterates `filter_field.iter_versioned()` to discover which values exist for a field. Only falls back to `silo.filter_entries()` when no in-memory FilterField exists. The silo fallback itself is also broken — it scans ALL manifest strings and parses prefixes.

### 5. Alive Bitmap Eagerly Loaded to Heap

The alive bitmap is loaded from BitmapSilo into an in-memory `VersionedBitmap` at startup and maintained via the flush thread. Should use ops-on-read like everything else.

---

### 6. Time Bucket Maintenance Depends on In-Memory SortIndex

**File:** `src/engine/flush.rs:227-338`

The flush thread's incremental time bucket refresh reads `sort_field.reconstruct_value(slot)` for every slot in each bucket bitmap to find aged-out slots. This requires the in-memory SortIndex. Also:

- **Insert maintenance** (`flush.rs:138-146`): reads `sort_field.reconstruct_value(slot)` to determine if a newly-alive slot belongs in a bucket. Requires in-memory SortField.
- **PendingBucketDiffs** (`bucket_diff_log.rs`): computed diffs stored via ArcSwap, designed for lazy cache application. But cache live maintenance is dead, so these diffs are computed and never applied.
- **ArcSwap\<PendingBucketDiffs\>** (`concurrent_engine.rs:87`): V2 snapshot pattern, no consumers.

### 7. Planner Uses In-Memory Cardinality

**File:** `src/query/planner.rs`

`ff.cardinality(key)` returns `base_len()` from in-memory VersionedBitmap. This is used for clause reordering. With V2 state gone, needs silo-based cardinality (could be stored in the HashIndex entry metadata or a separate cardinality cache).

---

## Kill List (V2 → Delete)

### Infrastructure

| Component | File(s) | What It Does | Why Kill |
|-----------|---------|-------------|----------|
| **VersionedBitmap** | `engine/versioned_bitmap.rs` (~680 lines) | Base + Arc\<BitmapDiff\> deferred compaction | V3 uses frozen base + ops log, not in-memory diffs |
| **FilterIndex** | `engine/filter.rs` (~360 lines) | HashMap\<field, FilterField\> with VersionedBitmaps | V3 reads from BitmapSilo, not in-memory |
| **SortIndex** | `engine/sort.rs` (~450 lines) | Vec\<VersionedBitmap\> per sort field | V3 uses frozen sort layers from BitmapSilo |
| **SlotAllocator alive bitmap** | `engine/slot.rs` (~310 lines) | In-memory VersionedBitmap for alive | V3 uses BitmapSilo ops-on-read for alive |
| **FlushBatch** | `engine/flush_batch.rs` (~170 lines) | Groups MutationOps, applies to in-memory state | No in-memory state to apply to |
| **Flush thread bitmap apply** | `engine/flush.rs:100-107` | Acquires write locks, applies batch | Dead once in-memory state is gone |
| **MutationOp channel** | `mutation.rs` + `concurrent_engine.rs` | crossbeam channel from writers to flush thread | Mutations go to BitmapSilo ops log only |
| **InnerEngine** | `concurrent_engine.rs:39-43` | Staging buffer (slots + filters + sorts) | No staging; writes go directly to silo |
| **clone_staging()** | `concurrent_engine.rs:1060` | Clone live state for offline mutation | Dead |
| **publish_staging()** | `concurrent_engine.rs:1052` | Swap staging into live under write locks | Dead |
| **apply_bitmap_maps()** | `concurrent_engine.rs:1076` | Bulk OR pre-built bitmaps into staging | Dead; dump writes to silo directly |
| **RwLock\<FilterIndex\>** | `concurrent_engine.rs:72` | Lock-protected in-memory filter state | No in-memory filter state |
| **RwLock\<SortIndex\>** | `concurrent_engine.rs:73` | Lock-protected in-memory sort state | No in-memory sort state |
| **RwLock\<SlotAllocator\>** | `concurrent_engine.rs:70` | Lock-protected in-memory alive state | No in-memory alive state |
| **merge_dirty()** | Multiple files | Compact diffs into base | No diffs in V3 |
| **swap_diff()** | `versioned_bitmap.rs:301` | Flush thread's publish pattern | Dead |
| **Unload/mark_backed pattern** | `filter.rs:308-341`, `concurrent_engine.rs:975-1006` | Save to silo then create unloaded placeholders | No in-memory state to unload from |
| **save_all() / save_all_parallel()** | `bitmap_silo.rs:115-250` | Serialize in-memory bitmaps to silo | No in-memory bitmaps to save; janitor compacts silo directly |
| **enter_loading_mode() / exit_loading_mode()** | `concurrent_engine.rs` | Skip snapshot publishing during bulk insert | No snapshot publishing to skip |
| **snapshot_public()** | `concurrent_engine.rs:687` | Clone live state as InnerEngine | Dead with InnerEngine |
| **name_to_key / key_to_name** | `bitmap_silo.rs:44-46` | Heap HashMap string→u32 manifest | Replace with deterministic u64 key encoding |
| **bitmap_manifest.json** | `bitmap_silo.rs:66-76, 88-92` | JSON file mapping strings to silo keys | Dead; keys computed from (field_id, value) |
| **PendingBucketDiffs** | `bucket_diff_log.rs` | In-memory diff accumulator for lazy cache application | Dead; cache doesn't consume diffs |
| **ArcSwap\<PendingBucketDiffs\>** | `concurrent_engine.rs:87` | Lock-free diff snapshot for queries | Dead; time bucket ops go to silo |
| **BucketDiffLog** | `bucket_diff_log.rs` | On-disk diff log for boot restore | Dead; bucket bitmaps persisted in silo |

### Dump Processor

| Component | File | Why Kill |
|-----------|------|----------|
| `clone_staging()` call | `dump_processor.rs:2402` | Write directly to BitmapSilo |
| `apply_bitmap_maps()` call | `dump_processor.rs:2414` | Write directly to BitmapSilo |
| `publish_staging()` call | `dump_processor.rs:2431` | Dead |

### Loader

| Component | File | Why Kill |
|-----------|------|----------|
| `clone_staging()` call | `loader.rs:446` | Write directly to BitmapSilo |
| `apply_bitmap_maps()` call | `loader.rs:461` | Write directly to BitmapSilo |
| `publish_staging()` call | `loader.rs:500` | Dead |

---

## Build List (New for V3-Only)

### 1. Deterministic u64 Key Encoding for BitmapSilo

**Replace** the string manifest with deterministic key computation.

**Field Registry:** A small persistent file (`field_registry.bin`) mapping field names to stable u16 IDs. ~40 entries max. Loaded once at startup. New fields get the next available ID. Deleted fields are tombstoned.

**Key encoding:**
- Filter: `(field_id as u64) << 48 | (value as u64 & 0xFFFF_FFFF_FFFF)` — 16 bits field, 48 bits value
- Sort: `0x8000_0000_0000_0000 | (field_id as u64) << 32 | (bit_layer as u64)` — high bit = sort namespace
- Alive: constant key `1`
- Metadata: constant key `2`

**Lookup path:** `encode(field_id, value) → u64 → mmap HashIndex.get(key) → (offset, len) → mmap data slice`. Zero heap. No locks. Pure pointer arithmetic on mmap.

**In memory at runtime:** Only the field registry (~40 entries, <2KB). Everything else is mmap.

### 2. Sort Traversal from Frozen Layers Only

The bit-layer MSB-to-LSB traversal in `SortField::top_n_frozen()` already supports frozen layers. Need to:
- Make it work WITHOUT an in-memory SortField at all
- Create a standalone `frozen_sort_traversal(silo, field_name, num_bits, candidates, limit, desc)` function
- Read all bit layers from BitmapSilo frozen views directly

### 3. Alive Bitmap via Ops-on-Read

Replace the eagerly-loaded in-memory alive bitmap with `BitmapSilo::get_alive_with_ops()`. This already exists — just need to make the query path use it instead of `slots.read().alive_bitmap()`.

### 4. Range Scan Key Enumeration from Silo

Two options:
- **Per-field sorted value list** in the field registry (updated on writes). Small for most fields.
- **HashIndex scan** — iterate all index slots, filter by field_id prefix in the key encoding. The index is mmap'd and entries are 24 bytes, so scanning 100K entries is ~2.4MB sequential read.

For now: HashIndex scan is acceptable. Range queries are rare and the scan is fast on mmap. Optimize later if needed.

### 5. Cache Invalidation via Generation/Epoch

Replace the dead flush-thread invalidation with a simple epoch counter:
- BitmapSilo increments an atomic epoch counter on every ops-log append
- Cache entries store the epoch at creation time
- On cache hit, compare entry epoch vs current epoch
- If stale, re-validate or re-seed

Per-field epoch counters enable targeted invalidation: only invalidate cache entries that reference the mutated field. The cache key already includes field names via `CanonicalClause`, so matching is straightforward.

For time-bucket-backed cache entries (keyed by bucket name like `"7d"`), the bucket's epoch tracks when its bitmap last changed. Bucket ops (insert/expiry) increment the bucket epoch.

### 6. Time Buckets in BitmapSilo

**Current state:** Time buckets are in-memory `Arc<RoaringBitmap>` inside a `Mutex<TimeBucketManager>`. Maintained by the flush thread which reads sort values from in-memory SortIndex. PendingBucketDiffs computed via ArcSwap for lazy cache application (but cache doesn't consume them).

**V3 model:** Time buckets are just bitmaps — they belong in BitmapSilo.

**Storage:** Each bucket gets a silo key in a dedicated namespace:
`BUCKET_PREFIX | field_id << 16 | bucket_id`

The bucket bitmap is a frozen bitmap in the data file, read via ops-on-read like everything else.

**Live maintenance on insert/delete:**
- When a new record arrives (ops processor or dump), after writing filter/sort ops to BitmapSilo, check if the record's sort timestamp falls within any bucket window
- If yes: append a SET op to that bucket's silo key
- On delete: append CLEAR ops to all buckets the slot might be in
- This replaces the current flush thread insert/delete maintenance (`flush.rs:131-152`)

**Periodic expiry (aging out):**
- A lightweight timer (janitor or dedicated ticker) periodically:
  - For each bucket, compute `new_cutoff = snap(now - duration, refresh_interval)`
  - If cutoff advanced, read the bucket bitmap from silo (frozen, zero-copy)
  - For each slot in the bitmap, reconstruct sort value FROM FROZEN SORT LAYERS
  - Slots with `sort_value < new_cutoff` get CLEAR ops appended to the bucket's silo key
- This replaces the current flush thread incremental refresh (`flush.rs:255-338`)

**Query-time snapping:** `snap_range_clauses` currently reads `Arc::clone(bucket.bitmap())` from in-memory TimeBucketManager. In V3, it reads `silo.get_bucket_with_ops(field, bucket_name)` — same ops-on-read pattern as filter/sort bitmaps.

**What dies:**
- `PendingBucketDiffs` — no more lazy diff accumulation; expiry writes ops directly
- `ArcSwap<PendingBucketDiffs>` — no more snapshot pattern
- `BucketDiffLog` — no more on-disk diff log; bucket state is in silo
- `TimeBucketManager` (in current form) — replaced by bucket metadata in field registry + silo reads
- Flush thread time bucket maintenance — replaced by ops-based maintenance

**What stays (simplified):**
- Bucket configuration (names, durations, refresh intervals) — in config
- `snap_range_clauses` / `snap_duration` / `snap_nearest` — query-time clause rewriting (but reading from silo instead of in-memory)
- Periodic expiry timer — but reading frozen sort layers instead of in-memory SortField

### 7. Flush Thread Reduction

After removing all V2 state, the flush thread's remaining legitimate work:
- **Docstore writes** — draining the doc channel, batch writing to DocSilo
- **Compaction triggers** — checking if BitmapSilo/CacheSilo/DocSilo need compaction

Remove:
- All bitmap state application (FilterIndex/SortIndex/SlotAllocator writes)
- Time bucket maintenance (moves to ops-based model per item 6)
- PendingBucketDiffs computation
- stale_fields tracking (dead)

### 8. Reconstruct Value from Frozen Layers

Cache seeding and cursor reconstruction call `sort_field.reconstruct_value(slot)` which reads from in-memory sort bit-layers. Need a frozen equivalent:

```rust
fn reconstruct_value_from_silo(silo: &BitmapSilo, field: &str, num_bits: usize, slot: u32) -> u32 {
    let mut value = 0u32;
    for bit in 0..num_bits {
        if let Some(frozen) = silo.get_frozen_sort_layer(field, bit) {
            if frozen.contains(slot) {
                value |= 1 << bit;
            }
        }
    }
    value
}
```

Also needed by: time bucket expiry (to check which slots aged out).

### 9. Planner Cardinality from Silo

Two options:
- **Cardinality in HashIndex entries:** Extend the 24-byte entry with a `cardinality: u32` field (or store it as metadata in a reserved silo key per field). Updated on compaction.
- **Accept best-effort:** Cardinality estimation is used for clause reordering. Even stale estimates produce good-enough plans. Could use `frozen_bitmap.len()` on first access and cache the count in a small in-memory map (~40 entries). Updated lazily.

---

## Implementation Order

### Phase 1: Fix What's Broken (No Structural Changes)

1. **Fix cache invalidation** — implement epoch-based staleness check on cache hits. This is a correctness bug in production right now.
2. **Verify cache hit behavior** — audit what happens when stale entries are served.

### Phase 2: BitmapSilo Key Encoding

3. **Implement field registry** — persistent file, startup load, stable IDs
4. **Implement deterministic u64 key encoding** — replace string manifest
5. **Migrate BitmapSilo read path** — `get_filter_with_ops()` etc. use u64 keys directly
6. **Migrate BitmapSilo write path** — dump processor, ops processor write u64 keys
7. **Delete manifest machinery** — `name_to_key`, `key_to_name`, `bitmap_manifest.json`

### Phase 3: Kill In-Memory Read Paths

8. **Sort traversal from frozen only** — standalone function, no SortField required
9. **Alive via ops-on-read** — query path uses `get_alive_with_ops()`
10. **Range scan from HashIndex** — enumerate values by scanning index, not in-memory field
11. **Cache seeding from frozen** — `reconstruct_value_from_silo()` replaces in-memory sort reads
12. **Planner cardinality** — silo-based estimates (frozen bitmap len or metadata)

### Phase 4: Time Buckets to BitmapSilo

13. **Bucket storage in silo** — key encoding, initial bitmap write during dump/ops
14. **Insert/delete maintenance via ops** — append SET/CLEAR ops to bucket keys on mutation
15. **Periodic expiry via frozen sort** — reconstruct_value_from_silo for aging-out detection
16. **Query snapping reads from silo** — `snap_range_clauses` reads bucket bitmaps via ops-on-read
17. **Delete PendingBucketDiffs, ArcSwap, BucketDiffLog** — no lazy diffs needed
18. **Delete TimeBucketManager** (in current form) — replaced by config + silo reads

### Phase 5: Kill In-Memory Write Paths

19. **Mutations write to BitmapSilo only** — remove dual-write in `send_mutation_ops()`
20. **Remove MutationOp channel** — no flush thread consumer
21. **Reduce flush thread** — keep only: docstore drain + compaction triggers
22. **Delete FlushBatch** — no in-memory state to batch-apply

### Phase 6: Kill V2 Infrastructure

23. **Delete FilterIndex, SortIndex, SlotAllocator** (as RwLock'd engine fields)
24. **Delete VersionedBitmap, BitmapDiff**
25. **Delete InnerEngine, clone_staging, publish_staging, apply_bitmap_maps**
26. **Delete save_all, save_all_parallel** — no in-memory bitmaps to save
27. **Delete enter_loading_mode, exit_loading_mode, snapshot_public**
28. **Delete unload/mark_backed patterns**
29. **Update ConcurrentEngine** — remove RwLock fields, simplify to: config + BitmapSilo + CacheSilo + DocSilo + flush thread (docstore only)

### Phase 7: Dump Processor Direct-Write

30. **Dump writes directly to BitmapSilo** — no staging, no publish
31. **Dump writes time bucket ops** — bucket maintenance during dump phase
32. **Loader writes directly to BitmapSilo** — same treatment

---

## Performance Expectations

**Reads:** Should be faster. Eliminates RwLock acquisition, VersionedBitmap diff fusion, and HashMap lookups. Replaces with mmap pointer arithmetic.

**Writes:** Neutral to faster. Single-write to BitmapSilo ops log instead of dual-write. No flush thread contention.

**Memory:** Major reduction. The entire in-memory FilterIndex (31K tagIds × VersionedBitmap = most of bitmap memory) goes away. Only the field registry (~2KB) and mmap'd files remain.

**Dump pipeline:** Eliminates the 3.6s apply_bitmaps + staging overhead. Writes go directly to BitmapSilo during the dump phase.

---

## Open Questions

1. **Compaction during queries** — BitmapSilo compaction rewrites the data file. Queries reading frozen bitmaps via mmap during compaction need to be safe. DataSilo may already handle this (the old mmap stays valid until unmapped). Needs verification.

2. **Test infrastructure** — many tests create a ConcurrentEngine without a BitmapSilo. Need a test helper that creates a minimal silo, or accept that some tests use an in-memory-only mode.

3. **Migration path** — existing production data uses the string manifest format. Need a one-time migration to convert manifest keys to deterministic u64 keys, or support both formats during transition.

4. **Time bucket expiry performance** — periodic expiry needs to reconstruct sort values from frozen layers for every slot in the bucket bitmap. At 107M with a "30d" bucket containing ~80M slots, that's 80M × 32 frozen bitmap contains() checks. Need to benchmark whether this is fast enough from mmap or if we need a batched/streaming approach.

5. **Cardinality storage** — should cardinality live in the HashIndex entry (requires format change from 24 to 28+ bytes), in a separate metadata key per field, or just computed lazily from frozen bitmap len on first access? The lazy approach avoids any format changes but may be slow for the first query after compaction.
