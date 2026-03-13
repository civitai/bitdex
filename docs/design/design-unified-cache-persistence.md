# BoundStore — Unified Cache Persistence Design

**Status:** Approved for implementation
**Date:** 2026-03-12
**Authors:** Justin + Claude `1021b98a-d668-4c91-a789-8408e84a35a5`
**Reviewed by:** Gemini 3.1 Pro, GPT 5.4 (external gap review)

---

## 1. Motivation

The unified cache stores pre-computed bounded bitmaps that combine filter + sort results. These entries accelerate sorted, filtered queries from ~13ms (cold traversal at 105M) to ~3ms (cache hit). Currently, all cache entries are lost on server restart, forcing every query to pay the cold formation cost until the cache warms up.

**Goal:** Persist unified cache entries to disk so the server starts warm. First query after restart should be a cache hit, not a miss.

> **Sort Index Context (2026-03-12):** In parallel with this persistence work, the
> in-memory unified cache is being extended with a **sorted Vec** per entry — a
> `Vec<u64>` of packed `(sort_value << 32 | slot_id)` that eliminates the 2-5ms
> sort traversal on cache hits, giving ~55ns cursor-based pagination. This sort
> index is initially **in-memory only**, but its persistence story must be
> considered in this design. See §6.3 and §13 for analysis.

---

## 2. Naming

**BoundStore** — these cache entries are bounded bitmaps (approximate top-K result sets for sort acceleration). The name pairs with BitmapFs (raw index bitmaps) and DocStore (document storage).

- Module: `src/bound_store.rs`
- Directory: `bitmaps/bounds/`
- File extensions: `.meta` (meta-index), `.ucpack` (shard files)

---

## 3. Design Principles

1. **Disk is the superset of RAM.** Everything in the live cache is on disk. RAM eviction (LRU) does NOT trigger disk writes — evicted entries stay on disk and can be loaded back later.

2. **Never serve stale data.** If an entry can't be maintained (because its shard isn't loaded), tombstone it immediately and persist the tombstone. Stale entries must never be served to a reader.

3. **Don't load shards to write them.** Mutations should not force-load unloaded shards into RAM. That wastes memory and fights with hot entries. Tombstone instead.

4. **Reuse existing patterns.** Atomic writes (.tmp + fsync + rename), pack file format, lazy loading, per-component dirty flags — all proven in BitmapFs/DocStore.

5. **Fail gracefully.** Corrupt files are skipped, not fatal. A missing or broken cache file means cold start for that sort field — nothing worse.

---

## 4. Architecture Overview

### 4.1 On-Disk Layout

```
bitmaps/bounds/
├── meta.bin                          # Meta-index: registrations + tombstones
├── reactionCount_Desc.ucpack         # Shard: all entries sorting by reactionCount Desc
├── reactionCount_Asc.ucpack          # Shard: all entries sorting by reactionCount Asc
├── sortAt_Desc.ucpack                # Shard: all entries sorting by sortAt Desc
└── ...                               # One .ucpack per (sort_field, direction) pair
```

**~20-40 shard files** total (one per sort field × direction).
**1 meta file** containing all entry registrations and tombstones.

### 4.2 Components

| Component | File | Contents | Load Strategy |
|-----------|------|----------|---------------|
| **Meta-index** | `meta.bin` | Entry registrations (ID → key, fields, shard) + tombstone bitmap | **Eager** — loaded on startup |
| **Bitmap shards** | `{field}_{dir}.ucpack` | Packed cache entries (keys + roaring bitmaps + metadata) | **Lazy** — loaded on first query for that sort field |

---

## 5. Entry Lifecycle — Three States

Each cache entry exists in one of three states:

| State | In meta-index | In RAM | On disk | Queryable |
|-------|---------------|--------|---------|-----------|
| **Live** | Registered | Maybe (hot) or not (orphan) | Yes | Yes (if in RAM or shard loaded) |
| **Tombstoned** | Registered + tombstone bit set | No | Yes (stale bytes) | No — skipped on shard load |
| **Free** | Deregistered, ID recycled | No | No (removed on shard rewrite) | No |

**Transitions:**
```
                form_and_store()
    [Free] ─────────────────────► [Live]
                                    │
              LRU evict             │  mutation to unloaded entry
           (stays Live, just       │
            not in RAM)             ▼
                                [Tombstoned]
                                    │
              shard rewrite         │
           (entry omitted,          │
            ID recycled)            ▼
                                  [Free]
```

Key invariant: **the meta-index always tracks Live and Tombstoned entries.** It does NOT deregister on LRU eviction. Deregistration only happens when an entry transitions to Free (tombstone cleanup on shard rewrite).

This ensures the flush thread can always find affected entries for tombstoning, even if they've been evicted from RAM.

---

## 6. File Formats

### 6.1 Meta File (`meta.bin`)

Contains everything needed to know WHAT exists and WHERE, without loading any bitmaps.

```
[u32 version = 1]
[u32 num_entries]
[entries: N × {
    u32 entry_id            (CacheEntryId — sequential, recycled)
    u16 sort_field_len
    [u8] sort_field          (UTF-8 sort field name)
    u8  direction            (0 = Desc, 1 = Asc)
    u32 key_len
    [u8] key_bytes           (msgpack-serialized Vec<CanonicalClause>)
    u32 capacity
    u32 max_capacity
    u32 min_tracked_value
    u64 total_matched       (cached filter cardinality — avoids recomputing on cache hit)
    u8  has_more
}]
[u32 tombstone_bitmap_len]
[u8] tombstone_bitmap_bytes  (serialized RoaringBitmap of dead entry IDs)
[u32 next_entry_id]          (for ID allocation continuity across restarts)
```

**Size estimate:** ~200 bytes/entry. At 5K entries: ~1MB. At 100K entries: ~20MB.

**Loaded eagerly on startup.** Populates the in-memory MetaIndex with all registrations, restores the tombstone set, and records which shards have pending entries (for lazy loading).

### 6.2 Shard File (`{field}_{direction}.ucpack`)

Contains the actual roaring bitmaps for all cache entries sharing a sort field + direction.

```
[u32 version = 1]
[u32 num_entries]
[index: N × {
    u32 entry_id             (matches meta.bin entry_id)
    u32 key_offset           (offset into key section)
    u32 key_length
    u32 bitmap_offset        (offset into bitmap section)
    u32 bitmap_length
}]
[key section: concatenated msgpack-serialized Vec<CanonicalClause>]
[bitmap section: concatenated serialized roaring bitmaps]
```

**Note:** Metadata (capacity, min_tracked_value, has_more) is stored in `meta.bin`, not duplicated in the shard. The shard contains only what's needed for bitmap deserialization: the key (for HashMap insertion) and the bitmap bytes.

**Size estimate:** ~6.4KB/entry average (dominated by roaring bitmap). At 1K entries per shard: ~6.4MB.

### 6.3 Sort Index Persistence — Lazy Reconstruction (Recommended)

> **Background:** Each in-memory cache entry now carries a sorted `Vec<u64>` of
> packed `(sort_value << 32 | slot_id)` pairs, enabling 55ns cursor pagination
> instead of 2-5ms bitmap traversal. The question is whether to persist this
> alongside the cache bitmap.

**Decision: Do NOT persist the sort index. Reconstruct lazily on restore.**

Rationale from benchmarking (see `benches/radix_sort_bench.rs`):

| Consideration | Analysis |
|---------------|----------|
| **Storage ratio** | Sorted Vec at 4K = 31 KB vs cache bitmap ~8 KB = **3.9x overhead**. Persisting it nearly quintuples the shard file size for marginal benefit. |
| **Reconstruction cost** | ~700µs per 4K-entry restore (4000 × `reconstruct_value` at ~89ns + sort). One-time cost on first access after shard load. Negligible vs shard I/O. |
| **Write amplification** | Every live maintenance update that modifies the bitmap would also need to update the sorted Vec on disk. At 289ns/slot for Vec inserts, this adds ~145µs per flush cycle for the sort index alone — write cost approaches the read savings. |
| **Disk is the superset** | If we aim for 50K-100K disk entries but only 5K in RAM, persisting 31 KB of sort index per entry costs 1.5-3.1 GB of disk for data that's only useful when the entry is hot in RAM. |

**Reconstruction flow on shard load:**

```
1. Shard loaded → entry inserted into UnifiedCache with bitmap only (sort_index = None)
2. First query hits this entry → fast path sees sort_index is None
3. Reconstruct: for each slot in bitmap, call sort_field.reconstruct_value(slot)
4. Pack into Vec<u64>, sort descending → ~700µs for 4K entry
5. Store sort_index on the entry → subsequent queries get 55ns pagination
```

**Cache stampede mitigation:** Multiple concurrent readers hitting the same cold
entry could all attempt reconstruction. Use the existing loading sentinel pattern
(§12.3) — first thread reconstructs, others get a cache miss for one request
and find the sort index populated on next attempt. Alternatively, use a per-entry
`AtomicBool` reconstruct guard (similar to `try_start_rebuild`).

**Alternative considered: Persist the sort index.**

If future profiling shows the 700µs reconstruction cost is problematic (e.g., at
64K entries or with many concurrent shard loads), the shard format can be extended:

```
[bitmap section: concatenated serialized roaring bitmaps]
[sort_vec section: concatenated packed u64 arrays]     ← NEW optional section
```

Each index entry would gain:
```
    u32 sort_vec_offset      (offset into sort_vec section; 0 = not present)
    u32 sort_vec_length      (number of u64 entries, NOT bytes)
```

This is a **v2 format extension** — not needed for initial implementation. The
version field in the shard header (currently 1) would bump to 2, and the loader
would handle v1 files (no sort_vec) by lazy reconstruction.

### 6.4 Radix Bucket Index — Future Consideration

> **Background:** Benchmarking also explored an 8-bit radix approach that
> partitions the cache bitmap into 256 sub-bitmaps by sort-value prefix. This
> gives 110x speedup for deep pagination (offset=32K) and enables O(1) bucket
> skipping. However, memory is 2.2x the cache bitmap at 4K (vs 3.9x for sorted
> Vec) and the implementation is more complex.

**The hybrid approach** being implemented in-memory is: sorted Vec at initial
capacity (4K), with potential radix buckets if/when entries expand to 64K.
For persistence, the same principle applies — don't persist the radix structure,
reconstruct it lazily from the bitmap + sort layers when an expanded entry is
loaded from disk.

Radix reconstruction at 64K is more expensive (~1.2ms with precomputed values,
~9ms with `reconstruct_value` per slot). If entries commonly expand to 64K on
disk, consider persisting the radix bucket structure in a future format version.
But given that most disk entries will be at initial capacity (4K) and radix is
only needed for the small fraction that expand, lazy reconstruction is
acceptable for v1.

---

## 7. Startup Sequence

### 7.1 Normal Startup

```
1. Load meta.bin eagerly
   → Populate MetaIndex with all registrations
   → Restore tombstone bitmap
   → Restore next_entry_id counter
   → Record pending shards: HashSet<(sort_field, direction)>

2. Do NOT load any .ucpack files

3. Server is ready — instant startup
```

### 7.2 Missing Meta File (Cache Purge)

```
1. meta.bin not found
2. If .ucpack files exist → delete them all (orphaned, no meta references)
3. Start with empty cache — cold start

This provides a simple admin operation: delete meta.bin to purge the cache.
Could also be exposed as an admin endpoint (DELETE /cache/persistent).
```

### 7.3 First Query for a Sort Field

```
1. Query arrives: filters=[nsfwLevel=1], sort=reactionCount Desc
2. Check pending shards: reactionCount_Desc is pending
3. Load reactionCount_Desc.ucpack:
   a. Deserialize index + keys + bitmaps
   b. For each entry:
      - If entry_id is NOT in meta-index → skip (orphan from crash)
      - If entry_id is tombstoned → skip (dead entry)
      - Otherwise → insert into UnifiedCache HashMap with metadata from meta-index
      - sort_index is set to None (lazy reconstruction — see §6.3)
   c. Remove reactionCount_Desc from pending shards
4. Proceed with normal cache lookup
5. Cache hit: sort_index is None → reconstruct from bitmap + sort layers (~700µs)
6. Store sort_index on entry → subsequent queries get 55ns pagination
```

---

## 8. Persistence Flow

### 8.1 When to Save

BoundStore persistence integrates into the existing merge thread:

```
Merge thread wakes (every ~500ms when dirty):
  1. Check dirty_since_snapshot flag (existing)
  2. Write filter/sort bitmap snapshots (existing)
  3. Write time bucket bitmaps (existing)
  4. NEW: If meta_dirty flag is set:
     → Rewrite meta.bin atomically
     → Clear meta_dirty
     (meta.bin is written BEFORE shard files — see §12.5)
  5. NEW: For each shard with shard_dirty flag set:
     → Read-modify-write: read old .ucpack, merge with RAM state
       - Entries in RAM: use the RAM version (may have been maintained)
       - Entries on disk but not in RAM and not tombstoned: preserve as-is (orphans)
       - Tombstoned entries: omit from new file
     → Write merged .ucpack atomically
     → Clear tombstones for omitted entries, deregister them, recycle IDs
     → Clear shard_dirty
```

### 8.2 What Sets Dirty Flags

| Event | meta_dirty | shard_dirty |
|-------|------------|-------------|
| New entry formed (`form_and_store`) | Yes | Yes (entry's shard) |
| Entry expanded (`expand()`) | Yes (capacity/min_tracked changed) | Yes |
| Entry tombstoned | Yes | No (shard untouched initially) |
| Live maintenance modifies bitmap | No | Yes |
| Live maintenance modifies bitmap (sort index) | No | No (sort index not persisted) |
| LRU eviction from RAM | No | No (disk is superset) |
| Entry rebuilt after `needs_rebuild` | Yes | Yes |
| Tombstone count > 50% of shard entries | — | Yes (forced cleanup) |

### 8.3 Dirty Flag Implementation

```rust
// On UnifiedCache:
meta_dirty: AtomicBool,
shard_dirty: HashMap<(Arc<str>, SortDirection), AtomicBool>,
```

---

## 9. Tombstone Flow

### 9.1 When Tombstones Are Created

During flush thread maintenance, when a mutation affects a field referenced by cache entries:

```
Mutation hits nsfwLevel:
  → meta-index: entries_for_filter_field("nsfwLevel") → {42, 87, 153}
  → entry 42: in RAM (HashMap contains it) → maintain normally (add/remove slot)
  → entry 87: NOT in RAM → tombstone(87), set meta_dirty
  → entry 153: NOT in RAM → tombstone(153), set meta_dirty
```

**Determining loaded vs unloaded:** The flush thread checks whether the entry ID exists in the UnifiedCache HashMap. If not, it's either in an unloaded shard or was LRU-evicted — either way, tombstone it.

### 9.2 Tombstone Storage

```rust
// In MetaIndex:
tombstoned: RoaringBitmap,  // Set of dead CacheEntryId values
```

Persisted as part of `meta.bin`. The tombstone bitmap is tiny — even 10K tombstoned entries is a few hundred bytes in roaring.

### 9.3 Tombstone Lifecycle

```
1. Created: flush thread tombstones entry ID during maintenance
   → tombstoned.insert(entry_id)
   → meta_dirty = true

2. Persisted: merge thread writes meta.bin
   → tombstone bitmap serialized to disk

3. Cleaned up: merge thread rewrites the affected shard (read-modify-write)
   → dead entries omitted from new .ucpack file
   → deregister(entry_id) called on meta-index → entry transitions to Free
   → tombstoned.remove(entry_id)
   → entry_id recycled to free list

4. On restart: meta.bin loaded with tombstones intact
   → shard load skips tombstoned entries
   → lifecycle continues from step 3 on next shard rewrite
```

### 9.4 Shard Dirty After Tombstone

Tombstoning sets `meta_dirty` but NOT `shard_dirty` immediately. Dead entries are cleaned up opportunistically when the shard is rewritten for another reason (maintenance, expansion, new entry).

To prevent unbounded tombstone accumulation, the merge thread also promotes a shard to dirty if >50% of its entries are tombstoned.

---

## 10. LRU Eviction and Disk

**RAM eviction does NOT write to disk and does NOT deregister from meta-index.**

When the in-memory cache evicts an entry via LRU:
- The entry is removed from the UnifiedCache HashMap
- The entry **remains registered** in the in-memory meta-index
- The entry remains on disk in its .ucpack shard file
- The entry is now an "orphan" — Live state, on disk, not in RAM

**Dirty entry protection:** Entries whose bitmaps have been modified by live maintenance since the last shard flush are marked with a per-entry `dirty` flag. LRU eviction **skips dirty entries** to prevent losing unsaved maintenance work. The dirty flag is cleared when the merge thread successfully writes the shard.

**Orphan behavior:**
- If a mutation affects the orphan's field → tombstoned (flush thread sees it's not in RAM)
- If the shard is rewritten → orphan is preserved via read-modify-write (its on-disk bitmap is copied to the new shard file)
- If the shard is loaded (restart or lazy load) → orphan is restored to RAM with `sort_index: None`
- On next query matching its key → cache hit (if shard loaded) or miss (if shard not loaded, entry rebuilt fresh)
- Sort index is dropped on eviction and reconstructed lazily (~700µs) when the entry is next loaded and queried

> **Note on sort index and LRU:** The sort index (sorted Vec) is a pure RAM
> optimization — it is never written to disk, and is dropped when an entry is
> evicted from RAM. This is intentional: the sort index is cheap to reconstruct
> (~700µs at 4K capacity) and expensive to persist (3.9x storage overhead +
> write amplification on every maintenance cycle). The bitmap on disk is
> sufficient to reconstruct the sort index on demand.

---

## 11. Corruption Handling

**No checksums.** Corruption is detected by deserialization failure:

- **`meta.bin` corrupt or missing:** Log warning, delete file and all `.ucpack` files, start with empty cache. This is also the admin purge mechanism — delete `meta.bin` to reset the cache.
- **`.ucpack` corrupt:** Log warning, delete file. Queries for that sort field get cache misses and rebuild entries organically. Tombstone all entries for that shard.
- **Entry in `.ucpack` but not in `meta.bin`:** Shard loader skips entries whose `entry_id` is not registered in the meta-index. This handles the crash-between-shard-and-meta-write case (see §12.5).

**Truncation detection:** The entry count in the header vs actual bytes available provides implicit truncation detection during deserialization.

---

## 12. Concurrency

### 12.1 Who Accesses What

| Thread | Meta-index | UnifiedCache HashMap | Shard files |
|--------|------------|---------------------|-------------|
| **Query thread** | Read (lookup) | Read (lookup) + Write (form_and_store, expand) | Read (lazy load) |
| **Flush thread** | Read (entries_for_field) + Write (tombstone) | Read (contains check) + Write (maintain) | — |
| **Merge thread** | Write (deregister cleaned tombstones) | Read (collect entries for shard write) | Read+Write (read-modify-write shards, write meta) |

### 12.2 Locking

- **UnifiedCache** is already behind `parking_lot::Mutex` — brief locks for lookup/maintain/form_and_store
- **Meta-index** is inside UnifiedCache, shares the same lock
- **Shard loading** — deserialize outside lock, insert under lock (see §12.3)
- **Shard writing** by merge thread — snapshot entries under lock, read old shard + serialize + write outside lock

### 12.3 Shard Load Race (Loading Sentinel)

Multiple query threads may simultaneously discover a pending shard. A loading sentinel prevents redundant I/O:

```rust
let mut uc = self.unified_cache.lock();
if uc.is_shard_pending(sort_field, direction) {
    if uc.is_shard_loading(sort_field, direction) {
        // Another thread is already loading this shard.
        // Drop lock and proceed without cache (full filter traversal).
        // Next request will find the shard loaded.
        drop(uc);
        // ... execute query without cache hit ...
    } else {
        // We are the loading thread. Set sentinel and release lock for I/O.
        uc.mark_shard_loading(sort_field, direction);
        drop(uc);

        let shard_data = store.load_bound_store_shard(...)?;

        let mut uc = self.unified_cache.lock();
        uc.load_shard_entries(shard_data);
        uc.mark_shard_loaded(sort_field, direction);
    }
}
```

Other threads get a cache miss for one request instead of all deserializing the same file concurrently.

### 12.4 Merge Thread Shard Write

The merge thread snapshots under lock, then does I/O outside:

```rust
// Under lock: collect RAM entries for this shard + tombstone set
let mut uc = self.unified_cache.lock();
let ram_entries: Vec<(EntryId, Key, Bitmap, Metadata)> = uc.entries_for_shard(shard_key);
let tombstones = uc.tombstoned_ids_for_shard(shard_key);
drop(uc);

// Outside lock: read old shard, merge, write new shard
let old_shard = store.load_bound_store_shard(shard_key)?;  // read existing
let merged = merge_shard(old_shard, ram_entries, tombstones);  // overlay + filter
store.write_bound_store_shard(shard_key, &merged)?;  // atomic write

// Under lock: clean up tombstones for entries we omitted
let mut uc = self.unified_cache.lock();
uc.finalize_shard_write(shard_key, &omitted_ids);
```

### 12.5 Write Ordering: Meta Before Shards

The merge thread writes `meta.bin` BEFORE any shard files. This ensures:

- If **meta succeeds, shard fails** (crash): meta has registrations for entries that don't exist on disk yet. Shard loader skips entries not found in the `.ucpack` → harmless cache misses.
- If **shard succeeds, meta fails** (impossible — meta is written first): N/A.
- If **both fail** (crash during meta write): old meta.bin survives (atomic rename didn't happen). Old shard files survive. State is consistent from the previous successful write.

---

## 13. Scale Analysis

### Disk Footprint

| Entries | Meta File | Shard Files (total) | Total |
|---------|-----------|---------------------|-------|
| 1,000 | ~200 KB | ~6.4 MB | ~6.6 MB |
| 5,000 | ~1 MB | ~32 MB | ~33 MB |
| 50,000 | ~10 MB | ~320 MB | ~330 MB |
| 100,000 | ~20 MB | ~640 MB | ~660 MB |

### Startup Cost

| Component | Time (est.) |
|-----------|-------------|
| Load meta.bin (5K entries) | ~2ms |
| Load meta.bin (100K entries) | ~20ms |
| Load one shard (1K entries, 6.4MB) | ~5ms |
| Load one shard (5K entries, 32MB) | ~25ms |

Startup is instant (meta-only). First query for each sort field pays the shard load cost once.

### Sort Index Reconstruction Cost (Post Shard Load)

After a shard is loaded, entries have bitmaps but no sort index. First query
triggers lazy reconstruction:

| Entry capacity | Reconstruction time | Notes |
|----------------|-------------------|-------|
| 4K slots | ~700µs | 4000 × reconstruct_value (89ns) + sort |
| 16K slots | ~3ms | Linear in slot count |
| 64K slots | ~12ms | Consider persisting at this scale |

These costs are per-entry, one-time. A shard with 1K entries at 4K capacity
would need ~700ms total to reconstruct all sort indices if every entry is
queried simultaneously. In practice, only hot entries get queried, and the
loading sentinel prevents concurrent reconstruction of the same entry.

> **Benchmark source:** `benches/radix_sort_bench.rs`, groups
> `scale_aware_bifurcate` (reconstruct_sort) and `sorted_vec` (formation).
> At 1M total slots: reconstruct_sort for 4K candidates = 692µs.

### Write Amplification

| Scenario | Shard size | Rewrite cost |
|----------|-----------|--------------|
| 5K entries, 20 shards | ~1.6 MB/shard | Negligible |
| 100K entries, 20 shards | ~32 MB/shard | Noticeable |

> **Sort index write impact (if persisted in future v2):** Adding sorted Vec
> data to shards would increase shard sizes by ~3.9x at 4K capacity:
>
> | Scenario | Bitmap-only | + Sort Vec | Ratio |
> |----------|-------------|-----------|-------|
> | 5K entries, 20 shards | ~1.6 MB/shard | ~6.2 MB/shard | 3.9x |
> | 100K entries, 20 shards | ~32 MB/shard | ~125 MB/shard | 3.9x |
>
> This is the primary reason for recommending lazy reconstruction over
> persistence for v1. The 3.9x write amplification compounds with the
> read-modify-write shard pattern on every merge cycle.

At 100K scale, if write amplification becomes a problem, sub-shard by hash within sort fields:
```
bounds/reactionCount_Desc/00.ucpack
bounds/reactionCount_Desc/01.ucpack
...
```
This is a future optimization — not needed for v1.

---

## 14. Implementation Plan

### Phase 1: File I/O (`bound_store.rs`)
- `write_meta()` — serialize meta-index registrations + tombstones
- `load_meta()` — deserialize meta file
- `write_shard()` — serialize entries for one (sort_field, direction)
- `load_shard()` — deserialize shard file
- `list_shards()` — scan directory for existing shard files
- `purge()` — delete meta.bin + all .ucpack files

### Phase 2: Cache Integration (`unified_cache.rs` + `meta_index.rs`)
- Add `tombstoned: RoaringBitmap` to MetaIndex
- Add `pending_shards` / `loading_shards` sets to UnifiedCache
- Add `shard_dirty` HashMap + `meta_dirty` AtomicBool
- Add per-entry `dirty` flag (prevents LRU eviction of unsaved entries)
- Remove `deregister()` from LRU eviction path (meta-index tracks all Live + Tombstoned)
- Add `entries_for_shard()` accessor
- Add `load_shard_entries()` for inserting deserialized entries
- Serialization/deserialization methods for meta-index registrations

### Phase 3: Startup + Lazy Load (`concurrent_engine.rs`)
- On construction: load meta.bin, populate meta-index, record pending shards
- On query: check pending shards, load on demand (loading sentinel pattern)
- Sync loaded shard data to flush thread via existing lazy_tx channel
- Handle missing meta.bin: purge orphaned .ucpack files
- **Sort index reconstruction:** Entries loaded from shard have `sort_index: None`. The fast path in `execute_query` must handle this — on first cache hit with `sort_index == None`, reconstruct the sorted Vec from the entry bitmap + sort layers (~700µs), store it, then proceed with 55ns pagination. Use a per-entry `AtomicBool` guard (like `try_start_rebuild`) to prevent concurrent reconstruction of the same entry. Second concurrent reader gets a cache miss for one request.

### Phase 4: Merge Thread Integration (`concurrent_engine.rs`)
- Write meta.bin BEFORE shard files (ordering guarantee)
- Read-modify-write for dirty shards (preserve orphaned entries)
- Tombstone cleanup on shard rewrite → transition to Free
- Threshold-based shard dirty promotion (>50% tombstoned)
- Clear per-entry dirty flags after successful shard write

### Phase 5: Flush Thread Tombstoning (`concurrent_engine.rs`)
- During maintain_filter_changes / maintain_sort_changes / maintain_alive_changes:
  - Check if affected entry IDs are in RAM (HashMap lookup)
  - If not in RAM: tombstone in meta-index, set meta_dirty

### Phase 6: Tests
- Unit tests: serialize/deserialize round-trip for meta and shard files
- Unit tests: tombstone lifecycle (create → persist → load → clean up → free)
- Unit tests: lazy shard loading with tombstone filtering
- Unit tests: LRU eviction skips dirty entries
- Unit tests: read-modify-write preserves orphaned entries
- Unit tests: shard loader skips entries not in meta-index
- Integration test: restart with warm cache, verify cache hits
- Integration test: mutation → tombstone → restart → shard load → skip dead entry
- Integration test: crash recovery — meta.bin without shard, shard without meta.bin
- E2E test: extend existing `e2e-unified-cache.mjs` with restart scenarios

---

## 15. What This Design Does NOT Include

- **Sub-sharding** — deferred until write amplification is measured at scale
- **Eligibility filtering** — disk is superset of RAM, all entries have value
- **Checksums** — deserialization failure is sufficient corruption detection
- **Generation counters** — the unified cache uses live maintenance, not lazy invalidation
- **Compression** — roaring bitmaps are already compressed; keys are small
- **Background preloading** — pure lazy loading is simple and sufficient for v1
- **Separate autovac** — tombstone cleanup happens naturally on shard rewrite; forced cleanup at >50% threshold
- **Persisted sort index** — the sorted Vec / radix bucket index that accelerates cache-hit pagination is reconstructed lazily from bitmap + sort layers on first access after shard load (~700µs at 4K capacity). Persisting it would add 3.9x storage overhead per entry with marginal benefit (one-time 700µs vs ongoing write amplification). If 64K-capacity entries become common on disk and the 12ms reconstruction cost is problematic, a v2 shard format can add an optional `sort_vec` section (see §6.3).
- **Radix bucket persistence** — the 8-bit radix structure (256 sub-bitmaps per entry) is even more complex to serialize and only relevant for expanded 64K entries. Lazy reconstruction at 64K costs ~1.2ms with precomputed values. Not needed for v1.
