# Unified Cache — Implementation Q&A

Answers to the implementer's open questions, based on the current Bitdex V2 codebase.

---

## 1. Merge thread / flush thread handoff during bitmap write

**Short answer: Zero contention. No locks between them.**

The merge thread and flush thread operate on completely independent copies of the bitmap data. There is no lock handoff.

**How it works:**

- The **flush thread** owns a private `staging: InnerEngine` that it mutates directly. No other thread touches staging.
- The **merge thread** loads a snapshot via `ArcSwap::load_full()` — a lock-free atomic pointer read. It then calls `.clone()` on the snapshot, which copies Arc pointers (not bitmap data). It works entirely on this private clone.
- The only coordination is an `Arc<AtomicBool>` dirty flag:
  - Flush thread: `dirty_flag.store(true)` after applying mutations
  - Merge thread: `dirty_flag.swap(false)` before deciding whether to write

**Merge thread loop (`concurrent_engine.rs:668-748`):**
```
sleep(merge_interval_ms)                     // default 5s
→ swap dirty flag (atomic, ~ns)
→ if dirty:
    → load_full() (lock-free ArcSwap read)
    → clone snapshot (copies Arc pointers, not bitmaps)
    → merge_dirty() on private copy (filter diff compaction)
    → write_full_snapshot() to BitmapFs (file I/O on private data)
```

**Can new writes arrive during the merge?** Yes, and they do. The flush thread continues applying mutations to its private staging and publishing new snapshots via ArcSwap while the merge thread writes the old snapshot to disk. The merge thread's snapshot is a point-in-time capture — new mutations will be picked up on the next merge cycle.

**Write stalls under high throughput:** Not from this interaction. The flush thread's bottleneck is `staging.clone()` for ArcSwap publish (O(num_fields) Arc pointer copies). Loading mode exists specifically to skip this during bulk inserts. The merge thread's disk I/O is entirely off the critical path.

---

## 2. Sort packed file update strategy

**Short answer: Sort files are written as full snapshots by the merge thread. No incremental file updates.**

Sort fields use the same VersionedBitmap (base + diff) as filter fields, but with **eager merge** — diffs are always merged into bases before snapshot publish. The on-disk format is a single packed file per sort field.

**In-memory update path:**

When `reactionCount` changes from 5→6 for a slot:
1. XOR diff: `101 ^ 110 = 011` — only 2 bits changed
2. Two MutationOps: `SortClear(bit 0)`, `SortSet(bit 1)`
3. Coalescer groups by (field, bit_layer), applies bulk via `set_layer_bulk()` / `clear_layer_bulk()`
4. All 32 layers eagerly merged before snapshot publish

**On-disk format (`bitmaps/sort/{field}.sort`):**
```
[u8 num_layers=32]
[index: 32 × (u8 bit_pos, u32 offset, u32 length)]  // 289 bytes header
[32 serialized roaring bitmaps concatenated]
```

All 32 layers are written atomically as a single file (tmp→fsync→rename). The merge thread writes the entire sort file every cycle when dirty — there is no incremental on-disk update.

**File sizes at 105M records:**

| Field | Size |
|-------|------|
| sortAt | ~381 MB |
| id | ~279 MB |
| reactionCount | ~63 MB |
| collectedCount | ~32 MB |
| commentCount | ~1.2 MB |

**Is sortAt handled differently from reactionCount?**

No — both use the same code path. The only difference is behavioral:
- `sortAt` only receives inserts (timestamps don't change), so diffs only have `sets`, never `clears`
- `reactionCount` receives updates (value changes), so diffs have both `sets` and `clears`
- The XOR-based diff naturally handles both cases: inserts produce pure sets, updates produce targeted bit flips

The merge thread writes both fields the same way — full 32-layer snapshot per file.

---

## 3. Cache key hash collision handling

**Short answer: There are no hash-based cache keys. This question is based on a design that doesn't exist yet.**

The current system uses **full structural keys**, not hashes:

**Trie cache keys:** `CacheKey = Vec<CanonicalClause>` — the full list of canonicalized filter clauses stored as a trie path. Each `CanonicalClause` contains `(field: String, op: String, value_repr: String)`. The trie navigates one clause per level via `HashMap<CanonicalClause, TrieNode>`. No hashing into a digest occurs.

**Bound cache keys:** `BoundKey { filter_key: CacheKey, sort_field: String, direction: SortDirection, tier: u32 }` — stored directly as a HashMap key (derives `Hash` for the HashMap, but the full key is always compared via `Eq`).

**No `.meta` files exist for cache entries.** The cache is entirely in-memory, not persisted to disk. On restart, the cache starts empty and warms up from queries.

**No hash collisions are possible** in the current design because keys are compared structurally. If the unified cache design introduces hash-based filenames for persisted cache entries, collision handling would need to be designed from scratch — it's not an existing concern.

---

## 4. GC and meta-index cleanup atomicity

**Short answer: There is no GC. The meta-index is in-memory only and cleaned up synchronously.**

The meta-index (`src/meta_index.rs`) is a purely in-memory structure that maps `(field, op, value)` → set of cache entry IDs. It tracks which trie cache entries reference which filter clauses, enabling O(1) lookup during flush thread live updates.

**Cleanup happens synchronously during eviction:**

When the trie cache evicts an entry (either via LRU at capacity or decay below threshold):
1. `deregister(entry_id)` called on the meta-index
2. Removes the entry ID from all clause→entry mappings
3. Cleans up empty mappings
4. Recycles the entry ID (LIFO free list)

This is a single-threaded operation inside the cache mutex — no atomicity concern.

**There are no bitmap cache files on disk to GC.** The BitmapFs stores *source data* (filter/sort bitmaps), not cache entries. The merge thread writes full snapshots — it doesn't manage individual cache files. If the unified cache adds disk-persisted cache entries, GC and crash-recovery (orphan cleanup) would be new concerns to design.

**For a future disk-persisted cache**, the proposed order (meta-index removal before file deletion + self-healing boot scan) is sound. The boot scan would enumerate cache files on disk, compare against the meta-index, and delete orphans.

---

## 5. Shard rewrite batching on PATCH

**Short answer: PATCH does not touch the docstore at all. Upsert (PUT) rewrites shards, with batching for bulk paths.**

**PATCH path (`concurrent_engine.rs:850-875`):**
- Takes a `PatchPayload` with field-level changes
- Computes bitmap diffs via `diff_patch()` — updates filter/sort bitmaps
- Sends ops to the coalescer channel
- **Does NOT read or write the docstore** — PATCH changes are lost on restart

This is by design: PATCH is for high-frequency lightweight updates (like reaction count bumps) where the source of truth is upstream (Postgres). On restart, a full re-sync brings the docstore back to the correct state.

**Upsert (PUT) path:**
- Reads old doc from docstore (one shard read, binary search in index)
- Diffs old vs new, generates bitmap ops
- Enqueues new doc on `doc_tx` channel
- Flush thread batches doc writes: `put_batch()` groups docs by shard ID, rewrites each touched shard once

**Batching behavior:**
- Single PUT: read-modify-write one shard per upsert (no coalescing across upserts)
- `put_batch()`: groups by shard, merges all updates for the same shard into one rewrite
- `put_bulk` / `BulkWriter`: per-shard DashMap locks, parallel writes via rayon, 100K docs/batch
- Flush thread background batches: doc writes accumulated and flushed in 100K batches

**For high-frequency reaction count updates hitting the same shard:** Currently each individual PUT rewrites the shard. If this becomes a bottleneck, the doc_tx channel's batching (which groups by flush cycle, typically 100μs) provides natural coalescing — multiple updates to the same shard within one flush cycle are combined into a single `put_batch()` call. The 512-doc shard size means hot shards are small (~75KB compressed), so rewrites are fast.

---

## 6. Memory hints for mmap (madvise)

**Short answer: No mmap is used. All I/O is standard `std::fs::read()` / `std::fs::write()`. No madvise hints are issued.**

The current architecture deliberately avoids mmap:

**How bitmaps are loaded:**
```rust
// bitmap_fs.rs - all reads use standard I/O
let data = std::fs::read(&path)?;           // Read entire file into Vec<u8>
let bitmap = RoaringBitmap::deserialize_from(&data[..])?;  // Deserialize into owned struct
```

**How bitmaps are written:**
```rust
// bitmap_fs.rs - atomic write pattern
std::fs::write(&tmp_path, &buf)?;           // Write to .tmp file
std::fs::rename(&tmp_path, &final_path)?;   // Atomic rename
```

**Why not mmap?**
1. Roaring's serialization format requires deserialization — it's not zero-copy friendly. You can't just point into an mmap'd region and use the bitmap.
2. Standard I/O with the OS page cache provides the same benefit as mmap for read-heavy workloads: hot files stay in RAM, cold files are evicted.
3. On Windows (the development platform), mmap semantics differ from Linux (no `madvise`, different page fault behavior).
4. The lazy loading architecture already manages what's in memory: only queried fields are loaded, and the OS page cache handles the rest.

**If mmap were adopted**, the relevant hints would be:
- `MADV_RANDOM` for query-time bitmap access (correct — bitmap bit-reads are random)
- `MADV_SEQUENTIAL` during bulk load (correct — full file scans during restore)
- `MADV_WILLNEED` for pre-warming (correct — could warm critical fields at startup)
- `MADV_DONTNEED` for eviction hints after lazy unload

However, this would also require a zero-copy bitmap format (e.g., frozen roaring bitmaps) to avoid the deserialization step. The `roaring-rs` crate does support `RoaringBitmap::deserialize_unchecked_from()` which is faster but still copies.

**The OS page cache currently handles memory management transparently.** At 105M records with lazy loading:
- Startup: instant (<1s, only alive + slot counter loaded)
- First query: 87ms for nsfwLevel (7 values, tiny), 6.6s for tagIds (31K values, 79% of memory)
- Subsequent queries: cached by OS, sub-ms
- Memory pressure: OS evicts cold bitmap pages automatically

For the unified cache design with disk-persisted cache entries, madvise hints would become more relevant since cache files would have known access patterns (random lookups by cache key hash).
