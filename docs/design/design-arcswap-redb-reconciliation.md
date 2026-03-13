# Design: ArcSwap + Diff-Based Mutations + Two-Tier redb Storage

## Context

Three pieces of architecture need to compose:

1. **ArcSwap lock-free reads** — Readers call `inner.load()` for a zero-cost snapshot Guard. No locks, no refcount ops on the read path.
2. **redb bitmap persistence** — All bitmaps stored in redb as source of truth. Memory is a cache over disk. Cold bitmaps evictable.
3. **High write throughput** — Mutations batched via crossbeam channel, applied by a single flush thread, published atomically.

The original `contention-bench` branch used `Arc::make_mut()` copy-on-write: when the flush thread mutates a bitmap shared with a published snapshot, `Arc::make_mut()` clones the entire bitmap. For dense bitmaps (~12MB each), this creates hundreds of MB of transient copies per flush cycle under write load.

## Solution: Diff-Based Mutations

Replace full-bitmap CoW with a lightweight diff layer.

### Data Structure

```rust
struct VersionedBitmap {
    base: Arc<RoaringBitmap>,  // immutable, shared with published snapshots
    diff: BitmapDiff,          // mutable, owned by staging only
    generation: u64,           // incremented on each merge
}

struct BitmapDiff {
    sets: RoaringBitmap,       // bits to add
    clears: RoaringBitmap,     // bits to remove
}
```

- `base` is shared between staging and published snapshots via `Arc`. Never cloned during normal operations.
- `diff` accumulates mutations. Only the flush thread touches it (single-threaded, no locks).
- On publish: `ArcSwap::store()` publishes the current `base` + `diff`. Readers see both.

### Write Path (Flush Thread)

```
1. coalescer.prepare()                     // drain channel, group ops (no lock)
2. Apply mutations to diff bitmaps:
   - Insert → diff.sets.insert(bit)
   - Remove → diff.clears.insert(bit)
   Single-threaded, no locks, operates on tiny diff bitmaps.
3. inner.store(snapshot)                   // atomic pointer swap
   Published snapshot contains same base Arc + updated diff.
4. Periodically: merge cycle (see below)
```

No full bitmap clones. Writes touch only the small diff bitmaps — KB to low MB even under heavy mutation.

### Read Path (Lock-Free, Zero-Copy)

Readers call `inner.load()` for an immutable snapshot. When evaluating a filter clause, if a bitmap has a non-empty diff, **fuse the diff into the intersection** rather than materializing a merged copy:

Instead of:
```
merged = (base | sets) &! clears    // full-size temporary copy
result = merged & other_filter      // another intersection
```

Do:
```
result = (base & other_filter) | (sets & other_filter) &! clears
```

Or even better — compute the full filter intersection against base bitmaps normally, producing a small result set. Then patch the small result:
```
result = base_intersection          // small: already narrowed by filters
result |= (sets & alive & filters)  // add bits from diff that pass all filters
result &!= clears                   // remove cleared bits
```

The diff is applied to the **small output**, never to the large base. No temporary full-size copies. The cost scales with diff size and result size, not base bitmap size.

### Merge Cycle (Compaction)

Periodically, the flush thread compacts diffs into their bases:

```
1. Check Arc::strong_count(&bitmap.base)
2. If count == 1 (no readers holding this base):
   → Mutate base in place: base |= sets; base &!= clears
   → Reset diff to empty
   → Zero duplication
3. If count > 1 (readers still using this base):
   → Wait briefly (queries complete in microseconds) or
   → Accept momentary duplication of one bitmap
4. Publish merged snapshot with empty diff
5. Write merged bitmaps to redb in a single batch transaction
```

Merges are infrequent (every few seconds) and operate on one bitmap at a time. The brief window where `strong_count > 1` resolves as reader Guards drop — typically microseconds.

### Memory Overhead Comparison

| Approach | Peak Overhead Per Flush |
|---|---|
| `Arc::make_mut()` CoW | Full clone of every mutated bitmap. Dense 12MB bitmap → 12MB copy. N mutated bitmaps → N × 12MB. |
| Diff-based | Diff bitmaps only. 1000 mutations → ~few KB of sets/clears. Brief single-bitmap duplication during merge. |

---

## Two-Tier Storage

### Tier 1 — ArcSwap Snapshot (Always in Memory)

Contents:
- Alive bitmap
- All sort layer bitmaps (all fields, all layers)
- Low-cardinality filter fields: `nsfwLevel`, `type`, `onSite`, `hasMeta`
- Time range bucket bitmaps
- Trie cache, bound cache, and meta-index bitmaps

All stored as `VersionedBitmap` with diff layers. Lock-free reads via ArcSwap. ~1.5 GB.

### Tier 2 — moka Cache over redb (Loaded on Demand)

Contents:
- High-cardinality filter fields: `tagIds` (~500k values, ~5.5GB total), `userId` (~millions)

Use `moka` concurrent cache instead of raw DashMap. moka provides: built-in LRU/LFU eviction, size-based memory budgeting, concurrent access without external locking, TTL support.

Configurable per field:

```yaml
filter_fields:
  - name: nsfwLevel
    field_type: single_value
    storage: snapshot         # Tier 1

  - name: tagIds
    field_type: multi_value
    storage: cached           # Tier 2

  - name: userId
    field_type: single_value
    storage: cached           # Tier 2

  - name: onSite
    field_type: boolean
    storage: snapshot         # Tier 1
```

Rule of thumb: fields with < ~10k distinct values → `snapshot`. Fields with > ~10k → `cached`.

### Tier 2 Bitmaps with Diffs

Cold bitmaps loaded from redb into moka start with the redb version as `base` and an empty diff. Mutations accumulate in the diff while the bitmap lives in the cache. At merge time, the diff is applied to the base and the merged result is written back to redb.

**Eviction before merge:** If moka evicts a bitmap that has a non-empty diff, only the small diff needs to be flushed to redb — not the full bitmap. Load base from redb, apply diff, write back. The diff is tiny (KB), so this is fast even under eviction pressure.

**Cold bitmap mutations:** If a mutation targets a Tier 2 bitmap not loaded in moka, buffer it as a pending mutation (just the set/clear bit IDs). On next query that loads that bitmap from redb, apply pending mutations to the diff. On next redb merge cycle, drain pending mutations by loading from redb, applying, writing back.

### Architecture Diagram

```
                    ┌──────────────────────────────┐
                    │      ArcSwap<Snapshot>        │
                    │  ┌────────────────────────┐   │
 inner.load()       │  │ alive: VersionedBitmap │   │
 (lock-free) ──────►│  │ sorts: VersionedBitmap │   │  Tier 1
                    │  │ low-card filters: VB   │   │  (~1.5 GB)
                    │  │ buckets: VB            │   │
                    │  └────────────────────────┘   │
                    └──────────────────────────────┘

                    ┌──────────────────────────────┐
                    │      moka Cache               │
 cache.get(k)       │  ┌────────────────────────┐   │
 (concurrent) ─────►│  │ tag=anime → VB         │   │  Tier 2
                    │  │ tag=photo → VB         │   │  (~0.5-1 GB hot)
       miss ──┐     │  │ user=1234 → VB        │   │
              │     │  └────────────────────────┘   │
              ▼     └──────────────────────────────┘
                    ┌──────────────────────────────┐
 redb.get(key)      │      redb (NVMe)              │
 (~1-5ms) ─────────►│  ALL bitmaps (base only)      │  Source of truth
                    │  (~8-10 GB on disk)            │  (~8-10 GB)
                    └──────────────────────────────┘
```

### Read Path

```
Query: { tag=anime, nsfwLevel=1, sort: reactionCount desc }

1. snap = inner.load()                                    // lock-free Guard
2. nsfwLevel bitmap → snap.filters.get("nsfwLevel", 1)    // Tier 1, in snapshot
   → VersionedBitmap: apply diff fusion during intersection
3. tag bitmap → moka.get("tagIds", anime_id)              // Tier 2 lookup
   3a. HIT  → VersionedBitmap with base + diff
   3b. MISS → load base from redb (~1-5ms)
              check pending mutations → apply to diff
              insert into moka
4. Intersect bitmaps with diff fusion (no full merges)
5. AND with alive (from snapshot, diff-fused)
6. Sort via sort layers (from snapshot)
```

### Write Path (Flush Thread)

```
1. coalescer.prepare()                           // drain channel, group (no lock)
2. For Tier 1 ops: apply to staging diffs         // private, no lock
3. For Tier 2 ops:
   3a. Loaded in moka → apply to moka entry's diff
   3b. Not loaded     → buffer as pending mutation
4. inner.store(snapshot)                          // publish Tier 1 (atomic swap)
```

### Merge Cycle (Separate from Flush)

```
Every merge_interval_ms (e.g., 5 seconds) or when any diff exceeds threshold:

1. For each dirty Tier 1 VersionedBitmap:
   a. Check Arc::strong_count(&base)
   b. If 1 → mutate in place, zero copy
   c. If >1 → brief wait or accept momentary single-bitmap duplication
   d. Reset diff to empty

2. For each dirty Tier 2 VersionedBitmap in moka:
   a. Same merge logic
   b. Write merged base to redb

3. For pending mutations (cold Tier 2 bitmaps):
   a. Load base from redb
   b. Apply pending mutations
   c. Write back to redb
   d. Clear pending buffer

4. Publish merged Tier 1 snapshot with empty diffs
5. Batch write all merged Tier 2 bitmaps to redb in one transaction
```

### Configuration

```yaml
storage:
  engine: redb
  bitmap_path: "./data/bitmaps.redb"
  flush_interval_ms: 100         # publish diffs this often
  merge_interval_ms: 5000        # compact diffs into bases this often
  merge_diff_threshold: 10000    # merge if any diff exceeds this many bits
  tier2_cache_size: 1GB          # moka memory budget for Tier 2
```

### What This Eliminates

- No `Arc::make_mut()` cloning of full bitmaps on every flush
- No temporary full-size bitmap copies on the read path
- No locks on the read path (ArcSwap Guard + moka concurrent access)
- No locks on the write path (single flush thread)
- No snapshot sidecar process (redb is the snapshot)
- No custom WAL (redb provides durability, Postgres WAL is ultimate source of truth for replay)

### Memory Budget

| Component | Estimated Size |
|---|---|
| Tier 1: alive + sort layers + low-card filters | ~1.5 GB |
| Tier 2: hot tag/user bitmaps in moka | ~0.5-1 GB |
| Diffs (between merges) | ~1-10 MB |
| Trie cache + bound + meta-index bitmaps | ~0.2-0.5 GB |
| redb read cache + allocator | ~0.5 GB |
| **Total RSS** | **~3-4 GB** |
| redb on disk (all bitmaps) | ~8-10 GB |

Down from 12 GB RSS at 104M records. Scales to 500M+ without memory growing proportionally — Tier 2 cache size is fixed regardless of dataset size.

### Startup

1. Open redb.
2. Load Tier 1 bitmaps from redb into `InnerEngine` (alive, sort layers, low-card filters). ~1.5GB, a few seconds.
3. Publish initial snapshot via `ArcSwap::new(...)`.
4. Start serving. Tier 2 bitmaps load on demand from redb as queries arrive.
5. Within seconds the hot tags/users are in moka and queries are at full speed.

### Cache Layer (Above Both Tiers)

The trie cache, bound cache, and meta-index operate above the tier split. Cache hits skip tier lookups entirely — a cached filter result bitmap is a pre-computed intersection that doesn't reference individual source bitmaps anymore. Cache maintenance via the meta-index uses tiny bitmaps always in Tier 1.

Bound caches that get bits set from live writes accumulate diffs too. Their diff fuses into the query-time intersection just like any other VersionedBitmap.
