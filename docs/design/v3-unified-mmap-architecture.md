---
status: FINAL
created: 2026-03-30
architecture: Justin (vision, all key design decisions)
author: Edward (team lead)
reviewers: Tom (CTO), Dakota (doc keeper), Mark + Ollie (engineers)
---

# BitDex V3 — Unified DataSilo Architecture

> One generic mmap'd storage engine for everything: docs, bitmaps, cache.
> Variable-size entries with buffered in-place updates. Sharded compaction.
> Near-zero heap. Instant startup. 13.6x faster writes. 55% faster sorts.

---

## 1. The DataSilo

A `DataSilo<K>` is a generic mmap'd key-value store. Three components, all mmap'd:

```
┌─────────────────────────────────────────────┐
│  Index Table (mmap'd)                       │
│  key → (shard_id, offset, length, allocated) │
├─────────────────────────────────────────────┤
│  Data Shards (mmap'd, N files)              │
│  [entry bytes][buffer]  [entry bytes][buffer]│
├─────────────────────────────────────────────┤
│  Ops Log (per-silo, append-only)            │
│  [op][crc32]  [op][crc32]  ...              │
└─────────────────────────────────────────────┘
```

**Index table:** `key → (offset, length, allocated)` per entry. mmap'd for nanosecond lookups.

**Data shards:** N files of packed variable-size entries. Each entry has buffer space
(configurable ratio, e.g., 20% extra) for in-place updates without relocation.

**Ops log:** Per-silo append-only file with CRC32 per entry. Crash safety — replay on restart.

**In-memory:** `HashMap<K, pending_bytes>` for read-time apply of pending ops.

### Config

```rust
SiloConfig {
    num_shards: u32,        // files to spread entries across
    buffer_ratio: f32,      // extra space per entry (1.2 = 20%)
    compact_threshold: f32, // dead space % that triggers compaction (0.20)
}
```

### Write path

**In-place update (common case — new data fits in allocated space):**
1. Overwrite entry bytes at existing offset in data shard
2. Update index table: length = new_length (allocated unchanged)
3. Append op to ops log (crash safety)
4. Cost: one mmap write. No relocation, no dead space.

**Relocating update (new data exceeds allocated space):**
1. Append new entry + buffer to end of data shard
2. Update index table → new offset, new length, new allocated
3. Old bytes become dead space (add to shard's `dead_bytes` counter)
4. Append op to ops log

**Bulk load:**
1. Each thread writes sequentially to its shard region (mmap memcpy)
2. Build index table in one pass after all threads finish
3. 5.53M entries/sec at 32 threads (proven)

### Read path

1. Check in-memory ops HashMap → hit? Return pending version.
2. Index table lookup (mmap deref, ~nanoseconds) → offset + length
3. Data shard read at offset (mmap deref, ~nanoseconds if page hot)
4. Two pointer derefs total. ~1μs worst case, ~84ns hot.

### Compaction

Per-shard, triggered when `dead_bytes / total_bytes > compact_threshold`:
1. Write new shard file: pack all live entries contiguously (with fresh buffers)
2. Update index table entries for affected keys
3. Atomic swap: mmap new shard, delete old
4. Reset `dead_bytes = 0`

Duration: ~1.9 seconds per 844 MB shard. One shard at a time. Readers continue on old
shard until swap completes.

---

## 2. Three Instantiations

Same engine, different key types and values:

| Silo | Key | Value | Entries | Shards | Buffer |
|------|-----|-------|---------|--------|--------|
| Docs | `u32` slot_id | msgpack doc bytes (~230B) | 107M | 32 | 20% |
| Bitmaps | `(field, value)` | frozen bitmap bytes (1KB-12MB) | ~32K | TBD | TBD |
| Cache | `query_hash` | cache entry bytes (~3.3KB) | 100K-1.2M | TBD | 20% |

**Docs:** Slot ID maps to doc content. Reads serve `include_docs` queries. Bulk loaded
from CSV dump pipeline. Updated at 72 ops/sec steady state.

**Bitmaps:** Field+value key maps to frozen CRoaring bitmap. Read via
`FrozenRoaringBitmap::view()` for zero-copy access. Direct AND/OR/Sub from our roaring-rs
fork. In-memory diff bitmap applied at read time for pending mutations.

**Cache:** Query hash maps to unified cache entry (bounded top-K bitmap + sort metadata).
Live-maintained by mutation thread. Replaces both unified_cache.rs and bound_store.rs.
Expanded budget: 4GB → ~1.2M entries → ~95% hit rate (up from 71.6% at 333MB).

---

## 3. Read Consistency

**Problem:** A query spans multiple bitmap lookups. If the mutation thread applies ops between lookups, the query sees a mix of pre- and post-mutation state.

**Solution:** Snapshot the ops HashMap at query start via `Arc` clone.

```
Query start:
  let ops_snapshot = mutation_thread.ops.clone();  // Arc refcount bump, ~nanoseconds
  // All reads within this query use ops_snapshot
  // Mutation thread keeps writing to its live copy — reader's snapshot is immutable
```

The ops HashMap is small (~1K entries at 72 ops/sec with 1K-ops compaction threshold, ~256KB). The Arc clone is a reference-count bump, not a deep copy.

**V2 comparison:** Same conceptual model as V2's ArcSwap snapshots, but lighter — snapshotting a small HashMap instead of cloning an entire InnerEngine with Arc-per-bitmap pointers.

**Cost:** ~nanoseconds per query start. No contention — readers hold immutable references, writer holds the live mutable copy.

---

## 4. Mutation Thread + Janitor

**Single background thread** (replaces V2's flush thread, merge thread, and all compaction):

**Job 1 — Apply ops (continuous):**
- Drain mutation channel (same as V2's crossbeam channel pattern)
- For each op: write to silo (in-place or relocating) + append to ops log + update in-memory buffer
- Readers see updates immediately via in-memory HashMap

**Job 2 — Compact shards (periodic):**
- Round-robin across all silos, check `dead_bytes / total_bytes`
- If over threshold: rewrite shard, swap, reset counter
- At 72 ops/sec: ~1 doc shard every 3.8 days. ~1.9 seconds each.

**V2 equivalence:**
| V2 | V3 |
|----|----|
| Flush thread + ArcSwap publish | Mutation thread + in-memory HashMap |
| Merge thread | Not needed — no snapshot cloning |
| DocStore janitor | Shard compaction (same janitor) |
| ShardStore compaction | Shard compaction (same janitor) |
| Write coalescer | Not needed — direct shard writes |
| Loading mode | Not needed — bulk load is just fast mmap writes |

---

## 5. Separate Crate

`datasilo` as its own Rust crate. Zero BitDex-specific knowledge.

**Public API:**
```rust
pub struct DataSilo<K: SiloKey> { ... }

impl<K: SiloKey> DataSilo<K> {
    fn open(path: &Path, config: SiloConfig) -> Result<Self>;
    fn get(&self, key: &K) -> Option<&[u8]>;
    fn put(&self, key: K, data: &[u8]) -> Result<()>;
    fn delete(&self, key: &K) -> Result<()>;
    fn bulk_load<I: Iterator<Item=(K, Vec<u8>)>>(&mut self, entries: I) -> Result<()>;
    fn compact(&self) -> Result<CompactionStats>;
}

pub trait SiloKey: Hash + Eq + Clone + Send + Sync { ... }
```

BitDex imports `datasilo` and provides:
- `SlotId` as SiloKey for docs
- `BitmapKey` as SiloKey for bitmaps (wraps field+value)
- `CacheKey` as SiloKey for cache entries (wraps query hash)
- Frozen bitmap serialization/deserialization on top of raw silo bytes

---

## 6. Implementation

### Module structure (~1,500 lines total)

```
crates/datasilo/
  src/
    lib.rs          DataSilo<K> + SiloKey trait (~500-800 lines)
    compaction.rs   Shard compaction logic (~200 lines)
    ops_log.rs      Append-only ops log with CRC32 (~150 lines)
    index.rs        mmap'd index table (~150 lines)

src/v3/
  mod.rs            V3Engine: wires DataSilo to BitDex query path (~200 lines)
  bitmap.rs         Frozen bitmap read/write on top of DataSilo (~100 lines)
  cache.rs          Cache entry format on top of DataSilo (~100 lines)
  loader.rs         Dump pipeline → DataSilo bulk load (~200 lines)
```

### What gets ported from V2

| Module | Source | Changes |
|--------|--------|---------|
| Query executor | executor.rs | `Arc<RoaringBitmap>` → `FrozenRoaringBitmap` refs |
| Sort traversal | sort.rs:174 | Frozen sort layers |
| Filter evaluation | executor.rs | Direct frozen AND/OR from fork |
| Query parsing | query.rs, compact_query.rs, meili_query.rs | None |
| HTTP server | server.rs | Wire to V3Engine |
| Config + metrics | config.rs, server.rs | Add V3 knobs |

### What gets deleted (11,700+ lines)

docstore.rs (3,204), doc_cache.rs (786), bitmap_fs.rs (1,137), shard_store.rs (1,485),
shard_store_bitmap.rs (1,723), shard_store_meta.rs (292), ops_wal.rs (430),
bucket_diff_log.rs (487), bitmap_memory_cache.rs (294), memory_pressure.rs (160),
data_silo.rs (~600), bound_store.rs (1,083).

### Development strategy

Build in `src/v3/` while V2 stays stable at src root. V2 continues running production.
Feature-flag V3 engine (`--features v3`). Validate V3 at 107M scale.
Once proven: flatten `src/v3/` to `src/`, delete V2 code. Clean root, no legacy.

### Phases

1. **DataSilo crate** — build `crates/datasilo/`, generic engine, test standalone
2. **Doc integration** — `src/v3/` wiring, include_docs reads via DataSilo
3. **Bitmap integration** — frozen bitmaps in DataSilo, wire into executor
4. **Cache integration** — cache entries in DataSilo
5. **V3 engine** — port executor + sort + server, full query path in v3/
6. **107M validation** — bulk load, loadtest, compare against V2 production numbers
7. **Flatten + ship** — move src/v3/ to src/, delete V2 code, update all docs

---

## 7. Roaring-RS Fork

**Repo:** `C:/Dev/Repos/open-source/roaring-rs` (branch: `frozen-mmap-support`)

Adds: `FrozenRoaringBitmap::view()` (zero-copy), direct `BitAnd`/`BitOr`/`Sub` on frozen refs,
`serialize_frozen_into()`. 842 lines, 25 tests. CRoaring-compatible format.

---

## 8. Experiment Evidence

| Exp | Result |
|-----|--------|
| Exp 2: mmap writes | 6.49M/s fixed-offset, 5.53M/s slot table (13.6x V2) |
| Exp 3: Frozen bitmap | view()=10μs (223x), direct AND at 8 clauses matches heap |
| Exp 4: Real data replay | Frozen 1.25x slower at 3 ANDs (caveat: synthetic queries) |
| Exp 5: Frozen sort walk | DESC 22% faster, ASC/500 55% faster than heap |
| Exp 6: Slot table | 5.53M/s write, 1μs read, 1.2GB table at 107M |
| Cache analysis | 100K entries/333MB/71.6% hit → 4GB/1.2M entries/~95% hit |
| Ops volume | 72/sec steady, 275K/sec burst. 1.43 GB/day dead space |
| Compaction | 1.9s per 844MB shard. ~1 shard every 3.8 days at 72 ops/sec |

---

## 9. Open Questions

1. **Buffer ratios** — Mark testing bitmap size variance under mutations. Docs ~20%, bitmaps TBD.
2. **Shard counts** — Ollie researching per-silo-type recommendations.
3. **Real query traces** — Aidan enabling on v1.0.101. Validates frozen AND cost at real complexity.
4. **Fork publishing** — vendor or publish roaring-rs fork for CI/production.
5. **Migration path** — V2 → V3 via bulk dump+reload, or online migration?

---

## 10. Success Criteria

| Metric | Target |
|--------|--------|
| Query latency p50/p95/p99 | ≤ current (validated with real traces) |
| Cache hit rate | ≥ 90% |
| Bulk load time | ≤ current |
| Steady-state ops | ≥ 72/sec |
| RSS | < 25 GB (down from 29 GB) |
| Startup time | < 1 second (down from 22+ seconds) |
| Disk usage | Comparable (27-30 GB) |
| Codebase size | ~1,500 lines core (down from 11,700) |
