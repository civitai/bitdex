# DataSilo Implementation Plan

## Benchmark Findings

### Write Throughput (10M entries × 230B, 32 threads)

| Approach | Rate | At 109M |
|---|---|---|
| Current StreamingDocWriter (200K shard files) | 82K/s | 22 min |
| BufWriter (single file, sequential) | 6.2M/s | 17.5s |
| DataSilo parallel mmap (1MB regions, cold) | 35.3M/s | 3.1s |
| DataSilo parallel mmap (hot pages) | 56.1M/s | 1.9s |

### Read Throughput

| Approach | Rate |
|---|---|
| Current DocStoreV3 (cold, shard file open) | ~60/s (16ms each) |
| Current DocCache (hot) | ~1M/s (<1μs) |
| DataSilo mmap (random keys, hot) | 23-27M/s |

### Encoding Formats (1M iterations, 20-field doc)

| Format | Encode | Decode | Size | Verdict |
|---|---|---|---|---|
| msgpack (rmp_serde) | 334ns (3.0M/s) | 177ns (5.6M/s) | ~230B | Too slow |
| Raw binary (hand-rolled) | 72ns (13.9M/s) | 17ns (58.8M/s) | 211B | Fast |
| **DocOpCodec (current BitDex)** | **71ns (14.1M/s)** | **16ns (62.5M/s)** | 221B | **Winner — keep** |

**Decision:** Keep DocOpCodec format. Encoding at 71ns with 32 threads = ~2.2ns amortized — well within the 28.6ns budget at 35M writes/sec.

### Pre-faulting

| Strategy | Prefault | Write | Total | Rate |
|---|---|---|---|---|
| Cold (no prefault) | — | 0.283s | 0.283s | **35.3M/s** |
| Sequential memset | 1.376s | 0.177s | 1.552s | 6.4M/s |
| Parallel memset | 0.322s | 0.181s | 0.503s | 19.9M/s |
| Parallel page-touch | 0.355s | 0.173s | 0.527s | 19.0M/s |

**Decision:** No pre-faulting. Cold writes at 35M/s are already faster than any prefault+write combination. Pre-faulting doubles I/O (touch every page twice). The OS handles page faults efficiently for sequential-within-region access patterns.

**Caveat:** On the 32GB K8s pod under memory pressure, cold page faults may be slower. If needed, parallel page-touch (0.36s for 2.3GB) is the best cross-platform option. Gemini also flagged `MADV_POPULATE_WRITE` (Linux 5.14+) and `SetFileValidData` (Windows, admin-only) as OS-specific accelerators.

### Pipeline Bottleneck Analysis (images phase, 14.6M rows from 1GB CSV)

| Step | Time | Notes |
|---|---|---|
| Enrichment load | 7s | posts.csv HashMap |
| Parallel parse + bitmap build + doc write | 26s | 32 rayon threads |
| Bitmap merge | 6.5s | rayon fold+reduce |
| **Enrichment drop** | **50.5s** | Freeing 56M String allocations |
| StreamingDocWriter finalize | 1s | (after fsync removal) |
| Bitmap save to disk | 4s | ShardStore writes |
| **Total** | **~95s** | Enrichment drop was the hidden bottleneck |

**Fix applied:** Background-thread enrichment drop. Reduced wall-clock from 145s → 51s.

---

## Architecture

### Generic DataSilo Crate

One engine, trait-parameterized. No code duplication across doc/bitmap/cache silos.

```rust
// crates/datasilo/src/lib.rs
pub struct DataSilo<K: SiloKey> {
    index: MmapMut,       // key → (offset, length, allocated)
    data: MmapMut,        // packed variable-size entries
    ops_log: OpsLog,      // append-only mutations with CRC32
    pending: HashMap<K, Vec<u8>>,  // in-memory ops for read-time apply
}

pub trait SiloKey: Copy + Eq + Hash + Send + Sync {
    fn to_index(&self) -> usize;
}

// Three instantiations:
type DocSilo = DataSilo<u32>;           // slot_id → DocOpCodec bytes
type BitmapSilo = DataSilo<BitmapKey>;  // (field,value) → frozen bitmap bytes
type CacheSilo = DataSilo<CacheKey>;    // query_hash → cache entry bytes

// Parallel writer for bulk loads (dump pipeline)
pub struct ParallelWriter { ... }
pub struct ThreadWriter<'a> { ... }  // per-thread, 1MB regions, lock-free
```

### Three Files per Silo (replaces 205K shard files)

| Silo | Index | Data | Ops |
|---|---|---|---|
| DocSilo | 2GB (126M × 16B) | 25GB (109M × 230B) | small |
| BitmapSilo | <1MB (32K × 16B) | 5-6GB (frozen bitmaps) | small |
| CacheSilo | <1MB | variable | small |

**Total: ~9 files** (down from 205K)

### Dump Pipeline Architecture (all merge ops, compaction after)

```
For each CSV phase (images, tags, resources, tools, techniques, metrics):
  32 rayon threads in parallel:
    parse CSV row → slot_id + field values
    encode doc fields → DocOpCodec bytes
    doc_silo.thread_writer.write(slot_id, &doc_bytes)     ← mmap memcpy
    for each bitmap field:
      bitmap_silo.thread_writer.write(bitmap_key, &op)    ← mmap append merge op
    
After ALL phases complete:
  bitmap_silo.compact()  → replay merge ops, build final bitmaps
  doc_silo is already final (each slot written once per phase, Merge semantics)
```

**Key insight:** During dump, bitmap data is written as merge ops (append-only, no memory accumulation). Compaction after dump replays ops to build final bitmaps. This means:

- **Zero bitmap memory during parse** — no per-thread HashMaps of RoaringBitmaps
- **Maximum write throughput** — each thread writes at mmap speed (35M/s)
- **Compaction is fast** — ops are binary (no CSV re-parse), smaller than CSV, parallelizable by bitmap key

**Trade-off:** Bitmap ops log for tags would be ~36GB (4.5B × 8B). Compaction reads 36GB and builds 28K bitmaps. This is disk I/O traded for memory. On machines with limited RAM (32GB pod) this is a win. On 128GB machines the current in-memory approach is faster.

**Hybrid option:** Use merge ops for large multi-value phases (tags: 4.5B rows) and in-memory accumulation for small phases (images: 109M rows with few distinct values per filter field).

---

## Implementation Phases

### Phase 1: DataSilo Crate (crates/datasilo/)

Core generic engine. ~500-800 lines.

- [x] `DataSilo<u32>` with open/get/bulk_load
- [x] OpsLog with CRC32 append + replay
- [x] IndexEntry (16 bytes: offset + length + allocated)
- [x] ParallelWriter with atomic bump + 1MB thread-local regions
- [x] ThreadWriter for sequential-within-region writes
- [x] 5 unit tests passing
- [x] Benchmarks: 35M/s write, 23-27M/s read, 56M/s hot
- [ ] Make generic over `K: SiloKey` (currently hardcoded u32)
- [ ] Thread-safe append_op (interior mutability for concurrent ops)
- [ ] Compaction (rewrite data file, reclaim dead space, clear ops log)
- [ ] Delete support (mark index entry as tombstone)
- [ ] Multi-shard support (optional, for very large data files)

### Phase 2: DocSilo Integration

Replace DocStoreV3 → DataSilo for doc storage. Immediate dump perf fix.

- [ ] Wire `DataSilo` as ConcurrentEngine's doc store
- [ ] Dump: parse threads write docs via `ThreadWriter` inline (no channel, no StreamingDocWriter)
- [ ] Multi-phase merge: later phases append via ops log (Merge semantics in caller, DataSilo stores raw bytes)
- [ ] Server read path: `silo.get(slot)` + DocOpCodec decode → StoredDoc
- [ ] Remove DocCache (mmap reads at 23M/s replace it)
- [ ] Remove StreamingDocWriter, ShardStoreBulkWriter, ShardPreCreator

### Phase 3: BitmapSilo Integration

Replace FilterBitmapStore + SortBitmapStore + AliveBitmapStore.

- [ ] `BitmapKey` type: hash of (field_name, value) or (field_name, bit_layer)
- [ ] Dump: write bitmap merge ops via ThreadWriter
- [ ] Post-dump compaction: replay ops → build RoaringBitmaps → serialize → write to data file
- [ ] Query path: `silo.get(key)` → frozen bitmap bytes → `FrozenRoaringBitmap::view()` (zero-copy)
- [ ] Mutation path: bitmap diffs as ops (union/subtract)
- [ ] Lazy loading eliminated (mmap = instant access)

### Phase 4: CacheSilo + Cleanup

- [ ] BoundStore → CacheSilo
- [ ] Delete old storage code (~11K lines): docstore.rs, doc_cache.rs, bitmap_fs.rs, shard_store.rs, shard_store_bitmap.rs, shard_store_meta.rs, shard_store_doc.rs, bound_store.rs
- [ ] Update CLAUDE.md, tests, docs

---

## What Stays vs What Goes

| Keep | Why |
|------|-----|
| ConcurrentEngine | Core query/mutation orchestration |
| InnerEngine + ArcSwap | Snapshot isolation for reads |
| Flush thread | Mutation batching + cache maintenance |
| FilterIndex, SortIndex | In-memory bitmap structures for queries |
| QueryExecutor, sort.rs | Query evaluation logic |
| DocOpCodec format | Fastest encode/decode (71ns/16ns) |
| DumpProcessor CSV parsing | Parse + enrichment logic unchanged |

| Delete | Replaced by |
|--------|-------------|
| DocStoreV3 + DocShardStore | DataSilo (doc reads/writes) |
| StreamingDocWriter | ParallelWriter (dump) |
| ShardStoreBulkWriter | ParallelWriter |
| ShardStore generic | DataSilo |
| FilterBitmapStore | DataSilo (bitmap silo) |
| SortBitmapStore | DataSilo (bitmap silo) |
| AliveBitmapStore | DataSilo |
| DocCache | Eliminated (mmap reads fast enough) |
| ShardPreCreator | Eliminated (no per-shard files) |
| BoundStore | DataSilo (cache silo) |
| bitmap_fs.rs | Eliminated |

**Lines deleted: ~10,000. Lines added: ~1,500 (DataSilo crate). Lines rewritten: ~750.**

---

## Code Removal Map (from LSP scout)

### Files to Delete Entirely (9,790 lines)

| File | Lines | Purpose |
|---|---|---|
| `src/shard_store.rs` | 1,779 | ShardStore generic engine, generation system, codecs |
| `src/shard_store_bitmap.rs` | 1,723 | Alive/Filter/Sort bitmap stores |
| `src/shard_store_meta.rs` | 292 | MetaStore (slot_counter, time_buckets, cursors) |
| `src/bitmap_fs.rs` | 1,137 | Legacy BitmapFs (.roar file persistence) |
| `src/doc_cache.rs` | ~786 | DocCache (generational LRU, replaced by mmap) |
| `src/bound_store.rs` | 1,083 | BoundStore (cache persistence, replaced by CacheSilo) |

### From shard_store_doc.rs — Partial Delete

**Delete:** DocStoreV3, DocSnapshot, DocOp enum, DocOpCodec apply logic, DocSnapshotCodec, SlotHexShard, ShardStoreBulkWriter, StreamingDocWriter, ShardPreCreator.

**Keep:** `StoredDoc` (doc schema type), `PackedValue` (value enum), `DocOpCodec::encode_op/decode_op` (fastest encoding at 71ns), field conversion utilities. Move these to a new `src/doc_format.rs` or keep in a trimmed `shard_store_doc.rs`.

### Files to Rewire (12 files, ~750 lines)

| File | Lines Changed | Key Changes |
|---|---|---|
| `concurrent_engine.rs` | ~500 | Remove 6 storage fields + doc_cache, delete pin_shard_generations/compact_all/purge_bound_store, rewrite build() init, rewrite docstore accessor |
| `dump_processor.rs` | ~250 | Rewrite save_phase_to_disk signature (4 ShardStore params → DataSilo), rewrite bitmap save loops, delete StreamingDocWriter/ShardPreCreator refs |
| `server.rs` | ~25 | Remove 3 pin_shard_generations() calls in capture handlers |
| `capture.rs` | ~40 | Remove gen_start/gen_stop fields and set methods |
| `ops_processor.rs` | ~20 | Rewrite DocStoreV3 constructor + tests |
| `ingester.rs` | ~30 | Rewrite DocSink wrapper type |
| `engine.rs` | ~15 | Rewrite DocStoreV3::open() calls |
| `mutation.rs` | ~20 | Rewrite docstore parameter types + tests |
| `config.rs` | ~25 | Delete DocCacheConfigEntry + doc_cache field |
| `pg_sync/backfill.rs` | ~40 | Remove BitmapFs references |
| `pg_sync/bulk_loader.rs` | ~5 | Update writer type |
| `metrics.rs` | ~30 | Remove BoundStore/DocCache/ShardStore metric stubs |

### Generation System Removal

All generation/pinning symbols removed with ShardStore:
- `shard_store.rs`: `current_generation()`, `pin_generation()` — deleted with file
- `concurrent_engine.rs`: `pin_shard_generations()` method — delete
- `server.rs`: 3 call sites to `pin_shard_generations()` — delete
- `capture.rs`: `gen_start`, `gen_stop`, `set_gen_start()`, `set_gen_stop()` — delete

**Safe:** `ops_wal.rs::current_generation()` is unrelated (WAL file naming) — KEEP.

### Files Safe / No Changes

- `src/loader.rs` — only imports StoredDoc (schema type, stays)
- `src/ops_wal.rs` — WAL generations separate from ShardStore
- `src/query.rs`, `src/sort.rs`, `src/filter.rs` — pure in-memory operations

---

## Execution Plan

### Step 1: Finish DataSilo Crate

Complete the generic `DataSilo<K: SiloKey>` with:
- [ ] Generic over key type (currently u32-only)
- [ ] Thread-safe `append_op` (Mutex<BufWriter> for ops log — low contention)
- [ ] Compaction: replay ops → rewrite data file → clear ops log
- [ ] Delete support (tombstone in index)
- [ ] `flush()` method (explicit mmap flush for crash safety)

### Step 2: Delete Old Storage + Wire DocSilo

Do this in ONE pass — delete the files, fix compile errors by wiring DataSilo:

1. Delete 6 storage files
2. Trim `shard_store_doc.rs` → `doc_format.rs` (keep StoredDoc, PackedValue, DocOpCodec)
3. Add `DataSilo<u32>` as docstore field in `ConcurrentEngine`
4. Rewrite `build()` to open/create DocSilo
5. Rewrite doc read path: `silo.get(slot)` + DocOpCodec decode
6. Rewrite dump pipeline: ParallelWriter inline in parse loop
7. Delete generation pinning from server.rs + capture.rs
8. Delete DocCache, config entries, metric stubs
9. Fix all compile errors in secondary consumers
10. Run tests

### Step 3: Wire BitmapSilo

1. Add `DataSilo<BitmapKey>` for filter + sort + alive bitmaps
2. Dump pipeline: write bitmap merge ops to BitmapSilo during parse
3. Post-dump compaction: replay ops → build bitmaps → write to data file
4. Query path: read frozen bitmaps from silo
5. Mutation path: diffs as ops
6. Remove in-memory bitmap accumulation from dump (optional — can keep for now)

### Step 4: Wire CacheSilo + Final Cleanup

1. Replace BoundStore with CacheSilo
2. Final code cleanup — remove any remaining dead refs
3. Update CLAUDE.md architecture section
4. Update all design docs
