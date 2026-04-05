# DataSilo Implementation Plan

## Architecture (Final — agreed with Justin 2026-04-03)

### Core Principle: One Write Path

ALL writes go through the mmap'd ops log. The data file is ONLY written by compaction. No hybrid approaches, no separate ParallelWriter for bulk vs steady-state.

### Three mmap'd files per silo

| File | Purpose | Written by |
|------|---------|------------|
| `index.bin` | key → (offset, length, allocated) in data.bin | Compaction only |
| `data.bin` | Packed values, read-only between compactions | Compaction only |
| `ops.log` | Append-only mutations with CRC32 framing | Everything |

### Write primitive: Parallel mmap append

All writes use the same primitive: atomic bump allocator with 1MB thread-local regions on an mmap'd file. This achieves 32.7M ops/s with 32 threads (benchmarked).

- **Dump (32 rayon threads):** Each thread grabs 1MB regions, writes CRC32-framed ops sequentially within its region. Zero contention.
- **Steady state (1 thread):** Same primitive, just one thread bumping the cursor. 8.4M ops/s.
- **Compaction (writes to data file):** Same primitive rebuilding the data file.

### No pending HashMap

The mmap'd ops log IS the read cache (page cache handles it). No heap duplication of ops. On read: check data file via index, then scan ops log for overrides.

### Two index modes

- **Dense (u32 key):** Array index, O(1) lookup. For doc storage (slot_id = position).
- **Hash (u64 key):** Open-addressed mmap'd hash table. For bitmap/cache storage (sparse keys).

---

## Compaction

### Cold compaction (initial dump — no existing data file)

After dump phases write all ops to the log:

1. Scan ops log: for each key, find the LAST Put op (last-write-wins)
2. Build index: key → (offset_in_ops_log, length)
3. Write index.bin from the index
4. Rename ops.log → data.bin (the ops log becomes the data file)
5. Start a fresh ops.log

**No value copying.** Just an index scan + rename. Index for 109M keys = 1.7GB.

### Hot compaction (steady state — existing data file with buffer)

Ops have pre-allocated slots in data.bin (1.3x buffer ratio + 256B min):

1. Read ops log (mmap'd, streaming via `for_each`)
2. For each Put op:
   - Look up key in index → get (offset, length, allocated) in data.bin
   - If new value fits in `allocated` bytes: **write in-place** at that offset
   - If too big: mark for overflow (append to end of data file)
3. Parallel: threads can write to different slots simultaneously (disjoint regions in data file)
4. Handle overflows: extend data file, write overflow entries, update index
5. Truncate ops log

**Embarrassingly parallel** for in-place updates — each key's allocated region is disjoint.

---

## Benchmark Data (all on 128GB machine)

| What | Rate | Notes |
|------|------|-------|
| Ops log write (1MB regions, 32 threads) | 32.7M/s | CRC32 framed |
| Ops log write (64KB regions, 32 threads) | 10.7M/s | 0.1% waste |
| Ops log write (sequential, 1 thread) | 8.4M/s | Steady state |
| BufWriter sequential (old approach) | 7.9M/s | Replaced |
| DataSilo read (random key, hot mmap) | 23-27M/s | Index deref + data deref |
| DocOpCodec encode | 71ns (14.1M/s) | Keep this format |
| DocOpCodec decode | 16ns (62.5M/s) | Fastest option |
| HashIndex insert | 40M/s | Open-addressed mmap |
| HashIndex lookup | 430M/s | Hot cache |

---

## Implementation Phases

### Phase 1: DataSilo Crate Core ✅ DONE
- [x] DataSilo with open/get/bulk_load
- [x] OpsLog with CRC32 append + replay
- [x] IndexEntry (16 bytes: offset + length + allocated)
- [x] ParallelWriter with atomic bump + 1MB regions
- [x] Buffer headroom (1.3x ratio, 256B min_entry_size)
- [x] HashIndex for sparse u64 keys (12 tests, 40M/430M ops/s)
- [x] 18 tests passing

### Phase 2: Simplified Write Architecture (IN PROGRESS)
- [x] Mmap'd OpsLog (replaces BufWriter-based log)
- [x] Parallel write support in OpsLog (1MB regions, 32M ops/s)
- [ ] Remove pending HashMap from DataSilo
- [ ] Remove separate ParallelWriter/ThreadWriter structs from DataSilo
- [ ] Remove bulk_load, prepare_parallel_writer, finish_parallel_write
- [ ] OpsLog.for_each() streaming iterator (no Vec allocation)
- [ ] get_with_ops() reads data file + scans ops log (no HashMap)
- [ ] Update DocSiloAdapter to use new API
- [ ] Update all callers in concurrent_engine, dump_processor
- [ ] Tests passing

### Phase 3: Compaction
- [ ] Cold compaction: scan ops → build index → rename ops.log → data.bin
- [ ] Hot compaction: in-place writes to pre-allocated slots in data.bin
- [ ] Overflow handling for entries that grew beyond allocated buffer
- [ ] Parallel compaction using same mmap write primitive
- [ ] File swap: atomic rename of new data file, truncate ops log
- [ ] Tests for both cold and hot paths

### Phase 4: DocSilo Integration (MOSTLY DONE)
- [x] DocSiloAdapter wired into ConcurrentEngine
- [x] Mutation path: put/patch/delete via DocSiloAdapter → ops log
- [x] Query path: get(slot) + DocOpCodec decode → StoredDoc
- [x] DocCache removed (mmap reads at 23-27M/s replace it)
- [x] StreamingDocWriter removed
- [x] ShardStoreBulkWriter removed
- [ ] Dump pipeline: all phases write through ops log
- [ ] Post-dump compaction (cold path)
- [ ] Validation with small dataset

### Phase 5: BitmapSilo Integration (NOT STARTED)
- [ ] BitmapKey type: hash of (field_name, value) or (field_name, bit_layer)
- [ ] Dump: write bitmap ops via ops log
- [ ] Post-dump compaction builds final bitmaps
- [ ] Query path: get(key) → frozen bitmap bytes → FrozenRoaringBitmap::view()
- [ ] Mutation path: bitmap diffs as ops
- [ ] Lazy loading eliminated (mmap = instant access)
- [ ] Save/restore on restart

### Phase 6: CacheSilo + Final Cleanup (NOT STARTED)
- [ ] BoundStore → CacheSilo
- [ ] Meta persistence (slot_counter, cursors, deferred_alive)
- [ ] Update CLAUDE.md, tests, docs
- [ ] Remove all remaining TODO comments

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

| Deleted | Lines | Replaced by |
|---------|-------|-------------|
| shard_store.rs | 1,779 | DataSilo |
| shard_store_bitmap.rs | 1,723 | BitmapSilo (Phase 5) |
| shard_store_meta.rs | 292 | Simple file I/O |
| shard_store_doc.rs | 2,990 | doc_format.rs + DocSiloAdapter |
| bitmap_fs.rs | 1,137 | BitmapSilo (Phase 5) |
| doc_cache.rs | 786 | Eliminated (mmap reads fast enough) |
| bound_store.rs | 1,083 | CacheSilo (Phase 6) |
| field_handler.rs | ~200 | Dead code |
| preset.rs | ~100 | Dead code |
| **Total deleted** | **~10,090** | |
