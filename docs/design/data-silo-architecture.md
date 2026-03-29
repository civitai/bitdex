---
status: PROPOSED
created: 2026-03-28
author: Justin (voice memo) + Fredrick (benchmarks) + Dakota (documentation)
---

# Data Silo Architecture

> Replaces the current DocStore V2 sharded file system with per-thread "data silos" — large files with mmap reads and append-only writes. Solves the docstore write bottleneck that accounts for 85% of row processing time at 107M scale.

---

## Problem Statement

DocStore V2 (append-only tuple logs across ~210K shard files) is the dominant bottleneck in the dump pipeline:

| Metric | Current State |
|--------|--------------|
| Docstore write share | **85% of images row processing time** (486 thread-seconds for 10M rows) |
| Write rate | 431K rows/s (DashMap + BufWriter + 210K shards) |
| File creation overhead | ~4K files/s — metadata operations dominate |
| BufWriter contention | Multiple threads competing for NVMe with small flushes |
| Read path | DashMap DocCache at <1us (fast, but requires memory) |

The root cause: too many small files creates filesystem metadata overhead that dominates the I/O budget. File creation is slower than file writing.

## Solution: Data Silos

A **data silo** is a large file — one per CPU thread during bulk load. Instead of 210K shard files, the dump processor writes to N large files (N = thread count, typically 28). Each silo contains the full document data for the slots processed by that thread.

### Core Concept (from Justin's voice memo)

> "You have these dock stores that are however many threads you have on your machine. And they create indexes as they're populating the stores. And then the indexes get put together. Because then you just read all of that index into memory, and it points you to exactly the location within this store that you need to go to. These silos."

> "A data silo — it has the value, the snapshot, the state, and it has operations on the state."

### Architecture

```
Bulk Load (dump processor):
  Thread 0 → silo_00.dat (append docs sequentially, build local index)
  Thread 1 → silo_01.dat
  ...
  Thread 27 → silo_27.dat
                    ↓
  Merge phase: combine 28 local indexes → global doc_index.bin
                    ↓
Serving:
  mmap all silo files → read via index[slot_id] → (file_id, offset, length) → mmap slice
                    ↓
Steady-state writes:
  Append new doc version to active silo → update index entry → old data is dead space
                    ↓
Compaction:
  When dead space exceeds threshold → rewrite silo excluding dead entries
```

## Benchmark Results

### 1M Scale (Fredrick, session 3587f522)

From `bench_unified_docstore.exe` at 1M rows × ~200 bytes/doc:

| Approach | Rate | vs Current (431K/s) |
|----------|------|---------------------|
| Per-thread staging (28 files, 256KB BufWriter) | 20.1M rows/s | 47x faster |
| Single file sequential | 227K/s | 0.5x (slower) |
| Current (DashMap + BufWriter + 210K shards) | 431K/s | baseline |

**NOTE:** The 20.1M/s number was entirely in OS buffer cache (1M × 208 bytes = 208MB fits in RAM). It does not hold at production scale.

### 107M Scale (Josh + Ollie, 2026-03-28)

Full-scale validation at 107M rows × 208 bytes = 21.4 GB total data:

| Approach | Rate | GB/s | Notes |
|----------|------|------|-------|
| **BufWriter 8MB (28 threads)** | **4.77M/s** | **0.92** | **Winner — NVMe saturated** |
| BufWriter 256KB (28 threads) | 4.60M/s | 0.89 | Marginal difference |
| mmap write (28 threads) | 2.76M/s | 0.54 | BufWriter beats mmap for sequential |
| Direct write (28 threads) | 2.17M/s | 0.42 | Worst — no buffering |
| Single thread mmap | 2.14M/s | 0.42 | Single thread = ~same as NVMe ceiling |

**Root cause of 1M→107M gap:** At 208MB the OS buffer cache absorbs all writes (20.1M/s = memory speed). At 21.4GB the OS must flush to NVMe (~0.9 GB/s sequential write ceiling on this hardware). The bursty rate pattern (0.8→2.8→1.5 M/s) confirms write-back cache filling then flushing.

**Revised target: >=4M/s** (approved by Tom, 2026-03-28). 4.77M/s = 22s for 107M docs vs 248s for current DocStore V2 = **11x faster**. Docstore drops from 85% of dump time to ~10%.

**Design change:** BufWriter increased from 256KB to 8MB for production.

### Read Path (Ollie, Benchmark 3)

mmap index at 107M entries (1.3GB file):

| Metric | Goal | Actual | Pass/Fail |
|--------|------|--------|-----------|
| mmap creation | <100ms | 0.118ms | PASS (830x under) |
| First random access | <1ms | 0.006ms | PASS |
| 10K random (warm) | <1ms | 0.418ms | PASS |
| Per-access (warm) | 7ns | 42ns | Acceptable (TLB pressure at scale) |
| 10K random (cold) | - | 39.7ms | Page faults on first access |

**Recommendation:** Add startup warmup pass to pre-fault index pages. Once warm, stays in page cache permanently.

| Read/Upsert | Latency |
|-------------|---------|
| **mmap random lookup (warm)** | **42 ns** |
| seek + read | 3.9 us/lookup |
| Append upsert | 10.4 us/upsert |
| Dead space after 10K upserts | 0.99% of file |

## Design Details

### 1. Bulk Write Path (Per-Thread Staging)

During dump processing, each rayon thread writes to its own dedicated silo file:
- **Zero contention** — no locks, no DashMap, no shared BufWriter
- **8MB BufWriter per thread** — large buffer reduces syscalls, saturates NVMe at ~0.9 GB/s
- **Sequential writes** — each thread appends docs contiguously to its file
- **Local index built concurrently** — `Vec<(u32 slot_id, u64 offset, u32 length)>` per thread
- Files are pre-created before dump starts (avoids the 4K files/s creation bottleneck)

### 2. Silo Index

After bulk load, the per-thread local indexes are merged into a single global index:

```
doc_index.bin:
  [u32 num_entries]
  [entries: N × (u8 file_id, u64 offset, u32 length)]   // indexed by slot_id
```

- 107M entries × 13 bytes = ~1.4 GB in memory
- Persisted to disk, mmap'd on startup
- `index[slot_id]` → `(file_id, offset, length)` — O(1) lookup

### 3. mmap Read Path

All silo files are mmap'd. Reading a document:
1. `index[slot_id]` → `(file_id, offset, length)`
2. `silo_files[file_id][offset..offset+length]` → document bytes
3. **42 ns per lookup at 107M scale** (7ns at 1M due to TLB pressure at scale — Benchmark 3 validated). Still 24x faster than DocCache (<1us) and 380,000x faster than cold DocStore disk reads (16ms).

> "With something like mmap you can access it as if it was memory but not have the actual thing in memory."

The OS page cache manages hot/cold data transparently. Frequently accessed documents stay in page cache; cold data pages out to disk.

### 4. Steady-State Write Path (Upserts)

After bulk load, single-document upserts:
1. Serialize new doc version
2. Append to one designated "active" silo file
3. Update `index[slot_id]` to new `(file_id, offset, length)`
4. Old data in the original silo becomes dead space

**10.4 us/upsert** from benchmarks. Old data stays on disk until compaction.

### 5. Compaction

When dead space in a silo exceeds a threshold:
1. Scan the index for entries pointing into this silo
2. Rewrite the silo sequentially, excluding dead entries
3. Update index entries to new offsets
4. Delete old silo file

Compaction is lazy — only happens when needed. Dead space after 10K upserts was measured at 0.99% of file.

### 6. Silo Index Merging

> "You could even keep those files separate. You just mmap all of them and then join them in memory. They're just different maps that all come together."

Options for the global index:
- **Option A:** Merge into single `doc_index.bin` — simpler, one mmap
- **Option B:** Keep per-silo index files, mmap all, join in memory — allows independent compaction

### 7. Relation to ShardStore

**Decision (Josh review):** Build as a clean new module (`src/data_silo.rs`), NOT integrated into ShardStore. ShardStore's bucket sharding, generation model, and CRC ops log add overhead silos don't need. The silo model is fundamentally different: large sequential files vs many small shards.

- **ShardStore** stays for bitmap persistence (filter, sort, alive)
- **Data silos** handle document persistence (replaces DocStore V2)
- Both coexist — different storage models for different access patterns

### 8. Crash Recovery

On startup:
1. Load persisted `doc_index.bin` (may be slightly stale)
2. Scan the ops/append region of each silo for entries newer than the persisted index
3. Replay into index
4. No data loss — everything is append-only

## Design Evolution (How We Got Here)

This architecture didn't emerge in a vacuum. It was driven by systematic elimination of alternatives across two intensive sessions (Josh dump processor + Fredrick/Liz perf optimization).

### Approaches Tried and Eliminated (Josh's Session)

| # | Approach | Result | Why It Failed |
|---|----------|--------|---------------|
| 1 | Parallel bucket writes + zero-clone encoding | 18m26s → 16m03s | Helped, but still 85% docstore I/O |
| 2 | Pipeline overlap (concurrent save + processing) | Backfired | Disk I/O contention — readers and writers on shared NVMe degrade each other |
| 3 | Channel-based single writer thread | Failed | Single consumer can't keep up with 28 producers |
| 4 | Batched channel writes (10K tuples/batch) | Marginal | Still 10x too slow — fundamentally can't avoid per-shard Mutex |
| 5 | Direct writes with buffer reuse | Abandoned | Mutex contention dominates over allocation cost |
| 6 | `filter_only` skip for tags/tools/etc | **Shipped** | Only approach that worked within current arch — avoids the write entirely |

### The Breakthrough (Fredrick's Session)

After micro-optimizations proved insufficient, Fredrick explored the fundamental question: what if we eliminate the 210K shard files entirely?

**Key insights from the exploration:**
- Parse optimization (2.2x), indexed field reuse (1.2x), sort layer packing (2.6x), docstore batching (2.3x) — total estimated savings only ~25 seconds. Not enough.
- Docstore sub-timing revealed: serialize = 4%, **write = 96%**. The bottleneck is filesystem I/O, not CPU.
- Pre-existing files gave 5.7x speedup (300K/s → 1.7M/s) — proving file creation metadata is the dominant cost
- GPT and Gemini were both consulted and independently recommended two-phase staging (write to N large files, then merge)
- Per-thread staging benchmark: **20.1M rows/s with zero contention** — 47x faster than 210K shards

The data silo concept emerged from Justin's synthesis of these findings: per-thread large files with mmap reads and append-only ops. See voice memo (2026-03-28T01-28-04).

### Session Reviews

Full extraction docs with all decisions, gotchas, and regression risks:
- `docs/reviews/fredrick-data-silo-session.md` — benchmark evolution, design alternatives, GPT/Gemini consultation
- `docs/reviews/josh-dump-processor-session.md` — six approaches tried, why each failed, filter_only as interim fix
- `docs/reviews/liz-dump-perf-session.md` — 10-hour perf session, root cause (missing filter_only), Windows gotchas

## Design Review Concerns (Josh, 2026-03-28)

### 1. Index Memory — mmap the index itself
1.4GB index (107M × 13 bytes) fits on our 128GB machine but could be tight on smaller deployments. **Resolution:** mmap `doc_index.bin` instead of reading into a Vec. The OS pages in what's needed. Also speeds up startup — no 1.4GB read.

### 2. Multi-Value Field Accumulation
Tags phase writes (tag_id, image_id) — each image has ~42 tags on average. Storing per-row entries and merging on read would create 42x bloat. **Resolution:** Accumulate the full value list per slot during bulk load, write the final merged document once. Same pattern as bitmap building (accumulate into Vec/HashMap per thread).

### 3. Phase Ordering (Biggest Concern)
Dump phases run sequentially — images writes nsfwLevel/userId/etc, then resources writes modelVersionIds/baseModel, then metrics writes reactionCount/etc. Each phase writes different fields for the SAME slot to potentially DIFFERENT thread files (since slot→thread mapping varies per phase). **Resolution:** Each phase reads the existing silo entry for the slot, merges its new fields, and writes a new complete entry. Old entry becomes dead space. The index always points to the latest complete document.

### 4. filter_only Still Needed
Even with 20M/s silo writes, tags phase (5.4B rows) would take ~270s. Currently tags runs in 4m7s WITH filter_only (no docstore). Adding 270s of silo writes = ~8.5 min total for tags — a regression. **Resolution:** Keep `filter_only` for bitmap-only fields (toolIds, techniqueIds, modelVersionIdsManual) even with silos. tagIds and modelVersionIds still need docstore for post-query merge filtering.

### 5. Clean Module, Not ShardStore Integration
ShardStore's bucket sharding, generation model, and CRC ops log add overhead silos don't need. The silo model is fundamentally different: large sequential files vs many small shards. **Resolution:** New `src/data_silo.rs` module. Keep ShardStore for bitmap persistence, silos for document persistence.

## What Needs to Be Built

1. `DocDataFile` — append-only data file with bulk write + single-doc append
2. `DocIndex` — flat `Vec<(u8, u64, u32)>`, persist/restore, mmap read
3. `BulkDocWriter` — per-thread staging during dump, builds local index
4. Wire into dump processor (replace `BulkWriter` / `append_tuple_raw`)
5. Wire into read path (replace `DocStore::get`)
6. Wire into upsert path (replace `DocStore::put`)
7. Compaction background task

## Impact on Current Architecture

| Component | Current | After Data Silos |
|-----------|---------|-----------------|
| Dump write | BulkWriter → 210K shard files (431K/s) | Per-thread silos (4.8M/s, 11x) |
| Point read | DocCache (DashMap, <1us) or DocStore disk (16ms) | mmap (42ns warm) |
| Upsert | Read-merge-write shard + DocCache update | Append to silo + update index (10.4us) |
| Memory for reads | DocCache 1GB LRU | mmap page cache (OS manages) |
| Files at 107M | ~210K shard files + directories | 28 silo files + 1 index file |
| Startup | Load field_dict.bin, lazy shard reads | mmap silo files + load index (~1.4GB) |

## Source Material

- **Voice memo:** `C:\Dev\Repos\ai\ai-conversations\conversations\voice-memos\2026-03-28T01-28-04.md`
- **Extracted summary:** `C:\Dev\Repos\ai\ai-conversations\docs\bitdex-storage-definition.md`
- **Benchmark session:** Fredrick, session 3587f522 (`bench_unified_docstore.exe`)
- **Docstore bottleneck data:** Liz perf session (`docs/reviews/liz-dump-perf-session.md`)
- **Josh dump processor benchmarks:** memory `project_dump_processor_107m_benchmarks.md`
