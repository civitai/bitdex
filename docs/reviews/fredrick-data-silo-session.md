# Fredrick Session Review: Data Silo / DocStore Write Architecture

**Session:** `3587f522-1326-451b-93e5-5ff9e8deaaa1`
**Agent:** Fredrick (with Justin)
**Date:** 2026-03-28
**Duration:** Long session (~6+ hours), extensive benchmarking and design iteration

---

## Session Overview

Justin brought Fredrick in to investigate why the dump processor (bulk CSV loader) takes 16 minutes vs the old single-pass loader's ~10 minute target. The session evolved from bottleneck analysis into a fundamental rethink of the docstore write architecture, driven by microbenchmark data.

---

## 1. Initial Bottleneck Analysis

### Dump Processor Pipeline (Sequential Phases)

| Phase | CSV | Size | Rows |
|-------|-----|------|------|
| Tags | tags.csv | 63 GB | 4.48B (multi-value) |
| Images | images.csv | 14 GB | 107.6M |
| Resources | resources.csv | 777 MB | 41.5M |
| Tools | tools.csv | 50 MB | 4.1M |
| Techniques | techniques.csv | 71 MB | small |
| Metrics | metrics.csv | ~500 MB | 91M |
| Collections | collection_items.csv | variable | variable |

### Key Correction

Fredrick initially read the wrong file (`single_pass.rs`). The actual production code is `dump_processor.rs` (2735 lines) — a config-driven, generic loader. Many optimizations Fredrick flagged as missing were already implemented:

- **Save pipelining** via `SaveHandle::spawn` (background thread)
- **Zero-copy CSV parsing** via `parse_delimited_line` returning `Vec<&[u8]>` slices into mmap
- **Reusable serialize buffer** allocated once per thread, reused via `clear()`
- **Parallel merge** via `into_par_iter()`

### Josh's Timing Data (Baseline: 16m03s on commit `2c6191d`)

| Phase | Parse | Merge | Save | Wall Clock |
|-------|-------|-------|------|------------|
| Tags | 181s | -- | 45s | ~226s |
| Images | 174s | 17s | 54s | ~245s (+47s enrichment) |
| Resources | 50s | 2s | 54s | ~108s (+2s enrichment) |
| Tools | <1s | -- | <1s | <1s |
| Techniques | <1s | -- | <1s | <1s |
| Metrics | 90s | -- | <1s | ~90s |

**Important:** The "sub-10 min" target came from Justin's memory of the scatter-gather loader's bitmap-only phases, not end-to-end time. Scatter-gather was actually 28 min total at 107M with docstore. The dump processor at 16 min is faster overall.

---

## 2. Code-Level Findings (Verified, Not Assumed)

1. **`to_indexed_fields` allocates per row instead of reusing buffer.** Line 1392 allocates `Vec<Option<&str>>` per row. A `fill_indexed_fields` function exists that reuses a pre-allocated buffer, but nobody calls it. ~91M unnecessary Vec allocations per metrics phase.

2. **Single-pass metrics is NOT zero-alloc either.** Uses `line.splitn(5, ...).collect()` which also allocates `Vec<&[u8]>` per row, plus `rmp_serde::to_vec` per field. The gap between single-pass and dump processor is smaller than initially assumed.

---

## 3. ShardStore vs BitmapFs Write Overhead

### What the Explorer Sub-Agent Got Wrong
- CRC32 is NOT computed on snapshot writes. Only on ops log entries (empty during dumps).

### What the Explorer Got Right
- Redundant `create_dir_all` in `write_shard_file_atomic` (line 314) after `ensure_filter_dirs` already pre-created directories.

### The Real Issue: Sort Layer File Count

| | BitmapFs | ShardStore |
|--|----------|-----------|
| Sort format | All 32 layers in one `.sort` file | One `.shard` file per bit position |
| Files for 5 sort fields | 5 files | 160 files |
| Fsyncs | 5 | 160 |

This was confirmed by benchmarking and fixed with the packed sort layer implementation (see Section 5).

---

## 4. Microbenchmark Results (10M rows, release mode)

| Bench | Current | Optimized | Speedup | At-Scale Impact |
|-------|---------|-----------|---------|-----------------|
| Parse: Vec split vs zero-alloc | 0.45s | 0.20s | 2.2x | Tags already fast-pathed; Metrics ~2s save |
| Indexed fields: alloc vs reuse | 2.82s | 2.31s | 1.2x | ~5s across all phases. Marginal. |
| Sort layer write: 32 files vs 1 | 80.7ms | 31.3ms | 2.6x | ~1-2s total across all save phases |
| Docstore: per-field vs batched | 0.30s/1M | 0.13s/1M | 2.3x | Images 107M: ~32s to ~14s |

**Total estimated savings from all four fixes: ~25s (16m to ~15.5m).** Not enough to hit the target.

---

## 5. Implementations Completed (Three Sub-Agents)

### 5a. Sort Layer Packing
- **Files changed:** `shard_store_bitmap.rs` (+676 lines), `shard_store.rs` (+6 lines)
- New `PackedSortBitmapStore` with `PackedSortSnapshot` (BTreeMap<u8, RoaringBitmap>)
- Legacy fallback: reads both old per-layer and new packed format
- Writes always use new format
- 27 tests pass (10 new)

### 5b. Docstore Write Batching
- **Files changed:** `docstore.rs` (+165 lines), `dump_processor.rs` (+39 lines)
- New `BulkWriter::append_tuples_raw` method: one DashMap lookup + one Mutex lock for all fields per row
- Fredrick's review fix: replaced `serialize_buf.clone()` per field with offset tracking into a shared buffer

### 5c. Per-Component Instrumentation
- **Files changed:** `dump_processor.rs` (+152 lines)
- Thread-local accumulators, 8 components timed in main loop
- Fredrick's review fix: renamed `index_ns` to `field_loop_ns` to avoid confusion about double-counting

### 5d. Pipeline Simplification
- Inlined save+reload into `process_dump`, removed `SaveHandle` pipeline complexity
- Net change: -86 lines. Server handler reduced from ~90 lines to trivial.
- `reload_after_dumps` extracted as a separate function called only for `sets_alive` phases

---

## 6. Full Benchmark Run Results

### Run 1: 17m47s (vs 16m03s baseline)

**Confounding factor:** Built on commit `3699a42` which Josh said already regressed to 18m16s due to pipeline overlap changes. Relative to that base, our changes improved by ~30s.

**Sort save confirmed:** Images sort save dropped from ~45s to 0.7s. Packed sort layers work.

**Save breakdown (new instrumentation):**

| Phase | Filter Save | Sort Save | Total Save |
|-------|-------------|-----------|------------|
| Tags | 42.3s | 0s | 42.3s |
| Images | 26.0s | 0.7s | 26.7s |
| Resources | 1.6s | 0s | 1.6s |
| Metrics | 0s | 1.0s | 1.0s |

**Regression cause:** `mark_fields_pending_reload` was called after EVERY dump phase, triggering lazy loading of all fields from disk (tagIds = 31K values = ~6.6s per reload). Called 6 times = ~40s wasted. Fixed by moving reload to server handler, called once for `sets_alive` phases only.

### Run 2: Images 10M Rows (Component Breakdown)

This was the critical run that identified the real bottleneck:

| Component | Thread-seconds | % of Row Time | Wall Time (~div 28) |
|-----------|---------------|---------------|---------------------|
| **docstore** | **511.0s** | **84.8%** | **~18.2s** |
| computed | 44.9s | 7.5% | ~1.6s |
| bitmap | 22.8s | 3.8% | ~0.8s |
| enrich | 14.8s | 2.5% | ~0.5s |
| parse | 8.2s | 1.4% | ~0.3s |
| filter | 0.3s | 0.1% | ~0.01s |

**Docstore writes are 85% of the entire processing time.**

### Run 3: With Docstore Sub-Timing

| | Thread-seconds | % of Docstore |
|--|---------------|---------------|
| Serialize | 21.9s | 4% |
| **Write (BufWriter write_all)** | **486.2s** | **96%** |

**The bottleneck is disk I/O to 210K shard files, not serialization.**

---

## 7. Design Decision: Why Per-Thread Files? Why Flat Index? Why mmap?

### The Core Problem

The V2 docstore uses ~210K shard files (512 docs per shard). Each image row writes ~15 fields via `append_tuple_raw`, each doing: DashMap lookup + Mutex lock + `write_v2_tuple` (4x `write_all` calls through BufWriter). At 107M rows x 15 fields = 1.6 billion lock-write cycles across thousands of files. The OS filesystem metadata layer is the bottleneck.

### Options Considered and Rejected

**1. Docstore batching (one lock per row instead of per field):**
- Implemented but only yielded 5% improvement (511s to 485s)
- The lock was not the bottleneck; the sheer volume of small writes was

**2. Larger BufWriter buffers (64KB/256KB):**
- Microbench: +24% to +30% improvement
- Helps but does not change the fundamental problem

**3. Single write_all per row (pre-encode all fields into one blob):**
- Microbench: only +6% improvement
- Not meaningful

**4. Accumulate full shards in memory then write once per shard:**
- Microbench: -34% (slower). HashMap overhead for accumulation exceeded the write savings.

**5. Buffer-then-flush to deterministic shards (memory-budgeted):**
- Microbench: ~300K/s, no faster than current 431K/s
- Confirmed: the bottleneck is 210K small files, not contention or buffering

**6. Larger shard size (4K docs/shard instead of 512):**
- Microbench: +31% (5860 shards to 733 shards at 1M rows)
- Helps but still fundamentally the small-files problem

**7. Vec<Mutex<BufWriter>> instead of DashMap:**
- No difference single-threaded. Shard ID is deterministic (`slot >> SHIFT`), so DashMap hash is unnecessary.
- Conceptually cleaner but not a performance win.

### What Actually Works

**Pre-creating shard files: 2.1x improvement** (microbench) and **5.7x improvement** (Justin's parallel bench: 300K/s to 1.7M/s). File creation syscalls are the dominant cost, not data writing.

**Per-thread staging files: 47x improvement** (microbench: 20.1M rows/s vs 431K/s). Each of 28 rayon threads writes to a single owned append-only file. Zero contention, zero locks, purely sequential I/O. At 107M rows extrapolated: ~5.3s total write time instead of ~230s.

### GPT and Gemini Consensus

Both independently recommended a two-phase bulk load:

**Phase 1 (fast):** Each thread writes serialized docs to its own sequential staging file. One packed blob per doc. No per-shard routing.

**Phase 2 (convert):** Read staging files, group docs by shard_id, write serving-format shard files. Single-threaded Phase 2 at 1M rows: 2.6-2.8s for 5860 shards.

Both agreed: the serving format (512-doc shards with deterministic paths) is fine. Only the bulk write path needs to change.

### Justin's Pushback: Are We Just Moving the Problem?

Justin questioned whether two-phase just delays the I/O. Fredrick explained:
- Phase 1 is essentially free (22.7M rows/s) because each thread writes to ONE file with zero contention
- Phase 2 writes 210K files, but does so single-pass with no contention, pre-sorted by shard
- The fundamental difference: Phase 2 creates files sequentially and writes each one once, vs the current approach where 28 threads fight over random shard files simultaneously

### Justin's Key Insight: Pre-Create Files During Tags Phase

Tags is the first and longest phase (~200s). Tags does not write to docstore. If a background thread pre-creates the ~210K docstore shard files during tags processing, by the time the images phase begins, all files exist. This yielded 5.7x improvement in Justin's parallel bench.

This is the simplest viable fix: no architectural changes needed. Same shard layout, same serving format. Just overlap file creation with the first processing phase.

### Exploration: Single-File + Flat Index (mmap)

Fredrick benchmarked the "Bitcask" model: all docs in one big file, flat `Vec<u64>` index mapping slot_id to (offset, length).

| Metric | Result |
|--------|--------|
| Write rate (per-thread staging) | 20.1M rows/s |
| mmap random lookup | 7 ns/lookup |
| seek + read | 3.9 us/lookup |
| Append upsert | 10.4 us/upsert |
| Dead space after 10K upserts | 0.99% |
| Index size at 107M | 856MB |

Reads at 7ns via mmap are faster than the current DashMap DocCache. But Justin raised the key concern: **the current system has deterministic shard locations requiring zero index.** The single-file approach trades that elegance for write speed, adding crash recovery, compaction, and dead space tracking complexity.

---

## 8. Instrumentation Gotchas Discovered

1. **Per-row `Instant::now()` at 5.4B rows is catastrophic.** Even with sampling at 1:1000, the `if sample` branch check and branch prediction misses in the tight inner loop caused a 25x slowdown on the tags phase.

2. **Both branches of `if sample { Instant::now() } else { Instant::now() }` call `Instant::now()`.** A bug in the sampling implementation called the timer unconditionally. This was the root cause of the 4x slowdown observed on the sampled run.

3. **Compile-time feature flags (`#[cfg(feature = "dump-timing")]`) are the right approach** for inner-loop instrumentation. Runtime sampling, even with a simple modulo check, is measurable at billions of rows.

4. **Phase-level timing (emit_stage) is sufficient for most analysis.** Save breakdown (filter/sort/alive/dict) and per-table enrichment timing give 90% of the needed insight without touching the hot loop.

5. **PowerShell `Tee-Object` drops stderr lines.** UTF-16 encoding issues caused component_timing output to vanish. Fix: redirect stderr to a separate file instead of merging with stdout.

6. **Stale binaries after code changes.** The binary wasn't rebuilt after the instrumentation commit, causing a full benchmark run to produce no component data. Always verify binary timestamp matches latest commit.

7. **Windows zombie processes from bash background jobs.** Launching the server as `&` in bash on Windows creates unkillable zombies holding 55-67GB of mmap'd file descriptors. Launch from PowerShell for clean process control.

8. **`RAYON_NUM_THREADS` env var may not propagate through bash background jobs on Windows.** The 25% CPU usage and 25x slowdown (1.5M/s vs 37M/s tags) was caused by rayon defaulting to 1 thread because the env var was lost.

---

## 9. Relationship to DocStore V2 (Current) and V3 (Proposed)

### DocStore V2 (Current Production)
- Append-only tuple logs per shard (512 docs/shard)
- Per-field tuples: `slot(4) + field_idx(2) + len(2) + value(~5)` = ~13 bytes per field
- LIFO scan for reads (read backwards, first match wins)
- Field dictionary encoding for low-cardinality strings
- The V2 format is the root cause of slow bulk writes: 15 tiny tuples per row across thousands of shards

### DocStore V3 Oplog Design (Referenced: `docs/design/docstore-v3-oplog.md`)
- Snapshot + ops model: each shard has a snapshot (point-in-time) and an oplog (incremental changes)
- The data silo session's findings directly inform V3: the snapshot format should be one packed blob per document, not 15 separate tuples
- ShardStore already supports this pattern (snapshot + ops codecs)

### Where This Session Leaves Off

The session ended with Justin's observation that pre-creating files during the tags phase gave a 5.7x improvement (300K/s to 1.7M/s). Combined with packed sort layers (45s to 0.7s on images save), the estimated total pipeline time drops to approximately 7.3 minutes.

**Open questions for follow-up:**
1. Should docstore migrate to ShardStore's snapshot model (one packed blob per doc) for writes?
2. Is the per-thread staging approach worth the complexity vs. simpler pre-creation overlapping?
3. How does the pre-creation approach scale in memory-constrained environments?
4. What's the interaction between these changes and the `docstore-v3-oplog.md` design?

---

## 10. Commits on Josh's Branch

All work was cherry-picked onto `worktree-josh-dump-processor`:

```
526e7ff refactor: inline save+reload in process_dump, remove SaveHandle pipeline
09a389b feat: pack sort layer bitmaps into single shard file per field
e78d3be perf: eliminate per-field Vec clone in docstore tuple collection
035208a feat: batch docstore writes per row in dump processor
53d6b3f fix: rename index_ns to field_loop_ns and clarify it includes nested bitmap timers
c422246 feat: add per-component timing instrumentation to dump processor
```

Note: The per-row instrumentation was later stripped and replaced with phase-level-only timing. The instrumentation commits above may not reflect the final state.

---

## Summary of Key Takeaways

1. **DocStore writes are 85% of the bulk load processing time.** Everything else (parse, filter, bitmap, enrichment, computed) is noise by comparison.

2. **The bottleneck is filesystem metadata (file creation), not data throughput.** Pre-existing files write at 1.7M rows/s; creating files drops to 300K/s.

3. **Packed sort layers are a clear win.** 160 files down to 5, sort save from 45s to 0.7s. Implemented and tested.

4. **Per-row instrumentation at billion-row scale requires compile-time gating.** Runtime sampling, even with a modulo check, is measurable overhead at 5.4B rows.

5. **The current 512-doc shard layout is good for reads but hostile to bulk writes.** The serving format should stay, but the write path needs either (a) pre-creation of files during an earlier phase, or (b) a staging-then-convert approach.

6. **Pre-creating shard files during tags processing is the simplest viable fix** -- no architectural changes, 5.7x write speedup, estimated total pipeline ~7.3 minutes.
