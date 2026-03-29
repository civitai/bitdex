# Session Review: Liz — Dump Processor Performance Optimization

**Session:** 340479f6-0432-4584-9fe6-9e0b17f04bf7
**Agent:** Liz (formerly Alexandra)
**Project:** josh-dump-processor
**Date:** 2026-03-27 12:58 PM — 2026-03-28 11:09 PM (~10 hours)
**Reviewer:** Conversation Reviewer (spawned by Dakota)

## Key Decisions

### 1. filter_only: true on tagIds — THE root cause fix

The entire 15min-to-10min regression was caused by a missing `filter_only: true` on tagIds in config.json. Josh's notes documented it: *"tagIds needs `filter_only: true` in config.json. Without this, tags docstore writes are 3x slower."* But it was never applied to the config being used for benchmarks. With `filter_only`, tags went from 5M/s to 53M/s (peak), and the full load dropped from 16+ minutes to **10m05s**.

**Rationale:** tagIds (and toolIds, techniqueIds, modelVersionIdsManual) are multi-value fields used only for filtering, never served in document responses. Writing 5.4 billion rows to the docstore for tagIds is pure waste.

**Impact:** Single config change accounted for the majority of the regression. Applied to both `data/indexes/civitai/config.json` and `deploy/configs/civitai-index.json`.

### 2. Inline save+reload in process_dump, remove SaveHandle pipeline

Josh recommended simplifying the dump pipeline: `process_dump` becomes the single entry point doing parse + save + reload. The `SaveHandle`, `pending_save_handle`, and monitor thread were removed. Server handler becomes trivial: `spawn_blocking(process_dump) -> set_complete`.

**Rationale:** Having save as a separate step added complexity with no benefit. The data showed save is fast enough to be inline.

### 3. Per-row instrumentation must be opt-in or absent

Per-row timing (even with sampling) is too expensive for the multi-value hot path. Liz initially added `Instant::now()` calls per row, then sampling via `BITDEX_DUMP_SAMPLE_RATE`, but both approaches caused severe regressions:

- Full per-row timing: tags went from 37M/s to unusable
- Sampling at 1000: still caused a **4x slowdown** (22M/s to 5.5M/s) due to a bug where `Instant::now()` was called in BOTH branches of an `if sample { ... } else { ... }`
- Even correct sampling: 5.4B rows / 1000 = 5.4M `Instant::now()` calls, plus the branch prediction cost on every row

**Final decision:** All per-row timing was stripped. Phase-level timing (emit_stage), save breakdown (filter/sort/alive), and per-table enrichment timing are kept — they run once per phase with zero per-row overhead.

### 4. Instrumentation sampling via BITDEX_DUMP_SAMPLE_RATE env var

Before being ripped out, the sampling design was: `BITDEX_DUMP_SAMPLE_RATE=0` (default, no overhead), `=1000` means every 1000th row gets timed. Justin directed this to be opt-in: "make the sample rate be a thousand or whatever... for option one. So it allows us to kind of take two at once."

**Outcome:** Sampling was implemented but ultimately removed because even the branching overhead was too costly at 5.4B rows. The feature flag approach (`#[cfg(feature = "dump-timing")]`) was considered but also abandoned in favor of simply not having per-row timing.

## Performance Findings

### Component Breakdown — Images 10M, 28-32 Threads

The one successful component timing run (images 10M with pre-created files) showed:

| Component | Thread-seconds | % of Row Time |
|-----------|---------------|---------------|
| **docstore** | **511.0s** | **84.8%** |
| computed | 44.9s | 7.5% |
| bitmap | 22.8s | 3.8% |
| enrich | 14.8s | 2.5% |
| parse | 8.2s | 1.4% |
| filter | 0.3s | 0.1% |

> "Docstore is 85% of row processing time. Everything else is noise."

### Docstore Write Sub-Timing

Within docstore, serialization is only 4% of the cost:
- **Serialize: 21.9s** (collecting fields into tuple format)
- **Write: 486.2s** (BufWriter `write_all` to disk)

> "96% of docstore time is the write — 486 thread-seconds on `write_all` to BufWriter."

### Docstore Optimization Microbench Results

| Approach | Rate | vs Baseline |
|----------|------|-------------|
| DashMap + lazy + 8KB (current) | 177K/s | baseline |
| DashMap + lazy + 64KB | 197K/s | +11% |
| Vec + pre-create + 64KB | 1,775K/s write phase | **10x write phase** |
| Vec + pre-create + write (total) | 56.4s total | Same as baseline (pre-create cost) |
| Thread-local accumulate + flush | 433s | **-2.4x** (flush kills it) |

**Key insight:** File creation is the bottleneck, not the write pattern. Once files exist, DashMap and Vec perform identically (~7.3s for 10M rows).

### Shard Size Sweep

| Docs/Shard | Shards at 10M | Rate | vs 512 |
|------------|--------------|------|--------|
| 512 (current) | 5,860 | 1.16M/s | baseline |
| 2,048 | 1,466 | 1.49M/s | +28% |
| 4,096 | 733 | 1.52M/s | **+31%** |
| 8,192+ | <400 | ~1.5M/s | Flat |

Sweet spot: 4K docs/shard. Returns flatten after that because per-shard write time increases.

### Sort Layer Packing

Packing 32 sort layer bitmaps into a single shard file: **sort save went from ~45s to 0.7s for images**. Confirmed working.

### Final Full Load: 107.6M records in 10m05s

| Phase | Rows | Time | Rate |
|-------|------|------|------|
| Tags | 4.48B | 2m46s | 27.0M/s |
| Images | 107.6M | 5m16s | 340K/s |
| Resources | 41.5M | 15.1s | 2.8M/s |
| Tools | 4.1M | 3.0s | 1.4M/s |
| Techniques | 6.4M | 3.0s | 2.1M/s |
| Metrics | 91.1M | 1m42s | 890K/s |

## Gotchas Discovered

### 1. Windows Instant::now() cost at billion-row scale

> "107M x 8 calls x 20ns = ~17 seconds of thread-time across all threads. For tags at 5.4B rows with 2 timing calls: 5.4B x 2 x 20ns = ~216 seconds thread-time, / 28 ~ 7.7s wall time. That's actually significant."

Even `Instant::now()` is measurable at 5.4B rows. Any per-row work in the tags hot path must be essentially free (a comparison, an increment, nothing more).

### 2. Sampling bug: Instant::now() in both branches

The initial sampling implementation had `let t_p = if sample { Instant::now() } else { Instant::now() }` — calling `Instant::now()` on EVERY row regardless of sampling. This caused a 4x slowdown that was initially attributed to the sampling itself.

### 3. Windows zombie processes from mmap'd files

Launching bitdex-server as a background process from bash on Windows creates unkillable zombie processes. The process holds file handles to mmap'd files (63GB tags.csv) and Windows refuses to release them. `taskkill /F` fails. Only a system restart or Process Explorer can kill them.

> "Windows zombie process holding 67GB of memory. Can't kill it from bash."

This consumed multiple hours of the session. Charlie's fix (`process::exit(0)` + `shutting_down` AtomicBool) helps for new processes but cannot kill existing zombies. The additional fix added during this session: pass `shutdown` flag into the dump processor's rayon loop, checked every 1M rows, so `spawn_blocking` tasks abort on Ctrl+C.

### 4. PowerShell Start-Process does not reliably inherit env vars

`$env:RAYON_NUM_THREADS = "28"` set in PowerShell does not reliably propagate through `Start-Process` or `cmd /c` wrappers. This caused several runs to use 1-2 threads instead of 28, producing misleadingly slow results (~5M/s instead of 35M/s for tags).

**Fix:** Run the server directly via `&` in the same PowerShell session, or pass env vars explicitly.

### 5. PowerShell Tee-Object drops stderr lines

When using `Tee-Object` to capture server output, it converts to UTF-16 and can drop `eprintln!` lines between stdout lines. The component breakdown timing was missing from logs due to this. Fix: redirect stderr separately to its own file.

### 6. Stale binaries from zombie file locks

When a zombie bitdex-server holds the binary, `cargo build` silently fails to overwrite it (or reports success with exit code 0 despite "Access denied"). Workaround: rename the old binary (`mv bitdex-server.exe bitdex-server-old.exe`) so cargo writes to a fresh path.

### 7. Wrong baseline comparison

Josh's `2c6191d` (16m03s) was the real baseline, but work was built on `3699a42` which Josh explicitly said had regressed to 18m16s. This wasted time investigating a phantom regression.

> "Our baseline is wrong. Josh's 16m03s was on commit 2c6191d. But we built on top of 3699a42 which Josh explicitly said regressed to 18m16s."

### 8. Windows Defender real-time scanning

Justin discovered Windows Defender was scanning every shard file creation, adding massive latency to the pre-creator and docstore writes. Adding an exclusion for the data directory improved throughput.

### 9. 64KB BufWriter is SLOWER than 8KB default

Counterintuitively, larger BufWriter (64KB) performed worse than the 8KB default when many shards compete for the same NVMe. More buffered data means larger individual flushes that compete for I/O bandwidth.

## Design Changes

### ShardPreCreator — background file pre-creation

A `ShardPreCreator` was implemented that watches an `AtomicU64` watermark (updated by rayon parse threads) and progressively creates docstore shard files + filter bitmap directories on a background thread. The idea: tags phase runs first (~200s), pre-creator creates files during that time, images phase benefits from pre-existing files.

**Result:** Pre-creator worked (created 200K files), but docstore write time was unchanged. The DashMap write path was the bottleneck under contention, not file creation. However, microbench showed that once files exist, writes are 10x faster — the issue is that 242K files take ~60s to create (at ~4K files/s), and the pre-creator may not finish before images starts.

### Docstore batching (append_tuples_raw)

Changed from 15 separate `append_tuple_raw` calls per row to one `append_tuples_raw` call that holds the lock once. Result: only 5% improvement — the lock acquisition was not the bottleneck, the `write_all` calls inside were.

Then changed to pre-encode the entire row as one contiguous byte blob with a single `write_all`. Still minimal improvement because BufWriter flushes to disk are the real cost.

### Validation script: sequential dump submission confirmed necessary

Attempted to submit all dumps in parallel, but the server only allows one dump task at a time ("Another task is already running"). Sequential submission is required.

## Undocumented Knowledge

### IDs in images.csv are not sequential

> "Not sequential — 12.7% of rows jump backwards. IDs are roughly increasing but with significant out-of-order patches."

This means shard-boundary-based batching strategies (flush when shard changes) won't work cleanly within a thread's chunk.

### Tags CSV is 5.4 billion rows, not 107M

The tags CSV has 5.4 billion rows (one per tag-image pair), not one per image. This is why any per-row overhead in the multi-value path is so devastating — 50x more rows than images.

### DashMap vs Vec for shard writers

At 28+ threads with pre-created files, DashMap and Vec<Mutex<BufWriter>> perform identically. The DashMap hash + shard lock is not the bottleneck. No need to replace DashMap — it's fine.

### Docstore write is I/O bound, not CPU bound

CPU was at ~25% during load. The bottleneck is disk I/O from millions of small writes across thousands of shard files. File creation metadata operations are the slowest part.

### Server data-dir path resolution

`--data-dir ./data` resolves relative to CWD, not to the binary location. When running from a worktree, the server may write to the wrong data directory. Always use absolute paths.

### Enrichment timing

Posts.csv enrichment loading: ~5-14s (varies by run). Model versions: <1s. This is included in the images phase wall time but is a one-time per-phase cost.

## Recommended Memory Entries

1. **`filter_only: true` required on tagIds, toolIds, techniqueIds, modelVersionIdsManual** — without this, tags phase writes 5.4B rows to docstore at 3-5M/s instead of 50M/s. Config must be in both `data/indexes/civitai/config.json` and `deploy/configs/civitai-index.json`.

2. **Per-row instrumentation is forbidden in the multi-value (tags) hot path** — even `Instant::now()` adds measurable overhead at 5.4B rows. Phase-level timing only.

3. **Dump processor full load: 10m05s at 107.6M** — baseline with filter_only fix, sort layer packing, pre-creator, and stripped instrumentation. Tags 2m46s, Images 5m16s, everything else <2m.

4. **Docstore write is 85% of images row processing time** — 486 thread-seconds of I/O for 10M rows. Serialization is only 4%. Next optimization should target reducing file I/O (fewer shards, batch snapshots, or in-memory accumulation with periodic flush).

5. **Windows zombie processes from mmap'd files** — must kill server cleanly before rebuilding. Shutdown flag now passed into rayon loops. If zombie persists, rename old binary and rebuild.

6. **PowerShell RAYON_NUM_THREADS** — must be set in the SAME session that runs the server binary via `&`. `Start-Process` and `cmd /c` may not propagate env vars.

7. **Windows Defender exclusion needed for data directory** — real-time scanning adds significant latency to shard file creation/writes.

8. **BufWriter 64KB is worse than 8KB default** — for high-shard-count workloads on NVMe, larger buffers cause larger competing flushes.
