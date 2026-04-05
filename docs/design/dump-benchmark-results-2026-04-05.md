# Dump Pipeline Benchmark Results — 2026-04-05

## Summary

**Dataset:** images-small.csv (14,652,234 rows)
**Branch:** design/zero-downtime-deploy
**RAYON_NUM_THREADS:** 24
**Machine:** Windows 11, 16-core/32-HT

### Final Numbers: 474K → 1,428K rows/sec (+201%)

| Metric | Baseline (pre-opt) | Previous Best | Direct Silo Write |
|---|---|---|---|
| Parse+merge rows/sec | 474,000 | 1,048,000 | **1,427,723** |
| Parse+merge time | 30.9s | 14.0s | **10.3s** |
| Apply/write phase | 5.0s (staging) | 5.0s (fused staging) | **1.0s (direct silo)** |
| save_snapshot (post-dump) | ~10.5s | ~10.5s | **0s (eliminated)** |
| Total process_dump | ~46s | ~19.9s | **11.2s** |

### Per-Stage Breakdown (direct silo write build)

| Stage | Time | Notes |
|---|---|---|
| Enrichment build | 1.25s | mmap Dense Vec (posts.csv, 23M rows) |
| Parallel parse | 5.6s | rayon 24 threads, mmap'd CSV |
| Merge | 1.4s | per-field parallel merge |
| Doc write | 1.7s | parallel mmap ops writer |
| write_to_silo | 1.0s | frozen serialize + write_batch_parallel |
| Doc compact | 15.7s | sequential (not part of process_dump) |

### Optimization History (this session + previous)

| # | Optimization | Before | After | Commit |
|---|---|---|---|---|
| 1 | Mmap enrichment (Dense Vec offset index) | 474K | 750K | 55bb01f |
| 2 | Batch bitmap inserts (Approach B) | 750K | 821K | 55bb01f |
| 3 | Compiled DocFieldPlan (zero HashMap lookups) | 821K | 886K | 55bb01f |
| 4 | Duplicate config sort elimination | — | — | 55bb01f |
| 5 | DumpFieldValue zero-copy strings + shared wire format | 886K | ~900K | 55bb01f |
| 6 | Per-field parallel merge | 900K | 987K | 55bb01f |
| 7 | into_iter clone elimination (apply phase) | 987K | 1,048K | b6e7de9 |
| 8 | Fused write (apply inside merge function) | 1,048K | 987K | b5d7263 |
| 9 | **Direct BitmapSilo write (bypass V2 staging)** | 987K | **1,428K** | pending |

### What Changed (Direct Silo Write)

**Before:** dump merge → `clone_staging()` (deep clone InnerEngine) → `apply_bitmap_maps()` (OR into staging) → `publish_staging()` (swap RwLock) → `save_snapshot()` (re-read from RwLock, serialize frozen, write to silo)

**After:** dump merge → `BitmapSilo::write_dump_maps()` (serialize frozen + write_batch_parallel directly) → update slot counter/alive via RwLock (tiny op)

Eliminated:
- `clone_staging()` deep clone (~2s at 14.6M)
- `publish_staging()` RwLock swap
- `save_snapshot()` re-serialization (~10.5s at 14.6M)
- Double-write: bitmaps no longer go to in-memory staging AND disk

### Thread Count Sweep (from previous session)

| Threads | Rows/s |
|---|---|
| 4 | 435K |
| 8 | 791K |
| 12 | 865K |
| 16 | 979K |
| 24 | **1,068K** (sweet spot) |
| 32 | 992K |

### Alive Count Note

7,326,270 alive out of 14,652,234 total rows. The other ~7.3M rows have
`publishedAt = null` and are deferred (not immediately alive). This is correct
behavior per the `deferred_alive` config.
