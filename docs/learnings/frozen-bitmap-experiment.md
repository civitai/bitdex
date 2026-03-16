# Learning: Frozen Bitmap Format Experiment

**Date:** March 16, 2026
**Status:** Parked — not worth the complexity at current performance levels
**Fork:** https://github.com/civitai/roaring-rs (branch: `frozen-mmap-support`)

## What we tried

Implemented CRoaring's "frozen" bitmap format in a roaring-rs fork:
- `FrozenRoaringBitmap<'a>` — zero-copy view from mmap'd buffer
- `serialize_frozen_into()` — writes 32-byte aligned frozen format
- Integrated into BitDex's BitmapFs with `.frozenpack` file format
- Auto-detection: loads .frozenpack if available, falls back to .fpack

## Benchmark results (105M records, real Civitai data)

| Field | Standard fpack | Frozen + to_owned | Speedup |
|-------|---------------|-------------------|---------|
| userId (749K values, 288MB) | 829ms | 759ms | 1.09x |
| nsfwLevel (7 values, 72MB) | 69ms | 73ms | 0.94x |
| isPublished (2 values, 27MB) | 17ms | 20ms | 0.85x |

**9% improvement at best, break-even or worse for low-cardinality fields.**

## Why it didn't help

The frozen format eliminates deserialization overhead by storing bitmaps in a layout that can be read directly from mmap. But BitDex needs **owned** `RoaringBitmap` values (stored in `Arc<RoaringBitmap>` for ArcSwap CoW snapshots). So we still call `to_owned()` which copies all container data into heap memory — essentially the same work as standard deserialization.

The `to_owned()` copy dominates the loading cost. Whether you copy from a stream-parsed buffer (standard) or from an mmap'd aligned buffer (frozen), you're doing the same memcpy into `Vec<u16>` / `[u64; 1024]` containers.

## When frozen WOULD help

The frozen format's value is **zero-copy reads** — keeping bitmaps borrowed from mmap without copying to owned. This requires:

1. **Read-only bitmap access** — no mutations, no `Arc::make_mut`
2. **Lifetime management** — the mmap handle must outlive all borrowed bitmaps
3. **Engine architecture change** — either a separate `FrozenRoaringBitmap<'a>` type throughout the engine (lifetime infection on every struct), or separate reader processes that only read from mmap

### Potential architectures where frozen shines:
- **Read replicas**: stateless query servers that mmap bitmap files, never mutate. Writer process handles mutations and file updates.
- **Separation of hot/cold bitmaps**: hot (frequently mutated) bitmaps stay owned, cold (rarely touched) bitmaps stay frozen from mmap.

Neither is needed at current scale (105M records, 12μs cache hits, 40ms cold miss, single process handles 80K+ queries/sec).

## What we learned

1. **The bottleneck isn't deserialization format — it's the copy.** Any format that requires `to_owned()` at the end gains almost nothing. The parsing overhead (stream vs aligned buffer) is negligible compared to the memcpy.

2. **Parallel I/O beats format optimization.** Going from sequential to parallel fpack reads (rayon par_iter) gave 3x speedup. The frozen format gave 9%. Parallelism wins.

3. **Cache persistence eliminates the loading question entirely.** With bound cache shards restored in 337μs on startup, the first query hits cache at 12μs. Bitmap loading only matters for fields not yet in cache, which is a one-time cost.

4. **Don't optimize loading when you can avoid loading.** Eager load config + cache persistence + lazy per-value loading means most bitmaps never load at all (multi_value fields), and the ones that do load are already optimized with parallel I/O.

## Artifacts preserved

- **roaring-rs fork**: https://github.com/civitai/roaring-rs
  - Branch `frozen-mmap-support` with 3 commits
  - `FrozenRoaringBitmap<'a>` type, frozen serialize/deserialize, 12 tests
- **CRoaring reference**: `C:\Dev\Repos\open-source\CRoaring` (cloned for study)
- **Design docs**: `roaring-rs/docs/` in the fork
- **Benchmark**: `scratch/src/bin/bench_frozen.rs` in BitDex repo
