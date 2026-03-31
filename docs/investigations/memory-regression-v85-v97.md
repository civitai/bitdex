---
name: Memory regression investigation handoff
description: Per-bitmap memory overhead regression between v1.0.85 and v1.0.115 — RSS doubled from 14GB to 32GB at 109M records
type: project
---

## Problem Statement

At 109M records, BitDex RSS after a fresh restart was **~14 GB on March 24** (v1.0.85) but is now **~32 GB** (v1.0.115). Disk bitmap sizes are ~9.4 GB total — unchanged. Something in the code between v1.0.85 and v1.0.97 introduced per-bitmap memory overhead that roughly doubled the in-memory footprint.

This causes OOM at the 32 GB pod limit when queries trigger lazy loading of large fields (postId: 22M values, tagIds: 27K values).

## Evidence

**Prometheus data (container_memory_working_set_bytes):**
- Mar 24 12:00 — 13.7 GB (v1.0.85, after restart, full 109M dataset)
- Mar 24 14:00 — 13.6 GB (stable baseline)
- Mar 27 17:00 — 31.8 GB (v1.0.97, same data, no restart since Mar 26)
- Mar 31 current — 32+ GB immediately on restart → OOM

**Disk bitmap sizes (unchanged):**
- Filter: 7.7 GB (tagIds 5.3G, postId 892M, postedToId 892M, userId 293M, rest ~270M)
- Sort: 1.6 GB (sortAt 385M, existedAt 371M, publishedAt 385M, id 322M, reactionCount 76M, collectedCount 39M, commentCount 1.4M)
- Total shardstore: 9.4 GB

**Implication**: The in-memory representation is using ~2-3x more memory per bitmap than it should. On disk: 9.4 GB. In memory at v1.0.85: ~12-13 GB (reasonable ~1.4x expansion for roaring). In memory now: ~30+ GB (~3.2x expansion — something is wrong).

## Versions to Compare

- **v1.0.85** (tag `v1.0.85`, commit `e5e3b7b`) — last known stable at ~14 GB. Mar 24 17:19.
- **v1.0.97** (tag `v1.0.97`, commit `e127fd6`) — first version with all sync-v2 code. Mar 26 08:42.
- **v1.0.115** (tag `v1.0.115`, commit `ec3f74f`) — current, includes all fixes from today's session.

## Suspect Code Changes (v1.0.85 → v1.0.97)

PRs most likely to affect per-bitmap memory:

1. **PR #78 — `perf: O(n log n) batch eviction in finish_restore and insert_restored_entry`** (commit `6eb7570`)
   - Changed the RESTORE code path — how bitmaps are loaded from ShardStore on boot
   - Could have changed memory layout, added intermediate data structures, or changed when bitmaps are materialized

2. **PR #82 — `feat: computed sort fields`** (commit `700f310`)
   - Added computed field infrastructure (GREATEST operator for sortAt)
   - May have added per-field metadata or caching overhead

3. **PR #83 — `feat: RSS-aware memory pressure eviction`** (commit `0631f8c`)
   - Added eviction infrastructure — tracking data structures per cache entry
   - May add overhead to every cached bitmap

4. **PR #85 + #86 — Sync V2 pipeline** (commits `316f6bc`, `abe45df`)
   - Massive PR adding WAL, ops processor, dump pipeline
   - May have changed FilterField or SortField data structures
   - Added FieldMeta, BitmapSink, WAL infrastructure

5. **PR #76 — width/height fields** (commit `b2310ed`)
   - Added new doc_only fields — shouldn't affect bitmap memory but worth checking

## What to Investigate

### 1. Per-bitmap overhead comparison

Build and run both versions locally with the same dataset. Compare:
```bash
# Use local CSVs at C:\Dev\Repos\open-source\bitdex-v2\data\load_stage\
# images.csv (15GB, 109M records), tags.csv (89GB), resources.csv, etc.

# Build v1.0.85
git checkout v1.0.85
cargo build --release --features server
# Run with --port 3001 --data-dir ./data-85

# Build v1.0.115 (or current main)
git checkout main
cargo build --release --features server
# Run with --port 3002 --data-dir ./data-115
```

After loading, compare:
- Total RSS (`/proc/PID/status` VmRSS)
- Bitmap memory report (re-enable via `/metrics`)
- Per-field memory: `sizeof` each FilterField HashMap, each SortField layer vec

### 2. FilterField struct size

Check if FilterField grew between versions. In v1.0.85 vs now:
- `src/filter.rs` — FilterField struct
- Look for new fields added (tracking metadata, dirty flags, versioned bitmap overhead)
- Check if VersionedBitmap wrapper grew (added diff layers, generation tracking, etc.)

### 3. ArcSwap snapshot overhead

The ArcSwap snapshot (InnerEngine) is cloned on every publish. Check:
- Does v1.0.115 keep more snapshots alive simultaneously?
- Did the clone get deeper (more Arc::make_mut triggers)?
- Are old snapshots being held longer by readers?

### 4. ShardStore restore path

Compare `finish_restore()` between versions:
- v1.0.85: how does it materialize bitmaps from disk?
- v1.0.115: same function — any new intermediate data structures?
- Does the new code keep both the shard data AND the deserialized bitmap?

### 5. Unified cache entry overhead

Cache entries at 512 MB max — but check:
- Did the per-entry size change? (pre-computed bitmaps, metadata)
- Are cache entries being created during restore?
- BoundStore at boot: loading 14 shards with 21K+ entries each

### 6. Doc cache overhead

1 GB max, 30 generations. Check if generation management is holding more data than expected.

## How to Reproduce

```bash
# Local dataset
DATA_DIR=C:\Dev\Repos\open-source\bitdex-v2\data\load_stage

# Config (old stable — use civitai-index-old-stable.json)
CONFIG=C:\Dev\Repos\open-source\bitdex-v2\deploy\configs\civitai-index-old-stable.json

# Run server, load data, measure RSS
cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./data
```

Use `images-10m.csv` (1.4 GB, ~10M records) for faster iteration if full dataset is too slow. Scale findings by 10x.

## Key Files

- `src/filter.rs` — FilterField, VersionedBitmap
- `src/concurrent_engine.rs` — InnerEngine, finish_restore, flush thread
- `src/shard_store_bitmap.rs` — ShardStore restore
- `src/unified_cache.rs` — UnifiedCache
- `src/bound_store.rs` — BoundStore
- `src/doc_cache.rs` — DocCache

## Current Production State

- v1.0.115 deployed, shadow mode OFF, writes flowing (3-4 GB RSS stable)
- existedAt + publishedAt sort fields removed from ConfigMap (saves ~1.5 GB)
- Compaction auto-load disabled in code
- Lucy's correctness fix for writes on unloaded fields deployed
- Cannot enable shadow mode (queries) until memory regression is resolved

## Production Configs (saved locally)

All three config files are saved in `deploy/configs/`:

1. **`civitai-index-old-stable.json`** — the index config from before sync-v2 changes (git commit `45e22ef`)
2. **`prod-sync.toml`** — the sidecar wrapper config (poll_interval_ms, data_dir, etc.)
3. **`prod-sync-config-civitai.yaml`** — the V2 sync config (dump phases, triggers, ClickHouse metrics)

The current production index config (ConfigMap `bitdex-index-config`) matches `civitai-index-old-stable.json` except:
- existedAt + publishedAt sort fields removed (today's change)
- sortAt has `computed: { op: greatest, source_fields: [existedAt, publishedAt] }`
- filter_only on modelVersionIdsManual, toolIds, techniqueIds
- existedAt + index fields in data_schema

## Success Criteria

Identify the specific code change(s) causing the ~2x memory overhead and propose a fix that brings RSS back to ~14-16 GB at 109M records under query traffic.
