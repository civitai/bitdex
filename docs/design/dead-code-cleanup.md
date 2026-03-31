# Dead Code Cleanup

**Created:** 2026-03-31 (data fix sprint)
**Context:** ~3 hours spent debugging/fixing code paths that production never executes. PRs #107 and #108 modified dead code. This doc inventories what to clean up.

## Production Dump Flow (the ACTUAL path)

```
bitdex-sync (pg_sync.rs)
  → run_streaming_pipeline()
    → download_phase_csvs() [PG COPY to disk]
    → PUT /api/indexes/{name}/dumps [register with server]
    → POST /dumps/{name}/loaded [signal CSV ready]
    → server: dump_processor.rs processes CSV directly
      → parallel rayon parse → filter/sort bitmaps + docstore writes
      → save_phase_to_disk() → ShardStore
```

**NOT** csv_ops → WAL → ops_processor. That path exists in code but is never called.

## Dead Code Inventory

### 1. `src/pg_sync/csv_ops.rs` (entire module)

**Status:** DEAD. Zero callers outside the module.

Functions defined but never called:
- `images_csv_to_wal()` — converts image CSV rows to WAL ops
- `tags_csv_to_wal()` — converts tag CSV to WAL ops
- `tools_csv_to_wal()` — converts tools CSV to WAL ops
- `techniques_csv_to_wal()` — converts techniques CSV to WAL ops
- `multi_value_csv_to_wal()` — generic multi-value CSV→WAL
- `image_row_to_ops()` / `image_row_to_ops_pub()` — row→ops conversion

**Why it exists:** Written as an alternative dump path (CSV→ops→WAL→ops_processor) but never wired into the streaming pipeline. The streaming pipeline uses dump_processor.rs directly instead.

**Action:** Delete entire module. Remove `pub mod csv_ops;` from `src/pg_sync/mod.rs`.

### 2. `src/ops_processor.rs::process_wal_dump()` (line ~1259)

**Status:** DEAD. Zero callers outside ops_processor.rs.

Reads a WAL file and applies ops via `apply_ops_batch_dump()` to a BitmapAccum. Was the intended consumer of csv_ops output, but the dump pipeline uses dump_processor.rs instead.

**Action:** Delete function. If needed for future WAL replay, re-implement when actually needed.

### 3. `src/ops_processor.rs::apply_ops_batch_dump()` (line ~1241)

**Status:** DEAD. Only caller is `process_wal_dump()` (also dead).

Variant of `apply_ops_batch()` that writes to a BitmapAccum instead of a BitmapSink.

**Action:** Delete function.

## Why This Matters

During the 2026-03-31 data fix sprint:

- **3+ hours** spent auditing the "metrics pipeline end-to-end" including csv_ops.rs code paths that never execute
- **PRs #107/#108** modified ops_processor code (the WAL/ops path) to fix issues that only manifest through a code path production doesn't use
- Team members (Scarlet, Josh) traced through csv_ops.rs believing it was the production dump path, leading to incorrect diagnosis of sortAt computation

Dead code that looks like production code is actively dangerous — it wastes investigation time and leads to fixes in the wrong place.

## Cleanup Plan

| Item | Action | Risk |
|------|--------|------|
| `src/pg_sync/csv_ops.rs` | Delete module + mod.rs entry | None — zero callers |
| `process_wal_dump()` | Delete | None — zero callers |
| `apply_ops_batch_dump()` | Delete | None — only caller is process_wal_dump |
| `src/pg_sync/copy_queries.rs` | Audit — may have functions only used by csv_ops | Check `parse_image_row`, `parse_tag_row` callers |
| `image_row_to_ops_pub` | Delete with csv_ops.rs | None |

## Related: Code That IS Production

For reference, the code paths that ARE live in production:

- **Dump path:** `dump_processor.rs` (CSV → bitmaps + docstore)
- **Steady-state ops:** `ops_processor.rs::apply_ops_batch()` (WAL reader → bitmap mutations)
- **Metrics poller:** `metrics_poller.rs` (ClickHouse → Op::Set → /ops endpoint)
- **Ops endpoint:** `server.rs::handle_ops()` (POST /ops → WAL append)
