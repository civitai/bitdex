# DocSilo Architecture Reference

## What it is

Single mmap'd key-value store for document storage. Replaces the 62-file hex-sharded DocStoreV3. One `data.bin` + one `index.bin` + an ops log. Near-zero heap.

## On-disk files

All live under `<data_dir>/silo/`:

| File | Purpose | Size at 107M |
|------|---------|-------------|
| `data.bin` | Packed document bytes. Each entry = `[alive:u8][num_fields:u16][field_pairs...]`. Entries have buffer headroom (4x) for in-place updates. | ~25 GB |
| `index.bin` | HashIndex — open-addressing hash table mapping `slot_id → (offset, length, allocated)` in data.bin. Built once after bulk load, updated in-place on writes. | ~670 MB |
| `ops_a.log` / `ops_b.log` | Append-only ops log with CRC32 per entry. A/B swap prevents ops loss during compaction. Replayed on startup. | Variable |
| `layouts.bin` | Temporary sidecar written at dump finalize. Contains layout entries for deferred HashIndex build. Consumed on first `DocSilo::open`, then deleted. | ~2.5 GB (temporary) |
| `field_dict.json` | Field name → u16 index mapping. Loaded on open, saved on dict changes. | <1 KB |

## Write paths

### Bulk dump (phases 1-6)

**Phase 1 (images):**
1. `DocSiloBulkWriter` pre-allocates `data.bin` at `estimated_rows * bytes_per_doc * buffer_ratio`
2. Rayon workers write doc bytes directly to mmap via per-thread cursor leases (lock-free)
3. Each write records a `(key, offset, length)` layout entry
4. At phase end: `finalize()` writes `layouts.bin`, truncates `data.bin` to used size
5. Server restart: `DocSilo::open` reads `layouts.bin`, builds `index.bin` via `HashIndex::build_bulk`

**Phases 2+ (tags, resources, tools, techniques):**
1. `DumpMergeWriter` opens the existing `data.bin` (writable mmap) + `index.bin`
2. For each slot: reads existing doc at `index[slot].offset`, calls merge function to combine existing fields + new fields, writes merged result back **in-place** to same offset
3. `buffer_ratio` (4x) provides headroom — merged doc fits without relocation
4. If merged doc exceeds allocated space → overflow counter increments, logged as error
5. `index.bin` entry's `length` updated in-place (offset and allocated unchanged)
6. No new layout entries. No growing Vec. Zero additional memory.

### Steady-state (sync-v2 ops)

1. `DocWriter` receives Set/Append/Remove ops from WAL reader
2. Ops encoded as `DocOp` and appended to the ops log (`ops_a.log`)
3. Ops log is append-only with CRC32 per entry
4. No data.bin modification until compaction

## Read path

1. Check ops log for pending ops on this key
2. If pending ops exist: read base snapshot from `data.bin` via `index.bin` lookup, apply ops, return merged result
3. If no pending ops: read directly from `data.bin` via `index.bin` lookup (one hash probe + one mmap slice)
4. `doc_cache` (DashMap) sits above this — cache hit skips both steps

## Compaction

Merges pending ops from the ops log into `data.bin`:
- **In-place (common):** merged doc fits in allocated buffer → overwrite at same offset, update index length
- **Relocating (rare):** merged doc exceeds buffer → append to end of data.bin, update index to new offset
- A/B log swap: freeze current log, new ops go to other log, compact from frozen log, delete frozen log
- Merge function handles multi-value semantics (Mi array union, not last-write-wins)

## Key invariants

- **Slot = PG ID.** No mapping layer. `key = slot_id + SLOT_KEY_OFFSET`.
- **One entry per slot in data.bin.** Phases 2+ update in-place, never create second entries.
- **Ops are always applied on read.** Reads never return stale snapshots.
- **buffer_ratio provides growth room.** Set to 4.0 for dump (docs grow ~3-5x across phases). Set to default for steady-state.
- **field_dict is global.** All phases share the same field name → u16 mapping. Persisted to `field_dict.json`.
- **layouts.bin is temporary.** Only exists between `finalize()` and the next `DocSilo::open`. Consumed and deleted.
