# DocStore V2 → ShardStore V3 Migration Plan

> Migrate document storage from the legacy V2 tuple log (BulkWriter) to the
> ShardStore generation-based ops log with typed DocOp operations.
>
> **Status**: NOT STARTED
> **Prerequisite**: ShardStore framework (DONE), Doc codec (DONE, `shard_store_doc.rs`)
> **Reference**: `docs/design/docstore-v3-oplog.md`

---

## Why

The V2 docstore uses append-only tuple logs with LIFO dedup on read. It has
no concept of typed operations — every write is a raw bytes append. This means:

- **No multi-value append**: Writing individual tag values requires an Mi-tagged
  workaround with merge-on-read (current hack in `docstore.rs` lines 1044-1055).
  The V3 `DocOp::Append` solves this cleanly.
- **No schema awareness**: V2 doesn't know field types. V3 validates ops against
  the field handler registry before writing.
- **No generation snapshots**: V2 can't do point-in-time snapshots for documents
  (bitmaps already can via ShardStore).
- **Duplicate infrastructure**: ShardStore already provides the same I/O patterns
  (per-shard files, hex-bucketed directories, append-only ops, compaction) but
  with generation management and typed codecs.

## Current State

| Layer | Bitmaps | Documents |
|-------|---------|-----------|
| ShardStore V3 | **DONE** — alive, filter, sort, meta | Codec exists, **NOT WIRED** |
| Legacy | BitmapFs (being phased out) | **DocStore V2 (active)** |

The ShardStore doc codec (`src/shard_store_doc.rs`) provides:
- `DocOp` enum: Set, Append, Remove, Delete, Create
- `DocSnapshotCodec`: Encode/decode materialized documents
- `DocOpCodec`: Encode/decode typed operations with LIFO apply
- `SlotHexShard`: Same hex-bucketed path layout as V2

## V2 Usage Audit

### Writers (4 components)

| Component | File | Call Sites | Pattern | Difficulty |
|-----------|------|------------|---------|------------|
| Dump processor | `src/dump_processor.rs` | 6x `append_tuple_raw()` | Per-row inline writes from 28 rayon threads | Medium |
| PG sync loader | `src/pg_sync/single_pass.rs` | 9x `append_tuple_raw()` | Per-row writes during CSV processing | Medium |
| BulkWriter core | `src/docstore.rs:1341-1588` | `write_batch_encoded()`, `write_batch_fresh()`, `append_tuple_raw()`, `flush_v2_writers()` | Append-only shard files with per-shard Mutex | Medium-High |
| Concurrent upsert | `src/concurrent_engine.rs:2820-2901` | `put_inner()`, `patch()` via `doc_tx` channel | Batched writes through flush thread | Hard |

### Readers (3 components)

| Component | File | Lines | Difficulty |
|-----------|------|-------|------------|
| V2 reader core | `src/docstore.rs` | `get_v2()` (1089), `get_v2_from_data()` (1004-1068) | Hard — LIFO dedup + staleness tracking |
| Engine wrapper | `src/concurrent_engine.rs` | `get_document()` (5441) | None — abstract via engine |
| Server endpoints | `src/server.rs` | GET /documents (2386-2448) | None — calls engine |

### Infrastructure to Remove

| Component | File | Lines | Notes |
|-----------|------|-------|-------|
| V2 magic/header | `src/docstore.rs` | 44-48 | `BDX2` magic, 16-byte header |
| `write_v2_tuple()` | `src/docstore.rs` | 953-967 | Tuple format encoder |
| `parse_v2_tuples()` | `src/docstore.rs` | 969-994 | Tuple parser |
| `is_v2_shard()` | `src/docstore.rs` | 929-932 | Format detection |
| `flush_v2_writers()` | `src/docstore.rs` | 1582-1587 | Per-shard BufWriter flush |
| V2 compaction | `src/docstore.rs` | 1070-1086 | Staleness-based enqueue |
| `shard_id()`/`shard_path()` | `src/docstore.rs` | 186-197 | Already in `SlotHexShard` |

### What's NOT V2 (no changes needed)

- **DocCache** (`src/doc_cache.rs`) — generic LRU, works with any backend
- **FieldDictionary** (`src/dictionary.rs`) — orthogonal to storage
- **ShardStore doc codec** (`src/shard_store_doc.rs`) — ready to use
- **Server document formatting** (`src/server.rs:609`) — works with `StoredDoc`

---

## Migration Phases

### Phase 1: Dump Processor Write Path (LOW RISK)

**Effort**: ~150 lines, 2-3 days

Replace `BulkWriter.append_tuple_raw()` with `ShardStore::append_op()`:

```rust
// Before (V2):
let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
bulk_writer.append_tuple_raw(slot, fidx, &packed);

// After (V3):
doc_store.append_op(&SlotHexShard::key(slot), &DocOp::Set {
    slot, field: fidx, value: PackedValue::I(v)
})?;

// Multi-value (V3 — no memory accumulation):
doc_store.append_op(&SlotHexShard::key(slot), &DocOp::Append {
    slot, field: fidx, value: PackedValue::I(tag_id)
})?;
```

Changes:
1. Create `ShardStore<DocSnapshotCodec, DocOpCodec, SlotHexShard>` in dump processor
2. Replace 6 `append_tuple_raw` calls with `append_op` (Set for scalars, Append for multi-value)
3. Remove the V2 multi-value merge hack from `docstore.rs` (no longer needed)
4. Verify concurrent write safety (ShardStore may need per-shard locking wrapper)

### Phase 2: PG Sync Loader Write Path (MEDIUM RISK)

**Effort**: ~150 lines, 2-3 days

Same pattern as Phase 1 for `src/pg_sync/single_pass.rs`. Replace 9 call sites.
This file is being superseded by `dump_processor.rs` (Sync V2), so this phase
may be skipped if single_pass.rs is removed first (Phase 3.7h in sync-v2 plan).

### Phase 3: Read Path Migration (HIGH RISK)

**Effort**: ~200 lines, 3-5 days

Replace `DocStore::get_v2()` with ShardStore reads:

```rust
// Coexistence approach (recommended):
fn get(&self, slot_id: u32) -> Result<Option<StoredDoc>> {
    // Try ShardStore first (new data)
    if let Ok(Some(doc)) = self.read_from_shardstore(slot_id) {
        return Ok(Some(doc));
    }
    // Fall back to V2 (old data)
    self.get_v2(slot_id)
}
```

Key concerns:
- Preserve LIFO semantics for ops replay
- Preserve staleness tracking for compaction
- DocCache integration (cache results from either backend)
- Test coverage for both read paths

### Phase 4: Concurrent Upsert Integration (HIGH RISK)

**Effort**: ~300 lines, 5-7 days

Migrate `put_inner()` and `patch()` in `concurrent_engine.rs`:
- Currently sends `StoredDoc` through `doc_tx` crossbeam channel
- Flush thread batches writes via `write_batch_encoded()`
- Need to decompose into `DocOp` sequences and route through ShardStore
- Mutation coalescer (`write_coalescer.rs`) needs to understand DocOps

This is the hardest phase — it touches the hot write path with concurrent
readers, flush thread coordination, and cache invalidation.

### Phase 5: Remove V2 Code (LOW RISK)

**Effort**: ~200 lines removed, 1-2 days

After all read/write paths are on ShardStore:
1. Delete `BulkWriter` struct and all V2 methods
2. Delete V2 header/tuple format code
3. Delete V2 shard detection (`is_v2_shard`)
4. Delete V2-specific compaction logic
5. Remove `shard_id()`/`shard_path()` (use `SlotHexShard` everywhere)
6. Run V2 data migration tool (compact V2 shards into ShardStore format)

---

## Timeline

| Phase | Effort | Risk | Depends On |
|-------|--------|------|------------|
| Phase 1: Dump processor | 2-3 days | Low | None |
| Phase 2: PG sync loader | 2-3 days | Medium | Phase 1 |
| Phase 3: Read path | 3-5 days | High | Phase 1 |
| Phase 4: Upsert integration | 5-7 days | High | Phase 3 |
| Phase 5: V2 removal | 1-2 days | Low | Phase 4 |
| **Total** | **~3-4 weeks** | | |

## Interim State (Current)

The dump processor uses a V2 workaround for multi-value fields:
- Writes `PackedValue::Mi(vec![single_value])` per row (Mi-tagged)
- V2 reader merges Mi entries for the same (slot, field) pair
- Scalar fields still use LIFO dedup (unchanged)
- This works correctly but should be replaced by Phase 1 (DocOp::Append)

## Files to Change

```
Phase 1:  src/dump_processor.rs (~6 call sites)
Phase 2:  src/pg_sync/single_pass.rs (~9 call sites)
Phase 3:  src/docstore.rs (get_v2 → ShardStore read)
          src/concurrent_engine.rs (get_document wrapper)
Phase 4:  src/concurrent_engine.rs (put_inner, patch)
          src/write_coalescer.rs (flush thread)
          src/mutation.rs (diff computation)
Phase 5:  src/docstore.rs (delete ~500 lines of V2 code)
```
