# DataSilo Port Plan — Doc Storage Layer Replacement

**Branch:** `ivy/datasilo-port-doc-layer` (on main worktree)
**Scope:** Replace `DocStoreV3` hex-sharded file storage with a DataSilo-style mmap layout, **keeping v2's existing `DocOp` / `OpCodec` typed-ops abstraction from ShardStore**. Keep `DocCache` as a thin decoded-`StoredDoc` overlay (Justin: option B).
**Not in scope:** BitmapSilo, CacheSilo, frozen bitmaps, sync-v2 pipeline format changes.

---

## FRAMING CORRECTION (post-Justin review)

**The v3 datasilo crate implements its ops log wrong for our purposes.** It stores opaque `Put(key, bytes) / Delete(key)` entries and delegates all merge semantics to a caller-supplied `merge_fn(existing, new)` that only runs at compaction. That means every single-field update re-encodes the whole doc's merge frame, and the per-field op types (`Set` / `Append` / `Remove`) live only in the pre-encoded blob.

**ShardStore already has the correct model** (`src/shard_store.rs:50` `OpCodec` trait, `src/shard_store_doc.rs:145` `DocOp` enum, `src/shard_store_doc.rs:424` `DocOpCodec`): a typed ops log with `{ Set, Append, Remove, Delete, Create, Merge }`, a snapshot type, and a `SnapshotCodec + OpCodec` pair where compaction folds ops into a snapshot.

**The correct port:** keep DataSilo's **on-disk infrastructure** — single `data.bin` + `index.bin` hash index + A/B ops log with CRC-framed entries + parallel rayon mmap writes + `DumpMergeWriter` — and **replace its ops log entry format with a typed, OpCodec-driven one** modeled on ShardStore's pattern. DataSilo becomes "ShardStore with a single mmap'd backing file instead of 210K tiny shard files."

---

## Scout findings (direct reads)

### DataSilo crate (`bitdex-v2-v3/crates/datasilo/` — 3489 lines)

- **Fully standalone.** No bitdex deps. Only `std`, `parking_lot`, `memmap2`, `rayon`, `crc32fast`, `thiserror`. Cargo-move as-is — but we'll modify the ops log.
- **Three mmap'd files per silo:** `index.bin` (HashIndex), `data.bin` (compacted values), `ops_a.log` / `ops_b.log` (A/B swap).
- **Wrong ops log shape (will be replaced):** stores only `Put(key, bytes)` and `Delete(key)`. All merge semantics delegated to `merge_fn` at compaction. **We'll replace this with a typed-op layout matching ShardStore's `OpCodec` pattern.**
- **"In-place updates" are buffer-slack based.** `HashIndex::IndexEntry { offset, length, allocated }` — each slot is allocated `max(length * buffer_ratio, min_entry_size)` bytes. If a re-encoded doc fits within `allocated`, writes are truly in-place (`DumpMergeWriter` path). Otherwise the old entry becomes dead space (`dead_bytes`) and compaction reclaims it later.
- **Compaction (`compact()`):** atomically `active_is_b.fetch_xor(true)`, drains frozen log, merges ops into data file (hot or cold path), flushes, truncates the frozen log. No read-window where ops disappear — `get_with_ops()` locks both logs.
- **Bulk write bypass (`write_batch_parallel`):** used once for the images phase to pre-allocate all slots. Parallel rayon mmap writes, pre-computes layout with buffer_ratio slack.
- **Hot dump merges (`DumpMergeWriter::merge_put`):** subsequent phases read-modify-write in place using 1024 striped mutexes keyed on `slot % 1024`. Bypasses the ops log entirely.

### v3 DocSiloAdapter (`bitdex-v2-v3/src/silos/doc_silo_adapter.rs` — 327 lines)

**This is the single biggest port accelerant.** It already imports `crate::mutation::FieldValue` and `crate::query::Value` — the **same types** v2 uses (`src/mutation.rs:21`, `src/query.rs`). `StoredDoc` is also the same shape (v2: `src/shard_store_doc.rs:36`). The adapter is **effectively portable by copy-paste**.

- `get(slot) -> Option<StoredDoc>` — calls `silo.get_with_ops(slot+1)`, decodes via `doc_format::decode_stored_doc`.
- `put(slot, &doc)` — `encode_merge_fields` → `silo.append_op`. Auto-registers new field names.
- `put_batch(&[(slot, doc)])` — `silo.append_ops_batch` (locks ops log once, appends all, flushes).
- `prepare_dump_merge()` — returns `DumpMergeWriter` for dump phases.
- `compact()` — delegates to `silo.compact()`.
- Field dictionary: JSON `field_dict.json` (`Vec<String>`, idx = position).
- `SLOT_KEY_OFFSET = 1` — avoids HashIndex `key=0` sentinel.
- `schema_version: u8` is a stub — `#[serde(skip)]`, defaults to 0. **Not persisted.** (See decision point.)

### v3 doc_format (`bitdex-v2-v3/src/silos/doc_format.rs` — 910 lines)

Relevant exports we need:
- `encode_merge_fields(slot, &[(u16, PackedValue)]) -> Vec<u8>`
- `encode_merge_fields_into(slot, fields, buf)` (zero-alloc variant)
- `decode_stored_doc(bytes, idx_to_field, defaults) -> io::Result<StoredDoc>`
- `decode_doc_fields(bytes) -> io::Result<Vec<(u16, PackedValue)>>`
- `merge_encoded_docs(existing, new) -> io::Result<Vec<u8>>` ← used as `merge_fn`
- `field_value_to_packed` / `packed_to_field_value` — FieldValue ↔ PackedValue
- `json_to_packed_with_dict` — for field defaults / DataSchema integration
- `PackedValue { I, F, B, S, Mi, Mm }`, `DocOp`, `DocSnapshot`

---

## Port plan

### What we're reusing vs rewriting from v3's datasilo crate

**REUSE as-is:**
- `hash_index.rs` — 826 lines, on-disk hash table (linear probing, bulk build, atomic in-place entry updates). No changes.
- `lib.rs` data file layout: `IndexEntry { offset, length, allocated }`, buffer_ratio / min_entry_size slack, `write_batch_parallel` for bulk load, `DumpMergeWriter` for in-place dump merges.
- A/B swap protocol for compaction (`active_is_b` atomic, `compact_cold_from` / `compact_hot_from`).
- `SiloConfig`, `ParallelOpsWriter`, `MERGE_STRIPE_COUNT`.

**REWRITE:**
- `ops_log.rs` — replace `SiloOp { Put(u64, Vec<u8>), Delete(u64) }` with a generic `OpsLog<C: OpCodec>` that stores framed typed ops. Frame format:
  `[u8 op_kind][u32 len][op bytes][u32 crc32]`
  where `op bytes` is whatever `C::encode_op` produces. The key (slot) is encoded by the codec, not the log. This matches ShardStore's OpCodec pattern.
- `lib.rs` read path — `get(slot) -> Option<Snapshot>` decodes the data file snapshot for `slot`, then scans both ops logs for typed ops against that slot and folds them in via `C::apply_op(&mut snapshot, &op)`. That is the "ops on read" pattern already in ShardStore.
- `lib.rs` write path — `append(op)` appends a typed op via `C::encode_op`. No more "encode whole doc then Put".
- Compaction — fold typed ops into the snapshot via `C::apply_op`, then re-encode via `S::encode_snapshot`. No caller-supplied `merge_fn`.

**Result:** DataSilo becomes the mmap-backed equivalent of ShardStore. Same trait surface, same op semantics, same "snapshot + ops log" mental model — just with one `data.bin` + `index.bin` + two ops logs instead of 210K hex-sharded files.

### Crates

Copy `bitdex-v2-v3/crates/datasilo/` → `bitdex-v2/crates/datasilo/` as a starting point, then modify per above. Add to workspace `Cargo.toml`:

```toml
[workspace]
members = [".", "crates/datasilo", /* ... existing ... */]

[dependencies]
datasilo = { path = "crates/datasilo" }
```

### New files in `bitdex-v2/src/`

- **`src/doc_silo.rs`** — new. Instantiates `DataSilo<DocSnapshot, DocOpCodec>` (reusing v2's existing `DocOp` + codec from `shard_store_doc.rs`). Provides `DocSilo::{open, get, get_many, put, apply_op, apply_ops_batch, compact}` and the field-dict management layer.
- **v2 already has the codec.** `src/shard_store_doc.rs:145` defines `DocOp`, line 424 defines `DocOpCodec: OpCodec`, and there's a `DocSnapshot` type used by `SnapshotCodec`. **We lift these into a new `src/doc_codec.rs` module** (or keep them in `shard_store_doc.rs` until the file is deleted) and the silo generics over them.
- **No need to port `bitdex-v2-v3/src/silos/doc_format.rs` wholesale** — v2's existing DocOp/DocOpCodec + PackedValue encoding already covers the same ground. v3's version is a clone with minor tweaks; keep v2's and discard v3's.

### Files to DELETE (Justin's guidance — surface the gaps as compile errors)

- `src/shard_store_doc.rs` — DocStoreV3, ~2400 lines.
- `src/doc_cache.rs` is **NOT deleted** — the hot-path `apply_ops_in_place` optimization must survive. It becomes a decoded-`StoredDoc` cache on top of `DocSilo.get()`. Populate on miss from `DocSilo::get()` instead of `DocStoreV3::get()`.

### Call-site swap list (by layer)

Source for these file:lines: the three parallel scout reports filed this session.

**Handler (`src/server.rs`)**
- `handle_query` line 2822: `engine.get_documents_batch(&slot_ids)` — no change at the handler boundary; the engine method itself swaps its inside.

**Engine (`src/concurrent_engine.rs`)**
- 5898, 5903, 5905: `get_document()` cache-first + disk fallback. Replace `docstore.read().get(slot)` with `docsilo.get(slot)`.
- 5985, 6071, 6079: `get_documents_batch()` three-phase (cache probe → disk batch → populate). Replace `docstore.read().get_many(&miss_ids)` with a new `docsilo.get_many(&miss_ids)` helper (see new APIs below).
- 6106–6111: shard prepopulate — **remove this path**. With DataSilo, cold reads are 4 μs; shard prepopulate is no longer load-bearing. Delete the branch and the config flag.
- 6243, 6257, 6271, 6277: `doc_cache_refresh_slots()` — same pattern, swap disk layer.
- 6352, 6380: `doc_cache_apply_ops_batch()` — **preserve as-is**. This is the v1.0.186 refresh-I/O fix. Only its fallback path changes backing store.
- 3346, 3371, 3403: mutation upsert paths (`put_via_wal`, `patch_document_via_wal`, `put_inner`) — all read old doc via `docstore.read().get(id)`. Swap to `docsilo.get(id)`.
- 7489, 7513: `spawn_docstore_writer` / `write_docs_to_docstore` — bulk load path. Replace with DataSilo bulk writer loop (see Dump below).

**Ops processor (`src/ops_processor.rs`)**
- `DocWriter` struct (line 48+): currently buffers Set/Add/Remove and flushes via `append_tuples_batch_concurrent` / `append_multi_ops_batch_concurrent` (which write into DocStoreV3 shard files). With typed DocOps in the silo ops log, `DocWriter.flush()` becomes a direct batch append: `docsilo.apply_ops_batch(&pending_ops)` where each op is a `DocOp::Set/Append/Remove`. Zero doc re-encoding. Zero reads.
- `FieldMeta::has_computed_deps` — keep as-is, still gates which ops route to fallback.

**WAL reader (`src/server.rs:1172-1325`)**
- Line 1227: `apply_ops_batch()` — no change in signature, only underlying writers swap.
- Line 1233: `doc_writer.flush()` — see above (reads existing doc from silo, re-encodes, appends).
- Line 1250: `doc_cache_apply_ops_batch()` — unchanged. Fallback path goes through silo instead of DocStoreV3.

**Mutation (`src/mutation.rs`)**
- Line 674, 951: `put()`, `delete()` staging-engine paths — swap disk layer.

**Dump (`src/dump_processor.rs`)**
- Line 1573: `StreamingDocWriter::new()` — replace with a `DocSiloBulkBuilder` that collects `(slot, encoded_bytes)` tuples and flushes via `silo.write_batch_parallel(&entries)` at phase boundary.
- Line 2042: `bulk_writer.append_merge_payload(slot, &buf)` — for phase 1 (images) this goes into the bulk builder; for phases 2+ it goes through `DumpMergeWriter::merge_put(slot, &buf, doc_format::merge_encoded_docs)`.
- Line 2573: `bulk_writer.finalize()` — phase 1 calls `silo.write_batch_parallel`; phases 2+ call `writer.flush()` + `silo.reload_data()`.
- Lines 2625-2737: bitmap saves — **unchanged**. Bitmaps still live in ShardStore. Only doc storage is replaced.
- Lines 1444, 1471: `engine.clear_doc_cache()` — unchanged. The DocCache overlay still needs clearing across phase boundaries.

### DocSilo API shape

```rust
pub struct DocSilo {
    silo: DataSilo<DocSnapshot, DocOpCodec>,  // generics in the vendored crate
    field_to_idx: HashMap<String, u16>,
    idx_to_field: Vec<String>,
    field_defaults: HashMap<u16, PackedValue>,
}

impl DocSilo {
    pub fn open(path: &Path) -> io::Result<Self>;
    pub fn get(&self, slot: u32) -> io::Result<Option<StoredDoc>>;
    pub fn get_many(&self, slots: &[u32]) -> io::Result<Vec<Option<StoredDoc>>>;
    pub fn apply_op(&self, op: DocOp) -> io::Result<()>;          // single typed op to ops log
    pub fn apply_ops_batch(&self, ops: &[DocOp]) -> io::Result<()>; // batch append
    pub fn put(&self, slot: u32, doc: &StoredDoc) -> io::Result<()>; // DocOp::Create convenience
    pub fn bulk_load(&mut self, docs: &[(u32, StoredDoc)]) -> io::Result<()>; // write_batch_parallel
    pub fn compact(&mut self) -> io::Result<u64>;
    pub fn prepare_dump_merge(&self) -> io::Result<Option<DumpMergeWriter>>;
}
```

The hot-path batch read is:
1. `get_many(&[slot])` — for each slot: hash probe → decode snapshot from data.bin → scan both ops logs and fold matching DocOps via `DocOpCodec::apply_op` → return StoredDoc.
2. ~42ns per snapshot hash probe warm, ~4μs cold. Ops log scan is O(ops in log) per batch, but ops log is bounded by `compact_threshold` + heartbeat compaction, so it stays small in steady state.
3. The `batch_unique_shards P99 = 62` file-open problem dissolves — every slot is an independent hash probe on the same mmap.

### Cache integration (Justin's option B)

Keep `DocCache` exactly as-is. Change only two things:

1. **Populate-on-miss source**: in `concurrent_engine::get_document()` and `get_documents_batch()`, when the cache misses, call `docsilo.get(slot)` / `docsilo.get_many(miss_ids)` and populate the cache with the decoded `StoredDoc`.
2. **Refresh path in `doc_cache_apply_ops_batch` fallback**: currently calls `doc_cache_refresh_slots` → `docstore.get_many`. Swap to `docsilo.get_many`.

The `apply_ops_in_place` hot path is untouched: it mutates cached `StoredDoc`s directly, independent of the disk layer.

### Janitor / compaction trigger

`DataSilo::compact()` is caller-driven. Wire into a background task in `ConcurrentEngine::run_background_loop()` (or create a new loop if one doesn't exist):

- **Trigger conditions** (OR):
  - `silo.needs_compaction()` → `dead_ratio > config.compact_threshold` (default 0.20)
  - `silo.ops_size() > 512 MB` (cap active ops log before it mmaps beyond sane size)
  - Every 5 min heartbeat if `silo.has_ops()`
- **Non-blocking**: compact runs on its own thread, holding neither engine snapshot nor doc_cache lock. Reads continue via the A/B swap.
- **Metric emission**: `docsilo_compaction_count`, `docsilo_compaction_duration_seconds`, `docsilo_dead_ratio`, `docsilo_ops_size_bytes`.

### Schema versioning decision (task #128)

**Recommendation:** Encode schema_version as a leading byte in the merge frame payload (extend `encode_merge_fields_into`). Cheap, self-describing, forward-compatible. Decoder checks first byte; if unknown version, either error or call a version-specific decoder.

This is a pre-port decision to discuss with Justin. Default to 0 if he wants to defer.

### Compaction semantics (replaces old merge_fn section)

With typed ops in the log, compaction is standard LSM folding:
1. Freeze active ops log (A/B swap).
2. For each slot touched in the frozen log: load snapshot from `data.bin`, fold each frozen `DocOp` via `DocOpCodec::apply_op(&mut snapshot, &op)` (Set replaces, Append unions+dedups Mi, Remove filters Mi, Delete tombstones, Merge upserts).
3. Re-encode snapshot via `SnapshotCodec::encode_snapshot`; write in-place if fits `allocated`, else allocate at end of `data.bin` and mark old slot as `dead_bytes`.
4. Bulk-update index offsets via `HashIndex::update_existing_concurrent`.
5. Truncate frozen ops log.

`DocOpCodec::apply_op` already exists in v2 (`src/shard_store_doc.rs` — used by ShardStore compaction today). Lift it into the silo module unchanged.

---

## Task order (revised)

| # | Task | Blocks |
|---|---|---|
| 118 | Deep scout (this doc) | ✓ done in part, writing up |
| 120 | Vendor datasilo crate into `crates/datasilo` | 121 |
| 128 | Schema version decision (Justin call) | 121 |
| 121 | Port DocSilo adapter to `src/doc_silo.rs`, `src/doc_format.rs` | 122 |
| 131 | Audit/adapt merge_encoded_docs | 121 |
| 122 | Replace DocStoreV3 consumers (~35 call sites across read + write paths) | 123 |
| 123 | Rewrite DocWriter flush path for silo ops log | 124 |
| 130 | Wire janitor into background loop | 126 |
| 124 | Integration tests — port `tests/doc_cache_apply_ops.rs` + new silo tests | 125 |
| 125 | External review (Gemini + GPT) | 126 |
| 126 | Dev bench at 107M vs replay capturelog | 127 |
| 127 | Prod ship: clean data dir + full redump | — |

---

## Risks & open questions

1. **OpCodec lift scope** — v2's `OpCodec` trait is in `src/shard_store.rs`, intertwined with the ShardStore generic. We may need a lightweight copy in the vendored datasilo crate (`trait OpCodec` + `trait SnapshotCodec`) so the crate stays workspace-lib-only without a circular dep on the main bitdex-v2 lib. Decide early: keep the traits in the vendored crate and let bitdex-v2 implement them, or re-export from bitdex-v2 and let datasilo depend on it.
2. **Ops-on-read cost** — scanning both ops logs on every read is O(ops_in_log). Fine at steady state if compaction keeps ops logs small, but a query storm right after a lot of mutations could pay the cost. Mitigation: aggressive compaction heartbeat + per-slot ops index in RAM (HashMap<slot, smallvec<op_offset>>) that the scan uses instead of a linear walk. Start without the index; add if profiler says it matters.
3. **Field-dict drift** — decode must tolerate unknown field indices. Bounds-check `idx_to_field` on every read.
4. **DocCache cold-miss decode** — silo returns decoded `StoredDoc`, cache stores decoded. Fine at p50 because apply_ops_in_place handles steady-state. Cold miss pays ~4μs mmap + decode. Accept for now.
5. **Dump phase coupling** — `write_batch_parallel` is destructive (truncates ops logs). Verify this matches v2 dump's "images first, then merge phases" order — it should, but confirm in `dump_processor.rs`.
6. **Crash recovery** — ops log is append-only with CRC. On restart, cursor scan recovers to last valid entry. `write_batch_parallel` truncation window is a known risk; given full-redump flow, acceptable.
