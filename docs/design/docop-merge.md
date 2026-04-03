# DocOp::Merge — Multi-Phase Dump Docstore Fix

## Problem

Multi-phase CSV dumps lose data from earlier phases. After all 6 phases complete (images → tags → resources → tools → techniques → metrics), documents only contain the last phase's fields. All earlier fields are zeroed out.

### Root Cause

The dump processor writes all phases using `DocOp::Create`, which **replaces** the entire document:

```rust
// shard_store_doc.rs:531-533
DocOp::Create { slot, fields } => {
    snapshot.docs.insert(*slot, fields.clone());  // REPLACES
}
```

Phase 1 (images) writes `Create { slot=42, fields=[userId, nsfwLevel, url, ...] }`. Phase 6 (metrics) writes `Create { slot=42, fields=[reactionCount, commentCount, collectedCount] }`. On read, phase 6's Create replaces phase 1's data entirely.

### Why Set Alone Doesn't Fix It

`DocOp::Set` works field-by-field and merges correctly. But using Set for object-level writes during dumps would mean N individual ops per document per phase (one per field), which is far less compact than a single op with all fields. At 109M records x 20 fields, that's 2.18B ops vs 109M ops.

## Design: DocOp::Merge

Add a new `DocOp::Merge` variant that combines fields into an existing document without replacing it.

### Op Definition

```rust
pub enum DocOp {
    Set { slot: u32, field: u16, value: PackedValue },
    Append { slot: u32, field: u16, value: PackedValue },
    Remove { slot: u32, field: u16, value: PackedValue },
    Delete { slot: u32 },
    Create { slot: u32, fields: Vec<(u16, PackedValue)> },
    Merge { slot: u32, fields: Vec<(u16, PackedValue)> },  // NEW
}
```

### Apply Semantics

```rust
DocOp::Merge { slot, fields } => {
    let doc = snapshot.docs.entry(*slot).or_default();
    for (field_idx, value) in fields {
        if let Some(entry) = doc.iter_mut().find(|(f, _)| *f == *field_idx) {
            entry.1 = value.clone();  // overwrite existing field
        } else {
            doc.push((*field_idx, value.clone()));  // add new field
        }
    }
}
```

**Key semantic rules:**
- **Merge is an upsert on document existence.** If slot exists, patch fields. If not, create doc with provided fields via `or_default()`.
- **Last-write-wins per field.** If the merged field already exists, overwrite it.
- **Duplicate field indices within one Merge op** resolve by last occurrence wins (linear scan behavior). Reject/deduplicate at write time when practical.
- **Field order is not semantically meaningful.** All lookups use `.find()` linear scan, not binary search. No sorting required.
- **Delete does not block future writes.** `Delete` removes current doc state. A subsequent `Merge` or `Set` recreates the doc via `or_default()`. This is standard log-structured upsert semantics.

Key difference from Create:
- **Create**: `snapshot.docs.insert(slot, fields)` — replaces entire document
- **Merge**: iterates fields and upserts each one into the existing document

### Wire Format

- Tag: `OP_TAG_MERGE = 0x06`
- Encoding: identical to Create — `[tag][slot:u32][num_fields:u16][field_pairs...]`
- Only the tag byte differs

### Backward/Forward Compatibility

- **New reader + old file**: Fully supported. Old files contain no Merge ops.
- **Old reader + new file**: Old binaries will encounter `0x06` and fail with "unknown doc op tag" error (existing error path in `decode_op`). This is a clear, fast failure.
- **Rollback after writing Merge ops**: Requires compaction first to resolve Merge ops into snapshot data. After compaction, the shard file contains only a snapshot (no ops), which old binaries can read.
- **Mitigation**: Deploy new binary, run compaction, verify. If rollback is needed, compact all shards first.

### Compaction Behavior

During compaction (`read_up_to_generation`), ops are applied in order over the snapshot:
1. Snapshot (if present) provides the base document
2. Ops are applied sequentially via `OpCodec::apply()`
3. Merge ops merge fields into whatever exists

After compaction writes a new snapshot, the snapshot contains the fully merged document. No special compaction logic needed — the standard apply path handles it.

### When to Use Each Op

| Op | Use Case | During Dump |
|----|----------|-------------|
| `Create` | Destructive full replacement (ops pipeline upserts where full doc is known) | NOT used during dump — Merge is safer |
| `Merge` | Add/update fields on an existing or new document | ALL object-level dump phases (images, resources enrichment, metrics) |
| `Set` | Single field update | Individual tuple writes (tags, tools, techniques) |
| `Append` | Add value to multi-value field | Not used during dump currently |

**Critical design decision (per GPT/Gemini review):** ALL dump phases use `Merge` for object-level writes, including phase 1 (images). This eliminates the ordering hazard where a late `Create` could wipe earlier `Merge` data. `Create` is reserved for the ops pipeline where full-document replacement semantics are explicitly intended.

### Dump Processor Changes

The `StreamingDocWriter` gets explicit methods instead of a boolean mode flag (per review feedback — explicit methods are harder to misuse):

1. **`write_merge_doc(slot, fields)`** — NEW: Writes `DocOp::Merge`. Used by all dump phases for object-level writes.
2. **`write_doc(slot, fields)`** — EXISTING: Continues to write `DocOp::Create`. Used by ops pipeline only.
3. **`write_field(slot, field_idx, value)`** — EXISTING: Writes `DocOp::Set`. Used for individual tuple fields (tags, tools, techniques). Unchanged.

In `dump_processor.rs`, change all calls from `write_doc()`/`append_tuples_raw()` to `write_merge_doc()` for object-level phase writes. Tuple phases (tags, tools, techniques) continue using `write_field()` / `Set` as today.

### Hardcoded Generation: gen_000

The docstore has a hardcoded `gen_000` path:

```rust
// shard_store_doc.rs:1164
root.join("gen_000")
```

This is fine — the docstore uses a single-generation model (unlike bitmap shardstore which uses multi-gen). The `gen_000` is effectively a constant directory name, not a dynamic generation. No change needed here.

### Files Changed

1. **`src/shard_store_doc.rs`**
   - Add `Merge` variant to `DocOp` enum (line ~159)
   - Add `OP_TAG_MERGE = 0x06` constant (line ~170)
   - Add encode/decode for Merge in `DocOpCodec` (identical to Create encoding, different tag)
   - Add apply logic for Merge in `DocOpCodec::apply()` (line ~469)
   - Add `write_merge_doc()` method to `StreamingDocWriter`
   - Add `append_tuples_merge()` method (like `append_tuples_raw` but emits Merge)

2. **`src/dump_processor.rs`**
   - Change object-level write calls from `append_tuples_raw()` to `append_tuples_merge()`
   - Tuple phases (tags, tools, techniques) unchanged — they use `write_field()` / `Set`

3. **No changes to `concurrent_engine.rs`** — the writer creation doesn't need a flag

### Operational Invariants

- **Phase ordering within a dump**: Not strictly required for correctness since all phases use Merge, but phases should still run in documented order for operational clarity.
- **Every slot need not appear in phase 1**: If a slot only appears in phase 3, Merge creates a partial doc. This is acceptable — the alive bitmap (set by phase 1) determines visibility.
- **No Create after Merge for same slot during dumps**: Enforced by using only Merge in the dump path. Create is reserved for the ops pipeline.
- **Field index consistency**: All phases share the same `field_to_idx` mapping from the config-driven schema. This is enforced by the `StreamingDocWriter` using the engine's field registry.

### Test Plan

#### Unit Tests (shard_store_doc.rs)

1. `test_merge_op_roundtrip` — encode/decode Merge
2. `test_apply_merge_combines_fields` — Merge into existing doc preserves old fields
3. `test_apply_merge_overwrites_existing_field` — Merge updates fields that already exist
4. `test_apply_merge_on_empty_doc` — Merge on nonexistent slot creates the doc
5. `test_merge_then_merge_accumulates` — Two Merge ops for same slot, verify union of fields
6. `test_create_then_merge_preserves_both` — Create phase 1, Merge phase 2, verify both fields present
7. `test_merge_then_create_replaces` — Verify Create after Merge still replaces (for ops pipeline correctness)
8. `test_delete_then_merge_resurrects` — Delete followed by Merge creates new doc
9. `test_merge_duplicate_fields_last_wins` — Merge with duplicate field indices, verify last occurrence wins
10. `test_compaction_preserves_merge_chain` — Create + Merge + Merge, compact, read, verify all fields

#### Integration Tests

11. `test_streaming_writer_merge_between_phases` — Phase 1 write_merge_doc, finalize, Phase 2 write_merge_doc, verify combined
12. `test_streaming_writer_merge_and_set_between_phases` — Phase 1 write_merge_doc, Phase 2 write_field (Set), verify combined
13. `test_read_before_and_after_compaction_identical` — Build state via ops, read, compact, read again, compare

#### Local Dump Tests

14. Small dataset (1000 records), 2 phases (images + metrics), verify all fields present
15. Small dataset, 3 phases (images + tags + metrics), verify mixed Merge + Set works

#### Full Dump Test

16. 109M records, all 6 phases, verify documents have all fields from all phases

### Potential Gaps

1. **Crash/recovery mid-phase**: If phase 2 writes Merge for 30% of docs then crashes, rerunning phase 2 writes duplicate Merge ops. This is safe — Merge is idempotent for scalar fields (last write wins). For multi-value fields written via Set, duplicates are also safe (Set overwrites).
2. **Partial/corrupt op at tail**: The existing shard reader truncates incomplete trailing ops (CRC validation). Merge ops use the same framing, so tail recovery works unchanged.
3. **Wrong method selected**: Using `write_doc()` (Create) instead of `write_merge_doc()` during a dump would still cause data loss. Mitigated by: explicit method names, no boolean mode, clear documentation. Could add a runtime warning if Create is used during an active dump task.
4. **Schema drift**: If phases somehow use different field index mappings, Merge would silently write wrong fields. Mitigated by: all phases use the same engine's field registry. Could add a schema hash to shard headers for extra safety (future work).
5. **Append/Remove interaction with Merge**: A field introduced by Merge and later modified by Append should work correctly since Merge upserts the field entry and Append modifies the existing value. Should be covered by unit tests.

### Review History

- **GPT-5.4 review**: Recommended Merge for all phases (not just 2+), explicit methods over boolean flag, stronger compaction tests, post-Delete resurrection semantics, forward compatibility gating.
- **Gemini 3.1 Pro review**: Flagged field ordering (confirmed not an issue — linear scan), alive bitmap interaction, downgrade compatibility, property-based testing.
- Both agreed the design is sound with these additions.
