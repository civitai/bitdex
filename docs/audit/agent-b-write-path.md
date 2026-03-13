# Write Path & Mutation Audit

**Auditor**: Agent B
**Date**: 2026-03-13
**Scope**: Write coalescer, mutations, docstore, bitmap persistence, flush/merge threads, pg-sync CDC path

## Files Examined

- `src/write_coalescer.rs` — WriteBatch grouping, MutationSender, WriteCoalescer
- `src/mutation.rs` — diff_document, diff_patch, collect_filter_remove/insert_ops
- `src/docstore.rs` — DocStore (sharded zstd msgpack), BulkWriter
- `src/bitmap_fs.rs` — BitmapFs filesystem persistence
- `src/concurrent_engine.rs` — flush thread, merge thread, put/patch/delete, snapshot publishing
- `src/loader.rs` — NDJSON bulk loading pipeline
- `src/server.rs` — HTTP upsert/delete endpoints
- `src/pg_sync/outbox_poller.rs` — CDC outbox poller
- `src/pg_sync/bitdex_client.rs` — HTTP client for CDC push
- `src/pg_sync/bulk_loader.rs` — PG COPY bulk load pipeline
- `src/config.rs` — Default channel capacity, flush intervals

---

## Findings

### 1. Multi-value upsert write amplification — remove-all + insert-all instead of symmetric diff

**Severity**: HIGH
**Impact**: At Civitai scale, a document with 30 tagIds where 1 tag changes generates 60 MutationOps (30 removes + 30 inserts) instead of 2 (1 remove + 1 insert). With tagIds dominating 79-80% of filter bitmap memory, this is the single largest source of unnecessary work in the write path.

**File**: `src/mutation.rs:162-181`

When `field_values_equal` returns `false` for a multi-value field (even if only 1 of 30 values changed), the code removes ALL old values and inserts ALL new values:

```rust
// Clear old filter bits (ALL of them)
if let Some(old) = old_val {
    collect_filter_remove_ops(&mut ops, &arc_name, slot, old);
}
// Set new filter bits (ALL of them)
if let Some(new) = new_val {
    collect_filter_insert_ops(&mut ops, &arc_name, slot, new);
}
```

The `WriteBatch::apply` method applies removes before inserts (line 275-293) to handle the overlap correctly, but the redundant ops still consume channel capacity, coalescer grouping time, and bitmap mutation work.

**Suggested fix**: For `Multi` fields, compute the symmetric difference — only emit remove ops for values in old but not new, and insert ops for values in new but not old. Use a `HashSet` intersection approach:

```rust
let old_keys: HashSet<u64> = old_vals.iter().filter_map(value_to_bitmap_key).collect();
let new_keys: HashSet<u64> = new_vals.iter().filter_map(value_to_bitmap_key).collect();
for &removed in old_keys.difference(&new_keys) { /* FilterRemove */ }
for &added in new_keys.difference(&old_keys) { /* FilterInsert */ }
```

---

### 2. Docstore read-decompress-rewrite on every single-doc put/upsert

**Severity**: HIGH
**Impact**: Every upsert reads the entire shard file (up to 512 docs), decompresses it, filters out the target doc, re-inserts the new doc, re-compresses, and re-writes. Under sustained CDC writes (outbox poller), this means the same shard can be read-decompress-rewrite multiple times per second if writes cluster by slot range.

**File**: `src/docstore.rs:605-633` (`put` method) and `src/docstore.rs:636-697` (`put_batch` method)

The flush thread batches doc writes (`put_batch` at line 836-839 of `concurrent_engine.rs`), which helps by grouping by shard. But each shard still gets a full read-decompress-rewrite per batch cycle. The `put_batch` method at line 676-683 reads the existing shard, decompresses everything, filters, merges, sorts, re-compresses, and re-writes.

Additionally, the single-doc `get` at line 550-567 (used by `put` for upsert diff and `delete` for cleanup) decompresses the entire shard to read one document. `find_in_shard` at line 373-387 calls `read_shard_file` which decompresses everything, then binary-searches the index.

**Suggested fix**:
- For reads: keep the index in the uncompressed header so `find_in_shard` can seek to the exact doc offset and decompress just that doc's bytes (requires per-doc compression instead of per-shard).
- For writes: accumulate a write-ahead buffer (in-memory HashMap of pending doc writes) and coalesce multiple writes to the same shard before flushing. This avoids repeated decompress-recompress of the same shard within a batch window.

---

### 3. Missing fsync in bitmap persistence — data loss risk on power failure

**Severity**: HIGH
**Impact**: The doc comment in `bitmap_fs.rs:6` says "Atomic tmp->fsync->rename pattern" but the actual code at lines 65-79 does `std::fs::write` + `std::fs::rename` with NO fsync. On Linux, rename is not durable without fsync of the file AND the parent directory. On power loss, the rename can be visible (metadata persisted) but the file contents may be empty/corrupted.

**File**: `src/bitmap_fs.rs:65-79` (`write_bitmap_atomic`), `src/bitmap_fs.rs:94-105` (`write_bytes_atomic`)

```rust
fn write_bitmap_atomic(path: &Path, bitmap: &RoaringBitmap) -> Result<()> {
    let tmp_path = path.with_extension("roar.tmp");
    // ... serialize ...
    std::fs::write(&tmp_path, &buf)?;  // NO fsync
    std::fs::rename(&tmp_path, path)?; // rename without prior fsync
    Ok(())
}
```

Same pattern in `write_bytes_atomic` and all docstore writes (`write_shard_file` at line 436-438, `save_field_dict` at line 167-169).

**Suggested fix**: After `std::fs::write`, open the file, call `.sync_all()`, then rename. Also fsync the parent directory after rename. Since Postgres is the source of truth, this is not catastrophic (data can be re-synced), but it would cause unnecessary re-bulk-loads on crash.

---

### 4. Docstore lock contention: put and delete both lock the docstore on the caller thread

**Severity**: MEDIUM
**Impact**: `put()` at line 1177 and `delete()` at line 1245 both acquire `self.docstore.lock()` on the caller (HTTP handler) thread to read the old doc for diffing. Under concurrent writes from the outbox poller, every upsert and delete contends on this single Mutex. The flush thread also locks docstore at line 837 for batch writes. This creates a three-way contention: multiple writer threads reading + flush thread writing.

**File**: `src/concurrent_engine.rs:1177` (put), `src/concurrent_engine.rs:1245` (delete), `src/concurrent_engine.rs:837` (flush batch write)

```rust
// put() - caller thread holds docstore lock during disk I/O
let old_doc = if is_upsert || was_allocated {
    self.docstore.lock().get(id)?  // blocks on Mutex while reading from disk
} else {
    None
};
```

**Suggested fix**: Use a read-write lock (RwLock) instead of Mutex for the docstore. Reads (get) can happen concurrently; only batch writes need exclusive access. Alternatively, since docstore.get() does disk I/O anyway, consider a lock-free approach: read the shard file without holding the lock (files are atomically renamed, so reads always see a consistent file).

---

### 5. Existence set clone-on-insert: O(n) clone per new distinct value

**Severity**: MEDIUM
**Impact**: At line 467 of `concurrent_engine.rs`, when a new distinct value appears for a lazy-value field (e.g., a new tagId), the entire `HashSet<u64>` is cloned to insert one value. For tagIds with 31K existing values, this is 31K * 8 bytes = ~248KB cloned per new tag. During CDC steady-state this is rare, but during initial ingestion or schema changes with many new values, this could cause significant allocation pressure on the flush thread.

**File**: `src/concurrent_engine.rs:462-473`

```rust
if !current.contains(&fgk.value) {
    let mut updated = (**current).clone();  // Clone entire HashSet
    updated.insert(fgk.value);
    ek.store(Arc::new(updated));
}
```

**Suggested fix**: Batch all new values per field in the flush cycle, then clone once and insert all. Or switch to a concurrent set (e.g., DashSet) instead of ArcSwap<HashSet>.

---

### 6. Hardcoded schema_version: 0 in put() docstore writes

**Severity**: MEDIUM
**Impact**: The `put()` method at line 1195 creates `StoredDoc` with `schema_version: 0` (the legacy marker). This means all runtime upserts via the HTTP API or CDC get stored as legacy docs, bypassing the schema versioning and default-elision system. The `encode_doc` method will still use the docstore's current `schema_version` when encoding to bytes (prepend_version at line 509), but the StoredDoc struct's version field doesn't match. This is currently harmless because `schema_version` is `#[serde(skip)]` and reconstructed on decode (line 532-544), but it's a code smell that will bite if the struct is ever serialized directly or if the version field gains semantic meaning beyond decode-time defaults.

**File**: `src/concurrent_engine.rs:1193-1196`, also at lines 2677 and 2786

```rust
let stored = StoredDoc {
    fields: doc.fields.clone(),
    schema_version: 0,  // Should be docstore.schema_version
};
```

**Suggested fix**: Either remove the `schema_version` field from `StoredDoc` entirely (it's skip-serialized and reconstructed on decode), or set it correctly from the docstore's current version.

---

### 7. Delete does not mark in-flight — race with concurrent put

**Severity**: MEDIUM
**Impact**: The `put()` method marks the slot in-flight (line 1160) before reading the docstore, ensuring concurrent readers know the slot is being mutated. But `delete()` at line 1243 does NOT mark in-flight. If a `put` and `delete` arrive concurrently for the same slot, the delete's docstore read could see the old doc while the put has already sent new bitmap ops to the coalescer. The resulting remove ops from delete could clear bits that were just set by the concurrent put.

**File**: `src/concurrent_engine.rs:1243-1287` (delete method)

Compare with `put()` at line 1160 which does `self.in_flight.mark_in_flight(id)`.

**Suggested fix**: Add `self.in_flight.mark_in_flight(id)` at the start of `delete()` and `self.in_flight.clear_in_flight(id)` at the end, matching the pattern in `put()`.

---

### 8. diff_patch uses linear scan for field config lookup

**Severity**: LOW
**Impact**: `diff_patch` at line 326-333 does `config.filter_fields.iter().any()` and `config.sort_fields.iter().find()` for every field in the patch. With 10-15 fields in a typical Civitai config, this is negligible. But under a pathological config with many fields, or with PATCH batches processing thousands of patches per second, this becomes O(fields * config_fields) per patch.

**File**: `src/mutation.rs:326-333`

```rust
let is_filter = config.filter_fields.iter().any(|f| f.name == *field_name);
if let Some(sort_config) = config.sort_fields.iter().find(|s| s.name == *field_name) {
```

**Suggested fix**: Pre-build a `HashMap<String, FilterFieldConfig>` and `HashMap<String, SortFieldConfig>` at engine construction time (or in `FieldRegistry`). O(1) lookup instead of O(n) scan.

---

### 9. Per-op MutationOp Vec<u32> allocation — 1 alloc per op for single-slot mutations

**Severity**: LOW
**Impact**: Every `MutationOp` variant carries `slots: Vec<u32>`. For single-document put/patch/delete, every op allocates a `Vec` with exactly one element: `vec![slot]`. A Civitai document with 7 filter fields + 2 sort fields generates ~15-40 MutationOps per upsert, each with a heap-allocated `Vec<u32>` containing a single u32.

**File**: `src/mutation.rs` (throughout — e.g., lines 131-134, 209-212, 265-268, 293-296, 377-381)

```rust
ops.push(MutationOp::SortClear {
    field: arc_name.clone(),
    bit_layer: bit,
    slots: vec![slot],  // heap allocation for 1 u32
});
```

**Suggested fix**: Use `SmallVec<[u32; 1]>` (from the `smallvec` crate) instead of `Vec<u32>`. Single-slot ops stay on the stack (zero heap alloc). Bulk ops that exceed 1 element spill to heap as before. This eliminates ~15-40 heap allocations per single-doc mutation.

---

### 10. Merge thread clones entire snapshot for persistence

**Severity**: LOW
**Impact**: At line 905 of `concurrent_engine.rs`, the merge thread does `(*snap).clone()` to get a mutable copy of the entire InnerEngine for compaction before persistence. This is an O(num_fields) clone (Arc refcount bumps), not a deep copy, so it's relatively cheap. But `merge_dirty()` at lines 921-933 then iterates all non-pending filter fields and calls `iter_versioned()` to collect every (value, bitmap) pair as owned clones for serialization. At 105M scale with tagIds having 31K values, this creates 31K `RoaringBitmap` clones (via `vb.base().as_ref().clone()` at line 930).

**File**: `src/concurrent_engine.rs:904-933`

```rust
let mut compacted = (*snap).clone();
// ...
for (name, field) in compacted.filters.fields_mut() {
    // ...
    for (&value, vb) in field.iter_versioned() {
        filter_entries.push((name.clone(), value, vb.base().as_ref().clone()));
    }
}
```

**Suggested fix**: Serialize bitmaps directly from the snapshot without cloning into intermediate Vec. Or only persist dirty fields (track per-field dirty flags that survive across merge intervals).

---

### 11. Outbox poller does not batch-optimize writes — sequential per-doc upserts

**Severity**: LOW
**Impact**: The outbox poller (`pg_sync/outbox_poller.rs:126-137`) sends upserts via `client.upsert_batch()` which hits the HTTP endpoint. The server's `handle_upsert` (line 1158-1172) iterates documents sequentially, calling `engine.put()` for each one. Each `put()` acquires the docstore lock, reads the old doc from disk, computes the diff, and sends ops to the coalescer channel. For a batch of 100 documents, this is 100 sequential docstore reads under a Mutex.

**File**: `src/server.rs:1158-1172`, `src/pg_sync/outbox_poller.rs:126-137`

**Suggested fix**: Add a bulk upsert method to ConcurrentEngine that reads all needed shards in parallel (group by shard, decompress once per shard), computes all diffs, then sends ops in one batch. This would reduce docstore lock acquisitions from N to ~N/512 (shard count).

---

### 12. Merge thread writes ALL loaded filter bitmaps on every dirty cycle

**Severity**: MEDIUM
**Impact**: When the dirty flag is set, the merge thread at lines 920-933 writes ALL loaded non-pending filter bitmaps to disk, not just the ones that changed. At 105M scale, this means serializing and writing every loaded filter field on every merge cycle (default 5 seconds). For tagIds with 31K values, this is significant I/O even when only a handful of tag bitmaps changed.

**File**: `src/concurrent_engine.rs:916-933`

The comment at line 916-919 explains the race condition that prevents per-field dirty checking:
```rust
// Note: we don't check has_dirty() per-field because the flush
// thread's periodic compaction (merge_dirty) clears per-field
// dirty flags before the merge thread runs, creating a race.
```

**Suggested fix**: Use a separate dirty-for-merge flag (not shared with the flush thread's compaction) that the merge thread clears only after successful write. Or maintain a set of field names mutated since last merge.

---

## Prioritized Top 5

| Rank | Finding | Severity | Estimated Impact on Production |
|------|---------|----------|-------------------------------|
| 1 | **#1: Multi-value write amplification** | HIGH | 10-30x more bitmap ops than necessary per CDC upsert when tagIds change. Under sustained writes, this dominates coalescer throughput and flush thread work. |
| 2 | **#2: Docstore read-decompress-rewrite per upsert** | HIGH | Every CDC upsert decompresses an entire 512-doc shard from disk to read one doc. Under write pressure, the same shard gets repeatedly decompressed and rewritten. |
| 3 | **#3: Missing fsync in bitmap/docstore persistence** | HIGH | On power failure, persisted bitmap/docstore data may be corrupted despite "atomic" write pattern. Requires full re-sync from Postgres. Low probability but high cost when it happens. |
| 4 | **#7: Delete missing in-flight marker** | MEDIUM | Correctness issue: concurrent put+delete on same slot can produce stale bitmap state. Probability depends on CDC ordering guarantees (outbox dedup helps, but race window exists). |
| 5 | **#4: Docstore Mutex contention (read vs write)** | MEDIUM | Every put/delete blocks on the same Mutex as the flush thread's batch writes. Under sustained CDC pressure (100+ upserts/sec), this becomes the write path bottleneck before the coalescer or flush thread. |
