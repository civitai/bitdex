# Indexed Value Lookup for `per_value_lazy` / multi-value Fields — Design

Owner: Donovan (draft)
Reviewers: Scarlet (team lead), Justin (operator + path-call)
Date: 2026-04-25
Status: Design draft. Ships as the V2 follow-up to mission-gate measurement (per `docs/_in/lazy-load-localization-2026-04-25.md`).

---

## 1. Problem

`FilterBitmapStore::load_field_values(field, values)` reads the **entire bucket snapshot** to extract one (or a few) requested values. For `postId` at 22.8 M values across 256 buckets, each bucket is ≈ 89 K values / 30–80 MB on disk. Cold-path single-value lookup → 350–900 ms typical, observed up to 3.16 s.

Code, `src/shard_store_bitmap.rs:891`:

```rust
pub fn load_field_values(&self, field: &str, values: &[u64]) -> io::Result<HashMap<u64, RoaringBitmap>> {
    let mut by_bucket: HashMap<u8, Vec<u64>> = HashMap::new();
    for &v in values {
        let bucket = ((v >> 8) & 0xFF) as u8;
        by_bucket.entry(bucket).or_default().push(v);
    }
    let mut result = HashMap::new();
    for (bucket, wanted) in by_bucket {
        let key = FilterBucketKey { field: field.to_string(), bucket };
        if let Some(snap) = self.read(&key)? {        // ← ENTIRE bucket snapshot
            for v in wanted {
                if let Some(bm) = snap.values.get(&v) {
                    result.insert(v, bm.clone());
                }
            }
        }
    }
    Ok(result)
}
```

`self.read()` is `ShardStore::read()` which deserializes the full `BucketSnapshot` (HashMap<u64, RoaringBitmap>) before any filtering.

## 2. Affected fields (live observation)

Captured 25 minutes of shadow-on traffic, 27,066 lazy-load events:

| Field | Events | Type |
|---|---|---|
| postId | 9,859 (36%) | single_value, `per_value_lazy: true` |
| modelVersionIds | 4,940 (18%) | multi_value (lazy by default) |
| postedToId | 4,834 (18%) | single_value, `per_value_lazy: true` |
| modelVersionIdsManual | 787 (3%) | multi_value |
| tagIds | 224 (1%) | multi_value |
| toolIds, techniqueIds, isRemix, minor, poi | < 25 each | various |

Slowest 10 lazy-load events: 3.16s, 2.24s, 1.77s, 1.52s, 1.51s, 1.49s, 1.47s, 1.34s, 1.33s, 1.30s. **Multiple fields are affected; the fix targets the shared code path.**

## 3. Goal

Single-value lookup against `FilterBitmapStore` should be O(value_size) wall-clock — not O(bucket_size). Target: 5–20 ms per cold lookup, 30–50× speedup on the long tail.

## 4. Design

### 4.1 Sparse index per shard file

Each shard file `shards/<field>/<bucket:02x>.shard` gains a paired sparse index at `shards/<field>/<bucket:02x>.idx`. The index maps `value → (offset, length)` within the shard's serialized values payload.

#### Shard format (current, unchanged for backwards compat)

```
[magic: 4 bytes]
[version: 4 bytes]
[crc32: 4 bytes over body]
[body_len: 8 bytes]
[body: serde-encoded BucketSnapshot { values: HashMap<u64, RoaringBitmap> }]
```

`self.read()` deserializes the full body to get all entries.

#### Sparse index format (new, side-car file)

```
[magic: 4 bytes "BIDX"]
[version: 4 bytes]
[crc32: 4 bytes over entries]
[entry_count: 4 bytes]
[entries: entry_count * (u64 value, u64 offset, u64 length)]
```

24 bytes per entry. For postId at 89 K values per bucket → ~2 MB index file per bucket → ~512 MB total across 256 buckets. Acceptable in-memory cost; index files load into a `HashMap<(field, bucket), Arc<HashMap<u64, (u64, u64)>>>` on first access.

#### Shard payload format change (V2 — opt-in)

To support indexed reads, the shard body needs to be deserializable per-value. Two options:

**Option a — preserve serde, add seek-table.** The serialized BucketSnapshot uses bincode (current). Adding a seek-table after the body lets us know where each value's bytes start. Inverts the serialization: write `[entry_count][entries: (value, len, bytes...)*]` instead of `serde-encoded HashMap`. Reader can fseek to byte offset N, decode just that entry.

**Option b — change to length-prefixed value-bitmap pairs.** New shard payload format `[count][value][bitmap_len][bitmap_bytes][value][bitmap_len][...]`. Index points at value's bitmap_len byte. Reader reads len + bitmap. Simpler than option a.

Recommend **option b** for the new format. Cleaner semantics, easier migration.

### 4.2 Read path

```rust
pub fn load_field_values(&self, field: &str, values: &[u64]) -> io::Result<HashMap<u64, RoaringBitmap>> {
    let mut by_bucket: HashMap<u8, Vec<u64>> = HashMap::new();
    for &v in values {
        let bucket = ((v >> 8) & 0xFF) as u8;
        by_bucket.entry(bucket).or_default().push(v);
    }
    let mut result = HashMap::new();
    for (bucket, wanted) in by_bucket {
        let key = FilterBucketKey { field: field.to_string(), bucket };
        if let Some(idx) = self.read_index(&key)? {
            // INDEXED PATH: read just the bytes for wanted values.
            let mut file = self.open_shard(&key)?;
            for v in wanted {
                if let Some(&(offset, len)) = idx.get(&v) {
                    let bytes = self.read_at(&mut file, offset, len)?;
                    let bitmap = decode_bitmap(&bytes)?;
                    result.insert(v, bitmap);
                }
            }
        } else {
            // FALLBACK: full-bucket read for shards written before V2 format.
            if let Some(snap) = self.read(&key)? {
                for v in wanted {
                    if let Some(bm) = snap.values.get(&v) {
                        result.insert(v, bm.clone());
                    }
                }
            }
        }
    }
    Ok(result)
}
```

Index existence per shard is the migration signal. Shards without an index file fall back to full-bucket read (current behavior). Shards with an index get the new fast path.

### 4.3 Write path

`ShardStore::write_filter_bucket` (and `write_filter_bucket_raw`) gain an index emission step. While serializing values, track each value's byte offset + length. After body is written, flush the index file alongside.

```rust
pub fn write_filter_bucket(...) -> io::Result<()> {
    // Build payload + index in one pass
    let mut payload = Vec::new();
    let mut index = Vec::with_capacity(entries.len());
    payload.write_u32_le(entries.len() as u32)?;
    for (value, bitmap) in entries {
        let bm_bytes = encode_bitmap(bitmap)?;
        let offset = payload.len() as u64;
        payload.write_u64_le(*value)?;
        payload.write_u64_le(bm_bytes.len() as u64)?;
        payload.write_all(&bm_bytes)?;
        let len = (payload.len() as u64) - offset;
        index.push((*value, offset, len));
    }
    // Write shard
    self.write_shard_atomic(&key, &payload)?;
    // Write index sidecar
    self.write_index_atomic(&key, &index)?;
    Ok(())
}
```

Atomicity: write `.shard.tmp` + `.idx.tmp`, fsync both, rename in order (shard first, then idx). On crash mid-write, an existing index for an old shard might point at stale offsets — the magic + version + CRC of the index protect against this. If the index doesn't match the shard's CRC, fall back to full-bucket read.

### 4.4 In-memory index cache

Loading the index is a small disk read (2 MB for postId per bucket). After first load, cache in memory: `Arc<DashMap<(String, u8), Arc<HashMap<u64, (u64, u64)>>>>`. ≤ 256 MB total index cache for postId; bounded by num shards.

LRU eviction if memory pressure rises (per task #19's bucket-snapshot cache pattern). Probably unnecessary at current scale.

## 5. Migration

### 5.1 First-run after deploy

Shards written by the old format have no index. `load_field_values` falls back to full-bucket read (current behavior). No regression.

### 5.2 Re-encoding existing shards

Two paths:

**(a) Lazy re-encode on next compaction.** Compaction already rewrites the whole shard. Add index emission to compaction's write path. Eventually all shards get indexed as compaction runs.

**(b) Eager re-encode on deploy.** A boot-time pass walks every shard, re-emits with index. Adds ~minutes to boot, gives clean state immediately. Not recommended for first ship; (a) is safer.

Recommend (a).

### 5.3 Backwards compat

Both shard formats coexist. Reader picks based on index file presence + magic header. No flag day required.

## 6. Test plan

### Unit
- Round-trip a bucket: write 100 values, read 1 random value → matches the source bitmap.
- Read fallback: write old-format shard (no index) → read still works.
- Crash mid-write: write `.shard.tmp` only, no `.idx.tmp` → fall back to full-bucket on read.
- Index CRC mismatch: corrupt index file → fall back to full-bucket on read.

### Integration
- Full-dump load → compaction emits indices → load_field_values uses fast path.
- Mixed shards (some indexed, some not) → works correctly.

### Performance
- Microbench: cold single-value lookup, indexed vs unindexed → expect ≥ 30× speedup on the 89K-value postId bucket shape.
- Mission-gate replay: re-run shadow-on traffic against indexed-shard build → expect 0 queries above 1 s.

## 7. Risk + mitigations

| Risk | Mitigation |
|---|---|
| Index file corruption | CRC + fallback to full-bucket read |
| Shard / index out of sync (write crashed between rename calls) | Magic + version + CRC check rejects mismatched index |
| Format-change scope creep | Backwards compat fallback means we ship without re-encoding existing shards immediately; compaction does it lazily |
| Extra disk usage | Index files ≈ 1–3 % of shard size. Acceptable. |
| First-cold-read still slow until index is loaded into memory | LRU index cache (drop-in, lazy populate); cold cost is one-time per (field, bucket) tuple |

## 8. Alternatives considered

| Alt | Why not |
|---|---|
| **Smaller buckets (4096 instead of 256)** | Linear improvement. 22.8 M / 4096 ≈ 5.5K values/bucket → bucket size shrinks 16× → ~25 ms cold lookup. Less than indexed lookup's 30-50× and incurs 16× more shard files (file system scaling concern). |
| **Per-value file shard** | 22.8 M files. Filesystem scaling unworkable. |
| **In-memory bucket cache only (no index)** | First read still slow. Cache miss = 350-900 ms. |

## 9. Out of scope (V3+)

- Sort-layer fusion under writes (separate surface; not in this PR's path).
- Auto-warm prediction of postId values (orthogonal; handled by the warm registry shape persistence).
- Compression scheme changes.

## 10. Implementation order

Recommended ship sequence:

1. **PR-A.** Add `BIDX` index file format + `read_index()` + indexed `load_field_values` fast path with backwards-compat fallback. Tests: unit + microbench. ~250-400 LOC.
2. **PR-B.** Wire write path to emit index alongside shard. Tests: round-trip + crash-mid-write. ~150 LOC.
3. **PR-C.** Compaction emits index too. Lazy migration of existing shards. ~50 LOC.
4. **Replay-rig validation.** Run shadow-on rig against indexed build, confirm zero outliers > 1 s.
5. **Tag + ship.**

PR-A + PR-B can land in either order (PR-A is read-only, PR-B is write-only). Compaction (PR-C) requires both to be effective.

---

*Draft. Awaiting Justin's path call (A1 ship-as-is / A2 fix-first / A3 hybrid) before engineering time goes in.*
