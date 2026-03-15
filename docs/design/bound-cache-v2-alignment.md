# Bound Cache V2 Alignment: Append-Only + Tombstoning

> BoundStore already uses tombstoning for invalidation. Aligning it with the V2 docstore pattern means: writes are appends or tombstone marks, cleanup is background work by the janitor. Review what BoundStore does today and what changes to make it consistent with the always-appendable philosophy.

## Current BoundStore Design (src/bound_store.rs)

BoundStore persists unified cache entries to disk for warm restarts. Two file types:

- **meta.bin** — loaded eagerly on startup. Contains entry metadata (cache keys, clause hashes, sort field, direction). Small (~1 MB).
- **.ucpack shard files** — lazy-loaded per sort field on first query. Contains serialized bitmaps + sorted keys per cache entry. Loaded into memory on demand.

### Current Tombstoning

On mutation (upsert/delete), the flush thread marks affected bound cache entries as invalid via the meta-index. For entries backed by BoundStore, it **tombstones** them: marks the entry in meta.bin as invalid without rewriting the shard file. On next query, tombstoned entries are skipped. On shard reload, tombstoned entries are filtered out.

This IS the append-only pattern — mutations don't rewrite files, they mark entries dead. Cleanup happens lazily.

### What's Missing

1. **No background cleanup of tombstoned shards.** Tombstoned entries accumulate in shard files. Over time, a shard can be 90% tombstones. The shard file gets read in full on lazy load, wasting I/O on dead entries.

2. **No dirty/tombstone count in shard headers.** The janitor can't scan for shards that need cleanup without reading the full meta.bin.

3. **Shard rewrites during merge.** The merge thread currently rewrites shards when merging RAM entries into persisted entries. This is a full read-modify-write, same problem as V1 docstore.

## Proposed Changes

### 1. Add tombstone count to meta.bin

Track `tombstoned_count` alongside `total_count` in meta.bin. The janitor checks: if `tombstoned_count / total_count > 0.3`, rewrite the shard to remove dead entries.

### 2. Janitor cleans tombstoned shards

```rust
// In janitor loop:
for shard_key in bound_store.shard_keys() {
    let (total, tombstoned) = bound_store.tombstone_ratio(&shard_key);
    if tombstoned > total / 3 {
        bound_store.compact_shard(&shard_key);  // rewrite without tombstones
    }
}
```

### 3. Merge thread appends instead of rewriting

Instead of read-modify-write for merge, append new entries to the shard file (same as V2 docstore). LIFO on shard load — newest entry per cache key wins. Compaction cleans up.

This eliminates the merge thread's I/O bottleneck and makes bound cache writes as fast as docstore writes.

## Alignment Summary

| Property | DocStore V2 | BoundStore (current) | BoundStore (aligned) |
|----------|------------|---------------------|---------------------|
| Write | Append tuple | Read-modify-write shard | Append entry |
| Invalidation | Newest wins (LIFO) | Tombstone in meta.bin | Tombstone + LIFO |
| Cleanup | Janitor compacts dirty shards | Manual purge endpoint | Janitor compacts tombstoned shards |
| Background | Always appendable | Partially (meta.bin yes, shards no) | Always appendable |
