# Bitdex V2 — CLAUDE.md

## What is Bitdex?

Bitdex is a purpose-built, in-memory bitmap index engine written in Rust. Its primary job is to take filter predicates and sort parameters and return an ordered list of integer IDs. Indexing is bitmaps all the way down.

**In:** Filter predicates + sort field + sort direction + limit
**Out:** Ordered `Vec<i64>` of IDs

Documents are stored on disk via a custom sharded filesystem store keyed by slot ID. This serves two purposes: (1) enabling efficient targeted bitmap updates on upsert by diffing old vs new field values, and (2) optionally serving document content alongside query results. Full-text search happens downstream.

---

## Inviolable Design Principles

These are non-negotiable. Any agent working on this project MUST follow these rules. Violating them is grounds for rejecting a PR.

1. **Bitmaps are the index.** All filtering and sorting is done via roaring bitmap operations. No Vecs for column storage. No skip lists. No sorted arrays. No forward maps. No reverse indexes as index structures.

2. **Documents are stored on disk.** A custom sharded filesystem store persists documents keyed by slot ID. On upsert, the old document is read from disk, diffed against the new one, and only the changed bitmaps are updated. This makes writes O(changed fields) instead of O(all bitmaps). Documents can also be served alongside query results.

3. **No sorted data structures.** No sorted Vecs, no skip lists, no B-trees for maintaining sort order. Sorting is done via bit-layer bitmap traversal. Period.

4. **No in-memory forward maps or reverse indexes.** The on-disk document store replaces the need for these. On upsert, read old doc from disk to determine which bitmaps to update. For DELETE WHERE on high-cardinality fields, scan the bitmaps.

5. **Clean deletes.** Deletes read the stored doc and clear all filter/sort bitmap bits before clearing the alive bit. This keeps filter bitmaps clean (no stale bits), eliminating the alive AND from the query hot path.

6. **Slot = Postgres ID** for integer ID users. No mapping layer.

7. **Full precision sort layers first.** Do not implement log encoding or reduced bit depths until benchmarks prove it's necessary.

8. **JSON transport, pluggable query formats.** Three formats: `bitdex` (default, typed JSON), `compact` (MongoDB-style), `meilisearch` (string DSL). All parse to the same `BitdexQuery` via the `QueryParser` trait. Select per-request with `?format=` or set a server default. See `docs/guide/query-formats.md`.

9. **Single process, single node.** No clustering, no replication, no distributed consensus. A Postgres fallback in the API layer handles the (rare) downtime during restarts.

---

## Architecture Overview

### Slot Model

- Each document's Postgres ID IS the slot (its position in every bitmap)
- Slots are monotonically assigned via atomic counter on insert
- Deleted slots have their filter/sort bitmap bits cleared immediately (clean delete)
- The alive bitmap tracks active documents but is NOT ANDed into queries (filter bitmaps are always clean)
- An autovac process periodically produces a clean bitmap of recycled slots
- New inserts check the clean bitmap first (grab first set bit), append only if none available

### Document Store

- Custom sharded filesystem store (`src/docstore.rs`) keyed by slot ID
- Documents grouped into shard files (512 docs/shard), zstd-compressed msgpack with per-field dictionary encoding
- Hex-nested directory structure keeps each dir under ~1000 files at 105M+ scale
- On PUT upsert: read old doc from disk, diff old vs new, update only changed bitmaps
- On fresh insert (slot not alive): write doc to disk, set bitmaps directly — no diff needed
- On DELETE: clear alive bit (doc stays on disk until autovac cleans it)
- NVMe random reads are microseconds — disk lookup adds negligible latency to writes
- Documents can optionally be returned alongside query result IDs

### Bitmap Categories

1. **Alive Bitmap** — One bitmap tracking all active documents. Used for slot management, stats, and as the universe for negation operators (NotEq, Not). NOT ANDed into queries — filter bitmaps are kept clean via clean deletes. Delete = clear filter/sort bits + clear alive bit.
2. **Filter Bitmaps** — One roaring bitmap per distinct value per filterable field. Boolean fields: one bitmap per boolean. Multi-value fields: one bitmap per distinct value.
3. **Sort Layer Bitmaps** — Each sortable numeric field decomposed into N bitmaps (one per bit position). A u32 field = 32 bitmaps. Top-N retrieval via MSB-to-LSB traversal using AND operations.

### Concurrency Model — ArcSwap Lock-Free Reads

- **Snapshot architecture**: Flush thread owns a private staging `InnerEngine`, publishes immutable snapshots via `ArcSwap::store()`. Readers load snapshots with `ArcSwap::load()` (zero-cost Guard, no refcount ops).
- **Arc-per-bitmap CoW**: Each `RoaringBitmap` wrapped in `Arc`. `Arc::make_mut()` only clones bitmaps with refcount > 1. Filter/sort fields also Arc-wrapped for O(num_fields) snapshot clone.
- **Write path**: Writers compute diffs and send `MutationOp`s to a crossbeam channel. Flush thread drains, batches, applies to staging, publishes new snapshot atomically.
- **In-flight tracking**: Writers mark slot IDs in an atomic in-flight set before mutation, clear after. Readers post-validate overlapping IDs.
- **Cache**: `Arc<Mutex<UnifiedCache>>` flat HashMap keyed by (filter_clauses, sort_field, direction). Live maintenance by flush thread. Persistent on disk via BoundStore (`bitmaps/bounds/`). Targeted invalidation: only filter fields that actually changed are invalidated; sort-only flushes skip cache invalidation entirely.
- **Loading mode**: `enter_loading_mode()` / `exit_loading_mode()` skips snapshot publishing and all maintenance during bulk inserts. Avoids `Arc::make_mut()` deep-cloning FilterField HashMaps every flush cycle. On exit, force-publishes staging and invalidates all caches.

### Unified Cache

- Flat HashMap keyed by (filter_clauses, sort_field, direction) — consolidates former trie cache + bound cache
- Dynamic capacity: 4K initial (sorted vec binary search), expands to 64K (8-bit radix bucketing) on pagination
- Live maintenance: flush thread adds/removes slots on mutations for all clause types
- LRU eviction by `max_bytes` (512MB default) with `last_used` timestamps
- Meta-index for targeted invalidation: bitmaps tracking which cache entries reference each (field, value) pair
- **Persistent via BoundStore** (`src/bound_store.rs`): meta.bin loaded eagerly on startup, bitmap shards lazy-loaded on first query per sort field. Tombstoning invalidates unloaded entries on mutations. Purge via `DELETE /cache/persistent`.
- Sort queries 2-13x faster at 104M scale via pre-filtered working sets

---

## Reference Materials

Run `/architecture` for the full guide — design docs, learnings, known gaps, and expectations for updating docs when changing architecture.

Key guides: `docs/guide/api.md` (HTTP API), `docs/guide/query-formats.md` (query syntax), `docs/guide/config-schema.md` (config), `docs/guide/testing.md` (tests), `docs/guide/bitdex-civitai-schema.md` (Civitai fields).

---

## Development Status

Run `/dev-guide` for full phase details, workflow expectations, and what's built vs not.

**Summary:** Phases 1-3 COMPLETE. Phase 4 partial (server/metrics/UI done, cache persistence COMPLETE, autovac/admin not started). Phase 5 in progress (shadow mode comparison).

---

## Running Locally

```bash
# Run the server for testing (port 3001 to avoid conflicts with other dev servers)
cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./data
```

---

## Coding Standards

- **Language**: Rust
- **Bitmap Library**: `roaring-rs` (roaring bitmaps)
- **Every PR must include tests** for the code it adds
- **Property-based tests** using `proptest` or `quickcheck` for bitmap operations
- **Fuzz the JSON query parser** with arbitrary input — nothing should panic or corrupt state
- **Benchmark suite** must run on every PR — any PR that degrades benchmarks by >10% gets flagged
- Correctness first, performance second
- When in doubt, refer to `docs/_in/prepared-prompt.md` for the authoritative specification

### Testing Guide

Run `/testing` for the full guide — change→test mapping, E2E development patterns, and test data directory standards. Run `/microbench` for throwaway performance experiments.

---

## Performance & Memory

Run `/perf` for memory baselines at scale, measurement methodology, regression thresholds, and benchmark commands.

**Quick reference:** 104.6M records = 6.51 GB bitmap memory, 14.51 GB RSS. tagIds = 79-80% of filter memory. ~62 bytes/record scaling.

---

## Future Roadmap (NOT V2 Scope — Do Not Build)

- LSH vector similarity search
- Postgres extension (pgrx)
- Log encoding for sort fields (unless benchmarks demand it)
- OpenSearch/Meilisearch query parser plugins
- Visual bitmap explorer
- Multi-index support
- Shared memory hot restarts
