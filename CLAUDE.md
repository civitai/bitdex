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

8. **JSON query parser only for V2.** OpenSearch and Meilisearch syntax plugins are future work.

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
- **Cache**: Separate `Arc<Mutex<TrieCache>>` with brief locks (lookup ~μs, store ~μs). Targeted invalidation: only filter fields that actually changed are invalidated; sort-only flushes skip cache invalidation entirely.
- **Loading mode**: `enter_loading_mode()` / `exit_loading_mode()` skips snapshot publishing and all maintenance during bulk inserts. Avoids `Arc::make_mut()` deep-cloning FilterField HashMaps every flush cycle. On exit, force-publishes staging and invalidates all caches.

### Unified Cache

- Flat HashMap keyed by (filter_clauses, sort_field, direction) — consolidates former trie cache + bound cache
- Dynamic capacity: 4K initial (sorted vec binary search), expands to 64K (8-bit radix bucketing) on pagination
- Live maintenance: flush thread adds/removes slots on mutations for all clause types
- LRU eviction by `max_bytes` (512MB default) with `last_used` timestamps
- Meta-index for targeted invalidation: bitmaps tracking which cache entries reference each (field, value) pair
- Sort queries 2-13x faster at 104M scale via pre-filtered working sets

---

## Reference Materials

> **Before proposing architectural changes**, read the relevant design doc in `docs/design/`. These documents capture the rationale behind decisions and prevent re-inventing approaches that were already evaluated. See `docs/learnings/` for things we tried that didn't work.
>
> See `docs/README.md` for the full folder structure explanation.

### Design Documents (read before changing architecture)

- **Concurrency**: `docs/design/design-concurrency.md` — ArcSwap snapshot architecture, Arc-per-bitmap CoW, VersionedBitmap diffs, flush/merge threads, loading mode, lazy bitmap loading
- **Storage**: `docs/design/design-storage.md` — BitmapFs (hex-bucketed bitmap persistence), DocStore (sharded zstd-compressed msgpack documents), persistence lifecycle
- **Unified Cache**: `docs/design/design-unified-cache-final.md` — Cache architecture consolidating filter+sort+time bucket caching
- **Cache Persistence**: `docs/design/design-unified-cache-persistence.md` — BoundStore design for warm cache restarts (APPROVED, not yet built)
- **Idle Eviction**: `docs/design/design-idle-eviction.md` — Per-value bitmap eviction for multi_value fields
- **Radix Sort**: `docs/design/design-radix-sort-trie.md` — 8-bit radix bucketing for large cache entries (Phase 1 implemented)
- **Rolling Restart Cursors**: `docs/design/design-rolling-restart-cursors.md` — Named cursors for zero-downtime restarts (Phases 1-3 implemented)

### Design Conversations (understand WHY decisions were made)

- **Architecture Conversations**: `docs/_in/architecture-conversations.md` — Merged design conversations covering the evolution from OpenSearch to bitmaps, slot model, sort layer design, meta-index innovation, bound cache tiering, time buckets, and bulk loading. Has a navigable summary with line references.
- **Full Project Brief**: `docs/_in/prepared-prompt.md` — Authoritative specification with complete architecture, API specs, config schemas, testing strategy, and development phases.
- **Storage Overhaul**: `docs/_in/storage-overhaul.md` — Requirements for the redb-to-filesystem pivot

### Learnings (what we tried that didn't work)

- `docs/learnings/write-pipeline.md` — Loading mode vs adaptive pressure, persist thread, bulk accumulator
- `docs/learnings/storage.md` — Lazy loading vs tiered caching, redb vs custom filesystem
- `docs/learnings/ingestion.md` — Parsing bottlenecks, simd-json/rkyv evaluation

### Benchmarks

- **Performance Baselines**: `docs/benchmarks/performance-baseline.md` — Consolidated baselines with regression thresholds (authoritative)
- **Benchmark Report**: `docs/benchmarks/benchmark-report.md` — 5M/50M/100M/104.6M scaling analysis
- **Loading Mode Comparison**: `docs/benchmarks/benchmark-comparison-loading-mode.md` — Before/after bound cache impact
- **Write Regression Analysis**: `docs/benchmarks/write-regression-loading-mode.md` — ArcSwap clone cascade root cause
- **Loadtest Guide**: `docs/benchmarks/loadtest-guide.md` — Rust loadtest binary usage and baselines

### Guides

- **HTTP API**: `docs/guide/api.md` — All endpoints, request/response examples
- **Config Schema**: `docs/guide/config-schema.md` — Configuration reference
- **Civitai Schema**: `docs/guide/bitdex-civitai-schema.md` — Field mapping for Civitai dataset
- **Testing**: `docs/guide/testing.md` — Test suite guide

### External References

- **V1 Codebase**: `C:\Dev\Repos\open-source\bitdex\` — Reference for reusable code (filter bitmaps, WAL consumer, server scaffolding). DO NOT bring over Vecs, skip lists, sorted arrays, forward maps, or reverse indexes.

---

## Development Phases

### Phase 1: Core Engine — COMPLETE (commit 7bc60fd)
Slot allocation, alive bitmap, filter bitmaps, sort layer bitmaps, mutation API (PUT/PATCH/DELETE/DELETE WHERE), query execution, JSON query parser, config loading. Full test coverage.

### Phase 2: Persistence — COMPLETE
Custom filesystem storage: BitmapFs for bitmap persistence (hex-bucketed pack files), sharded DocStore for documents (zstd-compressed msgpack). Save-and-unload with zero-copy `fused_cow()` for memory reclamation. Lazy bitmap loading per-field on first query (<1s startup at 105M). No WAL (not needed — Postgres is the source of truth).

### Phase 3: Performance — COMPLETE (commits 95df2a5 through bdccbe2)
- Cardinality-based query planning (planner.rs)
- Trie cache with prefix matching and generation-counter invalidation (cache.rs)
- Unified cache with bounded top-K bitmaps per (filter, sort, direction) (unified_cache.rs)
- ArcSwap lock-free snapshot reads with Arc-per-bitmap CoW (concurrent_engine.rs)
- Write coalescing via crossbeam channels with batched flush loop (write_coalescer.rs)
- Targeted cache invalidation — sort-only flushes skip invalidation
- Arc<str> field name interning for zero-copy mutation ops
- Loading mode for bulk inserts — skips snapshot publishing to avoid clone cascade (6fb2b78)
- Bound cache with tiered bounds for sort query acceleration (2-13x at 104M)
- Meta-index for targeted bound cache invalidation
- Idle eviction for high-cardinality multi-value fields (tagIds)
- Fused parse+bitmap loader pipeline (320-460K/s sustained)
- Benchmark harness with 20 query types + contention benchmark, memory reporting

### Phase 4: Operations — PARTIAL
- Prometheus metrics endpoint (`/metrics`) — COMPLETE
- HTTP server with index management, query, upsert, delete endpoints — COMPLETE
- Web UI with infinite scroll image grid — COMPLETE
- Grafana dashboard — COMPLETE
- Autovac, admin API, graceful shutdown — NOT yet started

### Phase 5: Integration — IN PROGRESS
- NDJSON bulk loading from file — COMPLETE
- Upsert/delete endpoints — COMPLETE
- E2E test suite (6 self-contained suites, 31 tests) — COMPLETE
- Postgres CDC sync (pg-sync) — COMPLETE (external tool)
- Shadow mode comparison — IN PROGRESS

---

## Running Locally

```bash
# Run the server for testing (port 3001 to avoid conflicts with other dev servers)
cargo run --release --features server --bin server -- --port 3001 --data-dir ./data
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

Run `/testing` for the full guide. Key points:

- **Before committing**, check which tests cover your changed files. The `/testing` command has a change→test mapping table.
- **Rust tests** (`cargo test`) for bitmap correctness, property-based testing, and high-throughput benchmarks. Node can't match Rust's throughput for load tests.
- **Node E2E tests** (`node tests/e2e/e2e-*.mjs`) for HTTP API behavior, full write pipeline, and observable client behavior.
- **Automated runner**: `node tests/e2e/run-e2e.mjs` starts a fresh server, runs all self-contained E2E suites, and produces JSON results.
- **Full docs**: `docs/guide/testing.md` — master reference for all test suites, run commands, and coverage gap analysis.

---

## Measured Memory (Civitai dataset, remapped IDs, 4 threads)

| Scale | Bitmap Memory | RSS | Worst Query p50 |
|------:|-------------:|----:|----------------:|
| 5M | 328 MB | 1.20 GB | 0.83ms |
| 50M | 2.95 GB | 6.09 GB | 13.5ms |
| 100M | 6.19 GB | 11.66 GB | 18.7ms |
| 104.6M | 6.49 GB | 12.14 GB | 21.1ms |
| 104.6M (bound cache) | 6.51 GB | 14.51 GB | 6.08ms |

tagIds dominates filter memory at 79-80% across all scales.
Full results: `docs/benchmarks/benchmark-report.md`, `docs/benchmarks/benchmark-comparison-loading-mode.md`

### Extrapolation to 150M

| Component | Estimated Size |
|---|---|
| Filter bitmaps | ~8.1 GB |
| Sort bitmaps | ~1.1 GB |
| Trie cache | ~160 MB |
| **Total bitmap memory** | **~9.3 GB** |
| **Total RSS** | **~17.4 GB** |

Within the original 7-11 GB bitmap target. RSS overhead is ~48% from allocator + OS page cache.

Document store on disk: ~6 GB at 100M records.

---

## Future Roadmap (NOT V2 Scope — Do Not Build)

- LSH vector similarity search
- Postgres extension (pgrx)
- Log encoding for sort fields (unless benchmarks demand it)
- OpenSearch/Meilisearch query parser plugins
- Visual bitmap explorer
- Multi-index support
- Shared memory hot restarts
