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

### Bound Cache

- Pre-filters sort candidates with approximate top-K bitmaps (one per sort field + direction)
- Tiered bounds: tight bound (top 2K) attempted first, loose bound (top 200K) as fallback
- Negligible memory: 6 bounds = 2.28 KB at 104M records
- Lazy refresh: updated during flush cycles when sort fields change
- Sort queries 2-13x faster at 104M scale

### Meta-Index

- Bitmaps indexing bitmaps: tracks which cache IDs contain each (field, value) pair
- Enables targeted bound cache invalidation without scanning all bounds
- Negligible memory: 6 entries = 180 B at 104M records

### Trie Cache

- Keyed by canonically sorted filter clauses (sorted by field name, then value)
- Supports prefix matching for partial cache hits
- Automatic promotion/demotion based on exponential-decay hit stats
- Lazy invalidation via generation counters per filter field
- Sort fields (reactionCount, etc.) are NOT part of cache keys — sort is applied after cache lookup

---

## Reference Materials

### Design Conversations (read these to understand WHY decisions were made)

- **Original Architecture**: `docs/in/Claude-Bitdex.md` — Full brainstorming conversation showing the evolution from OpenSearch to the bitmap-only architecture. Covers the core "bitmaps all the way down" philosophy, slot model, sort layer design, and why we rejected Vecs/skip lists/B-trees.
- **Continued Design (Persistence + Bulk Loading)**: `docs/in/claude-bitdex-continued-1.md` — Continuation covering bulk loading pipeline design, put_bulk() architecture, decompose/apply worker pools, accumulator buffers, sharded doc persistence, and performance targets.
- **Parallelization Strategy**: `docs/in/new-parallelization-strat.md` — Justin's design notes on the dual-endpoint write pipeline (put vs put_bulk), decomposition pools, accumulator buffers, size-based promotion, and doc persistence batching.
- **Workers vs Threads**: `docs/in/Claude-Workers vs threads in Rust.md` — Discussion of Rust concurrency patterns relevant to the worker pool design.

### Architecture & Design Docs

- **ArcSwap + Storage Reconciliation**: `docs/design-arcswap-redb-reconciliation.md` — How the ArcSwap snapshot architecture and filesystem persistence layer work together. Two-tier storage (Tier 1 in-memory, Tier 2 on-disk via BitmapFs + DocStore), startup sequence, and memory budget analysis.
- **Performance & Persistence Roadmap**: `docs/roadmap-performance-and-persistence.md` — Full implementation roadmap (Prereq→A→B→C→D→E phases) with detailed task breakdowns for bitmap persistence, sort-by-slot, time handling, bound caches, and meta-index.
- **Architecture Risk Review**: `docs/architecture-risk-review.md` — Risk analysis of architectural decisions and mitigations.
- **Backpressure Design**: `docs/design-backpressure-implementation.md` — Backpressure and auto-throttle design for the write pipeline.

### Specifications & Benchmarks

- **Full Project Brief & Development Guide**: `docs/in/prepared-prompt.md` — Contains complete architecture, API specs, config schemas, testing strategy, development phases, and team structure. This is the authoritative specification.
- **Benchmark Report**: `docs/benchmark-report.md` — 5M/50M/100M/104.6M scaling analysis with memory and query latency breakdowns.
- **Loading Mode Comparison**: `docs/benchmark-comparison-loading-mode.md` — Before/after comparison showing loading mode fix impact.
- **Write Regression Analysis**: `docs/write-regression-loading-mode.md` — Root cause analysis of the ArcSwap clone cascade write regression and the loading mode fix.

### Phase Audits

- `docs/audit/prereq-audit.md` through `docs/audit/phase-e-audit.md` — Post-implementation audits for each roadmap phase. `docs/audit/synthesis.md` has the cross-phase summary.

### External References

- **V1 Codebase**: `C:\Dev\Repos\open-source\bitdex\` — Reference for reusable code (filter bitmaps, WAL consumer, server scaffolding). DO NOT bring over Vecs, skip lists, sorted arrays, forward maps, or reverse indexes.

---

## Development Phases

### Phase 1: Core Engine — COMPLETE (commit 7bc60fd)
Slot allocation, alive bitmap, filter bitmaps, sort layer bitmaps, mutation API (PUT/PATCH/DELETE/DELETE WHERE), query execution, JSON query parser, config loading. Full test coverage.

### Phase 2: Persistence — PARTIAL
Custom filesystem storage: BitmapFs for bitmap persistence (hex-bucketed pack files), sharded DocStore for documents (zstd-compressed msgpack). WAL and sidecar snapshot builder are NOT yet implemented.

### Phase 3: Performance — COMPLETE (commits 95df2a5 through bdccbe2)
- Cardinality-based query planning (planner.rs)
- Trie cache with prefix matching and generation-counter invalidation (cache.rs)
- ArcSwap lock-free snapshot reads with Arc-per-bitmap CoW (concurrent_engine.rs)
- Write coalescing via crossbeam channels with batched flush loop (write_coalescer.rs)
- Targeted cache invalidation — sort-only flushes skip invalidation
- Arc<str> field name interning for zero-copy mutation ops
- Loading mode for bulk inserts — skips snapshot publishing to avoid clone cascade (6fb2b78)
- Bound cache with tiered bounds for sort query acceleration (2-13x at 104M)
- Meta-index for targeted bound cache invalidation
- Benchmark harness with 20 query types + contention benchmark, memory reporting

### Phase 4: Operations
Prometheus metrics, autovac, admin API, graceful shutdown, health check. NOT yet started.

### Phase 5: Integration
Postgres WAL consumer, backfill pipeline, shadow mode, end-to-end tests. NOT yet started.

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
- When in doubt, refer to `docs/in/prepared-prompt.md` for the authoritative specification

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
Full results: `docs/benchmark-report.md`, `docs/benchmark-comparison-loading-mode.md`

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
