---
name: dev-guide
description: Development guide covering project phases, what's built vs not, and workflow expectations (running tests, creating E2E tests, updating docs). Use when starting new work, onboarding, or checking project status.
disable-model-invocation: false
user-invocable: true
---

# BitDex Development Guide

## Project Status

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
- Loading mode for bulk inserts — skips snapshot publishing to avoid clone cascade
- Bound cache with tiered bounds for sort query acceleration (2-13x at 104M)
- Meta-index for targeted bound cache invalidation
- Idle eviction for high-cardinality multi-value fields (tagIds)
- Fused parse+bitmap loader pipeline (320-460K/s sustained)
- Unified cache live maintenance with MetaIndex clause-level narrowing

### Phase 4: Operations — PARTIAL
- Prometheus metrics, HTTP server, Web UI, Grafana dashboard — COMPLETE
- Rebuild from docstore (`--rebuild` flag) — COMPLETE
- Autovac, admin API, graceful shutdown — NOT yet started

### Phase 5: Integration — IN PROGRESS
- NDJSON bulk loading, upsert/delete endpoints, E2E test suite — COMPLETE
- Postgres CDC sync (pg-sync) — COMPLETE (external tool)
- Shadow mode comparison — IN PROGRESS

### Future Roadmap (NOT V2 Scope — Do Not Build)
LSH vector similarity, Postgres extension, log encoding, OpenSearch/Meilisearch parsers, visual bitmap explorer, multi-index support, shared memory hot restarts.

## Workflow Expectations

### After Editing Code

1. **Run the corresponding tests.** Use `/testing` to see which tests map to which files. At minimum, run `cargo test --lib` for any Rust change.

2. **Run E2E tests for server/API changes.** If you touched `src/server.rs`, `src/executor.rs`, `src/docstore.rs`, or any file in the write pipeline, run the relevant E2E suite. The `/testing` skill has the full mapping table.

3. **Create new E2E tests for complex features.** If you're building something with multi-step logic (e.g., a new API endpoint, a new mutation path, a new caching behavior), write a Node E2E test. Follow the pattern in `tests/e2e/e2e-*.mjs`. Self-contained tests that create their own index, insert data, assert, and clean up.

4. **Update API docs** (`docs/guide/api.md`) when adding or changing HTTP endpoints. Include request/response examples.

5. **Update config docs** (`docs/guide/config-schema.md`) when adding config fields.

6. **Update `bitdex.default.toml`** when adding or changing server CLI parameters. This file is embedded into the binary at compile time via `include_str!()` and written out as `bitdex.toml` on first run. Every `--flag` the server accepts should have a corresponding entry in this file.

7. **Update design docs** when changing architecture. See `/architecture` for details.

### Running the Server Locally

```bash
# Test server (port 3001 to avoid conflicts with model-share dev server on 3000)
cargo run --release --features server --bin bitdex-server -- --port 3001 --data-dir ./data
```

### Test Data Directory

All test data goes under `.test-data/` in the project root (gitignored). **Never create test data directories in the project root.**

- `.test-data/e2e/` — automated E2E runner
- `.test-data/manual/` — manual test server runs
- `.test-data/bench/` — benchmark data

### Benchmarks & Performance

- Run `/microbench` for throwaway performance experiments (use the scratch crate, not `tests/`)
- Run `/perf` for memory measurement baselines and methodology
- Benchmark suite must run on every PR — >10% regression gets flagged
