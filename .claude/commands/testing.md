---
description: BitDex testing guide — maps code changes to required tests, explains Rust vs Node test choices, and guides agents on running and developing E2E tests. Use proactively before committing, or when building new tests.
---

# BitDex Testing

Guide for running the right tests based on what you changed, and for developing new tests.

## Quick Reference: What Changed → What to Test

**Before committing, check this table.** Run the tests matching your changed files.

### Core Engine Changes

| Files Changed | Required Tests | Command |
|--------------|----------------|---------|
| `src/write_coalescer.rs` | Lib tests + E2E write handling | `cargo test --lib` then `node tests/e2e/e2e-write-handling.mjs --url <server>` |
| `src/executor.rs` | Lib tests + E2E write handling + E2E query operators | `cargo test --lib` then `node tests/e2e/e2e-write-handling.mjs --url <server>` and `node tests/e2e/e2e-query-operators.mjs --url <server>` |
| `src/filter.rs` or `src/sort.rs` | Lib tests + proptest + E2E write handling | `cargo test --lib && cargo test --test proptest_correctness` |
| `src/planner.rs` | Lib tests (query planning) | `cargo test --lib` |
| `src/query.rs` | Lib tests + proptest | `cargo test --lib && cargo test --test proptest_correctness` |
| `src/cache.rs` or `src/unified_cache.rs` | Lib tests + E2E pagination-overhead + E2E unified cache (if prod data) | `cargo test --lib` then `node tests/e2e/e2e-pagination-overhead.mjs --url <server>` |
| `src/concurrent_engine.rs` | Lib tests + relevant E2E test (see below) | `cargo test --lib` + whichever E2E covers your change |
| `src/mutation.rs` | Lib tests + proptest + E2E write handling | `cargo test --lib && cargo test --test proptest_correctness` |

### Persistence Changes

| Files Changed | Required Tests | Command |
|--------------|----------------|---------|
| `src/bitmap_fs.rs` | Lib tests + restart test | `cargo test --lib && cargo test --test restart_test` |
| `src/docstore.rs` | Lib tests + restart test + E2E schema versioning | `cargo test --lib && cargo test --test restart_test` then `node tests/e2e/e2e-schema-versioning.mjs --url <server>` |
| Snapshot save/restore logic | Restart test + E2E eviction + E2E save-unload | `cargo test --test restart_test` then `node tests/e2e/e2e-eviction.mjs --url <server>` and `node tests/e2e/e2e-save-unload-lazy.mjs --url <server>` |
| Schema versioning / field elision | Lib tests + E2E schema versioning | `cargo test --lib` then `node tests/e2e/e2e-schema-versioning.mjs --url <server>` |

### Eviction Changes

| Files Changed | Required Tests | Command |
|--------------|----------------|---------|
| Eviction stamps/sweep in `concurrent_engine.rs` | Eviction tests + E2E eviction | `cargo test --test eviction_stamp_gap_test && cargo test --test eviction_atomics_test` then `node tests/e2e/e2e-eviction.mjs --url <server>` |

### Server / API Changes

| Files Changed | Required Tests | Command |
|--------------|----------------|---------|
| `src/server.rs` | Build check + E2E error handling + relevant E2E | `cargo build --features server` then `node tests/e2e/e2e-error-handling.mjs --url <server>` + test the endpoint you changed |
| `src/config.rs` | Lib tests (config validation) | `cargo test --lib` |
| `src/metrics.rs` | Build check | `cargo build --features server` |

### Time Handling Changes

| Files Changed | Required Tests | Command |
|--------------|----------------|---------|
| `src/time_buckets.rs` | Time handling test | `cargo test --test time_handling_test` |
| Deferred alive logic | Time handling test | `cargo test --test time_handling_test` |

### If In Doubt

Run everything:
```bash
cargo test --lib --features server          # 409 unit tests (~10s)
cargo test --test proptest_correctness      # Property-based tests (~1s)
cargo test --test restart_test              # Persistence round-trip
node tests/e2e/run-e2e.mjs --skip-build         # All self-contained E2E tests (~10s)
```

## Running E2E Tests

E2E tests run against a live HTTP server. Two modes:

### Automated (all self-contained suites)
```bash
node tests/e2e/run-e2e.mjs                      # Build + start server + run all + cleanup
node tests/e2e/run-e2e.mjs --skip-build          # Skip cargo build (use existing binary)
node tests/e2e/run-e2e.mjs --keep                # Keep test data dir for debugging
```

### Manual (single suite against running server)
```bash
# Start a test server
cargo run --release --features server --bin server -- --port 3100 --data-dir ./test-data

# Run one suite
node tests/e2e/e2e-write-handling.mjs --url http://localhost:3100
node tests/e2e/e2e-eviction.mjs --url http://localhost:3100
node tests/e2e/e2e-unified-cache.mjs --url http://localhost:3100   # needs production data

# With structured JSON results
node tests/e2e/e2e-write-handling.mjs --url http://localhost:3100 --results-dir docs/test-results
```

### Flags (all E2E tests)
- `--url <url>` — Server address (default: http://localhost:3000)
- `--verbose` — Show all HTTP request/response details
- `--keep` — Don't delete test index after completion
- `--results-dir <dir>` — Write structured JSON results

## When to Use Rust vs Node for Tests

BitDex uses a blend of Rust and Node tests. Choose based on what you're testing:

### Use Rust (`cargo test`) when:
- **Testing bitmap operations directly** — No HTTP overhead, direct API access
- **Property-based testing** — proptest generates random inputs at high volume
- **High-throughput benchmarks** — Node can't generate enough load; Rust does 300K+ ops/s
- **Concurrency correctness** — Thread barriers, atomic ordering, ArcSwap behavior
- **Persistence round-trips** — Engine-level save/restore without server lifecycle

### Use Node (`node tests/e2e/e2e-*.mjs`) when:
- **Testing the HTTP API contract** — Request/response format, status codes, error messages
- **Testing the full write pipeline** — HTTP → mutation → coalescer → flush → snapshot → query
- **Testing server lifecycle** — Index create/delete, loading, snapshot endpoints
- **Testing observable behavior** — What a client actually sees (cache hits, pagination, eviction stats)
- **Rapid test development** — Faster iteration than Rust compile cycles

### Rule of thumb:
> If you need to verify that bitmaps are correct, use Rust.
> If you need to verify that the API behaves correctly, use Node.
> If you need both, write a Rust unit test for the bitmap logic AND a Node E2E test for the API contract.

## Developing New E2E Tests

### Structure

Follow the existing pattern in `tests/e2e/e2e-*.mjs`:

1. **Self-contained**: Create own index, insert data, test, clean up
2. **Groups**: Each test has named groups (Setup, A, B, C...) with clear assertions
3. **Index config**: Use `flush_interval_us: 50` for fast flush cycles in tests
4. **Wait for flush**: After mutations, either `sleep(300)` or poll stats for expected `alive_count`
5. **Clear cache before assertions**: `await apiDelete(/api/indexes/${INDEX}/cache)` to ensure queries hit bitmap path, not cache
6. **JSON results**: Support `--results-dir` flag for structured output

### Template

```javascript
#!/usr/bin/env node
import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const VERBOSE = process.argv.includes('--verbose');
const RESULTS_DIR = process.argv.includes('--results-dir')
  ? process.argv[process.argv.indexOf('--results-dir') + 1]
  : null;

const INDEX = 'my-test';
let passed = 0, failed = 0;
const groupResults = [];

// ... helper functions (apiPost, apiGet, apiDelete, query, upsert, etc.)
// Copy from tests/e2e/e2e-write-handling.mjs

// ... test groups (setup, testA, testB, etc.)

// ... runner with JSON output
// Copy the main() pattern from tests/e2e/e2e-write-handling.mjs
```

### Filter clause syntax

```javascript
// Eq
{ Eq: ['fieldName', { Integer: 42 }] }
{ Eq: ['fieldName', { Bool: true }] }

// In (multi-value)
{ In: ['fieldName', [{ Integer: 1 }, { Integer: 2 }]] }

// NotEq
{ NotEq: ['fieldName', { Integer: 42 }] }

// Range
{ Gt: ['fieldName', { Integer: 100 }] }
{ Gte: ['fieldName', { Integer: 100 }] }
{ Lt: ['fieldName', { Integer: 100 }] }
{ Lte: ['fieldName', { Integer: 100 }] }

// Boolean combinators
{ And: [clause1, clause2] }
{ Or: [clause1, clause2] }
{ Not: clause }
```

### Sort clause syntax

```javascript
{ field: 'reactionCount', direction: 'Desc' }
{ field: 'reactionCount', direction: 'Asc' }
```

### Common gotchas

- **Flush timing**: Mutations are async. Always wait for flush before asserting. Either `sleep(300)` or poll `stats().alive_count`.
- **Cache**: The unified cache can mask bitmap bugs. Clear cache with `DELETE /api/indexes/{name}/cache` before assertions that test bitmap correctness.
- **Multi-value In syntax**: `In` expects `[{ Integer: N }]` not `{ IntegerArray: [N] }`.
- **Sort field bits**: Must match the value range. Use `bits: 32` for u32 fields.
- **Port conflicts**: Default BitDex port is 3000. Use `--port 3100` for test servers to avoid conflicts.

## Existing Test Suites

Full descriptions of all test suites are in `docs/guide/testing.md`.

### E2E (Node, against live server)
| Suite | File | Self-contained | Tests |
|-------|------|---------------|-------|
| Write Handling | `tests/e2e/e2e-write-handling.mjs` | Yes | Insert, upsert filter/sort, delete, concurrent, multi-value |
| Eviction | `tests/e2e/e2e-eviction.mjs` | Yes | Load, idle, evict, reload, existence set |
| Query Operators | `tests/e2e/e2e-query-operators.mjs` | Yes | Range filters (Gt/Gte/Lt/Lte), NotEq, combined range+filter |
| Error Handling | `tests/e2e/e2e-error-handling.mjs` | Yes | Invalid JSON, unknown index 404, empty index, slot recycling |
| Pagination & Overhead | `tests/e2e/e2e-pagination-overhead.mjs` | Yes | Cursor pagination, cache acceleration, expansion, structural overhead |
| Save/Unload/Lazy | `tests/e2e/e2e-save-unload-lazy.mjs` | Yes | Snapshot save, query after save, mutation survival, stats integrity |
| LowCardinalityString | `tests/e2e/e2e-low-cardinality-string.mjs` | Yes | Auto-dictionary, case-insensitive, upsert, doc serving, nonexistent value, dict persistence |
| Delisting | `tests/e2e/e2e-delisting.mjs` | Yes | Availability filtering, delist/relist, blockedFor moderation, combined |
| Schema Versioning | `tests/e2e/e2e-schema-versioning.mjs` | Yes | Default elision, reconstruction, missing fields, upsert round-trip, snapshot preserves defaults |
| Unified Cache | `tests/e2e/e2e-unified-cache.mjs` | No (prod data) | Cache population, pagination, mutation/delete maintenance |

### Integration (Rust, in-process)
| Suite | File | Tests |
|-------|------|-------|
| Phase 1 | `tests/phase1_integration.rs` | Core PUT/PATCH/DELETE/query correctness |
| Proptest | `tests/proptest_correctness.rs` | Property-based random mutation + query |
| Restart | `tests/restart_test.rs` | Persist and restore round-trip |
| Time Handling | `tests/time_handling_test.rs` | Deferred alive, time buckets |
| Eviction Stamps | `tests/eviction_stamp_gap_test.rs` | Evict-reload race conditions |
| Eviction Atomics | `tests/eviction_atomics_test.rs` | ArcSwap + DashMap safety |

### Microbenchmarks (Rust, `--release --nocapture`)
| Suite | File | Measures |
|-------|------|----------|
| Bucket Diff | `tests/bench_bucket_diff.rs` | Time bucket diff cost |
| Cache Maintenance | `tests/cache_maintenance_bench.rs` | Batch bitmap AND for cache maintenance |
| Eviction Clone | `tests/eviction_clone_bench.rs` | HashMap clone cost for eviction |
| Eviction DashMap | `tests/eviction_dashmap_bench.rs` | DashMap stamping hot-path overhead |
| HashMap Keys | `tests/bench_hashmap_keys.rs` | Composite key lookup latency |

## Coverage Gaps

See the "E2E Coverage Gap Analysis" section in `docs/guide/testing.md` for 36 prioritized missing scenarios and suggested new test suites.
