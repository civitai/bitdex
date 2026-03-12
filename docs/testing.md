# BitDex V2 — Testing Guide

Master reference for all test suites in the BitDex V2 project.

---

## E2E Tests (Against Live Server)

E2E tests run against a live BitDex HTTP server using Node.js. They exercise the full stack: HTTP API, write pipeline, flush cycles, bitmap engine, and query execution.

All E2E tests support `--url <url>` to override the server address (default: `http://localhost:3000`), `--verbose` for detailed request/response logging, and `--results-dir <dir>` to write structured JSON results.

### Automated Runner

```bash
node tools/run-e2e.mjs
```

Starts a fresh server on port 3100, runs all self-contained E2E suites, produces JSON results in `docs/test-results/`, prints a summary, and cleans up. Exit code 1 if any suite fails.

Options:
- `--port <port>` — Override test server port (default: 3100)
- `--skip-build` — Skip the `cargo build` step (use an existing binary)
- `--keep` — Keep the test data directory after completion (useful for debugging)
- `--verbose` — Pass `--verbose` to each E2E suite

---

### e2e-write-handling.mjs

**File:** `tools/e2e-write-handling.mjs`

**What it tests:** Write correctness — that inserts, upserts, and deletes correctly update filter and sort bitmaps, and that queries reflect the changes after flush cycles complete. Also validates concurrent read/write safety and multi-value field update correctness (old values cleared, new values set).

**Why it exists:** Validates the core write path end-to-end. Catches regressions in the upsert diff logic (where old doc is read from disk and only changed bitmaps are updated), the clean delete path (filter/sort bits cleared before alive bit), and ArcSwap snapshot consistency under concurrent reads and writes.

**Test groups:**
| ID | Name | What it validates |
|----|------|-------------------|
| Setup | Create test index | Index creation via API |
| A | Fresh insert appears in query | Inserted docs appear in filter and tag queries with correct sort order |
| B | Upsert updates filter values | Changing nsfwLevel: old value bitmap cleared, new value bitmap set |
| C | Upsert updates sort values | Changing reactionCount moves doc in sort order (first in DESC, last in ASC) |
| D | Delete removes from query | Deleted doc absent from filter and tag queries; re-insert works |
| E | Concurrent reads during writes | 20 parallel writes + 20 parallel reads: no errors, consistent doc counts |
| F | Multi-value field update | Changing tagIds: removed tag no longer matches, added tag matches, kept tag still matches |

**Self-contained:** Yes. Creates its own `write-test` index and cleans up after.

**How to run:**
```bash
# Standalone (server must be running)
node tools/e2e-write-handling.mjs --url http://localhost:3000

# With JSON results output
node tools/e2e-write-handling.mjs --url http://localhost:3000 --results-dir docs/test-results
```

**Expected output:** 7 groups pass (Setup + A-F). Exit code 0.

---

### e2e-eviction.mjs

**File:** `tools/e2e-eviction.mjs`

**What it tests:** The idle eviction lifecycle for multi-value filter fields. Verifies that lazily-loaded bitmap values become resident, go idle after no queries, get evicted by the sweep thread, and reload from disk on the next query. Also tests the existence set (nonexistent values skip disk lookup).

**Why it exists:** Eviction is a memory management feature for high-cardinality fields like tagIds (31K+ distinct values at Civitai scale). Without eviction, rarely-queried tag bitmaps consume memory indefinitely. This test validates the full lifecycle: load -> idle -> evict -> reload.

**Test groups:**
| ID | Name | What it validates |
|----|------|-------------------|
| Setup | Create test index + insert data | Index with tagIds eviction (idle_seconds=0.5), 100 docs with tags [1,2,100,101,102] |
| A | Query triggers value loading | Querying tag 100/101 makes them resident (eviction stats show resident_values >= 2) |
| B | Idle values get evicted | Pumping flush cycles for 3s while only querying tag 1: idle tags 100/101 evicted (evicted_total increases, resident_values decreases) |
| C | Re-query reloads evicted values | Querying tag 100 after eviction: results correct (10 docs), resident count increases |
| D | Nonexistent tag skipped (existence set) | Querying tag 999999: 0 results, < 5ms latency (no disk lookup) |

**Self-contained:** Yes. Creates its own `eviction-test` index with fast eviction settings and cleans up after (unless `--keep`).

**How to run:**
```bash
# Standalone
node tools/e2e-eviction.mjs --url http://localhost:3000

# With JSON results output
node tools/e2e-eviction.mjs --url http://localhost:3000 --results-dir docs/test-results
```

**Expected output:** 5 groups pass (Setup + A-D). Exit code 0.

---

### e2e-unified-cache.mjs

**File:** `tools/e2e-unified-cache.mjs`

**What it tests:** The unified cache system: population on miss, hit speedup, pagination correctness (no duplicates, correct sort order), deep pagination with cache expansion, mutation maintenance (upsert updates cached entries), delete maintenance (deleted docs removed from cache), min_filter_size threshold (narrow queries bypass cache), and multiple filter combinations.

**Why it exists:** The unified cache is the primary query acceleration layer. It caches pre-computed filter bitmaps and sort orderings to avoid re-traversing sort layers on repeated queries. This test validates cache correctness across the full lifecycle including writes that invalidate cached entries.

**Test groups:**
| ID | Name | What it validates |
|----|------|-------------------|
| A | Cache Population | Clear -> miss -> entry created -> hit -> identical results, speedup |
| B | Pagination Correctness | 3 pages: no duplicates, sort order preserved via cursor values |
| C | Deep Pagination / Expansion | 250 pages: no duplicates, no short pages, cache capacity expands to max (64000), <= 1 slow expansion page |
| D | Mutation Maintenance | Upsert doc with high reactionCount -> cache entries stay stable |
| E | Delete Maintenance | Delete doc -> no longer in results, cache entries stable |
| F | Min Filter Size Threshold | Narrow userId query (< 1000 matches) -> not cached |
| G | Multiple Filter Combinations | 3 different filter/sort combos: each gets own cache entry, hits on re-query |

**Self-contained:** NO. Requires production data loaded (Civitai dataset with nsfwLevel, reactionCount, tagIds, type, userId fields). Cannot be run by the automated runner.

**How to run:**
```bash
# Requires server with production data loaded
node tools/e2e-unified-cache.mjs --url http://localhost:3000

# Bench mode (latency percentiles)
node tools/e2e-unified-cache.mjs --url http://localhost:3000 --bench --iterations 200

# With JSON results output
node tools/e2e-unified-cache.mjs --url http://localhost:3000 --results-dir docs/test-results
```

**Expected output:** 7 groups pass (A-G). Exit code 0.

---

## Integration Tests (Rust, In-Process)

Integration tests run inside the Rust test harness using `cargo test`. They exercise the engine API directly without HTTP or the server binary. All are self-contained.

### phase1_integration.rs

**File:** `tests/phase1_integration.rs`

**What it tests:** Core engine correctness across the full mutation and query API: filter correctness vs brute-force scan, bitmap consistency after insert/update/delete sequences, sort correctness vs naive sort, cursor pagination (no gaps, no duplicates), DELETE WHERE with predicate resolution, and PATCH (partial update preserving unchanged fields).

**Why it exists:** Foundation correctness tests from Phase 1. Every bitmap operation must produce results identical to a brute-force scan of all documents. Catches off-by-one errors in sort layer traversal, stale bits from incomplete deletes, and pagination cursor edge cases.

**How to run:**
```bash
cargo test --test phase1_integration
```

---

### proptest_correctness.rs

**File:** `tests/proptest_correctness.rs`

**What it tests:** Property-based tests using proptest. Generates random documents, random mutations, and random queries. After every operation, verifies that the query engine produces identical results to a brute-force scan.

**Why it exists:** Catches edge cases that hand-written tests miss. Random input generation exercises unusual bitmap configurations (empty bitmaps, single-bit bitmaps, fully-set bitmaps) and mutation sequences (insert-delete-reinsert, update to same value, etc.).

**How to run:**
```bash
cargo test --test proptest_correctness
```

---

### restart_test.rs

**File:** `tests/restart_test.rs`

**What it tests:** ConcurrentEngine persist and restore. Verifies that engine state survives a shutdown/restart cycle: alive_count, slot_counter, filter query results, sort ordering, and deleted documents all match pre-shutdown state.

**Why it exists:** Validates the BitmapFs persistence layer. The engine saves bitmap snapshots to disk and must reconstruct identical state on restart. Catches serialization bugs, missed fields, and slot counter drift.

**How to run:**
```bash
cargo test --test restart_test
```

---

### time_handling_test.rs

**File:** `tests/time_handling_test.rs`

**What it tests:** Deferred alive lifecycle and TimeBucketManager integration. Documents inserted with future publish times become visible only after their scheduled time. Time bucket bitmaps snap to configured boundaries.

**Why it exists:** Validates Phase C features (deferred alive, time buckets, bucket snapping). Catches race conditions where documents become visible before their scheduled time and incorrect time range filter behavior.

**How to run:**
```bash
cargo test --test time_handling_test
```

---

### eviction_stamp_gap_test.rs

**File:** `tests/eviction_stamp_gap_test.rs`

**What it tests:** The "stamp gap" race condition in idle eviction. Simulates the scenario where: (1) flush thread evicts value V, (2) publishes new snapshot without V, (3) query arrives for V and triggers reload, (4) concurrent readers on old snapshot might stamp V. Verifies that stamp-based idle eviction is safe against rapid evict-reload-evict cycles.

**Why it exists:** Eviction introduces a subtle concurrency hazard between the eviction sweep, ArcSwap snapshot publishing, and lazy reload. This test uses ArcSwap + DashMap directly to simulate the race and prove the stamp-first-then-publish protocol is safe.

**How to run:**
```bash
cargo test --test eviction_stamp_gap_test
```

---

### eviction_atomics_test.rs

**File:** `tests/eviction_atomics_test.rs`

**What it tests:** Whether reader threads can safely do relaxed `AtomicU64` stores on values inside a `HashMap` behind `Arc + ArcSwap`. Validates the idle-eviction design where `FilterField` contains `HashMap<u64, AtomicU64>` for last-touched stamps.

**Why it exists:** The eviction design relies on readers stamping values with `Relaxed` ordering while the writer clones the struct. This test proves the pattern is safe and measures overhead.

**How to run:**
```bash
cargo test --test eviction_atomics_test
```

---

## Microbenchmarks (Rust)

Microbenchmarks measure specific operation costs to validate design assumptions. Run with `--release` and `--nocapture` for timing output.

### bench_bucket_diff.rs

**File:** `tests/bench_bucket_diff.rs`

**What it measures:** Time bucket diff cost at 24h / 30d / 1y scales. Measures the cost of computing bitmap diffs between old and new time bucket boundaries.

**How to run:**
```bash
cargo test --release --test bench_bucket_diff -- --nocapture
```

---

### cache_maintenance_bench.rs

**File:** `tests/cache_maintenance_bench.rs`

**What it measures:** Batch bitmap AND operations for cache live maintenance. Simulates the flush thread evaluating N cache entries against a small batch of changed slots, each with 2-3 filter clauses.

**How to run:**
```bash
cargo test --release --test cache_maintenance_bench -- --nocapture
```

---

### eviction_clone_bench.rs

**File:** `tests/eviction_clone_bench.rs`

**What it measures:** `HashMap<u64, AtomicU64>` clone cost for idle eviction. During snapshot publish, the flush thread clones FilterField containing the eviction stamps map. Threshold: clone > 1ms at 31K values = problem.

**How to run:**
```bash
cargo test --release --test eviction_clone_bench -- --nocapture
```

---

### eviction_dashmap_bench.rs

**File:** `tests/eviction_dashmap_bench.rs`

**What it measures:** DashMap stamping overhead in the query hot path. Measures the cost of stamping `DashMap` entries, which is the eviction tracking mechanism. Go/no-go threshold: < 500ns per stamp to keep < 5% overhead on 11us cached queries.

**How to run:**
```bash
cargo test --release --test eviction_dashmap_bench -- --nocapture
```

---

### bench_hashmap_keys.rs

**File:** `tests/bench_hashmap_keys.rs`

**What it measures:** HashMap lookup latency with complex composite keys (canonical filter clause keys used by the cache).

**How to run:**
```bash
cargo test --release --test bench_hashmap_keys -- --nocapture
```

---

## Running Everything

### Quick: Self-Contained E2E Tests (No Production Data)

```bash
# Automated: starts server, runs tests, cleans up
node tools/run-e2e.mjs
```

### All Rust Tests

```bash
# All integration tests + property tests
cargo test

# All integration tests in release mode (faster execution, timing-sensitive tests more reliable)
cargo test --release
```

### All Microbenchmarks

```bash
cargo test --release --test bench_bucket_diff -- --nocapture
cargo test --release --test cache_maintenance_bench -- --nocapture
cargo test --release --test eviction_clone_bench -- --nocapture
cargo test --release --test eviction_dashmap_bench -- --nocapture
cargo test --release --test bench_hashmap_keys -- --nocapture
```

### Full Suite (Requires Production Data)

```bash
# 1. Start server with production data
cargo run --release --features server --bin server -- --port 3000

# 2. Wait for data to load, then run unified cache tests
node tools/e2e-unified-cache.mjs --url http://localhost:3000

# 3. Run self-contained E2E tests against the same server
node tools/e2e-write-handling.mjs --url http://localhost:3000
node tools/e2e-eviction.mjs --url http://localhost:3000
```

### JSON Results

All E2E tests support `--results-dir <dir>`. When provided, each test writes a structured JSON file:

```bash
node tools/e2e-write-handling.mjs --results-dir docs/test-results
node tools/e2e-eviction.mjs --results-dir docs/test-results
node tools/e2e-unified-cache.mjs --results-dir docs/test-results
```

The automated runner (`tools/run-e2e.mjs`) writes a combined results file to `docs/test-results/e2e-{timestamp}.json`.
