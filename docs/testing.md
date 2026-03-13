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

### e2e-query-operators.mjs

**File:** `tools/e2e-query-operators.mjs`

**What it tests:** Query operators that had zero test coverage through the HTTP API path: range filters (Gt, Gte, Lt, Lte), NotEq, and combined range+filter queries with sorted output.

**Why it exists:** Range operators (`range_scan()` in executor.rs) iterate all stored filter values and union matching bitmaps. This is a fundamentally different code path from Eq/In lookups. NotEq uses `alive - eq_bitmap` which depends on the alive bitmap being correct. Both paths had zero coverage at any level — no unit tests, no property tests, no integration tests.

**Test groups:**
| ID | Name | What it validates |
|----|------|-------------------|
| A | Range Filters | Gt/Gte/Lt/Lte on integer `score` field. 10 docs with scores 10-100. Verifies exact result counts and correct ID inclusion/exclusion at boundaries. |
| B | NotEq Filter | NotEq on string `category` field. Verifies excluded category absent, non-existent value returns all docs. |
| C | Range + Filter Combination | And(Gte, Lte) for range window. Verifies result set correctness and sort order (Desc top-3 and Asc bottom-3). |

**Self-contained:** YES. Creates own index, inserts data, tests, cleans up.

**How to run:**
```bash
node tools/e2e-query-operators.mjs --url http://localhost:3100
node tools/e2e-query-operators.mjs --url http://localhost:3100 --results-dir docs/test-results
```

**Expected output:** 4 groups pass (Setup + A-C). Exit code 0.

---

### e2e-error-handling.mjs

**File:** `tools/e2e-error-handling.mjs`

**What it tests:** HTTP error handling and edge cases with zero server-level test coverage: malformed requests, unknown index 404s, empty index queries, and slot recycling (the "clean deletes" design principle end-to-end).

**Why it exists:** The server (`src/server.rs`) has zero tests. A panic on bad input is a production incident. Slot recycling (delete → reinsert same ID) verifies that clean deletes fully clear all filter/sort bitmap bits — if stale bits leak, queries return ghost results from deleted documents.

**Test groups:**
| ID | Name | What it validates |
|----|------|-------------------|
| A | Invalid JSON / Malformed Requests | Garbage body, empty object, wrong-type filters field. Asserts non-500 responses. |
| B | Unknown Index Name | GET stats, POST query, PUT docs to nonexistent index. All expect 404. |
| C | Empty Index Queries | Create index with 0 docs. Query with no filters, with sort, with Eq filter. All expect 0 results, no crash. |
| D | Slot Recycling | Insert doc → delete → verify clean (no stale bits) → reinsert same ID with new values → verify old values fully gone, new values present, sort reflects new score. |

**Self-contained:** YES. Creates own index, inserts data, tests, cleans up.

**How to run:**
```bash
node tools/e2e-error-handling.mjs --url http://localhost:3100
node tools/e2e-error-handling.mjs --url http://localhost:3100 --results-dir docs/test-results
```

**Expected output:** 4 groups pass (A-D). Exit code 0.

---

### e2e-pagination-overhead.mjs

**File:** `tools/e2e-pagination-overhead.mjs`

**What it tests:** Cursor pagination correctness, unified cache acceleration, cache expansion on deep pagination, structural memory overhead, and filtered cursor pagination. Produces quantitative measurements (bytes per entry, bytes per doc, hit/miss latency, capacity progression) for regression tracking.

| Group | Name | Key Assertions |
|-------|------|---------------|
| Setup | Create test index | 50 docs inserted (25 per category), flush confirmed |
| A | Cursor pagination correctness | 5 pages of 10, no gaps/duplicates, correct DESC sort order |
| B | Cache hit acceleration | Hit faster than miss, stats show entries/hits/misses |
| C | Cache expansion on deep pagination | Cardinality reaches 50, has_more=false after full traversal |
| D | Structural overhead | Bytes per doc < 1000, cache memory populated |
| E | Filtered cursor pagination | category=1 only (25 docs), no category=2 leakage, correct sort order |

**How to run:**
```bash
node tools/e2e-pagination-overhead.mjs --url http://localhost:3100
node tools/e2e-pagination-overhead.mjs --url http://localhost:3100 --results-dir docs/test-results
```

**Expected output:** 6 groups pass (Setup, A-E). Exit code 0. Measurements logged for regression tracking.

---

### e2e-save-unload-lazy.mjs

**File:** `tools/e2e-save-unload-lazy.mjs`

**What it tests:** The save-snapshot lifecycle end-to-end: bitmap snapshot save via the `/api/indexes/{name}/save` endpoint, query correctness before and after snapshot, mutation survival after snapshot, and stats integrity throughout. Validates that save_and_unload() preserves query behavior and that the lazy reload path works correctly after unloading.

**Why it exists:** The save-and-unload path (`save_and_unload()` in concurrent_engine.rs) is complex — it saves bitmaps zero-copy via `fused_cow()`, then builds a new InnerEngine with empty field shells and marks fields pending for lazy reload. A bug in the lazy-value field skip condition (unconditionally skipping multi-value fields during save) was caught by E2E eviction tests. This suite directly validates the save lifecycle to prevent similar regressions.

**Test groups:**
| ID | Name | What it validates |
|----|------|-------------------|
| Setup | Create test index + insert data | 100 docs with category, tags, score fields; flush confirmed |
| A | Snapshot save + stats verification | Warmup query, save endpoint returns 200, alive_count unchanged |
| B | Query correctness after snapshot | Category filter + multi-value tag filter produce exact expected IDs and sort order |
| C | Mutation survival after snapshot | Upsert changes score, sort order reflects change, original value restored |
| D | Stats integrity | alive_count, flush_cycle, slot_count, cache stats all valid |

**Self-contained:** Yes. Creates its own `save-unload-test` index and cleans up after (unless `--keep`).

**How to run:**
```bash
node tools/e2e-save-unload-lazy.mjs --url http://localhost:3100
node tools/e2e-save-unload-lazy.mjs --url http://localhost:3100 --results-dir docs/test-results
```

**Expected output:** 5 groups pass (Setup + A-D). Exit code 0.

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

---

## E2E Coverage Gap Analysis

Prioritized list of missing E2E test scenarios identified via codebase analysis (2026-03-12). Build new test suites from the top down.

### HIGH PRIORITY — Query & Filter Paths

| # | Scenario | Why it matters |
|---|----------|---------------|
| 1 | **NotEq filter** | Uses `alive - eq_bitmap`; stale alive or wrong eq = wrong results |
| 2 | **NotIn filter** | `alive - In(values)` union; overlapping values could break |
| 3 | **Not(And(...)) nested negation** | Recursive evaluation of complex clause trees |
| 4 | **Or with empty branches** | Branch returning empty bitmap must not corrupt union |
| 5 | **Range filters (Gt/Gte/Lt/Lte)** | `range_scan()` iterates field bitmaps; VersionedBitmap fusing untested |
| 6 | **Offset pagination** | `fetch_limit = limit + offset`, then `split_off(offset)`. Off-by-one risk at boundaries |
| 7 | **Offset + sort direction** | Offset applied after sort; wrong iteration order = silent data corruption |

### HIGH PRIORITY — Persistence & Startup

| # | Scenario | Why it matters |
|---|----------|---------------|
| 8 | **Full restart cycle** | Load → save → stop → restart → query: core production scenario |
| 9 | **Cursor persistence across restart** | pg-sync loses CDC position if cursors don't survive |
| 10 | **include_docs after restore** | Docstore/bitmap mismatch = wrong documents returned |
| 11 | **Index delete → recreate** | Leftover state, file handle leaks, old data bleeding through |
| 12 | **Loading mode transitions** | enter/exit loading mode + cache invalidation correctness |

### HIGH PRIORITY — Edge Cases & Error Handling

| # | Scenario | Why it matters |
|---|----------|---------------|
| 13 | **Invalid JSON / malformed requests** | Should return 400, not panic or 500 |
| 14 | **Unknown index name** | All endpoints should return 404 cleanly |
| 15 | **Duplicate index creation** | Should return 409, not corrupt existing index |
| 16 | **Empty index queries** | 0 docs: div-by-zero, nil pointer, or incorrect total_matched |
| 17 | **Single document** | Off-by-one in pagination, sort with 1 item |
| 18 | **Max page size enforcement** | limit > max_page_size should be capped |
| 19 | **Slot recycling (delete → reinsert same ID)** | "Clean deletes" principle: stale bits must not leak |
| 20 | **Type mismatch in filter values** | String value on integer field should error, not silently return 0 |

### MEDIUM PRIORITY

| # | Scenario | Why it matters |
|---|----------|---------------|
| 21 | Cursor pagination during concurrent filter mutation | Cursor slot_id may leave filter bitmap between pages |
| 22 | Sort without filter (all docs) | alive bitmap correctness as universe |
| 23 | Metrics accuracy under mutations | Prometheus counters must stay in sync |
| 24 | Empty value Eq (non-existent key) | Returns empty bitmap; VersionedBitmap fusing correctness |
| 25 | In() with duplicate values | Union should be idempotent |
| 26 | Boolean filter field | Bool→0/1 mapping correctness |
| 27 | Lazy field first-query latency | First touch loads from disk; second should be fast |
| 28 | Concurrent snapshot + writes | save_snapshot during active mutations |
| 29 | Load status transitions | loading → complete progression |
| 30 | Concurrent load requests | Second load while first running should reject |

### LOW PRIORITY

| # | Scenario | Why it matters |
|---|----------|---------------|
| 31 | Range filter on unknown field | Should error cleanly |
| 32 | Deeply nested clause trees | Stack overflow protection |
| 33 | include_docs with offset | Docs must match offset-adjusted IDs |
| 34 | Sort-only cache invalidation skip | Sort field change without filter change |
| 35 | Index creation with invalid config | Negative max_page_size, zero-bit sort fields |
| 36 | Delete of non-existent slot | Idempotent or 404, not panic |

### Suggested New E2E Test Suites

Based on the gaps above, the following new test files would close the most critical gaps:

1. **`tools/e2e-query-operators.mjs`** — Gaps 1-7: All filter operators (NotEq, NotIn, Not, Or, Range), offset pagination, sort direction + offset interaction
2. **`tools/e2e-persistence.mjs`** — Gaps 8-12: Full restart cycle, cursor persistence, include_docs after restore, index lifecycle, loading mode
3. **`tools/e2e-error-handling.mjs`** — Gaps 13-20: Invalid input, unknown index, empty index, single doc, max page size, slot recycling, type mismatches
4. **`tools/e2e-metrics.mjs`** — Gaps 23, 27: Prometheus counter accuracy, lazy loading metrics
