# Write Path Benchmarks

**Date**: 2026-03-13
**Platform**: Windows 11 Pro, Desktop (NVMe SSD)
**Server**: bitdex-server (release build, rpmalloc)
**Test tools**: `tests/e2e/bench-write-path.mjs`, `tests/e2e/bench-write-stress.mjs`

---

## Overview

These benchmarks measure write (upsert) performance under varying cache entry counts and concurrency levels. The key question: how does cache maintenance cost scale as the number of cached query results grows?

Each write changes all fields on a random document (worst-case for cache maintenance — every field mutation triggers entry evaluation). Production writes typically change 1-2 fields.

## Stress Test Results (bench-write-stress.mjs)

5,000 documents, writes change all fields per upsert.

### Single Writer Throughput

| Cache Entries | ops/s | p50 | p95 | max |
|---:|---:|---:|---:|---:|
| 0 | 241 | 3.1ms | 10.1ms | 20ms |
| 100 | 315 | 2.4ms | 8.1ms | 15ms |
| 1,000 | 340 | 1.0ms | 10.1ms | 18ms |
| 5,000 | 455 | 710us | 11.7ms | 26ms |
| 10,000 | 709 | 540us | 6.7ms | 30ms |
| 50,000 | 1,682 | 440us | 650us | 23ms |

p50 decreases as cache entries grow because the maintenance budget cap kicks in at high entry counts, marking entries for rebuild instead of per-slot evaluation. This makes writes faster at the cost of cache misses on the next query.

### Concurrent Writer Scaling (ops/s)

| Cache Entries | c=1 | c=2 | c=4 | c=8 |
|---:|---:|---:|---:|---:|
| 0 | 254 | 322 | 389 | 464 |
| 100 | 326 | 363 | 467 | 565 |
| 1,000 | 390 | 466 | 446 | 575 |
| 5,000 | 461 | 626 | 740 | 844 |
| 10,000 | 673 | 1,208 | 1,379 | 1,596 |
| 50,000 | 1,573 | 2,945 | 3,122 | 3,084 |

### Concurrent Writer Max Latency (ms)

| Cache Entries | c=1 | c=2 | c=4 | c=8 |
|---:|---:|---:|---:|---:|
| 0 | 19 | 35 | 31 | 46 |
| 100 | 16 | 21 | 29 | 41 |
| 1,000 | 18 | 37 | 30 | 39 |
| 5,000 | 24 | 29 | 27 | 35 |
| 10,000 | 28 | 35 | 51 | 33 |
| 50,000 | 51 | 25 | 22 | 41 |

Max latency stays under 51ms at all scales and concurrency levels. Before the MetaIndex + budget cap optimization, 50K entries had 148-second max latency spikes under concurrent load.

### Correctness

All cache correctness checks pass at every scale (5/5 rounds per level). Each round: populate cache, fire 50 random writes, compare cached query results against fresh (cache-cleared) results.

### Memory

| Cache Entries | Cache Memory | Meta Memory |
|---:|---:|---:|
| 100 | 150 KB | 2.1 KB |
| 1,000 | 208 KB | 10.9 KB |
| 5,000 | 357 KB | 58.9 KB |
| 10,000 | 411 KB | 98.6 KB |
| 50,000 | 823 KB | 383.6 KB |

Cache + meta memory combined stays under 1.2 MB even at 50K entries.

## Before/After: MetaIndex + Budget Cap Optimization

Measured at 50,000 cache entries.

| Metric | Before | After | Improvement |
|---|---:|---:|---:|
| Single writer max latency | 4,247ms | 23ms | **185x** |
| c=1 max latency | 143,253ms | 51ms | **2,809x** |
| c=8 max latency | 148,097ms | 41ms | **3,612x** |
| c=8 ops/s | 30 | 3,084 | **103x** |
| Correctness at 50K | FAIL (1 mismatch) | PASS | Fixed |

### What Changed

1. **MetaIndex field-level narrowing**: Maintenance functions use `entries_for_filter_field()` / `entries_for_sort_field()` to visit only entries referencing mutated fields (O(1) bitmap lookup) instead of scanning all entries linearly.

2. **Clause-level Eq narrowing**: For the common case (Eq operator), `entries_for_clause(field, "eq", value)` narrows to entries matching the specific changed value. Non-Eq operators fall through to field-level.

3. **Maintenance budget cap**: When estimated work (affected_entries x changed_slots) exceeds 50,000, affected entries are marked for rebuild instead of per-slot evaluation. Prevents positive feedback loops under burst writes.

4. **Reverse index**: `meta_id_to_key: HashMap<CacheEntryId, UnifiedKey>` for O(1) lookup from MetaIndex results to cache entries.

## Production Relevance

Civitai production characteristics:
- ~2,500 cache entries (distinct query patterns)
- ~10-50 upserts/second
- Most upserts change 1-2 fields (reactionCount, commentCount)

At production parameters, maintenance per flush is ~15us (50 affected entries x 1 slot x 300ns). Max latency stays under 30ms. The budget cap never triggers.

## Running the Benchmarks

```bash
# Build server
cargo build --release --features server --bin bitdex-server

# Start server
target/release/bitdex-server --port 3100 --data-dir ./test-bench-data

# Write path benchmark (fan-out, throughput, mixed read/write)
node tests/e2e/bench-write-path.mjs --url http://localhost:3100

# Stress test (scaling to N caches)
node tests/e2e/bench-write-stress.mjs --url http://localhost:3100 --max-caches 50000
```

Results are written to `docs/benchmarks/results/` as timestamped JSON files.

## Known Limitations

See `docs/to-resolve/cache-maintenance-scaling.md` for gaps and future improvements:
- Clause-level narrowing only covers Eq operators
- Budget cap marks entries for rebuild (next query is a miss)
- `reconcile_bytes()` and `remove_slot_from_all()` are still O(N)
- Key cloning overhead in maintenance functions
