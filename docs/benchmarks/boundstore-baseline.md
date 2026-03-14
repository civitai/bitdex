# BoundStore — Performance Baselines

**Last Updated**: 2026-03-13
**Platform**: Windows 11 Pro, Desktop (NVMe SSD)

> Initial baselines from criterion microbenchmarks and smoke benchmark.
> Large-scale (105M) baselines will be added after production data testing.

---

## 1. Serialization Performance (criterion, isolated I/O)

### Shard Round-Trip

| Entries | File Size | Bytes/Entry | Write | Read |
|--------:|----------:|------------:|------:|-----:|
| 100 | 928 KB | 9,282 | 6.4ms | 1.8ms |
| 1,000 | 9.3 MB | 9,282 | 37ms | 43ms |
| 5,000 | 46.4 MB | 9,282 | 205ms | 168ms |

Bytes/entry dominated by 4K-cardinality roaring bitmaps. At production scale
with real Civitai data, entry bitmaps will be smaller (typical cache entry has
~1-4K slots in the bounded top-K set).

### Meta File Round-Trip

| Entries | File Size | Bytes/Entry | Write | Read |
|--------:|----------:|------------:|------:|-----:|
| 100 | 6.0 KB | 60 | 1.7ms | 96µs |
| 1,000 | 60 KB | 60 | 2.4ms | 456µs |
| 10,000 | 603 KB | 60 | 8.2ms | 5.0ms |

Meta is tiny and fast. Even 100K entries = 6 MB, loads in ~50ms.

### Meta-Index Lookup (in-memory, no I/O)

| Registrations | Filter Field Lookup | Sort Field Lookup |
|--------------:|--------------------:|------------------:|
| 1,000 | 69ns | 296ns |
| 10,000 | 61ns | 408ns |
| 100,000 | 91ns | 673ns |

Essentially free at any scale. Filter lookup is a HashMap get on field name.
Sort lookup combines two HashMap gets (Asc + Desc) with bitmap OR.

### Fragmented Shard Rewrite (1K entries, read-modify-write)

| Tombstoned % | Rewrite Time | Notes |
|-------------:|-------------:|-------|
| 0% (no dead) | 71ms | Full read + full write |
| 25% dead | 55ms | 750 live entries |
| 50% dead | 41ms | 500 live entries |
| 75% dead | 24ms | 250 live entries |

Rewrite cost scales linearly with live entry count, not dead entry count.

---

## 2. End-to-End Smoke Benchmark (1K docs, 20 cache entries)

Run: `node tests/e2e/e2e-boundstore-smoke.mjs`

| Metric | Value |
|--------|------:|
| Meta bytes/entry | 63 |
| Disk bytes/entry | 517 |
| Shard files | 4 |
| Total disk footprint | 10.3 KB |
| Persist time (incl. merge wait) | ~2s |
| Restore time (server startup) | ~500ms |
| Meta entries restored | 20 |
| Tombstones from 200 mutations | 20 |
| Warm p50 (after restore) | ~1.0ms |
| Cold p50 (no cache) | ~1.0ms |

At 1K docs, cache speedup is negligible (queries already sub-ms). The value
of BoundStore shows at 105M+ scale where cold sort traversal is 13ms+ and
warm cache hits are <3ms.

---

## 3. Regression Thresholds

### Local Smoke (deterministic synthetic, tight)

| Metric | Warn | Fail |
|--------|------|------|
| meta load time | >20% regression | >35% |
| meta write time | >20% | >35% |
| meta bytes/entry | >10% increase | >20% |
| shard load ms/MB | >20% regression | >35% |
| shard write ms/MB | >20% | >35% |
| shard bytes/entry | >15% increase | >25% |
| write amplification factor | >25% increase | >50% |
| tombstone cleanup ratio | <90% of expected | <75% |
| restore hit-rate delta | >-2 pp | >-5 pp |
| restored first-hit vs fresh warm | >25% slower | >50% |

### Large-Scale Session (105M, same-session comparison)

| Metric | Investigate |
|--------|------------|
| meta load/write | >50% regression |
| shard load/write ms/MB | >30-40% |
| disk bytes/entry | >20% |
| write amplification | >2x baseline |
| restore hit-rate delta | >3 pp drop |
| first query after restore | >2x baseline |
| tombstone backlog | sustained growth over merge cycles |
| fragmentation ratio | >30-40% dead bytes in hot shards |

---

## 4. How to Run

### Criterion Microbenchmarks (isolated subsystem)

```bash
cargo bench --bench bound_store_bench
```

### Smoke Benchmark (full server lifecycle, ~25s)

```bash
node tests/e2e/e2e-boundstore-smoke.mjs [--verbose]
```

### E2E Correctness Tests (24 assertions, ~45s)

```bash
node tests/e2e/e2e-cache-persistence.mjs [--verbose]
```

---

## 5. Prometheus Metrics

Available at `/metrics` endpoint when server is running:

| Metric | Type | Description |
|--------|------|-------------|
| `bitdex_boundstore_meta_entries` | gauge | Entries in meta-index |
| `bitdex_boundstore_tombstones` | gauge | Current tombstone count |
| `bitdex_boundstore_pending_shards` | gauge | Shards awaiting lazy load |
| `bitdex_boundstore_disk_bytes` | gauge | Bounds directory size |
| `bitdex_boundstore_shard_loads_total` | gauge | Cumulative shard loads |
| `bitdex_boundstore_tombstones_created_total` | gauge | Cumulative tombstones created |
| `bitdex_boundstore_tombstones_cleaned_total` | gauge | Cumulative tombstones cleaned |
| `bitdex_boundstore_entries_restored_total` | gauge | Cumulative entries loaded from shard |
| `bitdex_boundstore_bytes_written_total` | gauge | Cumulative bytes written |
| `bitdex_boundstore_bytes_read_total` | gauge | Cumulative bytes read |
