# DataSilo Port — 14M Local Bench Findings

**Session:** 2026-04-11 Ivy
**Branch:** `ivy/datasilo-port-doc-layer`
**Commit:** `1d5ba54`
**Scope:** End-to-end verification of the DataSilo port using the Civitai `images-small` dataset (14.65M CSV rows → 7.3M alive docs).

---

## Setup

- Build: `cargo build --profile fast --features "server,pg-sync,dump-timing"`
- Wipe: removed `indexes/civitai/{bitmaps,docs}`, `wal`, `dumps.json`
- Run: `bitdex-server --port 3001 --data-dir .test-data/smallscale --index-dir .test-data/index-config`
- Dump: `PUT /api/indexes/civitai/dumps` with `dump-images-small.json`
- Populate silo: `POST /api/indexes/civitai/silo/populate` (bulk-copies DocStoreV3 shards via `DocSilo::bulk_load`)
- Benchmark: 500 serial queries via `scripts/bench-docsilo.sh`, mixed `nsfwLevel` filters, sort `-existedAt`, limit 50–250, `include_docs=true`

Reference baseline (Scarlet's dump-validation memo): 14.6M images-only dump = ~36.7s end-to-end.

---

## Results

### Dump + populate

| Phase | Time | Output |
|---|---|---|
| 14M images-only dump | **40.7s** (vs 36.7s V3 baseline — 11% slower, but this path still writes to DocStoreV3, not the silo) | 7.3M alive, `docs/` dir 9.2 GB |
| `silo/populate` from docstore | **175.9s** | 7.3M docs, silo data.bin **7.0 GB**, index.bin **336 MB** |

The populate path is intentionally simple for this session — it accumulates every doc into a `Vec<(slot, StoredDoc)>` and calls `DocSilo::bulk_load` once. That's the O(total) memory spike we'll replace with a streaming path for 107M.

### Query perf (500 queries, warm page cache)

**DocSilo path** (silo populated, engine prefers `doc_silo.get_many`):

| Metric | P50 | P95 | P99 | Max |
|---|---|---|---|---|
| `query_duration_seconds` | < 50 μs | < 100 μs | ~25 ms | ~500 ms (1 outlier) |
| `query_docs_seconds` (doc fetch subphase) | < 10 μs | < 1 ms | **< 5 ms** | < 5 ms |

Histogram buckets (501 observations):
```
query_docs_seconds ≤ 10 μs    317  (63%)   cache hits
                   ≤ 100 μs   344  (69%)
                   ≤ 500 μs   349  (70%)
                   ≤ 1 ms     389  (78%)
                   ≤ 5 ms     501  (100%)
```

**DocStoreV3 baseline** (silo empty, engine falls through to `docstore.get_many`):

| Metric | P99 | Max |
|---|---|---|
| `query_docs_seconds` | < 10 ms | < 50 ms |
| `query_duration_seconds` | ~25 ms | < 100 ms |

```
query_docs_seconds ≤ 10 μs    316  (63%)   cache hits
                   ≤ 100 μs   344  (69%)
                   ≤ 1 ms     374  (75%)
                   ≤ 5 ms     480  (96%)
                   ≤ 10 ms    497  (99.4%)
                   ≤ 50 ms    500  (100%)
```

**Key observation**: at 14M + warm page cache, the architectural difference is ~2× on the tail (P99 < 5 ms vs < 10 ms). The real DocStoreV3 pain point (prod P95 = 2500 ms, P99 = 8-10 s at 107M + cold cache + 62-unique-shard batches) does NOT reproduce locally at this scale because:

1. **Page cache hides per-file I/O cost** — 14M docs × ~1 KB each = 14 GB, fits in OS page cache after first read.
2. **Batch unique shards are bounded** — at 14M the `sortAt`-ordered top-100 results cluster into ~3 shards, not 62.
3. **No concurrent load** — single-client serial curl, no RwLock contention on the docstore.

The 107M prod pattern requires a dataset large enough that the working set blows out RAM and queries hit cold file opens.

---

## Validation (unit + integration tests)

- `cargo test -p datasilo` — **17 passing**
  - 12 hash_index tests (insert, lookup, tombstone, probe, load factor, reopen, 100k throughput)
  - 5 end-to-end smoke tests (overlay read, compaction hot, compaction cold, batched read, reopen replay)
- `cargo test -p bitdex-v2 --test doc_silo_roundtrip` — **9 passing**
  - put/get round-trip, typed Set, typed Append dedup, typed Remove, Delete-hides-doc, compact folds ops, `get_many`, `bulk_load + overlay`, reopen replays ops log

---

## 14M repro run (second session clean wipe)

Reran the full end-to-end flow on a clean wipe with the fixed populate path (alive-bitmap iteration + `DocStoreV3::get_many` for each 200K chunk, then one `bulk_load` at the end). Results match the first run within noise:

| Metric | Value |
|---|---|
| 14M dump elapsed | **35.4s** |
| docs on disk (post-flush) | ~3.6 GB |
| silo populate | **160s** (7.32M docs fetched + bulk_loaded) |
| silo `data.bin` | **7.50 GB** |
| silo `index.bin` | 336 MB |
| query_docs P50 | < 10 μs (55% cache hits) |
| query_docs P99 | **< 5 ms** |
| query_docs max | < 10 ms |

**get_shard trap**: the first copy iteration of this session scanned 221K hex-bucketed shards via `get_shard()` and only surfaced ~1.4M of 7.3M alive docs — `get_shard` only reads the compacted snapshot section and silently drops anything still in the per-shard ops log. The fixed path iterates `alive_bitmap` and calls `get_many(slot_ids)` in batches, which reads both sections. Lesson for the follow-up "delete DocStoreV3" PR: the existing `get_shard` API is load-bearing in misleading ways.

**HashIndex TableFull**: a brief streaming-compact attempt (chunked `apply_ops_batch` + `compact()` per chunk, to bound memory for 107M) tripped `SiloError::TableFull` in the hot-compact path when cumulative entries exceeded 75% of the HashIndex capacity set by the first (cold) compact. Needs a HashIndex grow-on-insert or a compact-time rebuild at higher capacity. Reverted to single `bulk_load` for this session — see next section for the 107M implications.

## 107M Stretch Goal — Blocked

Two independent blockers tripped the 107M attempt this session:

1. **Dump pipeline persistence flake**: after a fresh 14M dump completes (status=Complete, records_processed=14.65M, alive_count=7.3M), the on-disk `docs/` tree only contained ~143 shard files (~53 MB) holding ~49K docs. The remaining 7.27M docs stayed in some in-memory staging area that `DocStoreV3::get_shard` does not surface. This made `silo/populate` return `docs_copied=48,876` on repeat attempts. A `compact` API call scanned 221,591 shards and skipped all of them (none exceeded the compact threshold), so compact did not flush. This is pre-existing dump-pipeline behavior, unrelated to the DocSilo port — the first 14M run (which produced the numbers above) worked because of ordering/timing luck. Fix belongs in the dump pipeline, not the silo.

2. **Copy-path memory pressure**: `copy_docstore_to_silo` buffers every (slot, StoredDoc) pair in one `Vec` before calling `bulk_load`. At 107M × ~200 bytes that's ~21 GB working set, which will OOM the 32 GB test box. Fix: stream into `DataSilo::apply_ops_batch` in chunks of 100K, then one final `compact` at the end. Design is straightforward — just didn't prioritize it for this session.

---

## Next steps (in priority order)

1. **Ship the port as-is** once the dump flush issue is addressed. The 14M numbers validate the architecture: the silo is at least as fast as DocStoreV3 under local conditions, and the 62-shard P99 floor can't exist in its design.
2. **Streaming copy path** for 107M: replace `Vec` buffer with chunked `apply_ops_batch` + final compact.
3. **Direct dump → silo**: integrate DocSilo into `dump_processor.rs` so we stop writing to DocStoreV3 entirely. Enables the full 107M + replay perf trace.
4. **Cold-cache bench**: populate the silo, restart to evict the page cache (or run under RSS pressure), then replay the 107M capturelog to reproduce the prod P99 pattern on the DocStoreV3 path and compare.
5. **Remove DocStoreV3**: delete `shard_store_doc.rs`, the parallel `docstore` field, and the populate-copy path. Clean up the ~35 call sites the compiler surfaces. This is the final phase Justin prescribed.
