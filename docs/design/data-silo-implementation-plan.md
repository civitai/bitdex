---
status: ACTIVE
updated: 2026-03-28
---

# Data Silo — Implementation Plan

> Phased task list for replacing DocStore V2 with per-thread data silos.
> Design spec: `docs/design/data-silo-architecture.md`
> Benchmark plan: `docs/benchmarks/data-silo/benchmark-plan.md`

---

## Architecture Decisions (Finalized)

| Decision | Resolution | Source |
|----------|-----------|--------|
| Write approach | 8MB BufWriter, 28 per-thread files | B0 benchmarks (4.77M/s) |
| Read approach | mmap silo files, 42ns warm access | B3 benchmark |
| Phase ordering | Read-merge-write (zero overhead at hot cache) | B1 benchmark |
| Module | Clean `src/data_silo.rs`, NOT ShardStore | Josh concern #5 |
| filter_only | Keep for toolIds/techniqueIds/modelVersionIdsManual | Josh concern #4 |
| Cross-phase V1 | Per-phase append with separate index entries. Each phase writes its own fields to its own silo file for each slot. The global index stores the LATEST entry per slot. Earlier phases' data becomes dead space. At query time, only the final phase's doc (which contains all merged fields for that slot) is returned. This avoids data loss because dump phases run sequentially and the last phase to touch a slot writes the complete accumulated document. | Josh concern #3, design doc section 8 |
| Index storage | Vec in V1 (1.4GB in memory). mmap deferred to post-V1 | Josh concern #1, fits 128GB machine |
| Startup | Add warmup pass for mmap index pages | B3 finding (cold = 4us, warm = 42ns) |

---

## Phase 0: Benchmarks (COMPLETE)

- [x] **B0:** Full-scale write throughput at 107M — 4.77M/s PASS (revised goal >=4M/s) `Josh`
- [x] **B0-variants:** NVMe bandwidth analysis — confirmed 0.9 GB/s ceiling `Ollie`
- [x] **B3:** mmap index startup — 0.1ms mmap, 42ns warm PASS `Ollie`
- [x] **B1:** Phase ordering cost — zero overhead for read-merge-write PASS `Ollie`
- [ ] **B4:** Cross-silo point reads — DEFERRED (B1 validated mmap reads are free at hot cache). **Un-defer trigger:** Run B4 if Phase 3 integration reveals >100us point read latency.
- [ ] **B2:** Multi-value accumulation memory — DEFERRED (hypothetical, tags are filter_only)

---

## Phase 1: Core Module (COMPLETE)

- [x] **1.1** `DocDataFile` — append-only file with 8MB BufWriter, bulk + single-doc append `Josh`
- [x] **1.2** `IndexEntry` — 13-byte struct (u8 file_id, u64 offset, u32 length), encode/decode `Josh`
- [x] **1.3** `DocIndex` — flat Vec indexed by slot_id, persist/load binary format, auto-grow `Josh`
- [x] **1.4** `BulkDocWriter` — per-thread staging, local index accumulation, 1M pre-alloc `Josh`
- [x] **1.5** `DataSiloReader` — mmap all silo files, O(1) index lookup, get(slot_id) -> &[u8] `Josh`
- [x] **1.6** `create_bulk_writers()` + `merge_indexes()` — pool creation and index merge `Josh`
- [x] **1.7** Unit tests — 6 tests covering append, persist/load, end-to-end, encode/decode, auto-grow `Josh`

**Branch:** `worktree-josh-dump-processor` HEAD: 1ef7a1d (includes 8MB BufWriter)

---

## Phase 2: Dump Processor Integration (COMPLETE)

### 2a: General Phase (images/resources/metrics) — `Mark`

- [x] **2a.1** Create BulkDocWriter pool at dump start (28 writers via `create_bulk_writers()`) — Mark commit 3c29123
- [x] **2a.2** Thread-local writer access via AtomicUsize counter + `Vec<Mutex<BulkDocWriter>>` — Mark commit 3c29123
- [x] **2a.3** Replace `write_docstore_row_indexed` at deferred alive path with `BulkDocWriter::append` — Mark commit 3c29123
- [x] **2a.4** Replace `write_docstore_row_indexed` at main phase path with `BulkDocWriter::append` — Mark commit 3c29123
- [x] **2a.5** Serialize via existing rmp_serde path — `Vec<u8>` feeds `append(slot_id, &bytes)` — Mark commit 3c29123
- [x] **2a.6** Skip docstore write when `field_to_idx.get(target)` returns None (filter_only) — Mark commit 3c29123
- [x] **2a.7** After phase completes: `into_local_index()` on all writers, `merge_indexes()`, persist `doc_index.bin` — Mark commit 3c29123
- [x] **2a.8** Tests: unit test for writer pool creation, integration test for single-phase dump with silo output — Mark commit 3c29123, 8 tests pass

### 2b: Multi-Value Phase (tools/techniques) — `Ollie`

- [x] **2b.1** Replace `append_tuple_raw` channel writer with BulkDocWriter::append — Ollie commit ff44e9f, branch worktree-agent-ae32d72d (feat/sync-v2 base)
- [x] **2b.2** Redirect crossbeam channel batches to silo append instead of BulkWriter — Ollie commit ff44e9f, cfg-gated with `data_silo` feature
- [x] **2b.3** Handle per-slot value accumulation (tools/techniques are multi-value) — Ollie commit ff44e9f, PackedValue::Mi msgpack serialization
- [x] **2b.4** After phase completes: collect local indexes, merge, persist — Ollie commit ff44e9f
- [x] **2b.5** Tests: unit test for multi-value silo write, verify accumulated values round-trip — Ollie commit ff44e9f, 7/7 tests pass including msgpack roundtrip

### 2c: Integration Cleanup — `Mark`

- [x] **2c.1** Remove old BulkWriter usage from dump processor (behind `data_silo` feature flag) — Mark commit 898a9d4
- [x] **2c.2** Update dump processor config to pass silo_dir path — Mark commit 898a9d4
- [x] **2c.3** `cargo check` — both configurations compile: 23 tests without flag, 26 tests with `data_silo` flag — Mark commit 898a9d4

### V2: Small-Scale Dump Validation (GATE for Phase 3) — PASSED

- [x] **V2.1** Run dump processor on small dataset (1K rows) with silo writes enabled — Mark commit bbc07df, test_v2_validation_1000_rows
- [x] **V2.2** Verify silo files created with expected sizes — 4 silo files created — Mark commit bbc07df
- [x] **V2.3** Verify doc_index.bin persisted and loadable — round-trip verified — Mark commit bbc07df
- [x] **V2.4** Verify documents readable via DataSiloReader — 10 spot-checked docs deserialize correctly — Mark commit bbc07df
- [x] **V2.5** `cargo test` — 9/9 tests pass — Mark commit bbc07df

---

## Phase 3: Read Path Integration (COMPLETE) — `Mark`

- [x] **3.1** Replace `DocStore::get_v2` with `DataSiloReader::get` in query serving path — Mark commit 4e56895, `get_document()` tries silo first via mmap (42ns)
- [x] **3.2** Fallback: if silo index has no entry for slot, fall back to DocStore V2 — Mark commit 4e56895, `decode_silo_doc()` with fallback
- [x] **3.3** Startup: mmap silo files + index on server boot, add warmup pass — Mark commit 4e56895, `load_silo_reader()`
- [x] **3.4** Tests: query that returns document fields, verify content matches silo data — Mark commit ab3b587, 3 decode_silo_doc tests, 12 total tests pass

---

## Phase 4: Upsert Path Integration (COMPLETE) — `Ollie`

- [x] **4.1** Single-doc upsert: append new version to active silo file, update index entry — Ollie commit c6e1664, `DataSiloWriter::upsert()`
- [x] **4.2** `DocDataFile::open_for_append()` for steady-state writes (64KB BufWriter) — Ollie commit c6e1664, `DataSiloWriter::open()` finds highest silo file
- [ ] **4.3** Wire into ops processor (POST /ops upsert path) — DEFERRED until bulk-load validation complete. Hook point: `apply_ops_batch` at line ~903
- [x] **4.4** Tests: upsert a doc, verify new version returned by DataSiloReader, old entry becomes dead space — Ollie commit c6e1664, 3 new tests (upsert_new_doc, upsert_overwrite, upsert_dead_space), 10/10 pass

---

## Phase 5: Full-Scale Validation (107M) — Same bar as production readiness checklist

**Prerequisite:** Current DocStore V2 must pass Gate 5 first — data silos integrate AFTER that baseline.

### V5a: Dump Correctness (matches Gate 1 pattern)

- [ ] **V5a.1** 107M CSV load completes with data silos — all 6 phases (tags, images, resources, tools, techniques, metrics)
- [ ] **V5a.2** Peak RSS under 15 GB during dump
- [ ] **V5a.3** Total dump time <5 min (down from 11m21s with DocStore V2)
- [ ] **V5a.4** Document count matches: `GET /stats` alive count = expected from images.csv minus deferred
- [ ] **V5a.5** Silo file sizes reasonable: 28 files, index ~1.4 GB, total data ~21 GB

### V5b: Document Field Correctness (per civitai-field-requirements.md)

All 30 document fields verified in `GET /documents/{slot}` responses:

- [ ] **V5b.1** Core fields: url, hash, width, height present and correct (spot check 100 random docs)
- [ ] **V5b.2** Filter fields in docs: nsfwLevel, userId, postId, type match images.csv values
- [ ] **V5b.3** Multi-value fields: tagIds array matches tags.csv rows for sampled images
- [ ] **V5b.4** Multi-value fields: modelVersionIds matches resources.csv for sampled images
- [ ] **V5b.5** Enrichment fields: baseModel, poi, availability, isPublished correct for sampled docs
- [ ] **V5b.6** Computed fields: hasMeta, sortAtUnix correct for sampled docs
- [ ] **V5b.7** Metric fields: reactionCount, commentCount, collectedCount match metrics.csv

### V5c: Query Correctness (matches Gate 1 V1.2-V1.4 pattern)

- [ ] **V5c.1** `nsfwLevel eq 1` count matches images.csv count
- [ ] **V5c.2** `tagIds eq {known_tag}` count matches tags.csv (minus disabled)
- [ ] **V5c.3** `type eq "image"` uses LCS dictionary correctly
- [ ] **V5c.4** `baseModel eq "SDXL"` only from Checkpoint model types
- [ ] **V5c.5** `sort=reactionCount desc limit 10` matches metrics TSV top 10
- [ ] **V5c.6** Deferred alive: future publishedAt images not in query results
- [ ] **V5c.7** Compare 20 diverse queries against DocStore V2 baseline — same result IDs

### V5d: Performance (no regressions)

- [ ] **V5d.1** Point read latency: `GET /documents/{slot}` <=1us (warm, via mmap)
- [ ] **V5d.2** Query latency: p50/p95/p99 equal or better than DocStore V2 baseline
- [ ] **V5d.3** Loadtest at production QPS (89 QPS): no regressions in latency or error rate
- [ ] **V5d.4** Dictionary persistence: restart server, LCS queries still work

**Gate:** ALL V5a-V5d must pass. Crafted test data passing is necessary but NOT sufficient — real 107M data required.

---

## Phase 6: Production Deploy

- [ ] **6.1** PR review by Adam (design doc compliance)
- [ ] **6.2** PR approval by Justin (architectural change)
- [ ] **6.3** Docker build + tag via Aidan
- [ ] **6.4** K8s staging deploy via Aidan
- [ ] **6.5** Production deploy + monitoring via Ivanna
- [ ] **6.6** Verify dump time and read latency in production

---

## Deferred (Post-V1)

- mmap DocIndex instead of Vec (startup optimization)
- Cross-phase read-merge-write (currently overwrite-only)
- Compaction background task (dead space <1% after 10K upserts)
- Crash recovery (scan silo ops region on startup)
- Remove DocStore V2 code entirely

---

## Team

| Role | Agent | Scope |
|------|-------|-------|
| Team Lead | Edward | Coordination, plan, QA verification |
| Rust Engineer | Mark | Phase 2a (general phase integration) |
| Rust Engineer | Ollie | Phase 2b (multi-value), then Phase 3 |
| Doc Keeper | Dakota | Plan auditing, design doc updates |
| Design Review | Adam | PR compliance review |
| CTO | Tom | Escalation, approval |
| Infra | Aidan | Docker build, K8s deploy |
| Observability | Ivanna | Production monitoring |
