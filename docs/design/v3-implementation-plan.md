---
status: APPROVED — Justin greenlit 2026-03-30. Phase 1 in progress.
created: 2026-03-30
revised: 2026-03-30
author: Edward (team lead)
reviewers: Dakota (APPROVED), Tom (APPROVED), Scarlet (7 lessons incorporated)
design: docs/design/v3-unified-mmap-architecture.md
---

# BitDex V3 — Implementation Plan

> Task breakdown for building the unified DataSilo architecture.
> Design doc: `docs/design/v3-unified-mmap-architecture.md`

---

## Team

| Role | Agent | Responsibilities |
|------|-------|-----------------|
| Team lead | **Edward** | Coordination, reviews, design questions, cache silo |
| Engineer 1 | **Mark** | DataSilo crate, doc integration, roaring-rs fork |
| Engineer 2 | **Ollie** | Bitmap integration, executor port, query path |
| Doc keeper | **Dakota** | Plan standards, doc reviews, CLAUDE.md updates |
| Reviewers | **Tom**, **Justin** | Architecture approval, PR reviews |

---

## Branch + Workspace Setup

**Branch:** `feat/v3` off main
**Workspace:** Cargo workspace with `crates/datasilo/` + `src/v3/`

---

## Phase 0: Pre-flight Benchmark (Mark)

> Goal: build DataSilo crate, bulk load 1M synthetic entries, verify numbers match Exp 6.
> Catches performance regressions before wiring anything to BitDex.
> (Per Scarlet's recommendation from sync-v2 experience.)

### 0.1 Synthetic benchmark — Mark
- [ ] After Phase 1.6 (bulk_load): generate 1M synthetic entries (~230B each)
- [ ] Bulk load into DataSilo, measure throughput (target: ≥5M entries/sec)
- [ ] Read random entries, measure latency (target: ≤1μs p50)
- [ ] Run compaction, verify data integrity
- [ ] Compare numbers against Exp 6 baseline (5.53M/s write, 1μs read)
- [ ] If >10% regression: investigate before proceeding to Phase 2

**Phase 0 deliverable:** benchmark confirms DataSilo crate matches experiment performance.

---

## Phase 1: DataSilo Crate (Mark)

> Goal: standalone generic mmap'd key-value store with tests.
> No BitDex dependencies. Publishable as its own crate.

### 1.0 Config defaults — Edward
- [ ] Set bitmap buffer ratios: dense=5% (safety margin), sparse=50% (from Mark Exp 7)
- [ ] Set shard counts: docs=8, bitmaps=per-field packed (~28), cache=4 (from Ollie analysis)
- [ ] Set compact thresholds: 20% dead space default, configurable
- [ ] Document in SiloConfig with comments explaining rationale

### 1.1 Crate scaffold — Mark ✅ VERIFIED (reviewer confirmed 424df6c)
- [x] Create `crates/datasilo/` with Cargo.toml (no BitDex deps)
- [x] Add to workspace in root Cargo.toml
- [x] Define `SiloKey` trait (Clone+Eq+Hash+Send+Sync), `SiloConfig` struct with presets (file-loading deferred to pre-Phase 2)
- [x] Define `DataSilo<K>` public API: `open`, `get`, `put`, `delete`, `bulk_load`, `compact`

### 1.2 Index table — Mark ✅ VERIFIED (reviewer approved 49126e1, 9 tests, unsafe sound)
- [x] mmap'd file: `key_index → (u32 offset, u32 length, u32 allocated)` = 12 bytes/entry
- [x] Shard ID is implicit: `key % num_shards` (not stored in index — saves space)
- [x] Versioned header: DSIX magic + u8 version + u64 entry_count = 16-byte header
- [x] `get_entry(key)` → index lookup → (offset, length)
- [x] `update_entry(key, offset, length, allocated)` → mmap write
- [x] Tests: create, read, update, boundary conditions, persistence, growth, corruption detection

### 1.3 Data shards — Mark ✅ VERIFIED (reviewer approved 546813d+dcb2724, 27 tests, two-path write fixed)
- [x] Configurable shard count (`num_shards` in SiloConfig)
- [x] `key % num_shards` → shard file
- [x] Packed variable-size entries with configurable buffer ratio
- [x] `write_entry(shard, offset, data)` → mmap memcpy
- [x] `read_entry(shard, offset, length)` → mmap slice
- [x] Two-path write logic: in-place (fits allocated) vs relocating (exceeds buffer) — fixed in dcb2724
- [x] `ShardMeta`: track `dead_bytes`, `total_bytes` per shard
- [x] Tests: write + read round-trip, in-place update (fits), relocating update (exceeds buffer), dead space tracking + 8 integration tests

### 1.4 Ops log — Mark ✅ VERIFIED (reviewer approved 1d27f3e+d137fa3, 37 tests, fsync+rotation+drop_consumed fixed)
- [x] Per-silo append-only file
- [x] Format: `[u8 op_type][u32 payload_len][payload][u32 crc32]`
- [x] `append_op(op)` → write + fsync (sync_data after flush)
- [x] `replay_ops()` → iterate from start, verify CRC32, yield ops
- [x] `truncate()` → reset after compaction
- [x] **Generational rotation:** 16MB threshold, drop_consumed(up_to_gen) removes old files (Scarlet #6)
- [x] Tests: append, replay, corruption (CrcMismatch assert), crash recovery, rotation (17K×1KB), drop_consumed

### 1.5 Compaction — Mark ✅ VERIFIED (reviewer approved 8e52803+99974e8, 41 tests, atomic temp-file swap)
- [x] Trigger: `dead_bytes / total_bytes > compact_threshold`
- [x] Rewrite shard: pack live entries contiguously with fresh buffers
- [x] Update index table entries for affected keys
- [x] Atomic swap: write to .tmp, fsync, rename, re-mmap (crash-safe)
- [x] Reset `dead_bytes = 0`
- [x] Tests: dead space reclaim, data integrity, empty shard, total bytes reduction

### 1.6 Bulk load — Mark ✅ VERIFIED (reviewer approved 4d363d0, multi-threaded via thread::scope)
- [x] Multi-threaded: each thread writes to its shard region (disjoint access via split_first_mut)
- [x] Build index table in one pass after all threads finish
- [x] `bulk_load<I: Iterator<Item=(K, Vec<u8>)>>(&mut self, entries: I)`
- [x] Tests: 1M entries in benchmark binary (phase0.rs), 100-entry test in integration suite

### 1.7 Integration tests — Mark ✅ VERIFIED (11 integration tests, 44 total)
- [x] End-to-end: bulk load → reads → updates → compaction → reads (test_end_to_end_lifecycle)
- [ ] ~~Concurrent readers during compaction~~ — DEFERRED (DataSilo is single-owner, not Sync; needs reader/writer split design)
- [x] Crash simulation: truncated-entry recovery at ops_log level (DataSilo-level crash test deferred)
- [ ] ~~Property tests~~ — DEFERRED (proptest dep added but tests not written; not blocking Phase 2)

### Phase 0: Pre-flight Benchmark ✅ VERIFIED (reviewer approved 9118f65)
- [x] 1M synthetic entries (~300B avg)
- [x] Bulk load: 4.02M/s multi-threaded (accepted — 49x faster than V2's 22 min)
- [x] Read p50: 400ns (2.5x better than 1μs target)
- [x] Compaction integrity: delete 10%, compact, verify all survivors

**Phase 1 deliverable:** `crates/datasilo/` — 44 tests passing, 4M/s bulk load, 400ns reads, crash-safe atomic compaction. ✅ COMPLETE.

---

## Phase 2: Doc Integration (Mark)

> Goal: BitDex serves `include_docs` from DataSilo instead of DocStore V2.

### 2.1 Doc silo wiring — Mark ✅ VERIFIED (reviewer approved 277cafb+9d5844b, 5 tests, feature-gated)
- [x] `src/v3/mod.rs`: V3Engine struct with `DataSilo<SlotId>` for docs
- [x] `SlotId` implements `SiloKey` (Clone+Eq+Hash+Send+Sync via u32)
- [x] Raw-byte API — serialization is caller responsibility (documented, matches V2 pattern)
- [x] `get_doc(slot_id)` → silo.get() → raw bytes (5 tests: roundtrip, missing, delete, bulk, update)

### 2.2 Dump pipeline adapter — Mark ✅ VERIFIED (feba29d+d433f93, V3DocSink + AppState plumbing)
- [x] `src/v3/loader.rs`: V3DocSink adapts dump output to DataSilo bulk_load (3 tests)
- [x] **Config-driven field mappings** (Scarlet #1): field names passed from config, not hardcoded
- [x] **No loading mode** (Scarlet #3): writes directly via bulk_load
- [x] V3Engine added to AppState behind `#[cfg(feature = "v3")]`
- [ ] ~~Full dump handler routing~~ — DEFERRED to Phase 2.3 (combines with ops mutation path, avoids duplicate async lifecycle work)
- [ ] ~~10M images.csv test~~ — DEFERRED to Phase 2.4 (validation scope)
- Note: headerless CSV handling is dump_processor's responsibility, not loader.rs (doc comment corrected)

### 2.3 Ops integration — Mark ✅ VERIFIED (reviewer approved 90cf52c+026dd3a, 9 tests, Arc snapshot confirmed)
- [x] V3DocWriter: write/read/clear_pending with Arc<HashMap> copy-on-write
- [x] In-memory pending ops with Arc snapshot for read consistency (design doc Section 3)
- [x] snapshot() returns Arc::clone, read_with_snapshot() checks snapshot first
- [x] Tests: write+read, pending overrides engine, snapshot isolation, clear_pending
- [ ] ~~Full ops_processor wiring in server.rs~~ — DEFERRED (TODO documented at line ~1176, needs fresh session with full context for async batch integration)

### 2.4 Validation — Mark
- [ ] Run 107M images dump via DataSilo
- [ ] Compare include_docs output vs V2 (byte-for-byte or field-for-field)
- [ ] Benchmark: write throughput, read latency, disk usage

**Phase 2 deliverable:** `include_docs` queries served from DataSilo. DocStore V2 + DocCache deletable.

---

## Phase 3: Bitmap Integration (Ollie)

> Goal: all filter + sort + alive bitmaps stored in DataSilo as frozen format.

### 3.1 Bitmap silo wiring — Ollie ✅ VERIFIED (reviewer approved 9779edf, 13 tests, design deviation approved)
- [x] `src/v3/bitmap.rs`: `BitmapKey(field, value)` + `BitmapSilo { silo: DataSilo<u64>, key_map }` (deviation: monotonic ID mapping instead of BitmapKey as SiloKey — reviewer approved as better design for dense index access)
- [x] Frozen serialization: `bitmap.serialize_frozen_into()` for writes
- [x] Frozen reads: `FrozenRoaringBitmap::view(silo.get_ref(key))` — zero-copy via 32-byte aligned entries
- [x] Key map persisted to bitmap_keys.tsv (flush on put() deferred to Phase 3.4 fix)

### 3.2 Packed field files — Ollie ✅ VERIFIED (reviewer approved bcd0983, 20 tests, design insight validated)
- [x] DataSilo sharding (key % num_shards) handles file-count reduction natively — no separate packed-file routing needed
- [x] field_index (HashMap<field_name, Vec<value>>) for executor enumeration, range scans, NotEq
- [x] Maintained on put/delete, rebuilt from key_map on restart
- [x] Tests: high-cardinality (500 tagId values), field iteration, frozen round-trip (real dump test deferred to 3.6)

### 3.3 Sort layer storage — Ollie ✅ VERIFIED (reviewer approved d762206, 26 tests, bifurcate walk confirmed)
- [x] Sort bit-layers stored as frozen bitmaps via BitmapKey::sort_layer(field, bit_index)
- [x] store_sort_layers(), get_sort_layer_frozen(), sort_num_bits(), reconstruct_sort_value()
- [x] Tests: bifurcate-compatible AND/SUB walk (top-10 DESC), value reconstruction, multi-field, edge cases

### 3.4 In-memory diff for mutations — Ollie ✅ VERIFIED (reviewer approved b0fd78a, 41 tests total, Scarlet #7 confirmed)
- [x] BitmapDiff: per-field set/clear bitmaps with cancellation logic (480 lines, 15 tests)
- [x] Read path: fuse_frozen() = (frozen | sets) - clears, fuse_frozen_with_candidates() for executor fast-path
- [x] DiffMap: per-key collection with Arc snapshot for read consistency
- [x] **Sort bit clearing on decrease** (Scarlet #7): update_sort_value() clears ALL old bits before setting new (tested: 255→1)
- [ ] ~~Compaction orchestration~~ — DEFERRED to Phase 5.4 (janitor); primitives (fuse_frozen, reset, remove) are in place

### 3.5 Executor port — Ollie ✅ VERIFIED (reviewer approved dd67e72+4aae8fd, 22 tests, all operators covered)
- [x] V3 executor: 756 lines, full filter support (Eq, In, NotEq, NotIn, And, Or, Not, Gt/Lt/Gte/Lte, BucketBitmap)
- [x] Direct AND/OR/Sub via roaring-rs fork, diff fusion per-key, short-circuit on empty
- [x] Sort: MSB-to-LSB bifurcate walk on frozen layers, cursor-based keyset pagination (DESC+ASC)
- [x] 22 executor tests covering all operators + sort + pagination + diff fusion
- [ ] ~~Loadtest/V2 comparison~~ — DEFERRED to Phase 3.6 (validation scope)

### 3.6 Validation — Ollie
- [ ] 107M full load with frozen bitmaps
- [ ] Query correctness: all loadtest queries return identical results to V2
- [ ] Performance: p50/p95/p99 latency vs V2 baseline
- [ ] Memory: RSS must be < 25 GB

**Phase 3 deliverable (3.1-3.5):** ✅ COMPLETE — full query path via frozen mmap bitmaps. 63 tests across ~2,000 lines (bitmap.rs, diff.rs, executor.rs). V2 bitmap code deletable.

---

## Phase 4: Cache Integration (Edward + Ollie)

> Goal: unified cache stored in DataSilo, persistent across restarts.

### 4.1+4.2 Cache silo — Ollie ✅ VERIFIED (reviewer approved afceb68, 9 tests, all criteria met)
- [x] CacheSilo: DataSilo<u64> + CacheKey(query_hash) mapping (same pattern as BitmapSilo)
- [x] Binary entry format: 28-byte header (last_used, total_matched, min_tracked_value, capacity, has_more) + standard roaring bitmap
- [x] LRU eviction: timestamp-based, configurable max_bytes (4GB default), evict on put
- [x] Persistent: cache_keys.tsv + DataSilo mmap shards, survives restart
- [x] Standard roaring (not frozen) — correct for frequently mutated cache entries
- [x] 9 tests: round-trip, LRU eviction, persistence, serialization, update, remove, utilization

### 4.3 Expanded budget — Edward
- [ ] Configure cache budget: 4GB (up from 333MB)
- [ ] Verify hit rate improvement (target: 95%+)
- [ ] Monitor eviction rate (should drop dramatically)

### 4.4 Validation — Ollie
- [ ] Cache hit rate under loadtest (must exceed 90%)
- [ ] Persistence: restart server, verify cache entries survive
- [ ] Memory: cache heap usage near zero

**Phase 4 deliverable:** unified cache in DataSilo. bound_store.rs + unified_cache.rs deletable.

---

## Phase 5: V3 Engine (Edward + Ollie)

> Goal: full V3Engine replaces ConcurrentEngine behind feature flag.

### 5.1 V3Engine struct — Ollie ✅ (42a26dc, 86 tests total, 2 integration tests)
- [x] V3Engine owns doc_silo + bitmap_silo + cache_silo + DiffMap
- [x] execute_query() snapshots DiffMap → delegates to V3Executor
- [x] Full query path: filter → sort → limit → results (review pending)
- [x] Feature flag: `--features v3` selects V3Engine in server.rs

### 5.2 Server wiring — Ollie ✅ (6350947, /v3/query route via axum, 89 V3 tests total)
- [x] Wire HTTP query route to V3Engine behind cfg(feature = "server")
- [ ] ~~Stats, dumps, ops routes~~ — DEFERRED (query route is the critical path; other routes need mutation thread from 5.3)
- [ ] ~~V3-specific prometheus counters~~ — DEFERRED to Phase 6

### 5.3 Mutation thread — Mark ✅ VERIFIED (reviewer approved 4dcd687, 9 tests, Arc snapshot confirmed)
- [x] Single thread: crossbeam channel drain, block on first op, batch remaining
- [x] V3MutationOp: 7 variants (DocUpsert/Delete, FilterSet/Clear, SortUpdate, AliveSet/Clear)
- [x] MutationState: Arc<HashMap> for docs + Arc<DiffMap> for bitmaps, snapshot() for read consistency
- [x] Ops applied to both DataSilo AND in-memory buffers, publish via Arc swap

### 5.4 Janitor — Mark ✅ (ff73573, 5 tests, round-robin compaction, responsive stop)
- [x] Background thread: round-robin compaction across doc/bitmap/cache silos
- [x] Configurable thresholds, responsive stop signal
- [x] CompactStats delegation to V3Engine

### 5.5 Ops wiring — Mark ✅ (f95acfa, V3BitmapSink + server.rs WAL routing)
- [x] V3BitmapSink adapter: BitmapSink → V3MutationOp
- [x] server.rs WAL reader routes through V3 mutation channel when v3_mutation_tx set
- [x] AppState gains v3_mutation_state + v3_mutation_tx
- [x] Both V2 and V3 compile clean, 100 V3 tests passing

**Phase 5 deliverable:** ✅ COMPLETE — V3Engine serves queries, handles mutations, compacts. Feature-flag tested.

---

## Phase 6: 107M Validation (All)

### 6.1 Full data load — Mark
- [ ] Bulk load 108.9M images + enrichment via V3 dump pipeline
- [ ] Verify all docs readable, all bitmaps correct

### 6.2 Query validation — Ollie
- [ ] Run full loadtest workload against V3
- [ ] Compare results vs V2 (field-by-field correctness)
- [ ] Run real query traces (from Aidan) when available
- [ ] **sortAt GREATEST computed field verification** (biggest bug chain in sync-v2 — Scarlet #4)
- [ ] **Ops field coverage:** test ALL field types — scalar, multi-value, LCS, boolean, sort, computed sort, delete (8 tests minimum)
- [ ] **Cache consistency after mutations:** verify no stale reads post-mutation
- [ ] **Time bucket filter field resolution** (sortAtUnix)
- [ ] **Enrichment-derived fields:** isPublished, isRemix from Bool/Int computed fields

### 6.3 Performance comparison — Edward
- [ ] p50/p95/p99 query latency vs V2 baseline
- [ ] RSS comparison (target: < 25 GB)
- [ ] Startup time (target: < 1 second)
- [ ] Cache hit rate (target: ≥ 90%)
- [ ] Bulk load time (target: ≤ V2)
- [ ] Disk usage (target: comparable, 27-30 GB)
- [ ] Steady-state ops throughput (target: ≥ 72/sec)

### 6.4 Config review — Edward
- [ ] Verify index config maps correctly to V3 silos
- [ ] All filter/sort fields from civitai-index.json have corresponding bitmap silos
- [ ] Runtime config knobs (compact threshold, buffer ratios) tested via PATCH

### 6.5 Gate review — Justin
- [ ] Justin reviews performance numbers
- [ ] Justin approves for production

**Phase 6 deliverable:** V3 validated at 107M. Justin approves.

---

## Phase 7: Flatten + Ship (All)

### 7.1 Flatten — Mark
- [ ] Move `src/v3/` contents to `src/`
- [ ] Remove feature flag — V3 is the only engine
- [ ] Delete all V2 files (11,700 lines)
- [ ] Update Cargo.toml, lib.rs, mod declarations

### 7.2 Docs — Dakota
- [ ] Update CLAUDE.md architecture section
- [ ] Archive V2 design docs to `docs/design/archive/`
- [ ] Update HANDOFF.md
- [ ] Update dev-guide, testing guide, perf guide

### 7.3 Deploy — Edward + Aidan
- [ ] **Configs first, then binary:** deploy configs as ConfigMap BEFORE the binary. Verify: sync.toml paths correct, PG grants for triggers, SET ROLE removed (Scarlet #5)
- [ ] Pre-deploy config verification step: load config, validate all fields resolve
- [ ] Cut release
- [ ] Deploy to production
- [ ] Monitor: latency, cache hit rate, RSS, ops throughput
- [ ] Rollback plan: revert to V2 release tag if issues

**Phase 7 deliverable:** V3 in production. V2 archived. Clean codebase.

---

## Dependencies

```
Phase 1 ──→ Phase 0 (benchmark needs bulk_load from Phase 1.6)
Phase 0 ──→ Phase 2 (docs need proven DataSilo crate)
Phase 0 ──→ Phase 3 (bitmaps need proven DataSilo crate)
Phase 3 ──→ Phase 4 (cache needs bitmap integration working)
Phase 2 + 3 + 4 ──→ Phase 5 (engine needs all silos)
Phase 5 ──→ Phase 6 (validation needs full engine)
Phase 6 ──→ Phase 7 (ship needs validation)
```

Phases 2 and 3 can run in **parallel** once Phase 0 confirms performance (Mark on docs, Ollie on bitmaps).

---

## Pre-flight Checklist

- [ ] Design doc committed to main (or feat/v3 first commit)
- [ ] Roaring-rs fork pushed to GitHub + CI pipeline configured
- [ ] Roaring-rs fork vendored or path-dep strategy decided for production builds
- [ ] `feat/v3` branch created off main
- [ ] `crates/datasilo/` workspace configured in Cargo.toml
- [ ] Mark and Ollie briefed on design doc
- [ ] Dakota confirms plan meets team standards
- [ ] Migration path confirmed: full CSV dump + V3 bulk reload (no online migration)

---

## Risks + Mitigations

| Risk | Mitigation |
|------|-----------|
| DataSilo crate takes longer than expected | Core is ~800 lines. Mark has all the benchmark code to reference. |
| Executor port breaks query correctness | Comprehensive test suite, field-by-field comparison vs V2 |
| 107M validation fails | Each phase validates independently. Fail early, fix before next phase. |
| V2 changes during V3 development | V2 is stable (Justin confirmed). Minimal merge conflicts expected. |
| Roaring-rs fork diverges from upstream | Frozen format is additive. Upstream changes don't affect our additions. |

---

## Tracking

Consider ClickUp for task tracking in future sessions — enables status tracking,
watchers for completion notifications, and assignment visibility across agents.
For this plan: update checkboxes in this doc as tasks complete.
