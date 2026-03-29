# Data Silo Implementation Plan — Coverage Review

> Reviewer: Plan Reviewer (automated)
> Date: 2026-03-28
> Plan: `docs/design/data-silo-implementation-plan.md`
> Design doc: `docs/design/data-silo-architecture.md`

---

## Summary

The implementation plan covers the major design elements and is well-structured with clear phases, gates, and team assignments. However, there are several gaps where the design doc specifies behavior that the plan either defers without adequate justification, silently omits, or contradicts.

**Verdict: 7 gaps found, 2 critical.**

---

## 1. Design Element Coverage

| Design Element | Design Section | Plan Coverage | Status |
|----------------|---------------|---------------|--------|
| Bulk write path (per-thread staging) | Section 1 | Phase 1 + Phase 2a/2b | COVERED |
| Silo index (global doc_index.bin) | Section 2 | Phase 1.2, 1.3, 1.6 | COVERED |
| mmap read path | Section 3 | Phase 1.5, Phase 3 | COVERED |
| Steady-state upserts | Section 4 | Phase 4 | COVERED |
| Compaction | Section 5 | Deferred list | COVERED (deferred) |
| Crash recovery | Section 8 | Deferred list | COVERED (deferred) |
| Silo index merging (Option A vs B) | Section 6 | Phase 1.6 (merge_indexes) | PARTIAL — see Gap 1 |
| ShardStore separation | Section 7 | Architecture Decisions table | COVERED |
| 8MB BufWriter | Design change note | Architecture Decisions table | COVERED |
| Startup warmup pass | B3 recommendation | Architecture Decisions table + Phase 3.3 | COVERED |
| DocDataFile | "What Needs to Be Built" #1 | Phase 1.1 | COVERED |
| DocIndex | "What Needs to Be Built" #2 | Phase 1.3 | COVERED |
| BulkDocWriter | "What Needs to Be Built" #3 | Phase 1.4 | COVERED |
| Wire into dump processor | "What Needs to Be Built" #4 | Phase 2a/2b | COVERED |
| Wire into read path | "What Needs to Be Built" #5 | Phase 3 | COVERED |
| Wire into upsert path | "What Needs to Be Built" #6 | Phase 4 | COVERED |
| Compaction background task | "What Needs to Be Built" #7 | Deferred list | COVERED (deferred) |

---

## 2. Josh's 5 Design Review Concerns

### Concern 1: Index Memory — mmap the index itself
**Design resolution:** mmap `doc_index.bin` instead of Vec. Speeds up startup, avoids 1.4GB allocation.
**Plan says:** Architecture Decisions table: "Index storage: mmap doc_index.bin (defer from Vec for V1)". Deferred list: "mmap DocIndex instead of Vec (startup optimization)".
**Status: CONTRADICTION.** The design doc resolved this as "do mmap" but the plan explicitly defers it, shipping V1 with a 1.4GB Vec. The plan's Architecture Decisions table says "mmap doc_index.bin" but the Deferred section says "mmap DocIndex instead of Vec". These contradict each other. Phase 1.3 builds a "flat Vec" which confirms the deferred reading is correct.
**Impact: CRITICAL.** At 107M entries x 13 bytes = 1.4GB, this adds to RSS. The design doc specifically called this out because of smaller deployments. The plan should either follow the design resolution or document why V1 ships with Vec and what the RSS impact is.

### Concern 2: Multi-value field accumulation
**Design resolution:** Accumulate full value list per slot during bulk load, write final merged document once.
**Plan says:** Phase 2b.3: "Handle per-slot value accumulation (tools/techniques are multi-value)".
**Status: COVERED.** Task 2b.3 directly addresses this.

### Concern 3: Phase ordering (biggest concern)
**Design resolution:** Each phase reads existing silo entry, merges new fields, writes new complete entry. Old entry becomes dead space.
**Plan says:** Architecture Decisions: "Cross-phase V1: Overwrite-only (defer read-merge-write)". Deferred: "Cross-phase read-merge-write (currently overwrite-only)".
**Status: GAP (Gap 2).** The design doc calls this "biggest concern" and specifies read-merge-write as the resolution. The plan defers it entirely, shipping V1 with overwrite-only. This means if images phase writes nsfwLevel for slot 42, then resources phase writes baseModel for slot 42 to a different silo file, the resources phase overwrites will lose the nsfwLevel data. The plan does not explain how overwrite-only handles multi-phase writes to the same slot without data loss. If each phase writes a complete document (including fields from prior phases), this needs to be stated explicitly with the mechanism described.

### Concern 4: filter_only still needed
**Design resolution:** Keep filter_only for toolIds/techniqueIds/modelVersionIdsManual even with silos.
**Plan says:** Architecture Decisions: "filter_only: Keep for toolIds/techniqueIds/modelVersionIdsManual". Phase 2a.6: "Skip docstore write when field_to_idx.get(target) returns None (filter_only)".
**Status: COVERED.** Both the decision table and a specific task address this.

### Concern 5: Clean module, not ShardStore integration
**Design resolution:** New `src/data_silo.rs` module.
**Plan says:** Architecture Decisions: "Module: Clean src/data_silo.rs, NOT ShardStore".
**Status: COVERED.**

---

## 3. Benchmark Findings Reflected in Plan

| Benchmark | Design Value | Plan Reference | Status |
|-----------|-------------|----------------|--------|
| B0: 4.77M/s write throughput | >=4M/s target | Phase 0 B0 PASS, V5a.3 (<5 min dump) | COVERED |
| B3: mmap 42ns warm | 42ns | Phase 0 B3 PASS, Architecture Decisions | COVERED |
| B3: cold access 4us/39.7ms | Page faults on first access | Phase 3.3 warmup pass | COVERED |
| B3: mmap creation 0.118ms | <100ms goal | Phase 0 B3 PASS | COVERED |
| Upsert 10.4us | Benchmark result | Not in validation goals | GAP (Gap 3) |
| Dead space 0.99% after 10K | Low dead space | Not in validation goals | GAP (Gap 4) |
| NVMe 0.9 GB/s ceiling | Hardware limit | Phase 0 B0-variants | COVERED |

**Gap 3:** Phase 5 validation (V5d) does not include an upsert latency target. The design doc reports 10.4us/upsert as a benchmark finding. V5d should include a gate like "upsert latency <=50us" to catch regressions.

**Gap 4:** Phase 5 does not validate dead space growth after steady-state upserts. The design reports 0.99% after 10K upserts. Without a validation step, dead space could grow undetected (especially given compaction is deferred).

---

## 4. Deferred Items

| Deferred Item in Plan | Covered in Design Doc | Explicitly Listed | Status |
|-----------------------|----------------------|-------------------|--------|
| mmap DocIndex instead of Vec | Section 2, Concern #1 | YES | OK (but contradicts Architecture Decisions table) |
| Cross-phase read-merge-write | Section 8 "Phase Ordering", Concern #3 | YES | OK (but risk not documented) |
| Compaction background task | Section 5 | YES | OK |
| Crash recovery | Section 8 | YES | OK |
| Remove DocStore V2 code | Implied by "replaces" | YES | OK |

**Silently omitted deferrals: None found.** All deferred items are explicitly listed.

However, one design element is neither implemented nor deferred:

**Gap 5: Silo Index Merging — Option A vs Option B (Design Section 6).** The design doc presents two options for the global index: single merged file vs per-silo index files with in-memory join. The plan assumes Option A (single merge via `merge_indexes()`) without documenting the decision or deferring Option B. This should be recorded as a decision in the Architecture Decisions table.

---

## 5. Specific Area Coverage

### Bulk Write Path
**COVERED.** Phase 1.1 (DocDataFile), 1.4 (BulkDocWriter), 2a (general phases), 2b (multi-value phases). Pre-creation of files mentioned in design but not as an explicit task in Phase 2a — assumed handled by `create_bulk_writers()`.

### Silo Index
**COVERED.** Phase 1.2 (IndexEntry), 1.3 (DocIndex), 1.6 (merge). Binary format, auto-grow, persist/load all addressed.

### mmap Read Path
**COVERED.** Phase 1.5 (DataSiloReader), Phase 3 (integration), Phase 3.3 (warmup).

### Steady-State Upserts
**COVERED.** Phase 4.1-4.4. Includes 64KB BufWriter for steady-state (smaller than bulk 8MB — reasonable).

### Compaction
**DEFERRED.** Listed in Deferred section. Design doc Section 5 describes the algorithm. Dead space measured at 0.99% after 10K upserts justifies deferral for V1.

### Crash Recovery
**DEFERRED.** Listed in Deferred section. Design doc Section 8 describes the algorithm (scan append region, replay into index).

**Gap 6:** The plan does not address what happens on crash WITHOUT crash recovery implemented. If the server crashes mid-upsert in V1, the silo file may have a partial append but the index was not persisted. On restart, the index will point to stale data or have no entry for recently upserted docs. The plan should document the V1 crash behavior explicitly: "V1 crash behavior: documents upserted since last index persist may be lost. Acceptable because sync will re-deliver them."

---

## 6. Additional Gaps

**Gap 7: DocCache removal/bypass not addressed.** The design doc's "Impact on Current Architecture" table shows memory for reads changing from "DocCache 1GB LRU" to "mmap page cache (OS manages)". The plan's Phase 3 wires in DataSiloReader but does not include a task to disable or remove DocCache for silo-backed reads. If DocCache remains active alongside mmap reads, documents will be cached in both the DashMap and the OS page cache, wasting up to 1GB of RSS. Phase 3 needs a task: "Bypass DocCache for slots served by DataSiloReader."

---

## Gap Summary

| # | Severity | Gap | Design Section | Plan Location |
|---|----------|-----|---------------|---------------|
| 1 | CRITICAL | Architecture Decisions table contradicts Deferred list on mmap index. Plan ships Vec but table says mmap. Resolve the contradiction and document RSS impact. | Concern #1 | Architecture Decisions + Deferred |
| 2 | CRITICAL | Phase ordering deferred to overwrite-only without explaining how multi-phase writes to the same slot avoid data loss. The mechanism must be documented. | Concern #3 (biggest concern) | Architecture Decisions + Deferred |
| 3 | MEDIUM | No upsert latency validation target in Phase 5. Design reports 10.4us benchmark. | Section 4 | Phase 5 V5d |
| 4 | LOW | No dead space growth validation after upserts. Compaction is deferred, so dead space should be monitored. | Section 5 | Phase 5 V5d |
| 5 | LOW | Option A vs Option B for index merging not recorded as a decision. | Section 6 | Architecture Decisions |
| 6 | MEDIUM | V1 crash behavior undocumented. Crash recovery is deferred but no description of what happens on crash without it. | Section 8 | Deferred |
| 7 | MEDIUM | DocCache bypass/removal not tasked. Risk of double-caching (DashMap + page cache) wasting 1GB RSS. | Impact table | Phase 3 |

---

## Recommendations

1. **Fix Gap 1:** Remove the contradiction. Either change Phase 1.3 to implement mmap index (matching the design resolution) or update the Architecture Decisions table to say "Vec for V1, mmap deferred" with a note on RSS impact at 107M.

2. **Fix Gap 2:** Add a paragraph to Phase 2a explaining the V1 cross-phase strategy. Likely: "Each phase writes a complete document for the slot (including fields accumulated from prior phases via the existing accumulation logic in the dump processor). The last phase to write a slot produces the final document." If this is not the mechanism, document what is.

3. **Fix Gap 3:** Add V5d.5: "Upsert latency: single-doc append + index update <=50us (10.4us benchmark baseline)."

4. **Fix Gap 6:** Add a note in the Deferred section under crash recovery: "V1 crash behavior: index is persisted after each dump phase and periodically during steady-state. Documents upserted between persists may be lost on crash. Acceptable because pg-sync will re-deliver missed ops."

5. **Fix Gap 7:** Add Phase 3.5: "Bypass DocCache for slots with silo index entries. Remove DocCache population for silo-backed reads to avoid double-caching."

6. **Fix Gaps 4 and 5:** Low priority. Add to Phase 5 or record as decisions.
