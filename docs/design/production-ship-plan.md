---
status: ACTIVE
updated: 2026-03-28
owner: Tom (CTO)
---

# Production Ship Plan

> Management-level sequence for shipping Sync V2 + Data Silos to production.
> This is NOT an implementation plan — it's the strategic coordination document
> that Tom uses to sequence team leads through validation → merge → deploy → verify.

---

## Phase 1: Sync Pipeline Validation (Scarlet's Team)

**Owner:** Scarlet (team lead) | **Engineers:** Josh, Nate, Lucy
**Dependency:** None — can proceed immediately
**Current status:** Gates 1-2 CLEAR, Gates 3-5 PARTIAL

### What must happen
- [ ] Gate 3: Real PG trigger validation (not crafted data)
- [ ] Gate 5: Full production flow reproduction (see checklist below)
- [ ] 158-item E2E checklist passes (`docs/design/e2e-validation-checklist-sync-v2.md`)
- [ ] Config audit: `civitai-index.json` matches `civitai-field-requirements.md`

### Gate 5 Production Flow Checklist

Gate 5 is a **local reproduction of the full production pipeline**, not "load CSVs and query." Every step below must pass in a single continuous run:

1. [ ] `bitdex-sync` connects to PG via tunnel (Aidan's deploy skill: `tunnel pg`)
2. [ ] `bitdex-sync` dumps CSVs from PG via `COPY` (not pre-downloaded files)
3. [ ] `bitdex-sync` sends dumps to BitDex server via `PUT /dumps` (D3 schema)
4. [ ] PG triggers deployed and firing ops into `BitdexOps` table
5. [ ] `bitdex-sync` polls `BitdexOps`, POSTs ops to BitDex server via `POST /ops`
6. [ ] Full lifecycle validated: dump completes → ops polling active → queries return correct results reflecting real-time updates
7. [ ] All 30 document fields present and correct in query results
8. [ ] All 9 CSV phases process successfully in single pass
9. [ ] No manual intervention or pre-staged data — the pipeline produces its own data end-to-end

**This is not a unit test.** Pre-downloaded CSVs, crafted test data, or piecemeal component testing do NOT satisfy Gate 5.

### Exit criteria
- All gates CLEAR with real production data (not pieces tested separately)
- Gate 5 checklist above passes as a single continuous run
- Tom verifies end-to-end before reporting to Justin
- PR opened from `feat/sync-v2` to `main`

### Decision gate
**Justin approval required** for sync-v2 merge to main. No exceptions.

### Rollback trigger
If queries return incorrect results or memory exceeds 28GB during validation: stop, diagnose, fix, re-validate.

---

## Phase 2: Data Silo Integration — DEFERRED (2026-03-29)

**Status:** DEFERRED — shipping sync-v2 from main directly without data silos.

**Reason:** Edward's team investigating bugs (resources phase hang, performance gap between microbenchmarks and full-scale runs). Data silos will be a follow-up project after sync-v2 is stable in production.

**Original plan preserved below for when work resumes:**

<details>
<summary>Original Phase 2 plan (click to expand)</summary>

**Owner:** Edward (team lead) | **Engineers:** Mark, Ollie, Josh
**Dependency:** Phase 1 must be merged to main first
**Current status:** Phases 1-4 implementation COMPLETE, Phase 5 validation PENDING

### What must happen
- [ ] Rebase data silo branch onto main (picks up Scarlet's merged sync pipeline)
- [ ] Resolve any merge conflicts between data silo + sync v2 code
- [ ] Run Phase 5 full validation: 107M load with data silos replacing DocStore V2
- [ ] Same Gate 5 validation bar: all 30 fields, query correctness, no perf regressions
- [ ] Performance confirmed: dump time < current 11m21s, read latency <= current

### Exit criteria
- Full 107M validation passes with data silos active
- Performance improvement confirmed (target: dump <5 min from current 11m21s)
- PR opened from data silo branch to main

### Decision gate
**Justin approval required** for data silo merge to main.

### Rollback trigger
If data silo reads return different results than DocStore V2 for the same queries: revert to DocStore V2 path, investigate.

</details>

---

## Phase 3: Production Deploy (Aidan + Tom Coordination)

**Owner:** Aidan (infra) + Tom (coordination)
**Dependency:** Phase 1 must be merged to main (Phase 2 deferred)
**Coordination:** Donovan (shadow mode), Arabella (Flux/K8s)

### Pre-deploy checklist
- [ ] All validation gates passed (Phase 1 + 2)
- [ ] Docker image built and pushed to GHCR
- [ ] Donovan disables shadow mode (or coordinates V2 sidecar replacement)
- [ ] Production memory stable (monitor RSS trend before deploy)
- [ ] Aidan confirms K8s readiness (pod resources, PVC, Flux config)

### Deploy sequence
- [ ] K8s staging deploy via Aidan
- [ ] Staging smoke test: load data, run queries, verify correctness
- [ ] Production deploy: rolling restart via Flux
- [ ] Verify V2 sidecar starts, boots, transitions to ops polling

### Post-deploy monitoring (30 min)
- [ ] `bitdex_rss_bytes` < 28 GB
- [ ] `bitdex_query_duration_seconds` p99 < 100ms
- [ ] `bitdex_pgsync_cursor_position` advancing
- [ ] Zero `bitdex_sync_errors`
- [ ] Donovan re-enables shadow mode

### Rollback trigger
If V2 sidecar fails: revert to V1 outbox poller (keep V1 running until V2 proven).
If queries degrade: disable shadow mode immediately.
If memory spikes: reduce cache `max_bytes` via PATCH /config.

---

## Phase 4: Stability Verification (Gate 7)

**Owner:** Tom + Aidan + Ivanna (monitoring)
**Dependency:** Phase 3 deploy completed
**Duration:** 2+ hours minimum

### Verification checklist
- [ ] Shadow mode running 2+ hours with comparison metrics clean
- [ ] Query latency comparable to V1 baseline (p50=0.23ms, p95=15ms, p99=36ms)
- [ ] Memory trend stable (not climbing)
- [ ] Cache hit rate comparable (44% was V1 shadow mode baseline)
- [ ] Sync lag < 30s (ops polling catching changes in near-real-time)
- [ ] No 6-second stalls (known V1 issue from shadow mode)
- [ ] All 30 document fields returned correctly in shadow comparison

### On success
- [ ] Notify Justin via mailbox: "sync-v2 stable in production"
- [ ] Update HANDOFF.md with new deployed version
- [ ] Dakota documents final state in CLAUDE.md and memory

---

## Dependencies Map

```
Phase 1 (Sync Validation)
    ↓ merge to main (Justin approval)
Phase 2 (Data Silo Validation)
    ↓ merge to main (Justin approval)
Phase 3 (Production Deploy)
    ↓ 30-min monitoring pass
Phase 4 (Stability Verification)
    ↓ 2-hour shadow mode clean
DONE — notify Justin
```

Phases are strictly sequential. No phase starts until the previous phase's exit criteria are met.

---

## Team Assignments

| Phase | Lead | Engineers | Support |
|-------|------|----------|---------|
| 1 | Scarlet | Josh, Nate, Lucy | Adam (design review) |
| 2 | Edward | Mark, Ollie, Josh | Adam (design review) |
| 3 | Aidan | — | Tom (coordination), Donovan (shadow), Arabella (Flux) |
| 4 | Tom | — | Aidan (monitoring), Ivanna (Prometheus), Donovan (comparison) |
| All | Dakota | — | Documentation, verification, knowledge extraction |

---

## Key Documents

| Doc | Purpose |
|-----|---------|
| `docs/design/production-readiness-checklist.md` | Detailed gate tracking |
| `docs/design/sync-v2-final-implementation-plan.md` | Sync V2 task tracker |
| `docs/design/data-silo-implementation-plan.md` | Data silo task tracker |
| `docs/design/civitai-field-requirements.md` | Field ground truth |
| `docs/guide/team-standards.md` | Verification standards |
| `docs/HANDOFF.md` | Operational context |
