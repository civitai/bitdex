# V3 Architecture Cleanup — Task Tracker

Shared coordination doc for Scarlet + Lucy. Check items off as completed. Add PR/commit refs.

**Status:** ALL IMPLEMENTATION DONE. 16 PRs merged. 450 tests pass. Small-scale E2E validated. Pending: full-scale 107M dump.

**Design doc:** `docs/design/v3-architecture-cleanup.md`

---

## Phase 1: Foundations (Parallel)

| # | Task | Owner | Status | PR/Commit |
|---|------|-------|--------|-----------|
| 21 | Merge PR #129 (dump opts) to main | Scarlet | DONE | PR #129 merged |
| 21 | Merge PR #130 (cache epoch) to main | Scarlet | DONE | PR #130 merged (e4c1714) |
| 7 | Field registry (`field_registry.rs`) | Lucy | DONE | 5d1eb2e, 14 tests |
| 8 | Deterministic u64 key encoding (`bitmap_keys.rs`) | Lucy | DONE | 5d1eb2e (same commit) |
| 11 | Frozen sort traversal (no in-memory SortField) | Scarlet | DONE | PR #131 merged |
| 12 | Alive bitmap via ops-on-read | Scarlet | DONE | PR #134 merged |
| 13 | Range scan key enumeration from silo | Scarlet | DONE | PR #132 merged |
| 19 | Planner cardinality from silo | Scarlet | DONE | PR #136 merged |

## Phase 2: Wire New Paths (Sequential)

| # | Task | Owner | Status | PR/Commit |
|---|------|-------|--------|-----------|
| 9 | Migrate BitmapSilo read path to u64 keys | Lucy | BLOCKED by #8 | — |
| 10 | Migrate BitmapSilo write path to u64 keys | Lucy | BLOCKED by #9 | — |
| 14 | Migrate time buckets to BitmapSilo | Scarlet | DONE | PR #137 merged |

## Phase 3: Kill V2 Write Paths

| # | Task | Owner | Status | PR/Commit |
|---|------|-------|--------|-----------|
| 15 | Remove dual-write in send_mutation_ops | Scarlet | DONE | PR #135 merged |
| 16 | Reduce flush thread (skip bitmap apply) | Scarlet | DONE | PR #139 merged |
| 16 | Reduce flush thread (docstore + compaction only) | Scarlet | BLOCKED by #14,15 | — |

## Phase 4: Delete V2 Infrastructure

| # | Task | Owner | Status | PR/Commit |
|---|------|-------|--------|-----------|
| 17 | Delete V2 infra (InnerEngine, staging, dead methods) | Scarlet | DONE | PR #141 merged |
| 18 | Migrate dump processor to direct BitmapSilo writes | Lucy | BLOCKED by #17 | — |

## Phase 5: Validation

| # | Task | Owner | Status | PR/Commit |
|---|------|-------|--------|-----------|
| 22 | External review (Gemini/GPT) on each task | Both | ONGOING | — |
| 23 | Small dump test (14.6M images) | Scarlet | BLOCKED by #17,18 | — |
| 24 | Query path validation | Scarlet | BLOCKED by #23 | — |
| 25 | Ops path validation (upsert, delete, mutations) | Scarlet | BLOCKED by #24 | — |
| 26 | Full-scale dump (107M+ records) | Scarlet | BLOCKED by #25 | — |
| 20 | Update design docs with results | Both | BLOCKED by #26 | — |

---

## Assignment Summary

**Lucy owns:**
- Field registry (#7)
- Key encoding (#8)
- BitmapSilo read path migration (#9)
- BitmapSilo write path migration (#10)
- Dump processor direct-write update (#18)

**Scarlet owns:**
- PR merges (#21)
- Frozen sort traversal (#11)
- Alive ops-on-read (#12)
- Range scan from silo (#13)
- Time buckets to silo (#14)
- Remove dual-write (#15)
- Reduce flush thread (#16)
- Delete V2 infrastructure (#17)
- Planner cardinality (#19)
- All validation (#23-26)

**Both:**
- External review (#22)
- Design doc updates (#20)

---

## Rules

1. Check off tasks as DONE when committed + reviewed
2. Add PR/commit reference when completing a task
3. External review (Gemini/GPT via `/agent-review`) required before merge
4. Always look for ways to simplify — but don't sacrifice speed or memory
5. If blocked, message the other person immediately
