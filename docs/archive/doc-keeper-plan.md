# Doc Keeper Master Plan

> Dakota's working document. Updated each session with progress, findings, and next actions.
> Last updated: 2026-03-28

---

## Session 1 — Initial Audit (2026-03-28)

### Tom's Briefing (received)
- Gates 1, 2, 4 CLEAR. Gates 3, 5 PARTIAL (crafted tests pass, real PG integration NOT done)
- Gates 3+5 reverted from CLEAR to PARTIAL (Justin directive)
- Priority: verify production readiness checklist, implementation plan, CLAUDE.md
- Rule: "Trust but verify" — don't take gate claims at face value

### Phase 1: Documentation Inventory — COMPLETE
- [x] Read CLAUDE.md, HANDOFF.md, agent definition
- [x] Read Tom CTO overview + Scarlet team lead notes (memory)
- [x] Inventory docs/design/ (36 files), docs/guide/ (13 files)
- [x] Read sync-v2-final-implementation-plan.md (Phases 1-3 + validation)
- [x] Read production-readiness-checklist.md (Gates 1-7)
- [x] MEMORY.md at 136 lines (limit 180) — healthy

### Phase 2: Accuracy Audit — IN PROGRESS

#### CLAUDE.md Findings
- [x] File paths verified — all 8 CLAUDE.md files exist
- [x] **GAP: No mention of sync-v2 architecture** — dump_processor.rs (3005 lines), dump_expression.rs (1065), dump_enrichment.rs (1086), ops_wal.rs, write_coalescer.rs, ops_processor.rs (2108) all undocumented
- [ ] Verify "BitmapFs legacy" claim (Explorer agent sent, awaiting result)
- [ ] Verify ArcSwap concurrency model description matches code
- [ ] Verify unified cache description matches current implementation

#### HANDOFF.md Findings
- [x] **STALE: Version v1.0.57** — should be v1.0.97 (confirmed by Tom via mailbox)
- [x] **STALE: Key files table** — 3 files deleted (single_pass.rs, outbox_poller.rs, row_assembler.rs), 5+ new files not listed
- [x] **STALE: Team contacts** — missing sync-v2 agents (Josh, Nate, Lucy, Scarlet, Tom as CTO)
- [ ] Verify replica count (says "2 replicas")
- [ ] Verify key endpoints list

#### Implementation Plan Findings
- [x] **Phase 1 tasks**: Correctly marked as done (1.1-1.14 checked, 1.15 partial)
- [x] **Phase 2 tasks**: All 12 tasks UNCHECKED — but Gate 2 says CLEAR with 17 tests passing. Discrepancy.
- [x] **Phase 3 tasks**: All unchecked — but Tom says 3.1-3.3 and 3.7 are DONE
- [x] **Note**: Per feedback rule "dirty tracking docs — keep on main", the impl plan is a working doc and progress shouldn't be committed to worktree. But this creates confusion for agents who read it.
- [ ] Need to verify Lucy's Phase 2 work actually exists in code (trust but verify)
- [ ] Need to verify Nate's Phase 3 work (3.1-3.3, 3.7) exists in code

#### Production Readiness Checklist Findings
- [x] Gates 1, 2 marked CLEAR — matches Tom's briefing
- [x] Gate 3 marked PARTIAL — matches Tom's briefing
- [x] Gate 4 at 60% — matches Tom
- [x] Gate 5 PARTIAL — matches Tom
- [x] Blocker table current as of 2026-03-28
- [ ] Need to verify V1.1-V1.9 validation claims (Josh's 107M run)

### Phase 3: Gap Analysis — FINDINGS

| # | Gap | Severity | Location |
|---|-----|----------|----------|
| 1 | HANDOFF.md version v1.0.57 vs actual v1.0.97 | HIGH | HANDOFF.md:70 |
| 2 | HANDOFF.md references deleted V1 files | HIGH | HANDOFF.md:112-118 |
| 3 | CLAUDE.md has no sync-v2 architecture section | HIGH | CLAUDE.md |
| 4 | New sync-v2 files not in any key files table | MEDIUM | HANDOFF.md, CLAUDE.md |
| 5 | Implementation plan Phase 2/3 tasks unchecked but work done | MEDIUM | sync-v2-final-implementation-plan.md |
| 6 | HANDOFF.md team list missing sync-v2 members | MEDIUM | HANDOFF.md:186 |
| 7 | WAL file is src/ops_wal.rs not src/wal.rs (plan says wal.rs) | LOW | impl plan, team lead notes |
| 8 | Implementation plan D6 says "BitmapFs restore" but dump_processor removed BitmapFs | NEEDS VERIFY | impl plan:148 |
| 9 | pg_sync/ has 17 files, most undocumented | MEDIUM | pg_sync/ directory |
| 10 | design-storage.md says DocStore is "zstd-compressed msgpack" (V1) | HIGH | docs/design/design-storage.md |
| 11 | design-storage.md describes BitmapFs as primary, no mention of ShardStore | HIGH | docs/design/design-storage.md |
| 12 | backfill.rs still uses BitmapFs (unmerged worktree has the fix) | LOW | src/pg_sync/backfill.rs |

### Phase 4: Planned Updates (Priority Order)

1. **[DONE] Update HANDOFF.md** — version, team, key files, deleted refs
2. **[DONE] Add sync-v2 section to CLAUDE.md** — dump processor, WAL, enrichment, ops processor
3. **[DONE] Update key files tables** — remove deleted, add new sync-v2 files
4. **[DONE] Send implementation plan discrepancy to Scarlet** — Phase 2/3 unchecked vs CLEAR gates
5. **[DONE] Create trigger deployment design doc** — docs/design/trigger-deployment-process.md
6. **[DONE] Create team standards doc** — docs/guide/team-standards.md
7. **[DONE] Update BitmapFs description** — precise about backfill.rs caveat
8. **[FLAGGED] design-storage.md stale** — sent to Tom, awaiting direction on who updates
5. **[LOW] Verify remaining CLAUDE.md claims** — ArcSwap, cache, BitmapFs status

---

## Stale Items Log

| Item | Location | Issue | Verified | Fixed |
|------|----------|-------|----------|-------|
| Version v1.0.57 | HANDOFF.md:70 | Should be v1.0.97 | Tom confirmed via mailbox | YES |
| Team list | HANDOFF.md:186-194 | Missing Josh, Nate, Lucy, Scarlet, Tom | Tom CTO overview | YES |
| single_pass.rs | HANDOFF.md:115 | File deleted (V1 cleanup task 3.7) | `ls` confirms missing | YES |
| outbox_poller.rs | HANDOFF.md:116 | File deleted (V1 cleanup task 3.7) | `ls` confirms missing | YES |
| row_assembler.rs | HANDOFF.md:117 | File deleted (V1 cleanup task 3.7) | `ls` confirms missing | YES |
| No sync-v2 in CLAUDE.md | CLAUDE.md | 5 major new modules undocumented | `ls src/` confirms existence | YES |
| BitmapFs description imprecise | CLAUDE.md | backfill.rs still uses BitmapFs | Explorer agent verified | YES |
| design-storage.md DocStore V1 | design-storage.md | Says "zstd-compressed msgpack" | HANDOFF.md pitfall #6 | FLAGGED to Tom |
| design-storage.md BitmapFs primary | design-storage.md | No mention of ShardStore | grep confirms ShardStore primary | FLAGGED to Tom |
| Impl plan Phase 2 unchecked | impl plan | Gate 2 CLEAR but tasks show `[ ]` | Tom + checklist say CLEAR | ASSIGNED to Scarlet |

---

## Notes for Future Sessions

- Tom is CTO, Scarlet is sync-v2 team lead — go to them first
- Justin requires personal approval before merging sync-v2 PRs
- Current branch is feat/sync-v2 — lots of recent dump processor work
- MEMORY.md has extensive project memories — good source but needs freshness check
- Feedback memories are critical — these are Justin's rules, non-negotiable
- Implementation plan lives only on feat/sync-v2 (not on main)
- "Dirty tracking docs — keep on main" rule means progress updates shouldn't be committed to worktree
- Production readiness checklist is the gating doc (Tom owns it)
- BitmapFs status still needs verification — Explorer agent was sent
