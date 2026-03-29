# Clarification #001: Implementation Plan Ownership

**Status:** PENDING
**Created:** 2026-03-28 by Dakota (Doc Keeper)
**Priority:** MEDIUM
**Affects:** docs/design/sync-v2-final-implementation-plan.md

---

## The Issue

The sync-v2 implementation plan has Phase 2 and Phase 3 task checkboxes that are out of date. The production readiness checklist says Gates 1 and 2 are CLEAR, but the implementation plan still shows all Phase 2 tasks (2.1-2.12) as unchecked. Phase 3 tasks 3.1-3.3 and 3.7 are also done per Tom but unchecked.

## Context

From the **implementation plan** (current state):
```markdown
- [ ] **2.1** WAL reader background thread — spawn on server startup, tail WAL file
- [ ] **2.2** Wire DocSink into apply_ops_batch
...
- [ ] **2.12** Prometheus metrics
```
All 12 tasks are `[ ]` (unchecked).

From the **production readiness checklist** (Tom's update, same day):
```markdown
**Owner:** Lucy | **Status:** CLEAR (2026-03-28) — 17 passed, 0 failed, 2 skipped
```

From the **feedback memory** (`feedback_dirty_tracking_docs.md`):
> "Don't commit implementation plan progress to worktree branch"

## The Question

1. **Who is responsible for keeping the implementation plan checkboxes current?** I've told Scarlet (team lead) this is her job, but she may need it explicitly in her agent definition.

2. **Where should the "current" version of the plan live?** The plan only exists on `feat/sync-v2` (not on `main`). The feedback rule says don't commit progress to worktree branches. This creates a conflict — the plan can't be updated where it lives.

3. **Should the plan be the source of truth for task status, or is the production readiness checklist the authority?** Right now they disagree.

## Options

**A)** Scarlet owns the plan, updates it on `feat/sync-v2`. The "don't commit progress to worktree" rule is relaxed for the team lead maintaining the plan.

**B)** The production readiness checklist becomes the single source of truth for status. The implementation plan is just a task list (what needs doing), not a status tracker.

**C)** Move the plan to a location that doesn't have the "don't commit to worktree" constraint (e.g., a separate doc or memory file).

## Impact

Without a clear answer, agents reading the plan will think Phase 2/3 work hasn't started, potentially duplicating effort or making wrong assumptions about what's available.

---

**Justin's answer:** *(pending)*
