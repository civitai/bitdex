# Team Operations Standards

> How agents and team members are expected to work on BitDex.
> These standards are non-negotiable. Violating them will get work rejected.

---

## Done With Proof, or Explicitly Deferred

Every checklist item has exactly two valid states:

1. **Done with proof** — checked off with evidence (file:line, test output, screenshot)
2. **Explicitly deferred** — unchecked with a documented reason why and who will do it later

There is no third state. Silent skipping is corner-cutting. If a task is hard to verify, ask for help — use scout agents, ask teammates, escalate to Tom. The bar doesn't lower because something is difficult.

## Verify Before Claiming Done

When you claim a task is done, you must provide proof:

- **Generated SQL?** Show the output. Don't just say "trigger SQL generated."
- **Tests run?** Show the pass/fail output with actual data. Don't just say "tests pass."
- **Cleanup done?** Show the before/after state. Don't just say "cleaned up."

**The standard:** Work is done when a reviewer can independently verify the claim from the evidence you provide. If someone has to run your code again to check, you haven't provided enough proof.

---

## Gates Require Real Validation

A gate is not CLEAR because crafted test data passes. A gate is CLEAR when:

1. The code runs against **real production-scale data** (107M+ records)
2. The results are **verified against known-good values** (CSV counts, metrics TSV)
3. **Edge cases** are exercised (null transitions, disabled tags, future publishedAt)
4. A **sub-agent or independent reviewer** has verified the claims

**Crafted tests are necessary but not sufficient.** They prove the logic works. They don't prove it works at scale, with real data quirks, and under production conditions.

**Recent lesson (2026-03-28):** Gates 3 and 5 were marked CLEAR based on crafted test data only. Justin reverted them to PARTIAL. This cost the team time and eroded trust.

**End-to-end rule:** A gate is NOT clear until a full end-to-end run completes with real production data in a single pass. Separate pieces passing individually does not equal the system working. The CTO must verify this before reporting to Justin. Testing the dump processor and WAL reader separately, even if both pass, does not prove they work together.

### Pre-Validation Requirements (2026-03-28)

Before running any gate validation, team leads must complete these steps:

1. **Pre-validation config review:** Review the full config and identify every computed, derived, enriched, and composite field. Each needs an explicit test case — not just "filter mutation works."
2. **Ops field coverage:** Every gate that touches the ops pipeline must test ops against every field type — filters, sort fields (including computed/composite like sortAt), docstore fields, and enriched fields. One field proving "it works" is not sufficient.
3. **Feature flag verification:** When code exists behind a feature flag, the validation must explicitly verify the flag is active in the binary being tested. Dormant code is not validated code.
4. **No partial ops validation:** Marking ops as "verified" requires independent confirmation of: BitmapSink (filter mutations), DocSink (docstore updates), sort field recomputation, and cache invalidation.

**Lesson (2026-03-28):** Scarlet marked ops pipeline as verified after basic filter bitmap mutation worked. Justin's sortAt composite test revealed DocWriter/sortAt recomputation was inactive behind a feature flag. Subsequent gap analysis found 18 untested paths (10 critical). Partial validation wastes everyone's time.

### Config-to-Behavior Testing (2026-03-29)

Unit tests that manually construct test fixtures prove the algorithm works in isolation. They do NOT prove the feature works in production. Every config-dependent feature requires:

1. **No QA verification without config-to-behavior test:** Every feature that depends on config must have an integration test that loads from the actual config file, not manually constructed fixtures. If a test sets up its own data structures instead of going through the parser, it doesn't count as verification.

2. **Full-path verification:** The test must prove the complete chain: config file → parser → runtime data structure → feature behavior → observable result. Any break in that chain means the feature is NOT implemented.

3. **Config parser coverage:** Every new config property must have a test that verifies the parser reads it and populates the correct runtime data structure. A config property without parser coverage is dead code.

**Lesson (2026-03-29):** Task 2.3 (computed sort recomputation) was marked QA-verified, but the config parser never populated `computed_deps` from the YAML. The unit tests passed because they manually constructed `computed_deps`. The algorithm worked in isolation but never ran in production because the wiring was never built. Correct tests that exercise dead code are worse than no tests — they create false confidence.

---

## Design Doc Is the Contract

The design docs in `docs/design/` define what the code should do. When reviewing or implementing:

- If the code doesn't match the doc, the code is wrong (unless the doc was updated first with approval)
- If the doc is ambiguous, clarify with Adam (architect) or Justin before implementing
- If you discover the doc needs updating, flag it — don't silently change behavior

**Design doc compliance is a PR review requirement.** PRs are checked not just for code correctness but for alignment with the design spec.

---

## Sub-Agents Verify Claims

When a team member claims work is done:

1. A QA sub-agent should review the actual commits (not just the self-report)
2. The sub-agent checks: Does the code match the task description? Are tests present? Does it compile?
3. Only after sub-agent verification should the task be marked complete

**Why:** Agents over-report completion. Adam and Ivanna discovered this pattern — agents say "done" but the code has gaps, missing error handling, or doesn't match the design doc. Trust but verify.

---

## PR Standards

Every PR must:
- Include tests for the code it adds
- Pass `cargo check` (compilation)
- Not degrade benchmarks by >10%
- Be reviewed for design doc compliance
- Get Justin's personal approval for any sync-v2 changes

---

## Implementation Plan Maintenance

The sync-v2 implementation plan (`docs/design/sync-v2-final-implementation-plan.md`) is the source of truth for task status.

- **Team leads** (Scarlet) must update checkboxes when agents complete tasks
- **QA verification notes** should be appended to each task line
- If a task is partially done, note what's remaining
- The **Doc Keeper** (Dakota) audits the plan for accuracy

---

## Design Process — From Idea to Validated Implementation

New architecture proposals follow this process. No shortcuts.

### 1. Capture the idea
- Voice memo, conversation, or written proposal
- Dakota (Doc Keeper) or the proposer creates a design doc in `docs/design/`
- Status: **PROPOSED**

### 2. Document with context
- Include the problem statement with numbers (what's slow, what's broken, what's missing)
- Include any existing benchmarks or session review findings
- Link to source material (voice memos, session reviews, prior design docs)

### 3. Broadcast and review
- Share with the team via mailbox broadcast
- Engineers with domain expertise review and raise concerns
- Concerns are documented IN the design doc with proposed resolutions
- Status: **REVIEWED**

### 4. Benchmark before implementing
- Each design assumption gets a specific benchmark with a **goal threshold**
- Benchmarks are standalone binaries in `scratch/` (per `/microbench` pattern)
- Code is preserved alongside results in `docs/benchmarks/{feature}/`
- Results include: goal vs actual, methodology, hardware config
- If a benchmark misses its goal, the design is revised before implementation proceeds

### 4b. Plan review before implementation
- When an implementation plan is created from a design doc, a **Plan Reviewer** verifies coverage
- Every design element must have a corresponding plan task (or be explicitly deferred)
- Review concerns and their resolutions must be reflected in the plan
- Benchmark findings must appear as validation goals
- Dakota spawns a Plan Reviewer sub-agent for this check
- Plan is not APPROVED until the coverage review passes

### 5. Implement with the design doc as the contract
- Engineers read the design doc before coding
- Code must match the doc — if it diverges, flag it
- After completion: session review to extract undocumented decisions, send findings to Dakota

### 6. Validate at scale
- Run against production-scale data (107M+ records)
- Gates require real validation, not just crafted test data
- Results documented with proof

**Recent example:** Data silo architecture — voice memo → design doc → Josh's 5 concerns documented → benchmark plan with 5 experiments and goal thresholds → validation pending.

## Worktree Branch Verification (Mandatory)

When spawning agents with `isolation: "worktree"`, the worktree frequently forks from the **wrong branch** (typically `main` instead of the active working branch like `feat/sync-v2`). This is a recurring issue that has caused wasted work and merge conflicts.

**After EVERY worktree agent spawn:**

1. Run `git worktree list` and check the commit hash of the new worktree
2. Compare against the intended base branch: `git log --oneline -1 <intended-branch>`
3. If mismatched, send the agent `git reset --hard <correct-branch>` via SendMessage **before it commits**
4. Include the correct branch explicitly in the agent prompt: *"Your base branch must be feat/sync-v2 (commit XXXXXXX). Verify with `git log --oneline -1` before making changes."*

**This is not optional.** Treat it like a pre-flight check. Multiple agents have been caught working on stale code from the wrong branch.

---

## Communication Standards

- Use mailbox for inter-agent coordination
- Don't bug individual agents unless you need specific info — go to leads first
- When blocked, say so clearly with what you need and from whom
- Status updates should include: what you did, what you found, what's next
