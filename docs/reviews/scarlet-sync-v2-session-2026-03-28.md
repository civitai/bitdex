# Session Review: scarlet — Sync V2 Team Lead (Gate 5 Push to Production)

**Session:** 70e45cdf-8ab7-496d-a7c4-9809544ba661
**Agent:** Scarlet (Sync V2 team lead)
**Date:** 2026-03-28 through 2026-03-30
**Reviewer:** Conversation Reviewer (spawned by Dakota)
**Purpose:** Extract what Justin catches that Tom missed, how Justin guides team leads, and what "done" means to Justin vs. what agents propose

---

## Executive Summary

This session reveals a sharp contrast between how Tom (CTO agent) assessed Gate 5 readiness and how Justin actually defined it. Tom accepted piecemeal validation across separate runs. Justin rejected this explicitly and required a single combined, end-to-end local run. The session also shows Justin making several mid-session architectural corrections when Scarlet or her sub-agents chose shortcuts over correct designs. This document is intended to improve Tom's CTO agent behavior.

---

## Key Decisions

### Decision 1: Gate 5 Piecemeal Validation — Rejected

**What happened:** Scarlet reported Gate 5 as verified with the following structure:

> "What we've proven (separately):
> 1. Full 107M dump pipeline works — 107.5M alive, 5.36 GB bitmap memory, correct tag/filter counts (Lucy session 1)
> 2. Doc fields return correctly with production civitai-index.json config — all 30+ fields verified (Lucy session 2, subset data)
> 3. PG schema matches our COPY queries — verified against real database (Donovan)
> 4. Config is correct — Tom audited all 30 doc fields, 20 filter fields"

Tom had accepted this. Justin did not.

**Justin's words (10:35 AM prompt):** "We need a local run that goes completely end-to-end. Otherwise, what's going to happen is we'll get this stuff into production, and it won't work for one reason or another, and now we have to try and figure out why. But if we have it running locally, then we can debug because we're essentially in the same environment."

**Justin's follow-up (10:38 AM prompt):** "Be sure to report to Tom to let him know that I declined your piecemeal gate five validation."

**Rationale:** Justin's core reasoning is debuggability. Piecemeal validation across separate runs creates an integration gap — each piece can pass individually while the combined flow fails. A local E2E run creates a debug environment that mirrors production, so if something fails in production, you already have a working local baseline to compare against.

**Impact:** Gate 5 was declared NOT cleared. A comprehensive 158-item validation checklist was created. A full local E2E run was required before merge approval.

**What Tom missed:** Tom accepted that "pieces proven separately" constituted validation. Justin's threshold is a single combined run where every component interacts — not just each component tested in isolation.

---

### Decision 2: No Bitmap Fallback for existedAt — Use Proper Data Contract

**What happened:** Lucy implemented a "bitmap fallback" approach for computing `existedAt` in the ops path: when the docstore didn't have the field, she would reconstruct the value from sort bitmap bit layers.

**Scarlet's initial take:** "On one hand: this is pragmatic — it works with existing loaded data, no re-dump needed, and `SortField::reconstruct_value()` is reading from the bitmaps that ARE correct."

**Justin's correction:** He called this a hack. Scarlet accepted it and redirected.

**Scarlet's summary of why it's wrong:**
> "It makes the ops path depend on the bitmap state being correct, creating a circular dependency... It violates the data contract: the docstore should be the source of truth for field values, not the bitmaps... It means the ops path and the dump path have different assumptions about what a 'correct' document looks like."

**The correct fix:** Fix the dump processor to persist `existedAt` to the docstore properly. If the docstore doesn't have the field, that's a bug in the dump path, not something to paper over in the ops path.

**Impact:** Lucy was redirected to fix the dump processor directly. A full clean redump was required to verify the fix.

---

### Decision 3: Server-Side 0-Row Phase Rejection (Not Just Sidecar)

**What happened:** When tools/techniques CSVs loaded 0 rows, Lucy fixed the sidecar to check per-phase completion before skipping.

**Justin's correction (prompt, 9:54 AM):** "Per-phase completion, sure, I guess the sidecar needs to do that, but technically, if it's in Bitdex's registry for dumps, it should be getting resumed by Bitdex, right? Unless those statuses are failed or something."

**Scarlet's articulation of the correct design:**
> "If the server marked them 'completed' with 0 rows, that's the actual bug. The server should either fail or retry phases that loaded 0 rows from a non-empty CSV. The sidecar registered them, so the server should be the one responsible for resuming incomplete work."

**Outcome:** Lucy shipped both fixes — server-side 0-row rejection (`d3c351d`) plus the sidecar per-phase check (`4887eaf`).

**Impact:** Justin caught that the sidecar was patching over a server-side responsibility boundary. The server should own the invariant that completed phases have non-zero rows.

---

### Decision 4: Config-to-Behavior Testing Required

**What happened:** The sortAt computed sort had three bugs layered on top of each other, none of which was caught by the existing tests. Scarlet discovered this:

> "Lucy's unit tests passed — but they manually constructed `computed_deps` in test setup, bypassing the config parser. The unit test proved the recomputation algorithm works. It never tested that the config parser populates the data structure that feeds the algorithm. QA verified 'the code compiles.' It never verified 'load this YAML, create this index, send this op, verify this query result.'"

**Justin's response:** Anger. He was right to be. The QA pass (done by Dakota's doc keeper) had declared task 2.3 as "QA-verified" when in fact only the algorithm was tested, not the config-to-behavior path.

**The fix:** Dakota updated `team-standards.md` with three mandatory config-to-behavior testing requirements. The principle: for every config property with runtime behavior, the test must load from the actual YAML, create an index, and verify the behavior works end-to-end. Not just that the config parses.

---

## Performance Findings

### sortAt Bug Chain — Three Layers Deep

**Finding:** The sortAt composite sort field (`GREATEST(existedAt, publishedAt)`) had three independent bugs that each masked the next:

1. **Layer 1:** Config missing `computed` property — fixed in commit `830c231`
2. **Layer 2:** JSON→YAML migration dropping the `computed` property during serialization — fixed by eliminating the migration entirely (`2c5fa6c`)
3. **Layer 3:** Computed sort reads 0 instead of stored value for unchanged source fields — fixed in `4b85932`
4. **Layer 4 (root cause):** `existedAt` was never persisted to docstore by the dump processor — computed ephemerally for sort bitmaps only, so the ops path had nothing to read back

**Scarlet's summary:**
> "Justin's sortAt test uncovered all three. Each layer masked the next — you'd never find #3 without fixing #1 and #2 first. This is exactly why Gate 5 with real data matters."

**Context:** This is the canonical example of why piecemeal testing fails. The sortAt computed sort looked correct in isolation at every level — config, code, algorithm — but the end-to-end path was broken because of an unconnected dependency between the dump processor and the ops path docstore contract.

---

## Gotchas Discovered

### Gotcha 1: Config Changes Require Index Recreation

**What happened:** After the sortAt config fix (`computed` property added), Nate's test still didn't see the behavior change. The server was using the persisted YAML from the original index creation.

**Scarlet's finding:**
> "It was never a code bug OR a config bug. It was a deployment issue: the config JSON was updated but the server was using the persisted YAML from the original index creation. Nate needs to delete + recreate the index to pick up the new config."

**Root cause:** Index config is persisted at creation time. Config changes are not hot-reloaded — index must be dropped and recreated.

**Prevention:** Gate 5 testing must explicitly include a delete+recreate step after any config change. Production deploys that change config fields must account for this.

---

### Gotcha 2: Tags CSV V1 vs V2 Format

**What happened:** The validation script had the V1 tags format (2 columns: `tagId, imageId`) when V2 requires 3 columns (`tagId, imageId, attributes`) including the disabled-tag filter `(attributes >> 10) & 1 = 0`.

**Root cause:** The CSV dump was re-run at some point with V2 format but the validation scripts were written against V1.

**Prevention:** Any validation script must be verified against the actual CSV column count before running. The sync config YAML is the authoritative source — validation scripts must derive their column lists from it.

---

### Gotcha 3: Dump Processor Crash from Server Restart During Run

**What happened:** Scarlet restarted the server to test PG auth (`bitdex-sync setup`) while a dump was running in the background. The server kill terminated the dump.

**Impact:** Lost ~112 GB of partial dump data, requiring a full cleanup and restart.

**Prevention:** Never restart the server while a dump is in progress. Dump state is not resume-safe across server restarts (at least not in a way that was tested). The sidecar's idempotent `.done` marker only helps if the server itself survives.

---

### Gotcha 4: PG `SET ROLE civitai` Hardcoded in Dump Pipeline

**What happened:** The dump pipeline had a hardcoded `SET ROLE civitai` that failed when the sidecar connected as the `bitdex` user.

**Scarlet's summary:** "Hardcoded `SET ROLE civitai` blocking the pipeline. Quick fix needed."

**Fix:** Lucy removed the hardcoded role in commit `ebc4f41`.

**Prevention:** Connection role must be config-driven. No hardcoded role assumptions in production code. The `bitdex` user must have the grants it needs or the role escalation must be conditional.

---

### Gotcha 5: Enrichment Header Auto-Detect

**What happened:** The enrichment CSV loader (for posts, models, etc.) didn't handle cases where the CSV had no header row. The fix for dump CSVs (commit `05b8742`) covered the main dump path but not the enrichment code path.

**Justin's clarification:** "Code should handle both cases, no re-download needed. Lucy needs to fix enrichment loader to auto-detect headers."

**Fix:** Two commits — `72f8e7f` (initial fix) and `756c810` (improved auto-detect per Justin's request).

**Prevention:** Enrichment loading is a separate code path from main CSV loading. Any fix that applies to "CSV loading" must check both paths.

---

### Gotcha 6: Missing Sync Config YAML in K8s

**What happened:** The dump pipeline skipped all data during the production deploy because the sync config YAML was not deployed as a K8s ConfigMap. The code knew where to look but the file wasn't there.

**Scarlet's fix:** Pointed Aidan to `docs/design/reference/sync-config-civitai.yaml` and requested a ConfigMap deploy.

**Prevention:** Deployment checklist must explicitly include ConfigMap creation for the sync config. Binary deployment alone is not sufficient — config file must be present before the sidecar runs.

---

## Design Changes

### Change 1: YAML-Only Config — Migration Layer Eliminated

**Changed:** The config migration path (JSON→YAML) was eliminated entirely. YAML is now the only config format in production.

**Reason:** The JSON→YAML serialization silently dropped the `computed` property, causing the sortAt recomputation to fail. Eliminating the migration removes the conversion bug.

**Old approach:** Server accepted JSON config (civitai-index.json), migrated to YAML internally on first load.

**Commit:** `2c5fa6c` (Nate)

**Regression risk:** If someone reintroduces a migration layer or a JSON-to-YAML conversion step, they need to ensure all config properties including `computed`, nested objects, and arrays survive the round-trip. Any such conversion must have a test that verifies config-to-behavior (not just config-to-parse).

---

### Change 2: Per-Phase CSV Streaming Download

**Changed:** The reload pipeline now downloads and processes CSVs sequentially per phase (download phase N, process immediately, prefetch phase N+1 in background) rather than waiting for all CSVs to be downloaded before processing.

**Reason:** Reduces time to first queryable data from ~20 min (wait for all downloads) to ~5 min (images phase processes immediately after download). ClickHouse metrics download in background from the start.

**Commit:** `33e09b0` (Lucy)

**Impact:** Server is queryable within 5 minutes of sidecar start, not 20.

---

## Justin's Guidance Style With Scarlet

### Autonomy vs. Correction

Justin gave Scarlet considerably more autonomy than he gives Aidan. He does not micromanage team composition, task assignment, or agent tooling choices. He trusts Scarlet to manage Lucy and Nate without directing those interactions.

However, Justin does intervene sharply when:
1. A completion claim doesn't match his definition of "done" (the piecemeal rejection)
2. An implementation takes a shortcut that violates a data contract (bitmap fallback rejection)
3. A fix is at the wrong layer (sidecar vs. server responsibility boundary)
4. A check on a sub-agent's work reveals something uncertain (10:05 AM prompt: "Not sure if she used the correct tags.csv or the old one. You need to check on her")

### How Justin Communicates Corrections

Justin's corrections are brief and direct, often one sentence. He does not explain at length — he states the principle and expects Scarlet to work out the implications.

- **On E2E requirement:** "We need a local run that goes completely end-to-end." (One sentence. No elaboration.)
- **On piecemeal rejection:** "Be sure to report to Tom to let him know that I declined your piecemeal gate five validation." (Administrative, not argumentative.)
- **On bitmap fallback:** He called it a hack. Scarlet accepted and redirected without requiring further explanation.
- **On server responsibility:** "Technically, if it's in Bitdex's registry for dumps, it should be getting resumed by Bitdex, right?" (Question form, but clearly corrective.)

### When Justin Steps Back

Justin is comfortable stepping back once correction is accepted. After the Gate 5 rejection, he did not supervise Scarlet's creation of the 158-item checklist — he let her build it. After the bitmap fallback rejection, he did not dictate the correct fix — he stated the principle and let Lucy implement it.

Justin also manages Scarlet's team size pragmatically: "I think we should go ahead and go with option B... Aiden should have created a tunnel for you already. If he hasn't, reach out to him" — giving direction without micromanaging how to implement it.

---

## What Tom (CTO Agent) Missed vs. What Justin Caught

This is the most critical section for improving Tom's behavior.

### Miss 1: Accepted Piecemeal Gate 5 Validation

Tom received Scarlet's status "Gate 5 verified, waiting on Justin deploy approval" and treated this as valid. The mailbox message from Tom at 10:34 AM reads: "Your status says 'Gate 5 verified, waiting on Justin deploy approval.' Did —" (truncated, but context shows Tom was inquiring, not blocking).

Justin's view was the opposite. He was not willing to deploy based on separate validations of separate components.

**Tom's gap:** Tom should apply the same standard Justin does — a gate is not cleared unless there is a single combined run that exercises all components together in a configuration matching production. Separate proofs of separate components do not constitute gate clearance.

---

### Miss 2: Did Not Audit Config-to-Behavior Coverage

Tom audited field mappings (30 doc fields, 20 filter fields) but did not audit whether the config parsing layer actually fed runtime behavior. The sortAt computed sort bug chain was invisible to a field-by-field audit because the algorithm code was correct — only the config→algorithm wiring was broken.

**Tom's gap:** Config audits must include at least one config-to-behavior test per novel config property type. If a config property is supposed to trigger a behavior (like computed sort), the audit must verify the behavior fires, not just that the property is present in the config file.

---

### Miss 3: Accepted "Code Exists" as "Feature Works"

The wiring audit during Gate 5 confirmed that DocSink and computed sort recomputation code existed. Tom had previously treated code presence as feature readiness. Justin's Gate 5 caught that "code exists" and "code is connected and exercised" are different things.

**Tom's gap:** Wiring audits should produce runtime evidence, not just code-tracing conclusions. "The code path exists" is not the same as "we ran through the code path with real data."

---

### Miss 4: Did Not Track Tags Disk Space as a Blocker

Tags CSV at 80-100 GB uncompressed was a genuine gate blocker for local E2E validation. Tom's CTO overview (from the prior review doc) did not surface this as a prerequisite. Justin raised the issue only after Scarlet started the validation run and hit the disk constraint.

**Tom's gap:** Prerequisite validation for gate clearance should include infrastructure requirements (disk space, tunnel availability, CSV readiness) before declaring a gate runnable.

---

## Correction Pattern Summary

Justin's correction pattern with Scarlet follows a consistent structure:

1. **State the principle in one sentence** — no lengthy argument
2. **Expect Scarlet to accept and redirect** — he does not argue or elaborate unless asked
3. **Administrative enforcement** — "Be sure to report to Tom" shows he tracks whether corrections propagate up the chain
4. **Defers implementation** — once he corrects the direction, he lets Scarlet work out the implementation without micromanaging
5. **Intervenes on layer errors** — Justin consistently catches when a fix is applied at the wrong layer (sidecar vs. server, ops path vs. dump path, algorithm vs. config parser)

---

## Undocumented Knowledge

1. **Gate clearance requires a single combined run, not piecemeal proofs.** This is not written anywhere in the production readiness checklist. It's Justin's standard and he enforces it verbally.

2. **The sidecar is not the server.** Justin expects responsibility boundaries to be respected: if the server accepted a completed dump phase with 0 rows, the bug is in the server, not the sidecar. The sidecar should not compensate for server-side invariant failures.

3. **Config-to-behavior is a testing category, not just config auditing.** After this session, Dakota added this to team-standards.md. But the principle that config audits alone are insufficient — that you must verify the config actually causes the expected runtime behavior — should be part of Tom's review criteria for any gate.

4. **Justin expects Tom to block on the same standards he blocks on.** The explicit "Be sure to report to Tom to let him know that I declined your piecemeal gate five validation" was not just informational — it was a signal that Tom's acceptance was wrong and he needed to be corrected.

5. **Option B = full pipeline, not shortcut.** When Justin said "go with Option B... because we want to be able to see that the full pipeline works because that's what we're aiming to do in production," he was expressing a general principle: when there's a choice between a complete test and a partial test, always choose the complete one if production will run the complete path.

---

## Recommended Memory Entries

### Entry 1: `feedback_gate5_single_combined_run`
**Name:** Gate 5 requires single combined run, not piecemeal proofs
**Description:** Justin explicitly rejected Scarlet's piecemeal Gate 5 validation (separate runs proving separate components). His requirement is a single local E2E run that exercises all components together: dump pipeline + field verification + ops pipeline + trigger verification + query correctness. Reason: debuggability — if production fails, you need a local baseline that mirrors the full flow.

### Entry 2: `feedback_config_to_behavior_testing`
**Name:** Config audits must include config-to-behavior verification
**Description:** A config audit that verifies field presence is not sufficient. Every novel config property with runtime behavior must have a test that loads from the actual YAML, creates an index, and verifies the behavior fires end-to-end. "The code exists" and "the config parses" are not evidence that the feature works.

### Entry 3: `feedback_responsibility_layer_hygiene`
**Name:** Fix bugs at the correct layer — server owns server invariants
**Description:** Justin corrects when fixes are applied at the wrong layer. If the server marks a completed dump phase with 0 rows, the server has a bug, not the sidecar. The sidecar should not compensate for server-side invariant failures. Same principle applies to data contracts: if the ops path needs existedAt, the dump path must persist it — the ops path should not reconstruct it from bitmaps.

### Entry 4: `feedback_tom_must_match_justin_standards`
**Name:** Tom (CTO agent) must apply Justin's gate standards, not lower ones
**Description:** Tom accepted piecemeal Gate 5 validation that Justin then rejected. Tom's role is to apply Justin's standards autonomously — not to be a pass-through. When Scarlet reports gate clearance, Tom should ask: "Was this a single combined run?" and "Does this include ops pipeline with live PG triggers, not just CSV loading?" If the answer is no, Tom should not approve.

### Entry 5: `project_scarlet_sync_v2_correctons`
**Name:** Scarlet session corrections — architectural patterns Justin enforces
**Description:** From the 2026-03-28 Scarlet session: (1) piecemeal gate validation rejected; (2) bitmap fallback for missing docstore fields rejected as violating data contract; (3) sidecar must not compensate for server invariant failures; (4) config-to-behavior testing is mandatory. See `docs/reviews/scarlet-sync-v2-session-2026-03-28.md` for full detail.
