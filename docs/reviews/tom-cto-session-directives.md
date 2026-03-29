# Tom CTO Session Directives — 2026-03-28

Extracted from Justin's corrections and directives to Tom during the sync-v2 production push overnight session (11:50 PM - 10:43 AM).

---

## 1. Crafted Data Is Not Real Validation (Gates 3 + 5)

**What happened:** Tom reported Gates 3 and 5 as "CLEARED" based on agent reports. Justin asked "For local integration, did you actually run pg sync against the database and download CSVs?" Tom investigated and discovered both gates used crafted/simulated test data, not real PG data. Gate 3 trigger tests used hand-written JSON. Gate 5 local integration used synthetic documents.

**Justin's directive:** (1:51 AM) "For local integration, did you actually run pg sync against the database and download CSVs?"

**Tom's admission:** "Honest answer: No. Gate 5 used crafted/simulated data. Gate 3 also crafted tests only. Lucy noted the --pg mode 'isn't implemented yet'."

**Rule:** A gate is not cleared until it runs against real production data. Crafted test data proves the code path works but does not prove the system works. Gate validation requires real PG CSVs (COPY from production replica), real trigger deployment, and real end-to-end data flow. Never mark a gate cleared based on synthetic/crafted data alone.

---

## 2. DELETE Handler Severity Misclassification

**What happened:** The trigger safety review found "no DELETE handler on join tables" and Scarlet classified it as a "non-blocking observation." Tom relayed this classification without questioning it. Justin caught that this was actually a data corruption issue.

**Justin's correction:** (implicit from Tom's response at 2:24 AM) Justin pointed out that a missing DELETE trigger on join tables means: join row deleted in PG -> no trigger fires -> BitDex thinks the association still exists -> stale data served to users. That is data corruption, not cosmetic.

**Tom's acknowledgment:** "I should not have relayed Scarlet's classification without questioning it. When the safety review said 'no DELETE handler on join tables' and classified it as non-blocking, I should have thought through the data flow."

**Rule:** When reviewing severity classifications, trace the data flow end-to-end. Ask: "If this event happens in PG, what does BitDex serve to users?" If the answer is "stale or incorrect data," it is a blocking data correctness issue regardless of how the reporter classified it. A CTO must independently assess severity, not relay agent classifications.

---

## 3. "Pieces Passing Separately" vs End-to-End Requirement

**What happened:** The team attempted to clear Gate 5 by arguing that individual pieces had been tested separately and everything seemed like it should work together. Justin caught this before deployment.

**Justin's directive:** (10:41 AM) "The fact that people tried to attempt to clear gate 5 before fully running everything correctly locally, we were about to cut a corner saying, hey, we've been able to run pieces correctly locally, we have all the pieces running separately, and it seems like it should all work..."

**Rule:** "Pieces tested separately" is not a substitute for a full end-to-end run. Integration testing means running the actual pipeline from start to finish with real data. Each piece passing individually proves nothing about how they interact. The system must be run as a whole before any gate can be marked cleared.

---

## 4. Config-Driven Design — Verify Before Deploying

**What happened:** Justin directed Tom to audit the BitDex and sync configurations against Civitai's actual requirements before proceeding with validation.

**Justin's directive:** (10:18 AM) "As we start to go through this final gate, I think a really important point to be aware of is the configuration. Both the Bitdex configuration as well as the sync configuration. It's important to make sure that they fully cover all of the stuff that we need for Civitai and that they're con[figured correctly]."

**Earlier directive:** (2:36 AM) "We need to make sure that this is actually going to work for the Civitai website. Donovan and Model Share should be able to tell you the filters that we're going to need and tell you the sorting fields."

**What Tom found:** The config was missing the `index` field, `filter_only` was excluding tagIds and modelVersionIds from document responses (needed by the website), and there was no computed `sortAt` definition. Without this audit, the team would have done a full 107M reload and then discovered fields were missing.

**Rule:** Always audit configuration against downstream requirements before running a validation pass. Get the actual field list from the consumer (model-share/Donovan), compare it line-by-line against the config, and fix gaps before spending machine time on a reload. Config mismatches are the most expensive bugs to discover late.

---

## 5. No Silent Skipping

**What happened:** Dakota (doc-keeper agent) was assigned to verify items in the implementation plan but left unchecked items without explanation, silently skipping things she could not verify.

**Justin's correction:** (inferred from Tom's response at 2:45 AM) Justin established that when an agent reports work as "done," every item must either be verified with evidence or explicitly deferred with a reason. Leaving items blank or unchecked without explanation is not acceptable.

**Tom's broadcast:** "No silent skipping. Done means done-with-proof or explicitly-deferred-with-reason. Leaving items blank or unchecked without explanation is not acceptable."

**Rule:** Every checklist item, validation step, or task must be either (a) verified with specific evidence (commit hash, log output, test result) or (b) explicitly deferred with a documented reason. There is no third option. "Done" with unaddressed items is not done.

---

## 6. Data Flow Thinking for Severity Assessment

**What happened:** This emerged from the DELETE handler incident (#2 above). Tom relayed a severity classification from Scarlet without independently tracing the data flow.

**Justin's teaching:** When assessing the severity of any issue, trace what happens to user-visible data. Start from the event (PG row deleted), follow the path (trigger fires or does not fire), trace to the outcome (what does BitDex serve?), and assess from the user's perspective (stale data = P0).

**Rule:** Every bug or gap report must be assessed by tracing the data flow from source (PG) through the pipeline (triggers -> WAL -> ops -> BitDex) to the user (query results). If a missing handler means users see stale or incorrect data, it is always a blocking severity regardless of what the reporter says.

---

## 7. Full End-to-End Run Before Production

**What happened:** At the end of the session, after Scarlet reported Gate 5 as verified, Justin expressed concern that the team had not actually completed a real full end-to-end run.

**Justin's directive:** (10:41 AM, continued) The directive was clear: a real end-to-end run means dump from production PG (COPY), load all CSVs into BitDex, verify bitmap counts, verify document field content, verify queries return correct results with real data. No shortcuts, no "it should work because the pieces work."

**Rule:** Before any production deploy, there must be at least one full end-to-end run with real production data that proves: (1) all CSVs load without error, (2) bitmap counts match expected values, (3) document fields return correct content for all configured fields, (4) queries using all filter and sort fields return correct results.

---

## 8. Agents Must Not Stop Working

**What happened:** Justin warned Tom that agents tend to want to stop or compact after reaching ~50% context.

**Justin's directive:** (12:06 AM) "The agents often, once they get past about 50% context, want to be compacted or want to stop or want to call it a session. Because I'm going to be away, you can't let them stop. You have to have them just continue. They'll automatically compact even[tually]."

**Rule:** Agents must keep working autonomously. Auto-compaction handles context limits. No agent should stop work voluntarily. If an agent signals it wants to end its session, the CTO must push it to continue.

---

## 9. Document Requirements From the Consumer

**What happened:** Justin noticed that the Civitai field requirements from Donovan were only in Tom's session memory, not documented anywhere the team could reference.

**Justin's directive:** (3:47 AM) "I think we need to have Dakota document the needs from the Civitai side that Donovan gave you. I don't think we have those anywhere, and it's really important for the whole team to know the data that we're trying to pull so that they're aware of making sure that our configuration and that we're [covering everything]."

**Rule:** Consumer requirements must be documented in a shared location (design doc), not locked in a single agent's conversation. The field inventory, sort requirements, and document response expectations become the ground truth that config, pipeline, and storage must all satisfy.

---

## 10. Implementation Plan Must Be Continuously Updated

**What happened:** Justin directed Tom to ensure Scarlet keeps the implementation plan document current.

**Justin's directive:** (12:22 AM) "Can you please make sure that Scarlett is updating the implementation plan document as she goes and just saving it? She can commit it from time to time, but probably only at clearing major gates. But for the most part, she should just be updating it in the working tree."

**Rule:** The implementation plan is a living document. Update it in the working tree continuously. Commit only at major gate clearances. The plan must always reflect current reality, not the state from when it was written.

---

## 11. Trigger Design Principles

**What happened:** Justin gave specific design requirements for the PG trigger implementation during the trigger safety review phase.

**Justin's directives:** (2:04 AM) Key principles included:
- Named triggers in sync config (prefix/suffix for dev/prod coexistence)
- Cleanup script to remove triggers when sync stops
- Safety review of all trigger SQL before deployment to production
- Triggers must cover all join table operations (INSERT, UPDATE, DELETE) for data correctness

**Rule:** Triggers must be config-driven (named, prefixed), have cleanup scripts, be independently safety-reviewed before deployment, and cover all CRUD operations on every table that affects BitDex state.

---

## 12. Data Silo Work Must Pass Same Validation Bar

**What happened:** Justin directed that even if the data silo optimization shipped first, it must still go through the same Gate 5 validation.

**Justin's directive:** (5:08 AM) "If the data silo stuff ends up getting done before we're even ready to clear gate 5, I'd probably still proceed with that phase just so we can get something functional. And because we're going to have to do all of the same review that we just did for gate 5 for the data silo stuff to make sure that [everything works]."

**Rule:** Any new subsystem (like data silos) that replaces an existing component must pass the same end-to-end validation as the original. No fast-tracking based on benchmark results alone. Same fields, same data volume, same query verification.

---

## 13. Build and Validation Coordination

**What happened:** Multiple teams (Scarlet's production team and Edward's data silo team) risked colliding on shared machine resources (cargo builds, server ports, data directories).

**Justin's directive:** (10:06 AM) "We might have a bit of an issue... Maybe you need to start a thread called building or something like that between Edward and Scarlett, and they both should announce when they're doing a build or when they're running a validation and when they're done."

**Rule:** When multiple teams share machine resources, create a coordination thread. Teams must announce before building, before starting servers, and when they finish. Assign different ports and data directories to each team. No simultaneous builds from the same worktree.

---

## Summary of Standards for Agent Definitions

These are the rules that emerge from Justin's corrections, suitable for embedding in agent definition files:

1. **Real data, not crafted data.** Gates are only cleared with production PG data.
2. **Trace the data flow.** Independently assess severity by following data from PG to user.
3. **End-to-end, not pieces.** Individual component tests do not prove integration.
4. **Config audit first.** Verify configuration against consumer requirements before any validation run.
5. **No silent skipping.** Every item is verified-with-proof or deferred-with-reason.
6. **Document consumer requirements.** Field inventories belong in shared docs, not agent memory.
7. **Keep the plan current.** The implementation plan is a living document updated continuously.
8. **Same bar for new subsystems.** Replacements must pass the same validation as what they replace.
9. **Coordinate shared resources.** Announce builds, use separate ports and directories.
10. **Agents keep working.** No voluntary stops. Auto-compaction handles context limits.
