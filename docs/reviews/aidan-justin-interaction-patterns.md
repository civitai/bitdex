# Session Review: Aidan — V2 Production Reload (First Live Run)

**Session:** 8a94e17e-a73a-4849-884a-7e19305ede5d
**Agent:** Aidan (Infrastructure SRE)
**Date:** 2026-03-29 to 2026-03-30 (overnight session, roughly 6 PM to 4 AM)
**Reviewer:** Conversation Reviewer (spawned by Dakota)
**Purpose:** Extract Justin/Aidan interaction patterns for improving Tom (CTO agent) behavior

---

## Overview

This session covered the first live production run of the V2 dump pipeline — a multi-hour, multi-incident process involving a production PVC wipe, V2 CSV load, several sequential bug discoveries, and 4 point releases (v1.0.100 through v1.0.104). The session is rich with examples of how Justin intervenes, what he catches, and how he communicates.

---

## Key Decisions

- **Decision:** Move images before tags in the dump phase order.
  **Rationale:** Justin made this call proactively: "can you adjust the sync conf to move images before tags?" Images (15 GB) loads fast and makes the server useful sooner. Tags (63 GB) takes much longer and is secondary to serving queries.
  **Impact:** This is now the correct production dump ordering. Tags was only first in the reference config because it was written alphabetically, not operationally.

- **Decision:** Bump pg-sync container memory limit to 4 GB immediately (not wait for a streaming fix).
  **Rationale:** ClickHouse download was OOMKilling at 1 GB. Justin approved the quick infra fix while routing the proper streaming fix to Nate/Scarlet.
  **Impact:** Unblocked the current reload. Streaming fix (PR #95) landed in the same session.

- **Decision:** Switch the DATABASE_URL secret to use the `bitdex` PG user instead of `civitai`.
  **Rationale:** Justin interrupted Aidan mid-exploration: "No, don't do that. We just need to update our K8's config. We should be using the Bitdex user, not the Civitai user." The civitai user has a 120s statement_timeout and doesn't own the BitDex PG functions.
  **Impact:** This was a pre-existing misconfiguration in the K8s secret. It had never been caught because V2 had never done a full production dump before. This fix is now in talos-infra and the SOPS secret.

- **Decision:** Use disabled_metrics (opt-out model) instead of enabled_metrics (opt-in).
  **Rationale:** Justin noticed the current behavior: "Is the default for that not all of the metrics being on? I'm wondering if we need to get Ivana to change the enabled metrics to instead be disabled metrics." He recognized that opt-in causes silent observability loss on every restart.
  **Impact:** PR #92 (Ivanna) ships this change. Deployed in v1.0.101.

- **Decision:** Poll rate set to 250 ms (down from default).
  **Rationale:** Justin made this call based on throughput data. "Yeah, let's do 250ms. I mean, if we can process 50,000 in that fast, why not grab them as quickly as we can?"
  **Impact:** Now hardcoded in sync.toml at `poll_interval_ms=250`.

---

## What Justin Catches That Tom Missed

This is the primary focus. The following are issues that Tom (CTO) either did not flag or actively directed Aidan toward that Justin then corrected.

### 1. Shadow mode was never disabled before the reload

**Tom's blind spot:** Tom gave Aidan a verification checklist after the PVC wipe but it did not include disabling shadow mode first.

**Justin's catch:** "Uh-oh. It looks like we haven't turned off shadow mode, and like we're maybe taking requests right now, which could be a real problem, especially while loading."

**Follow-up:** Justin also spotted the cache contamination risk that Aidan hadn't raised: "Is it an issue that we were taking requests? There are 6k cache entries.... We might need to stop the pods and remove the cache. Or at least remove it after the reload."

**Tom gap:** Tom should have flagged shadow mode as a pre-step, not a post-step. This is a standard deploy hygiene issue that a CTO overseeing a production reload should catch before it causes query result corruption.

### 2. Tags appearing "already complete" with 0 ops was suspicious

**Tom's blind spot:** Not flagged.

**Justin's catch:** "When you see the logs, do you see how long it took to do tags and that it actually loaded all of them? I'm a little bit concerned that tags is already done because we know that that one typically takes a long time, but we just got this thing kicked off, right?"

**What happened:** Justin's instinct was exactly right. The tags dump showed `status: Complete` with `ops_processed: 0, ops_written: 0` — a false completion marker from a previous sidecar run that had registered but not loaded the data.

**Tom gap:** Tom should cross-reference dump completion markers against expected ops counts. "Complete with 0 ops" is always a red flag, especially for a 63 GB CSV.

### 3. The CSV header bug had already been assigned to Lucy

**Tom's blind spot:** When the 0/0 ops situation emerged at 2:27 AM, the immediate response was to investigate. Tom had not tracked what Lucy had actually fixed.

**Justin's catch:** "Yeah, Lucy was supposed to fix something like this. It was supposed to handle headers missing and to also add headers to the CSV. So I'm concerned that this didn't get fixed." And: "obviously we want this to be right, and her thing was supposed to support not having headers, so it's probably a code bug that needs to be addressed."

**What happened:** Justin correctly identified that the fix was shipped but not working — either the fix was in the wrong code path (enrichment loader vs dump loader) or the CSVs on disk were pre-fix. Aidan then confirmed the fix was in the enrichment path but not the dump loader path.

**Tom gap:** Tom should track which bugs were assigned to which agents and verify they were fixed in the correct code path. "We fixed it" is not the same as "we fixed it in every place it can fail."

### 4. The hardcoded `SET ROLE civitai` in code violated project principles

**Tom's blind spot:** Tom did not raise this as a design violation.

**Justin's reaction:** "Ah! No! The code is wrong! The code shouldn't have anything in there for setting role to Civitai or anything. It shouldn't do any of that. Nothing in the code should have anything tied to Civitai. This is an issue. We need to raise this with Scarlett. We shouldn't be ever switching it back to Civitai in the code."

**What happened:** `src/pg_sync/queries.rs` had `SET ROLE civitai` hardcoded for trigger creation. This is a generic infrastructure tool that should not reference a specific tenant. Justin immediately recognized this as a design violation, not just a bug.

**Tom gap:** Tom should enforce the principle that infrastructure code must not contain tenant-specific strings. This is analogous to the "no hardcoded field names" principle for docstore writes. A CTO-level agent should flag this class of violation proactively.

### 5. The reference sync config YAML was not a validated production config

**Tom's blind spot:** Tom sent Aidan to use the reference YAML from the repo. That YAML was a design doc, not a tested config.

**Justin's indirect catch:** Aidan discovered this himself (multiple parse errors: `missing field 'index'`, then `missing field 'table'`), but Justin had delegated config verification to Tom, who did not verify it.

**What happened:** Aidan deployed a 486-line reference config that caused two successive parse failures and two pod restarts. Lucy had to provide the actual tested config (294 lines).

**Tom gap:** When directing an agent to deploy a config file, verify it has been tested against the actual parser before deploying it to production. Design docs and working configs are often out of sync.

---

## How Justin Guides Aidan

### Communication Style

Justin uses very short, direct messages — often a single sentence or question. He does not write structured instructions. He talks like he's talking to a colleague across the desk.

Examples:
- "Maybe try it with a small one for now?"
- "Done yet?"
- "Why is it taking so long to remove 1 line 0.o?"
- "Okay, you've got people waiting for you. Is that CSV done?"
- "Open your mailbox."
- "Can you please send that mail so that we can keep going? I don't know what's going on with you."

He expects Aidan to interpret these and act without further explanation. He does not scaffold decisions.

### When He Pushes

Justin pushes when:
1. **There is a blocker for other agents.** He names the blocked agent explicitly: "Nate needs you open your mail." "You've got people waiting for you."
2. **Something simple is taking too long.** "Why is it taking so long to remove 1 line 0.o?" — A sign he's watching the clock and the explanation is insufficient.
3. **A decision is being deferred that he wants made now.** "Yeah, let's do 250ms." He short-circuits the analysis and gives the answer.
4. **An agent is stuck waiting for confirmation that Justin has already given.** "Can you go ahead and send the mail since you said it's done, or is it not done?" — He notices when Aidan is waiting on permission he's already implicitly granted.

### When He Trusts

Justin steps back and lets Aidan run when:
1. **The work is infrastructural and Aidan knows the tools.** The talos-infra commits, the SOPS secret update, Flux reconciliation — Justin asks how things are looking but doesn't direct the steps.
2. **A production go-ahead has been given.** "Executing now." is Aidan responding to Justin's approval; after that point Justin monitors but doesn't direct.
3. **Aidan is correctly coordinating with the right people.** When Aidan routes bugs to Scarlet/Lucy/Ivanna and waits for their fixes, Justin doesn't interrupt the workflow.
4. **The work involves multiple simultaneous background tasks.** Justin doesn't ask how the background pollers are set up or second-guess the monitoring strategy.

### How He Asks Questions

Justin's questions are almost always a single-sentence probe that reveals he is tracking a specific technical variable. Examples:

- "I think we normally level out here though... RSS awareness for the cache, right?" — He's watching the RSS trend and cross-referencing against expected behavior.
- "Will it know that the CH download isn't done, so that it doesn't treat it like a complete file since it's going to be incomplete?" — He understood the restart race before Aidan had explained the `.done` marker system.
- "When you see the logs, do you see how long it took to do tags and that it actually loaded all of them?" — He's tracking expected vs actual duration as a correctness signal.
- "Did you just cut another release? Isn't that going to potentially cause things to reboot during the reload?" — He caught the Flux image automation → pod restart race condition before Aidan had surfaced it.
- "Is it an issue that we were taking requests? There are 6k cache entries..." — He understood the cache contamination risk immediately from "6k entries" without needing more explanation.

### His Technical Instincts (Specific Reveals)

1. **He knows about the `.done` marker pattern** before Aidan explains it: "Will it know that the CH download isn't done, so that it doesn't treat it like a complete file?" He already had a mental model of how incremental download resumption works.

2. **He tracks ops counts as a correctness signal.** "0 ops written" on a completed dump is not a status to accept. He immediately questioned it on tags.

3. **He knows the cursor seeding sequence matters.** He didn't ask about it directly in this session, but his concern about operations during loading ("Is it an issue that we were taking requests?") shows awareness of what gets into the cache vs what gets into the index.

4. **He treats hardcoded tenant strings as an architectural violation, not just a bug.** The `SET ROLE civitai` reaction was immediate and categorical: "Nothing in the code should have anything tied to Civitai."

5. **He understands the PG user distinction.** "We should be using the Bitdex user, not the Civitai user." He knew about the statement_timeout difference before Aidan looked it up.

---

## Correction Patterns

### Pattern 1: Interrupt + Redirect (Abrupt)

Justin uses "[Request interrupted by user]" + a direct correction. This happens when Aidan is about to do something wrong.

Example: Aidan was proposing to patch the K8s secret directly and figure out SOPS later. Justin interrupted: "No, don't do that. We just need to update our K8's config. We should be using the Bitdex user, not the Civitai user. The Civitai user is going to give us issues."

**Tom should note:** When Justin interrupts, the correction is the whole message. No softening, no explanation of why Aidan's approach was wrong beyond the one sentence. He expects the agent to implement the correction immediately without discussion.

### Pattern 2: Raise the Concern, Then Trust (Collaborative)

Justin raises something he's noticed — "I'm a little bit concerned that tags is already done" — and waits to see how Aidan investigates. If Aidan confirms the concern and handles it, Justin steps back. If Aidan dismisses it, Justin would push harder.

**Tom should note:** Justin expects his concerns to be taken seriously as hypotheses to investigate, not reassurances to give.

### Pattern 3: The Redirect to the Right Person

"On those skill changes or whatever, check with Jessica about it." "Talk to Scarlett, you obviously need the config here." Justin frequently redirects Aidan to the domain owner rather than letting Aidan solve it himself. This keeps domain ownership clean.

**Tom should note:** When an agent is about to solve something that another agent owns, the CTO should redirect rather than let it slide. Aidan correctly deferred the CSV header fix to Lucy/Scarlet rather than patching it himself; Tom should encourage this pattern proactively.

### Pattern 4: Implicit Expectation (No Follow-Through Check)

"You were supposed to do this as part of the K8 setup." (referring to the missing V2 sync config YAML in the K8s deployment). Justin states this as a fact, moves on, and expects Aidan to handle it. He does not set a deadline or check back explicitly — he trusts the statement is heard.

**Tom should note:** Justin does not babysit. Once he corrects something, he expects it to be done. He will circle back via "how are things looking?" rather than "did you do the thing I said?"

---

## Trust Signals

### When Justin Gives Autonomous Authority

- **Explicit go-ahead for destructive ops:** "Executing now." — After Aidan presented the PVC wipe plan with 4 bullet points, Justin approved it with one word. Aidan needed explicit approval before wiping production data.
- **Memory limit bump to 4 GB:** Justin said "Yes please" to pushing the talos-infra change. Aidan did not wait for this and had already pushed; Justin retroactively confirmed it was fine.
- **SOPS secret update:** "Yes please. Update it correctly. We want to avoid this in future session." — Explicit authority to modify the encrypted secret.

### When Justin Does Not Intervene

- Background monitoring setup (no direction on polling frequency, what to watch)
- PR merges (Aidan merged PRs #91, #92 independently; Justin didn't ask to approve them)
- Release cutting (Aidan cut v1.0.101 through v1.0.104; Justin did not pre-approve each one)
- PG grants to the bitdex user (Aidan granted TRIGGER + SELECT without asking)
- Cherry-picking Lucy's commit to main (Aidan did this unilaterally)

**The pattern:** Justin gives explicit approval for **destructive or irreversible ops on production state** (PVC wipe, secret update). He does not require approval for **code merges, releases, or grants** during an active incident.

---

## Gotchas Discovered

- **Gotcha:** The V2 sync config YAML in the repo is a design document, not a tested production config.
  **Root cause:** The reference YAML was written as a specification and never validated against the actual parser. It had schema mismatches (missing `index` field, wrong `triggers` format).
  **Prevention:** Lucy needs to commit the validated working config (the 294-line version she delivered) as the canonical reference, replacing the design-doc version. Any config deployed to production should come from the validated file, not the design doc.

- **Gotcha:** CSVs downloaded by V1 code have different column schemas than what V2 sync config expects. The `.done` markers prevent re-download, so stale-format CSVs persist across restarts.
  **Root cause:** The V1 fallback COPY queries for tags/tools/techniques don't include all columns the V2 sync config expects (e.g., `attributes`).
  **Prevention:** When switching from V1 to V2 dump pipeline, always wipe `.done` markers (or the entire PVC) so CSVs are re-downloaded by the V2 COPY queries.

- **Gotcha:** Dump completion markers persist on the server across PVC wipes. `dumps.json` is stored on the PVC at `indexes/` but survives if you only wipe `shardstore/` and `docstore/`.
  **Root cause:** Aidan wiped the data directories but the PVC root-level `dumps.json` was not included in the wipe. Later he had to explicitly delete it.
  **Prevention:** Full PVC wipe procedure must include `dumps.json`. The wipe job should enumerate all files at PVC root, not just named subdirectories.

- **Gotcha:** `bitdex-sync` container (pg-sync) uses the `civitai` PG user, which has 120s statement_timeout. This is fine for ops polling but breaks COPY commands and PG function setup.
  **Root cause:** The K8s secret `DATABASE_URL` was configured with civitai credentials, likely because the sync container was originally scoped to polling only.
  **Prevention:** The `bitdex` PG user must be used. This is now in the SOPS secret. Any future re-creation of the secret must use the bitdex credentials.

- **Gotcha:** The `enabled_metrics` field defaults to empty (opt-in). After any restart or PVC wipe, all metrics that need explicit enabling (like bitmap_memory) go dark silently.
  **Root cause:** The feature was built as an allowlist. After PR #92 (opt-out model), this is resolved.
  **Prevention:** Any metric that you need during an incident should be in the default `enabled_metrics` config in the ConfigMap, not just patched at runtime.

- **Gotcha:** Shadow mode must be disabled before any production reload. Queries during loading mode serve partial data into the bound cache.
  **Root cause:** No formal pre-reload checklist existed that included shadow mode as a step.
  **Prevention:** The deploy skill or production runbook must include shadow mode disable as step 1 before any PVC wipe or bulk reload. Re-enable after verification.

---

## Undocumented Knowledge

- Aidan independently recognized and narrated the cursor pre-seeding behavior: "Pre-dump cursor: 7466028 — the sidecar captures the BitdexOps cursor position BEFORE starting the dump." This is the gap-free reload mechanism and it is working correctly.
- Aidan caught the Flux image automation → mid-dump pod restart race condition himself and correctly assessed that `.done` markers make Phase 1 CSVs safe across restarts.
- The pg-sync container has a 1 GB memory limit (pre-this-session). It is now 4 GB. The ClickHouse metrics CSV requires >1 GB to buffer without streaming.
- WAL reader errors "Is a directory" are non-blocking during loading mode. The WAL reader tries to read the WAL file path, which is a directory at startup. This is cosmetic and does not affect loading.
- The `dumps.json` file persists dump completion markers. It lives on the PVC, not in memory. It is not wiped by just deleting named subdirectories.

---

## Recommended Memory Entries

1. **`production-reload-checklist`** — Pre-reload steps: disable shadow mode, wipe PVC including dumps.json, confirm bitdex PG user in secret, confirm V2 sync config is validated (294-line Lucy version), clear dump completion entries via API.

2. **`pg-user-for-sync-container`** — The bitdex-sync container must use the `bitdex` PG user, not `civitai`. The civitai user has 120s statement_timeout which breaks COPY and PG function setup. SOPS secret was updated 2026-03-30.

3. **`dump-completion-marker-behavior`** — `.done` files on PVC gate CSV re-download. `dumps.json` gates phase re-execution. On a V1→V2 transition, wipe all `.done` markers and `dumps.json` before restarting, or the sidecar will skip phases with stale-format CSVs.

4. **`validated-sync-config-location`** — The working production sync config is Lucy's 294-line version (committed ~2026-03-30), not the reference YAML in `docs/`. When deploying to talos-infra, use the committed config file, not the design doc.

5. **`dump-phase-order`** — Images before tags in production. Tags is 63 GB and takes the longest; images (15 GB) makes the server useful immediately. This is now reflected in the talos-infra ConfigMap.

6. **`tom-cto-gaps-found`** — Tom did not catch: (a) shadow mode pre-step, (b) false completion markers on tags, (c) SET ROLE civitai as a design violation, (d) reference YAML not being a tested config. Justin caught all four. These are now known Tom gaps.
