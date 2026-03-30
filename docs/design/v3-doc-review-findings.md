---
status: REVIEW
created: 2026-03-30
author: Conversation Reviewer (spawned by Dakota)
session_reviewed: 781a5fb6-dcd0-4fbd-9383-0f70c7b128d9
doc_reviewed: docs/design/v3-unified-mmap-architecture.md
---

# Session Review: Edward V3 Design Session

**Session:** 781a5fb6-dcd0-4fbd-9383-0f70c7b128d9 (the session that produced the doc also spans
session f537cacf-254a-499d-b4c1-6f703c938de3, which is a continuation of the same conversation
stored in a separate temp directory — both are part of the same Edward design session)
**Agent:** Edward (team lead)
**Date:** 2026-03-29 to 2026-03-30 (overnight session)
**Reviewer:** Conversation Reviewer (spawned by Dakota)

---

## Summary

The design doc at `docs/design/v3-unified-mmap-architecture.md` is largely accurate and covers
the core architecture decisions made in the session. However, there are several gaps and
mismatches worth calling out — particularly around: (1) the doc size discrepancy, (2) the
cache silo deferral vs Justin's explicit vision, (3) Justin's specific concern about starting
from scratch vs refactoring that shaped the src/v3/ decision, and (4) the query distribution
flag that Justin raised directly.

---

## Contradictions

### 1. Doc size 256 bytes "makes configurable" but rationale is confused

**What the doc says (Section 3.1):**
> "256 bytes is tight for Civitai docs (~230 bytes avg, some exceed with long URLs/many tags).
> 512 bytes provides headroom with ~55% waste."

**What actually happened in the session:**
The baseline Ollie ran showed DocStore V2 p50 doc size = **148 KB / p95 = 161 KB**. That is
the size of a full DocStore V2 shard record including all the tuple overhead and framing.

Mark's Experiment 2 measured a completely different thing: the flat binary encoding of a doc
in the mmap format came out at ~230 bytes average (25 GB total / 108M docs). These two numbers
measure different encodings and are not comparable.

The doc conflates these two measurements. "230 bytes avg" refers to the *new flat mmap format*
(Mark's prototype), not DocStore V2 documents. The doc needs to make this explicit: 230 bytes is
the V3 mmap encoding size, not the current stored doc size. The 148 KB figure is V2 and is
irrelevant to V3 slot sizing.

**Impact:** Anyone reading the motivation table in Section 1 will be confused by "DocCache heap:
1 GB" alongside "10us warm (cache)" if they also see 148 KB docs — the 1 GB DocCache with only
~7K docs would not explain the 10us warm reads. The correct explanation is that V2 docs are
large because they use a tuple log format; V3 docs are 230 bytes in flat binary. This needs
a clarifying sentence.

---

### 2. BoundStore deletion rationale is incomplete

**What the doc says (Section 4, deletion table):**
> `src/bound_store.rs | 1,083 | Replaced by cache_silo.rs (mmap'd cache persistence)`

**What actually happened:**
BoundStore's deletion was added late in the session after Edward discovered that BoundStore does
NOT use ShardStore (it has its own file format). The decision to delete it came from Edward's
realization that "In V3, cache persistence is just part of the cache silo's mmap file." This
was Edward's inference, not a direction Justin gave.

Justin's actual words about BoundStore (from the session at ~11:49 PM):
> "It would be cool if we could make it work for all aspects of the system rather than having
> to have multiple different ways that the system works."

Justin did not explicitly say "BoundStore goes away." Edward made that call. The doc presents
it as settled when it is actually Edward's architectural inference from Justin's general
direction. This may be correct, but should be labeled as "architectural decision from design
session" rather than just listed as a deletion without comment.

---

### 3. Cache silo deferred to V3.1 — but Justin's vision included it in V3

**What the doc says (Section 3.3):**
> "Unified Cache: Stays in heap (initially). 333MB, nanosecond lookup, live-maintained.
> Future (V3.1): Explore mmap'd cache with two-tier entry pools. Research complete (Ollie),
> implementation deferred — heap cache is not the bottleneck."

**What Justin actually said (at ~11:49 PM):**
> "It would be cool if we could make it work for all aspects of the system rather than having
> to have multiple different ways that the system works. I mean, I was kind of imagining that
> even for caches we could just have logs get applied to them, especially if we just keep them
> as raw bitmaps."

Justin's stated vision was "one storage pattern for everything including the cache." He
explicitly asked whether Ollie should spend time exploring it. Ollie did the research and
confirmed it was viable. The session summary at ~11:52 PM from Edward included:

> "One storage model for everything: Docs / Bitmaps / Cache — all mmap'd files"

The decision to defer cache to V3.1 was made by Edward when writing the doc, not by Justin.
The doc should document this as a **scoping decision made during doc writing** and note that
Justin's original vision included the cache in V3. The current wording "heap cache is not the
bottleneck" is accurate but understates the reason — Justin wanted cache included, and Ollie's
research showed it was viable; the deferral was a scope call, not a technical blocker.

**Regression risk:** If someone reads the doc and sees "heap cache is not the bottleneck" as
the justification for keeping it out, they may not realize Justin's intent was to eventually
unify everything. The V3.1 note preserves this but buries it.

---

## Missing from the Doc

### 4. Justin's direct question about starting from scratch

**What Justin said (at ~12:18 AM):**
> "Looking at the amount of stuff that's about to be rewritten, let me ask you, do you think it
> makes sense to continue with this code base or to just start from the ground up?"

And at ~12:29 AM:
> "With that in mind, do you really think there's that much worth saving versus just starting in
> a fresh directory? I mean, I guess v3 could just be fresh, could have everything new. I don't
> know. Maybe it's like we just port things from the old version into the v3 directory rather
> than using the old version at all. Because you're right, there's a lot of good stuff there,
> but just bring it out as we need it rather than referencing it at all."

This direct exchange with Justin is the **origin of the src/v3/ clean-room approach**. The doc
explains the approach in Section 4 but frames it entirely as Edward's reasoning ("V2 has 25-27K
lines of storage/concurrency code..."). It does not mention that Justin asked the question and
explicitly suggested the "port things from the old version into the v3 directory" model.

This matters for future agents: the clean-room approach is not just a technical preference — it
reflects Justin's stated desire to have V3 be clean and free of legacy cruft. That intent should
be in the doc.

**Recommended addition to Section 4:** A sentence like: "Justin's explicit direction was that
V3 should be a clean room — port proven pieces in as needed, rather than refactoring V2 in
place."

---

### 5. Justin's flag about query distribution being wrong

**What Justin said (at ~11:15 PM):**
> "Most queries are more than three ands, unfortunately. We actually have logs on the server
> that Aiden can grab... Also, I'm not sure where he got those numbers, because I'm actually
> looking at the numbers on Prometheus in production, and it says we have almost 100,000 cache
> entries and 333 megabytes being used for that."

The doc correctly notes in Section 8 (Open Questions) that "Replay harness used synthetic
workload. Aidan enabling traces on v1.0.101 for real production queries." But it does not record
the specific thing Justin said: that **most queries have 3+ AND clauses**, contradicting the
80% single-filter assumption from the loadtest. This is material to the risk assessment.

**What the doc says in the risk table (Section 9):**
> "Frozen AND 1.3x slower at 3 ANDs (real data) | Bigger cache (95% hit rate) reduces cold
> path frequency."

The mitigation is reasonable, but the doc does not say "Justin confirmed production queries
have 3+ ANDs." This matters because the 1.3x overhead is measured at exactly the typical query
complexity Justin described. The risk is not speculative — it is the typical production case.

---

### 6. Justin's clarification about the flush cycle

**What Justin asked (at ~12:00 AM):**
> "Does this make the flush cycle go away completely too? Basically, the janitor is the flush
> cycle now, in a sense. But readers read through ops on top of snapshots?"

The doc does not have a section on how the flush cycle maps to V3 concepts. Edward explained
this during the session (ops log is the WAL, janitor is the compactor, readers apply pending
ops at read time), but none of this mapping is in the doc. A new engineer reading the doc will
not know what happens to the existing flush thread, ArcSwap, and staging InnerEngine.

The doc mentions "V2 has 25-27K lines...ArcSwap...flush thread model" in the motivation for
src/v3/ but does not explicitly say "the flush thread is replaced by the mutation thread and
janitor." Section 3.4 (Ops Log) partially covers this but only describes the new pattern, not
the mapping from V2 concepts.

---

### 7. Ivanna's histogram PR was mentioned but not referenced

**What Justin said (at ~11:19 AM):**
> "Good call on the histogram need there. You should reach out to Ivana and have her add that
> as well. She's the one that focuses on Prometheus metrics."

The doc references this in Open Question #2: "Ivanna shipped PR #92." But the session was from
2026-03-29/30 and PR #92 is listed as "once deployed, validates query complexity distribution."
This was an action item from the session that should be tracked. The doc references it as
already done ("shipped"), but Justin asked for it during this session, suggesting it was
not yet deployed when the doc was written.

---

## Numbers That Do Not Match the Conversation

### 8. Code removal total: doc says ~11,700 lines deleted, Mark reported ~10,000

**What Mark reported (~12:03 AM):**
> "Files deleted entirely: ~10,000"
> "Major rewrites (~60-70% new): ~15-17,000"

The doc's deletion table totals ~11,700 lines (after adding bound_store.rs), which is
higher than Mark's ~10,000 figure. This is because BoundStore (1,083 lines) was added to
the deletion list after Mark's audit. The discrepancy is explainable but creates a potential
confusion: Mark said ~10K, the doc says ~11,700. The doc should note "~10K from Mark's audit
plus bound_store.rs (~1,083 lines) added post-review = ~11,700 total."

---

### 9. Cache entry count: 100K entries at 333MB (from Justin via Prometheus)

**What Justin said (at ~11:15 PM):**
> "I'm actually looking at the numbers on Prometheus in production, and it says we have
> almost 100,000 cache entries and 333 megabytes being used for that."

**What the doc says (Section 3.3 and Experiment Evidence):**
> "100K entries at 333MB (3.33KB/entry). 71.6% hit rate."

The 100K and 333MB numbers came directly from Justin reading Prometheus in real time. The doc
correctly attributes them to "prod data from Aidan" in the cache analysis section, but it
was Justin who provided the numbers. The 71.6% hit rate also came from Justin ("our actual
cache hit rate's about 70% for some reason"). The doc should say "from production Prometheus
(Justin, 2026-03-30)" to make the source clear and distinguish this from a benchmark result.

---

## Design Decisions Agreed But Not Documented

### 10. Agent confusion solved by the src/v3/ boundary

Justin's follow-up at ~12:30 AM to Edward's pitch:
> "You're right — and Mark's phased approach actually supports this... This also solves the
> agent confusion problem — agents working on V3 only look at src/v3/."

The doc mentions "Agent confusion from V2+V3 coexistence" in the risk table with the mitigation
"Clean src/v3/ boundary. Agents only work in v3/." But it does not document this as a primary
*motivation* for the clean-room approach. It is treated as risk mitigation rather than a design
goal. For a project built with AI agents, this is an important distinction — the architecture
was partly designed to reduce cognitive load on future agents.

---

### 11. The "one storage pattern for everything" principle was Justin's vision, not just Edward's

The design doc opens with "One pattern for everything" as if it is a natural architectural
conclusion from the experiments. In the session, Justin stated this vision explicitly at
~11:49 PM and at ~12:00 AM. He framed it as something he was "hoping" the architecture would
achieve. The doc presents it as a given rather than as the project owner's stated goal.

Documenting that this is Justin's explicitly stated vision matters: it signals that trade-offs
that compromise the "one pattern" principle should be escalated to Justin rather than resolved
locally by engineers.

---

## Recommended Memory Entries

The following items from this session are not captured in existing memory files and would be
valuable to add:

1. **V3 design session decision log** — Create `project_edward_v3_design_session.md` in
   memory/ capturing: (a) Justin's "start from scratch" question and answer, (b) the cache silo
   deferral rationale, (c) Justin's confirmation that most production queries have 3+ ANDs,
   (d) the BoundStore deletion decision.

2. **Query complexity fact** — Justin confirmed from production Prometheus that real queries
   have 3+ AND clauses. The loadtest's 80% single-filter assumption is NOT representative of
   production. This contradicts the benchmark framing in Experiment 4 and should be in memory
   as a caveat on all frozen bitmap benchmarks.

3. **Cache entry count source** — The 100K / 333MB / 71.6% hit rate numbers came from Justin
   reading production Prometheus live during the session (2026-03-30). Not from a benchmark.
   Should be attributed correctly in any downstream docs.

4. **Doc size encoding difference** — 148 KB (V2 tuple log format per doc) vs 230 bytes
   (V3 flat mmap format). These are different encodings, not a compression win. Future agents
   working on the V3 doc silo must know that the slot size (512 bytes default) is based on the
   V3 flat format, not DocStore V2 format.
