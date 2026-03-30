---
status: FINAL
created: 2026-03-30
author: Conversation Reviewer (spawned by Dakota)
session: 781a5fb6-dcd0-4fbd-9383-0f70c7b128d9
subject: Edward V3 DataSilo team lead — bulk load integration failure, dual-write mistake, review process gap
---

# Session Review: Edward — V3 DataSilo Implementation Post-Mortem

**Session:** 781a5fb6-dcd0-4fbd-9383-0f70c7b128d9
**Agent:** edward (bitdex-v2)
**Date:** 2026-03-30 (~9:00 AM through ~2:20 PM)
**Reviewer:** Conversation Reviewer (spawned by Dakota)
**Purpose:** Post-mortem requested by Edward and Justin. Extract root causes, process failures, and rules for Edward's agent definition.

---

## Session Goal

Edward led Mark and Ollie through V3 DataSilo implementation (Phases 1–5), followed by a 107M validation run. The session ended when the V3 save phase tested 82% slower than V2 — a critical performance violation that halted validation and triggered this post-mortem.

---

## Timeline of What Happened

### Phase 1–5 implementation (~9:00 AM – ~10:45 AM)

Both engineers built through the implementation plan with reviewer-gated progression. Edward spawned sub-agent reviewers at each sub-task boundary. Results were largely clean — Mark needed fixes in several phases (two-path write not wired, fsync gap, atomic swap, Arc snapshot), while Ollie passed most reviews cleanly.

At ~10:14 AM Mark delivered `V3BitmapLoadSink` — the integration piece that connects the dump pipeline to the V3 bitmap silos. This was the last structural wiring piece before full validation could run.

By ~10:44 AM, after several wiring bugs (borrow-after-move in server.rs, process_dump vs process_dump_v3 handler mismatch), Ollie completed an images-only dump at 108.9M rows in 384.8s. The V3 alive bitmap worked correctly (108.9M entries). Filter bitmaps for type (225K values) and postId (4.8M values) populated correctly.

**What appeared to be passing:** V3 architecture was responding to queries at 108M scale. The alive bitmap was correct. Two of the simpler fields (type, postId) populated.

### Full multi-phase dump attempt (~12:13 PM – ~12:57 PM)

After compaction and context refresh, Ollie kicked off a full multi-phase dump (tags first, being the heaviest at 89 GB). At ~12:51 PM the server was at 16.4 GB RSS and still processing the tags save phase. The problem surfaced: the dual-write design meant every bitmap write hit both V2 ShardStore and V3 BitmapSilo simultaneously. The save phase was not completing in any reasonable time.

Justin cancelled the run at ~12:57 PM.

### V3-only dump mode built (~1:16 PM – ~1:52 PM)

Justin directed that a V3-only dump mode be built — no V2 writes at all. Ollie built it (commit 3af9376). The images-only dump ran with V3 writes only.

**Row processing phase result:** 375K rows/sec — 11.6% faster than V2 baseline of 336K rows/sec. Appeared to be a win.

**Save phase result:** The save phase was catastrophically slow. Final numbers:

| Phase | V2 | V3 | Delta |
|-------|----|----|-------|
| CSV row processing | 336K rows/sec | 375K rows/sec | +11.6% |
| Save to disk | fast (parallel rayon in ShardStore) | slow (sequential DataSilo) | approximately -82% |
| Total dump time | 324s | 589s+ | +82% slower overall |

Edward halted immediately per the "V3 must beat V2" rule. Both agents were stood down.

### Root cause identified (~2:11 PM – ~2:16 PM)

Edward delivered a self-assessment of where the review process failed (quoted in full below under Root Causes).

Edward sent Dakota a conversation review request at ~2:18 PM. This document is the result.

---

## Root Causes

### Root Cause 1: The dump pipeline was calling put() instead of bulk_load()

The design doc for the DataSilo crate specifies a two-path write model:
- `put()` — single entry write (steady-state mutations, one bitmap at a time)
- `bulk_load()` — multi-threaded parallel bulk write (5.53M entries/sec at 32 threads, proven in benchmark)

What the dump pipeline actually did via `V3BitmapLoadSink.flush_to()`:

```
save_phase_to_v3 calls add_filter_bitmap(field, value, bitmap)  [one at a time]
  -> each call does: serialize frozen -> silo.put() -> single mmap write + index update + ops log append
```

This is the steady-state mutation path, not the bulk load path. The Phase 0 benchmark validated `bulk_load()`. The dump pipeline never called it.

**In Edward's words:**
> "What's actually happening in the dump pipeline: `save_phase_to_v3` calls `add_filter_bitmap(field, value, bitmap)` **one at a time**. Each call does: serialize frozen → `silo.put()` → single mmap write + index update + ops log append. This is the steady-state mutation path — single-entry, single-threaded, with full ops log overhead on each call. The design doc says `bulk_load` uses 32 threads and 5.53M entries/sec. We never called it."

### Root Cause 2: The V3BitmapLoadSink was designed wrong from the start

`V3BitmapLoadSink` was built as a connector piece: the dump pipeline calls `flush_to()` on the sink, which then calls `silo.put()` for each bitmap. This design decision was made without verifying which DataSilo API the bulk path required.

The sink connected two correct-in-isolation components via the wrong API. Mark's DataSilo crate was correct. Ollie's BitmapSilo was correct. The sink that connected them to the dump pipeline was wrong.

### Root Cause 3: Phase-by-phase review missed the integration boundary

Edward's self-assessment:

> "**Phase-by-phase review missed the integration.** I reviewed each sub-task against its plan checklist — 'does put() work? does bulk_load() work? do tests pass?' — but never reviewed the *connection* between them. The dump pipeline (Mark's code) and the silo layer (Ollie's code) were reviewed independently. Nobody verified that the dump pipeline actually uses the fast path."

The review process was oriented toward per-component correctness. No review checkpoint was designed to ask: "does the dump pipeline call bulk_load() instead of put()?" That question exists at the boundary between two separately-reviewed components — and boundary reviews were not part of the process.

### Root Cause 4: The dual-write design was a conceptual mistake

`process_dump_v3()` was implemented as a wrapper that writes V3 data first, then lets the V2 path run normally — both V2 ShardStore and V3 BitmapSilo receive every write.

**In Edward's words:**
> "Mark's `process_dump_v3()` was designed as a wrapper that writes V3 *before* V2, then lets V2 run normally. It was the quickest way to get V3 populated without touching the existing V2 dump path — a feature-flag approach where both engines get data simultaneously. But you're right — it's wrong. The whole point of V3 is to **replace** V2, not layer on top. We should have built the V3 dump as a standalone path from the start, not a wrapper around V2. The dual-write was a shortcut that was wrong."

This caused the full multi-phase dump to become essentially unfeasible — tags alone generates ~788K unique bitmaps across 108M slots, and writing all of them twice (once to ShardStore, once to BitmapSilo) multiplied the I/O cost while also masking whether V3's performance was actually acceptable.

---

## Process Failures (What Reviews Should Have Caught)

### Failure 1: No integration-level benchmark gate

The plan included Phase 0 (DataSilo standalone benchmark at 5.53M entries/sec). Phase 0 passed. But there was no equivalent gate for Phase 6 validation that asked: "does the end-to-end dump use the fast path?" Phase 0 benchmarked `bulk_load()` in isolation. Phase 6 ran the full dump. The gap between "does bulk_load work?" and "does the dump pipeline call bulk_load?" was never reviewed.

**What should have happened:** An integration benchmark sub-task — run a small (10M) dump with V3 writes and compare rows/sec against V2 baseline before proceeding to full validation. This would have surfaced the put()-vs-bulk_load() problem within the first integration test, not at the end.

### Failure 2: Sink design not reviewed for API correctness

`V3BitmapLoadSink.flush_to()` was reviewed for: does it compile, does it connect the sink to the silo, do tests pass? It was not reviewed for: does it call the correct DataSilo API for bulk loading?

The reviewers checked Mark's DataSilo (`bulk_load` has the right signature, tests pass) and Ollie's BitmapSilo (field storage, frozen bitmaps, correct) independently. No reviewer was asked to check whether the sink, which bridges the dump pipeline to BitmapSilo, calls the fast path.

**What should have happened:** A specific review checkpoint when the sink was delivered — "verify that flush_to() batches all entries into a single bulk_load call, not individual put() calls." One line of code inspection would have caught this.

### Failure 3: Dual-write never questioned during planning

The plan described `process_dump_v3()` as writing to V3 silos during the dump. The plan did not specify whether this was additive (V2 + V3) or replacement (V3 only). The implementation defaulted to additive (easier to build — just wrap V2). No reviewer flagged this as wrong.

**What should have happened:** The planning phase should have stated explicitly: "V3 dump mode writes to V3 only — V2 writes are disabled behind the feature flag." The phrase "V3 replaces V2" should have been operationalized in the plan as a test: "dump with v3 feature flag produces zero V2 ShardStore writes."

### Failure 4: The "must beat V2" rule was codified too late

Justin's rule — "if any V3 operation is slower than V2, STOP immediately" — was written as feedback memory (`feedback_v3_perf_rule.md`) after the performance violation, not before. The rule was known conceptually, but it was not an explicit gate in the implementation plan.

**What should have happened:** The "must beat V2" baseline requirement should have been a numbered gate in Phase 6, with explicit V2 comparison numbers documented at plan time. V2 dump baseline was known (324s at 108M = 336K rows/sec). That number should have been in the plan as the target, not discovered post-failure.

---

## Concrete Rules for Edward's Agent Definition

The following rules should be added to Edward's agent definition to prevent a repeat of this failure pattern.

### Rule 1: Require an integration benchmark before full validation

When a new data path is connected between two separately-built components, run an integration benchmark at 10M scale before committing to a full 107M run. The benchmark must compare against V2 baseline. If V3 is slower at 10M, investigate before running at scale. Do not proceed to 107M validation until 10M integration benchmark passes.

**Operationally:** "Does the dump pipeline actually call bulk_load()?" is an integration question, not a unit test question. Include it as a required check in any phase that wires the dump pipeline to a new storage backend.

### Rule 2: When reviewing a sink/adapter/connector, verify the API callsite

When reviewing any component that connects two subsystems (a sink, an adapter, a bridge), include a specific checklist item: "verify that the connector uses the correct API for the throughput class of the operation." For bulk operations, verify `bulk_load()` not `put()`. For single-entry mutations, verify `put()`. The API choice is not obvious from tests — it requires reading the callsite.

### Rule 3: "V3 replaces V2" must be operationalized in the plan

Any V3 dump implementation must be built as a standalone path from the start. The plan checklist for any dump-path phase must include: "dump with V3 enabled produces zero V2 ShardStore writes (verify by checking V2 data directory is not modified)."

Dual-write is not an acceptable intermediate state. It doubles I/O, masks performance characteristics, and obscures whether V3 is actually ready to replace V2.

### Rule 4: Document V2 baseline numbers in the plan before validation begins

Before Phase 6 (validation), document the V2 baseline numbers in the implementation plan file. Specifically: dump throughput (rows/sec), total dump time (seconds), and save phase time. These become the pass/fail criteria for V3 validation. Do not start a 107M run without knowing what number V3 needs to beat.

V2 baseline for reference: 108.9M rows in 324s = 336K rows/sec. Save phase was fast (parallel rayon). Any V3 result exceeding 324s total is a failure.

### Rule 5: When a long run stalls or looks wrong, cancel immediately

When monitoring a long dump run (>5 minutes), if the save phase is running longer than the row-processing phase, that is a performance anomaly. Cancel the run immediately and investigate before continuing. Do not wait for completion to discover the problem.

**Practical signal:** If row processing takes N seconds and save phase has been running for >N seconds with no progress indication, something is wrong.

---

## Gotchas for Future Agents Working on V3

### Gotcha 1: put() vs bulk_load() — these are not interchangeable

`DataSilo::put()` appends to the ops log with full CRC32 + generation metadata overhead on each call. It is designed for steady-state single-entry mutations.

`DataSilo::bulk_load()` uses rayon with 32 threads, writing directly to shard regions, building the index table in a single pass after all threads complete. It is designed for dump-time bulk population.

For any code that writes hundreds of thousands of entries during a dump phase, always use `bulk_load()`. The performance difference is not incremental — `put()` at scale produces the 589s result, `bulk_load()` is expected to produce results closer to the 324s baseline.

The two functions have different call signatures. `bulk_load()` requires all entries to be batched before the call. The correct pattern in `V3BitmapLoadSink.flush_to()` is to accumulate all (field, value, bitmap) triples during the phase, then call `bulk_load()` once per silo at flush time.

### Gotcha 2: The dual-write trap

The easiest way to add a new storage backend to the dump pipeline is to call it before the existing path: write to V3, then let V2 run. This is wrong for any backend that is meant to replace V2, not supplement it.

The correct approach: a feature flag that disables V2 writes entirely (`#[cfg(feature = "v3")]` or similar) so the dump pipeline follows exactly one path. Dual-write doubles I/O and makes performance comparison impossible.

### Gotcha 3: Long save phases are a signal, not just a wait

The tags phase at 88 GB generates ~788K unique tagId bitmaps across 108M slots. Writing all of those to disk takes real time. But if the save phase is dramatically slower than expected — specifically slower than V2's parallel rayon-based ShardStore — that indicates something is wrong with the write path. It is not just "tags is big."

At 108M scale: V2 saves tags in parallel using rayon across multiple shards. If a new implementation does not use parallel saves, the save phase for tags alone can take longer than the entire V2 dump. This is the failure mode that surfaced in this session.

### Gotcha 4: Watcher death causes compounding communication failures during long runs

Agents monitoring long-running dump operations (5+ minute runs) regularly lose their mailbox watcher processes. This creates a gap where messages (including cancellation requests) are not received until the agent's next watcher check or sleep cycle wakes. During a failed run, this means multiple attempts to cancel a dump may go unprocessed for 4–5 minutes each.

For long-running validation operations: use explicit sleep-based polling with short intervals (60s max) and check mail on every wake cycle. Do not rely on watcher processes staying alive through OS-level I/O stress.

### Gotcha 5: An images-only dump cannot validate filter fields that require enrichment

Fields like nsfwLevel, userId, and collectionIds are populated from data in non-images CSVs (posts.csv, resources.csv, etc.) via the enrichment chain. An images-only dump will show those fields as empty, which looks like a V3 bug but is actually just a missing data phase.

Before concluding that filter bitmaps are broken, verify which CSV phases the field depends on. The enrichment chain for nsfwLevel is: images.csv falls back to combinedNsfwLevel, which comes from resources/posts. No resources phase = no nsfwLevel values.

---

## Undocumented Knowledge

1. **V3 dump must disable V2 writes via feature flag, not wrap them.** This is not stated anywhere in the design doc or implementation plan. The plan says "dump pipeline writes to V3 silos." It does not say "and disables V2 writes." A future reader of the plan would naturally implement this as a wrapper (the easy path). The plan needs an explicit statement: V3 feature flag = V2 writes disabled.

2. **The "must beat V2" rule needs V2 numbers in the plan.** The feedback memory `feedback_v3_perf_rule.md` contains the rule but not the numbers. An agent reading that rule doesn't know what they're racing against. The V2 baseline (324s, 336K rows/sec) should be embedded in the implementation plan as a boxed requirement.

3. **Phase 0 benchmark validates isolation, not integration.** The Phase 0 synthetic benchmark (5.53M entries/sec at 32 threads) proves the DataSilo crate works in isolation. It does not prove that the dump pipeline calls `bulk_load()`. These are orthogonal questions. The plan needs an explicit integration gate that connects the two: run a 10M dump through the full dump-to-silo path and measure rows/sec.

4. **107M V3 validation sequence is specific.** The correct sequence for running V3 validation after a fresh build: (1) clean data directory, (2) run full multi-phase dump with feature flag v3 and no V2 writes, (3) verify all CSV phases complete, (4) run comparison queries against known V2 results, (5) compare timing against 324s baseline. Skipping any step produces misleading results.

5. **nsfwLevel field mapping gap.** The civitai-index.yaml config maps nsfwLevel with `fallback: combinedNsfwLevel`. During images-only validation runs, combinedNsfwLevel is not available, so nsfwLevel ends up with a single entry in the filter map. This is a data gap, not a V3 bug. Mark's diagnostic confirmed the issue is upstream in PhaseResult, not in the V3 sink iteration logic.

---

## Recommended Memory Entries

1. **`feedback_v3_dump_must_be_v3_only`** — V3 dump mode must produce zero V2 ShardStore writes. Dual-write (V3 before V2) doubles I/O, masks performance, and is wrong by design. Implement as a feature flag that disables V2 writes entirely.

2. **`feedback_v3_integration_benchmark_required`** — Before running a 107M validation, run a 10M integration benchmark against the V2 baseline (336K rows/sec). If V3 rows/sec is lower at 10M, investigate the API path (put() vs bulk_load()) before scaling up.

3. **`feedback_v3_must_use_bulk_load_not_put`** — For dump-time population of DataSilo, the sink must batch all entries and call `bulk_load()` once per silo, not call `put()` per entry. put() is for steady-state single-entry mutations. bulk_load() is for dump-time batch writes. Using put() at 108M scale produces 589s; bulk_load() is expected to match the 324s V2 baseline.

4. **`insight_v3_baseline_numbers`** — V2 dump baseline: 108.9M rows in 324s (336K rows/sec), save phase is fast (parallel rayon). Any V3 result that exceeds 324s total is a failure. Stop and diagnose.

5. **`feedback_edward_integration_gate`** — Edward's agent definition should include: when two separately-built components are connected by a sink or adapter, always spawn a reviewer with the specific question: "does this connector call the correct API for the throughput class of the operation?" This is not covered by per-component unit test reviews.

---

## Summary Assessment

The implementation work itself was high quality. Mark and Ollie delivered ~5,000 lines across ~30 commits, with 130+ tests and a review process that caught 10+ real issues. The architecture is sound — the alive bitmap worked at 108M scale, the executor responded to queries, the filter bitmaps for correctly-populated fields were accurate.

The failure was one of integration: a single line in `V3BitmapLoadSink.flush_to()` called `put()` instead of `bulk_load()`. That single mistake, combined with a dual-write design that was inherently wrong, turned what should have been a validation pass into an 82% performance regression.

The review process that Edward ran was strong at the per-component level but had a structural blind spot at integration boundaries. Reviews verified components in isolation; no review was designed to verify that the components were connected correctly for the throughput requirements of the operation.

The fix is straightforward (rewire flush_to to batch entries and call bulk_load once), but the process change is more important: add an explicit integration benchmark gate between "components built" and "full validation run."
