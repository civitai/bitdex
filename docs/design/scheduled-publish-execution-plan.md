# Scheduled-Publish Fix — Execution Plan

**Status: KICKED OFF 2026-07-16 — final adversarial review PASSED** ("no attack breached the core
value-not-event mechanics"); findings folded below as [PR-n]. Wave 1 launched. Executes
`scheduled-publish-design.md` **§2.D (upstream-maximal, engine frozen)**. Owner: alexandra
(coordinator). All engine options (A/B) parked; goal = retire Meili image feeds.

## What we are doing (one paragraph)

PG becomes the author of every per-image value BitDex holds. The Post publish fan-out stops being a
query BitDex resolves ("`postId eq X`") and becomes per-image op rows resolved inside the publishing
transaction. `Image."sortAt"` (existing, dead column) is maintained by triggers
(`GREATEST(post.publishedAt, scannedAt, createdAt)`; NULL publishedAt → `GREATEST(scannedAt,
createdAt)`) and ingested by BitDex through the dormant `sortAtUnix → sortAt` mapping, replacing the
engine-computed GREATEST. An upstream re-emitter cron re-asserts publish values for recently-due
posts, making every miss self-heal within the lookback window. `model3dId` (Post column) gets
indexed for Meili parity. **Zero BitDex engine changes** — config, trigger codegen (sidecar), and
model-share only.

## Success criteria (measured, pre-registered)

1. Shadow-compare divergence vs Meili ~0 and holding through a full publish cycle **including
   scheduled posts crossing Tf** and at least one reschedule + one unpublish. **[W1-4 measurement
   flag]: future-dated (scheduled) slots MUST be excluded/normalized in the compare** — Meili shows
   them (query-time gate), BitDex hides them (isPublished gate) BY DESIGN; ~4,249 scheduled posts
   exist at any time and would read as a false regression at the Newest head.
2. `query_op_set_*` counters for the Post fan-out go to zero (path unused).
3. PG↔BitDex sortAt sampled reconcile clean over 48h (instrument proven able to fire first, on a
   seeded mismatch).
4. Verifier orphan/publish-lag counters: no NEW confirmed drops post-cutover (`redriven_total`
   flat); re-emitter heal events observable (its ops land as no-ops ≥99% — a high no-op rate is
   SUCCESS, it means nothing needed healing).
5. No P50/P95/P99 regression on image queries; publish-txn latency within gate (see W1-3).

## Work waves (each code task: Opus agent, worktree, PR, independent Opus reviewer, my audit vs
design doc, Justin's final approval before merge — per standing rule)

### Wave 1 — parallel, no interdependencies
- **W1-1 (#7)** `trigger_gen` per-image materialized fan-out type (bitdex-v2 repo, Rust sidecar
  codegen). New trigger type emitting `INSERT INTO "BitdexOps" SELECT i.id, <ops> FROM "Image" i
  WHERE i."postId" = NEW.id`. Post trigger only; ModelVersion/Model untouched (millions-wide).
  Generated-SQL snapshot tests + ops-shape tests. Also: Image trigger emits
  `sortAtUnix = extract(epoch from NEW."sortAt")::bigint * 1000` with COALESCE(GREATEST...) belt.
  **[PR-M1, PINNED — misbuild hazard]: the per-image Post fan-out RETAINS the full payload
  {publishedAt, availability, postedToId}.** publishedAt keeps flowing per-image — it drives BOTH
  deferred-alive activation stamping AND the isPublished shadow. Do NOT model this on design §2.2
  (Options A/B delete publishedAt — those are PARKED). Only Phase 2 ever removes
  availability/postedToId; publishedAt stays until an engine simplification is separately decided.
  **[PR-m2]**: ops-shape test asserting the Post fan-out and the Image sortAt trigger never emit
  the same field for one image (disjointness is what makes double-emission safe; op_dedup would
  LIFO-resolve an overlap nondeterministically).
- **W1-2 (#3)** model-share Prisma migration: `BEFORE INSERT OR UPDATE` Image sortAt trigger +
  `AFTER UPDATE OF "publishedAt"` Post→Image UPDATE trigger + drop `@default(now())` on sortAt.
  **Worktree in model-share, PR there.** Must verify: no recursion, anti-bump guard
  (post.service.ts:906-918), model-version publish/unpublish paths, and that the Post→Image UPDATE
  uses `IS DISTINCT FROM` to avoid no-op row churn.
- **W1-3 (#4)** Re-emitter design + sizing doc: home (default: Civitai app job infra — pg_cron
  availability on managed PG unconfirmed), lookback (default 15 min), volume estimate from prod
  (posts published per window × images/post), idempotency argument, metrics. Design doc first;
  implementation PR after Justin skims sizing. **[PR-M3, REQUIRED]: an unpublish-race fence** —
  "idempotent" is not ordering-safe: a re-emit whose SELECT snapshotted publishedAt=T just before a
  concurrent unpublish commits can land its `Set publishedAt=T` AFTER the unpublish's remove op =
  ghost re-publish. Fence options (pick one, justify): re-read inside the emitting txn; exclude
  posts with updatedAt newer than the lookback read; version-stamp. **[PR-B2 — CORRECTED by W1-4/PR-m5 prod evidence 2026-07-16]: prod scheduled slots are NOT in
  the deferred map.** They are ALIVE with all bits set, gated by an explicit isPublished=false
  shadow (empirical: postId query returns them; isPublished:false matches 10/10). The Post fan-out
  flow takes a quarantine branch (future publishedAt held in the doc, not applied) — `activate_due`
  only drives map-resident slots, which this flow doesn't create. **Tf activation for the dominant
  flow = shadow flip via the overdue sweep (600s) + opportunistic recompute** — the sortAt-layer
  half of which is the unreliable part (= W1-4's 7-28% misorder class; and the 2026-07-03 ~49.7k
  stuck-invisible audit, ops_processor.rs:1450). ⇒ **the re-emitter is MORE load-bearing than
  "safety net"** — a re-emitted past-value `Set publishedAt` is the reliable flip trigger.
  [PR-B1] (re-emitter ON before the shadow window) is therefore mandatory, already ordered.
- **W1-4 (#6)** sortAt divergence specimens via compare endpoint (read-only investigation;
  validates one-root theory; informs W3 acceptance).

### Wave 2 — after W1-1 merges (config depends on the new trigger type)
- **W2-1 (#8)** Sync + index config redo (bitdex-v2 + talos-infra mirror): Image trigger
  +sortAtUnix +model3dId track_fields; Post trigger → per-image type; posts enrichment +model3dId;
  index config: sortAt drop `computed:` (SAME deploy as emission flip), +model3dId filter
  (single_value per_value_lazy). Delete/refresh stale prod-sync-config-civitai.yaml.
  **[PR-B3, must-fix — the earlier shorthand was a SQL TYPE ERROR]**: `Image."sortAt"` is a
  TIMESTAMP; the GREATEST belt operands are bigint epoch-seconds. The dump copy_query must be
  spelled with explicit casts, e.g.
  `extract(epoch from COALESCE(i."sortAt", to_timestamp(GREATEST(existedAtSecs, publishedAtSecs))))::bigint`
  — never `COALESCE(timestamp, bigint)`. W3 gains an assertion: dump-produced sortAt ==
  steady-state trigger-produced sortAt on the same rows.
  **[PR-B4, must-verify BEFORE cutover — units of the period filter]**: the Civitai builder's
  period filter already sends `sortAtUnix Gte <seconds>` (image.service.ts:3940) while the plan
  pins EMISSION in ms (mapping ÷1000 → seconds in the layers). Resolve explicitly: which field do
  `sortAtUnix` filter clauses resolve against (the seconds-stored sortAt layers via the data_schema
  mapping/time-bucket snapping, or something else), and does the period filter even function today
  given sortAtUnix is emitted by nothing? Outcome must be a single pinned unit statement covering
  BOTH the emission chain AND the query-filter chain, with a test. (Memory gotcha corroborates:
  "sortAt seconds correct; sortAtUnix ms wraps with truncate_u32.")
- **W2-2 (#5+#10)** model-share query builder: +model3dId filter to getImagesFromBitdexPreFilter;
  re-diff BitDex vs Meili builders for any new Meili-only filter. (model-share worktree + PR.)
- **W2-3 (#11)** Backfill script (batched keyset, paced, replica-lag watched) + PG↔BitDex sortAt
  audit reconcile (stood up BEFORE cutover; alarm proven on seeded mismatch).

### Wave 3 — gate (blocks rollout)
- **W3 (#9)** Local full-scale proof at 107M: new dump config end-to-end; steady-state via
  bitdex-sync + generated triggers against local PG; scheduled-post lifecycle E2E (schedule →
  images pre-Tf invisible → cross Tf → visible with correct sortAt; reschedule; unpublish);
  publish-txn latency measured at P99 images-per-post + model-version worst case ([AR-5]).
  **[PR-M2]: latency gate EXPANDED to steady-state** — the Image BEFORE trigger adds a Post
  subselect to EVERY image INSERT/UPDATE (scannedAt bumps, tag edits, all mutations), not just
  publishes; measure per-image-update overhead at prod write rate. **[PR-m1]**: postId-CHANGE test
  (image moved between posts: sortAt recomputes from NEW post, postId filter bit moves).
  **[PR-m4]**: re-emitter heal-path E2E (inject a missed op, verify heal).
  **[W1-4 findings — REQUIRED W3 coverage]** (docs/_in/sortat-divergence-specimens-2026-07-16.md):
  the DOMINANT prod bug is *visible-but-misordered* — publishedAt DELIVERED correctly but the
  engine's sortAt recompute silently fails on ~7–28% of recently-published images (persists days,
  healed only by redump). Ingested sortAt deletes that mechanism — W3 must assert: publish a post
  locally, verify EVERY image's sortAt equals the PG value (not just visibility). Cover BOTH
  scheduled populations: image inserted AFTER scheduling AND image alive BEFORE scheduling —
  prod evidence (PR-m5): BOTH resolve to alive-with-isPublished=false via the quarantine branch;
  the deferred map is bypassed by the Image→Post flow entirely. The gate, not sort position, hides
  them; it must stay load-bearing. **CRITICAL W3 ASSERTION (the case the current engine gets
  wrong): a scheduled slot crossing Tf WITH NO OP ARRIVING must both flip isPublished AND get
  sortAt == publishedAt, via sweep/re-emitter alone.**

### Wave 4 — staged prod rollout (needs Justin at each gate)
- **W4 (#12)**: (1) model-share migration triggers deploy (write-only, nothing reads sortAt);
  (2) backfill runs behind them — **MUST COMPLETE (or pause) BEFORE step 4**: once the regenerated
  sync triggers (which emit sortAtUnix on Image updates) are live, a running backfill's UPDATEs
  would stream millions of BitdexOps rows through the WAL; before step 4 the old trigger ignores
  sortAt and the backfill is ops-silent, with the redump (step 5) carrying historical values into
  BitDex instead; (3) audit reconcile clean; (4) regenerated sync triggers deploy
  (add-only flip pattern per v1.1.28 nuke-op playbook) — **[PR-M5] sortAtUnix is emitted but
  DORMANT here** (mapping inactive until step 5's config flip activates it atomically with the
  `computed:` drop — computed and ingested NEVER coexist; add a pre-flip test that the dormant
  mapping drops sortAtUnix cleanly); (5) config cutover deploy (computed→ingested atomic) +
  **redump** (routine, ~33min dump) to populate model3dId + ingested sortAt for historical slots —
  **[PR-M4] EXPLICITLY ON BOTH PODS: bitdex-1 (standby/failover target) gets the same redump**, or
  a failover reopens the model3dId parity gap; (6) Civitai builder deploy; (7) **re-emitter
  enabled** ([PR-B1] moved BEFORE the gate — success criteria #1/#4 require it running during the
  window); (8) shadow-compare window → success criteria; (9) Meili image-feed retirement decision.
- **#13** docs continuously; final pass at the end.

> **W4 update (2026-07-17): step (2) backfill is now NO-BACKFILL (PR #328).** Perf review on live prod
> found the backfill is a disguised full-table rewrite (~92M of 105M rows, 88.1% mismatch, 200–400GB
> WAL) for a column nothing reads. Decision: the dump recomputes `GREATEST(...)` inline and **never
> trusts `Image.sortAt`**, and the model-share `image_sort_at_before` BEFORE trigger authors the column
> for future writes (converges lazily). An optional paced backfill still exists but is not required for
> correctness, which removes step (2)'s "must complete before step 4" WAL-flood ordering constraint.
> The redump (step 5) still carries historical values + `model3dId`. model-share migration PRs are in
> review; prod hand-apply held for Justin's go.

## Decisions taken (defaults — flag now if wrong)

- Re-emitter lives in Civitai job infra (not pg_cron) unless W1-3 sizing says otherwise.
- Post→Image trigger is row-level UPDATE with `IS DISTINCT FROM` guard; statement-level
  transition-table variant is the fallback if W3 latency gate fails.
- `sortAtUnix` emitted in **milliseconds** (existing mapping divides by 1000; seconds would shrink
  values 1000× — pinned by unit test in W2-1).
- Unpublished/draft images: sortAt = `GREATEST(scannedAt, createdAt)` (matches current computed
  semantics; NO sentinel — visibility still gates on isPublished under the frozen engine).
- Cutover includes a redump (needed for model3dId regardless).

## Open questions (safe defaults chosen; Justin can override async)

1. Re-emitter lookback 15 min — long enough to cover WAL-reader lag spikes? (W1-3 sizes from prod
   sync-lag history.)
2. Does anything besides Civitai write Image/Post rows out-of-band (bulk moderation scripts, data
   repairs) that would bypass Prisma but still fire triggers? (Triggers fire regardless — believed
   safe — but W1-2 confirms no TRUNCATE/COPY paths on these tables.)
3. Meili retirement itself (flag flip + code removal) is deliberately OUT of this plan — separate
   decision after success criteria hold.

## Risks + mitigations (carried from design doc)

- [AR-5] publish-txn latency → W3 gate + statement-level fallback.
- [AR-4] units → pinned test.
- Trigger deploy ordering → add-only pattern, each stage reversible, rollback = re-deploy prior
  trigger set (old computed path still correct pre-cutover).
- Backfill churn → paced, replica-lag watched, off-peak.
- Residuals accepted: wide fan-out over-cap skip (FOLLOWUP.md), bitdex-1 asymmetry (separate
  track), family-B root (masked by re-emitter).
