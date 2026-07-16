# Upstream Idempotent Re-emitter — Design (W1-3 / issue #4)

**Status: DESIGN, awaiting Justin's sizing skim + approval.** 2026-07-16. Author: alexandra
(Bitmap Architect). Executes `scheduled-publish-execution-plan.md` W1-3 and
`scheduled-publish-design.md` §2.D item 2 ("the verifier, moved to the source of truth"). Design
only — no implementation. Reviewer findings from the plan are folded in as `[PR-M3]`, `[PR-B2]`.

---

## 0. TL;DR

A periodic job in PG re-asserts the per-image publish values (`publishedAt`, `availability`,
`postedToId`) **and the ingested `sortAt`** for every image whose parent Post was published in the
last `lookback` window. It emits ops by calling the **same two shared PG functions the W1-1 triggers
call** — `bitdex_post_fanout_ops(p) || bitdex_image_sortat_ops(i)` — just triggered by a clock instead
of a row UPDATE, so its op shape cannot drift from the triggers'. When BitDex already holds the right
values the ops are no-ops (the ≥99% case = success); when BitDex missed a write — a dropped op, an
activation miss, a reschedule straggler, or a silently-failed `sortAt` recompute (the *dominant* prod
class per W1-4) — the re-emit heals it from PG's authoritative values within one lookback window.

**Three load-bearing facts that shape the whole design:**

1. **Activation is engine-driven and load-bearing; the re-emitter is a pure safety net.** A scheduled
   slot goes live when the flush thread's wall-clock `activate_due(now)` fires (`slot.rs:280-298`),
   no op required. The re-emitter never drives activation — it heals the rare slot that `activate_due`
   or a fan-out missed. If the re-emitter is down, correctness is unaffected for the common path;
   only the rare miss stays unhealed until it returns. [PR-B2]
2. **The unpublish-race ghost is already fenced by the poller's gap machinery — IF emission is a
   single statement.** See §3. This is the [PR-M3] answer, and it is free.
3. **Idempotency is structural, not incidental:** the re-emit ops are scalar `Set`s to current PG
   values. Re-applying sets the same bit; op_dedup resolves any collision with a live write
   last-writer-wins per field, and the live write always carries the fresher value. See §5.

---

## 1. What it does, precisely

### 1.1 Selection

```sql
-- Runs every <cadence>; heals the trailing <lookback> window.
SELECT i.id, p."publishedAt", p."availability", p."postedToId", i."sortAt"
FROM "Post" p
JOIN "Image" i ON i."postId" = p.id
WHERE p."publishedAt" >= now() - :lookback
  AND p."publishedAt" <= now()          -- exclude still-scheduled (future) posts
  AND p."updatedAt"  <  now() - :settle  -- unpublish-race belt, §3
```

The `publishedAt <= now()` bound is what makes this cover **both** fresh publishes and
scheduled posts that just became due: a scheduled post has `publishedAt = Tf` in the future and is
excluded until the clock passes Tf, at which point it enters the window and stays for `lookback`.
That is exactly the interval in which a missed activation needs healing.

### 1.2 Emission (per-image ops — mirror of the W1-1 Post trigger)

The re-emitter writes **ordinary per-image op rows** into `BitdexOps`, the same table and op JSON
shape the triggers use (`src/pg_sync/ops.rs:16-70`, `trigger_gen.rs:490`). One `BitdexOps` row per
image, `entity_id = Image.id`, ops = scalar `Set`s. **It reuses the exact two shared PG functions
W1-1 defines** (per PR-M1/PR-m2), concatenating their outputs:

- `bitdex_post_fanout_ops(_p "Post") RETURNS jsonb` — `{publishedAt, availability, postedToId}`, the
  per-image Post fan-out payload (this is what the Post trigger emits).
- `bitdex_image_sortat_ops(_i "Image") RETURNS jsonb` — the `sortAtUnix` op (this is what the Image
  trigger emits; sortAtUnix belongs to the IMAGE, not the Post fan-out).

```sql
INSERT INTO "BitdexOps" (entity_id, ops)
SELECT i.id, bitdex_post_fanout_ops(p) || bitdex_image_sortat_ops(i)   -- jsonb concat
FROM "Post" p
JOIN "Image" i ON i."postId" = p.id
WHERE p."publishedAt" >= now() - :lookback
  AND p."publishedAt" <= now()
  AND p."updatedAt"  <  now() - :settle;
```

The two payloads are **disjoint by field** (post fan-out never emits `sortAtUnix`; the image function
emits only `sortAtUnix`), which is the [PR-m2] disjointness property — the `||` concat can never
produce two ops for the same field, so op_dedup has nothing to LIFO-arbitrate within the row. Before
the sortAt cutover, `bitdex_image_sortat_ops` may be a no-op (empty array) or the mapping is dormant;
after cutover it carries the live `sortAtUnix` — either way the re-emitter's SQL does not change, only
the function's body does.

**Healing sortAt is the point, not a bonus.** W1-4's specimens show `sortAt` staleness is the
*dominant* prod bug: the engine's `GREATEST(existedAt, publishedAt)` recompute silently failed on
~7-28% of publishes, leaving images in the wrong feed position even when visible. Post-cutover
`sortAt` is ingested, so a *missed `sortAtUnix` op* = a permanently stale sort until healed — the
re-emitter re-asserting `bitdex_image_sortat_ops(i)` every window is what closes that class. So the
re-emitter heals **both** faces of a missed write: the publish fields (visibility) via
`bitdex_post_fanout_ops`, and the sort position via `bitdex_image_sortat_ops`.

**[Shape-parity requirement — the idempotency contract.]** Because the re-emitter *calls the same two
functions* the triggers call, its op shape cannot drift from theirs by construction — that is the
whole reason to share the functions rather than re-spell the JSON here. If instead the re-emitter
re-spelled the ops (different `to_jsonb` null handling for a null `postedToId`, seconds vs ms on
`sortAtUnix`, a dropped field), a re-emit that should be a no-op would become a spurious write and the
"≥99% no-op" success signal would be destroyed. **Coordination note to W1-1 (#7):** both
`bitdex_post_fanout_ops` and `bitdex_image_sortat_ops` must be plain shared SQL functions (not inlined
into the trigger bodies) so the re-emitter can call them.

**Why per-image and not a `queryOpSet`:** a `queryOpSet` ("postId eq X") re-introduces the exact
FD #69397 defect — BitDex resolving *which images* against a moving index at apply time. The whole
point of §2.D is that PG names the image set transactionally. The re-emitter is post-publish, so the
image rows exist and the JOIN resolves them concretely. (This is also why the re-emitter is safe on
the ModelVersion/Model fan-outs' turf: it never touches them — it only heals the per-image Post
values, which are already the per-image path.)

---

## 2. Home: where it runs

**Decision: (A) a Civitai app job in model-share's native `createJob` system.** The decision gate below
resolved on the model-share inventory (confirmed 2026-07-16).

### The two candidates

**(A) Civitai app job infrastructure** — model-share's native `createJob` system (`src/server/jobs/job.ts:54`),
registered in the central job array (`src/pages/api/webhooks/run-jobs/[[...run]].ts:108`), invoked by
an **external scheduler** that reads each job's cron cadence from `/api/internal/get-jobs`. It already
provides a **cross-pod Redis lock** (`job.ts:291`) so only one pod runs a given job — the exact
single-runner guard the re-emitter needs, inherited for free. Raw SQL is idiomatic here via
`dbWrite.$executeRaw`, and `BitdexOps` is not in model-share's Prisma schema, so the `INSERT ... SELECT`
runs as raw SQL with no schema coupling. Pros: same deploy/observability/oncall surface as every other
Civitai job; the Redis lock removes the single-runner question entirely; Prometheus counters (§6) plug
into the app's existing metric surface; unit-testable in the app harness. Cons: a job that is purely
`INSERT INTO BitdexOps SELECT ...` is SQL wearing a TypeScript coat — but the lock + metrics + oncall
visibility more than justify the wrapper.

**(B) `pg_cron`** — schedule the `INSERT ... SELECT` directly in Postgres. Rejected: the scout confirmed
`pg_cron` is **unused** on this deployment (extension not in play), operational visibility is worse
(history in `cron.job_run_details`, not the app's dashboards), and it would introduce a scheduling
mechanism the platform does not otherwise use — all to save a network hop the re-emitter does not care
about.

### Recommendation (resolved)

**Build it as a `createJob` in model-share.** The plan's standing default
(`scheduled-publish-execution-plan.md`:106) and the decision gate agree: the app job system already has
the cross-pod Redis lock (`job.ts:291`), so no `pg_try_advisory_lock` belt is needed in the job body;
the re-emitter needs **oncall-visible metrics** (§6) and correctness observability during the W4 shadow
window far more than a saved hop, and the app surface gives both for free. Job body = the single
`INSERT ... SELECT` of §1.2, executed via `dbWrite.$executeRaw`.

**Deployment note (W4 item):** a newly-added `createJob` only starts running once the **external
scheduler picks up its cron entry from `/api/internal/get-jobs`** — registering the job in the array
(`[[...run]].ts:108`) is necessary but not sufficient; the scheduler must re-read the job list. Fold
this into the W4 rollout checklist (enable the job → confirm the scheduler has picked up the cron →
watch `reemit_runs_total` increment) so "deployed" is not mistaken for "running".

The home choice does not change the fence, the sizing, or the idempotency argument — those are
properties of the SQL and the poller, not the scheduler.

---

## 3. [PR-M3] The unpublish-race fence

**The hazard as stated:** a re-emit whose SELECT snapshotted `publishedAt = T` just before a
concurrent unpublish commits could land `Set publishedAt = T` *after* the unpublish's remove/clear op
— a ghost re-publish.

**Claim: with single-statement emission, the ghost cannot reach the engine out of order, because the
poller's existing gap-hold machinery orders it correctly.** Here is the proof, then the belt.

### 3.1 Why single-statement emission is ordering-safe

`BitdexOps.id` is `BIGSERIAL` — allocated at INSERT execution, not at commit — and the ops_poller was
explicitly hardened (`ops_poller.rs:45-80`, the `GapInfo` machinery, prod specimen post 29674681
2026-07-09) so that **it never advances its durable cursor past an allocated-but-not-yet-visible id.**
A lower id that commits late is held for and processed; it is never skipped. Rows beyond a gap are
POSTed as they become visible, i.e. **in commit/visibility order.**

Take the dangerous interleaving. The re-emit is one autocommit `INSERT ... SELECT` — one MVCC
snapshot, and its `BitdexOps` id is allocated during that statement. Let U = the unpublish's
trigger-INSERT id, R = the re-emit's id.

- **If the unpublish commits before the re-emit's snapshot:** the snapshot sees `publishedAt` already
  cleared → the `WHERE publishedAt <= now()` (or a null publishedAt) excludes the image → **no
  republish op is emitted at all.** Safe.
- **If the unpublish commits after the re-emit statement:** then U becomes visible to the poller
  *after* R. The poller POSTs R (republish=T) first, then U (unpublish=null). Applied in that order →
  final state = null. Safe.
- **The only way U < R yet the re-emit still reads `publishedAt = T`** is if U's trigger INSERT ran
  before R's INSERT but U's transaction had **not committed** at R's snapshot. Then R commits and
  becomes visible while U is still an invisible gap at id U < R. The poller **holds its durable cursor
  below U** (GapInfo) and POSTs R immediately; when U later commits, it POSTs U. Order to the engine:
  R (republish) then U (unpublish) → final = null. Safe.

In every interleaving the write that **commits last wins**, which is the correct answer, and the
re-emit can only emit a republish for a `publishedAt` its snapshot actually saw — which precludes the
one interleaving that would invert the order. The ghost is structurally impossible **provided emission
is a single statement** (so the id-allocation order matches the snapshot the values were read under).
A multi-statement emit-in-a-loop breaks this — it takes a fresh snapshot per row and can read T,
then have the unpublish commit, then INSERT — so **single-statement `INSERT ... SELECT` is a hard
requirement, not a style preference.**

### 3.2 Why version-stamping is not available

The clean textbook fence — stamp each op with `Post.updatedAt` and have the consumer apply
last-writer-by-version — is **off the table under §2.D's frozen engine.** `op_dedup` resolves
same-field conflicts by arrival order (LIFO within a batch, `op_dedup.rs:100-128`), with no
version/timestamp comparison; adding version arbitration is an engine change. So the fence has to come
from ordering (§3.1) or from not entering the race window at all (§3.3), not from the engine.

### 3.3 The `settle` belt (defense in depth, independent of the poller)

§3.1's safety is *correct but coupled*: it relies on the poller's GapInfo behaviour. If a future
change ever let the poller advance past a gap, the re-emitter would silently regain a ghost-republish
bug. To decouple, add a cheap independent belt: **exclude posts whose `updatedAt` is within the last
`settle` seconds** (`AND p."updatedAt" < now() - :settle`, default `settle = 10s`). A post being
published or unpublished *right now* has a fresh `updatedAt`; the belt skips it until it settles, so
the re-emit and the live publish/unpublish op never coexist in a processable window regardless of
poller internals.

Cost of the belt: a genuinely-fresh publish that was *also* missed is not healed for the first
`settle` seconds — immaterial, because activation at Tf is the load-bearing path (a miss is rare and
the re-emit heals it on the *next* run within `lookback`, not this instant). `settle` must be ≪
`lookback`.

**Recommendation: do both.** Single-statement emission (correct by construction) **and** the `settle`
belt (holds even if the poller's gap machinery ever regresses). Record in the doc + a code comment on
the poller's GapInfo that the re-emitter's ordering-safety depends on it, so the coupling is not
silently broken.

---

## 4. Lookback sizing

`lookback` must be ≥ the worst-case time between a publish and the moment BitDex is *reliably*
converged, across every failure class the re-emitter exists to cover. Failure classes and their
contribution:

| Failure class | Duration it can cost | Covered how |
|---|---|---|
| ops_poller downtime (pod restart / deploy roll) | seconds–low minutes | poller resumes from durable cursor; re-emit only needed if a row was lost, not merely delayed |
| WAL reader stall / ops_processor lag | seconds–minutes under load | delay, not loss; re-emit heals only if a batch was actually dropped |
| Gap-hold on a slow / rolled-back publish txn | up to `GAP_ALERT_AFTER` = 60s then resolved | poller holds ≤ minutes; loss only on a real rollback (then nothing to heal) |
| **Activation miss (~1/320, the real target)** | **unbounded until healed** | **re-emit re-stamps `publishedAt` → deferred-slot reschedule, §5** |
| Reschedule straggler (bug class #313) | unbounded until healed | same re-stamp path |
| Fan-out partial match (FD #69397) | unbounded until healed | C triggers fix the mechanism; re-emit is the backstop |

The delay-not-loss classes (poller/WAL) argue for a lookback comfortably above their P99, so a slow
window doesn't push a real miss out of range before the re-emitter looks. The unbounded classes are
covered by *any* lookback ≥ the run cadence, because a genuinely-stuck slot stays stuck (and thus
in-window) until a re-emit lands — the lookback just has to be long enough that the slot is still in
`[now - lookback, now]` on the first run after it went due.

**Default: `lookback = 15 min`.** Justification: it dominates the observed poller/WAL lag classes
(gap alert fires at 60s; WAL/poller lag in normal operation is seconds) by more than an order of
magnitude, so transient slowness never masks a real miss; and for the unbounded classes it gives the
job up to 15 minutes of retries (at the §6 cadence, ~3 attempts) before a stuck slot ages out of the
window — after which the *sampled reconcile* (W2-3), not the re-emitter, is the catch-all.

**Prod calibration is a follow-up, not a blocker.** I could not reach prod metrics from this
environment (`/metrics` 404 via the monitoring skill). Before enabling the job in W4, the
implementation PR should pull the real distribution of the poller/WAL lag from Grafana/Prometheus —
the series to check are `bitdex_sync_v2_lag` (rows behind) and the WAL-reader apply lag — over a
window that includes a deploy roll and a peak-traffic hour, and confirm P99 ≪ 15 min (amend upward if
a deploy roll routinely parks the poller for longer). 15 min is the safe default; the number is a
config knob (`reemit_lookback_secs`), overridable without a redeploy.

---

## 5. Idempotency + interaction with op_dedup and deferred slots

### 5.1 Idempotency

The re-emit ops are scalar `Set`s to the *current committed* PG values. Applying the same `Set` twice
is idempotent: for a filter field it re-sets a bit that is already set; for a sort field it re-writes
the same bit-layer decomposition. No `Add`/`Remove` multi-value ops are ever re-emitted (tags etc. are
untouched), so there is no additive churn and no cancellation hazard.

### 5.2 op_dedup (`op_dedup.rs`)

- **Across runs:** each re-emit run's rows have distinct, increasing `BitdexOps` ids and land in
  different poll batches. They are applied sequentially, each setting the same value → convergent.
- **Within a batch, colliding with a live write for the same (slot, field):** `dedup_entity_ops`
  applies LIFO — last op in arrival (= id) order wins per field (`op_dedup.rs:100-128`). Because the
  poller preserves commit/visibility order (§3.1), the op that committed later — always the *live*
  write, never the re-emit that only re-asserts an older snapshot — wins. The re-emit can never
  clobber a fresher live value.
- **The re-emit emits only `Set`, never `queryOpSet`,** so it does not interact with the queryOpSet
  merge path (`op_dedup.rs:119-124, 156-162`) at all — that path stays the exclusive domain of the
  Post/ModelVersion/Model triggers.

### 5.3 Deferred slots (`ops_processor.rs:969-1021`) — this is the heal mechanism

For a slot that is still **deferred** (scheduled, activation missed, not yet alive), a per-slot op is
not dropped: the ops_processor writes every field to the docstore (replayed at activation) and, **if
the op changes the deferred source field (`publishedAt`), reschedules activation** via
`sink.deferred_alive(slot, new_at)` (`ops_processor.rs:996-1001`). A now-past timestamp activates on
the next flush cycle.

This is precisely why re-emitting `publishedAt` heals a missed activation: the re-emit's
`Set publishedAt = T` (T now in the past, since the post is in `[now - lookback, now]`) hits the
deferred branch → docstore updated → activation rescheduled to T → `activate_due` fires next cycle →
slot goes live with the full replayed doc. One idempotent `Set` heals the invisible-in-gallery,
wrong-sort-order, and false-unpublished symptoms together (the engine recomputes `sortAt` on the
`publishedAt` write, `ops_processor.rs:2708`; after the sortAt cutover the re-emit carries
`sortAtUnix` directly). For a slot that is **already alive** (activation succeeded), the same op is a
plain no-op `Set`. Same op, both outcomes — heal or no-op — with no branching in the re-emitter.

`schedule_alive` dedupes the old key, so re-emitting across runs while a slot is briefly deferred does
not pile up duplicate schedule entries (`ops_processor.rs:966-968` comment).

---

## 6. Cadence, volume, and metrics

### 6.1 Cadence vs lookback

Cadence and lookback are independent knobs. With `cadence < lookback` each in-window image is
re-emitted `lookback/cadence` times over its window lifetime — redundant, but every copy is a cheap
no-op and the redundancy is what makes a *single* run failure non-fatal (the next run retries the
same slice). **Default: `cadence = 5 min`, `lookback = 15 min`** → overlap factor 3 (a stuck slot gets
~3 heal attempts before ageing out), at 3× the minimum emission volume.

### 6.2 Volume (ESTIMATE — must be re-derived from prod rates before W4)

I do not have Civitai's real publish rate from this environment; the following is an order-of-magnitude
estimate, **explicitly marked as such**, to size the job's footprint on `BitdexOps` / the poller.

Formula: `rows_per_run ≈ posts_published_in(lookback) × images_per_post`.

Illustrative plug-in (**assumed, not measured**: ~30k posts published/day, ~4 images/post):
- posts in a 15-min window ≈ 30000 / 96 ≈ **310 posts**
- rows per run ≈ 310 × 4 ≈ **1,240 `BitdexOps` rows**, every 5 min ≈ **~4 rows/sec average**, bursting
  to ~1.2k rows at each run instant.

At that scale the footprint is negligible against normal ops volume. **But the estimate is load-bearing
for the go/no-go:** if the real publish rate is 10× higher, or images/post is heavy (bulk uploads),
rows/run climbs proportionally and the `settle`/cadence knobs (or a keyset-paced emit) may be needed.
The implementation PR must replace this with a measured `posts_published_per_window × avg_images_per_post`
from prod (a one-line PG aggregate over `Post.publishedAt` in a recent window) and confirm the per-run
burst is absorbable by the poller's batch size.

### 6.3 Metrics

Emitted from the job (PG/app-side — no engine change):

- `reemit_runs_total` — runs started (liveness).
- `reemit_posts_scanned_total`, `reemit_images_emitted_total` — volume; `images_emitted / runs`
  tracks against the §6.2 estimate and alarms if it jumps (a rate spike or a stuck cursor).
- `reemit_run_duration_seconds` — the `INSERT ... SELECT` latency; guards against the emit itself
  becoming a slow, lock-heavy statement.

No-op rate and heal-detection (success = ≥99% no-ops) — the honest measurement problem:

- **The re-emitter cannot itself tell a no-op from a heal** — it just INSERTs; BitDex decides. Under
  the frozen engine there is no per-`Set` no-op counter, so no-op rate is measured *indirectly*, by
  the **absence of downstream effect on re-emit runs**: activation/alive counters and the deferred
  reschedule log (`"rescheduled deferred slot"`, `ops_processor.rs:999`) stay flat across re-emit
  runs, and shadow-compare divergence stays ~0. A jump in deferred reschedules *correlated with a
  re-emit run* IS a heal event and is the cleanest heal signal available without touching the engine.
- **Heal detection belongs to the sampled reconcile (W2-3), not the re-emitter.** The re-emitter's job
  is to *heal*; detecting-and-counting divergence is the audit's job (`PG.sortAt vs BitDex.sortAt`,
  and equivalently `PG.publishedAt` vs BitDex `isPublished`). Keeping them separate avoids the
  re-emitter having to query BitDex per-run to self-grade. The success criterion "≥99% no-ops" is
  therefore verified as "the audit finds ~0 divergence *and* the re-emitter is running" — a high
  emitted-volume with ~0 audit divergence and flat reschedule counters is the definition of a healthy
  no-op-dominant safety net.
- If a first-class no-op rate is later wanted, the minimal honest source is a single engine counter
  incremented when a `Set` changes no bit — but that is an engine change, out of §2.D scope, and
  noted here only as the future option.

---

## 7. What this design deliberately does NOT do

- **It does not drive activation.** `activate_due` is load-bearing; the re-emitter is the net under
  it. If the two ever disagree, activation is authoritative and the re-emit is a redundant re-assert.
- **It does not touch the ModelVersion/Model fan-outs** (`baseModel`, `poi`). Those keep `queryOpSet`;
  their reconcile is a separate FOLLOWUP.md item (the over-cap silent-skip hole,
  `ops_processor.rs:2523-2540`).
- **It does not re-emit multi-value fields** (tags, tools, techniques) — only the scalar publish
  values, so it can never cause additive/cancellation churn.
- **It is not a substitute for the sampled PG↔BitDex reconcile (W2-3).** The reconcile *detects*
  drift across the whole corpus; the re-emitter *heals* the recent window. Both are needed: the
  re-emitter bounds healing latency for fresh misses, the reconcile catches anything that aged out of
  the window.

---

## 8. Open items for the implementation PR

1. ~~Confirm the home (§2)~~ **RESOLVED:** model-share `createJob` (`job.ts:54`) with the inherited
   cross-pod Redis lock (`job.ts:291`); register in `[[...run]].ts:108`; scheduler picks up cron from
   `/api/internal/get-jobs` (W4). pg_cron unused → not used.
2. Replace §6.2's estimate with a measured prod publish rate + images/post; confirm per-run burst is
   absorbable.
3. Calibrate `lookback` against real `bitdex_sync_v2_lag` / WAL-reader-lag P99 over a deploy roll +
   peak hour (§4).
4. Confirm W1-1 exposes `bitdex_post_fanout_ops(_p "Post")` and `bitdex_image_sortat_ops(_i "Image")`
   as shared SQL functions (not inlined into trigger bodies) so the re-emitter can call them (§1.2).
5. Add a code comment on the poller's `GapInfo` recording that the re-emitter's ordering-safety
   depends on the gap-hold invariant (§3.3).
6. Decide `settle` (default 10s) and cadence (default 5 min) as config knobs; expose `reemit_lookback_secs`.
