# Scheduled-Publish Design — ingested `sortAt` + derived `pending` exclusion

**Status: DESIGN v2 (adversarially reviewed), awaiting Justin's review.** 2026-07-15. One
adversarial pass (Bitmap Architect) found 2 blockers + 4 majors, all folded in below and marked
`[AR-n]`; its verdict: *shippable after fixes, not structurally flawed — the value-not-event core
and the snapshot model hold*. Successor to
`scheduled-publish-redesign.md` (Justin's proposal + rationale record — read it for the failure-class
analysis; this doc turns it into a buildable design). Grounded in three exploration reports:
`docs/_in/scout-sync-config-2026-07-15.md` (prod config), the Civitai query mapping (image.service.ts),
and the BitDex machinery inventory (session transcript, 2026-07-15).

---

## 0. TL;DR

1. PG writes `Image."sortAt"` **transactionally** at schedule/publish/reschedule/unpublish time.
   The column already exists, is indexed, and is dead — nothing writes it today.
2. BitDex ingests `sortAt` as a plain value through the **already-wired but dormant**
   `sortAtUnix → sortAt` mapping. The engine-computed `GREATEST(existedAt, publishedAt)` dies.
3. Drafts and unpublished get a **far-future sentinel** `sortAt`. One rule covers drafts, scheduled,
   and unpublish: `visible ⇔ sortAt <= now`.
4. A **`pending` bitmap = {alive slots with sortAt > now}** is **derived from the sortAt sort layers**
   by a 32-op bit-layer range computation every tick. Derived ⇒ cannot drift, nothing to persist,
   nothing to lose.
5. The engine applies `AND NOT pending` to every query working set. Moderator "notPublished" views
   use an explicit `pending` pseudo-filter instead of `isPublished eq false`.
6. Publish sends BitDex **nothing**. No activation batch, no deferred map, no verifier, no
   `isPublished`, no `publishedAt` on the Post fan-out. ~3,100 LOC deleted; 2 of 7 sort fields and
   1 of 20 filter fields deleted.

What BitDex holds for an image becomes: values the Image row carries + `sortAt`. Every date-related
field is a **diffable value**, never a replayed event. `BitDex.sortAt != PG.sortAt` is a reconcile
query, not a forensic investigation.

---

## 1. Current state (verified by scouts, 2026-07-15)

### The write path today

- `publishedAt` / `availability` / `postedToId` reach images ONLY via the Post fan-out `queryOpSet`
  (`talos-infra .../v2-sync-config.yaml:264-274`). The Image trigger doesn't track them.
- `isPublished` = `exists_boolean` shadow of `publishedAtUnix` (flips inside `process_set_op`,
  `ops_processor.rs:1568-1575`).
- `sortAt` = engine-computed `GREATEST(existedAt, publishedAt)` (`deployment.yaml:111-116`),
  recomputed per fan-out slot (`ops_processor.rs:2708-2716`).
- Scheduled posts ride **deferred-alive** keyed on `publishedAt` (prod: `source_field: publishedAt`,
  sweep DISABLED, verifier `membership_field: postId`).
- **`Image."sortAt"` in PG is written by NOTHING** (`prisma/schema.prisma:1670`, default `now()`,
  indexed). Meili recomputes `GREATEST(p.publishedAt, i.scannedAt, i.createdAt)` at index time and
  gates at query time with `publishedAtUnix <= now` — Meili never needed an activation event.
  BitDex is the only consumer that turned publish into an event.

### The read path today (image.service.ts:3745)

- Filters actually used: nsfwLevel, userId, type, baseModel, availability, postId, postedToId,
  remixOfId, hasMeta, onSite, poi, minor, **isPublished (always)**, isRemix, blockedFor, tagIds,
  modelVersionIds, modelVersionIdsManual, toolIds, techniqueIds, id (hidden view),
  sortAtUnix Gte (period).
- Sorts actually used: **only** `sortAt`, `reactionCount`, `commentCount`, `collectedCount`.
- `publishedAt` is stored + returned but **never filtered or sorted on**. `existedAt` exists only to
  feed the sortAt computation. Both are pure overhead once sortAt is ingested.
- Own-content / own-unpublished carve-outs are deliberately NOT in the BitDex query (cache-key
  user-independence); a second PG pass merges them. **Moderator views DO query
  `isPublished eq false`** (image.service.ts:3905-3915) — the one consumer of unpublished state.

### What the event-based design costs us (the whole point)

Deferred map + activation + verifier + durability gate + sweep + deferred-reach ≈ **3,100 LOC**
(~2,000 prod + 1,100 test), spread over slot.rs / config.rs / shard_store_meta.rs / mutation.rs /
write_coalescer.rs / concurrent_engine.rs / ops_processor.rs / server.rs / metrics.rs / ingester.rs —
plus the entire orphan/verifier debugging saga of 2026-07 (see `scheduled-publish-redesign.md` §2).

---

## 2. The design

### 2.1 PG side — `sortAt` becomes transactional truth

**Semantics (identical to Meili's formula, plus a sentinel):**

```
Image.sortAt = CASE
  WHEN post.publishedAt IS NULL THEN 'infinity-sentinel'        -- draft / unpublished
  ELSE GREATEST(post.publishedAt, image.scannedAt, image.createdAt)
END
```

Sentinel = `2100-01-01T00:00:00Z` (4102444800s; fits u32, sorts above any real time, `> now` for the
next 74 years). Draft = "scheduled for never." Unpublish = re-schedule for never. **One rule:**
`visible ⇔ sortAt <= now`.

**Writers (all inside the owning transaction — no outbox, no query resolution):**

1. **Post trigger** `AFTER UPDATE OF "publishedAt" ON "Post"`:
   `UPDATE "Image" SET "sortAt" = <formula> WHERE "postId" = NEW.id AND "sortAt" IS DISTINCT FROM <formula>`.
   Covers publish, schedule, reschedule, unpublish, and the model-version publish/unpublish fan-outs
   (they all mutate Post.publishedAt — post.service.ts:906-918, model-version.service.ts:1245-1360).
   This is a real transactional row update: it cannot partially apply, and each touched Image row then
   fires the existing per-row Image trigger, which carries `sortAt` to BitDex as an ordinary per-image
   value op. **The fan-out moves into PG where it is atomic, instead of into BitDex where it raced.**
   **[AR-5, MAJOR — this has a cost, measure it]:** publish goes from 1 outbox row to N synchronous
   Image updates inside the publish transaction (+N per-row trigger fires, +N BitdexOps rows, +PG WAL
   amplification); model-version publish touches many posts at once. Gate 5 measures publish-txn
   latency at realistic fan-out (P99 images-per-post, and the model-version worst case) before
   cutover. Mitigations if hot: statement-level trigger with a transition-table UPDATE, or moving the
   write into `updatePost` app code (same transaction, one statement).
2. **Image trigger** extension: on INSERT, and on UPDATE of `scannedAt`, compute own `sortAt` from the
   parent Post directly (`SELECT publishedAt FROM "Post" WHERE id = NEW."postId"`) — the per-row path
   the scouts confirmed was always the healthy one.
3. Remove the Prisma `@default(now())` on sortAt (it is wrong for images on draft posts); the Image
   INSERT trigger owns it.

**Backfill:** one batched migration `UPDATE Image SET sortAt = <formula>` over ~105M rows (keyset
batches, replica-safe pacing). During rollout BitDex keeps the computed fallback (below), so backfill
order doesn't gate correctness.

**Audit (the property that makes this worth it):** `sortAt` is now a value in both systems.
A sampled reconcile — `SELECT id, sortAt FROM Image TABLESAMPLE ...` vs BitDex docs — detects any
missed trigger write. The `data-quality-verifier-design.md` machinery is the natural home. A stale
`sortAt` is a findable, repairable fact; "did the activation fire 3h ago" was neither.

### 2.2 Sync config — what changes

- **Image dump phase:** add `sortAt` to the copy_query + fields (`extract(epoch from "sortAt")`).
  Delete the `isPublished` computed field, delete the `publishedAt` enrichment field. The Post
  enrichment join shrinks to `availability` + `postedToId` (Phase 2 deletes it entirely).
  Delete `existedAt` computed field. Delete the dump's deferred branch — rows index fully.
- **Image trigger:** add `sortAt` track_field (`extract(epoch from {sortAt})::bigint` → target
  `sortAtUnix`, flowing through the **existing dormant** `sortAtUnix → sortAt` data_schema mapping,
  `deployment.yaml:261`). Delete the `existedAt` track_field.
- **Post fan-out:** delete the `publishedAt` track_field — **the FD #69397 trigger line ceases to
  exist**. `availability` + `postedToId` remain (Phase 2).
- **Index config:** `sortAt` becomes a plain ingested 32-bit sort field (drop `computed:`). Delete
  `publishedAt` and `existedAt` sort fields (64 layer bitmaps at 105M — real memory back). Delete the
  `isPublished` filter field + its exists_boolean shadow mapping. Delete `deferred_alive` and
  `activation_verify` blocks.
- **Transition [AR-2, was a BLOCKER — "computed + ingested coexist" is UNSAFE].** There is no
  per-slot arbitration between an ingested and a computed sort value: the computed-sort recompute
  (`recompute_computed_sorts_for_slot`, ops_processor.rs:2708) is a full overwrite — for a scheduled
  slot (publishedAt null) it would collapse an ingested FUTURE sortAt back to `existedAt` (past) on
  the next scannedAt/fan-out update ⇒ leak. So the two modes must never coexist:
  **backfill completes FIRST; the cutover deploy atomically deletes the `computed:` block and starts
  ingesting** (one index-config change). Pre-cutover, ingested `sortAtUnix` ops are simply not
  emitted yet (trigger deploy gated the same way). §5 rollout is ordered accordingly.
- **Sentinel/units [AR-4, MAJOR — pinned, not an open item].** The dormant mapping is
  `sortAtUnix → sortAt, ms_to_seconds: true`. Pin the whole chain: PG triggers emit
  **milliseconds** (`extract(epoch from {sortAt})::bigint * 1000` → `sortAtUnix`), the mapping
  divides by 1000, the u32 store happens AFTER division. Sentinel `2100-01-01` = 4102444800s —
  fits u32 (max 4294967295) post-division; pre-division ms value never touches a u32. Emitting
  seconds into this mapping would shrink every value 1000× (sentinel → Feb 1970 → drafts VISIBLE);
  a unit test pins sentinel-in-PG → 4102444800 in the layer.

### 2.3 Engine — `pending`, derived, engine-applied

**Definition:** `pending = {slot ∈ alive : sortAt(slot) > now}`.

**Derivation (the load-bearing simplification):** computed from the sortAt bit layers by a standard
MSB→LSB range traversal — "slots with value > K" is ~32 AND/ANDNOT/OR bitmap ops over the layers,
NOT a per-slot reconstruct. `sort.rs` already has the traversal idiom (`bifurcate_with_layers`,
sort.rs:323-412) and an (unused, tested) per-slot `slots_in_range` (sort.rs:566); the new
`slots_above(layers, K, universe) -> RoaringBitmap` is ~50 LOC beside them. Computed on the flush
thread against staging; published inside the same snapshot via ArcSwap like everything else, so a
query can never see filters and pending from different states.

**[AR-1, was a BLOCKER] When to recompute — per-flush, not per-timer.** A timer-only tick leaks: a
newly scheduled image is inserted fully-indexed with future sortAt; between its insert-flush and the
next tick, the published snapshot's stale `pending` lacks the slot ⇒ it shows at POSITION 1 of the
public feed for up to tick_secs. Rule: recompute `pending` inside any flush cycle whose batch
contained a sortAt mutation, AND on a timer tick (clock passage). Recompute stays a pure derivation
either way — the trigger changes, the computation doesn't. Gate 2 measures per-flush cost; if too
hot for sortAt-heavy batches, the flush-path narrows to "batch contained a FUTURE sortAt value"
(writer sees the value; still derivation-triggering, not state maintenance). Acceptance test: insert
future-sortAt slot, query the published snapshot before any timer tick — must be excluded.

Because it is derived: **no persistence, no boot restore, no durability domain, no reconcile, no
drift.** Boot = compute it from the layers you just loaded. Crash = nothing to lose. A wrong
`pending` is only ever a wrong `sortAt` — which is auditable state (§2.1).

**Application:**

- **Slow path:** `filter_bitmap -= pending` in `resolve_filters` immediately after
  `concurrent_engine.rs:8228`, before the Arc wrap — every downstream consumer (total counts, cache
  seeding, all `execute_from_*` sorts) sees the excluded set. Sort traversal needs zero changes.
- **Negation universe:** NotEq/NotIn/Not currently use `alive` as universe (executor.rs:552-608);
  covered by the top-level subtraction (negations are combined into the working set before :8228).
- **Fast cache-hit path** (`concurrent_engine.rs:6885-7018`) bypasses `resolve_filters`.
  **[AR-3, MAJOR] The cache must be kept pending-free by construction, not patched per-page.**
  New scheduled inserts enter EXISTING cached entries after seeding via live maintenance
  (collect/evaluate/apply, cache_worker.rs:448) — and a future sortAt lands at the TOP of a cached
  sortAt-DESC bitmap, i.e. page 1 of the hottest feed. So: (a) the maintenance evaluate step gains a
  pending check (one `pending.contains(slot)` test) — pending slots are never ADDED to cached
  entries; (b) un-pend transitions add them (§2.4); (c) the fast path still applies `ids -= pending`
  to the final page as a backstop, applied BEFORE the short-page/expansion decision (:6920-6974) so
  a subtracted page re-expands rather than returning blank, with the cursor derived post-subtract.
  With (a) in place the backstop genuinely is a backstop, `total_matched` from `cached_total`
  (:6976) stays honest, and a maintenance miss degrades to a count off-by-few, never a leak.
- **Moderator escape hatch [AR-6, MAJOR — specified, not hand-waved]:** a query-level pseudo-filter
  `pending`. `resolve_filters` branches on it: clause ABSENT ⇒ `working_set -= pending` (the default
  that makes FD #69397 inexpressible); `pending eq true` ⇒ `working_set &= pending` (moderator
  notPublished view, replaces `isPublished eq false`); `pending eq false` ⇒ explicit subtract (same
  as absent, distinct cache key). The clause participates in `UnifiedKey` canonicalization like any
  other, and the cache worker's evaluate step resolves it against the snapshot's derived pending
  bitmap (threaded in alongside alive). Mod-view traffic is tiny; if the evaluator plumbing isn't
  worth it, `pending eq true` entries can simply be marked uncacheable (`skip_cache`) — decided at
  implementation. Civitai change: one line in the query builder.

### 2.4 Transitions — the tick, cache, and time buckets

Each tick: `newly_live = pending_prev ANDNOT pending_next` (and `newly_pending` for the
unpublish/reschedule direction). Expected size: single digits per second, bursts of ~dozens at
minute boundaries (12 posts at one instant was a observed burst).

The cache scout's warning is the constraint: pending changes must NOT route through
`maintain_alive_changes()` (unified_cache.rs:2328 — marks EVERY entry needs_rebuild) or any other
whole-cache path. Design:

- **Unified cache:** feed `newly_live`/`newly_pending` slots through the existing two-phase
  collect/evaluate/apply maintenance (cache_worker.rs:448-565) as membership candidates — entry
  membership is decided by bitmap `contains()` tests against the entry's clauses (no doc reads),
  exactly how bucket membership maintenance works (`maintain_bucket_membership`,
  unified_cache.rs:2407, meta-index-scoped, work-budgeted). Per-slot, per-referencing-entry — the
  same cost activation maintenance pays today.
- **Time buckets:** buckets already exclude `sortAt > now` at their upper edge
  (time_buckets.rs:195,280). A slot crossing `now` must enter its buckets without waiting for the
  hourly reconcile: on tick, `insert_slot(slot, sortAt, now)` for each newly-live slot. Same hook.
- **Correctness backstop:** the slow path is always right by construction (working set derived fresh
  from bitmaps − pending). Cache/bucket maintenance misses degrade freshness (a slot late to a cached
  feed until rebuild/reconcile), never correctness of what's excluded.

### 2.5 What gets deleted

| Deleted | Where | ~LOC |
|---|---|---|
| Deferred map + slot API + coalescer staging/seq/pending bridge | slot.rs, write_coalescer.rs | 250 |
| Flush-thread activation + doc replay + persist ordering + durability gate | concurrent_engine.rs | 700 |
| Verifier (verdicts, ring, barrier, re-drive) + overdue sweep + deferred-reach | ops_processor.rs | 640 + ~900 test |
| DeferredAliveConfig + ActivationVerifyConfig | config.rs | 100 |
| MetaStore deferred_alive.bin + activation_verify.bin | shard_store_meta.rs | 100 |
| mutation.rs deferred branches + shadow-coherence for isPublished | mutation.rs | 110+ |
| server.rs sweep/verify drivers, metrics counters, ingester emits | server.rs, metrics.rs, ingester.rs | 295 |
| Dump deferred branch + force_deferred_shadows_false + isPublished materialization | dump_processor.rs | ~150 |
| `publishedAt` + `existedAt` sort fields (64 layer bitmaps @105M), `isPublished` filter field | config + memory | — |
| Open bugs closed by deletion: #313 class, ~1/320 activation-miss, publish-lag verifier saga (#314/#316/#320/#321/#323), FD #69397's mechanism for publishedAt | — | — |

Added: `slots_above` (~50), tick + transition diff (~100), pending in resolve_filters + fast-path
subtract (~30), pseudo-filter parse/plumb (~60), cache/bucket transition hooks (~150), tests.
**Net ≈ −2,600 LOC and two fewer moving durability domains.**

### 2.B — OPTION B: level-triggered convergence (RECOMMENDED, 2026-07-15 late)

Justin's review of Option A landed on its genuinely weakest point: §2.4's un-pend cache maintenance
is **event-shaped machinery sneaking back in** — "when a slot crosses `now`, add it to the right
cache entries, correctly, at that moment" is the same species of obligation the redesign exists to
delete. And every fix for it (maintenance pending-checks, fast-path subtracts, expansion-order
rules) spreads pending-awareness through the most perf-critical code in the engine.

Option B keeps everything upstream (PG transactional `sortAt`, sentinel, value-not-event) and
changes ONE decision: **pending slots are not in the filter bitmaps or alive at all.**

- On ingest, a slot with `sortAt > now` gets: **doc + sortAt layer bits only.** No filter bits, no
  alive bit. (Same shape as today's deferred insert branch, minus the map, minus the timestamps.)
- `pending` needs no bitmap, no map, no persistence: it IS `slots_above(sortAt_layers, now)` —
  future values always have high bits set, so pending is exactly derivable from layers we already
  persist. The deferred map's job is done by the sort layers themselves.
- **A convergence sweep** (flush thread, every tick AND whenever a flush batch carried a future
  sortAt) enforces one invariant, level-triggered, both directions:
  - `due_missing = pending_prev ANDNOT slots_above(now)` → for each: read doc, emit ordinary
    insert-shaped MutationOps (filter bits + alive last, as the "replay completed" marker).
    Idempotent — re-running sets already-set bits. Crash mid-replay ⇒ alive unset ⇒ picked up next
    tick. At boot and periodically, the backlog is audited exactly: `union(all sortAt layers)
    ANDNOT alive ANDNOT slots_above(now) ANDNOT clean` — pure bitmap math, the verifier's entire
    3,100-LOC question ("did it land?") becomes a one-line derivable set.
  - `live_future = alive AND slots_above(now)` → un-index (unpublish/re-schedule direction): read
    doc, clear filter bits, clear alive → slot returns to pending. Symmetric, same loop.
- Ops arriving for a pending slot (tag edits, nsfwLevel, reschedule) apply **doc + sortAt layers
  only** — the existing mutation.rs deferred branch, simplified (no DeferredAlive op, no Tf).

**Why this answers the cache concern structurally:** going live is an ordinary insert. The unified
cache, time buckets, meta-index — none of them learn pending exists; they see the same MutationOps
a brand-new image produces today, through machinery that is already load-bearing in prod. No
entries full of excluded slots (pending is ~90k = 0.09% of 105M anyway — it could never dominate
cache space, but under B the question doesn't even arise). No read-path change of any kind: **zero
ANDNOT on the hot path, CLAUDE.md's clean-filter-bitmap principle intact, gates 1 and 3 and the
whole of [AR-1]/[AR-3]/[AR-6] dissolve.**

**Sort-layer safety:** pending slots' layer bits are inert — sort traversal only walks the
working set (filter-derived, so pending never enters), negation universe is alive, time buckets
already exclude `ts > now`. Verified against executor.rs:552-608, sort.rs:246, time_buckets.rs:195.

**What B gives up vs A:**
- Pending slots are not findable by filters ⇒ the moderator notPublished view can't be served by
  BitDex. It moves to a PG query (mod-only traffic, tiny; own-content second pass already lives
  there). In exchange the `pending` pseudo-filter machinery [AR-6] is never built.
- The write path keeps a deferral branch (~150 LOC, simplified) and the sweep's doc-read replay
  (~150 LOC) instead of deleting deferral outright. **But the failure mode changes category:** the
  old design's replay was un-auditable (an event that either fired or didn't); B's replay is a
  convergence loop over a derivable set — failure is visible as a nonzero backlog bitmap and
  self-heals next tick. The verifier isn't replaced; its question stops being askable.

**Deletion table under B (delta from §2.5):** everything still dies EXCEPT mutation.rs's deferred
branch (simplified, stays) and ~300 LOC of new sweep/derivation code. The deferred map, MetaStore
files, coalescer seq/durability gate, verifier apparatus, activation-at-Tf, `isPublished`,
`publishedAt`+`existedAt` fields, and the publishedAt fan-out all still delete. Net ≈ −2,400 LOC.

**B's measurement gates:** (2) `slots_above` per-tick cost (unchanged); (5) publish-txn latency
(unchanged); new (7): sweep replay throughput at burst (a 12-post minute-boundary tick = a few
hundred doc reads — DocCache-warm, expected trivial); boot-time union-of-layers cost (one-time,
32-layer OR at 105M).

**Recommendation: build B.** A optimizes for "fully indexed, findable pending"; nothing needs that
except a mod view PG serves better. B keeps the read path — the part of this engine that is fast,
proven, and principle-bound — completely untouched, and turns activation from an event into an
invariant. Sections 2.3–2.4 above are retained as the record of Option A and its review.

### 2.C — OPTION C: minimal stopgap — resolve the fan-out in PG (no engine change)

Justin's follow-up (2026-07-15, late): can upstream emit a published status and we just fix the
triggers, skipping the overhaul?

**The half that works — kill the moving-index race at its source.** The Post fan-out's defect is
that BitDex resolves *which images* at apply time against an index that moved. PG knows the exact
set transactionally. Change the Post trigger from one queryOpSet row to per-image rows:

```sql
INSERT INTO "BitdexOps"(entity_id, ops)
SELECT i.id, <set/remove ops>
FROM "Image" i WHERE i."postId" = NEW.id;
```

The match set is decided inside the publishing transaction — it cannot be partial, early, or
re-resolved differently on WAL replay. BitDex-side these are ordinary per-slot ops; `queryOpSet`
handling goes unused for Post. This is FD #69397's mechanism dead, this week, ~trigger-only.
(It is also exactly Phase 2's mechanism — C is a stepping stone, not a detour. Same for
availability/postedToId/baseModel/poi later.)

**The half that can't work — "emit a status when scheduled becomes published."** For a scheduled
post there is NO publish moment in PG: `publishedAt` is set to a FUTURE value at schedule time and
nothing changes at Tf — time just passes (Meili handles this by gating on `publishedAtUnix <= now`
at query time). To emit a status flip at Tf, Civitai would need a new cron/job that writes a column
at the right instant — a new upstream EVENT that can lag, miss, or double-fire: the same failure
class, relocated to PG. Deriving visibility from time (sortAt design, A or B) is the only shape
that doesn't recreate the problem somewhere.

**What C leaves standing:** all of family B — deferred map, activation at Tf, the verifier, the
~1/320 activation miss, reschedule bugs, the durability domain, ~3,100 LOC. C fixes the worst of
family A only.

**Fact-check on "images added to a published post can't be marked published":** backwards, per the
prod trigger — the Image INSERT branch reads Post directly per-image
(`SELECT extract(epoch from p."publishedAt") FROM "Post" p WHERE p.id = NEW."postId"`) and every
post-publish insert in the 2026-07-15 window was healthy. The droppable path is images inserted
BEFORE publish, whose publishedAt depended entirely on the fan-out resolving correctly later.

**Why the fan-out matched 7/20 (working hypothesis, two mechanisms, unconfirmed):**
(a) immediate-publish bursts — images uploaded seconds before publish; their inserts not yet
visible in the snapshot the query resolved against (the 16-second 5-post burst fits);
(b) scheduled-activation ticks — scheduled images are DEFERRED (no postId bits), invisible to the
bitmap query by design, dependent on the deferred-reach doc-scan path (the 19:00:00 tick fits).
Confirmation route remains arabella's live `BitdexOps` tail. Note both mechanisms die under C
(per-image ops don't resolve against the index) — which is evidence C targets the actual defect.

**Recommended sequencing: C now → B next.** C stops the user-visible bleeding and is reusable; B
deletes the machinery C doesn't touch. A is retained above as the reviewed record.

### 2.D — DECIDED DIRECTION (Justin, 2026-07-15 late): upstream-maximal, BitDex frozen, retire Meili

Priority restated: get it working with **zero-to-minimal BitDex changes**, push moving parts
upstream; engine simplification (B) parked as future work. The package:

1. **C triggers (all three fan-outs).** Post / ModelVersion / Model triggers emit **per-image ops
   resolved transactionally in PG** (`INSERT INTO "BitdexOps" SELECT i.id, ... WHERE i."postId" =
   NEW.id`). queryOpSet stops being exercised. Kills both fan-out failure mechanisms (moving-index
   resolution AND deferred-reach dependence — per-slot ops for deferred slots flow through the
   existing doc-only deferred branch, which is per-slot and boring). Engine change: none.
2. **Upstream idempotent re-emitter (the verifier, moved to the source of truth).** A PG cron
   (pg_cron or app job) re-emits per-image publish ops for posts whose `publishedAt` falls in
   `[now - lookback, now]` — catching both fresh publishes and scheduled posts that just became
   due. Ops are per-slot value sets: no-ops when BitDex is correct, heals when wrong. The ~1/320
   activation miss, reschedule stragglers, any future unknown — all self-heal within the lookback
   window, from PG's authoritative values, with zero engine machinery. BitDex's verifier can then
   be quieted/retired on its own schedule (its question is now answered upstream). Engine change:
   none. Sizing: posts published per 10-min window × images/post = small op volume; measure.
3. **Meili-parity gap field (hard blocker for retirement, unrelated to publish):** `model3dId`.
   (`combinedNsfwLevel` retired upstream — dropped from scope, Justin 2026-07-15.) It is a **Post
   column** (`Post.model3dId`, see model3d-visible-ids-batch.test.ts:6), inherited by images via
   postId exactly like `postedToId`: set at post creation (before images exist), effectively
   immutable. Plumbing: dump posts.csv enrichment +1 column; Image INSERT trigger reads it from
   Post alongside publishedAt; carried on the Post per-image trigger for completeness; index config
   `single_value, per_value_lazy` (like postId); Civitai BitDex query builder adds the filter
   (Meili already sends it, image.service.ts:4148-50). Engine change: none.

**Scope correction on #1 (Justin, 2026-07-15): per-image resolution is for the POST fan-out ONLY.**
ModelVersion (`baseModel`) and Model (`poi`) fan-outs can match MILLIONS of images (popular
checkpoints) — materializing those as per-image op rows is off the table. They keep `queryOpSet`.
Their risk profile tolerates it: the values change rarely, and a miss is a stale, diffable filter
value, not a missed publish. ⚠️ Known hole to file, not fix now: an over-cap fan-out
(`> max_fanout`) is SKIPPED ENTIRELY with only a counter (`query_op_set_rejected_total`,
ops_processor.rs:2523-2540) — for wide fan-outs the safety cap is itself a silent drop; the
eventual answer is a periodic baseModel/poi reconcile against PG (values are diffable), FOLLOWUP.md
material.

**The out-of-order `sortAt` symptom is (probably) the same defect, third face.** BitDex computes
`sortAt = GREATEST(existedAt, publishedAt)` engine-side, recomputed when the publish fan-out lands
(ops_processor.rs:2708). Miss the fan-out ⇒ BOTH `isPublished` stays false (invisible) AND `sortAt`
stays at `existedAt` = upload time (wrong position if it ever surfaces, e.g. via re-defer or
partial heal). One missed op, three symptoms: invisible in gallery, wrong feed order, false
unpublished. The re-emitter heals all three in one idempotent set, since the engine recomputes
sortAt on every publishedAt write. CAVEAT unverified: out-of-order among VISIBLE images could also
be the scannedAt-bump semantics (`existedAt` rising above `publishedAt` post-scan) diverging from
Meili's reindex timing — needs specimens (divergence-hunting compare endpoint) before assuming
one root.
4. **Keep as-is:** deferred-alive keyed on publishedAt, isPublished shadow, engine query path.
   No pending bitmap.

5. **Ingest `sortAt` from PG (Justin, 2026-07-15: "use the dead column") — ADOPTED, minimal form.**
   Replaces the engine-computed `GREATEST(existedAt, publishedAt)` — the piece that "isn't working
   consistently" — with PG-authored truth, without waiting on a 105M-row backfill:
   - **`BEFORE INSERT OR UPDATE` trigger on Image** sets
     `NEW."sortAt" = GREATEST(post."publishedAt", NEW."scannedAt", NEW."createdAt")`
     (NULL publishedAt → `GREATEST(scannedAt, createdAt)`, matching today's computed semantics for
     unpublished — no sentinel; visibility still gates on isPublished under the frozen engine).
     A BEFORE trigger writes the column in the same row version — **zero extra row updates**.
   - **Post publish trigger** does the bounded per-image
     `UPDATE "Image" SET "sortAt" = ... WHERE "postId" = NEW.id` — the SAME statement as the
     per-image fan-out fix (#1); one PR, double duty.
   - **The sync emission computes inline with COALESCE fallback**: the AFTER trigger emits
     `COALESCE(extract(epoch from NEW."sortAt"), GREATEST(...)) * 1000 → sortAtUnix` — so rows the
     backfill hasn't reached still emit correct values. **BitDex correctness is independent of
     backfill progress.**
   - **BitDex change is CONFIG-ONLY**: delete the `computed:` block for sortAt + the dormant
     `sortAtUnix → sortAt` mapping (deployment.yaml:261) goes live. No redump needed: existing slot
     values were computed with the same formula, so they're already correct; the atomic
     config flip just changes who authors future writes ([AR-2] coexistence hazard doesn't apply —
     computed is OFF from the flip, nothing recomputes over ingested values).
   - **Backfill of the PG column: optional and gradual.** Needed only when upstream feeds start
     ORDERing BY the column (Justin's "actually use it upstream"). Batched keyset UPDATE, paced,
     low-traffic windows, replica-lag watched; sortAt is already indexed so each write maintains
     that index — that's the real cost, and it's controllable by pace. Days, not a migration event.
   - Deferred/scheduled unchanged: a scheduled image's future sortAt sits in its doc until
     activation replays it, same as today.
   - **What it buys:** sortAt correct-by-construction at the source, diffable
     (`PG.sortAt vs BitDex.sortAt` = the audit query), the out-of-order class killed at the root
     rather than healed after the fact — and if Option B is ever picked up, its sortAt plumbing is
     already live.

What this does NOT fix (accepted, parked): the deferred/verifier LOC stays; publish remains an
event — but a self-healing one, verified from upstream. Options A/B remain on the shelf for when
simplification becomes the priority again.

### 2.6 Phase 2 (separate decision) — kill `queryOpSet` entirely

`availability` and `postedToId` still ride the Post fan-out; `baseModel` (ModelVersion) and `poi`
(Model) ride theirs. Same pattern, same race-by-construction, lower stakes (rarely-changing fields,
and a miss is a stale filter value — diffable, unlike a missed publish).

The same move applies: make each a transactional per-image write (denormalized Image columns or
trigger-driven per-row UPDATE), delete `type: fan_out` from trigger_gen, delete
`apply_query_op_set` + deferred-reach + dedup handling (~600 LOC), delete the Post CSV enrichment
from the dump. Cost: Post/ModelVersion metadata changes fan out as PG row updates (rare events,
bounded by images-per-post). **Recommended, but sequenced after Phase 1 proves out — one durability
redesign at a time.**

---

## 3. Failure-mode comparison (design justification)

| Scenario | Today | Redesign |
|---|---|---|
| Publish at Tf | Fan-out queryOpSet resolves vs moving index; deferred batch replayed; verifier watches; orphans possible (FD #69397: 13/20 stuck) | **Nothing crosses the wire.** Bit leaves derived `pending` when clock passes sortAt |
| Missed/dropped write | Unanswerable ("did the event fire?") — built a verifier to guess | `BitDex.sortAt != PG.sortAt` — a diff query; sampled reconcile repairs |
| Reschedule | Re-stamp deferred Tf (bug #313: 7-week straggler) | Trigger rewrites sortAt; pending re-derives — no state to re-stamp |
| Unpublish | Remove op → shadow flip must land | sortAt = sentinel; slot re-enters derived pending |
| Crash mid-anything | Deferred map durability gate; WAL-cursor coupling (PR #291: 49.7k orphans) | pending recomputed from layers at boot; nothing to lose |
| Scheduled leaks early | App forgot isPublished filter = leak (caller-applied) | Engine-applied exclusion; leak requires explicit `pending eq true` |
| Image added to scheduled post | INSERT trigger has no publishedAt source (PR #291 Mode B) | INSERT trigger computes own sortAt from Post — per-row, transactional |

The general cure (from `scheduled-publish-redesign.md` §3): **a durable value that can be compared,
not an event that must fire correctly.** This design makes every date-related fact a value.

---

## 4. Measurement gates (before commit — numbers, not vibes)

1. **Hot-path ANDNOT:** `working_set -= pending` on real sorted queries at 105M, P50/P99 delta.
   Expectation: pending is sparse (~10-100k), roaring ANDNOT near-free. Microbench + loadtest rig.
2. **Derivation tick:** `slots_above(sortAt_layers, now, alive)` at 105M — full 32-layer traversal
   cost. Expectation: few ms (32 bitmap ops, ANDs shrink fast). If >50ms, drop tick to 5s (a
   scheduled post going live ≤5s late is invisible in product terms).
3. **Transition maintenance:** cache + bucket per-slot add cost at burst size (~50 slots at a minute
   boundary), confirm zero whole-cache invalidations (watch `needs_rebuild` marks under the tick).
4. ~~Does anything need to SEE pending?~~ **Answered:** moderator notPublished (pseudo-filter,
   §2.3) and own-content second pass (PG-side, untouched).
5. **[AR-5] Publish-transaction latency** at realistic fan-out: P99 images-per-post, and the
   model-version publish worst case (many posts × many images), on a prod-replica clone.
6. **[AR-1] Leak-window test:** insert a future-sortAt slot, query the published snapshot before any
   timer tick — must be excluded (pins per-flush derivation).

## 5. Rollout sketch

Order rewritten per AR-2: **backfill strictly precedes cutover; computed and ingested sortAt never
coexist.**

1. Engine work behind config (`pending_exclusion: {sort_field: sortAt, tick_secs, sentinel}`), fully
   testable locally against the 105M dataset; measurement gates 1-3 + 6 (leak-window).
2. PG: Image INSERT-trigger sortAt + Post→Image sortAt trigger deployed (add-only — the columns are
   written but NOT yet emitted to BitDex); gate 5 (publish-txn latency) measured here. Backfill
   batched behind the triggers; sampled PG-side check that trigger-written and backfilled values
   agree with the Meili formula.
3. Sampled PG↔BitDex sortAt reconcile stood up BEFORE cutover (against the computed values first —
   proves the instrument's alarm can fire on a seeded mismatch).
4. **Cutover (one deploy, atomic):** index config deletes the `computed:` block AND starts ingesting
   `sortAtUnix`; sync config emits it; engine exclusion on; Civitai query builder drops
   `isPublished eq true` + swaps the mod view to `pending eq true`. Requires a redump or in-place
   sortAt re-ingest so pre-cutover slots carry ingested values. Shadow-compare vs Meili
   (`reference_shadow_divergence_signal`) over a window.
5. Delete: verifier → deferred machinery → publishedAt fan-out field → isPublished/publishedAt/
   existedAt fields. Each deletion its own PR with the reconcile still clean.

## 6. Adversarial-review record (2026-07-15)

Attacks that FAILED (verified, don't re-litigate): snapshot consistency (pending published in the
same InnerEngine snapshot as filters — queries can't mix states); burst handling (recompute, not
per-event replay); time buckets (already exclude `ts > now` structurally); negation universe
(covered by the top-level subtract); PG trigger recursion (Post→Image→Image-trigger is the intended
carry path, no cycle).

Coupling flag: deferred-reach deletion is coherent ONLY because deferred-alive is fully deleted —
a partial rollback must not split them.

## 7. Open items

- Meili-parity gaps noted but out of scope: `model3dId`, `combinedNsfwLevel` filters exist in Meili
  only.
- Phase 2 decision point after Phase 1 soak.
- CLAUDE.md inviolable #5 ("nothing ANDed into queries") needs amending if this ships — the exclusion
  is a deliberate, measured exception, engine-applied for exactly the reason the principle existed
  (callers forget).
