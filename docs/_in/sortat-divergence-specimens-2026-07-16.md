# sortAt Divergence Specimens — Prod Investigation (W1-4)

**Date:** 2026-07-16 · **Owner:** alexandra · **Mode:** read-only prod (no writes, no config changes)
**Task:** W1-4 of `docs/design/scheduled-publish-execution-plan.md` — characterize the out-of-order
`sortAt` symptom with real specimens; test the one-root theory; flag any class the upstream plan
would NOT fix.

---

## TL;DR / Verdict

- **The one-root theory (missed `publishedAt` fan-out ⇒ `sortAt` stuck at `existedAt` AND
  `isPublished=false` ⇒ invisible) does NOT hold as the dominant class.** The invisible class is
  **~0** in prod right now: **0 invisible in ~2,750 sampled published-in-past images** (upper bound
  ≈0.1%). It is the bursty FD-#69397 anecdote, not the steady state.
- **The dominant, steady divergence is VISIBLE-BUT-MISORDERED:** `isPublished=true`, `publishedAt`
  **present and correct** in BitDex, but `sortAt` left at `existedAt` (= `GREATEST(scannedAt,
  createdAt)`), i.e. **`publishedAt` was never folded into the sort key.** Rate **~7–28% of
  recently-published images**, persisting for at least 2 days, healed only by a redump.
- **Neither stated mechanism is the real cause.** It is **not** a missed fan-out (the field arrives),
  and **not** scannedAt-bump semantics or Meili reindex timing (Meili uses the *identical* GREATEST
  formula — confirmed from source). The real root: **BitDex's engine-computed `sortAt` does not
  reliably recompute when `publishedAt` arrives via the publish fan-out.**
- **The plan fixes the dominant class — for the right reason.** §2.D makes PG author
  `Image.sortAt = GREATEST(publishedAt, scannedAt, createdAt)` and ships it as an *ingested value*,
  deleting the engine-side recompute that is failing. Meili's index uses that exact formula
  (`images.search-index.ts:359`), so post-cutover BitDex and Meili converge, including the
  scannedAt>publishedAt case.
- **No class found that the plan would NOT fix.** Two behaviors to keep on the acceptance radar
  (details below): (1) scheduled-future posts are a genuine BitDex-hides / Meili-shows divergence
  that the frozen engine keeps hiding — intended, but the shadow-compare will show it as non-zero
  overlap unless excluded; (2) BitDex inconsistently *does* bump some scheduled slots' `sortAt` to
  the future value while keeping them `isPublished=false` — harmless today, but proves the same
  unreliable-recompute bug and means the `isPublished` gate is the only thing hiding future-dated
  slots.

---

## Access & method

- **PG** (exact replica BitDex reads from): queried via the in-cluster `bitdex-psql` pod →
  `cnpg-cluster-nvme0-rw` as user `bitdex` (statement_timeout=0). `Image` has no `publishedAt`;
  it comes only from `Post` via the fan-out — the field the theory says gets dropped.
- **BitDex**: `GET https://bitdex.civitai.com/api/indexes/civitai/documents/{id}` with
  `BITDEX_ADMIN_TOKEN` (from `bitdex-secrets`). Fields used: `sortAt`, `isPublished`, `publishedAt`,
  `existedAt`.
- **Meili**: sort-key rule taken from source (`model-share/src/server/search-index/
  images.search-index.ts:359`) and confirmed on 25 feed-head IDs via `civitai.com/api/internal/
  bitdex-compare`. Direct Meili access via the tunnel needs a key not present in the bitdex namespace.
- **now = 1784226342** (2026-07-16 06:25 UTC). Sampled ~1,400 enrichment docs + 2,000 invisible-hunt
  docs. Working data in the session scratchpad.

**Authoritative sortAt (all three engines agree on the formula):**
`GREATEST(post.publishedAt, image.scannedAt, image.createdAt)`; NULL publishedAt →
`GREATEST(scannedAt, createdAt)`.

---

## Classification by publish-age band

Random images from `Public` posts whose `publishedAt` falls in each band, enriched with BitDex docs.

| Band (age since publish) | n | correct | **misordered** | invisible (isPub=false) | not-yet-indexed |
|---|---|---|---|---|---|
| 5–30 min | 150 | 25 | 10 (6.7%) | 44 | 71 |
| 1–2 h | 150 | 111 | **39 (26.0%)** | 0 | 0 |
| 4–6 h | 150 | 139 | 11 (7.3%) | 0 | 0 |
| 10–14 h | 150 | 130 | 20 (13.3%) | 0 | 0 |
| 2–3 d | 150 | 108 | **42 (28.0%)** | 0 | 0 |
| 10–12 d | 150 | 150 | **0 (0%)** | 0 | 0 |

Reading:

- The 5–30 min band's `invisible=44` and `not-yet-indexed=71` are **fresh-indexing lag**
  (`publishedAt` genuinely not yet delivered to BitDex, <30 min old). They resolve — by the 1–2 h
  band both are 0. This is NOT the FD-#69397 drop.
- **Misordered is not transient.** It persists at 7–28% out to 2–3 days. The non-monotonic rate is
  sampling noise: misordering clusters **whole-post** (of 142 posts in one 500-image batch: 121
  all-correct, 17 all-misordered, 4 mixed), so one unlucky 20-image post swings a 150-row band.
- **10–12 d = 0% is the tell.** Everything older than the last redump was rebuilt from the dump's
  `GREATEST(...)` copy_query and is correct; everything published since drifts via the live fan-out.
  The drift accumulates continuously in prod and is only reset by a redump.

---

## Class 1 — visible-but-misordered (DOMINANT)

`isPublished=true`; BitDex `publishedAt` present and equal to PG; `sortAt` left at `existedAt`.
For every one of 205 specimens, `GREATEST == publishedAt` (so the misorder gap = `publishedAt −
existedAt`, i.e. exactly the amount `publishedAt` failed to move the sort key). Meili's `sortAtUnix`
= `GREATEST × 1000` = `publishedAt × 1000` for these — so **PG and Meili agree; BitDex is the outlier.**

25 specimens (epoch seconds unless noted):

| image_id | post_id | BitDex sortAt | PG GREATEST = Meili(s) | Meili sortAtUnix (ms) | BitDex publishedAt | isPub | gap (s) |
|---|---|---|---|---|---|---|---|
| 136901338 | 29818925 | 1784224429 | 1784224577 | 1784224577000 | 1784224577 | true | 148 |
| 136901397 | 29818925 | 1784224513 | 1784224577 | 1784224577000 | 1784224577 | true | 64 |
| 136901248 | 29818900 | 1784224385 | 1784224521 | 1784224521000 | 1784224521 | true | 136 |
| 136901245 | 29818900 | 1784224377 | 1784224521 | 1784224521000 | 1784224521 | true | 144 |
| 136901246 | 29818900 | 1784224350 | 1784224521 | 1784224521000 | 1784224521 | true | 171 |
| 136901249 | 29818900 | 1784224381 | 1784224521 | 1784224521000 | 1784224521 | true | 140 |
| 136713371 | 29780902 | 1784054568 | 1784224500 | 1784224500000 | 1784224500 | true | **169932 (47 h)** |
| 136713372 | 29780902 | 1784054573 | 1784224500 | 1784224500000 | 1784224500 | true | **169927 (47 h)** |
| 136713373 | 29780902 | 1784054577 | 1784224500 | 1784224500000 | 1784224500 | true | **169923 (47 h)** |
| 136867849 | 29812609 | 1784195678 | 1784224500 | 1784224500000 | 1784224500 | true | 28822 (8 h) |
| 136895626 | 29817693 | 1784219132 | 1784224260 | 1784224260000 | 1784224260 | true | 5128 |
| 136895625 | 29817693 | 1784219127 | 1784224260 | 1784224260000 | 1784224260 | true | 5133 |
| 136885709 | 29815642 | 1784209651 | 1784224140 | 1784224140000 | 1784224140 | true | 14489 (4 h) |
| 136885712 | 29815642 | 1784209661 | 1784224140 | 1784224140000 | 1784224140 | true | 14479 (4 h) |
| 136885708 | 29815642 | 1784209675 | 1784224140 | 1784224140000 | 1784224140 | true | 14465 (4 h) |
| 136885713 | 29815642 | 1784209706 | 1784224140 | 1784224140000 | 1784224140 | true | 14434 (4 h) |
| 136885715 | 29815642 | 1784209706 | 1784224140 | 1784224140000 | 1784224140 | true | 14434 (4 h) |
| 136901076 | 29818869 | 1784224230 | 1784224239 | 1784224239000 | 1784224239 | true | 9 |
| 136901055 | 29818859 | 1784224180 | 1784224184 | 1784224184000 | 1784224184 | true | 4 |
| 136901409 | 29818900 | 1784224519 | 1784224521 | 1784224521000 | 1784224521 | true | 2 |
| 136901404 | 29818900 | 1784224519 | 1784224521 | 1784224521000 | 1784224521 | true | 2 |
| 136900921 | 29818836 | 1784224084 | 1784224087 | 1784224087000 | 1784224087 | true | 3 |
| 136900909 | 29818822 | 1784224074 | 1784224082 | 1784224082000 | 1784224082 | true | 8 |
| 136900906 | 29818822 | 1784224074 | 1784224082 | 1784224082000 | 1784224082 | true | 8 |
| 136901584 | (batch A) | 1784224689 | 1784225125 | 1784225125000 | 1784225125 | true | 436 |

**Gap distribution (n=205):** min 1 s, median 321 s (~5 min), max **21,483,967 s (~248 days)**;
104/205 > 5 min, 86/205 > 1 h. The gap equals how long the image existed before its post published:
short for fresh uploads (seconds), but **hours to months for drafts / re-published / scheduled-then-
published images** — those land far down the Newest feed instead of at the top. This is a real,
severe, user-visible mis-ordering, not a sub-minute rounding artifact.

**Mechanism.** The `publishedAt` value reaches BitDex (100% of published-in-past docs carry the
correct value), but the engine's `sortAt` recompute-on-publish is unreliable — some posts get their
sort key bumped to `publishedAt`, others keep `existedAt`, clustering whole-post. This is consistent
with the publish fan-out being a `queryOpSet` that sets the `publishedAt` field while the sort-layer
update is separate/racy (the classic "sort bitmap not updated on upsert" root cause).

---

## Class 2 — invisible / dropped (FD #69397): NOT REPRODUCED, bounded ~0

Signature: post published-in-past in PG, but BitDex `isPublished=false` / `publishedAt=0` / doc
missing. **0 hits across ~2,750 published-in-past images** (750 across the 1 h–10 d bands + 2,000 in
a dedicated hunt on 1–24 h-old `Public` posts). Rule-of-three upper bound ≈ 0.11%. Matches the
memory's read that FD #69397 is a **bursty, sub-observable anecdote (n=3 events)**, not a steady rate
— random sampling will not reliably catch it, and it did not appear here.

---

## Class 3 — scheduled-future (Post `publishedAt` > now)

BitDex holds these `isPublished=false` (hidden) with `sortAt=existedAt`. Meili indexes them with
`sortAtUnix = future publishedAt` (they sort to the top / future of Newest and are visible in Meili).

| image_id | post_id | BitDex sortAt | PG GREATEST = Meili(s) | Meili sortAtUnix (ms) | BitDex publishedAt | isPub |
|---|---|---|---|---|---|---|
| 136841798 | 29807402 | 1784172435 | 1784230020 | 1784230020000 | 1784230020 | false |
| 136841793 | 29807402 | 1784172418 | 1784230020 | 1784230020000 | 1784230020 | false |
| 136841799 | 29807402 | 1784172424 | 1784230020 | 1784230020000 | 1784230020 | false |
| 136841791 | 29807402 | 1784172476 | 1784230020 | 1784230020000 | 1784230020 | false |

Two sub-notes:

- **Intended divergence, kept by the plan.** Under the frozen engine, `isPublished` gates visibility,
  so BitDex correctly hides scheduled posts that Meili currently *shows* early. This is arguably a
  Meili bug the migration fixes — but the shadow-compare in success-criterion #1 **will register
  non-zero overlap here** (Meili-only IDs at the head of Newest) unless scheduled/future-dated slots
  are excluded from the comparison. Flag for W3 acceptance so it isn't misread as a regression.
- **The same unreliable recompute shows up here too.** 52 of 298 scheduled images (17%) had their
  `sortAt` bumped to the **future** `publishedAt` while still `isPublished=false` (the other 246 kept
  `existedAt`). Harmless today because the `isPublished` gate hides them — but it confirms the engine
  *sometimes* folds `publishedAt` into `sortAt` and sometimes doesn't, and it means **the
  `isPublished` gate is the sole thing keeping future-dated slots out of the feed.** Any weakening of
  that gate would surface future-dated slots at position 1. The plan's move to PG-authored sortAt +
  the `pending`/derived-exclude direction (if ever taken) is the durable fix; under the frozen engine
  the gate must stay load-bearing.

---

## Does the plan fix each class?

| Class | Prod rate | Plan fixes? | Why |
|---|---|---|---|
| Visible-but-misordered | ~7–28% of recent, persists ≥2 d | **Yes** | PG authors `sortAt=GREATEST(...)`, ingested as a value via per-image trigger (W1-1/W1-2); deletes the failing engine recompute. Meili uses the identical formula → convergence. |
| Invisible / dropped (FD #69397) | ~0 (bursty) | **Yes** | Per-image materialized fan-out (W1-1) replaces the `queryOpSet`-resolves-a-query mechanism that dropped images; re-emitter (W1-3) is the safety net. |
| scannedAt > publishedAt | 37% have scan>pub (7% by >60 s) | **N/A — not a divergence** | Meili uses `GREATEST(publishedAt, scannedAt, createdAt)` too (`images.search-index.ts:359`), so PG-authored GREATEST matches Meili exactly. Previously feared as a residual; it is not. |
| Scheduled-future | 4,249 posts scheduled (next 7 d) | **Intended (kept hidden)** | Frozen engine keeps `isPublished` gating. Not a bug to fix; **exclude from shadow-compare** so it doesn't read as a miss. |

**No class was found that the plan would fail to correct.** The one caveat is a *measurement* one, not
a data one: the scheduled-future Meili-shows/BitDex-hides gap must be excluded from the success-#1
shadow-compare window, or it will show as residual divergence that the plan is not meant to remove.

---

## Reconciling with the one-root theory

- **Missed fan-out** — refuted as the dominant cause: `publishedAt` is present in 100% of
  published-in-past docs; the field is not being dropped in steady state.
- **scannedAt-bump / existedAt>publishedAt semantics** — refuted as a *divergence* source: Meili
  applies the same GREATEST, so those cases match, not diverge.
- **Meili reindex timing** — not the cause: the gap is BitDex holding `existedAt` while both PG and
  Meili hold `publishedAt`; it is BitDex that is stale relative to a value it already received.

The unifying root is narrower and more actionable than either framing: **the engine's `sortAt` does
not reliably fold in `publishedAt` on publish.** The plan removes engine-side sortAt computation
entirely (PG authors the value), which is exactly the right lever.

---

## [PR-m5] Which state does BitDex hold scheduled slots in? — deferred map vs alive-with-shadow

**Answer: ALIVE, all filter/sort bits set, gated by an explicit `isPublished=false` shadow bitmap.
They are NOT in the deferred map.** This matches `scheduled-publish-redesign.md §8` and **refutes the
deferred-alive (not-alive, activated by `activate_due`) model for the prod flow.**

### Empirical proof (post 29807402, `publishedAt` = 1784230020 ≈ 1 h in the future at now=1784226342)

| Query (skip_cache, no cache) | total_matched | Meaning |
|---|---|---|
| `{postId: 29807402}` | **10** | Slots are **alive** — postId filter bits are set and findable. A deferred/not-alive slot would return 0. |
| `{postId: 29807402, isPublished: true}` | **0** | The `isPublished=true` gate hides them from published feeds. |
| `{postId: 29807402, isPublished: false}` | **10** | An explicit `isPublished=false` shadow bitmap carries all 10 slots (not just a docstore default). |

`alive_count` = 118,488,107; no `deferred_count` metric is even exposed. Prod config **does** have
`deferred_alive` enabled (`source_field: publishedAt`, `sweep_interval_secs: 600`,
`sweep_limit: 20000`) and `sortAt` as a `computed: greatest(...publishedAt)` field — but the map is
bypassed by the actual data flow (below).

### Why the map is bypassed (mechanism, from code)

- `Image` rows carry **no** `publishedAt` column, so images always insert **alive** with
  `isPublished=false` (derived from absence). The slot is in the index from the moment it's scanned.
- The Post publish/schedule fan-out is a `queryOpSet` (`postId eq X`) carrying `Set publishedAt`.
  When `publishedAt` is **future**, `apply_query_op_set` takes a **deferred branch that keeps the
  slot ALIVE** and merely stores the future timestamp in the doc as raw PG truth — it deliberately
  does **not** apply it to the `sortAt` layer or flip the `isPublished` shadow
  (`ops_processor.rs:1327-1345`, `1447-1469`; `concurrent_engine.rs:1710-1745`). The slot is never
  removed from the alive bitmap, so it never enters `slot.rs`'s deferred map.
- The `slot.rs` deferred map (`schedule_deferred` / `activate_due`) is only reached via the
  `diff_document` / PATCH path when `publishedAt` arrives **with the document**
  (`mutation.rs:384-398`) — which the Image→Post flow never does.

### Consequence for the plan — this is load-bearing, and it contradicts [PR-B2]

- **Tf activation is a SHADOW FLIP, not a deferred-map drain.** At Tf no new op arrives (the publish
  is just wall-clock crossing the scheduled time). The transition — flip `isPublished` false→true
  **and** fold `publishedAt` into the `sortAt` layer — is driven by the **overdue sweep (600 s)** plus
  an **opportunistic safety-net recompute** whenever any later op touches the slot
  (`ops_processor.rs:1447-1469`). It is **not** `activate_due` (`slot.rs:280-298`), because these
  slots are not in the map. **[PR-B2]'s claim that "the activation driver at Tf is the engine's
  wall-clock `activate_due`" is inaccurate for the dominant prod flow** — that path drives only
  map-resident slots, which the fan-out flow does not create. The re-emitter (W1-3) is therefore
  **more load-bearing than [PR-B2] frames it**: a re-emitted `Set publishedAt=<now-past>` is what
  reliably triggers the recompute + safety-net flip for a slot the sweep hasn't reached.
- **This exact gap has already bitten prod:** `ops_processor.rs:1450` records a 2026-07-03 audit
  finding ~49.7k slots that crossed Tf but were never flipped (`isPublished` stuck false, excluded
  from published feeds) — the deferred-map scheduling was "lost" and the safety net was added to
  bound the blast radius. See `docs/_in/deferred-publish-isPublished-corrected-diagnosis-2026-07-03.md`.
- **What works vs what doesn't, empirically:** the `isPublished` flip half of activation **does**
  work in prod today — 0 stuck-invisible in ~2,750 past-published images, and scheduled-ahead
  specimens that already crossed Tf (e.g. 136713371, existed 2 days before its post published) show
  `isPublished=true`. The **`sortAt`-layer half is the unreliable one** — that is precisely the
  misordering class in this report (the activation flips visibility but fails to move the sort key).
- **The plan does not change this activation model** (engine frozen): W1-1's per-image materialized
  fan-out still emits `Set publishedAt` per image; a future value still hits the same alive-quarantine
  branch. So the sweep/safety-net shadow-flip at Tf remains the mechanism post-cutover. The plan's
  improvement is orthogonal and real: PG authoring `Image.sortAt` and shipping `sortAtUnix` per image
  makes the **sort-key** correct independent of whether the engine's activation recompute fires —
  which is exactly the half that is broken today.

**Net for W3 acceptance:** treat Tf activation as sweep-driven shadow-flip + re-emitter, not
`activate_due`. The scheduled-post lifecycle E2E ([W3/#9]) must specifically assert that a
scheduled-ahead slot, after crossing Tf **with no op arriving**, both (a) flips `isPublished` true
and (b) has `sortAt` == `publishedAt` — driven only by the sweep/re-emitter, since that no-op-arrives
case is the one the current engine gets wrong.
