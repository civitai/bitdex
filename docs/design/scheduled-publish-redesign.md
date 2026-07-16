# Scheduled-Publish Redesign — `sortAt` + a pending bitmap

**Status: PROPOSAL. Not decided, not designed, not scheduled.** Justin's, 2026-07-15. This doc records the
proposal, the failure classes it targets, what it does *not* fix, and what must be measured before anyone
commits. Nothing here has been benchmarked.

---

## 1. The proposal

1. **PG**: a trigger writes `Image."sortAt"` **when a post is scheduled** — and on reschedule, and on
   immediate publish. `sortAt` becomes the authoritative sort key. **A scheduled post carries a FUTURE
   `sortAt`.**
2. **BitDex**: holds `sortAt`. That's it. No `publishedAt`, no `isPublished` shadow.
3. **A `pending` bitmap** marks slots not yet live. **The slot is otherwise fully indexed** — every bit set,
   findable, present in the docstore.
4. **The engine applies `AND NOT pending` to every query.** Always. Not the caller's job.
5. **A sweep** clears `pending` for slots whose `sortAt <= now`. Going live = clearing one bit, locally.

### The consequence that matters

**If `sortAt` is written at SCHEDULE time, then publishing sends BitDex nothing at all.** No op crosses the
wire at Tf. There is no batch to apply, lose, or verify. The sweep clears a bit from state BitDex already
holds.

That is the whole design. Everything below is a consequence of it.

---

## 2. The question this doc exists to answer

> *"It seems like it's all tied to the queryOpSet, right? Or have we been running into issues unrelated to
> that as well?"*

**No. It is not all `queryOpSet`.** There are **two distinct data families** plus **a third class that has
cost as much engineering time as either**. They share one root, which is why they feel like one thing.

### Family A — the fan-out family (`queryOpSet` is its worst instance)

**One PG row must become N BitDex writes, and BitDex works out which N.**

The `Post` publish trigger emits **one** op row containing a **query**, not per-image writes:

```sql
_query := 'postId eq ' || NEW."id"::text;
_ops   := [{op:'remove', field:'publishedAt', value: OLD."publishedAt"},
           {op:'set',    field:'publishedAt', value: NEW."publishedAt"}];
INSERT INTO "BitdexOps"(entity_id, ops) VALUES (NEW.id, [{op:'queryOpSet', query:_query, ops:_ops}]);
```

BitDex resolves that query **against its own index, at apply time**, and writes `publishedAt` to whatever
matched *right then*. **Fire and forget: no per-image op, no retry, and no check of what it matched.**
Resolve to 7 of 20 and 13 images silently never receive a `publishedAt`. Nothing revisits them.

Confirmed in prod 2026-07-15 (arabella): post `29797439`, **13 of 20 images stuck at `publishedAt = 0`**,
`isPublished = false`, **invisible in the Images gallery while the post showed in the Posts tab** — FD #69397's
report verbatim. Still zero 5.8h later. 22 such images across 7 posts in a 15,942-image window; **none
self-healed**.

**The confirming half — there are two paths to `publishedAt`.** The `Image` INSERT branch reads `Post`
directly, per image:

```sql
'value', to_jsonb((SELECT extract(epoch from p."publishedAt")::bigint FROM "Post" p WHERE p.id = NEW."postId"))
```

⇒ **image inserted BEFORE publish** → depends entirely on the queryOpSet → **droppable**
⇒ **image inserted AFTER publish** → reads `Post` itself → **correct, queryOpSet never involved**

Every one of the 22 dropped images was pre-publish. Every post-publish insert was healthy. **The rule was
never age or count — it's which code path carried your `publishedAt`.**

**Other members of family A** (all previously root-caused, all the same shape):
- Fan-out evicted `DocCache` by **Post** entity id rather than the matched **image** slots → stale docs served
  ~20 min until LRU (2026-07-08).
- Fan-out dedup collapse → total no-op (2026-07-09).
- `Post` fan-out fired **pre-insert**; the `Image` INSERT trigger had no `publishedAt` source at all
  (PR #291, Mode B).

### Family B — the deferred/activation family

**A scheduled post lives in a map that is not a bitmap, is not durable like the rest of the index, and
requires an event to be replayed at exactly the right instant.**

- **Deferred-map durability loss** — the map was never in the opslog; the merge thread regressed the flush
  thread's write; the WAL cursor went durable ahead of the deferred state → **~49.7k orphans** (PR #291).
- **#313 reschedule-drop** — a reschedule on an already-deferred slot failed to re-stamp Tf. Fixed v1.1.42.
  Seven-week straggler observed: **dropped in May, fired in July.**
- **The ~1/320 activation-miss** — a scheduled post never activates at Tf. **Root cause still unreproduced.**
- **The entire verifier apparatus** — #314, #316, #320, #321, PR #323, the publish barrier, `publish_seq`,
  `VerifyGate`, the ring, its persistence — **exists solely to detect family B's failures.**

**This is not the same bug as family A.** A durability domain losing a map is not a query resolving against a
moving index. They are different mechanisms with different fixes.

### Family C — instrument failures

Not data bugs, and they cost as much time as the data bugs.

- The verifier raised **false orphans for months** by reading pre-publish state mid-flush. Fixed v1.1.48.
  **Zero data was ever lost in that class** — the "residual data-loss bug" chased all day on 2026-07-15
  **did not exist**.
- On 2026-07-15, three adversarial review passes over PR #323's design found **zero defects in the design**
  and **~10 in the prose around it**: a comment claiming a sampler ran unconditionally when it sat behind an
  early return; a test named for an invariant it did not pin; an acceptance criterion that could never fire;
  a ratio with no denominator; a status board asserting a disproven claim in its most authoritative voice.

**See `reference_verification_rules_2026_07_15` (agent memory) for the full set.** The short version:
*the compiler checks the design; nothing checks the prose.*

---

## 3. The root the families share

**Both A and B are a WRITE THAT MUST BE RESOLVED OR REPLAYED LATER, AGAINST STATE THAT HAS MOVED, WITH NO
RECORD OF WHAT IT WAS SUPPOSED TO AFFECT.**

| | what is deferred | what moves under it | why it can't self-detect |
|---|---|---|---|
| **A — queryOpSet** | *which slots* to write | the index the query resolves against | it never knew how many it *should* have matched |
| **B — activation** | *when* to apply a batch | the snapshot the batch lands in | it never recorded whether the batch landed |

**Neither has an error path, because neither knows what correct looks like.** A race with no error is not a
bug you fix — it's a bug you rediscover.

### The cure, stated generally

> **Make it a durable VALUE that can be compared, not an EVENT that must fire correctly.**

- A **value** can be diffed against the source and repaired. `BitDex.sortAt != PG.sortAt` is a query.
- An **event** cannot. *"Did an op fire correctly three hours ago?"* has no answer, only consequences.

**This is why the redesign works.** `sortAt` is a value. `publishedAt`-via-queryOpSet is an event.

---

## 4. What the redesign fixes — and what it doesn't

### Deleted outright

- **Family B, entirely.** No batch on the wire ⇒ nothing to lose ⇒ **no orphan class**. The deferred map
  becomes a bitmap (the thing the engine is built around, rather than a special case). Going live is a local
  bit-clear.
- **The whole verifier**: #320, #321, PR #323, the barrier, `publish_seq`, `VerifyGate`, the ring and its
  persistence, the gate counters, the `Membership` enum. **All of it verifies a batch that would not exist.**
- **Durability domain #4** in `write-pipeline-overhaul.md`'s table — the deferred MetaStore file.
- **`isPublished`**, the time-aware shadow. `NOT pending` replaces `isPublished = true`.
- **The publish-time `queryOpSet` for `publishedAt`** — family A's worst instance, and the live bug above.
  The specific trigger line ceases to exist.

### NOT fixed — state this plainly or it will be forgotten

- **`queryOpSet` SURVIVES for every other field.** The same trigger shape fans out `availability`,
  `postedToId`, and any other post-level field that propagates to images. **The redesign removes one caller,
  not the pattern.** ⇒ *Ship a query, resolve it later against a moving index, never check what it matched*
  remains live.
- **The fan-out MOVES; it is not deleted.** Post-scheduled → N images still need `sortAt`. What changes is
  that it stops being **time-critical**: a missed schedule-time write is a **stale field you can detect and
  reconcile**, not a missed event. That is a large improvement and it is not elimination.
- **A missed reschedule still publishes early.** Stale `sortAt` ⇒ the sweep un-pends at the old time. Same
  class, simpler mechanism, **now auditable**.
- **The ~1/320 activation-miss's root cause** is never found — it's made irrelevant instead. Acceptable, but
  note we'd be closing an open investigation by deleting its subject.

---

## 5. Costs and risks — unmeasured

1. **The exclude goes on the hot path.** `CLAUDE.md`'s design principle is that filter bitmaps are kept clean
   *specifically so* nothing is ANDed into queries. This adds one ANDNOT to **every** query. Probably cheap
   — `pending` is sparse, roaring ANDNOT against a small set is near-free — **but that is an unmeasured
   premise, and unmeasured premises died four times in one evening on this exact subject.**
2. **A future `sortAt` sorts to POSITION 1** on any `sortAt DESC` feed. So the exclude is **load-bearing for
   correctness on every sorted query**, not only filtered ones. **Mitigated by being engine-applied**: today
   the app must remember `isPublished = true`, and *"the app must remember"* is how FD #69397 happened.
   Engine-applied is strictly better than the status quo.
3. **Cache interaction.** `pending` changes at each sweep ⇒ invalidation semantics for the unified cache are
   unexamined.
4. **Migration.** Every existing scheduled post needs a correct `sortAt` and a correct `pending` bit.

### The stronger version: **derive `pending`, don't store it**

`pending = {slots where sortAt > now}` is **computable from the sort layers we already have**. A derived
bitmap **cannot drift** — no durability domain, no persistence, no reconcile, nothing to lose on a crash.
The sweep stops being *"maintain a set correctly"* and becomes *"recompute a cheap thing."*

Same move as the overhaul's *"time buckets are a derived cache, not durable state"*, and the same move that
was correct roughly ten times in one evening: **make the bad state inexpressible rather than guard it.**

---

## 6. Before committing — three measurements

1. **ANDNOT cost** on a real sorted query at 105M.
2. **Is deriving `pending` from the sort layers cheap enough to run every second?**
3. **Does anything legitimately need to SEE pending content** — owner preview, moderation? That's where
   *"the engine always excludes"* meets its first exception, **and exceptions to always-invariants are where
   every defect in section 2's family C lived.**

---

## 7. Relationship to `write-pipeline-overhaul.md`

**Complementary, not overlapping — different axes.**

The overhaul's invariant is **"cursor advance = durable apply"**: it collapses nine independently-durable
stores into one apply-log + checkpoint. It answers *"does a write survive a crash?"*

This proposal answers *"should the write exist at all?"*

They agree on deleting the deferred map (domain #4) and reach it differently — the overhaul by making it
crash-consistent, this by making it a bitmap. **This proposal does not replace the overhaul**: the overhaul
still owns doc/bitmap disagreement, time-bucket drift, dictionary durability, and WAL retention. It also
does **not** fix `sortAt` staleness or family A — a durability design cannot repair a value that was already
wrong on arrival.

---

## 8. Evidence log

- **`matched = 514 = PG's published count, exactly**` — the deferred gate holds today. 19 genuinely-scheduled
  images correctly excluded. `isPublished` is **time-aware**, not a null-check (measured: a live scheduled doc
  carries a future non-null `publishedAt` **and** `isPublished: false`).
- **Bits are set BEFORE Tf** — scheduled slots are alive with `postId`/`userId` bits. Exclusion is
  `isPublished`, not the bitmap. **`bits present` does not prove activation fired.**
- **22 invisible images / 7 posts / 15,942-image window, no truncation, none self-healed at 2.3-5.8h.**
- **It's WHEN, not WHICH**: 5 posts, 5 different users, inside **16 seconds**, against a 0.19% base rate.
  63% of that window's publishes hit, 3 clean ⇒ partial, not an outage. The 19:00:00 hit was a
  **scheduled-activation tick** — 12 posts at one instant, 1 hit. **Bursts both times.**
- **REFUTED, each by one measurement**: scheduled posts leak early; stale `Image.sortAt` buries them on the
  feed (the legacy `ORDER BY i."sortAt"` path isn't used, and Meili computes `sortAt` correctly at index
  time); `isPublished` is a null-check; unrated content leaks to anonymous viewers (Meili's ingestion gate
  excludes unrated content structurally — 1 image site-wide in 7 days).

> ⚠️ **n = 3 EVENTS. This is an anecdote, not a rate.** 22 images across 7 posts, but 5 of those posts landed
> in one 16-second burst. The **denominator** (15,942 images / 3,593 posts, full window, no `LIMIT`) is the
> only solid number here. **Concurrency does not rank-order the drops** (non-monotonic at every width, and
> underpowered at 7 hits — *"no signal", not "no relationship"*). **One true partial** (29797439, 13/7),
> **n=1, unexplained — do not name a rule off it.**

### The one open question that survives the redesign

**Why did `postId eq 29797439` resolve to 7 of 20 slots at apply time, when all 20 carry `postId` bits now?**

Prime candidate: **`queryOpSet` resolves against a snapshot predating the batch that carried those INSERTs**
— the *identical* pre-publish-read shape as the verifier bug fixed in v1.1.48. If so, we spent 2026-07-15
building the vocabulary for the bug rather than the fix for it, and would recognise it in minutes.

**Route: a live tail of `BitdexOps`.** The outbox drains to ~1 row on poll, so past windows are
**unreconstructable** — catch the next one in the act rather than reconstructing.
