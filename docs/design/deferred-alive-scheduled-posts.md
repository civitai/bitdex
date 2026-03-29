---
status: ACTIVE
updated: 2026-03-28
---

# Design: Scheduled Posts Visibility Fix

**Author:** Ollie (perf eng)
**Date:** 2026-03-25
**Status:** REVISED after Justin's review

## Problem

Scheduled posts (publishedAt in the future) are visible to users when querying "all time".

### Root Cause: Bulk Loader Bypasses Deferred Alive

@justin: The nd.json loader doesn't ever get used anymore. We're using the CSV loader at start time. C:\Dev\Repos\open-source\bitdex-v2\src\pg_sync\bulk_loader.rs

@ollie: Confirmed. I had the wrong loader. The CSV bulk loader (`src/pg_sync/bulk_loader.rs`) and single_pass loader (`src/pg_sync/single_pass.rs`) both bypass `diff_document()` entirely — they build bitmaps via `BitmapAccum` and call `apply_bitmap_maps()` directly, which sets all slots alive unconditionally. This is the sole ingestion path that doesn't check `deferred_alive`. The pg-sync incremental write path (POST /documents/upsert) correctly routes through `diff_document()` and defers future posts.

**The bug is ONLY in the bulk load path.** Once deferred_alive is checked there, the problem is fixed.

### How Deferred Alive Currently Works

**Write path** (`src/mutation.rs:93-153`):
- `diff_document()` checks `config.deferred_alive` FIRST
- If `activate_at > now`, returns only `DeferredAlive { slot, activate_at }` — NO filter/sort/alive ops
- The slot is NOT set alive; the doc is written to docstore but invisible in queries

**Activation** (`src/concurrent_engine.rs:1545-1600`):
- Flush thread calls `activate_due(now_unix)` each cycle
- For each due slot: reads stored doc, runs `diff_document` (now as fresh insert), applies full bitmap mutations
- Persists updated deferred map to MetaStore

@justin: What tests do we have in place to verify that the write and activation is actually working the way that we intended it to? Not just, like, unit tests, but ensuring that, like, as time progresses, things are getting properly activated, if you will. This almost seems like it needs to be more of an end-to-end test or something. I guess I just want to make sure that this is actually functioning.

@ollie: We have unit tests in `tests/time_handling_test.rs` — `test_deferred_alive_far_future_invisible()`, `test_deferred_alive_past_timestamp_visible()`, `test_mixed_deferred_and_immediate()`. There's also an E2E test in `tests/e2e/e2e-load-save-snapshot.mjs` that tests the load/save cycle with deferred config. BUT — you're right that we don't have a time-progression E2E test that inserts a future doc, waits for the activation time, and verifies it becomes visible. That's a gap. We should add one.

### Why "All Time" Is Actually Protected (Correction)

@justin: I mean, with the way that you said that deferred alive is supposed to work, it seems like it shouldn't be showing up in all time without time buckets or whatever. It seems like the real bug is probably that we didn't handle deferred alive on bulk load, right?

@ollie: You're right. I was wrong in my original analysis. If deferred alive works correctly, a future-dated post is NEVER set alive and NEVER has filter/sort bits set. It simply doesn't exist in any bitmap. "All time" queries are safe because there's nothing to find — the slot isn't in the alive bitmap, isn't in any filter bitmap, and has no sort layer bits. The time bucket mechanism is irrelevant here; it's an optimization for range queries, not a visibility gate.

The ONLY way a future post leaks into queries is if it enters the bitmaps via a path that doesn't check deferred_alive — which is exactly what the bulk loader does.

## Constraint

We CANNOT add `publishedAt <= now()` to every query. This would make every query unique (now() changes) and break the unified cache entirely. Cache keying depends on stable, deterministic filter clauses.

## Options

### Option A: "Published Now" Bitmap (RECOMMENDED)

Maintain a `published_now` bitmap in the engine, updated by the flush thread. This is conceptually identical to the alive bitmap but excludes future-dated slots.

**Mechanism:**
- New field on `InnerEngine`: `published_now: Arc<RoaringBitmap>` (or `VersionedBitmap`)
- At boot: `published_now = alive AND NOT future_slots` where `future_slots` is derived from scanning the publishedAt sort field for values > now()
- Flush thread maintenance: every cycle (or every N seconds), scan deferred map + check newly activated slots. Update `published_now` via `Arc::make_mut()` and publish with ArcSwap.
- Query path: **replace alive bitmap with published_now wherever alive is used as the universe for negation (NotEq, Not)**. Since we do clean deletes (filter bitmaps don't have stale bits), the alive bitmap is only used for:
  1. Stats/counts (alive_count)
  2. Negation operators: `NotEq` and `Not` need a universe to subtract from
  3. Slot allocation (clean bitmap)

**For negation**: use `published_now` instead of `alive`. This automatically excludes future posts from `NOT nsfwLevel=1` or similar queries without changing cache keys.

**For sort queries without filters**: the sort traversal starts from an `initial_candidates` bitmap (either a filter result or the full universe). When no filters are applied, this is currently `alive`. Change to `published_now`.

**Key advantage:** No change to query syntax, cache keying, or the external API. The bitmap is maintained asynchronously like alive/clean/time_buckets.

**Staleness:** If `published_now` is refreshed every flush cycle (50-100ms), a scheduled post becomes visible within one cycle of its publishedAt time. For practical purposes this is instant.

**Memory:** One additional bitmap at 107M scale: ~13 MB (same as alive). Negligible.

**Performance:** Computing `published_now` is trivial — it's just `alive - deferred_set`. The deferred set is already maintained. No per-query cost.

@justin: This seems repetitive to me, and also kind of hard-coding it to our use case, right? The deferred alive thing. It's for publishing stuff. We already have the isPublished field that's being driven by the published at, or whatever, right? Not being zero or not being null. So you might need to look at that. And with us already having the deferred alive set. Like, I don't know that we need this. The alive bitmap should only show things that are currently alive. So having to do any sort of, is this published? Seems repetitive.

@ollie: You're right — this is redundant. The alive bitmap IS the "published now" bitmap when deferred alive works correctly. A separate `published_now` bitmap duplicates what `alive` already does. The `isPublished` field in the config (`deploy/configs/civitai-index.json`) is an `exists_boolean` derived from `publishedAtUnix` — it checks whether the field exists, not whether it's in the future. So it's a different concept, but the point stands: we don't need a new bitmap. We need to fix the bulk loader. **Withdrawing Option A.**

### ~~Option A: "Published Now" Bitmap~~ — WITHDRAWN

Redundant. If deferred alive works correctly, `alive` already excludes future posts. Adding `published_now` duplicates this and hard-codes scheduling logic into the query path.

### Option B: Fix Bulk Loader to Check Deferred Alive (NOW RECOMMENDED)

Fix the CSV bulk loader (`src/pg_sync/bulk_loader.rs`) and single_pass loader (`src/pg_sync/single_pass.rs`) to check `deferred_alive` config during bitmap building. Future-dated documents should be written to docstore but NOT have their alive/filter/sort bits set.

**This is the only fix needed.** Once bulk load respects deferred alive, the existing system handles everything else:
- Flush thread activates deferred slots via `activate_due()` when their time arrives
- The slot enters the alive bitmap and filter/sort bitmaps at activation time
- "All time" queries naturally exclude future posts because they're not in any bitmap

@justin: The no time filter equals no protection issue might not be correct. I think that this needs to be verified based on the way that you've described that the alive system should work.

@ollie: Confirmed — you were right. "No time filter = no protection" was incorrect. When deferred alive works, there IS protection for all queries because the slot isn't in any bitmap at all. The issue was exclusively the bulk loader bypass.

### Option C: Periodic Sweep of Alive Bitmap

A background task that scans publishedAt sort layers and removes future-dated slots from the alive bitmap, then re-adds them when their time arrives.

**Problem:** Modifying the alive bitmap directly is dangerous — clean deletes depend on alive being authoritative. Removing a slot from alive without clearing its filter/sort bits violates the "filter bitmaps are always clean" invariant. Queries would AND with alive (which we currently skip) or get stale results.

**Verdict:** Rejected — violates design principle #5 (clean deletes).

## Recommendation

**Implement Option B only: fix bulk loaders to check deferred_alive.**

### Implementation Plan

1. **`src/pg_sync/bulk_loader.rs`** — during `BitmapAccum` building, check `config.deferred_alive`. For each document where the source field timestamp > now(), skip adding to alive/filter/sort bitmaps. Instead, add `(slot, activate_at)` to a deferred set that gets written to MetaStore after loading.
2. **`src/pg_sync/single_pass.rs`** — same check during bitmap accumulation.
3. **Add time-progression E2E test** — insert a doc with `publishedAt` 2 seconds in the future, verify invisible, sleep 3 seconds, verify visible after flush thread activation. This closes the testing gap Justin identified.
4. **Expose metric** — `bitdex_deferred_pending_count` gauge (already partially tracked via `deferred_count()`)

### Cache Impact

None. Deferred slots were never in the cache to begin with (they have no bitmap presence). Activation adds them through the normal mutation path which handles cache invalidation.

### Testing

- Unit test: bulk loader skips future-dated docs when deferred_alive configured
- Unit test: deferred docs written to docstore but not in alive bitmap
- E2E test: time-progression — insert future doc, verify invisible, wait for activation, verify visible
- E2E test: bulk load with mix of past and future docs, verify correct alive count
