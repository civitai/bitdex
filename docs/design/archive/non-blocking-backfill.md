# Non-Blocking Backfill

**Status:** Not started (future improvement)
**Date:** 2026-03-16
**Depends on:** [filter-only-fields.md](filter-only-fields.md), [civitai-collections.md](civitai-collections.md)

## Problem

The current auto-backfill blocks sync startup entirely. While a filter_only field (e.g., collectionIds) is being backfilled from PG, the outbox poller doesn't run. This means **no data syncs for any field** — image updates, tag changes, metric refreshes — until the backfill finishes.

For collectionIds (~157M rows), this could block sync for several minutes. Acceptable for initial deployment but not for future fields or re-backfills.

## Current Flow (blocking)

```
pg-sync sync startup
  → wait for BitDex health
  → auto-backfill collectionIds (blocks)        ← ALL sync paused
    → COPY from PG → CSV
    → mmap + rayon parse → bitmaps
    → write to BitmapFs
    → reload existence set
    → set cursor
  → start outbox poller                         ← sync resumes
  → start metrics poller
```

The outbox retains all changes during the backfill window, so nothing is lost — but latency increases for all fields, not just the one being backfilled.

## Desired Flow (non-blocking)

```
pg-sync sync startup
  → wait for BitDex health
  → start outbox poller (all fields except backfilling ones)
  → start metrics poller
  → spawn background backfill task
    → COPY from PG → CSV
    → mmap + rayon parse → bitmaps
    → write to BitmapFs
    → reload existence set
    → set cursor
    → signal poller: "collectionIds ready, start processing its events"
```

## Implementation Options

### Option A: Outbox poller skips filter_only fields during backfill

The poller already fetches enrichment data (tags, tools, collections) in parallel. During backfill, it could skip the `fetch_collections` enrichment and the `filter_sync` call for the field being backfilled. Once the backfill task signals completion, the poller starts including it.

**Pros:** Simple, no cursor changes.
**Cons:** Events for the backfilling field that arrive during the window are silently skipped. They'd need to be replayed or the backfill must cover them.

### Option B: Per-field outbox cursors

Each filter_only field gets its own outbox cursor. The main poller uses the primary cursor for document fields. Each filter_only field has an independent cursor that starts after its backfill completes.

**Pros:** Clean separation, no skipped events.
**Cons:** More complex cursor management. Outbox cleanup must consider all cursors.

### Option C: Background backfill with event buffering

Run the backfill in background. Buffer any outbox events that touch the backfilling field. After backfill completes, replay the buffered events via filter-sync, then switch to live processing.

**Pros:** No lost events, no cursor complexity.
**Cons:** Memory usage for buffered events. Replay ordering.

## Recommendation

**Option A** for simplicity. The backfill produces a complete baseline from PG. Any changes during the backfill window are in the outbox. After the backfill completes and the poller starts processing the field, the next outbox cycle will pick up any changes and re-sync them via filter-sync (which sends the full current value set from PG, so it's self-correcting).

The key insight: the outbox poller's enrichment query (`SELECT collectionId FROM CollectionItem WHERE imageId = ANY($1) AND status = 'ACCEPTED'`) always returns the **current** state from PG. So even if an event was "missed" during backfill, the next event for any image in that collection will produce a correct full sync.

## Scope

- Estimated effort: ~1 day
- Files: `src/pg_sync/outbox_poller.rs`, `src/bin/pg_sync.rs`
- No engine changes needed
- No new endpoints needed
