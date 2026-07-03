# Metrics Poller — Config-Driven Overlapping-Window Redesign

Status: proposed (2026-07-02). Author: jack. Verified live against prod CH by Mark; deploy owned by Josh.

## Problem

The steady-state ClickHouse metrics poller (`src/pg_sync/metrics_poller.rs`) fed
~zero metric ops for roughly a full day (flat zero 07-01 23:07Z → 07-02 23:16Z,
one blip). Reactions/comments/collect counts in BitDex went stale.

Two independent faults, both structural:

1. **Wrong table.** Discovery queried `entityMetricEvents` — a sparse, batchy
   *legacy* feed (~730K rows/day). The complete CDC stream is
   `entityMetricEvents_month` (~6.2M rows/day). Both have run in parallel for
   6+ days; the legacy table was never the right source (not a recent migration).

2. **Tight non-overlapping window.** The poller used a 2-second tumbling window
   (`createdAt > since AND createdAt <= upper`, `upper = now - 60s`, non-overlapping,
   cursor advances every cycle even on empty result). ClickHouse ingestion is
   **bursty** — events flush in batches; between flushes a `createdAt > now-10s`
   query returns 0 rows, then a batch lands rows whose `createdAt` is already in
   the past. Any event that flushes *after* its 2s window slid past is **missed
   permanently** (cursor already advanced). The 60s safety lag was far short of
   real batch-lag, which spikes to **minutes** on watcher restarts / consumer-group
   rebalances (there was a rebalance storm on 07-02).

Note: discovery does **not** filter by `metricType`, so the ReactionLike→Like
vocab rename (v1.1.15) is *not* the discovery-zero cause. Vocab only matters on
the totals side, which reads `entityMetricDailyAgg_v2` (already bare-named).

## Design principles

- **Everything config-driven.** Table, entity type, poll cadence, window widths,
  and safety lag are TOML + env knobs — no rebuild for a table move or a tuning
  change. The next table migration is a one-line config edit + restart.
- **Overlap on the read side.** Never trust a tight non-overlapping window against
  a batch-ingested table.
- **Overlap on the settle side.** Additive totals (tips/comments) settle ~2.5min
  after an event in `entityMetricDailyAgg_v2`; toggle/reactions ~1min. The window
  must be wide enough that an id is re-read across the settle interval.

## Window algorithm

Two modes, chosen per cycle by how far behind the cursor is:

```
now = wall_clock_secs()
if now - cursor > reconcile_window:
    # BACKFILL — cursor far behind (fresh boot w/ old BITDEX_METRICS_SINCE,
    # or long downtime). Historical events_month partitions are immutable, so
    # forward tumbling chunks are safe — no jitter to guard against.
    since = cursor
    upper = min(cursor + backfill_chunk, now)
else:
    # STEADY — caught up. Overlapping trailing window absorbs batch-flush jitter
    # and the settle interval (reconcile_window >> batch-lag and >> settle-lag).
    since = now - reconcile_window
    upper = now
# ... run query [since, upper], emit changed ids ...
cursor = upper   # persisted for observability + restart catch-up
```

Backfill runs until `now - cursor <= reconcile_window`, then hands off to steady.
**Seam:** at handoff the last backfill `upper == cursor >= now - reconcile_window`,
and the first steady window starts at `now - reconcile_window <= cursor`, so the
two overlap — no gap (per Mark).

Defaults: `reconcile_window = 1200s` (20 min), `backfill_chunk = 3600s` (1 h),
`poll_interval = 30s`.

## Query (server-side subquery form — kept)

```sql
SELECT entityId AS id,
       sumIf(total, metricType IN ('Like','Heart','Laugh','Cry')) AS reactionCount,
       sumIf(total, metricType = 'commentCount')                  AS commentCount,
       sumIf(total, metricType = 'Collection')                    AS collectedCount
FROM entityMetricDailyAgg_v2
WHERE entityType = '{entity_type}'
  AND entityId IN (
    SELECT DISTINCT entityId
    FROM {table}                              -- entityMetricEvents_month
    WHERE entityType = '{entity_type}'
      AND createdAt >  fromUnixTimestamp({since})
      AND createdAt <= fromUnixTimestamp({upper})
  )
GROUP BY entityId
FORMAT JSONEachRow
```

- Drop `entityId IS NOT NULL` — `_month.entityId` is non-nullable.
- No `FINAL` — id-discovery dedupes anyway; totals view's `argMax` handles versions.
- CH keeps discovery + totals on one snapshot; no multi-KB literal id list.

**Prod measurement (Mark, live):** 20-min window = ~8,900 distinct Image ids;
full reconcile ~33 ms / ~156 MiB / ~1.5M rows read. Cost ~linear in window width,
sub-linear in id count. A single wide reconcile every 30s is free — no hot/slow split.

## Suppression (avoids re-emitting ~9k×3 ops every 30s)

Per-id last-sent `(reaction, comment, collected)` tuple in a bounded LRU. Emit
`Op::Set` ops only for ids whose tuple changed since last send. Re-evaluated every
cycle the id is in-window — so a not-yet-settled value is *not* silenced: the id
stays in the 20-min window across the 2.5-min settle, gets re-read, and emits once
the settled value differs. (Confirmed correct by Mark.)

## Config schema (PgSyncConfig / sync.toml)

```toml
metrics_poll_interval_secs   = 30      # reconcile cadence
metrics_table                = "entityMetricEvents_month"
metrics_entity_type          = "Image"
metrics_reconcile_window_secs = 1200   # steady trailing window (>> batch-lag, >> settle)
metrics_backfill_chunk_secs  = 3600    # bounded forward chunk when catching up
```

Env overrides (pod-tunable): `BITDEX_METRICS_TABLE`, `BITDEX_METRICS_RECONCILE_WINDOW_SECS`,
`BITDEX_METRICS_BACKFILL_CHUNK_SECS`. `BITDEX_METRICS_SINCE` (existing) seeds the
cold-start cursor to force a wide backfill.

## Deploy / backfill (Josh)

1. Image-bump PR (repoint + window fix), merge on green.
2. Temp env `BITDEX_METRICS_SINCE ≈ 1782864000` (07-01 00:00Z floor) on the pg-sync
   container in that PR — wide catch-up anchor (totals are all-time argMax, so the
   backfill self-heals every touched image; wide is free).
3. At rollout: `DELETE /cursors/metrics-poller-civitai` so cold start seeds from
   the env and backfill mode drains the gap in 1-h chunks, then settles to steady.
4. Verify one pass heals a sample slot vs CH; follow-up PR removes the temp env.
