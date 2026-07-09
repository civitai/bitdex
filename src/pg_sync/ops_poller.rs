//! V2 ops poller: reads from BitdexOps table, deduplicates, and POSTs to BitDex /ops endpoint.
//!
//! Replaces the V1 outbox_poller by reading self-contained ops (with old+new values)
//! instead of entity IDs that require enrichment queries.
//!
//! Poll loop:
//!   1. On boot: read cursor from PG bitdex_cursors table
//!   2. SELECT from BitdexOps WHERE id > cursor ORDER BY id ASC LIMIT N
//!   3. Deserialize JSONB ops arrays
//!   4. Dedup via shared dedup_ops()
//!   5. POST batch to BitDex /ops endpoint with sync metadata
//!   6. Advance cursor in PG
//!   7. Report max_outbox_id for lag calculation

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::time::interval;

use super::bitdex_client::BitdexClient;
use super::op_dedup::dedup_ops;
use super::ops::{EntityOps, Op, OpsBatch, SyncMeta};

/// How long a gap may stay unresolved before the poller starts alerting.
/// A held gap is NEVER skipped on time — a transaction slower than any
/// timeout is exactly the row this machinery exists to protect. The only way
/// an id is declared dead is the atomic finished-and-still-invisible check
/// (`gap_status`), which is definitionally a rollback.
const GAP_ALERT_AFTER: Duration = Duration::from_secs(60);

/// Upper bound on tracked missing ids. A hole this large is not a rolled-back
/// transaction — it's a trimmed table or an operator cursor reset. Hold the
/// cursor and alert instead of building a multi-million-entry set.
const GAP_SET_LIMIT: usize = 100_000;

/// Row from BitdexOps table.
#[derive(Debug, sqlx::FromRow)]
struct OpsRow {
    id: i64,
    entity_id: i64,
    ops: sqlx::types::Json<Vec<Op>>,
}

/// Sequence-allocated ids above the cursor that are not yet visible.
///
/// BIGSERIAL ids are handed out at INSERT time but rows only become visible at
/// COMMIT, so a long transaction's row can commit AFTER a later-id row from a
/// quick transaction. Advancing the cursor to the max VISIBLE id silently
/// skips the still-invisible row forever, and the bitdex_cursors cleanup
/// trigger then deletes it (repro: tests/integration/skip-race-repro.sh; prod
/// specimen: post 29674681's publish fan-out, 2026-07-09). The durable cursor
/// must hold below a gap until it fills in (commit) or is PROVEN a rollback.
struct GapInfo {
    /// Every id in (cursor, max seen] that was allocated but not visible when
    /// last checked. One rolled-back transaction can hold many.
    missing: BTreeSet<i64>,
    /// Backend txids in flight when the missing set was (last) recorded. Each
    /// missing id's inserting transaction is either among them or has since
    /// committed (in which case the id turns visible and leaves the set) —
    /// so once ALL of them finished AND an id is still invisible, that insert
    /// rolled back. Both facts are checked in ONE statement (one snapshot) in
    /// `gap_status`; checking them separately re-opens the race this fixes.
    xips: Vec<i64>,
    seen_at: Instant,
    last_alert: Instant,
}

/// Poller cursor state. The durable cursor never passes an unresolved gap;
/// rows beyond a gap still POST immediately (tracked per-id in `posted_ids`)
/// and may re-POST after a restart — the same at-least-once semantics as
/// crash recovery, absorbed by the WAL reader's LIFO dedup.
struct PollerState {
    cursor: i64,
    /// Ids POSTed but not yet passed by the durable cursor. A plain
    /// high-water mark cannot represent "posted" here: a gap row that fills
    /// in by commit appears BELOW the mark and would be silently dropped
    /// (review 2026-07-09 finding 1). Pruned as the cursor advances, so it
    /// stays bounded by the held window.
    posted_ids: BTreeSet<i64>,
    gap: Option<GapInfo>,
    /// Ids proven rolled-back; treated as consumed when walking the
    /// frontier. Pruned once the cursor passes them.
    dead_ids: BTreeSet<i64>,
    /// Boot-seed guard (re-review N1): a cursor==0 boot seeded to MIN(id)-1
    /// while transactions were in flight. One of them may hold an
    /// allocated-but-invisible id BELOW the seed (sequence head near MIN
    /// after trim), which we cannot enumerate. Until every boot-time txn
    /// finishes, the DURABLE cursor stays at 0 — the cleanup trigger's
    /// threshold is MIN(cursors), so nothing below the seed can be deleted —
    /// while `state.cursor` serves as the in-memory read anchor and advances
    /// normally. On release: one-shot sweep of `id <= seed` for late
    /// commits, POST them, then persist the anchor. A crash during the guard
    /// re-boots from durable 0 and re-derives everything — at-least-once.
    boot_guard: Option<BootGuard>,
    /// Rate limit for the oversized-hole alert (re-review N3a).
    last_oversized_alert: Option<Instant>,
}

struct BootGuard {
    seed: i64,
    xips: Vec<i64>,
}

/// Frontier walk result.
struct Frontier {
    /// Highest id the durable cursor may advance to without passing an
    /// unexplained hole.
    safe_id: i64,
    /// Every unexplained hole in (cursor, max id], capped at GAP_SET_LIMIT.
    missing: Vec<i64>,
    /// Hole exceeded GAP_SET_LIMIT — trimmed table / operator reset, not a
    /// transaction. Hold and alert; never enumerate or skip.
    oversized: bool,
}

/// Walk ids (sorted ascending, all > cursor) from `cursor + 1`. `dead_ids`
/// are ids proven rolled back — they count as present.
fn compute_frontier(cursor: i64, ids: &[i64], dead_ids: &BTreeSet<i64>) -> Frontier {
    let mut missing = Vec::new();
    let mut safe = None;
    let mut expected = cursor + 1;
    for &id in ids {
        // Bail before enumerating a pathological hole one id at a time.
        if (id - expected) as usize > GAP_SET_LIMIT + dead_ids.len() {
            return Frontier {
                safe_id: safe.unwrap_or(expected - 1),
                missing,
                oversized: true,
            };
        }
        while expected < id {
            if !dead_ids.contains(&expected) {
                if safe.is_none() {
                    safe = Some(expected - 1);
                }
                if missing.len() >= GAP_SET_LIMIT {
                    return Frontier { safe_id: safe.unwrap_or(cursor), missing, oversized: true };
                }
                missing.push(expected);
            }
            expected += 1;
        }
        expected = id + 1;
    }
    Frontier { safe_id: safe.unwrap_or(expected - 1), missing, oversized: false }
}

/// Run the V2 ops poller loop. Runs forever until cancelled.
pub async fn run_ops_poller(
    pool: &PgPool,
    client: &BitdexClient,
    poll_interval: Duration,
    batch_limit: i64,
    cursor_name: &str,
    replica_id: Option<&str>,
) -> Result<(), String> {
    // Wait for BitDex health
    eprintln!("Ops poller waiting for BitDex to be healthy...");
    loop {
        if client.is_healthy().await {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    eprintln!("BitDex is healthy.");

    // Read initial cursor from PG
    let durable: i64 = read_cursor_from_pg(pool, cursor_name)
        .await
        .unwrap_or(0);
    let mut state = init_state(pool, cursor_name, durable).await;
    eprintln!(
        "Ops poller started (interval={}ms, batch_limit={}, cursor_name={}, starting_cursor={})",
        poll_interval.as_millis(), batch_limit, cursor_name, state.cursor
    );

    let mut ticker = interval(poll_interval);
    let mut bitdex_was_down = false;

    loop {
        ticker.tick().await;

        // Health gate
        if !client.is_healthy().await {
            if !bitdex_was_down {
                eprintln!("Ops poller: BitDex unreachable, pausing");
                bitdex_was_down = true;
            }
            continue;
        }
        if bitdex_was_down {
            eprintln!("Ops poller: BitDex is back, resuming");
            bitdex_was_down = false;
        }

        let cycle_start = std::time::Instant::now();
        match poll_and_process(pool, client, batch_limit, cursor_name, &mut state, replica_id).await {
            Ok(processed) => {
                let cycle_secs = cycle_start.elapsed().as_secs_f64();
                if processed > 0 {
                    eprintln!(
                        "Ops poller: processed {processed} ops (cursor={}, cycle={cycle_secs:.3}s)",
                        state.cursor
                    );
                }
            }
            Err(e) => {
                eprintln!("Ops poller error: {e}");
            }
        }
    }
}

/// Build the initial poller state from the durable cursor (testable boot
/// seed — re-review N5).
///
/// A durable cursor of 0 against a cleanup-trimmed table means the hole
/// between 0 and MIN(id) is mostly trim — seed the read anchor to MIN(id)-1
/// instead of walking it. But "mostly" is not "entirely": a transaction in
/// flight RIGHT NOW can hold an allocated-but-invisible id below MIN when the
/// sequence head sits near MIN (re-review N1). Those ids cannot be
/// enumerated (invisible), so when any transactions are in flight the seed
/// carries a BootGuard: the durable cursor stays at 0 (freezing the cleanup
/// threshold) until they all finish, then a one-shot sweep below the seed
/// picks up late commits.
async fn init_state(pool: &PgPool, cursor_name: &str, durable: i64) -> PollerState {
    let mut state = PollerState {
        cursor: durable,
        posted_ids: BTreeSet::new(),
        gap: None,
        dead_ids: BTreeSet::new(),
        boot_guard: None,
        last_oversized_alert: None,
    };
    if durable != 0 {
        return state;
    }
    let Ok(min_id) = get_min_ops_id(pool).await else {
        return state;
    };
    if min_id <= 1 {
        return state;
    }
    let seed = min_id - 1;
    match snapshot_xips(pool).await {
        Ok(xips) if xips.is_empty() => {
            // No transaction can hold an invisible id — the hole is pure trim.
            eprintln!("Ops poller: cursor at 0, seeding to MIN(id)-1 = {seed} (no txns in flight)");
            state.cursor = seed;
        }
        Ok(xips) => {
            // The pin only freezes cleanup if OUR cursor row exists — a fresh
            // replica has none, and MIN(last_outbox_id) ignores absent rows.
            if let Err(e) = super::queries::upsert_cursor(pool, cursor_name, 0).await {
                eprintln!(
                    "Ops poller: failed to pin cursor row at 0 ({e}); not seeding \
                     (walking from 0 instead)"
                );
                return state;
            }
            eprintln!(
                "Ops poller: cursor at 0, seeding read anchor to {seed} with boot guard — \
                 {} txn(s) in flight may hold ids below the seed; durable cursor pinned at 0 \
                 until they finish",
                xips.len()
            );
            state.cursor = seed;
            state.boot_guard = Some(BootGuard { seed, xips });
        }
        Err(e) => {
            // Can't prove safety — walk from 0 (oversized guard will hold).
            eprintln!("Ops poller: snapshot_xips failed at boot seed ({e}); not seeding");
        }
    }
    state
}

/// Single poll + process cycle.
async fn poll_and_process(
    pool: &PgPool,
    client: &BitdexClient,
    batch_limit: i64,
    cursor_name: &str,
    state: &mut PollerState,
    replica_id: Option<&str>,
) -> Result<usize, String> {
    // ── Boot-guard resolution (re-review N1) ──
    // While active, the durable cursor is pinned at 0 (see advance_cursor_
    // safe), freezing the cleanup threshold so nothing below the seed can be
    // deleted. Once every boot-time txn has finished, any of their commits
    // below the seed are now visible: rewind the read anchor beneath the
    // lowest one and let the normal path deliver them.
    if let Some(bg) = &state.boot_guard {
        match gap_status(pool, &bg.xips, &BTreeSet::new()).await {
            Ok((true, _)) => {
                let seed = bg.seed;
                if let Ok(min_id) = get_min_ops_id(pool).await {
                    if min_id <= seed {
                        eprintln!(
                            "Ops poller: boot guard released — late commit(s) below seed \
                             {seed} detected, rewinding read anchor to {}",
                            min_id - 1
                        );
                        state.cursor = state.cursor.min(min_id - 1);
                        // Re-reads may re-POST rows pruned from posted_ids —
                        // at-least-once, absorbed downstream.
                        state.posted_ids.clear();
                    } else {
                        eprintln!(
                            "Ops poller: boot guard released — no late commits below seed {seed}"
                        );
                    }
                    state.boot_guard = None;
                }
                // On get_min_ops_id error: keep the guard, retry next cycle.
            }
            Ok((false, _)) => {} // boot-time txns still running — hold
            Err(e) => eprintln!("Ops poller: boot-guard check failed ({e}), holding"),
        }
    }

    // Fetch ops after the read anchor. While a gap is pending this re-reads
    // rows already POSTed — required, because the gap can only fill in behind
    // them (the newly visible row must then POST too).
    let rows = poll_ops_from_cursor(pool, state.cursor, batch_limit)
        .await
        .map_err(|e| format!("poll_ops: {e}"))?;

    if rows.is_empty() && state.gap.is_none() {
        return Ok(0);
    }

    // ── Gap resolution BEFORE advancing anything ──
    // An id leaves the missing set only by turning visible (commit — it then
    // POSTs like any row) or by the atomic finished-and-still-invisible check
    // (rollback — it joins dead_ids). There is NO time-based skip: a slower-
    // than-timeout transaction is exactly the row this protects.
    if let Some(g) = &mut state.gap {
        match gap_status(pool, &g.xips, &g.missing).await {
            Ok((all_finished, still_invisible)) => {
                g.missing = still_invisible; // visible ids re-enter via `rows`
                if g.missing.is_empty() {
                    state.gap = None;
                } else if all_finished {
                    // Every txn that could own these inserts finished, and the
                    // ids are STILL invisible (same snapshot) — rolled back.
                    eprintln!(
                        "Ops poller: {} missing id(s) proven rolled back (first: {:?})",
                        g.missing.len(),
                        g.missing.iter().next()
                    );
                    state.dead_ids.append(&mut g.missing);
                    state.gap = None;
                } else if g.seen_at.elapsed() > GAP_ALERT_AFTER
                    && g.last_alert.elapsed() > GAP_ALERT_AFTER
                {
                    g.last_alert = Instant::now();
                    eprintln!(
                        "Ops poller: ALERT — cursor held {}s behind {} unresolved id(s) \
                         (first: {:?}); long-running writer txn still open. New-op \
                         delivery stalls once the backlog exceeds batch_limit; remedy is \
                         pg_terminate_backend on the writer, NEVER a manual cursor bump",
                        g.seen_at.elapsed().as_secs(),
                        g.missing.len(),
                        g.missing.iter().next()
                    );
                }
            }
            Err(e) => {
                // Err on the held side: keep the gap, retry next cycle.
                eprintln!("Ops poller: gap_status failed ({e}), holding");
            }
        }
    }

    // ── Frontier walk + new-hole accounting ──
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let frontier = compute_frontier(state.cursor, &ids, &state.dead_ids);
    let safe_id = frontier.safe_id;
    if frontier.oversized
        && state
            .last_oversized_alert
            .map_or(true, |t| t.elapsed() > GAP_ALERT_AFTER)
    {
        state.last_oversized_alert = Some(Instant::now());
        eprintln!(
            "Ops poller: ALERT — hole above id {safe_id} exceeds {GAP_SET_LIMIT} ids; \
             holding cursor (trimmed table or cursor reset?). Recovery: verify the hole \
             is a rollback/trim, then manually advance the cursor past it."
        );
    }
    let known: BTreeSet<i64> = state.gap.as_ref().map(|g| g.missing.clone()).unwrap_or_default();
    let new_holes: Vec<i64> = frontier
        .missing
        .iter()
        .copied()
        .filter(|m| !known.contains(m))
        .collect();
    if !new_holes.is_empty() && !frontier.oversized {
        // Snapshot the in-flight txns for the new holes. On error do NOT
        // record them — an empty/partial xip list could later prove a live
        // transaction "finished". The cursor still holds (safe_id is
        // independent); recording retries next cycle.
        match snapshot_xips(pool).await {
            Ok(xips) => {
                eprintln!(
                    "Ops poller: holding cursor at {safe_id} — {} id(s) allocated but not \
                     yet visible (first: {}, {} txns in flight)",
                    new_holes.len(),
                    new_holes[0],
                    xips.len()
                );
                match &mut state.gap {
                    Some(g) => {
                        g.missing.extend(new_holes);
                        // Union: resolution must now wait for the freshest
                        // snapshot too — conservative, never premature.
                        for x in xips {
                            if !g.xips.contains(&x) {
                                g.xips.push(x);
                            }
                        }
                    }
                    None => {
                        state.gap = Some(GapInfo {
                            missing: new_holes.into_iter().collect(),
                            xips,
                            seen_at: Instant::now(),
                            last_alert: Instant::now(),
                        });
                    }
                }
            }
            Err(e) => eprintln!("Ops poller: snapshot_xips failed ({e}), holding"),
        }
    }
    state.dead_ids = state.dead_ids.split_off(&(state.cursor + 1));
    state.posted_ids = state.posted_ids.split_off(&(state.cursor + 1));

    // POST rows not already sent this lifetime — per-id set membership, NOT a
    // high-water mark: a gap row that fills in by commit sits below the mark.
    let rows: Vec<OpsRow> =
        rows.into_iter().filter(|r| !state.posted_ids.contains(&r.id)).collect();
    if rows.is_empty() {
        // Nothing new to send; the durable cursor may still advance (e.g. a
        // hole was proven rolled back behind already-POSTed rows).
        advance_cursor_safe(pool, cursor_name, safe_id, state).await?;
        return Ok(0);
    }

    let max_id = rows.iter().map(|r| r.id).max().unwrap_or(state.cursor);
    let sent_ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let total_rows = rows.len();

    // Convert to EntityOps
    let mut batch: Vec<EntityOps> = rows
        .into_iter()
        .map(|row| {
            // Check if trigger emitted an Alive op — signals this is an INSERT
            // on a sets_alive table. Remove the Alive op from the ops list
            // (it's a signal, not an actual bitmap mutation).
            let has_alive = row.ops.0.iter().any(|op| matches!(op, Op::Alive));
            let ops: Vec<Op> = if has_alive {
                row.ops.0.into_iter().filter(|op| !matches!(op, Op::Alive)).collect()
            } else {
                row.ops.0
            };
            EntityOps {
                entity_id: row.entity_id,
                ops,
                creates_slot: has_alive,
            }
        })
        .collect();

    // Dedup
    dedup_ops(&mut batch);

    if batch.is_empty() {
        // All ops cancelled out — mark consumed and advance the durable
        // cursor (to the safe frontier only, never past a gap).
        state.posted_ids.extend(sent_ids);
        advance_cursor_safe(pool, cursor_name, safe_id, state).await?;
        return Ok(total_rows);
    }

    // Get max ops ID for lag calculation
    let max_ops_id = get_max_ops_id(pool).await.unwrap_or(max_id);

    // Build batch with metadata
    let ops_batch = OpsBatch {
        ops: batch,
        meta: Some(SyncMeta {
            source: replica_id.unwrap_or("default").to_string(),
            cursor: Some(safe_id),
            max_id: Some(max_ops_id),
            lag_rows: Some(max_ops_id - safe_id),
        }),
    };

    // POST to BitDex. On failure nothing is marked posted — the whole window
    // re-reads next cycle (at-least-once, absorbed by WAL LIFO dedup).
    client
        .post_ops(&ops_batch)
        .await
        .map_err(|e| format!("post_ops: {e}"))?;

    state.posted_ids.extend(sent_ids);
    advance_cursor_safe(pool, cursor_name, safe_id, state).await?;

    Ok(total_rows)
}

/// Advance the read anchor to `safe_id` (never past an unresolved gap, never
/// backwards) and persist it as the durable cursor. The bitdex_cursors
/// upsert also fires the cleanup trigger, so a cursor that outran an
/// invisible row would let cleanup delete it — this is the single choke
/// point that prevents that. While a boot guard is active the durable cursor
/// stays pinned at 0 (only the in-memory anchor moves): persisting anything
/// higher would unfreeze cleanup below the seed (re-review N1).
async fn advance_cursor_safe(
    pool: &PgPool,
    cursor_name: &str,
    safe_id: i64,
    state: &mut PollerState,
) -> Result<(), String> {
    if safe_id <= state.cursor {
        return Ok(());
    }
    if state.boot_guard.is_none() {
        super::queries::upsert_cursor(pool, cursor_name, safe_id)
            .await
            .map_err(|e| format!("upsert_cursor: {e}"))?;
    }
    state.cursor = safe_id;
    Ok(())
}

/// Backend txids currently in flight. `pg_snapshot_xip` is set-returning —
/// one row per in-flight txid (empty when the system is idle).
async fn snapshot_xips(pool: &PgPool) -> Result<Vec<i64>, sqlx::Error> {
    let rows: Vec<(i64,)> = sqlx::query_as(
        r#"SELECT pg_snapshot_xip(pg_current_snapshot())::text::bigint"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
}

/// Atomic gap check: in ONE statement (= one READ COMMITTED snapshot) report
/// (a) whether every recorded in-flight txid has finished (none appears in
/// the current xip list — a txid leaves it on commit/abort, never re-enters)
/// and (b) which of the missing ids are STILL invisible. Both must come from
/// the same snapshot: checked separately, a transaction committing between
/// the two reads gets its id declared dead while its row sits committed on
/// disk (review 2026-07-09 finding 2 — the original bug, reintroduced).
async fn gap_status(
    pool: &PgPool,
    xips: &[i64],
    missing: &BTreeSet<i64>,
) -> Result<(bool, BTreeSet<i64>), sqlx::Error> {
    let missing_vec: Vec<i64> = missing.iter().copied().collect();
    let row: (bool, Vec<i64>) = sqlx::query_as(
        r#"SELECT
             NOT EXISTS (
               SELECT 1
               FROM (SELECT pg_snapshot_xip(pg_current_snapshot())::text::bigint AS x) cur
               WHERE cur.x = ANY($1::bigint[])),
             ARRAY(SELECT m FROM unnest($2::bigint[]) m
                   WHERE NOT EXISTS (SELECT 1 FROM "BitdexOps" b WHERE b.id = m)
                   ORDER BY m)"#,
    )
    .bind(xips)
    .bind(&missing_vec)
    .fetch_one(pool)
    .await?;
    Ok((row.0, row.1.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dead(ids: &[i64]) -> BTreeSet<i64> {
        ids.iter().copied().collect()
    }

    fn frontier(cursor: i64, ids: &[i64], dead_ids: &[i64]) -> (i64, Vec<i64>, bool) {
        let f = compute_frontier(cursor, ids, &dead(dead_ids));
        (f.safe_id, f.missing, f.oversized)
    }

    #[test]
    fn contiguous_ids_advance_to_max() {
        assert_eq!(frontier(5, &[6, 7, 8], &[]), (8, vec![], false));
    }

    #[test]
    fn empty_batch_holds_cursor() {
        assert_eq!(frontier(5, &[], &[]), (5, vec![], false));
    }

    /// The skip-race shape: a long transaction holds id 6 uncommitted while
    /// id 7 from a quick transaction is visible. The cursor must hold at 5 —
    /// the old `cursor = max_id` advance to 7 is exactly the prod data loss
    /// (specimen: post 29674681's publish fan-out).
    #[test]
    fn gap_at_frontier_holds_cursor() {
        assert_eq!(frontier(5, &[7], &[]), (5, vec![6], false));
    }

    #[test]
    fn gap_mid_batch_advances_to_gap_edge_and_collects_all_holes() {
        assert_eq!(frontier(5, &[6, 7, 9, 10, 13], &[]), (7, vec![8, 11, 12], false));
    }

    #[test]
    fn dead_id_bridges_gap() {
        assert_eq!(frontier(5, &[7], &[6]), (7, vec![], false));
    }

    #[test]
    fn dead_id_bridges_to_next_gap() {
        assert_eq!(frontier(5, &[7, 10], &[6]), (7, vec![8, 9], false));
    }

    /// A whole rolled-back multi-row transaction resolves in ONE declaration
    /// — the missing set carries every hole, not just the first (review
    /// 2026-07-09 finding 5).
    #[test]
    fn multi_id_hole_collected_at_once() {
        assert_eq!(frontier(5, &[9], &[]), (5, vec![6, 7, 8], false));
        assert_eq!(frontier(5, &[9], &[6, 7, 8]), (9, vec![], false));
    }

    #[test]
    fn gap_immediately_after_cursor_with_multiple_rows() {
        assert_eq!(frontier(10, &[12, 13], &[]), (10, vec![11], false));
    }

    /// Trimmed-table / cursor-reset hole: don't enumerate millions of ids —
    /// flag oversized and hold.
    #[test]
    fn pathological_hole_flags_oversized() {
        let f = compute_frontier(0, &[10_000_000], &BTreeSet::new());
        assert!(f.oversized);
        assert_eq!(f.safe_id, 0);
        assert!(f.missing.len() <= GAP_SET_LIMIT);
    }

    /// Review finding 1 regression (state-machine level): after a gap fills
    /// in by COMMIT, the newly visible row must not be filtered as already
    /// posted. posted_ids is a set of ids actually sent — the gap row (6) is
    /// absent from it even though 7 (a higher id) was sent earlier.
    #[test]
    fn commit_filled_gap_row_is_not_marked_posted() {
        let mut posted = BTreeSet::new();
        // Cycle 1: gap at 6, row 7 visible and POSTed.
        posted.insert(7_i64);
        // Cycle 2: txn committed, SELECT returns [6, 7].
        let visible = [6_i64, 7];
        let to_post: Vec<i64> =
            visible.iter().copied().filter(|id| !posted.contains(id)).collect();
        assert_eq!(to_post, vec![6], "gap row must POST after commit fill-in");
        let f = compute_frontier(5, &visible, &BTreeSet::new());
        assert_eq!((f.safe_id, f.missing), (7, vec![]));
    }
}

/// Integration tests exercising the REAL `poll_and_process` against a live
/// PostgreSQL and a mock engine endpoint. These are the merge gate for the
/// skip-race fix — the SQL simulation scripts demonstrate semantics only
/// (review 2026-07-09 finding 6).
///
/// Run: tests/integration/run-skip-race-rust.sh (starts a throwaway PG 16
/// container, sets SKIP_RACE_PG_URL, runs `cargo test ... -- --ignored`).
#[cfg(test)]
mod pg_integration_tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::sync::{Arc, Mutex};

    /// Minimal HTTP responder standing in for the BitDex /ops endpoint:
    /// answers 200 to everything, records request bodies.
    fn mock_engine() -> (u16, Arc<Mutex<Vec<String>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let captured = bodies.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { continue };
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                // Read headers, then exactly Content-Length body bytes.
                let body = loop {
                    match s.read(&mut tmp) {
                        Ok(0) => break String::new(),
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if let Some(pos) =
                                buf.windows(4).position(|w| w == b"\r\n\r\n")
                            {
                                let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                                let clen = head
                                    .lines()
                                    .find_map(|l| {
                                        let (k, v) = l.split_once(':')?;
                                        k.eq_ignore_ascii_case("content-length")
                                            .then(|| v.trim().parse::<usize>().ok())?
                                    })
                                    .unwrap_or(0);
                                let mut body = buf[pos + 4..].to_vec();
                                while body.len() < clen {
                                    match s.read(&mut tmp) {
                                        Ok(0) => break,
                                        Ok(n) => body.extend_from_slice(&tmp[..n]),
                                        Err(_) => break,
                                    }
                                }
                                break String::from_utf8_lossy(&body).to_string();
                            }
                        }
                        Err(_) => break String::new(),
                    }
                };
                if !body.is_empty() {
                    captured.lock().unwrap().push(body);
                }
                let _ = s.write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}",
                );
            }
        });
        (port, bodies)
    }

    async fn setup(pool: &PgPool) {
        sqlx::raw_sql(
            r#"DROP TABLE IF EXISTS "BitdexOps" CASCADE;
               DROP TABLE IF EXISTS bitdex_cursors CASCADE;"#,
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::raw_sql(super::super::queries::SETUP_V2_SQL)
            .execute(pool)
            .await
            .unwrap();
    }

    fn fresh_state() -> PollerState {
        PollerState {
            cursor: 0,
            posted_ids: BTreeSet::new(),
            gap: None,
            dead_ids: BTreeSet::new(),
            boot_guard: None,
            last_oversized_alert: None,
        }
    }

    async fn insert_op(pool: &PgPool, entity: i64) {
        sqlx::query(
            r#"INSERT INTO "BitdexOps" (entity_id, ops)
               VALUES ($1, '[{"op":"set","field":"publishedAt","value":1}]'::jsonb)"#,
        )
        .bind(entity)
        .execute(pool)
        .await
        .unwrap();
    }

    fn posted_entities(bodies: &Arc<Mutex<Vec<String>>>) -> Vec<i64> {
        bodies
            .lock()
            .unwrap()
            .iter()
            .flat_map(|b| {
                serde_json::from_str::<serde_json::Value>(b)
                    .ok()
                    .and_then(|v| {
                        v["ops"].as_array().map(|ops| {
                            ops.iter()
                                .filter_map(|e| e["entity_id"].as_i64())
                                .collect::<Vec<_>>()
                        })
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    async fn durable_cursor(pool: &PgPool) -> i64 {
        read_cursor_from_pg(pool, "itest").await.unwrap_or(0)
    }

    /// The prod bug end-to-end: long txn's row (id 1) invisible while id 2 is
    /// consumed; cursor must HOLD; after commit the gap row must be POSTed
    /// and only then may the cursor pass it.
    #[tokio::test]
    #[ignore = "needs live PG via SKIP_RACE_PG_URL (tests/integration/run-skip-race-rust.sh)"]
    async fn gap_filled_by_commit_delivers_gap_row() {
        let url = std::env::var("SKIP_RACE_PG_URL").unwrap();
        let pool = PgPool::connect(&url).await.unwrap();
        setup(&pool).await;
        let (port, bodies) = mock_engine();
        let client = BitdexClient::new(&format!("http://127.0.0.1:{port}/api/indexes/t"));
        let mut state = fresh_state();

        // Long publish txn: INSERT id 1, hold open.
        let mut txn = pool.begin().await.unwrap();
        sqlx::query(
            r#"INSERT INTO "BitdexOps" (entity_id, ops)
               VALUES (29674681, '[{"op":"set","field":"publishedAt","value":1}]'::jsonb)"#,
        )
        .execute(&mut *txn)
        .await
        .unwrap();
        // Quick op: id 2, committed.
        insert_op(&pool, 555).await;

        // Cycle 1: only id 2 visible → POST 555, HOLD cursor at 0.
        poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
        assert_eq!(posted_entities(&bodies), vec![555]);
        assert_eq!(state.cursor, 0, "durable cursor must hold below the gap");
        assert_eq!(durable_cursor(&pool).await, 0);

        // Cycles while txn still open: no time-based skip, still held.
        poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
        assert_eq!(state.cursor, 0);
        assert_eq!(posted_entities(&bodies), vec![555], "no duplicate POST while held");

        // Publish txn commits — gap fills in.
        txn.commit().await.unwrap();
        poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
        assert_eq!(
            posted_entities(&bodies),
            vec![555, 29674681],
            "gap row must POST after commit (review finding 1)"
        );
        assert_eq!(state.cursor, 2, "cursor passes the gap only after delivery");
        assert_eq!(durable_cursor(&pool).await, 2);
    }

    /// A rolled-back insert must be proven dead (atomic finished+invisible
    /// check) and skipped — without ever being POSTed.
    #[tokio::test]
    #[ignore = "needs live PG via SKIP_RACE_PG_URL (tests/integration/run-skip-race-rust.sh)"]
    async fn gap_from_rollback_is_proven_dead_and_skipped() {
        let url = std::env::var("SKIP_RACE_PG_URL").unwrap();
        let pool = PgPool::connect(&url).await.unwrap();
        setup(&pool).await;
        let (port, bodies) = mock_engine();
        let client = BitdexClient::new(&format!("http://127.0.0.1:{port}/api/indexes/t"));
        let mut state = fresh_state();

        let mut txn = pool.begin().await.unwrap();
        sqlx::query(r#"INSERT INTO "BitdexOps" (entity_id, ops) VALUES (111, '[]'::jsonb)"#)
            .execute(&mut *txn)
            .await
            .unwrap();
        insert_op(&pool, 555).await;

        // Cycle 1: gap recorded, cursor held.
        poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
        assert_eq!(state.cursor, 0);

        // The long txn ROLLS BACK — id 1 will never appear.
        txn.rollback().await.unwrap();

        // Within a few cycles the atomic check proves the rollback and the
        // cursor passes the dead id. (>1 cycle allowed: other concurrent
        // txns on the test server can keep the snapshot busy briefly.)
        let mut passed = false;
        for _ in 0..20 {
            poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
            if state.cursor >= 2 {
                passed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(passed, "cursor must pass a proven-rollback id");
        assert_eq!(posted_entities(&bodies), vec![555], "dead id must never POST");
    }

    /// Fresh replica against a trimmed table: boot seed jumps the cursor to
    /// MIN(id)-1 instead of walking a giant hole — exercising the REAL
    /// `init_state` (review finding 5 + re-review N5).
    #[tokio::test]
    #[ignore = "needs live PG via SKIP_RACE_PG_URL (tests/integration/run-skip-race-rust.sh)"]
    async fn trimmed_table_boot_seed() {
        let url = std::env::var("SKIP_RACE_PG_URL").unwrap();
        let pool = PgPool::connect(&url).await.unwrap();
        setup(&pool).await;
        sqlx::raw_sql(r#"SELECT setval('"BitdexOps_id_seq"', 5000000)"#)
            .execute(&pool)
            .await
            .unwrap();
        insert_op(&pool, 777).await; // id 5000001
        let (port, bodies) = mock_engine();
        let client = BitdexClient::new(&format!("http://127.0.0.1:{port}/api/indexes/t"));
        let mut state = init_state(&pool, "itest", 0).await;
        assert_eq!(state.cursor, 5_000_000, "seed to MIN(id)-1");
        assert!(state.boot_guard.is_none(), "idle test PG: no in-flight txns, no guard");
        poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
        assert_eq!(posted_entities(&bodies), vec![777]);
        assert_eq!(state.cursor, 5_000_001);
        assert_eq!(durable_cursor(&pool).await, 5_000_001);
    }

    /// Re-review N1: booting while a transaction holds an allocated-but-
    /// invisible id BELOW MIN(visible). The seed must carry a boot guard —
    /// durable cursor pinned at 0 (cleanup frozen) — and after the txn
    /// commits, the below-seed row must be delivered.
    #[tokio::test]
    #[ignore = "needs live PG via SKIP_RACE_PG_URL (tests/integration/run-skip-race-rust.sh)"]
    async fn boot_seed_guards_inflight_txn_below_min() {
        let url = std::env::var("SKIP_RACE_PG_URL").unwrap();
        let pool = PgPool::connect(&url).await.unwrap();
        setup(&pool).await;
        // Build: visible id 3, invisible id 2 (in-flight), id 1 trimmed.
        insert_op(&pool, 1).await; // id 1
        let mut txn = pool.begin().await.unwrap();
        sqlx::query(
            r#"INSERT INTO "BitdexOps" (entity_id, ops)
               VALUES (29674681, '[{"op":"set","field":"publishedAt","value":1}]'::jsonb)"#,
        )
        .execute(&mut *txn) // id 2, uncommitted
        .await
        .unwrap();
        insert_op(&pool, 555).await; // id 3
        sqlx::query(r#"DELETE FROM "BitdexOps" WHERE id = 1"#) // trim
            .execute(&pool)
            .await
            .unwrap();

        let (port, bodies) = mock_engine();
        let client = BitdexClient::new(&format!("http://127.0.0.1:{port}/api/indexes/t"));
        let mut state = init_state(&pool, "itest", 0).await;
        assert_eq!(state.cursor, 2, "seed = MIN(visible)-1");
        assert!(state.boot_guard.is_some(), "in-flight txn must trigger the guard");
        assert_eq!(durable_cursor(&pool).await, 0, "durable cursor pinned at 0");

        // While the txn is open: id 3 delivered, durable stays pinned.
        poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
        assert_eq!(posted_entities(&bodies), vec![555]);
        assert_eq!(durable_cursor(&pool).await, 0);

        // Txn commits — id 2 appears BELOW the seed. Guard releases, anchor
        // rewinds, the row is delivered, then the durable cursor catches up.
        txn.commit().await.unwrap();
        let mut delivered = false;
        for _ in 0..20 {
            poll_and_process(&pool, &client, 100, "itest", &mut state, None).await.unwrap();
            if posted_entities(&bodies).contains(&29674681) && state.cursor == 3 {
                delivered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(delivered, "below-seed late commit must be delivered (old code lost it)");
        assert!(state.boot_guard.is_none());
        assert_eq!(durable_cursor(&pool).await, 3);
    }
}

// ── SQL queries ──

/// Read cursor from PG bitdex_cursors table.
async fn read_cursor_from_pg(pool: &PgPool, cursor_name: &str) -> Result<i64, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"SELECT last_outbox_id FROM bitdex_cursors WHERE replica_id = $1"#,
    )
    .bind(cursor_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(0))
}

/// Poll ops from BitdexOps table after a cursor position.
/// On deserialization failure, logs the raw JSON of the failing row for debugging.
async fn poll_ops_from_cursor(
    pool: &PgPool,
    cursor: i64,
    limit: i64,
) -> Result<Vec<OpsRow>, sqlx::Error> {
    let result = sqlx::query_as::<_, OpsRow>(
        r#"SELECT id, entity_id, ops FROM "BitdexOps"
        WHERE id > $1
        ORDER BY id ASC
        LIMIT $2"#,
    )
    .bind(cursor)
    .bind(limit)
    .fetch_all(pool)
    .await;

    if let Err(ref e) = result {
        // Log the raw ops JSON for the failing batch to aid debugging
        eprintln!("ops_poller: deserialization error: {e}");
        if let Ok(raw_rows) = sqlx::query_as::<_, (i64, i64, String)>(
            r#"SELECT id, entity_id, ops::text FROM "BitdexOps"
            WHERE id > $1
            ORDER BY id ASC
            LIMIT $2"#,
        )
        .bind(cursor)
        .bind(limit)
        .fetch_all(pool)
        .await
        {
            for (id, entity_id, ops_text) in &raw_rows {
                let preview: String = ops_text.chars().take(200).collect();
                // Try to parse each row individually to find the exact failing one
                match serde_json::from_str::<Vec<super::ops::Op>>(ops_text) {
                    Ok(_) => {} // This row parses fine
                    Err(parse_err) => {
                        eprintln!(
                            "ops_poller: row id={id} entity_id={entity_id} fails: {parse_err}\n  ops={preview}"
                        );
                    }
                }
            }
        }
    }

    result
}

/// Get the current max ops ID (for lag calculation).
async fn get_max_ops_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (Option<i64>,) =
        sqlx::query_as(r#"SELECT MAX(id) FROM "BitdexOps""#)
            .fetch_one(pool)
            .await?;
    Ok(row.0.unwrap_or(0))
}

/// Get the current min ops ID (boot seed for a zero cursor against a
/// cleanup-trimmed table). Returns 1 when the table is empty so the seed
/// stays at 0.
async fn get_min_ops_id(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (Option<i64>,) =
        sqlx::query_as(r#"SELECT MIN(id) FROM "BitdexOps""#)
            .fetch_one(pool)
            .await?;
    Ok(row.0.unwrap_or(1))
}
