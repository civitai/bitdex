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

/// How long a frontier gap may stay unresolved before it is declared a
/// rolled-back insert and skipped. This is the fallback only — the normal
/// resolution path is the in-flight-transaction check, which settles as soon
/// as the transactions that were running when the gap appeared finish.
const GAP_TIMEOUT: Duration = Duration::from_secs(60);

/// Row from BitdexOps table.
#[derive(Debug, sqlx::FromRow)]
struct OpsRow {
    id: i64,
    entity_id: i64,
    ops: sqlx::types::Json<Vec<Op>>,
}

/// The lowest sequence-allocated id above the cursor that is not yet visible.
///
/// BIGSERIAL ids are handed out at INSERT time but rows only become visible at
/// COMMIT, so a long transaction's row can commit AFTER a later-id row from a
/// quick transaction. Advancing the cursor to the max VISIBLE id silently
/// skips the still-invisible row forever, and the bitdex_cursors cleanup
/// trigger then deletes it (repro: skip_repro.sh; prod specimen: post
/// 29674681's publish fan-out, 2026-07-09). The durable cursor must hold
/// below a gap until it either fills in (commit) or is proven a rollback.
struct GapInfo {
    first_missing: i64,
    seen_at: Instant,
    /// Backend txids in flight when the gap was first observed. The inserting
    /// transaction is necessarily among them (its row exists but is
    /// invisible), so once ALL of them have finished and the row is still
    /// invisible, the insert was rolled back and the id is safe to skip.
    xips: Vec<i64>,
}

/// Poller cursor state. The durable cursor never passes an unresolved gap;
/// rows beyond a gap are still POSTed immediately (tracked by `posted_hwm`)
/// and may re-POST after a restart — the same at-least-once semantics as
/// crash recovery, absorbed by the WAL reader's LIFO dedup.
struct PollerState {
    cursor: i64,
    posted_hwm: i64,
    gap: Option<GapInfo>,
    /// Ids declared rolled-back; treated as consumed when walking the
    /// frontier. Pruned once the cursor passes them.
    dead_ids: BTreeSet<i64>,
}

/// Walk ids (sorted ascending, all > cursor) from `cursor + 1` and return
/// `(safe_id, first_missing)`: the highest id the durable cursor may advance
/// to without passing an unexplained hole, and the first missing id if any.
/// `dead_ids` are ids proven rolled back — they count as present.
fn compute_safe_frontier(
    cursor: i64,
    ids: &[i64],
    dead_ids: &BTreeSet<i64>,
) -> (i64, Option<i64>) {
    let mut expected = cursor + 1;
    for &id in ids {
        while expected < id {
            if dead_ids.contains(&expected) {
                expected += 1;
            } else {
                return (expected - 1, Some(expected));
            }
        }
        expected = id + 1;
    }
    (expected - 1, None)
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
    let cursor: i64 = read_cursor_from_pg(pool, cursor_name)
        .await
        .unwrap_or(0);
    let mut state = PollerState {
        cursor,
        posted_hwm: cursor,
        gap: None,
        dead_ids: BTreeSet::new(),
    };
    eprintln!(
        "Ops poller started (interval={}ms, batch_limit={}, cursor_name={}, starting_cursor={})",
        poll_interval.as_millis(), batch_limit, cursor_name, cursor
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

/// Single poll + process cycle.
async fn poll_and_process(
    pool: &PgPool,
    client: &BitdexClient,
    batch_limit: i64,
    cursor_name: &str,
    state: &mut PollerState,
    replica_id: Option<&str>,
) -> Result<usize, String> {
    // Fetch ops after the durable cursor. While a gap is pending this re-reads
    // rows already POSTed (id <= posted_hwm) — required, because the gap can
    // only fill in behind them.
    let rows = poll_ops_from_cursor(pool, state.cursor, batch_limit)
        .await
        .map_err(|e| format!("poll_ops: {e}"))?;

    if rows.is_empty() && state.gap.is_none() {
        return Ok(0);
    }

    // Gap accounting BEFORE advancing anything.
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let (mut safe_id, mut first_missing) =
        compute_safe_frontier(state.cursor, &ids, &state.dead_ids);
    match (&state.gap, first_missing) {
        (Some(g), Some(fm)) if g.first_missing == fm => {
            // Same frontier gap still open: resolved as a rollback once every
            // transaction that was in flight at first sight has finished (the
            // inserting txn was necessarily among them), or on timeout.
            let expired = g.seen_at.elapsed() > GAP_TIMEOUT;
            if expired || xips_all_finished(pool, &g.xips).await {
                if expired {
                    eprintln!(
                        "Ops poller: gap at id {fm} unresolved after {}s — declaring rolled back",
                        GAP_TIMEOUT.as_secs()
                    );
                }
                state.dead_ids.insert(fm);
                state.gap = None;
                let (s, f) = compute_safe_frontier(state.cursor, &ids, &state.dead_ids);
                safe_id = s;
                first_missing = f;
            }
        }
        _ => {}
    }
    match first_missing {
        Some(fm) => {
            if state.gap.as_ref().map(|g| g.first_missing) != Some(fm) {
                // New frontier gap: capture the in-flight snapshot now.
                let xips = snapshot_xips(pool).await.unwrap_or_default();
                eprintln!(
                    "Ops poller: holding cursor at {safe_id} — id {fm} allocated but not \
                     yet visible ({} txns in flight)",
                    xips.len()
                );
                state.gap = Some(GapInfo { first_missing: fm, seen_at: Instant::now(), xips });
            }
        }
        None => state.gap = None,
    }
    state.dead_ids = state.dead_ids.split_off(&(state.cursor + 1));

    // POST only rows not already sent this lifetime.
    let rows: Vec<OpsRow> = rows.into_iter().filter(|r| r.id > state.posted_hwm).collect();
    if rows.is_empty() {
        // Nothing new to send; the durable cursor may still advance (e.g. a
        // gap just resolved behind already-POSTed rows).
        advance_cursor_safe(pool, cursor_name, safe_id, state).await?;
        return Ok(0);
    }

    let max_id = rows.iter().map(|r| r.id).max().unwrap_or(state.cursor);
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
        // All ops cancelled out — still advance the durable cursor (to the
        // safe frontier only, never past a gap).
        state.posted_hwm = state.posted_hwm.max(max_id);
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

    // POST to BitDex
    client
        .post_ops(&ops_batch)
        .await
        .map_err(|e| format!("post_ops: {e}"))?;

    state.posted_hwm = state.posted_hwm.max(max_id);
    advance_cursor_safe(pool, cursor_name, safe_id, state).await?;

    Ok(total_rows)
}

/// Advance the durable cursor to `safe_id` (never past an unresolved gap,
/// never backwards). The bitdex_cursors upsert also fires the cleanup
/// trigger, so a cursor that outran an invisible row would let cleanup delete
/// it — this is the single choke point that prevents that.
async fn advance_cursor_safe(
    pool: &PgPool,
    cursor_name: &str,
    safe_id: i64,
    state: &mut PollerState,
) -> Result<(), String> {
    if safe_id <= state.cursor {
        return Ok(());
    }
    super::queries::upsert_cursor(pool, cursor_name, safe_id)
        .await
        .map_err(|e| format!("upsert_cursor: {e}"))?;
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

/// True when none of `xips` is still in flight, i.e. none appears in the
/// CURRENT snapshot's xip list (a txid leaves the list the moment it commits
/// or aborts, and can never re-enter). Membership is checked against the
/// live list rather than `pg_xact_status`, which raises errors for txids it
/// cannot resolve. On query error, err on the safe side: still in flight
/// (the GAP_TIMEOUT fallback bounds the wait).
async fn xips_all_finished(pool: &PgPool, xips: &[i64]) -> bool {
    if xips.is_empty() {
        return true;
    }
    let row: Result<(bool,), _> = sqlx::query_as(
        r#"SELECT NOT EXISTS (
             SELECT 1
             FROM (SELECT pg_snapshot_xip(pg_current_snapshot())::text::bigint AS x) cur
             WHERE cur.x = ANY($1::bigint[]))"#,
    )
    .bind(xips)
    .fetch_one(pool)
    .await;
    row.map(|r| r.0).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dead(ids: &[i64]) -> BTreeSet<i64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn contiguous_ids_advance_to_max() {
        assert_eq!(compute_safe_frontier(5, &[6, 7, 8], &dead(&[])), (8, None));
    }

    #[test]
    fn empty_batch_holds_cursor() {
        assert_eq!(compute_safe_frontier(5, &[], &dead(&[])), (5, None));
    }

    /// The skip-race shape: a long transaction holds id 6 uncommitted while
    /// id 7 from a quick transaction is visible. The cursor must hold at 5 —
    /// the old `cursor = max_id` advance to 7 is exactly the prod data loss
    /// (specimen: post 29674681's publish fan-out).
    #[test]
    fn gap_at_frontier_holds_cursor() {
        assert_eq!(compute_safe_frontier(5, &[7], &dead(&[])), (5, Some(6)));
    }

    #[test]
    fn gap_mid_batch_advances_to_gap_edge() {
        assert_eq!(compute_safe_frontier(5, &[6, 7, 9, 10], &dead(&[])), (7, Some(8)));
    }

    #[test]
    fn dead_id_bridges_gap() {
        assert_eq!(compute_safe_frontier(5, &[7], &dead(&[6])), (7, None));
    }

    #[test]
    fn dead_id_bridges_to_next_gap() {
        assert_eq!(compute_safe_frontier(5, &[7, 10], &dead(&[6])), (7, Some(8)));
    }

    #[test]
    fn multi_id_rollback_needs_each_declared() {
        assert_eq!(compute_safe_frontier(5, &[9], &dead(&[6])), (6, Some(7)));
        assert_eq!(compute_safe_frontier(5, &[9], &dead(&[6, 7, 8])), (9, None));
    }

    #[test]
    fn gap_immediately_after_cursor_with_multiple_rows() {
        assert_eq!(compute_safe_frontier(10, &[12, 13], &dead(&[])), (10, Some(11)));
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
