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

use std::time::Duration;

use sqlx::PgPool;
use tokio::time::interval;

use super::bitdex_client::BitdexClient;
use super::op_dedup::dedup_ops;
use super::ops::{EntityOps, Op, OpsBatch, SyncMeta};

/// Row from BitdexOps table.
#[derive(Debug, sqlx::FromRow)]
struct OpsRow {
    id: i64,
    entity_id: i64,
    ops: sqlx::types::Json<Vec<Op>>,
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
    let mut cursor: i64 = read_cursor_from_pg(pool, cursor_name)
        .await
        .unwrap_or(0);
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
        match poll_and_process(pool, client, batch_limit, cursor_name, &mut cursor, replica_id).await {
            Ok(processed) => {
                let cycle_secs = cycle_start.elapsed().as_secs_f64();
                if processed > 0 {
                    eprintln!("Ops poller: processed {processed} ops (cursor={cursor}, cycle={cycle_secs:.3}s)");
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
    cursor: &mut i64,
    replica_id: Option<&str>,
) -> Result<usize, String> {
    // Fetch ops after cursor
    let rows = poll_ops_from_cursor(pool, *cursor, batch_limit)
        .await
        .map_err(|e| format!("poll_ops: {e}"))?;

    if rows.is_empty() {
        return Ok(0);
    }

    let max_id = rows.iter().map(|r| r.id).max().unwrap_or(*cursor);
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
        // All ops cancelled out — still advance cursor
        advance_cursor(pool, cursor_name, max_id, cursor).await?;
        return Ok(total_rows);
    }

    // Get max ops ID for lag calculation
    let max_ops_id = get_max_ops_id(pool).await.unwrap_or(max_id);

    // Build batch with metadata
    let ops_batch = OpsBatch {
        ops: batch,
        meta: Some(SyncMeta {
            source: replica_id.unwrap_or("default").to_string(),
            cursor: Some(max_id),
            max_id: Some(max_ops_id),
            lag_rows: Some(max_ops_id - max_id),
        }),
    };

    // POST to BitDex
    client
        .post_ops(&ops_batch)
        .await
        .map_err(|e| format!("post_ops: {e}"))?;

    // Advance cursor
    advance_cursor(pool, cursor_name, max_id, cursor).await?;

    Ok(total_rows)
}

async fn advance_cursor(
    pool: &PgPool,
    cursor_name: &str,
    max_id: i64,
    cursor: &mut i64,
) -> Result<(), String> {
    super::queries::upsert_cursor(pool, cursor_name, max_id)
        .await
        .map_err(|e| format!("upsert_cursor: {e}"))?;
    *cursor = max_id;
    Ok(())
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
