//! BitdexOutbox poller: cursor-based polling from PG, pushes to local Bitdex instance.
//!
//! Poll loop:
//!   1. On boot: read cursor from BitDex (persisted on disk at checkpoint)
//!   2. SELECT from BitdexOutbox WHERE id > cursor ORDER BY id ASC LIMIT N
//!   3. Deduplicate by entity_id (highest outbox ID wins per entity)
//!   4. For UPSERTs: fetch full documents from PG, POST to Bitdex with cursor
//!   5. For DELETEs: DELETE to Bitdex with cursor
//!   6. Report cursor to bitdex_cursors table for outbox cleanup

use std::collections::HashMap;

use sqlx::PgPool;
use tokio::time::{Duration, interval};

use super::bitdex_client::BitdexClient;
use super::queries;
use super::row_assembler::{assemble_batch, EnrichmentData};

/// Run the outbox poller loop. Runs forever until the task is cancelled.
pub async fn run_outbox_poller(
    pool: &PgPool,
    client: &BitdexClient,
    poll_interval_secs: u64,
    batch_limit: i64,
    cursor_name: &str,
) -> Result<(), String> {
    // Read initial cursor from BitDex (persisted on disk from last checkpoint)
    let mut cursor: i64 = match client.get_cursor(cursor_name).await? {
        Some(value) => value.parse::<i64>().unwrap_or(0),
        None => 0,
    };
    eprintln!(
        "Outbox poller started (interval={}s, batch_limit={}, cursor_name={}, starting_cursor={})",
        poll_interval_secs, batch_limit, cursor_name, cursor
    );

    let mut ticker = interval(Duration::from_secs(poll_interval_secs));

    loop {
        ticker.tick().await;

        match poll_and_process(pool, client, batch_limit, cursor_name, &mut cursor).await {
            Ok(processed) => {
                if processed > 0 {
                    eprintln!("Outbox: processed {processed} changes (cursor={})", cursor);
                }
            }
            Err(e) => {
                eprintln!("Outbox poll error: {e}");
                // Continue polling — transient errors are expected
            }
        }
    }
}

/// Single poll + process cycle. Returns number of changes processed.
async fn poll_and_process(
    pool: &PgPool,
    client: &BitdexClient,
    batch_limit: i64,
    cursor_name: &str,
    cursor: &mut i64,
) -> Result<usize, String> {
    // Step 1: Fetch outbox rows after cursor (FIFO — oldest first)
    let rows = queries::poll_outbox_from_cursor(pool, *cursor, batch_limit)
        .await
        .map_err(|e| format!("poll_outbox: {e}"))?;

    if rows.is_empty() {
        return Ok(0);
    }

    let max_id = rows.iter().map(|r| r.id).max().unwrap_or(*cursor);
    let total_rows = rows.len();

    // Step 2: Deduplicate by entity_id. Highest outbox ID wins per entity.
    let mut deduped: HashMap<i64, (i64, &str)> = HashMap::new(); // entity_id -> (outbox_id, event)
    for row in &rows {
        let entry = deduped.entry(row.entity_id).or_insert((row.id, row.event.as_str()));
        if row.id > entry.0 {
            *entry = (row.id, row.event.as_str());
        }
    }

    let upsert_ids: Vec<i64> = deduped
        .iter()
        .filter(|(_, (_, ev))| *ev == "UPSERT")
        .map(|(id, _)| *id)
        .collect();
    let delete_ids: Vec<i64> = deduped
        .iter()
        .filter(|(_, (_, ev))| *ev == "DELETE")
        .map(|(id, _)| *id)
        .collect();

    let cursor_value = max_id.to_string();

    // Step 3: Process UPSERTs — fetch full documents from PG and push to Bitdex
    if !upsert_ids.is_empty() {
        match fetch_and_push_upserts(pool, client, &upsert_ids, cursor_name, &cursor_value).await {
            Ok(count) => {
                eprintln!("  upserted {count} documents");
            }
            Err(e) => {
                eprintln!("  upsert batch failed: {e}");
                // Don't advance cursor — will retry on next poll
                return Err(e);
            }
        }
    }

    // Step 4: Process DELETEs
    if !delete_ids.is_empty() {
        let cursor_arg = if upsert_ids.is_empty() {
            // Only set cursor on delete if no upserts (upsert already set it)
            Some((cursor_name, cursor_value.as_str()))
        } else {
            None
        };
        match client.delete_batch(&delete_ids, cursor_arg).await {
            Ok(()) => {
                eprintln!("  deleted {} documents", delete_ids.len());
            }
            Err(e) => {
                eprintln!("  delete batch failed: {e}");
                return Err(e);
            }
        }
    }

    // Step 5: Report cursor to Postgres for outbox cleanup
    queries::upsert_cursor(pool, cursor_name, max_id)
        .await
        .map_err(|e| format!("upsert_cursor: {e}"))?;

    // Advance local cursor
    *cursor = max_id;

    Ok(total_rows)
}

/// Fetch full documents for upsert IDs and push to Bitdex with cursor.
async fn fetch_and_push_upserts(
    pool: &PgPool,
    client: &BitdexClient,
    ids: &[i64],
    cursor_name: &str,
    cursor_value: &str,
) -> Result<usize, String> {
    // Fetch images + enrichment in parallel
    let images = queries::fetch_images_by_ids(pool, ids)
        .await
        .map_err(|e| format!("fetch_images_by_ids: {e}"))?;

    if images.is_empty() {
        return Ok(0);
    }

    let image_ids: Vec<i64> = images.iter().map(|r| r.id).collect();

    let (tags, tools, techniques, resources) = tokio::try_join!(
        queries::fetch_tags(pool, &image_ids),
        queries::fetch_tools(pool, &image_ids),
        queries::fetch_techniques(pool, &image_ids),
        queries::fetch_resources(pool, &image_ids),
    )
    .map_err(|e| format!("enrichment queries: {e}"))?;

    let enrichment = EnrichmentData::from_rows(tags, tools, techniques, resources);
    let docs = assemble_batch(&images, &enrichment);
    let count = docs.len();

    client
        .upsert_batch(&docs, Some((cursor_name, cursor_value)))
        .await?;

    Ok(count)
}
