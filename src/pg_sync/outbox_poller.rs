//! BitdexOutbox poller: polls PG for changes, deduplicates, and pushes to Bitdex.
//!
//! Poll loop:
//!   1. SELECT from BitdexOutbox ORDER BY id DESC LIMIT N
//!   2. Deduplicate by entity_id (DELETE wins over UPSERT)
//!   3. For UPSERTs: fetch full documents from PG, POST to Bitdex /documents/upsert
//!   4. For DELETEs: POST to Bitdex /documents (DELETE)
//!   5. DELETE FROM BitdexOutbox WHERE id <= max_processed_id

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
) -> Result<(), String> {
    let mut ticker = interval(Duration::from_secs(poll_interval_secs));
    eprintln!(
        "Outbox poller started (interval={}s, batch_limit={})",
        poll_interval_secs, batch_limit
    );

    loop {
        ticker.tick().await;

        match poll_and_process(pool, client, batch_limit).await {
            Ok(processed) => {
                if processed > 0 {
                    eprintln!("Outbox: processed {processed} changes");
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
) -> Result<usize, String> {
    // Step 1: Fetch outbox rows (LIFO — newest first)
    let rows = queries::poll_outbox(pool, batch_limit)
        .await
        .map_err(|e| format!("poll_outbox: {e}"))?;

    if rows.is_empty() {
        return Ok(0);
    }

    let max_id = rows.iter().map(|r| r.id).max().unwrap_or(0);
    let total_rows = rows.len();

    // Step 2: Deduplicate by entity_id. DELETE wins over UPSERT.
    let mut deduped: HashMap<i64, &str> = HashMap::new();
    for row in &rows {
        let entry = deduped.entry(row.entity_id).or_insert(row.event.as_str());
        if row.event == "DELETE" {
            *entry = "DELETE";
        }
    }

    let upsert_ids: Vec<i64> = deduped
        .iter()
        .filter(|(_, ev)| **ev == "UPSERT")
        .map(|(id, _)| *id)
        .collect();
    let delete_ids: Vec<i64> = deduped
        .iter()
        .filter(|(_, ev)| **ev == "DELETE")
        .map(|(id, _)| *id)
        .collect();

    // Step 3: Process UPSERTs — fetch full documents from PG and push to Bitdex
    if !upsert_ids.is_empty() {
        match fetch_and_push_upserts(pool, client, &upsert_ids).await {
            Ok(count) => {
                eprintln!("  upserted {count} documents");
            }
            Err(e) => {
                eprintln!("  upsert batch failed: {e}");
                // Don't delete from outbox — will retry on next poll
                return Err(e);
            }
        }
    }

    // Step 4: Process DELETEs
    if !delete_ids.is_empty() {
        match client.delete_batch(&delete_ids).await {
            Ok(()) => {
                eprintln!("  deleted {} documents", delete_ids.len());
            }
            Err(e) => {
                eprintln!("  delete batch failed: {e}");
                return Err(e);
            }
        }
    }

    // Step 5: Clean up processed outbox rows
    queries::delete_outbox(pool, max_id)
        .await
        .map_err(|e| format!("delete_outbox: {e}"))?;

    Ok(total_rows)
}

/// Fetch full documents for upsert IDs and push to Bitdex.
async fn fetch_and_push_upserts(
    pool: &PgPool,
    client: &BitdexClient,
    ids: &[i64],
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

    client.upsert_batch(&docs).await?;

    Ok(count)
}
