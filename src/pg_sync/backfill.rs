//! Backfill filter_only fields from Postgres.
//!
//! Streams rows from PG ordered by value ID (for bitmap-optimal insertion),
//! groups by imageId, and calls the filter-sync endpoint in batches.
//! Runs while the BitDex server is live — no downtime needed.
//!
//! Tracks completion via a BitDex cursor (`backfill-{field_name}`).
//! On sync startup, checks for missing cursors and auto-backfills.

use std::collections::HashMap;

use sqlx::PgPool;

use super::bitdex_client::BitdexClient;
use super::queries;

/// Backfill collectionIds by streaming CollectionItem from PG.
///
/// Processes in range chunks ordered by collectionId for bitmap-optimal insertion.
/// Groups results by imageId and sends batched filter-sync calls.
pub async fn backfill_collection_ids(
    pool: &PgPool,
    client: &BitdexClient,
    batch_size: usize,
) -> Result<u64, String> {
    let max_id = queries::get_max_collection_id(pool)
        .await
        .map_err(|e| format!("get_max_collection_id: {e}"))?;

    if max_id == 0 {
        eprintln!("Backfill: CollectionItem table is empty, nothing to backfill");
        return Ok(0);
    }

    eprintln!("Backfill collectionIds: max collectionId = {max_id}");

    let range_size: i64 = 10_000; // Process 10K collectionIds per chunk
    let mut total_synced: u64 = 0;
    let mut start: i64 = 0;

    // Accumulate imageId → collectionIds across range chunks.
    // Flush to filter-sync when the accumulator reaches batch_size images.
    let mut accum: HashMap<i64, Vec<i64>> = HashMap::new();

    while start <= max_id {
        let end = start + range_size;

        let rows = queries::fetch_collections_by_range(pool, start, end)
            .await
            .map_err(|e| format!("fetch_collections_by_range({start}..{end}): {e}"))?;

        for row in &rows {
            accum
                .entry(row.image_id)
                .or_default()
                .push(row.collection_id);
        }

        // Flush when accumulator is large enough
        if accum.len() >= batch_size {
            let count = flush_batch(client, &mut accum).await?;
            total_synced += count;
        }

        start = end;
    }

    // Flush remaining
    if !accum.is_empty() {
        let count = flush_batch(client, &mut accum).await?;
        total_synced += count;
    }

    eprintln!("Backfill collectionIds complete: {total_synced} images synced");
    Ok(total_synced)
}

/// Flush the accumulator to the filter-sync endpoint and clear it.
async fn flush_batch(
    client: &BitdexClient,
    accum: &mut HashMap<i64, Vec<i64>>,
) -> Result<u64, String> {
    let entries: Vec<(i64, Vec<i64>)> = accum.drain().collect();
    let count = entries.len() as u64;
    client.filter_sync("collectionIds", &entries).await?;
    Ok(count)
}

/// Check if a filter_only field needs backfilling by checking its cursor.
/// Returns true if the backfill cursor does not exist (field not yet backfilled).
pub async fn needs_backfill(client: &BitdexClient, field_name: &str) -> Result<bool, String> {
    let cursor_name = format!("backfill-{field_name}");
    match client.get_cursor(&cursor_name).await? {
        Some(_) => Ok(false), // Cursor exists — already backfilled
        None => Ok(true),     // No cursor — needs backfill
    }
}

/// Mark a field as backfilled by setting a cursor with a completion timestamp.
pub async fn mark_backfilled(client: &BitdexClient, field_name: &str) -> Result<(), String> {
    // We don't have a set_cursor on the client yet — use upsert with a cursor payload.
    // For now, we'll use a filter-sync with empty docs to set the cursor implicitly.
    // Actually, the simplest approach: use the upsert_batch with an empty doc list + cursor.
    let cursor_name = format!("backfill-{field_name}");
    let timestamp = chrono::Utc::now().to_rfc3339();
    client
        .upsert_batch(&[], Some((&cursor_name, &timestamp)))
        .await
}

/// Auto-backfill all filter_only fields that haven't been backfilled yet.
/// Called on sync startup before entering the outbox poll loop.
///
/// Currently only supports collectionIds. To add more fields, extend the
/// match on field_name with the appropriate backfill function.
pub async fn auto_backfill(
    pool: &PgPool,
    client: &BitdexClient,
    filter_only_fields: &[String],
    batch_size: usize,
) -> Result<(), String> {
    for field_name in filter_only_fields {
        if needs_backfill(client, field_name).await? {
            eprintln!("Auto-backfill: field '{field_name}' needs backfilling");

            match field_name.as_str() {
                "collectionIds" => {
                    backfill_collection_ids(pool, client, batch_size).await?;
                }
                other => {
                    eprintln!("Auto-backfill: no backfill handler for field '{other}', skipping");
                    continue;
                }
            }

            mark_backfilled(client, field_name).await?;
            eprintln!("Auto-backfill: field '{field_name}' marked as backfilled");
        } else {
            eprintln!("Auto-backfill: field '{field_name}' already backfilled, skipping");
        }
    }
    Ok(())
}
