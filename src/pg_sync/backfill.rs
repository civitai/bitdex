//! Backfill filter_only fields from Postgres via COPY CSV → BitmapFs.
//!
//! Uses the same pattern as the single-pass bulk loader: mmap CSV, rayon
//! parallel parse, build HashMap<u64, RoaringBitmap>, save to BitmapFs.
//! Runs while the BitDex server is live — no downtime needed.
//!
//! After writing bitmaps to disk, signals the engine to reload the field's
//! existence set so lazy loading picks up the new data.
//!
//! Tracks completion via a BitDex cursor (`backfill-{field_name}`).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::bitmap_fs::BitmapFs;
use super::bitdex_client::BitdexClient;

/// Process collection_items.csv: build collectionIds filter bitmaps.
/// Returns HashMap<collection_id_u64, RoaringBitmap>.
///
/// Same mmap+rayon pattern as process_tags_csv in single_pass.rs.
/// CSV format: collectionId,imageId (2 columns, no header).
pub fn process_collection_items_csv(
    stage_dir: &Path,
) -> Result<HashMap<u64, RoaringBitmap>, String> {
    let csv_path = stage_dir.join("collection_items.csv");
    if !csv_path.exists() {
        return Err(format!("collection_items.csv not found in {}", stage_dir.display()));
    }

    let file = std::fs::File::open(&csv_path)
        .map_err(|e| format!("open collection_items.csv: {e}"))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap collection_items.csv: {e}"))?;
    let data = &mmap[..];
    let file_len = data.len();
    eprintln!(
        "  collection_items: mmap'd {} ({:.1} MB)",
        file_len,
        file_len as f64 / (1024.0 * 1024.0)
    );

    // Split into rayon chunks
    let num_threads = rayon::current_num_threads();
    let chunk_size = file_len / num_threads.max(1);
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(num_threads);
    let mut start = 0;
    for i in 0..num_threads {
        let end = if i == num_threads - 1 {
            file_len
        } else {
            let tentative = start + chunk_size;
            match data[tentative..].iter().position(|&b| b == b'\n') {
                Some(offset) => tentative + offset + 1,
                None => file_len,
            }
        };
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }

    let total = AtomicU64::new(0);
    let total_ref = &total;

    // Each thread builds its own HashMap<u64, RoaringBitmap>
    let thread_results: Vec<HashMap<u64, RoaringBitmap>> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &data[range_start..range_end];
            let mut bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();
            let mut count = 0u64;
            let mut line_start = 0;

            for i in 0..chunk.len() {
                if chunk[i] == b'\n' {
                    let line = &chunk[line_start..i];
                    line_start = i + 1;
                    if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                        continue;
                    }
                    if let Some((collection_id, image_id)) = parse_collection_line(line) {
                        bitmaps
                            .entry(collection_id as u64)
                            .or_insert_with(RoaringBitmap::new)
                            .insert(image_id as u32);
                        count += 1;
                    }
                }
            }
            total_ref.fetch_add(count, Ordering::Relaxed);
            bitmaps
        })
        .collect();

    // Merge thread-local HashMaps
    let mut merged: HashMap<u64, RoaringBitmap> = HashMap::new();
    for local in thread_results {
        for (key, bm) in local {
            merged.entry(key).or_insert_with(RoaringBitmap::new).bitor_assign(&bm);
        }
    }

    let total_rows = total.load(Ordering::Relaxed);
    eprintln!(
        "  collection_items: {} rows → {} distinct collectionIds",
        total_rows,
        merged.len()
    );

    Ok(merged)
}

/// Parse a single CSV line: "collectionId,imageId\r?\n"
fn parse_collection_line(line: &[u8]) -> Option<(i64, i64)> {
    let line = if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    };
    let comma = line.iter().position(|&b| b == b',')?;
    let collection_id = fast_parse_i64(&line[..comma])?;
    let image_id = fast_parse_i64(&line[comma + 1..])?;
    Some((collection_id, image_id))
}

/// Fast ASCII integer parser (no allocation).
fn fast_parse_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut result: i64 = 0;
    for &b in bytes {
        if b < b'0' || b > b'9' {
            return None;
        }
        result = result * 10 + (b - b'0') as i64;
    }
    Some(result)
}

use std::ops::BitOrAssign;

/// Save collectionIds bitmaps to BitmapFs and signal the engine to reload.
///
/// This is the main entry point for the backfill subcommand and auto-backfill.
/// Downloads the CSV from PG if not already staged, processes it, writes to
/// BitmapFs, and signals the engine to pick up the new data.
pub fn save_collection_bitmaps(
    bitmap_fs: &BitmapFs,
    bitmaps: &HashMap<u64, RoaringBitmap>,
) -> Result<u64, String> {
    crate::pg_sync::single_pass::save_filter_field_to_disk(bitmap_fs, "collectionIds", bitmaps)
}

/// Check if a filter_only field needs backfilling by checking its cursor.
pub async fn needs_backfill(client: &BitdexClient, field_name: &str) -> Result<bool, String> {
    let cursor_name = format!("backfill-{field_name}");
    match client.get_cursor(&cursor_name).await? {
        Some(_) => Ok(false),
        None => Ok(true),
    }
}

/// Mark a field as backfilled by setting a cursor.
pub async fn mark_backfilled(client: &BitdexClient, field_name: &str) -> Result<(), String> {
    let cursor_name = format!("backfill-{field_name}");
    let timestamp = chrono::Utc::now().to_rfc3339();
    client
        .upsert_batch(&[], Some((&cursor_name, &timestamp)))
        .await
}

/// Auto-backfill filter_only fields on sync startup.
///
/// For each filter_only field without a backfill cursor:
/// 1. Download CollectionItem CSV from PG via COPY (if not staged)
/// 2. Process CSV → bitmaps (mmap + rayon)
/// 3. Save to BitmapFs (atomic fpack writes)
/// 4. Signal engine to reload existence set
/// 5. Set backfill cursor
///
/// Fails hard if backfill cannot complete — sync must not start with
/// incomplete baseline data.
pub async fn auto_backfill(
    pool: &sqlx::PgPool,
    client: &BitdexClient,
    filter_only_fields: &[String],
    stage_dir: &Path,
    bitmap_path: &Path,
) -> Result<(), String> {
    for field_name in filter_only_fields {
        if !needs_backfill(client, field_name).await? {
            eprintln!("Auto-backfill: field '{field_name}' already backfilled, skipping");
            continue;
        }

        eprintln!("Auto-backfill: field '{field_name}' needs backfilling");

        match field_name.as_str() {
            "collectionIds" => {
                // Step 1: Download CSV if not staged
                let csv_path = stage_dir.join("collection_items.csv");
                let done_path = stage_dir.join("collection_items.csv.done");
                if !done_path.exists() {
                    eprintln!("  Downloading collection_items.csv from PG...");
                    super::bulk_loader::download_single_table(
                        pool, stage_dir, "collection_items", "collection_items.csv",
                    ).await?;
                }

                // Step 2: Process CSV → bitmaps
                let bitmaps = process_collection_items_csv(stage_dir)?;

                // Step 3: Save to BitmapFs
                let bitmap_fs = BitmapFs::new(bitmap_path);
                let bytes = save_collection_bitmaps(&bitmap_fs, &bitmaps)?;
                eprintln!(
                    "  Saved collectionIds: {} values ({:.1} MB)",
                    bitmaps.len(),
                    bytes as f64 / (1024.0 * 1024.0)
                );

                // Step 4: Signal engine to reload existence set
                if let Err(e) = client.reload_field("collectionIds").await {
                    eprintln!("  WARNING: failed to signal engine reload: {e}");
                    eprintln!("  Bitmaps are saved to disk — they'll be picked up on next restart");
                }
            }
            other => {
                return Err(format!("No backfill handler for field '{other}'"));
            }
        }

        // Step 5: Set cursor
        mark_backfilled(client, field_name).await.map_err(|e| {
            format!("Failed to mark backfill cursor for '{field_name}': {e}")
        })?;
        eprintln!("Auto-backfill: field '{field_name}' complete");
    }
    Ok(())
}
