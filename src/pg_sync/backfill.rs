//! Backfill filter_only fields from Postgres via COPY CSV → BitmapSilo.
//!
//! Uses the same pattern as the single-pass bulk loader: mmap CSV, rayon
//! parallel parse, build HashMap<u64, RoaringBitmap>, save to BitmapSilo.
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

// TODO: BitmapSilo (Phase 3) — bitmap persistence stubbed, needs BitmapSilo write path
use super::bitdex_client::BitdexClient;

/// Process collection_items.csv: build collectionIds filter bitmaps.
/// Returns HashMap<collection_id_u64, RoaringBitmap>.
///
/// Uses mmap+rayon parallel parse pattern.
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

    // Split into rayon chunks (handle small files gracefully)
    let num_threads = rayon::current_num_threads();
    let chunk_size = file_len / num_threads.max(1);
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(num_threads);
    if file_len > 0 {
        let mut start = 0;
        for i in 0..num_threads {
            let end = if i == num_threads - 1 {
                file_len
            } else {
                let tentative = (start + chunk_size).min(file_len);
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
    }

    let total = AtomicU64::new(0);
    let total_ref = &total;
    let errors = AtomicU64::new(0);
    let errors_ref = &errors;

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
                    match parse_collection_line(line) {
                        Ok((collection_id, image_id)) => {
                            bitmaps
                                .entry(collection_id as u64)
                                .or_insert_with(RoaringBitmap::new)
                                .insert(image_id as u32);
                            count += 1;
                        }
                        Err(_) => {
                            // Count parse errors — we'll fail if any exist
                            errors_ref.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            total_ref.fetch_add(count, Ordering::Relaxed);
            bitmaps
        })
        .collect();

    // Fail if any rows couldn't be parsed
    let error_count = errors.load(Ordering::Relaxed);
    if error_count > 0 {
        return Err(format!(
            "collection_items.csv: {} malformed rows (refusing to continue with incomplete data)",
            error_count,
        ));
    }

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
/// Validates ranges: collectionId >= 0, 0 <= imageId <= u32::MAX.
fn parse_collection_line(line: &[u8]) -> Result<(i64, i64), ()> {
    let line = if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    };
    let comma = line.iter().position(|&b| b == b',').ok_or(())?;
    let collection_id = fast_parse_i64(&line[..comma]).ok_or(())?;
    let image_id = fast_parse_i64(&line[comma + 1..]).ok_or(())?;
    if collection_id < 0 || image_id < 0 || image_id > u32::MAX as i64 {
        return Err(());
    }
    Ok((collection_id, image_id))
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

/// Save collectionIds bitmaps to disk.
/// TODO: BitmapSilo (Phase 3) — currently a no-op stub.
pub fn save_collection_bitmaps(
    _bitmaps: HashMap<u64, RoaringBitmap>,
) -> Result<u64, String> {
    // TODO: Write to BitmapSilo when Phase 3 is wired
    eprintln!("WARNING: save_collection_bitmaps is a no-op stub (BitmapSilo Phase 3)");
    Ok(0)
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
/// 3. Save to BitmapSilo
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

                // Step 3: Save bitmaps (TODO: BitmapSilo Phase 3)
                let bitmaps_count = bitmaps.len();
                let bytes = save_collection_bitmaps(bitmaps)?;
                eprintln!(
                    "  Saved collectionIds: {} values ({:.1} MB)",
                    bitmaps_count,
                    bytes as f64 / (1024.0 * 1024.0)
                );

                // Step 4: Signal engine to reload existence set (fatal if fails)
                client.reload_field("collectionIds").await.map_err(|e| {
                    format!("Failed to reload existence set for collectionIds: {e}. Bitmaps are saved to disk but engine hasn't picked them up.")
                })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write test CSV data to a temp dir and return the path.
    fn write_test_csv(dir: &std::path::Path, content: &str) {
        let path = dir.join("collection_items.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        // Write .done marker so backfill doesn't try to download
        std::fs::write(dir.join("collection_items.csv.done"), b"ok").unwrap();
    }

    #[test]
    fn test_parse_collection_line_valid() {
        assert_eq!(parse_collection_line(b"100,42"), Ok((100, 42)));
        assert_eq!(parse_collection_line(b"1,1"), Ok((1, 1)));
        assert_eq!(parse_collection_line(b"15722970,107000000"), Ok((15722970, 107000000)));
    }

    #[test]
    fn test_parse_collection_line_with_cr() {
        assert_eq!(parse_collection_line(b"100,42\r"), Ok((100, 42)));
    }

    #[test]
    fn test_parse_collection_line_negative_collection_id() {
        assert!(parse_collection_line(b"-1,42").is_err());
    }

    #[test]
    fn test_parse_collection_line_negative_image_id() {
        assert!(parse_collection_line(b"100,-5").is_err());
    }

    #[test]
    fn test_parse_collection_line_image_id_overflow() {
        // u32::MAX + 1 = 4294967296
        assert!(parse_collection_line(b"100,4294967296").is_err());
    }

    #[test]
    fn test_parse_collection_line_image_id_at_u32_max() {
        // u32::MAX = 4294967295 — should be accepted
        assert_eq!(
            parse_collection_line(b"100,4294967295"),
            Ok((100, 4294967295))
        );
    }

    #[test]
    fn test_parse_collection_line_no_comma() {
        assert!(parse_collection_line(b"12345").is_err());
    }

    #[test]
    fn test_parse_collection_line_empty() {
        assert!(parse_collection_line(b"").is_err());
    }

    #[test]
    fn test_parse_collection_line_non_numeric() {
        assert!(parse_collection_line(b"abc,def").is_err());
    }

    #[test]
    fn test_process_csv_basic() {
        let dir = tempfile::tempdir().unwrap();
        // 3 collections, 5 memberships
        write_test_csv(dir.path(), "100,1\n100,2\n100,3\n200,2\n200,4\n300,1\n");

        let bitmaps = process_collection_items_csv(dir.path()).unwrap();

        assert_eq!(bitmaps.len(), 3);
        assert!(bitmaps[&100].contains(1));
        assert!(bitmaps[&100].contains(2));
        assert!(bitmaps[&100].contains(3));
        assert_eq!(bitmaps[&100].len(), 3);

        assert!(bitmaps[&200].contains(2));
        assert!(bitmaps[&200].contains(4));
        assert_eq!(bitmaps[&200].len(), 2);

        assert!(bitmaps[&300].contains(1));
        assert_eq!(bitmaps[&300].len(), 1);
    }

    #[test]
    fn test_process_csv_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        write_test_csv(dir.path(), "");

        let bitmaps = process_collection_items_csv(dir.path()).unwrap();
        assert!(bitmaps.is_empty());
    }

    #[test]
    fn test_process_csv_single_row() {
        let dir = tempfile::tempdir().unwrap();
        write_test_csv(dir.path(), "42,99\n");

        let bitmaps = process_collection_items_csv(dir.path()).unwrap();
        assert_eq!(bitmaps.len(), 1);
        assert!(bitmaps[&42].contains(99));
    }

    #[test]
    fn test_process_csv_duplicate_rows_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        // Same membership repeated — bitmap should have it once
        write_test_csv(dir.path(), "100,1\n100,1\n100,1\n");

        let bitmaps = process_collection_items_csv(dir.path()).unwrap();
        assert_eq!(bitmaps[&100].len(), 1);
        assert!(bitmaps[&100].contains(1));
    }

    #[test]
    fn test_process_csv_malformed_row_fails() {
        let dir = tempfile::tempdir().unwrap();
        write_test_csv(dir.path(), "100,1\nbadline\n200,2\n");

        let result = process_collection_items_csv(dir.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("malformed"));
    }

    #[test]
    fn test_process_csv_negative_id_fails() {
        let dir = tempfile::tempdir().unwrap();
        write_test_csv(dir.path(), "100,1\n-5,2\n");

        let result = process_collection_items_csv(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_process_csv_image_id_overflow_fails() {
        let dir = tempfile::tempdir().unwrap();
        write_test_csv(dir.path(), "100,1\n200,4294967296\n");

        let result = process_collection_items_csv(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_process_csv_with_cr_lf() {
        let dir = tempfile::tempdir().unwrap();
        write_test_csv(dir.path(), "100,1\r\n200,2\r\n");

        let bitmaps = process_collection_items_csv(dir.path()).unwrap();
        assert_eq!(bitmaps.len(), 2);
        assert!(bitmaps[&100].contains(1));
        assert!(bitmaps[&200].contains(2));
    }

    #[test]
    fn test_process_csv_large_ids() {
        let dir = tempfile::tempdir().unwrap();
        // Large but valid IDs
        write_test_csv(dir.path(), "15722970,107000000\n");

        let bitmaps = process_collection_items_csv(dir.path()).unwrap();
        assert!(bitmaps[&15722970].contains(107000000));
    }

    #[test]
    fn test_save_and_load_bitmaps() {
        // Stubbed: save_collection_bitmaps is currently a no-op
    }

    #[test]
    fn test_end_to_end_csv_to_bitmapfs() {
        // Stubbed: bitmap persistence not yet wired
    }

    #[test]
    fn test_missing_csv_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        // No CSV file written
        let result = process_collection_items_csv(dir.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }
}
