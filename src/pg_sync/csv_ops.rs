//! CSV→ops adapter for the dump pipeline.
//!
//! Reads existing CSV files (from PG COPY or local dumps) and transforms
//! each row into ops using the sync config schema. Writes ops to WAL files
//! for processing by the WAL reader thread.
//!
//! This is the local testing path and also the production dump path when
//! CSVs are pre-fetched to disk.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use serde_json::json;

use super::copy_queries::{parse_image_row, parse_tag_row, parse_tool_row, CopyImageRow};
use super::ops::{EntityOps, Op};
use crate::ops_wal::WalWriter;

/// Stats from a CSV→WAL conversion.
#[derive(Debug, Default)]
pub struct CsvOpsStats {
    pub rows_read: u64,
    pub rows_skipped: u64,
    pub ops_written: u64,
    pub bytes_written: u64,
    pub elapsed_secs: f64,
}

/// Convert images.csv to ops and write to WAL.
/// Each image row produces set ops for all tracked scalar fields.
pub fn images_csv_to_wal(csv_path: &Path, writer: &WalWriter, batch_size: usize) -> std::io::Result<CsvOpsStats> {
    let start = Instant::now();
    let file = File::open(csv_path)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut stats = CsvOpsStats::default();
    let mut batch: Vec<EntityOps> = Vec::with_capacity(batch_size);

    for line in reader.split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        let row = match parse_image_row(&line) {
            Some(r) => r,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };
        stats.rows_read += 1;

        let ops = image_row_to_ops(&row);
        batch.push(EntityOps {
            entity_id: row.id,
            ops,
            creates_slot: true, // Image table creates alive slots
        });

        if batch.len() >= batch_size {
            let bytes = writer.append_batch(&batch)?;
            stats.ops_written += batch.len() as u64;
            stats.bytes_written += bytes;
            batch.clear();
        }
    }

    // Flush remaining
    if !batch.is_empty() {
        let bytes = writer.append_batch(&batch)?;
        stats.ops_written += batch.len() as u64;
        stats.bytes_written += bytes;
    }

    stats.elapsed_secs = start.elapsed().as_secs_f64();
    Ok(stats)
}

/// Convert a single image CSV row to ops (public for direct dump path).
pub fn image_row_to_ops_pub(row: &CopyImageRow) -> Vec<Op> {
    image_row_to_ops(row)
}

/// Convert a single image CSV row to ops.
fn image_row_to_ops(row: &CopyImageRow) -> Vec<Op> {
    let mut ops = Vec::with_capacity(12);

    ops.push(Op::Set { field: "nsfwLevel".into(), value: json!(row.nsfw_level) });
    ops.push(Op::Set { field: "type".into(), value: json!(row.image_type) });
    ops.push(Op::Set { field: "userId".into(), value: json!(row.user_id) });

    if let Some(post_id) = row.post_id {
        ops.push(Op::Set { field: "postId".into(), value: json!(post_id) });
    }

    // hasMeta and onSite from flags
    let has_meta = row.has_meta();
    let on_site = row.on_site();
    ops.push(Op::Set { field: "hasMeta".into(), value: json!(has_meta) });
    ops.push(Op::Set { field: "onSite".into(), value: json!(on_site) });

    // Minor and POI
    let minor = row.minor();
    let poi = row.poi();
    ops.push(Op::Set { field: "minor".into(), value: json!(minor) });
    ops.push(Op::Set { field: "poi".into(), value: json!(poi) });

    // existedAt = GREATEST(scannedAt, createdAt) in seconds
    let existed_at = match (row.scanned_at_secs, row.created_at_secs) {
        (Some(s), Some(c)) => s.max(c),
        (Some(s), None) => s,
        (None, Some(c)) => c,
        (None, None) => 0,
    };
    ops.push(Op::Set { field: "existedAt".into(), value: json!(existed_at) });

    // blockedFor
    if let Some(ref bf) = row.blocked_for {
        ops.push(Op::Set { field: "blockedFor".into(), value: json!(bf) });
    }

    ops
}

/// Convert tags.csv to add ops and write to WAL.
/// Each row: (tag_id, image_id) → add tagIds op on the image.
pub fn tags_csv_to_wal(csv_path: &Path, writer: &WalWriter, batch_size: usize) -> std::io::Result<CsvOpsStats> {
    multi_value_csv_to_wal(csv_path, writer, batch_size, "tagIds", |line| {
        // tags.csv: tag_id, image_id
        parse_tag_row(line).map(|(tag_id, image_id)| (image_id, tag_id))
    })
}

/// Convert tools.csv to add ops and write to WAL.
pub fn tools_csv_to_wal(csv_path: &Path, writer: &WalWriter, batch_size: usize) -> std::io::Result<CsvOpsStats> {
    multi_value_csv_to_wal(csv_path, writer, batch_size, "toolIds", |line| {
        parse_tool_row(line).map(|(tool_id, image_id)| (image_id, tool_id))
    })
}

/// Generic multi-value CSV→WAL converter.
/// Parser returns (slot_id, value) pairs.
fn multi_value_csv_to_wal(
    csv_path: &Path,
    writer: &WalWriter,
    batch_size: usize,
    field_name: &str,
    parser: impl Fn(&[u8]) -> Option<(i64, i64)>,
) -> std::io::Result<CsvOpsStats> {
    let start = Instant::now();
    let file = File::open(csv_path)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut stats = CsvOpsStats::default();
    let mut batch: Vec<EntityOps> = Vec::with_capacity(batch_size);

    for line in reader.split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        let (slot_id, value) = match parser(&line) {
            Some(pair) => pair,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };
        stats.rows_read += 1;

        batch.push(EntityOps {
            entity_id: slot_id,
            ops: vec![Op::Add {
                field: field_name.to_string(),
                value: json!(value),
            }],
            creates_slot: false, // Join tables don't create alive slots
        });

        if batch.len() >= batch_size {
            let bytes = writer.append_batch(&batch)?;
            stats.ops_written += batch.len() as u64;
            stats.bytes_written += bytes;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let bytes = writer.append_batch(&batch)?;
        stats.ops_written += batch.len() as u64;
        stats.bytes_written += bytes;
    }

    stats.elapsed_secs = start.elapsed().as_secs_f64();
    Ok(stats)
}

/// Run the full CSV dump pipeline: read all CSVs, convert to ops, write to WAL.
/// Returns per-table stats.
pub fn run_csv_dump(
    csv_dir: &Path,
    wal_path: &Path,
    batch_size: usize,
    limit: Option<u64>,
) -> std::io::Result<Vec<(String, CsvOpsStats)>> {
    let writer = WalWriter::new(wal_path);
    let mut results = Vec::new();

    // Phase 1: Images (must be first — sets alive + scalar fields)
    let images_csv = csv_dir.join("images.csv");
    if images_csv.exists() {
        eprintln!("CSV dump: loading images.csv...");
        let stats = if let Some(max) = limit {
            images_csv_to_wal_limited(&images_csv, &writer, batch_size, max)?
        } else {
            images_csv_to_wal(&images_csv, &writer, batch_size)?
        };
        eprintln!(
            "  images: {} rows, {} ops, {:.1}s ({:.0}/s)",
            stats.rows_read, stats.ops_written, stats.elapsed_secs,
            stats.rows_read as f64 / stats.elapsed_secs.max(0.001)
        );
        results.push(("images".into(), stats));
    }

    // Phase 2: Multi-value tables (parallel-safe, but sequential here for simplicity)
    let tags_csv = csv_dir.join("tags.csv");
    if tags_csv.exists() {
        eprintln!("CSV dump: loading tags.csv...");
        let stats = if let Some(max) = limit {
            multi_value_csv_to_wal_limited(&tags_csv, &writer, batch_size, "tagIds", max, |line| {
                parse_tag_row(line).map(|(tag_id, image_id)| (image_id, tag_id))
            })?
        } else {
            tags_csv_to_wal(&tags_csv, &writer, batch_size)?
        };
        eprintln!(
            "  tags: {} rows, {} ops, {:.1}s ({:.0}/s)",
            stats.rows_read, stats.ops_written, stats.elapsed_secs,
            stats.rows_read as f64 / stats.elapsed_secs.max(0.001)
        );
        results.push(("tags".into(), stats));
    }

    let tools_csv = csv_dir.join("tools.csv");
    if tools_csv.exists() {
        eprintln!("CSV dump: loading tools.csv...");
        let stats = if let Some(max) = limit {
            multi_value_csv_to_wal_limited(&tools_csv, &writer, batch_size, "toolIds", max, |line| {
                parse_tool_row(line).map(|(tool_id, image_id)| (image_id, tool_id))
            })?
        } else {
            tools_csv_to_wal(&tools_csv, &writer, batch_size)?
        };
        eprintln!(
            "  tools: {} rows, {} ops, {:.1}s ({:.0}/s)",
            stats.rows_read, stats.ops_written, stats.elapsed_secs,
            stats.rows_read as f64 / stats.elapsed_secs.max(0.001)
        );
        results.push(("tools".into(), stats));
    }

    Ok(results)
}

/// Limited version of images_csv_to_wal — stops after `limit` rows.
fn images_csv_to_wal_limited(csv_path: &Path, writer: &WalWriter, batch_size: usize, limit: u64) -> std::io::Result<CsvOpsStats> {
    let start = Instant::now();
    let file = File::open(csv_path)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut stats = CsvOpsStats::default();
    let mut batch: Vec<EntityOps> = Vec::with_capacity(batch_size);

    for line in reader.split(b'\n') {
        if stats.rows_read >= limit {
            break;
        }
        let line = line?;
        if line.is_empty() { continue; }
        let row = match parse_image_row(&line) {
            Some(r) => r,
            None => { stats.rows_skipped += 1; continue; }
        };
        stats.rows_read += 1;
        batch.push(EntityOps { entity_id: row.id, ops: image_row_to_ops(&row), creates_slot: true });
        if batch.len() >= batch_size {
            let bytes = writer.append_batch(&batch)?;
            stats.ops_written += batch.len() as u64;
            stats.bytes_written += bytes;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        let bytes = writer.append_batch(&batch)?;
        stats.ops_written += batch.len() as u64;
        stats.bytes_written += bytes;
    }
    stats.elapsed_secs = start.elapsed().as_secs_f64();
    Ok(stats)
}

/// Limited version of multi_value_csv_to_wal.
fn multi_value_csv_to_wal_limited(
    csv_path: &Path,
    writer: &WalWriter,
    batch_size: usize,
    field_name: &str,
    limit: u64,
    parser: impl Fn(&[u8]) -> Option<(i64, i64)>,
) -> std::io::Result<CsvOpsStats> {
    let start = Instant::now();
    let file = File::open(csv_path)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut stats = CsvOpsStats::default();
    let mut batch: Vec<EntityOps> = Vec::with_capacity(batch_size);

    for line in reader.split(b'\n') {
        if stats.rows_read >= limit {
            break;
        }
        let line = line?;
        if line.is_empty() { continue; }
        let (slot_id, value) = match parser(&line) {
            Some(pair) => pair,
            None => { stats.rows_skipped += 1; continue; }
        };
        stats.rows_read += 1;
        batch.push(EntityOps {
            entity_id: slot_id,
            ops: vec![Op::Add { field: field_name.to_string(), value: json!(value) }],
            creates_slot: false,
        });
        if batch.len() >= batch_size {
            let bytes = writer.append_batch(&batch)?;
            stats.ops_written += batch.len() as u64;
            stats.bytes_written += bytes;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        let bytes = writer.append_batch(&batch)?;
        stats.ops_written += batch.len() as u64;
        stats.bytes_written += bytes;
    }
    stats.elapsed_secs = start.elapsed().as_secs_f64();
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_image_row_to_ops() {
        let row = CopyImageRow {
            id: 1,
            url: Some("test.jpg".into()),
            nsfw_level: 16,
            hash: None,
            flags: (1 << 13), // hasMeta=true
            image_type: "image".into(),
            user_id: 42,
            blocked_for: None,
            scanned_at_secs: Some(1000),
            created_at_secs: Some(2000),
            post_id: Some(100),
            width: None,
            height: None,
            published_at_secs: None,
            availability: String::new(),
            posted_to_id: None,
        };
        let ops = image_row_to_ops(&row);
        // Should have: nsfwLevel, type, userId, postId, hasMeta, onSite, minor, poi, existedAt
        assert!(ops.len() >= 9);

        // Check nsfwLevel
        let nsfw = ops.iter().find(|o| matches!(o, Op::Set { field, .. } if field == "nsfwLevel")).unwrap();
        if let Op::Set { value, .. } = nsfw { assert_eq!(*value, json!(16)); }

        // Check existedAt = max(1000, 2000) = 2000
        let existed = ops.iter().find(|o| matches!(o, Op::Set { field, .. } if field == "existedAt")).unwrap();
        if let Op::Set { value, .. } = existed { assert_eq!(*value, json!(2000)); }

        // Check hasMeta (flags bit 13 set)
        let has_meta = ops.iter().find(|o| matches!(o, Op::Set { field, .. } if field == "hasMeta")).unwrap();
        if let Op::Set { value, .. } = has_meta { assert_eq!(*value, json!(true)); }
    }

    #[test]
    fn test_csv_to_wal_roundtrip() {
        let dir = TempDir::new().unwrap();
        let csv_path = dir.path().join("images.csv");
        let wal_path = dir.path().join("ops.wal");

        // Write a tiny CSV (comma-separated, matching PG COPY CSV format)
        std::fs::write(&csv_path, b"1,http://img.jpg,16,,8192,image,42,,1000,2000,100\n2,,1,,0,video,99,,500,600,200\n").unwrap();

        let stats = images_csv_to_wal(&csv_path, &WalWriter::new(&wal_path), 100).unwrap();
        assert_eq!(stats.rows_read, 2);
        assert_eq!(stats.ops_written, 2);
        assert!(stats.bytes_written > 0);

        // Read back from WAL
        let mut reader = crate::ops_wal::WalReader::from_legacy(&wal_path, 0);
        let batch = reader.read_batch(100).unwrap();
        assert_eq!(batch.entries.len(), 2);
        assert_eq!(batch.entries[0].entity_id, 1);
        assert_eq!(batch.entries[1].entity_id, 2);
    }
}
