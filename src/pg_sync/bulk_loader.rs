//! Bulk loader utilities: PG CSV download + ClickHouse metrics download.
//!
//! The V1 in-process bulk load pipeline (run_bulk_load / run_bulk_load_copy) has been
//! removed. Use the single-pass V2 loader via the pg-sync binary instead.
//!
//! Remaining functionality:
//!   - `download_all_tables` / `download_single_table`: Stream PG tables to local CSVs
//!   - `download_metrics_from_clickhouse`: Fetch aggregate metrics from ClickHouse
//!   - `finalize_from_bitmaps` / `scalars_to_json`: Docstore finalization helpers (used by tests)

use ahash::AHashMap as HashMap;
use std::time::Instant;

use roaring::RoaringBitmap;
use sqlx::PgPool;

use crate::loader::BitmapAccum;

use super::copy_queries;

// ---------------------------------------------------------------------------
// Compact per-image scalar storage (replaces 512-byte arena slots)
// ---------------------------------------------------------------------------

/// Compact per-image scalar data stored during CSV processing.
///
/// Only stores fields needed for docstore finalization that cannot be
/// reconstructed from filter/sort bitmaps. Multi-value fields (tagIds,
/// toolIds, etc.) are reconstructed from their filter bitmaps.
///
/// At ~80 bytes avg per image (including heap strings), 107M images ≈ 8.5 GB.
/// This replaces the 60GB memory-mapped SlotArena.
#[derive(Debug)]
struct ImageScalars {
    url: Option<Box<str>>,   // Box<str> instead of String saves 8 bytes/entry (no capacity field)
    hash: Option<Box<str>>,
    nsfw_level: u8,
    user_id: u64,
    image_type: u8,      // encoded via encode_image_type
    sort_at: u64,         // epoch seconds
    poi: bool,            // image-level poi (OR'd with resource_poi at finalization)
    minor: bool,
    has_meta: bool,
    on_site: bool,
    post_id: u64,
    posted_to_id: u64,
    availability: u8,     // encoded via encode_availability
    blocked_for: u8,      // encoded via encode_blocked_for
    published_at_ms: u64, // milliseconds
}

/// Per-slot resource enrichment data, written by the resources stream.
/// Stored separately because it arrives from a different CSV file.
#[derive(Debug, Default)]
struct ResourceEnrichment {
    base_model: u8,       // encoded via encode_base_model
    resource_poi: bool,
}

/// Statistics from a completed bulk load.
#[derive(Debug)]
pub struct BulkLoadStats {
    pub records_loaded: u64,
    pub errors: u64,
    pub elapsed: std::time::Duration,
}

// ---------------------------------------------------------------------------
// Phase 1: Download tables to local CSV files
// ---------------------------------------------------------------------------

/// Table descriptor for the download phase.
struct TableDownload {
    name: &'static str,
    file: &'static str,
}

const TABLES: &[TableDownload] = &[
    TableDownload { name: "images", file: "images.csv" },
    TableDownload { name: "posts", file: "posts.csv" },
    TableDownload { name: "tags", file: "tags.csv" },
    TableDownload { name: "tools", file: "tools.csv" },
    TableDownload { name: "techniques", file: "techniques.csv" },
    TableDownload { name: "resources", file: "resources.csv" },
    TableDownload { name: "model_versions", file: "model_versions.csv" },
    TableDownload { name: "models", file: "models.csv" },
    TableDownload { name: "collection_items", file: "collection_items.csv" },
];

/// Download a single named table from PG to a CSV file.
/// Public wrapper for use by backfill module.
pub async fn download_single_table(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    name: &'static str,
    file: &'static str,
) -> Result<u64, String> {
    let table = TableDownload { name, file };
    download_table(pool, stage_dir, &table).await
}

/// Download a single table from PG to a CSV file on the PVC.
/// Returns the number of bytes written.
/// Skips if the .done marker already exists.
async fn download_table(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    table: &TableDownload,
) -> Result<u64, String> {
    use futures_util::TryStreamExt;
    use tokio::io::AsyncWriteExt;

    let csv_path = stage_dir.join(table.file);
    let done_path = stage_dir.join(format!("{}.done", table.file));

    // Skip if already downloaded
    if done_path.exists() {
        let size = std::fs::metadata(&csv_path).map(|m| m.len()).unwrap_or(0);
        eprintln!("  {}: already downloaded ({:.1} MB), skipping", table.name, size as f64 / 1048576.0);
        return Ok(size);
    }

    // Get the COPY stream for this table
    let mut stream = match table.name {
        "images" => copy_queries::copy_images(pool).await,
        "posts" => copy_queries::copy_posts(pool).await,
        "tags" => copy_queries::copy_tags(pool).await,
        "tools" => copy_queries::copy_tools(pool).await,
        "techniques" => copy_queries::copy_techniques(pool).await,
        "resources" => copy_queries::copy_resources(pool).await,
        "model_versions" => copy_queries::copy_model_versions(pool).await,
        "models" => copy_queries::copy_models(pool).await,
        "collection_items" => copy_queries::copy_collection_items(pool).await,
        _ => return Err(format!("unknown table: {}", table.name)),
    }.map_err(|e| format!("{}: COPY start failed: {e}", table.name))?;

    // Stream to file
    let file = tokio::fs::File::create(&csv_path)
        .await
        .map_err(|e| format!("{}: create file: {e}", table.name))?;
    let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);
    let mut bytes_written = 0u64;
    let start = Instant::now();

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("{}: COPY stream: {e}", table.name))?
    {
        writer
            .write_all(&chunk)
            .await
            .map_err(|e| format!("{}: write: {e}", table.name))?;
        bytes_written += chunk.len() as u64;
    }
    writer.flush().await.map_err(|e| format!("{}: flush: {e}", table.name))?;

    // Write .done marker
    std::fs::write(&done_path, b"ok")
        .map_err(|e| format!("{}: write done marker: {e}", table.name))?;

    let elapsed = start.elapsed();
    eprintln!(
        "  {}: {:.1} MB in {:.1}s ({:.0} MB/s)",
        table.name,
        bytes_written as f64 / 1048576.0,
        elapsed.as_secs_f64(),
        bytes_written as f64 / 1048576.0 / elapsed.as_secs_f64().max(0.001),
    );

    Ok(bytes_written)
}

/// Download all tables from PG to CSV files on the PVC.
/// Each table runs concurrently. Completed tables are skipped on retry.
pub async fn download_all_tables(
    pool: &PgPool,
    stage_dir: &std::path::Path,
) -> Result<(), String> {
    std::fs::create_dir_all(stage_dir)
        .map_err(|e| format!("create stage dir: {e}"))?;

    eprintln!("\n=== Phase 1: Downloading tables to {} ===", stage_dir.display());
    let start = Instant::now();

    // Download all tables concurrently
    let results = tokio::join!(
        download_table(pool, stage_dir, &TABLES[0]), // images
        download_table(pool, stage_dir, &TABLES[1]), // posts
        download_table(pool, stage_dir, &TABLES[2]), // tags
        download_table(pool, stage_dir, &TABLES[3]), // tools
        download_table(pool, stage_dir, &TABLES[4]), // techniques
        download_table(pool, stage_dir, &TABLES[5]), // resources
        download_table(pool, stage_dir, &TABLES[6]), // model_versions
        download_table(pool, stage_dir, &TABLES[7]), // models
    );

    // Check all results
    let mut total_bytes = 0u64;
    for (i, result) in [results.0, results.1, results.2, results.3, results.4, results.5, results.6, results.7].into_iter().enumerate() {
        total_bytes += result.map_err(|e| format!("download {} failed: {e}", TABLES[i].name))?;
    }

    eprintln!(
        "Phase 1 complete: {:.1} GB in {:.1}s",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        start.elapsed().as_secs_f64(),
    );

    Ok(())
}

/// Download CSVs using copy_query from sync config dump phases.
///
/// Config-driven replacement for download_all_tables — uses the exact COPY SQL
/// from each DumpPhase (and its enrichment lookups) instead of hardcoded queries.
/// This ensures the CSVs match what the dump processor expects.
pub async fn download_from_sync_config(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    dump_phases: &[super::sync_config::DumpPhase],
) -> Result<(), String> {
    std::fs::create_dir_all(stage_dir)
        .map_err(|e| format!("create stage dir: {e}"))?;

    eprintln!("\n=== Downloading CSVs from sync config ({} phases) ===", dump_phases.len());
    let start = Instant::now();
    let mut total_bytes = 0u64;

    for phase in dump_phases {
        // Skip non-PG sources (e.g., ClickHouse metrics)
        if phase.source.as_deref() == Some("clickhouse") {
            eprintln!("  {}: ClickHouse source — skipping PG download", phase.name);
            continue;
        }

        // Download the main table CSV
        if let Some(ref copy_query) = phase.copy_query {
            let ext = if phase.format == "tsv" { "tsv" } else { "csv" };
            let filename = format!("{}.{}", phase.name, ext);
            let header = if !phase.columns.is_empty() { Some(&phase.columns) } else { None };
            let bytes = download_copy_query(pool, stage_dir, &phase.name, &filename, copy_query, header).await?;
            total_bytes += bytes;
        }

        // Download enrichment lookup CSVs
        download_enrichment_csvs(pool, stage_dir, &phase.enrichment).await?;
    }

    eprintln!(
        "CSV download complete: {:.1} GB in {:.1}s",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        start.elapsed().as_secs_f64(),
    );

    Ok(())
}

/// Download CSVs for a single dump phase (main table + enrichment lookups).
/// Used by the streaming pipeline to download per-phase instead of all-at-once.
pub async fn download_phase_csvs(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    phase: &super::sync_config::DumpPhase,
) -> Result<u64, String> {
    std::fs::create_dir_all(stage_dir)
        .map_err(|e| format!("create stage dir: {e}"))?;

    let mut total_bytes = 0u64;

    if phase.source.as_deref() == Some("clickhouse") {
        return Ok(0); // ClickHouse handled separately
    }

    // Download the main table CSV
    if let Some(ref copy_query) = phase.copy_query {
        let ext = if phase.format == "tsv" { "tsv" } else { "csv" };
        let filename = format!("{}.{}", phase.name, ext);
        let header = if !phase.columns.is_empty() { Some(&phase.columns) } else { None };
        let bytes = download_copy_query(pool, stage_dir, &phase.name, &filename, copy_query, header).await?;
        total_bytes += bytes;
    }

    // Download enrichment lookup CSVs
    download_enrichment_csvs(pool, stage_dir, &phase.enrichment).await?;

    Ok(total_bytes)
}

/// Clear all .done markers in the stage directory.
/// Called at boot to prevent stale markers from a previous run (e.g., after PVC wipe)
/// from causing downloads to be skipped.
pub fn clear_done_markers(stage_dir: &std::path::Path) {
    if let Ok(entries) = std::fs::read_dir(stage_dir) {
        let mut cleared = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("done") {
                if std::fs::remove_file(&path).is_ok() {
                    cleared += 1;
                }
            }
        }
        if cleared > 0 {
            eprintln!("Cleared {cleared} stale .done markers from {}", stage_dir.display());
        }
    }
}

/// Recursively download enrichment lookup CSVs.
async fn download_enrichment_csvs(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    enrichments: &[super::sync_config::EnrichmentDef],
) -> Result<(), String> {
    for enrich in enrichments {
        if let (Some(ref lookup), Some(ref copy_query)) = (&enrich.lookup, &enrich.copy_query) {
            let name = enrich.table.as_deref().unwrap_or(lookup.trim_end_matches(".csv"));
            let header = if !enrich.columns.is_empty() { Some(&enrich.columns) } else { None };
            download_copy_query(pool, stage_dir, name, lookup, copy_query, header).await?;
        }
        // Recurse into nested enrichments
        if !enrich.enrichment.is_empty() {
            Box::pin(download_enrichment_csvs(pool, stage_dir, &enrich.enrichment)).await?;
        }
    }
    Ok(())
}

/// Execute a COPY query and stream results to a CSV file.
/// Skips if the .done marker already exists (idempotent on retry).
async fn download_copy_query(
    pool: &PgPool,
    stage_dir: &std::path::Path,
    name: &str,
    filename: &str,
    copy_query: &str,
    columns: Option<&Vec<String>>,
) -> Result<u64, String> {
    use futures_util::TryStreamExt;
    use sqlx::postgres::PgPoolCopyExt;
    use tokio::io::AsyncWriteExt;

    let csv_path = stage_dir.join(filename);
    let done_path = stage_dir.join(format!("{}.done", filename));

    // Skip if already downloaded
    if done_path.exists() {
        let size = std::fs::metadata(&csv_path).map(|m| m.len()).unwrap_or(0);
        eprintln!("  {}: already downloaded ({:.1} MB), skipping", name, size as f64 / 1048576.0);
        return Ok(size);
    }

    eprintln!("  {}: downloading via COPY...", name);

    let file = tokio::fs::File::create(&csv_path)
        .await
        .map_err(|e| format!("{name}: create file: {e}"))?;
    let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);
    let mut bytes_written = 0u64;
    let start_time = Instant::now();

    // Prepend CSV header line when columns are specified (PG COPY TO STDOUT
    // doesn't support HEADER — we add it ourselves from the sync config).
    if let Some(cols) = columns {
        let header_line = format!("{}\n", cols.join(","));
        writer.write_all(header_line.as_bytes()).await
            .map_err(|e| format!("{name}: write header: {e}"))?;
        bytes_written += header_line.len() as u64;
    }

    // Use copy_out_raw — same API as copy_queries.rs
    let mut stream = pool
        .copy_out_raw(copy_query)
        .await
        .map_err(|e| format!("{name}: COPY start failed: {e}"))?;

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("{name}: COPY stream: {e}"))?
    {
        writer
            .write_all(&chunk)
            .await
            .map_err(|e| format!("{name}: write: {e}"))?;
        bytes_written += chunk.len() as u64;
    }
    writer.flush().await.map_err(|e| format!("{name}: flush: {e}"))?;

    // Write .done marker
    std::fs::write(&done_path, b"ok")
        .map_err(|e| format!("{name}: write done marker: {e}"))?;

    let elapsed = start_time.elapsed();
    eprintln!(
        "  {}: {:.1} MB in {:.1}s ({:.0} MB/s)",
        name,
        bytes_written as f64 / 1048576.0,
        elapsed.as_secs_f64(),
        bytes_written as f64 / 1048576.0 / elapsed.as_secs_f64().max(0.001),
    );

    Ok(bytes_written)
}

// ---------------------------------------------------------------------------
// Arena-free docstore finalization (used by V1 bulk loader, kept for tests)
// ---------------------------------------------------------------------------

/// Block size for chunked bitmap reconstruction.
/// Aligned with roaring bitmap container boundaries (65,536 = 2^16).
const FINALIZE_CHUNK_SIZE: u32 = 65_536;

/// Finalize alive slots to the docstore by reconstructing multi-value fields
/// from filter bitmaps and combining with stored scalars.
///
/// Processes alive slots in 65K-block chunks aligned to roaring container
/// boundaries for efficient `bitmap.range()` iteration.
#[allow(dead_code)]
fn finalize_from_bitmaps(
    bulk_writer: &crate::shard_store_doc::ShardStoreBulkWriter,
    schema: &crate::config::DataSchema,
    alive: &RoaringBitmap,
    image_scalars: &HashMap<u32, ImageScalars>,
    resource_enrichments: &HashMap<u32, ResourceEnrichment>,
    tag_bitmaps: &HashMap<u64, RoaringBitmap>,
    tool_bitmaps: &HashMap<u64, RoaringBitmap>,
    technique_bitmaps: &HashMap<u64, RoaringBitmap>,
    mv_bitmaps: &HashMap<u64, RoaringBitmap>,
) -> Result<(u64, u64), String> {
    use rayon::prelude::*;

    let total = alive.len() as u64;
    eprintln!("finalize_from_bitmaps: reconstructing {} docs from bitmaps...", total);

    // Determine the range of slots to process
    let max_slot = alive.max().unwrap_or(0);
    let num_chunks = (max_slot / FINALIZE_CHUNK_SIZE) + 1;

    eprintln!(
        "  Processing {} chunks of {} slots (max_slot={})",
        num_chunks, FINALIZE_CHUNK_SIZE, max_slot
    );

    // Process chunks in parallel via rayon
    let chunk_results: Vec<(u64, u64)> = (0..=num_chunks)
        .into_par_iter()
        .map(|chunk_idx| {
            let chunk_start = chunk_idx * FINALIZE_CHUNK_SIZE;
            let chunk_end = chunk_start + FINALIZE_CHUNK_SIZE;

            // Get alive slots in this chunk
            let chunk_alive: Vec<u32> = alive.range(chunk_start..chunk_end).collect();
            if chunk_alive.is_empty() {
                return (0u64, 0u64);
            }

            // Reconstruct multi-value fields for all slots in this chunk
            // For each multi-value field, iterate all value bitmaps and check
            // which slots in this chunk are set.
            let mut chunk_tags: Vec<Vec<u32>> = vec![Vec::new(); FINALIZE_CHUNK_SIZE as usize];
            let mut chunk_tools: Vec<Vec<u32>> = vec![Vec::new(); FINALIZE_CHUNK_SIZE as usize];
            let mut chunk_techniques: Vec<Vec<u32>> = vec![Vec::new(); FINALIZE_CHUNK_SIZE as usize];
            let mut chunk_mvs: Vec<Vec<u32>> = vec![Vec::new(); FINALIZE_CHUNK_SIZE as usize];

            // Reconstruct tagIds
            for (&tag_id, bm) in tag_bitmaps {
                for slot in bm.range(chunk_start..chunk_end) {
                    chunk_tags[(slot - chunk_start) as usize].push(tag_id as u32);
                }
            }

            // Reconstruct toolIds
            for (&tool_id, bm) in tool_bitmaps {
                for slot in bm.range(chunk_start..chunk_end) {
                    chunk_tools[(slot - chunk_start) as usize].push(tool_id as u32);
                }
            }

            // Reconstruct techniqueIds
            for (&tech_id, bm) in technique_bitmaps {
                for slot in bm.range(chunk_start..chunk_end) {
                    chunk_techniques[(slot - chunk_start) as usize].push(tech_id as u32);
                }
            }

            // Reconstruct modelVersionIds
            for (&mv_id, bm) in mv_bitmaps {
                for slot in bm.range(chunk_start..chunk_end) {
                    chunk_mvs[(slot - chunk_start) as usize].push(mv_id as u32);
                }
            }

            // Build JSON docs and encode
            let encoded: Vec<(u32, Vec<u8>)> = chunk_alive
                .iter()
                .filter_map(|&slot| {
                    let scalars = image_scalars.get(&slot)?;
                    let enrichment = resource_enrichments.get(&slot);
                    let offset = (slot - chunk_start) as usize;

                    let json = scalars_to_json(
                        slot,
                        scalars,
                        enrichment,
                        &chunk_tags[offset],
                        &chunk_tools[offset],
                        &chunk_techniques[offset],
                        &chunk_mvs[offset],
                    );
                    let bytes = bulk_writer.encode_json(&json, schema);
                    Some((slot, bytes))
                })
                .collect();

            let docs = encoded.len() as u64;
            let bytes: u64 = encoded.iter().map(|(_, b)| b.len() as u64).sum();

            // Write to docstore
            bulk_writer.write_batch_encoded(encoded);

            (docs, bytes)
        })
        .collect();

    let docs_written: u64 = chunk_results.iter().map(|(d, _)| d).sum();
    let bytes_written: u64 = chunk_results.iter().map(|(_, b)| b).sum();

    eprintln!(
        "finalize_from_bitmaps: finalized {} docs, {} MB encoded",
        docs_written,
        bytes_written / (1024 * 1024)
    );

    Ok((docs_written, bytes_written))
}

/// Convert compact ImageScalars + reconstructed multi-value fields to a
/// JSON document matching the Bitdex data schema.
///
/// Produces the same output as `slot_data_to_json` in slot_arena.rs.
fn scalars_to_json(
    slot: u32,
    s: &ImageScalars,
    enrichment: Option<&ResourceEnrichment>,
    tag_ids: &[u32],
    tool_ids: &[u32],
    technique_ids: &[u32],
    model_version_ids: &[u32],
) -> serde_json::Value {
    use super::slot_arena::{decode_image_type, decode_availability, decode_base_model};

    let base_model_enum = enrichment.map(|e| e.base_model).unwrap_or(0);
    let resource_poi = enrichment.map(|e| e.resource_poi).unwrap_or(false);
    let poi = s.poi || resource_poi;

    let mut doc = serde_json::json!({
        "id": slot as i64,
        "nsfwLevel": s.nsfw_level as i64,
        "userId": s.user_id as i64,
        "postId": s.post_id as i64,
        "postedToId": s.posted_to_id as i64,
        "type": decode_image_type(s.image_type),
        "baseModel": decode_base_model(base_model_enum),
        "availability": decode_availability(s.availability),
        "tagIds": tag_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "modelVersionIds": model_version_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "modelVersionIdsManual": serde_json::json!([]),
        "toolIds": tool_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "techniqueIds": technique_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "reactionCount": 0i64,
        "commentCount": 0i64,
        "collectedCount": 0i64,
        "sortAt": s.sort_at as i64,
        "publishedAt": (s.published_at_ms / 1000) as i64,
    });

    if let Some(obj) = doc.as_object_mut() {
        // Exists-boolean: isPublished = publishedAt is non-zero (matches outbox row_assembler)
        if s.published_at_ms > 0 {
            obj.insert("isPublished".into(), serde_json::json!(true));
        }
        if s.has_meta {
            obj.insert("hasMeta".into(), serde_json::json!(true));
        }
        if s.on_site {
            obj.insert("onSite".into(), serde_json::json!(true));
        }
        if poi {
            obj.insert("poi".into(), serde_json::json!(true));
        }
        if s.minor {
            obj.insert("minor".into(), serde_json::json!(true));
        }
        if let Some(ref url) = s.url {
            obj.insert("url".into(), serde_json::json!(url.as_ref()));
        }
        if let Some(ref hash) = s.hash {
            obj.insert("hash".into(), serde_json::json!(hash.as_ref()));
        }
        if s.blocked_for > 0 {
            obj.insert("blockedFor".into(), serde_json::json!("blocked"));
        }
    }

    doc
}

/// AND all filter and sort bitmaps in an accumulator against the alive bitmap.
///
/// Returns the number of bitmaps that were modified (had orphan bits stripped).
/// This enforces the clean bitmap invariant: filter bitmaps must be subsets of alive.
#[allow(dead_code)]
fn cleanup_orphan_bitmaps(accum: &mut BitmapAccum, alive: &RoaringBitmap) -> usize {
    let mut cleaned = 0;
    for value_map in accum.filter_maps.values_mut() {
        for bitmap in value_map.values_mut() {
            let before = bitmap.len();
            *bitmap &= alive;
            if bitmap.len() < before {
                cleaned += 1;
            }
        }
    }
    for bit_map in accum.sort_maps.values_mut() {
        for bitmap in bit_map.values_mut() {
            let before = bitmap.len();
            *bitmap &= alive;
            if bitmap.len() < before {
                cleaned += 1;
            }
        }
    }
    cleaned
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scalars(slot: u32) -> ImageScalars {
        ImageScalars {
            url: Some(format!("https://example.com/{slot}.jpg").into_boxed_str()),
            hash: Some(format!("hash{slot}").into_boxed_str()),
            nsfw_level: 1,
            user_id: slot as u64 * 7,
            image_type: 0, // "image"
            sort_at: 1700000000 + slot as u64,
            poi: false,
            minor: false,
            has_meta: true,
            on_site: false,
            post_id: 100 + slot as u64,
            posted_to_id: 200 + slot as u64,
            availability: 0, // "Public"
            blocked_for: 0,
            published_at_ms: 1700000000000 + slot as u64 * 1000,
        }
    }

    #[test]
    fn test_scalars_to_json_basic() {
        let scalars = make_scalars(42);
        let json = scalars_to_json(42, &scalars, None, &[], &[], &[], &[]);

        let obj = json.as_object().unwrap();
        assert_eq!(obj["id"], 42);
        assert_eq!(obj["nsfwLevel"], 1);
        assert_eq!(obj["userId"], 42 * 7);
        assert_eq!(obj["type"], "image");
        assert_eq!(obj["url"], "https://example.com/42.jpg");
        assert_eq!(obj["hash"], "hash42");
        assert_eq!(obj["hasMeta"], true);
        assert_eq!(obj["tagIds"].as_array().unwrap().len(), 0);
        assert_eq!(obj["modelVersionIds"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_scalars_to_json_with_multi_value() {
        let scalars = make_scalars(10);
        let tags = vec![100u32, 200, 300];
        let tools = vec![50u32];
        let techniques = vec![5u32, 6];
        let mvs = vec![999u32, 888];

        let json = scalars_to_json(10, &scalars, None, &tags, &tools, &techniques, &mvs);
        let obj = json.as_object().unwrap();

        let tag_ids: Vec<i64> = obj["tagIds"].as_array().unwrap()
            .iter().map(|v| v.as_i64().unwrap()).collect();
        assert_eq!(tag_ids, vec![100, 200, 300]);

        let tool_ids: Vec<i64> = obj["toolIds"].as_array().unwrap()
            .iter().map(|v| v.as_i64().unwrap()).collect();
        assert_eq!(tool_ids, vec![50]);

        let mv_ids: Vec<i64> = obj["modelVersionIds"].as_array().unwrap()
            .iter().map(|v| v.as_i64().unwrap()).collect();
        assert_eq!(mv_ids, vec![999, 888]);
    }

    #[test]
    fn test_scalars_to_json_with_enrichment() {
        let scalars = make_scalars(5);
        let enrichment = ResourceEnrichment {
            base_model: 3, // SDXL 1.0
            resource_poi: true,
        };

        let json = scalars_to_json(5, &scalars, Some(&enrichment), &[], &[], &[], &[]);
        let obj = json.as_object().unwrap();

        assert_eq!(obj["baseModel"], "SDXL 1.0");
        assert_eq!(obj["poi"], true); // resource_poi OR'd with image poi
    }

    #[test]
    fn test_scalars_to_json_poi_or() {
        // Image poi=true, resource_poi=false → poi=true
        let mut scalars = make_scalars(1);
        scalars.poi = true;
        let json = scalars_to_json(1, &scalars, None, &[], &[], &[], &[]);
        assert_eq!(json["poi"], true);

        // Image poi=false, resource_poi=true → poi=true
        let scalars2 = make_scalars(2);
        let enrichment = ResourceEnrichment { base_model: 0, resource_poi: true };
        let json2 = scalars_to_json(2, &scalars2, Some(&enrichment), &[], &[], &[], &[]);
        assert_eq!(json2["poi"], true);

        // Image poi=false, resource_poi=false → no poi field
        let scalars3 = make_scalars(3);
        let json3 = scalars_to_json(3, &scalars3, None, &[], &[], &[], &[]);
        assert!(json3.get("poi").is_none());
    }

    #[test]
    fn test_scalars_to_json_blocked_for() {
        let mut scalars = make_scalars(1);
        scalars.blocked_for = 1; // some blocked_for value
        let json = scalars_to_json(1, &scalars, None, &[], &[], &[], &[]);
        assert_eq!(json["blockedFor"], "blocked");
    }

    #[test]
    fn test_bitmap_reconstruction_single_chunk() {
        // Simulate the bitmap reconstruction logic for a single chunk
        let mut tag_bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();

        // Tag 100 is on slots 5 and 10
        let mut bm100 = RoaringBitmap::new();
        bm100.insert(5);
        bm100.insert(10);
        tag_bitmaps.insert(100, bm100);

        // Tag 200 is on slot 5 only
        let mut bm200 = RoaringBitmap::new();
        bm200.insert(5);
        tag_bitmaps.insert(200, bm200);

        // Tag 300 is on slot 10 only
        let mut bm300 = RoaringBitmap::new();
        bm300.insert(10);
        tag_bitmaps.insert(300, bm300);

        // Reconstruct for chunk 0..65536
        let chunk_start: u32 = 0;
        let chunk_end: u32 = FINALIZE_CHUNK_SIZE;
        let mut chunk_tags: Vec<Vec<u32>> = vec![Vec::new(); FINALIZE_CHUNK_SIZE as usize];

        for (&tag_id, bm) in &tag_bitmaps {
            for slot in bm.range(chunk_start..chunk_end) {
                chunk_tags[(slot - chunk_start) as usize].push(tag_id as u32);
            }
        }

        // Slot 5 should have tags [100, 200] (order may vary)
        let mut tags_5 = chunk_tags[5].clone();
        tags_5.sort();
        assert_eq!(tags_5, vec![100, 200]);

        // Slot 10 should have tags [100, 300] (order may vary)
        let mut tags_10 = chunk_tags[10].clone();
        tags_10.sort();
        assert_eq!(tags_10, vec![100, 300]);

        // Slot 0 should have no tags
        assert!(chunk_tags[0].is_empty());
    }

    #[test]
    fn test_bitmap_reconstruction_cross_chunk() {
        // Test that slots in different chunks are correctly handled
        let mut tag_bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();

        // Tag 100 spans two chunks
        let mut bm = RoaringBitmap::new();
        bm.insert(100);              // chunk 0
        bm.insert(FINALIZE_CHUNK_SIZE + 50); // chunk 1
        tag_bitmaps.insert(100, bm);

        // Check chunk 0
        let mut chunk0: Vec<Vec<u32>> = vec![Vec::new(); FINALIZE_CHUNK_SIZE as usize];
        for (&tag_id, bm) in &tag_bitmaps {
            for slot in bm.range(0..FINALIZE_CHUNK_SIZE) {
                chunk0[(slot) as usize].push(tag_id as u32);
            }
        }
        assert_eq!(chunk0[100], vec![100u32]);

        // Check chunk 1
        let chunk1_start = FINALIZE_CHUNK_SIZE;
        let chunk1_end = FINALIZE_CHUNK_SIZE * 2;
        let mut chunk1: Vec<Vec<u32>> = vec![Vec::new(); FINALIZE_CHUNK_SIZE as usize];
        for (&tag_id, bm) in &tag_bitmaps {
            for slot in bm.range(chunk1_start..chunk1_end) {
                chunk1[(slot - chunk1_start) as usize].push(tag_id as u32);
            }
        }
        assert_eq!(chunk1[50], vec![100u32]);
    }

    #[test]
    fn test_resource_enrichment_default() {
        let enrichment = ResourceEnrichment::default();
        assert_eq!(enrichment.base_model, 0);
        assert!(!enrichment.resource_poi);
    }
}

// ---------------------------------------------------------------------------
// ClickHouse metrics download
// ---------------------------------------------------------------------------

/// Download aggregate metrics from ClickHouse to a `<phase_name>.tsv` file.
///
/// The SQL is supplied by the caller (the `query` field of the `clickhouse`
/// dump phase in the sync config) rather than hardcoded, so ClickHouse schema
/// changes are a ConfigMap edit + reconcile, not a rebuild. The query must
/// SELECT the header columns the phase's field mappings expect
/// (e.g. `imageId, reactionCount, commentCount, collectedCount`); the output
/// format (`FORMAT TSVWithNames`) is appended here so the config query stays
/// pure SQL.
///
/// Output: `<phase_name>.tsv` in `stage_dir`.
pub async fn download_metrics_from_clickhouse(
    stage_dir: &std::path::Path,
    phase_name: &str,
    query: &str,
    ch_url: &str,
    ch_username: Option<&str>,
    ch_password: Option<&str>,
) -> Result<u64, String> {
    let done_path = stage_dir.join(format!("{phase_name}.tsv.done"));
    if done_path.exists() {
        eprintln!("{phase_name}.tsv already downloaded (found .done marker)");
        return Ok(0);
    }

    let csv_path = stage_dir.join(format!("{phase_name}.tsv"));
    eprintln!("Downloading ClickHouse metrics to {} ...", csv_path.display());

    let http = reqwest::Client::new();
    let mut file = tokio::fs::File::create(&csv_path)
        .await
        .map_err(|e| format!("create {phase_name}.tsv: {e}"))?;

    let mut row_count = 0u64;
    let mut bytes_written = 0u64;

    // Chunked mode: when the config query carries {id_lo}/{id_hi} placeholders,
    // the source view is too big to scan unbounded (~25s / CH-OOM risk), so we
    // stream it in entityId ranges — the predicate pushes down to the view PK
    // (~300ms per 1M slice). Only the FIRST chunk keeps its TSVWithNames header;
    // subsequent chunks are header-stripped so the file stays single-header.
    if query.contains("{id_lo}") && query.contains("{id_hi}") {
        const CHUNK: i64 = 5_000_000;
        // Headroom over the current max Image entityId (~135M per pipeline
        // owner). Empty ranges return instantly (predicate pushdown), so an
        // over-wide cap only costs a few no-op requests. Bump if IDs approach it.
        const MAX_ID: i64 = 200_000_000;
        let mut lo: i64 = 0;
        let mut wrote_header = false;
        while lo < MAX_ID {
            let hi = lo + CHUNK;
            let chunk_sql = query
                .replace("{id_lo}", &lo.to_string())
                .replace("{id_hi}", &hi.to_string());
            let full_query = format!(
                "{}\nFORMAT TSVWithNames",
                chunk_sql.trim().trim_end_matches(';').trim_end()
            );
            let (rows, bytes) = stream_ch_query(
                &http, ch_url, ch_username, ch_password, &full_query, &mut file, wrote_header,
            )
            .await
            .map_err(|e| format!("metrics chunk [{lo},{hi}): {e}"))?;
            wrote_header = true;
            row_count += rows;
            bytes_written += bytes;
            if lo % 50_000_000 == 0 {
                eprintln!("  metrics: scanned entityId < {hi}, {row_count} rows so far");
            }
            lo = hi;
        }
    } else {
        let full_query = format!(
            "{}\nFORMAT TSVWithNames",
            query.trim().trim_end_matches(';').trim_end()
        );
        let (rows, bytes) = stream_ch_query(
            &http, ch_url, ch_username, ch_password, &full_query, &mut file, false,
        )
        .await?;
        row_count += rows;
        bytes_written += bytes;
    }

    tokio::io::AsyncWriteExt::flush(&mut file)
        .await
        .map_err(|e| format!("flush {phase_name}.tsv: {e}"))?;
    eprintln!(
        "Downloaded {} metric rows from ClickHouse ({:.1} MB)",
        row_count,
        bytes_written as f64 / 1048576.0
    );

    std::fs::write(&done_path, format!("{row_count}"))
        .map_err(|e| format!("write .done marker: {e}"))?;

    Ok(row_count)
}

/// POST one ClickHouse query and stream its response to `file`. When
/// `skip_header` is true, the leading TSVWithNames header line is dropped
/// (so chunked downloads keep a single header). Returns (rows_written,
/// bytes_written). Streams chunk-by-chunk — never buffers the full response
/// (OOM-safe at 107M rows).
async fn stream_ch_query(
    http: &reqwest::Client,
    ch_url: &str,
    ch_username: Option<&str>,
    ch_password: Option<&str>,
    full_query: &str,
    file: &mut tokio::fs::File,
    skip_header: bool,
) -> Result<(u64, u64), String> {
    let mut req = http.post(ch_url).body(full_query.to_string());
    if let Some(username) = ch_username {
        req = req.basic_auth(username, Some(ch_password.unwrap_or("")));
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("ClickHouse request failed: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("ClickHouse returned {status}: {body}"));
    }

    let mut rows = 0u64;
    let mut bytes = 0u64;
    // While true, we're still dropping bytes of the header line (up to and
    // including the first newline), possibly across stream-chunk boundaries.
    let mut dropping_header = skip_header;
    let mut stream = resp;
    while let Some(chunk) = stream.chunk().await.map_err(|e| format!("read CH chunk: {e}"))? {
        let data: &[u8] = if dropping_header {
            match chunk.iter().position(|&b| b == b'\n') {
                Some(pos) => {
                    dropping_header = false;
                    &chunk[pos + 1..]
                }
                None => continue, // entire stream-chunk is still header
            }
        } else {
            &chunk[..]
        };
        if data.is_empty() {
            continue;
        }
        tokio::io::AsyncWriteExt::write_all(file, data)
            .await
            .map_err(|e| format!("write metrics tsv: {e}"))?;
        rows += data.iter().filter(|&&b| b == b'\n').count() as u64;
        bytes += data.len() as u64;
    }
    Ok((rows, bytes))
}
