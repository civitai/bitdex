//! In-process bulk loader: PG → engine bitmaps + docstore.
//!
//! Table-at-a-time pipeline:
//!   1. Download PG tables to local CSV files (resumable per-table)
//!   2. Load enrichment lookup tables (Post, ModelVersion, Model) into HashMaps
//!   3. Build filter/sort bitmaps from CSV files — NO arena
//!   4. Merge bitmap accumulators, apply to staging
//!   5. Finalize: reconstruct docs from bitmaps + stored scalars → docstore shards
//!   6. Save snapshot
//!
//! Arena-free design: multi-value fields (tagIds, toolIds, etc.) are reconstructed
//! from the filter bitmaps during finalization using 65K-block chunked iteration.
//! Only doc-only scalar fields (url, hash) and small per-image metadata are stored
//! in memory during loading, reducing peak memory from ~60GB mmap to ~10GB HashMap.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

use roaring::RoaringBitmap;
use sqlx::PgPool;

use crate::concurrent_engine::ConcurrentEngine;
use crate::loader::BitmapAccum;

use super::config::IndexDefinition;
use super::copy_queries;
use super::copy_streams;
use super::progress::LoadProgress;
use super::queries;
use super::slot_arena::SlotArena;
use super::table_streams;

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

/// Run the full table-at-a-time bulk load pipeline:
///
/// 1. Create BitdexOutbox table + triggers (so changes during load are captured)
/// 2. Allocate SlotArena for document staging
/// 3. Stream Image+Post table → scalar slots + filter/sort bitmaps
/// 4. Stream enrichment tables (tags, tools, techniques, resources) in parallel
/// 5. Merge bitmap accumulators, apply to engine staging
/// 6. Finalize: slots → msgpack → zstd → docstore shards
/// 7. Save snapshot (bitmaps + cursors)
pub async fn run_bulk_load(
    pool: &PgPool,
    engine: &ConcurrentEngine,
    index_def: &IndexDefinition,
    batch_size: i64,
    progress: Arc<AtomicU64>,
) -> Result<BulkLoadStats, String> {
    let schema = &index_def.data_schema;
    let config = engine.config();
    let wall_start = Instant::now();

    // Step 1: Create outbox table + triggers
    eprintln!("Setting up BitdexOutbox table and triggers...");
    queries::run_setup(pool)
        .await
        .map_err(|e| format!("setup failed: {e}"))?;
    eprintln!("BitdexOutbox setup complete.");

    // Step 2: Get max image ID and allocate SlotArena
    let max_id = queries::get_max_image_id(pool)
        .await
        .map_err(|e| format!("get_max_image_id failed: {e}"))?;
    eprintln!("Max image ID: {max_id}");

    if max_id == 0 {
        return Ok(BulkLoadStats {
            records_loaded: 0,
            errors: 0,
            elapsed: std::time::Duration::ZERO,
        });
    }

    // Pre-build field lookup tables for bitmap extraction
    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config
        .sort_fields
        .iter()
        .map(|f| (f.name.clone(), f.bits))
        .collect();
    let filter_set: HashSet<String> = filter_names.iter().cloned().collect();
    let sort_bits: HashMap<String, u8> = sort_configs.iter().cloned().collect();

    // Allocate SlotArena
    let storage_dir = config.storage.bitmap_path.as_ref()
        .map(|p| p.parent().unwrap_or(p).to_path_buf())
        .unwrap_or_else(|| std::env::temp_dir());
    let arena_path = storage_dir.join("slot_arena.bin");
    // Add safety margin for new inserts that arrive during the load.
    // Production inserts ~1K images/min; 100K headroom covers ~100 min of loading.
    let arena_max = (max_id as u32).saturating_add(100_000);
    let arena = SlotArena::new(arena_max, &arena_path)
        .map_err(|e| format!("SlotArena::new failed: {e}"))?;

    let (arena_mb, _) = arena.memory_usage();
    eprintln!("SlotArena: {} MB allocated", arena_mb / (1024 * 1024));

    // Step 3: Stream Image+Post (must run first — sets alive bitmap + scalars)
    eprintln!("\n=== Streaming Image+Post table ===");
    let (image_accum, image_stats) = table_streams::stream_images(
        pool, &arena, schema,
        &filter_names, &sort_configs, &filter_set, &sort_bits,
        max_id, batch_size, &progress,
    ).await?;

    let total_images = image_stats.rows_processed;
    eprintln!("Images: {} in {:.1}s", total_images, image_stats.elapsed.as_secs_f64());

    // Step 4: Stream enrichment tables in parallel
    eprintln!("\n=== Streaming enrichment tables (parallel) ===");
    let enrich_start = Instant::now();

    let (tag_result, tool_result, tech_result, res_result) = tokio::try_join!(
        table_streams::stream_tags(
            pool, &arena, &filter_names, &sort_configs, batch_size,
        ),
        table_streams::stream_tools(
            pool, &arena, &filter_names, &sort_configs, batch_size,
        ),
        table_streams::stream_techniques(
            pool, &arena, &filter_names, &sort_configs, batch_size,
        ),
        table_streams::stream_resources(
            pool, &arena, schema, &filter_names, &sort_configs, max_id, batch_size,
        ),
    )?;

    let (tag_accum, tag_stats) = tag_result;
    let (tool_accum, tool_stats) = tool_result;
    let (tech_accum, tech_stats) = tech_result;
    let (res_accum, res_stats) = res_result;

    eprintln!(
        "Enrichment complete in {:.1}s: {} tags, {} tools, {} techniques, {} resources",
        enrich_start.elapsed().as_secs_f64(),
        tag_stats.rows_processed,
        tool_stats.rows_processed,
        tech_stats.rows_processed,
        res_stats.rows_processed,
    );

    // Report overflow
    let (_, overflow_bytes) = arena.memory_usage();
    if overflow_bytes > 0 {
        eprintln!("SlotArena overflow: {} KB", overflow_bytes / 1024);
    }

    // Step 5: Merge all bitmap accumulators and apply to staging
    eprintln!("\n=== Applying bitmaps to staging ===");
    let merge_start = Instant::now();

    let merged = image_accum
        .merge(tag_accum)
        .merge(tool_accum)
        .merge(tech_accum)
        .merge(res_accum);

    // Keep alive bitmap before merging into staging (we need it for finalization)
    let alive_bitmap = merged.alive.clone();

    let mut staging = engine.clone_staging();
    ConcurrentEngine::apply_bitmap_maps(
        &mut staging,
        merged.filter_maps,
        merged.sort_maps,
        merged.alive,
    );
    engine.publish_staging(staging);

    eprintln!(
        "Bitmaps applied in {:.1}s",
        merge_start.elapsed().as_secs_f64()
    );

    // Step 6: Finalize slots → docstore
    eprintln!("\n=== Finalizing slots to docstore ===");
    let finalize_start = Instant::now();

    // Prepare BulkWriter for docstore encoding
    let all_field_names: Vec<String> = schema
        .fields
        .iter()
        .map(|f| f.target.clone())
        .chain(std::iter::once("id".to_string()))
        .collect();
    engine.set_docstore_defaults(schema);
    let bulk_writer = engine
        .prepare_bulk_writer(&all_field_names)
        .map_err(|e| format!("prepare_bulk_writer: {e}"))?;

    let (docs_finalized, bytes_finalized) = arena
        .finalize_to_docstore(&bulk_writer, schema, &alive_bitmap)
        .map_err(|e| format!("finalize_to_docstore: {e}"))?;

    eprintln!(
        "Finalized {} docs ({} MB) in {:.1}s ({:.0}/s)",
        docs_finalized,
        bytes_finalized / (1024 * 1024),
        finalize_start.elapsed().as_secs_f64(),
        docs_finalized as f64 / finalize_start.elapsed().as_secs_f64().max(0.001)
    );

    // Step 7: Save snapshot
    eprintln!("\nSaving snapshot...");
    engine
        .save_snapshot()
        .map_err(|e| format!("save_snapshot failed: {e}"))?;
    eprintln!("Snapshot saved.");

    // Cleanup arena
    if let Err(e) = arena.cleanup() {
        eprintln!("Warning: failed to cleanup arena file: {e}");
    }

    let elapsed = wall_start.elapsed();
    let rate = total_images as f64 / elapsed.as_secs_f64();
    eprintln!(
        "\nBulk load complete: {} images in {:.1}s ({:.0}/s)",
        total_images, elapsed.as_secs_f64(), rate
    );

    Ok(BulkLoadStats {
        records_loaded: total_images,
        errors: 0,
        elapsed,
    })
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
];

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

// ---------------------------------------------------------------------------
// Phase 2: Build bitmaps from local CSV files
// ---------------------------------------------------------------------------

/// Run the two-phase arena-free bulk load pipeline.
///
/// Phase 1: Download all tables from PG to CSV files on the PVC (resumable).
/// Phase 2: Build bitmaps from local CSV files (no PG dependency, no arena).
///
/// Multi-value fields (tagIds, toolIds, etc.) are reconstructed from filter
/// bitmaps during finalization, eliminating the 60GB memory-mapped SlotArena.
/// Only compact per-image scalars (~80 bytes/image) are stored in a HashMap.
pub async fn run_bulk_load_copy(
    pool: &PgPool,
    engine: &ConcurrentEngine,
    index_def: &IndexDefinition,
    progress: Arc<LoadProgress>,
) -> Result<BulkLoadStats, String> {
    let schema = &index_def.data_schema;
    let config = engine.config();
    let wall_start = Instant::now();

    // Step 1: Create outbox table + triggers
    eprintln!("Setting up BitdexOutbox table and triggers...");
    queries::run_setup(pool)
        .await
        .map_err(|e| format!("setup failed: {e}"))?;
    eprintln!("BitdexOutbox setup complete.");

    // Step 1b: Download all tables to local CSV files (resumable per-table)
    let storage_dir = config
        .storage
        .bitmap_path
        .as_ref()
        .map(|p| p.parent().unwrap_or(p).to_path_buf())
        .unwrap_or_else(|| std::env::temp_dir());
    let stage_dir = storage_dir.join("load_stage");
    download_all_tables(pool, &stage_dir).await?;

    // Step 2: Get max image ID (for progress reporting)
    let max_id = queries::get_max_image_id(pool)
        .await
        .map_err(|e| format!("get_max_image_id failed: {e}"))?;
    eprintln!("Max image ID: {max_id}");

    if max_id == 0 {
        return Ok(BulkLoadStats {
            records_loaded: 0,
            errors: 0,
            elapsed: std::time::Duration::ZERO,
        });
    }

    // Pre-build field lookup tables for bitmap extraction
    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config
        .sort_fields
        .iter()
        .map(|f| (f.name.clone(), f.bits))
        .collect();
    let filter_set: HashSet<String> = filter_names.iter().cloned().collect();
    let sort_bits: HashMap<String, u8> = sort_configs.iter().cloned().collect();

    // Arena-free: use compact HashMap instead of 60GB mmap
    let estimated_capacity = (max_id as usize).min(128_000_000);
    let mut image_scalars: HashMap<u32, ImageScalars> = HashMap::with_capacity(estimated_capacity);
    let mut resource_enrichments: HashMap<u32, ResourceEnrichment> = HashMap::new();

    eprintln!(
        "Arena-free mode: estimated {} slots, ~{} MB for scalar storage",
        estimated_capacity,
        estimated_capacity * 80 / (1024 * 1024)
    );

    // Step 2b: Load enrichment lookup tables from local CSV files.
    eprintln!("\n=== Phase 2: Building bitmaps from local CSV files (arena-free) ===");
    eprintln!("Loading enrichment tables...");
    let enrich_start = Instant::now();

    use super::copy_queries::{
        parse_post_row, parse_model_version_row, parse_model_row,
        parse_image_row, parse_tag_row, parse_tool_row, parse_technique_row,
        parse_resource_row,
    };
    use std::io::BufRead;

    // Post → HashMap<post_id, (published_at_secs, availability, model_version_id)>
    let mut post_map: HashMap<i64, (Option<i64>, String, Option<i64>)> = HashMap::new();
    let post_file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("posts.csv"))
            .map_err(|e| format!("open posts.csv: {e}"))?
    );
    for line in post_file.split(b'\n') {
        let line = line.map_err(|e| format!("read posts.csv: {e}"))?;
        if let Some(row) = parse_post_row(&line) {
            post_map.insert(row.id, (row.published_at_secs, row.availability, row.model_version_id));
        }
    }
    eprintln!("  Posts: {} rows in {:.1}s", post_map.len(), enrich_start.elapsed().as_secs_f64());

    // ModelVersion → HashMap<mv_id, (base_model, model_id)>
    let mv_start = Instant::now();
    let mut mv_map: HashMap<i64, (Option<String>, i64)> = HashMap::new();
    let mv_file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("model_versions.csv"))
            .map_err(|e| format!("open model_versions.csv: {e}"))?
    );
    for line in mv_file.split(b'\n') {
        let line = line.map_err(|e| format!("read model_versions.csv: {e}"))?;
        if let Some(row) = parse_model_version_row(&line) {
            mv_map.insert(row.id, (row.base_model, row.model_id));
        }
    }
    eprintln!("  ModelVersions: {} rows in {:.1}s", mv_map.len(), mv_start.elapsed().as_secs_f64());

    // Model → HashMap<model_id, (poi, model_type)>
    let model_start = Instant::now();
    let mut model_map: HashMap<i64, (bool, String)> = HashMap::new();
    let model_file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("models.csv"))
            .map_err(|e| format!("open models.csv: {e}"))?
    );
    for line in model_file.split(b'\n') {
        let line = line.map_err(|e| format!("read models.csv: {e}"))?;
        if let Some(row) = parse_model_row(&line) {
            model_map.insert(row.id, (row.poi, row.model_type));
        }
    }
    eprintln!("  Models: {} rows in {:.1}s", model_map.len(), model_start.elapsed().as_secs_f64());
    eprintln!("Enrichment tables loaded in {:.1}s", enrich_start.elapsed().as_secs_f64());

    // Step 3: Build bitmaps from local CSV files (no PG dependency, NO arena).
    progress.set_phase(1); // streaming/building

    eprintln!("\n=== Building bitmaps from local CSV files (arena-free) ===");
    let build_start = Instant::now();

    // Build images bitmaps + store compact scalars (reads images.csv, enriches from post_map)
    eprintln!("Building images...");
    let img_start = Instant::now();
    let mut image_accum = BitmapAccum::new(&filter_names, &sort_configs);
    let img_file = std::io::BufReader::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::open(stage_dir.join("images.csv"))
            .map_err(|e| format!("open images.csv: {e}"))?,
    );
    let mut img_total = 0u64;
    for line in img_file.split(b'\n') {
        let line = line.map_err(|e| format!("read images.csv: {e}"))?;
        if line.is_empty() { continue; }
        let mut row = match parse_image_row(&line) {
            Some(r) => r,
            None => continue,
        };

        // Enrich from Post lookup
        if let Some(post_id) = row.post_id {
            if let Some((pub_secs, avail, mv_id)) = post_map.get(&post_id) {
                row.published_at_secs = *pub_secs;
                row.availability = avail.clone();
                row.posted_to_id = *mv_id;
            }
        }

        let slot = row.id as u32;
        let sort_at = row.sort_at_secs();
        let published_at_ms = (row.published_at_secs.unwrap_or(0) * 1000) as u64;

        // Extract strings before borrowing row for flags
        let url_box = row.url.take().map(|s| s.into_boxed_str());
        let hash_box = row.hash.take().map(|s| s.into_boxed_str());

        // Store scalars + doc_only strings (Box<str> saves capacity overhead vs String)
        image_scalars.insert(slot, ImageScalars {
            url: url_box,
            hash: hash_box,
            nsfw_level: row.nsfw_level as u8,
            user_id: row.user_id as u64,
            image_type: super::slot_arena::encode_image_type(Some(&row.image_type)),
            sort_at,
            poi: row.poi(),
            minor: row.minor(),
            has_meta: row.has_meta(),
            on_site: row.on_site(),
            post_id: row.post_id.unwrap_or(0) as u64,
            posted_to_id: row.posted_to_id.unwrap_or(0) as u64,
            availability: super::slot_arena::encode_availability(Some(row.availability.as_str())),
            blocked_for: super::slot_arena::encode_blocked_for(row.blocked_for.as_deref()),
            published_at_ms,
        });

        image_accum.alive.insert(slot);

        // Build filter/sort bitmaps directly (same logic as copy_streams)
        copy_streams::build_image_bitmaps(
            &row, slot, sort_at, schema,
            &filter_set, &sort_bits,
            &mut image_accum.filter_maps, &mut image_accum.sort_maps,
        );

        img_total += 1;
        if img_total % 1_000_000 == 0 {
            progress.image_rows.store(img_total, std::sync::atomic::Ordering::Release);
            let elapsed = img_start.elapsed().as_secs_f64();
            eprintln!("  images: {} rows ({:.0}/s, {:.1}s)", img_total, img_total as f64 / elapsed, elapsed);
        }
    }
    progress.image_rows.store(img_total, std::sync::atomic::Ordering::Release);
    eprintln!("  images: complete — {} rows in {:.1}s ({:.0}/s)",
        img_total, img_start.elapsed().as_secs_f64(),
        img_total as f64 / img_start.elapsed().as_secs_f64().max(0.001));

    // Build tags bitmaps (NO arena writes — tags reconstructed from bitmaps at finalization)
    eprintln!("Building tags...");
    let tag_start = Instant::now();
    let mut tag_accum = BitmapAccum::new(&filter_names, &sort_configs);
    let tag_file = std::io::BufReader::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::open(stage_dir.join("tags.csv"))
            .map_err(|e| format!("open tags.csv: {e}"))?,
    );
    let mut tag_total = 0u64;
    for line in tag_file.split(b'\n') {
        let line = line.map_err(|e| format!("read tags.csv: {e}"))?;
        if line.is_empty() { continue; }
        if let Some((tag_id, image_id)) = parse_tag_row(&line) {
            let slot = image_id as u32;
            // NO arena.write_tags — reconstructed from bitmaps during finalization
            if let Some(fm) = tag_accum.filter_maps.get_mut("tagIds") {
                fm.entry(tag_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
            tag_total += 1;
            if tag_total % 10_000_000 == 0 {
                progress.tag_rows.store(tag_total, std::sync::atomic::Ordering::Release);
                let elapsed = tag_start.elapsed().as_secs_f64();
                eprintln!("  tags: {} rows ({:.0}/s, {:.1}s)", tag_total, tag_total as f64 / elapsed, elapsed);
            }
        }
    }
    progress.tag_rows.store(tag_total, std::sync::atomic::Ordering::Release);
    eprintln!("  tags: complete — {} rows in {:.1}s ({:.0}/s)",
        tag_total, tag_start.elapsed().as_secs_f64(),
        tag_total as f64 / tag_start.elapsed().as_secs_f64().max(0.001));

    // Build tools bitmaps (NO arena writes)
    let tool_start = Instant::now();
    let mut tool_accum = BitmapAccum::new(&filter_names, &sort_configs);
    let tool_file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("tools.csv")).map_err(|e| format!("open tools.csv: {e}"))?,
    );
    let mut tool_total = 0u64;
    for line in tool_file.split(b'\n') {
        let line = line.map_err(|e| format!("read tools.csv: {e}"))?;
        if line.is_empty() { continue; }
        if let Some((tool_id, image_id)) = parse_tool_row(&line) {
            let slot = image_id as u32;
            // NO arena.write_tools
            if let Some(fm) = tool_accum.filter_maps.get_mut("toolIds") {
                fm.entry(tool_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
            tool_total += 1;
        }
    }
    eprintln!("  tools: {} rows in {:.1}s", tool_total, tool_start.elapsed().as_secs_f64());

    // Build techniques bitmaps (NO arena writes)
    let tech_start = Instant::now();
    let mut tech_accum = BitmapAccum::new(&filter_names, &sort_configs);
    let tech_file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("techniques.csv")).map_err(|e| format!("open techniques.csv: {e}"))?,
    );
    let mut tech_total = 0u64;
    for line in tech_file.split(b'\n') {
        let line = line.map_err(|e| format!("read techniques.csv: {e}"))?;
        if line.is_empty() { continue; }
        if let Some((tech_id, image_id)) = parse_technique_row(&line) {
            let slot = image_id as u32;
            // NO arena.write_techniques
            if let Some(fm) = tech_accum.filter_maps.get_mut("techniqueIds") {
                fm.entry(tech_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
            tech_total += 1;
        }
    }
    eprintln!("  techniques: {} rows in {:.1}s", tech_total, tech_start.elapsed().as_secs_f64());

    // Build resources bitmaps (enriched from mv_map + model_map, NO arena writes)
    eprintln!("Building resources...");
    let res_start = Instant::now();
    let mut res_accum = BitmapAccum::new(&filter_names, &sort_configs);
    let res_file = std::io::BufReader::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::open(stage_dir.join("resources.csv")).map_err(|e| format!("open resources.csv: {e}"))?,
    );
    let mut res_total = 0u64;
    for line in res_file.split(b'\n') {
        let line = line.map_err(|e| format!("read resources.csv: {e}"))?;
        if line.is_empty() { continue; }
        let row = match parse_resource_row(&line) {
            Some(r) => r,
            None => continue,
        };

        let slot = row.image_id as u32;
        let mv_id = row.model_version_id as u32;

        // NO arena.write_model_version_ids — reconstructed from modelVersionIds bitmap

        // MV filter bitmap (both detected and manual go into same bitmap)
        if let Some(fm) = res_accum.filter_maps.get_mut("modelVersionIds") {
            fm.entry(mv_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
        }

        // Enrich from MV/Model lookups for baseModel + POI
        if let Some((mv_base_model, model_id)) = mv_map.get(&row.model_version_id) {
            if let Some((poi, model_type)) = model_map.get(model_id) {
                if model_type == "Checkpoint" {
                    if let Some(ref bm_str) = mv_base_model {
                        // Store base_model in resource enrichment (replaces arena.write_base_model)
                        let enrichment = resource_enrichments.entry(slot).or_default();
                        enrichment.base_model = super::slot_arena::encode_base_model(Some(bm_str));
                        // baseModel filter bitmap
                        if !bm_str.is_empty() {
                            let key = schema.fields.iter()
                                .find(|f| f.target == "baseModel")
                                .and_then(|f| f.string_map.as_ref())
                                .and_then(|map| {
                                    let lower = bm_str.to_lowercase();
                                    map.get(&lower).or_else(|| map.get(bm_str.as_str())).copied()
                                })
                                .unwrap_or(0) as u64;
                            if let Some(fm) = res_accum.filter_maps.get_mut("baseModel") {
                                fm.entry(key).or_insert_with(RoaringBitmap::new).insert(slot);
                            }
                        }
                    }
                }
                if *poi {
                    // Store resource_poi in enrichment (replaces arena.set_resource_poi)
                    resource_enrichments.entry(slot).or_default().resource_poi = true;
                }
            }
        }

        res_total += 1;
        if res_total % 1_000_000 == 0 {
            progress.resource_rows.store(res_total, std::sync::atomic::Ordering::Release);
        }
    }
    progress.resource_rows.store(res_total, std::sync::atomic::Ordering::Release);
    eprintln!("  resources: {} rows in {:.1}s", res_total, res_start.elapsed().as_secs_f64());

    eprintln!(
        "\nPhase 2 complete in {:.1}s: {} images, {} tags, {} tools, {} techniques, {} resources",
        build_start.elapsed().as_secs_f64(),
        img_total, tag_total, tool_total, tech_total, res_total,
    );

    // Report memory usage of scalar storage
    let scalar_mem = image_scalars.len() * std::mem::size_of::<ImageScalars>();
    let enrich_mem = resource_enrichments.len() * std::mem::size_of::<ResourceEnrichment>();
    eprintln!(
        "Scalar storage: {} MB ({} images), Resource enrichments: {} MB ({} slots)",
        scalar_mem / (1024 * 1024), image_scalars.len(),
        enrich_mem / (1024 * 1024), resource_enrichments.len(),
    );

    // Step 4: Alive cleanup — AND all enrichment bitmaps against alive
    // This strips orphan bits from tags/tools/techniques/resources that
    // reference non-existent images (TagsOnImageNew has no FK constraint).
    progress.set_phase(2); // cleanup
    eprintln!("\n=== Cleaning orphan bitmaps against alive ===");
    let cleanup_start = Instant::now();
    let alive = &image_accum.alive;

    let tag_cleaned = cleanup_orphan_bitmaps(&mut tag_accum, alive);
    let tool_cleaned = cleanup_orphan_bitmaps(&mut tool_accum, alive);
    let tech_cleaned = cleanup_orphan_bitmaps(&mut tech_accum, alive);
    let res_cleaned = cleanup_orphan_bitmaps(&mut res_accum, alive);

    eprintln!(
        "Cleanup complete in {:.1}s: {} tag, {} tool, {} tech, {} resource bitmaps cleaned",
        cleanup_start.elapsed().as_secs_f64(),
        tag_cleaned, tool_cleaned, tech_cleaned, res_cleaned,
    );

    // Step 5: Merge all bitmap accumulators and apply to staging.
    // We need to keep references to the merged filter bitmaps for docstore
    // reconstruction, so we clone the multi-value filter maps before merging.
    progress.set_phase(3); // applying
    eprintln!("\n=== Applying bitmaps to staging ===");
    let merge_start = Instant::now();

    let mut merged = image_accum
        .merge(tag_accum)
        .merge(tool_accum)
        .merge(tech_accum)
        .merge(res_accum);

    let alive_bitmap = merged.alive.clone();

    // Extract multi-value filter maps by REMOVING (not cloning) from merged.
    // This avoids duplicating 6-19GB of roaring bitmaps in memory.
    // We hold references to these for finalization while applying the rest to staging.
    let tag_bitmaps = merged.filter_maps.remove("tagIds").unwrap_or_default();
    let tool_bitmaps = merged.filter_maps.remove("toolIds").unwrap_or_default();
    let technique_bitmaps = merged.filter_maps.remove("techniqueIds").unwrap_or_default();
    let mv_bitmaps = merged.filter_maps.remove("modelVersionIds").unwrap_or_default();

    // Apply remaining bitmaps (scalar filters + sorts) to engine staging.
    // Multi-value bitmaps applied separately below after finalization.
    let mut staging = engine.clone_staging();
    ConcurrentEngine::apply_bitmap_maps(
        &mut staging,
        merged.filter_maps,
        merged.sort_maps,
        merged.alive,
    );
    engine.publish_staging(staging);

    eprintln!(
        "Bitmaps applied in {:.1}s",
        merge_start.elapsed().as_secs_f64()
    );

    // Step 6: Finalize — reconstruct docs from bitmaps + stored scalars → docstore
    progress.set_phase(4); // finalizing
    eprintln!("\n=== Finalizing slots to docstore (arena-free bitmap reconstruction) ===");
    let finalize_start = Instant::now();

    let all_field_names: Vec<String> = schema
        .fields
        .iter()
        .map(|f| f.target.clone())
        .chain(std::iter::once("id".to_string()))
        .collect();
    engine.set_docstore_defaults(schema);
    let bulk_writer = engine
        .prepare_bulk_writer(&all_field_names)
        .map_err(|e| format!("prepare_bulk_writer: {e}"))?;

    let (docs_finalized, bytes_finalized) = finalize_from_bitmaps(
        &bulk_writer,
        schema,
        &alive_bitmap,
        &image_scalars,
        &resource_enrichments,
        &tag_bitmaps,
        &tool_bitmaps,
        &technique_bitmaps,
        &mv_bitmaps,
    ).map_err(|e| format!("finalize_from_bitmaps: {e}"))?;

    eprintln!(
        "Finalized {} docs ({} MB) in {:.1}s ({:.0}/s)",
        docs_finalized,
        bytes_finalized / (1024 * 1024),
        finalize_start.elapsed().as_secs_f64(),
        docs_finalized as f64 / finalize_start.elapsed().as_secs_f64().max(0.001)
    );

    // Apply multi-value bitmaps to engine (removed earlier to avoid clone).
    // These are needed for query filtering.
    let mut mv_filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
    mv_filter_maps.insert("tagIds".to_string(), tag_bitmaps);
    mv_filter_maps.insert("toolIds".to_string(), tool_bitmaps);
    mv_filter_maps.insert("techniqueIds".to_string(), technique_bitmaps);
    mv_filter_maps.insert("modelVersionIds".to_string(), mv_bitmaps);
    let mut staging = engine.clone_staging();
    ConcurrentEngine::apply_bitmap_maps(
        &mut staging,
        mv_filter_maps,
        HashMap::new(), // no sort maps
        RoaringBitmap::new(), // alive already applied
    );
    engine.publish_staging(staging);
    eprintln!("Multi-value bitmaps applied to engine.");

    // Step 7: Save snapshot
    progress.set_phase(5); // saving
    eprintln!("\nSaving snapshot...");
    engine
        .save_snapshot()
        .map_err(|e| format!("save_snapshot failed: {e}"))?;
    eprintln!("Snapshot saved.");

    // Clean up staging CSV files (load succeeded, no longer needed)
    if let Err(e) = std::fs::remove_dir_all(&stage_dir) {
        eprintln!("Warning: failed to cleanup staging files: {e}");
    }

    progress.set_phase(6); // done

    let elapsed = wall_start.elapsed();
    let rate = img_total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "\nBulk load complete: {} images in {:.1}s ({:.0}/s)",
        img_total, elapsed.as_secs_f64(), rate
    );

    Ok(BulkLoadStats {
        records_loaded: img_total,
        errors: 0,
        elapsed,
    })
}

// ---------------------------------------------------------------------------
// Arena-free docstore finalization
// ---------------------------------------------------------------------------

/// Block size for chunked bitmap reconstruction.
/// Aligned with roaring bitmap container boundaries (65,536 = 2^16).
const FINALIZE_CHUNK_SIZE: u32 = 65_536;

/// Finalize alive slots to the docstore by reconstructing multi-value fields
/// from filter bitmaps and combining with stored scalars.
///
/// Processes alive slots in 65K-block chunks aligned to roaring container
/// boundaries for efficient `bitmap.range()` iteration.
fn finalize_from_bitmaps(
    bulk_writer: &crate::docstore::BulkWriter,
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
        "sortAtUnix": s.sort_at as i64 * 1000,
        "publishedAtUnix": s.published_at_ms as i64,
        "existedAtUnix": 0i64,
    });

    if let Some(obj) = doc.as_object_mut() {
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
