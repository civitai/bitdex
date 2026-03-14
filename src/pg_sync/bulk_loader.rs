//! In-process bulk loader: PG → engine bitmaps + docstore.
//!
//! Table-at-a-time pipeline:
//!   1. Create SlotArena (mmap'd fixed-size slots)
//!   2. Stream Image+Post → scalars + filter/sort bitmaps
//!   3. Stream enrichment tables in parallel → enrichment bitmaps + slot writes
//!   4. Merge bitmap accumulators, apply to staging
//!   5. Finalize slots → docstore shards (parallel encode + compress)
//!   6. Save snapshot
//!
//! Key optimization: tags stream ordered by tagId for 360x better bitmap insertion.

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
async fn download_all_tables(
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

/// Run the two-phase bulk load pipeline.
///
/// Phase 1: Download all tables from PG to CSV files on the PVC (resumable).
/// Phase 2: Build bitmaps from local CSV files (no PG dependency).
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
    let storage_dir = config
        .storage
        .bitmap_path
        .as_ref()
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

    // Step 2b: Load enrichment lookup tables from local CSV files.
    eprintln!("\n=== Phase 2: Building bitmaps from local CSV files ===");
    eprintln!("Loading enrichment tables...");
    let enrich_start = Instant::now();

    use super::copy_queries::{
        parse_post_row, parse_model_version_row, parse_model_row,
        parse_image_row, parse_tag_row, parse_tool_row, parse_technique_row,
        parse_resource_row, CopyResourceRow,
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

    // Step 3: Build bitmaps from local CSV files (no PG dependency).
    progress.set_phase(1); // streaming/building

    eprintln!("\n=== Building bitmaps from local CSV files ===");
    let build_start = Instant::now();

    // Build images bitmaps (reads images.csv, enriches from post_map)
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

        // Write scalars to arena
        arena.write_scalars(
            slot, row.id as u64, row.nsfw_level as u8, row.user_id as u64,
            super::slot_arena::encode_image_type(Some(&row.image_type)),
            sort_at, row.poi(), row.minor(),
            row.url.as_deref().map(|s| s.as_bytes()),
            row.hash.as_deref().map(|s| s.as_bytes()),
            row.has_meta(), row.on_site(),
            row.post_id.unwrap_or(0) as u64,
            row.posted_to_id.unwrap_or(0) as u64,
            super::slot_arena::encode_availability(Some(row.availability.as_str())),
            super::slot_arena::encode_blocked_for(row.blocked_for.as_deref()),
            published_at_ms,
        );

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

    // Build tags bitmaps
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
            arena.write_tags(slot, &[tag_id as u32]);
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

    // Build tools bitmaps
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
            arena.write_tools(slot, &[tool_id as u32]);
            if let Some(fm) = tool_accum.filter_maps.get_mut("toolIds") {
                fm.entry(tool_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
            tool_total += 1;
        }
    }
    eprintln!("  tools: {} rows in {:.1}s", tool_total, tool_start.elapsed().as_secs_f64());

    // Build techniques bitmaps
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
            arena.write_techniques(slot, &[tech_id as u32]);
            if let Some(fm) = tech_accum.filter_maps.get_mut("techniqueIds") {
                fm.entry(tech_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
            tech_total += 1;
        }
    }
    eprintln!("  techniques: {} rows in {:.1}s", tech_total, tech_start.elapsed().as_secs_f64());

    // Build resources bitmaps (enriched from mv_map + model_map)
    eprintln!("Building resources...");
    let res_start = Instant::now();
    let mut res_accum = BitmapAccum::new(&filter_names, &sort_configs);
    let res_file = std::io::BufReader::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::open(stage_dir.join("resources.csv")).map_err(|e| format!("open resources.csv: {e}"))?,
    );
    // Group resources by imageId for flush_resources logic
    let mut current_image_id: Option<i64> = None;
    let mut pending_resources: Vec<CopyResourceRow> = Vec::with_capacity(16);
    let mut res_total = 0u64;
    for line in res_file.split(b'\n') {
        let line = line.map_err(|e| format!("read resources.csv: {e}"))?;
        if line.is_empty() { continue; }
        let row = match parse_resource_row(&line) {
            Some(r) => r,
            None => continue,
        };

        // Resources may arrive unordered — use per-row bitmap inserts
        let slot = row.image_id as u32;
        let mv_id = row.model_version_id as u32;

        // Write MV to arena
        if row.detected {
            arena.write_model_version_ids(slot, &[mv_id]);
        } else {
            arena.write_model_version_ids_manual(slot, &[mv_id]);
        }

        // MV filter bitmap
        if let Some(fm) = res_accum.filter_maps.get_mut("modelVersionIds") {
            fm.entry(mv_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
        }

        // Enrich from MV/Model lookups for baseModel + POI
        if let Some((mv_base_model, model_id)) = mv_map.get(&row.model_version_id) {
            if let Some((poi, model_type)) = model_map.get(model_id) {
                if model_type == "Checkpoint" {
                    if let Some(ref bm_str) = mv_base_model {
                        arena.write_base_model(slot, super::slot_arena::encode_base_model(Some(bm_str)));
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
                    arena.set_resource_poi(slot);
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

    // Report overflow
    let (_, overflow_bytes) = arena.memory_usage();
    if overflow_bytes > 0 {
        eprintln!("SlotArena overflow: {} KB", overflow_bytes / 1024);
    }

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

    // Step 5: Merge all bitmap accumulators and apply to staging
    progress.set_phase(3); // applying
    eprintln!("\n=== Applying bitmaps to staging ===");
    let merge_start = Instant::now();

    let merged = image_accum
        .merge(tag_accum)
        .merge(tool_accum)
        .merge(tech_accum)
        .merge(res_accum);

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
    progress.set_phase(4); // finalizing
    eprintln!("\n=== Finalizing slots to docstore ===");
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
    progress.set_phase(5); // saving
    eprintln!("\nSaving snapshot...");
    engine
        .save_snapshot()
        .map_err(|e| format!("save_snapshot failed: {e}"))?;
    eprintln!("Snapshot saved.");

    // Cleanup arena + checkpoints (load succeeded, no longer needed)
    if let Err(e) = arena.cleanup() {
        eprintln!("Warning: failed to cleanup arena file: {e}");
    }
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
