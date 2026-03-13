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
    let arena = SlotArena::new(max_id as u32, &arena_path)
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
// COPY-based bulk loader (172x faster than range queries)
// ---------------------------------------------------------------------------

/// Run the COPY-based bulk load pipeline.
///
/// Uses `COPY (SELECT ...) TO STDOUT` for all table streams, with JOINs
/// done in PG (Image+Post, Resource+MV+Model). All 5 streams run fully
/// in parallel. After streaming, orphan bitmaps are cleaned against the
/// alive bitmap to enforce the clean bitmap invariant.
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
    let arena = SlotArena::new(max_id as u32, &arena_path)
        .map_err(|e| format!("SlotArena::new failed: {e}"))?;

    let (arena_mb, _) = arena.memory_usage();
    eprintln!("SlotArena: {} MB allocated", arena_mb / (1024 * 1024));

    // Step 3: All 5 COPY streams in parallel
    progress.set_phase(1); // streaming
    eprintln!("\n=== Streaming all tables via COPY (parallel) ===");
    let stream_start = Instant::now();

    let (image_result, tag_result, tool_result, tech_result, res_result) = tokio::try_join!(
        copy_streams::stream_images_copy(
            pool, &arena, schema,
            &filter_names, &sort_configs, &filter_set, &sort_bits,
            &progress,
        ),
        copy_streams::stream_tags_copy(
            pool, &arena, &filter_names, &sort_configs, &progress,
        ),
        copy_streams::stream_tools_copy(
            pool, &arena, &filter_names, &sort_configs, &progress,
        ),
        copy_streams::stream_techniques_copy(
            pool, &arena, &filter_names, &sort_configs, &progress,
        ),
        copy_streams::stream_resources_copy(
            pool, &arena, schema, &filter_names, &sort_configs, &progress,
        ),
    )?;

    let (image_accum, image_stats) = image_result;
    let (mut tag_accum, tag_stats) = tag_result;
    let (mut tool_accum, tool_stats) = tool_result;
    let (mut tech_accum, tech_stats) = tech_result;
    let (mut res_accum, res_stats) = res_result;

    eprintln!(
        "\nAll streams complete in {:.1}s: {} images, {} tags, {} tools, {} techniques, {} resources",
        stream_start.elapsed().as_secs_f64(),
        image_stats.rows_processed,
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

    // Cleanup arena
    if let Err(e) = arena.cleanup() {
        eprintln!("Warning: failed to cleanup arena file: {e}");
    }

    progress.set_phase(6); // done

    let total_images = image_stats.rows_processed;
    let elapsed = wall_start.elapsed();
    let rate = total_images as f64 / elapsed.as_secs_f64();
    eprintln!(
        "\nCOPY bulk load complete: {} images in {:.1}s ({:.0}/s)",
        total_images, elapsed.as_secs_f64(), rate
    );

    Ok(BulkLoadStats {
        records_loaded: total_images,
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
