//! In-process bulk loader: PG → engine bitmaps + docstore.
//!
//! Pipeline:
//!   PG range queries → assemble_document → rayon fold+reduce (extract_bitmaps + encode)
//!   → apply_bitmap_maps to staging → BulkWriter to docstore → save_snapshot
//!
//! Reuses the fused bitmap pipeline from loader.rs.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use rayon::prelude::*;
use sqlx::PgPool;

use crate::concurrent_engine::ConcurrentEngine;
use crate::loader::{extract_bitmaps, BitmapAccum};

use super::config::IndexDefinition;
use super::queries;
use super::row_assembler::{assemble_batch, EnrichmentData};

/// Statistics from a completed bulk load.
#[derive(Debug)]
pub struct BulkLoadStats {
    pub records_loaded: u64,
    pub errors: u64,
    pub elapsed: std::time::Duration,
}

/// Run the full bulk load pipeline:
/// 1. Create BitdexOutbox table + triggers (so changes during load are captured)
/// 2. Query PG in range batches, assemble documents, build bitmaps
/// 3. Save snapshot
pub async fn run_bulk_load(
    pool: &PgPool,
    engine: &ConcurrentEngine,
    index_def: &IndexDefinition,
    batch_size: i64,
    progress: Arc<AtomicU64>,
) -> Result<BulkLoadStats, String> {
    let schema = &index_def.data_schema;
    let config = engine.config();

    // Step 1: Create outbox table + triggers
    eprintln!("Setting up BitdexOutbox table and triggers...");
    queries::run_setup(pool)
        .await
        .map_err(|e| format!("setup failed: {e}"))?;
    eprintln!("BitdexOutbox setup complete.");

    // Step 2: Get max image ID for range iteration
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

    // Pre-build field lookup tables for bitmap extraction (same as loader.rs)
    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config
        .sort_fields
        .iter()
        .map(|f| (f.name.clone(), f.bits))
        .collect();
    let filter_set: HashSet<String> = filter_names.iter().cloned().collect();
    let sort_bits: HashMap<String, u8> = sort_configs.iter().cloned().collect();

    // Prepare BulkWriter for docstore encoding
    let all_field_names: Vec<String> = schema
        .fields
        .iter()
        .map(|f| f.target.clone())
        .chain(std::iter::once("id".to_string()))
        .collect();
    let bulk_writer = Arc::new(
        engine
            .prepare_bulk_writer(&all_field_names)
            .map_err(|e| format!("prepare_bulk_writer: {e}"))?,
    );

    // Clone staging for batch application
    let mut staging = engine.clone_staging();
    let wall_start = Instant::now();
    let mut total_loaded: u64 = 0;
    let mut total_errors: u64 = 0;
    let mut batch_num: u64 = 0;
    let mut ds_handles: Vec<thread::JoinHandle<()>> = Vec::new();

    // Step 3: Iterate over ID ranges
    let mut range_start: i64 = 0;
    while range_start <= max_id {
        let range_end = range_start + batch_size;
        batch_num += 1;

        // Fetch images first, then enrichment in parallel
        let images = queries::fetch_images_by_range(pool, range_start, range_end)
            .await
            .map_err(|e| format!("batch {batch_num} image fetch failed: {e}"))?;

        if images.is_empty() {
            range_start = range_end;
            continue;
        }

        // Collect image IDs for enrichment queries
        let image_ids: Vec<i64> = images.iter().map(|r| r.id).collect();

        // Run enrichment queries in parallel
        let (tags, tools, techniques, resources) = tokio::try_join!(
            queries::fetch_tags(pool, &image_ids),
            queries::fetch_tools(pool, &image_ids),
            queries::fetch_techniques(pool, &image_ids),
            queries::fetch_resources(pool, &image_ids),
        )
        .map_err(|e| format!("batch {batch_num} enrichment failed: {e}"))?;

        // Assemble documents
        let enrichment = EnrichmentData::from_rows(tags, tools, techniques, resources);
        let docs = assemble_batch(&images, &enrichment);

        // Rayon fold+reduce: extract bitmaps + encode docs
        let schema_ref = schema;
        let f_names = &filter_names;
        let s_configs = &sort_configs;
        let f_set = &filter_set;
        let s_bits = &sort_bits;
        let writer = &bulk_writer;

        let accum = docs
            .into_par_iter()
            .fold(
                || BitmapAccum::new(f_names, s_configs),
                |mut acc, json| {
                    let slot = match json
                        .get("id")
                        .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|n| n as u64)))
                    {
                        Some(id) => id as u32,
                        None => {
                            acc.errors += 1;
                            return acc;
                        }
                    };

                    // Encode doc to msgpack
                    let bytes = writer.encode_json(&json, schema_ref);
                    acc.encoded_docs.push((slot, bytes));

                    // Build bitmaps
                    acc.alive.insert(slot);
                    extract_bitmaps(
                        &json, schema_ref, f_set, s_bits, slot,
                        &mut acc.filter_maps, &mut acc.sort_maps,
                    );
                    acc.count += 1;
                    acc
                },
            )
            .reduce(
                || BitmapAccum::new(f_names, s_configs),
                |a, b| a.merge(b),
            );

        total_errors += accum.errors;
        let chunk_count = accum.count as u64;

        // Apply bitmaps to staging
        let t0 = Instant::now();
        ConcurrentEngine::apply_bitmap_maps(
            &mut staging,
            accum.filter_maps,
            accum.sort_maps,
            accum.alive,
        );
        let apply_ms = t0.elapsed().as_secs_f64() * 1000.0;

        total_loaded += chunk_count;
        progress.store(total_loaded, Ordering::Release);

        let elapsed = wall_start.elapsed();
        let rate = total_loaded as f64 / elapsed.as_secs_f64();
        eprintln!(
            "  batch {batch_num}: range [{range_start}, {range_end}) → {} images, {} total ({:.0}/s) apply={:.1}ms",
            chunk_count, total_loaded, rate, apply_ms
        );

        // Spawn docstore writer
        if !accum.encoded_docs.is_empty() {
            let writer = Arc::clone(&bulk_writer);
            ds_handles.push(thread::spawn(move || {
                writer.write_batch_encoded(accum.encoded_docs);
            }));
        }

        range_start = range_end;
    }

    // Wait for docstore writes
    for h in ds_handles {
        h.join().map_err(|_| "docstore thread panicked")?;
    }

    // Publish staging
    engine.publish_staging(staging);

    let elapsed = wall_start.elapsed();
    let rate = total_loaded as f64 / elapsed.as_secs_f64();
    eprintln!(
        "Bulk load complete: {} records in {:.1}s ({:.0}/s), errors: {}",
        total_loaded,
        elapsed.as_secs_f64(),
        rate,
        total_errors
    );

    // Save snapshot
    eprintln!("Saving snapshot...");
    engine
        .save_snapshot()
        .map_err(|e| format!("save_snapshot failed: {e}"))?;
    eprintln!("Snapshot saved.");

    Ok(BulkLoadStats {
        records_loaded: total_loaded,
        errors: total_errors,
        elapsed,
    })
}
