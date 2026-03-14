//! Scatter-gather bulk loader: CSV → scratch shards → bitmaps + docstore.
//!
//! Two-phase pipeline that keeps peak memory under 12 GB for 107M+ records:
//!
//! Phase 1 (scatter): Pure I/O — stream each CSV, route tuples to shard files.
//!   No bitmap work. Enrichment lookups resolve foreign keys inline.
//!
//! Phase 2 (gather): Process scratch shards in parallel via rayon fold+reduce.
//!   Each shard has all data for its slot range. Workers build bitmaps + encode
//!   docs in one pass (same pattern as the NDJSON loader). Bitmaps merged
//!   incrementally. Docs written via bounded channel with backpressure.

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::concurrent_engine::ConcurrentEngine;
use crate::loader::BitmapAccum;

use super::bulk_loader::BulkLoadStats;
use super::config::IndexDefinition;
use super::copy_queries::{
    parse_image_row, parse_model_row, parse_model_version_row, parse_post_row, parse_resource_row,
    parse_technique_row, parse_tool_row,
};
use super::copy_streams;
use super::progress::LoadProgress;
use super::scratch::{self, ScratchWriter, SlotDoc, DEFAULT_SHARD_SHIFT};

const LOG_INTERVAL: u64 = 1_000_000;
const PROGRESS_INTERVAL: u64 = 100_000;

/// Run the two-phase scatter-gather bulk load pipeline.
///
/// Phase 1 (scatter): CSV → scratch shard files (pure I/O, no bitmap work).
/// Phase 2 (gather): scratch shards → bitmaps + docstore (rayon parallel).
pub fn run_bulk_load_scatter(
    engine: &ConcurrentEngine,
    index_def: &IndexDefinition,
    stage_dir: &Path,
    progress: Arc<LoadProgress>,
) -> Result<BulkLoadStats, String> {
    let schema = &index_def.data_schema;
    let config = engine.config();
    let wall_start = Instant::now();

    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config
        .sort_fields
        .iter()
        .map(|f| (f.name.clone(), f.bits))
        .collect();
    let filter_set: HashSet<String> = filter_names.iter().cloned().collect();
    let sort_bits: HashMap<String, u8> = sort_configs.iter().cloned().collect();

    let scratch_dir = stage_dir
        .parent()
        .unwrap_or(stage_dir)
        .join("scratch_shards");
    let mut scratch = ScratchWriter::new(&scratch_dir, DEFAULT_SHARD_SHIFT)
        .map_err(|e| format!("ScratchWriter::new: {e}"))?;

    // ===================================================================
    // Phase 1: Scatter — CSV → scratch shard files (pure I/O)
    // ===================================================================
    progress.set_phase(1);
    eprintln!("\n=== Phase 1: Scatter (CSV → scratch shards, pure I/O) ===");

    // Load enrichment lookups
    let enrich_start = Instant::now();
    eprintln!("Loading enrichment tables...");

    let post_map = load_post_map(stage_dir)?;
    eprintln!("  Posts: {} rows in {:.1}s", post_map.len(), enrich_start.elapsed().as_secs_f64());

    let mv_start = Instant::now();
    let mv_map = load_mv_map(stage_dir)?;
    eprintln!("  ModelVersions: {} rows in {:.1}s", mv_map.len(), mv_start.elapsed().as_secs_f64());

    let model_start = Instant::now();
    let model_map = load_model_map(stage_dir)?;
    eprintln!("  Models: {} rows in {:.1}s", model_map.len(), model_start.elapsed().as_secs_f64());

    // Scatter all CSVs — pure I/O, no bitmap work
    eprintln!("\nScattering tags (63 GB)...");
    let t = Instant::now();
    let tag_total = scatter_tags_io(stage_dir, &mut scratch, &progress)?;
    eprintln!("  tags: {} rows in {:.1}s ({:.0}/s)", tag_total, t.elapsed().as_secs_f64(),
        tag_total as f64 / t.elapsed().as_secs_f64().max(0.001));

    let t = Instant::now();
    let tool_total = scatter_tools_io(stage_dir, &mut scratch)?;
    eprintln!("  tools: {} rows in {:.1}s", tool_total, t.elapsed().as_secs_f64());

    let t = Instant::now();
    let tech_total = scatter_techniques_io(stage_dir, &mut scratch)?;
    eprintln!("  techniques: {} rows in {:.1}s", tech_total, t.elapsed().as_secs_f64());

    eprintln!("Scattering resources...");
    let t = Instant::now();
    let res_total = scatter_resources_io(stage_dir, &mut scratch, schema, &mv_map, &model_map)?;
    eprintln!("  resources: {} rows in {:.1}s", res_total, t.elapsed().as_secs_f64());
    drop(mv_map);
    drop(model_map);

    eprintln!("Scattering images...");
    let t = Instant::now();
    let img_total = scatter_images_io(stage_dir, &mut scratch, &post_map, &progress)?;
    eprintln!("  images: {} rows in {:.1}s ({:.0}/s)", img_total, t.elapsed().as_secs_f64(),
        img_total as f64 / t.elapsed().as_secs_f64().max(0.001));
    drop(post_map);

    scratch.flush_all().map_err(|e| format!("scratch flush: {e}"))?;
    eprintln!(
        "\nScatter complete: {} shards, {} tuples, {:.1} GB in {:.1}s",
        scratch.shard_count(), scratch.tuples_written(),
        scratch.bytes_written() as f64 / (1024.0 * 1024.0 * 1024.0),
        wall_start.elapsed().as_secs_f64()
    );
    // Drop the writer to close all file handles
    drop(scratch);

    // Load ClickHouse metrics (if metrics.csv exists in staging dir)
    let metrics_map = load_metrics_csv(stage_dir);
    eprintln!("Loaded {} image metrics from ClickHouse", metrics_map.len());

    run_gather_and_apply(engine, index_def, &scratch_dir, &progress, &metrics_map)?;

    progress.set_phase(6);
    let elapsed = wall_start.elapsed();
    eprintln!(
        "\nBulk load complete: {} images in {:.1}s ({:.0}/s)",
        img_total, elapsed.as_secs_f64(), img_total as f64 / elapsed.as_secs_f64()
    );

    Ok(BulkLoadStats {
        records_loaded: img_total,
        errors: 0,
        elapsed,
    })
}

/// Run only Phase 2 (gather) using existing scratch shards on disk.
/// For iterating on gather performance without re-running scatter.
pub fn run_gather_only(
    engine: &ConcurrentEngine,
    index_def: &IndexDefinition,
    stage_dir: &Path,
    progress: Arc<LoadProgress>,
) -> Result<BulkLoadStats, String> {
    let wall_start = Instant::now();
    let scratch_dir = stage_dir
        .parent()
        .unwrap_or(stage_dir)
        .join("scratch_shards");

    let metrics_map = load_metrics_csv(stage_dir);
    eprintln!("Loaded {} image metrics from ClickHouse", metrics_map.len());

    run_gather_and_apply(engine, index_def, &scratch_dir, &progress, &metrics_map)?;

    let elapsed = wall_start.elapsed();
    Ok(BulkLoadStats {
        records_loaded: 0, // unknown in phase2-only mode
        errors: 0,
        elapsed,
    })
}

/// Shared gather + apply + save logic used by both full pipeline and phase2-only.
fn run_gather_and_apply(
    engine: &ConcurrentEngine,
    index_def: &IndexDefinition,
    scratch_dir: &Path,
    progress: &Arc<LoadProgress>,
    metrics_map: &MetricsMap,
) -> Result<(), String> {
    let schema = &index_def.data_schema;
    let config = engine.config();

    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config
        .sort_fields
        .iter()
        .map(|f| (f.name.clone(), f.bits))
        .collect();
    let filter_set: HashSet<String> = filter_names.iter().cloned().collect();
    let sort_bits: HashMap<String, u8> = sort_configs.iter().cloned().collect();

    progress.set_phase(2);
    eprintln!("\n=== Phase 2: Gather (scratch shards → bitmaps + docstore) ===");
    let gather_start = Instant::now();

    // Prepare BulkWriter for doc encoding
    let all_field_names: Vec<String> = schema
        .fields
        .iter()
        .map(|f| f.target.clone())
        .chain(std::iter::once("id".to_string()))
        .collect();
    engine.set_docstore_defaults(schema);
    let bulk_writer = Arc::new(
        engine
            .prepare_bulk_writer(&all_field_names)
            .map_err(|e| format!("prepare_bulk_writer: {e}"))?,
    );

    let shard_files = scratch::list_shard_files(&scratch_dir)
        .map_err(|e| format!("list_shard_files: {e}"))?;
    let num_shards = shard_files.len();
    eprintln!("Processing {} shard files with rayon...", num_shards);

    // Doc writes: each rayon worker writes directly via BulkWriter
    // (BulkWriter has per-shard DashMap locking — safe for concurrent use)
    let docs_written = std::sync::atomic::AtomicU64::new(0);
    let bytes_written = std::sync::atomic::AtomicU64::new(0);
    let docs_written_ref = &docs_written;
    let bytes_written_ref = &bytes_written;

    // Bounded channel for bitmap fragments — backpressure when merge thread is behind.
    // 32 slots × ~5 MB per fragment = ~160 MB max in-flight. Prevents RSS from
    // climbing unbounded when rayon workers outpace the merge thread.
    let (bm_tx, bm_rx) = crossbeam_channel::bounded::<ShardBitmaps>(32);

    // Bitmap merge thread — merges fragments as they arrive
    let f_names = filter_names.clone();
    let s_configs = sort_configs.clone();
    let merge_handle = std::thread::spawn(move || {
        let mut global = BitmapAccum::new(&f_names, &s_configs);
        let mut merged_count = 0u64;
        while let Ok(shard_bm) = bm_rx.recv() {
            for (field, value_map) in shard_bm.filter_maps {
                if let Some(target) = global.filter_maps.get_mut(&field) {
                    for (value, bm) in value_map {
                        target.entry(value).and_modify(|e| *e |= &bm).or_insert(bm);
                    }
                }
            }
            for (field, bit_map) in shard_bm.sort_maps {
                if let Some(target) = global.sort_maps.get_mut(&field) {
                    for (bit, bm) in bit_map {
                        target.entry(bit).and_modify(|e| *e |= &bm).or_insert(bm);
                    }
                }
            }
            global.alive |= &shard_bm.alive;
            merged_count += 1;
            if merged_count % 100 == 0 {
                eprintln!("  merged {}/{} bitmap fragments", merged_count, num_shards);
            }
        }
        eprintln!("  merged all {} bitmap fragments", merged_count);
        global
    });

    // Progress + timing accumulators (atomics, updated by rayon workers)
    let shards_done = std::sync::atomic::AtomicU64::new(0);
    let t_read_ms = std::sync::atomic::AtomicU64::new(0);
    let t_bitmap_ms = std::sync::atomic::AtomicU64::new(0);
    let t_encode_ms = std::sync::atomic::AtomicU64::new(0);
    let t_write_ms = std::sync::atomic::AtomicU64::new(0);
    let shards_done_ref = &shards_done;
    let t_read_ref = &t_read_ms;
    let t_bitmap_ref = &t_bitmap_ms;
    let t_encode_ref = &t_encode_ms;
    let t_write_ref = &t_write_ms;

    // Parallel shard processing — for_each (non-blocking, no collect deadlock)
    let bm_tx_ref = &bm_tx;
    let bulk_writer_ref = &bulk_writer;
    let schema_ref = schema;
    let filter_set_ref = &filter_set;
    let sort_bits_ref = &sort_bits;
    let filter_names_ref = &filter_names;
    let sort_configs_ref = &sort_configs;
    let metrics_map_ref = metrics_map;

    shard_files.par_iter().for_each(|shard_path| {
        // Phase A: Read + parse shard
        let tr = Instant::now();
        let slot_docs = match scratch::read_shard_fast(shard_path) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Warning: failed to read shard {:?}: {e}", shard_path);
                return;
            }
        };
        if slot_docs.is_empty() {
            return;
        }
        let read_ms = tr.elapsed().as_millis() as u64;

        // Phase B: Build bitmaps
        let tb = Instant::now();
        let mut bitmaps = ShardBitmaps {
            filter_maps: filter_names_ref.iter().map(|n| (n.clone(), HashMap::new())).collect(),
            sort_maps: sort_configs_ref.iter().map(|(n, _)| (n.clone(), HashMap::new())).collect(),
            alive: RoaringBitmap::new(),
        };
        for doc in &slot_docs {
            build_bitmaps_from_slot_doc(
                doc, filter_set_ref, sort_bits_ref, schema_ref,
                &mut bitmaps.filter_maps,
                &mut bitmaps.sort_maps,
                metrics_map_ref,
            );
            bitmaps.alive.insert(doc.slot);
        }
        let bitmap_ms = tb.elapsed().as_millis() as u64;

        // Phase C: Encode docs to msgpack
        let te = Instant::now();
        let mut encoded_docs: Vec<(u32, Vec<u8>)> = Vec::with_capacity(slot_docs.len());
        for doc in &slot_docs {
            let json = scratch::slot_doc_to_json(doc);
            let bytes = bulk_writer_ref.encode_json(&json, schema_ref);
            encoded_docs.push((doc.slot, bytes));
        }
        let encode_ms = te.elapsed().as_millis() as u64;

        // Send bitmaps to merge thread (unbounded, never blocks)
        let _ = bm_tx_ref.send(bitmaps);

        // Phase D: Write docs to docstore
        let tw = Instant::now();
        if !encoded_docs.is_empty() {
            let batch_bytes: u64 = encoded_docs.iter().map(|(_, b)| b.len() as u64).sum();
            let batch_count = encoded_docs.len() as u64;
            bulk_writer_ref.write_batch_fresh(encoded_docs);
            docs_written_ref.fetch_add(batch_count, Ordering::Relaxed);
            bytes_written_ref.fetch_add(batch_bytes, Ordering::Relaxed);
        }
        let write_ms = tw.elapsed().as_millis() as u64;

        // Accumulate timing
        t_read_ref.fetch_add(read_ms, Ordering::Relaxed);
        t_bitmap_ref.fetch_add(bitmap_ms, Ordering::Relaxed);
        t_encode_ref.fetch_add(encode_ms, Ordering::Relaxed);
        t_write_ref.fetch_add(write_ms, Ordering::Relaxed);

        let done = shards_done_ref.fetch_add(1, Ordering::Relaxed) + 1;
        if done % 100 == 0 || done <= 5 {
            let total_ms = (t_read_ref.load(Ordering::Relaxed)
                + t_bitmap_ref.load(Ordering::Relaxed)
                + t_encode_ref.load(Ordering::Relaxed)
                + t_write_ref.load(Ordering::Relaxed)).max(1);
            eprintln!(
                "  {}/{} shards | read {:.0}% bitmap {:.0}% encode {:.0}% write {:.0}%",
                done, num_shards,
                t_read_ref.load(Ordering::Relaxed) as f64 / total_ms as f64 * 100.0,
                t_bitmap_ref.load(Ordering::Relaxed) as f64 / total_ms as f64 * 100.0,
                t_encode_ref.load(Ordering::Relaxed) as f64 / total_ms as f64 * 100.0,
                t_write_ref.load(Ordering::Relaxed) as f64 / total_ms as f64 * 100.0,
            );
        }
    });

    // Close bitmap channel, wait for merge thread
    drop(bm_tx);
    let global_accum = merge_handle.join().unwrap();
    let docs_written = docs_written.load(Ordering::Relaxed);
    let bytes_written = bytes_written.load(Ordering::Relaxed);

    eprintln!(
        "Gather complete: {} docs ({:.1} GB) in {:.1}s ({:.0}/s)",
        docs_written,
        bytes_written as f64 / (1024.0 * 1024.0 * 1024.0),
        gather_start.elapsed().as_secs_f64(),
        docs_written as f64 / gather_start.elapsed().as_secs_f64().max(0.001)
    );

    // ===================================================================
    // Phase 3: Apply bitmaps + save snapshot (no clone spike)
    // ===================================================================
    progress.set_phase(3);
    eprintln!("\n=== Phase 3: Apply bitmaps + save snapshot ===");
    let apply_start = Instant::now();

    // Apply bitmaps directly to staging (in loading mode, no snapshot publish).
    // Uses apply_bitmap_maps on a clone — but we're in loading mode so
    // the staging refcount is 1 (no readers), making Arc::make_mut a no-op.
    let mut staging = engine.clone_staging();
    ConcurrentEngine::apply_bitmap_maps(
        &mut staging,
        global_accum.filter_maps,
        global_accum.sort_maps,
        global_accum.alive,
    );
    engine.publish_staging(staging);
    eprintln!("Bitmaps applied in {:.1}s", apply_start.elapsed().as_secs_f64());

    // Save snapshot + unload in one step — avoids the 22GB→38GB RSS spike
    // from the intermediate staging.clone() that exit_loading_mode() would do.
    eprintln!("Saving bitmap snapshot (save_and_unload)...");
    engine
        .exit_loading_mode_and_save_unload()
        .map_err(|e| format!("exit_loading_mode_and_save_unload: {e}"))?;
    eprintln!("Snapshot saved and unloaded in {:.1}s", apply_start.elapsed().as_secs_f64());

    // Clean up scratch shards
    if let Err(e) = std::fs::remove_dir_all(&scratch_dir) {
        eprintln!("Warning: failed to cleanup scratch shards: {e}");
    }
    eprintln!("Scratch shards cleaned up.");

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-shard result types for rayon fold+reduce
// ---------------------------------------------------------------------------

struct ShardBitmaps {
    filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
    sort_maps: HashMap<String, HashMap<usize, RoaringBitmap>>,
    alive: RoaringBitmap,
}

// ShardResult removed — ShardBitmaps + encoded_docs handled inline in gather loop

// ---------------------------------------------------------------------------
// Build bitmaps from a SlotDoc (same logic as copy_streams::build_image_bitmaps
// but also handles multi-value fields from scratch tuples)
// ---------------------------------------------------------------------------

/// Metrics for a single image: (reactionCount, commentCount, collectedCount).
pub type MetricsMap = HashMap<u32, (u32, u32, u32)>;

fn build_bitmaps_from_slot_doc(
    doc: &SlotDoc,
    filter_set: &HashSet<String>,
    sort_bits: &HashMap<String, u8>,
    schema: &crate::config::DataSchema,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
    sort_maps: &mut HashMap<String, HashMap<usize, RoaringBitmap>>,
    metrics_map: &MetricsMap,
) {
    let slot = doc.slot;

    // Scalar filter bitmaps
    if filter_set.contains("nsfwLevel") {
        insert_filter(filter_maps, "nsfwLevel", doc.nsfw_level as u64, slot);
    }
    if filter_set.contains("userId") {
        insert_filter(filter_maps, "userId", doc.user_id, slot);
    }
    if filter_set.contains("type") {
        // image_type is already encoded as u8
        let key = resolve_type_key(doc.image_type, schema);
        insert_filter(filter_maps, "type", key, slot);
    }
    if filter_set.contains("availability") {
        let key = resolve_availability_key(doc.availability, schema);
        insert_filter(filter_maps, "availability", key, slot);
    }
    if filter_set.contains("blockedFor") && doc.blocked_for > 0 {
        let key = resolve_blocked_for_key(schema);
        insert_filter(filter_maps, "blockedFor", key, slot);
    }
    if filter_set.contains("postId") {
        insert_filter(filter_maps, "postId", doc.post_id, slot);
    }
    if filter_set.contains("postedToId") {
        insert_filter(filter_maps, "postedToId", doc.posted_to_id, slot);
    }
    if filter_set.contains("hasMeta") {
        insert_filter(filter_maps, "hasMeta", if doc.has_meta { 1 } else { 0 }, slot);
    }
    if filter_set.contains("onSite") {
        insert_filter(filter_maps, "onSite", if doc.on_site { 1 } else { 0 }, slot);
    }
    if filter_set.contains("poi") {
        insert_filter(filter_maps, "poi", if doc.poi { 1 } else { 0 }, slot);
    }
    if filter_set.contains("minor") {
        insert_filter(filter_maps, "minor", if doc.minor { 1 } else { 0 }, slot);
    }
    if filter_set.contains("isPublished") {
        let published = doc.published_at_ms > 0;
        insert_filter(filter_maps, "isPublished", if published { 1 } else { 0 }, slot);
    }
    if filter_set.contains("isRemix") {
        insert_filter(filter_maps, "isRemix", 0, slot);
    }

    // Multi-value filter bitmaps (from scratch tuples)
    if filter_set.contains("tagIds") {
        if let Some(fm) = filter_maps.get_mut("tagIds") {
            for &tag_id in &doc.tag_ids {
                fm.entry(tag_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
        }
    }
    if filter_set.contains("toolIds") {
        if let Some(fm) = filter_maps.get_mut("toolIds") {
            for &tool_id in &doc.tool_ids {
                fm.entry(tool_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
        }
    }
    if filter_set.contains("techniqueIds") {
        if let Some(fm) = filter_maps.get_mut("techniqueIds") {
            for &tech_id in &doc.technique_ids {
                fm.entry(tech_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
        }
    }
    if filter_set.contains("modelVersionIds") {
        if let Some(fm) = filter_maps.get_mut("modelVersionIds") {
            for &mv_id in &doc.model_version_ids {
                fm.entry(mv_id as u64).or_insert_with(RoaringBitmap::new).insert(slot);
            }
        }
    }
    if filter_set.contains("baseModel") && doc.base_model > 0 {
        let key = resolve_base_model_key(doc.base_model, schema);
        insert_filter(filter_maps, "baseModel", key, slot);
    }

    // Sort bitmaps (bit decomposition)
    if let Some(&bits) = sort_bits.get("sortAt") {
        insert_sort_bits(sort_maps, "sortAt", doc.sort_at as u32, bits, slot);
    }
    if let Some(&bits) = sort_bits.get("publishedAt") {
        let pub_secs = (doc.published_at_ms / 1000) as u32;
        insert_sort_bits(sort_maps, "publishedAt", pub_secs, bits, slot);
    }
    let (reaction_count, comment_count, collected_count) =
        metrics_map.get(&slot).copied().unwrap_or((0, 0, 0));
    if let Some(&bits) = sort_bits.get("reactionCount") {
        insert_sort_bits(sort_maps, "reactionCount", reaction_count, bits, slot);
    }
    if let Some(&bits) = sort_bits.get("commentCount") {
        insert_sort_bits(sort_maps, "commentCount", comment_count, bits, slot);
    }
    if let Some(&bits) = sort_bits.get("collectedCount") {
        insert_sort_bits(sort_maps, "collectedCount", collected_count, bits, slot);
    }
    if let Some(&bits) = sort_bits.get("id") {
        insert_sort_bits(sort_maps, "id", slot, bits, slot);
    }
}

#[inline]
fn insert_filter(filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>, field: &str, value: u64, slot: u32) {
    if let Some(fm) = filter_maps.get_mut(field) {
        fm.entry(value).or_insert_with(RoaringBitmap::new).insert(slot);
    }
}

#[inline]
fn insert_sort_bits(sort_maps: &mut HashMap<String, HashMap<usize, RoaringBitmap>>, field: &str, value: u32, bits: u8, slot: u32) {
    if let Some(sm) = sort_maps.get_mut(field) {
        for bit in 0..(bits as usize) {
            if (value >> bit) & 1 == 1 {
                sm.entry(bit).or_insert_with(RoaringBitmap::new).insert(slot);
            }
        }
    }
}

// String map resolution helpers — resolve encoded u8 values to schema string_map keys
fn resolve_type_key(image_type: u8, schema: &crate::config::DataSchema) -> u64 {
    let type_str = super::slot_arena::decode_image_type(image_type);
    schema.fields.iter()
        .find(|f| f.target == "type")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| map.get(&type_str.to_lowercase()).or_else(|| map.get(type_str)).copied())
        .unwrap_or(0) as u64
}

fn resolve_availability_key(availability: u8, schema: &crate::config::DataSchema) -> u64 {
    let avail_str = super::slot_arena::decode_availability(availability);
    schema.fields.iter()
        .find(|f| f.target == "availability")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| map.get(&avail_str.to_lowercase()).or_else(|| map.get(avail_str)).copied())
        .unwrap_or(0) as u64
}

fn resolve_blocked_for_key(schema: &crate::config::DataSchema) -> u64 {
    schema.fields.iter()
        .find(|f| f.target == "blockedFor")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| map.get("blocked").or_else(|| map.values().next()).copied())
        .unwrap_or(1) as u64
}

fn resolve_base_model_key(base_model: u8, schema: &crate::config::DataSchema) -> u64 {
    let bm_str = super::slot_arena::decode_base_model(base_model);
    schema.fields.iter()
        .find(|f| f.target == "baseModel")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| map.get(&bm_str.to_lowercase()).or_else(|| map.get(bm_str)).copied())
        .unwrap_or(0) as u64
}

// ---------------------------------------------------------------------------
// Scatter functions — pure I/O, no bitmap work
// ---------------------------------------------------------------------------

fn scatter_tags_io(
    stage_dir: &Path,
    scratch: &mut ScratchWriter,
    progress: &Arc<LoadProgress>,
) -> Result<u64, String> {
    use std::io::Read;

    let mut file = std::io::BufReader::with_capacity(
        16 * 1024 * 1024,
        std::fs::File::open(stage_dir.join("tags.csv"))
            .map_err(|e| format!("open tags.csv: {e}"))?,
    );

    // Block reader: read large chunks, iterate lines zero-copy.
    // No per-line Vec allocation — 5.4B rows × 0 allocs.
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    let mut leftover = Vec::<u8>::with_capacity(64);
    let mut total = 0u64;
    let mut last_log = 0u64;

    loop {
        let bytes_read = file.read(&mut buf).map_err(|e| format!("read tags.csv: {e}"))?;
        if bytes_read == 0 {
            // Process leftover
            if !leftover.is_empty() {
                if let Some((tag_id, image_id)) = parse_tag_line(&leftover) {
                    scratch.write_tag_raw(image_id as u32, tag_id as u32)
                        .map_err(|e| format!("scratch write_tag: {e}"))?;
                    total += 1;
                }
            }
            break;
        }

        // Work on leftover + new data
        let (work, work_owned);
        if leftover.is_empty() {
            work = &buf[..bytes_read];
            work_owned = None;
        } else {
            leftover.extend_from_slice(&buf[..bytes_read]);
            work_owned = Some(&leftover);
            work = work_owned.as_ref().unwrap().as_slice();
        }

        // Find last newline
        let last_nl = match work.iter().rposition(|&b| b == b'\n') {
            Some(pos) => pos,
            None => {
                if leftover.is_empty() {
                    leftover = buf[..bytes_read].to_vec();
                }
                continue;
            }
        };

        // Save remainder as new leftover
        let new_leftover = if last_nl + 1 < work.len() {
            work[last_nl + 1..].to_vec()
        } else {
            Vec::new()
        };

        // Process complete lines in-place (zero-copy)
        let complete = &work[..last_nl + 1];
        let mut start = 0;
        for i in 0..complete.len() {
            if complete[i] == b'\n' {
                let line = &complete[start..i];
                start = i + 1;
                if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                    continue;
                }
                if let Some((tag_id, image_id)) = parse_tag_line(line) {
                    scratch.write_tag_raw(image_id as u32, tag_id as u32)
                        .map_err(|e| format!("scratch write_tag: {e}"))?;
                    total += 1;
                }
            }
        }

        leftover = new_leftover;

        if total / (LOG_INTERVAL * 10) > last_log / (LOG_INTERVAL * 10) {
            last_log = total;
            progress.tag_rows.store(total, Ordering::Release);
            eprintln!("  tags: {}M rows...", total / 1_000_000);
        }
    }

    progress.tag_rows.store(total, Ordering::Release);
    Ok(total)
}

fn scatter_tools_io(stage_dir: &Path, scratch: &mut ScratchWriter) -> Result<u64, String> {
    let file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("tools.csv"))
            .map_err(|e| format!("open tools.csv: {e}"))?,
    );
    let mut total = 0u64;
    for line in file.split(b'\n') {
        let line = line.map_err(|e| format!("read tools.csv: {e}"))?;
        if line.is_empty() { continue; }
        if let Some((tool_id, image_id)) = parse_tool_row(&line) {
            scratch.write_tool(image_id as u32, tool_id as u32)
                .map_err(|e| format!("scratch write_tool: {e}"))?;
            total += 1;
        }
    }
    Ok(total)
}

fn scatter_techniques_io(stage_dir: &Path, scratch: &mut ScratchWriter) -> Result<u64, String> {
    let file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("techniques.csv"))
            .map_err(|e| format!("open techniques.csv: {e}"))?,
    );
    let mut total = 0u64;
    for line in file.split(b'\n') {
        let line = line.map_err(|e| format!("read techniques.csv: {e}"))?;
        if line.is_empty() { continue; }
        if let Some((tech_id, image_id)) = parse_technique_row(&line) {
            scratch.write_technique(image_id as u32, tech_id as u32)
                .map_err(|e| format!("scratch write_technique: {e}"))?;
            total += 1;
        }
    }
    Ok(total)
}

fn scatter_resources_io(
    stage_dir: &Path,
    scratch: &mut ScratchWriter,
    schema: &crate::config::DataSchema,
    mv_map: &HashMap<i64, (Option<String>, i64)>,
    model_map: &HashMap<i64, (bool, String)>,
) -> Result<u64, String> {
    let file = std::io::BufReader::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::open(stage_dir.join("resources.csv"))
            .map_err(|e| format!("open resources.csv: {e}"))?,
    );
    let mut total = 0u64;
    for line in file.split(b'\n') {
        let line = line.map_err(|e| format!("read resources.csv: {e}"))?;
        if line.is_empty() { continue; }
        let row = match parse_resource_row(&line) {
            Some(r) => r,
            None => continue,
        };

        let slot = row.image_id as u32;
        scratch.write_model_version(slot, row.model_version_id as u32, row.detected)
            .map_err(|e| format!("scratch write_mv: {e}"))?;

        // Enrich from MV/Model lookups
        if let Some((mv_base_model, model_id)) = mv_map.get(&row.model_version_id) {
            if let Some((poi, model_type)) = model_map.get(model_id) {
                if model_type == "Checkpoint" {
                    if let Some(ref bm_str) = mv_base_model {
                        scratch.write_base_model(slot, super::slot_arena::encode_base_model(Some(bm_str)))
                            .map_err(|e| format!("scratch write_base_model: {e}"))?;
                    }
                }
                if *poi {
                    scratch.write_resource_poi(slot)
                        .map_err(|e| format!("scratch write_resource_poi: {e}"))?;
                }
            }
        }

        total += 1;
    }
    Ok(total)
}

fn scatter_images_io(
    stage_dir: &Path,
    scratch: &mut ScratchWriter,
    post_map: &HashMap<i64, (Option<i64>, String, Option<i64>)>,
    progress: &Arc<LoadProgress>,
) -> Result<u64, String> {
    let file = std::io::BufReader::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::open(stage_dir.join("images.csv"))
            .map_err(|e| format!("open images.csv: {e}"))?,
    );
    let mut total = 0u64;
    for line in file.split(b'\n') {
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
        let flags: u8 = (row.poi() as u8)
            | ((row.minor() as u8) << 1)
            | ((row.has_meta() as u8) << 2)
            | ((row.on_site() as u8) << 3);

        scratch.write_image_scalars(
            slot, row.nsfw_level as u8, row.user_id as u64,
            super::slot_arena::encode_image_type(Some(&row.image_type)),
            sort_at, flags,
            row.post_id.unwrap_or(0) as u64,
            row.posted_to_id.unwrap_or(0) as u64,
            super::slot_arena::encode_availability(Some(row.availability.as_str())),
            super::slot_arena::encode_blocked_for(row.blocked_for.as_deref()),
            published_at_ms,
            row.url.as_deref().map(|s| s.as_bytes()),
            row.hash.as_deref().map(|s| s.as_bytes()),
        ).map_err(|e| format!("scratch write_image: {e}"))?;

        total += 1;
        if total % PROGRESS_INTERVAL == 0 {
            progress.image_rows.store(total, Ordering::Release);
        }
        if total % LOG_INTERVAL == 0 {
            eprintln!("  images: {}M rows...", total / 1_000_000);
        }
    }
    progress.image_rows.store(total, Ordering::Release);
    Ok(total)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn load_post_map(stage_dir: &Path) -> Result<HashMap<i64, (Option<i64>, String, Option<i64>)>, String> {
    let mut map = HashMap::new();
    let file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("posts.csv"))
            .map_err(|e| format!("open posts.csv: {e}"))?,
    );
    for line in file.split(b'\n') {
        let line = line.map_err(|e| format!("read posts.csv: {e}"))?;
        if let Some(row) = parse_post_row(&line) {
            map.insert(row.id, (row.published_at_secs, row.availability, row.model_version_id));
        }
    }
    Ok(map)
}

fn load_mv_map(stage_dir: &Path) -> Result<HashMap<i64, (Option<String>, i64)>, String> {
    let mut map = HashMap::new();
    let file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("model_versions.csv"))
            .map_err(|e| format!("open model_versions.csv: {e}"))?,
    );
    for line in file.split(b'\n') {
        let line = line.map_err(|e| format!("read model_versions.csv: {e}"))?;
        if let Some(row) = parse_model_version_row(&line) {
            map.insert(row.id, (row.base_model, row.model_id));
        }
    }
    Ok(map)
}

fn load_model_map(stage_dir: &Path) -> Result<HashMap<i64, (bool, String)>, String> {
    let mut map = HashMap::new();
    let file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("models.csv"))
            .map_err(|e| format!("open models.csv: {e}"))?,
    );
    for line in file.split(b'\n') {
        let line = line.map_err(|e| format!("read models.csv: {e}"))?;
        if let Some(row) = parse_model_row(&line) {
            map.insert(row.id, (row.poi, row.model_type));
        }
    }
    Ok(map)
}

/// Load ClickHouse metrics CSV into a HashMap.
/// Format: id\treactionCount\tcommentCount\tcollectedCount (TSV, no header).
/// Returns empty map if file doesn't exist (graceful degradation).
pub fn load_metrics_csv(stage_dir: &Path) -> MetricsMap {
    let path = stage_dir.join("metrics.csv");
    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(_) => {
            eprintln!("No metrics.csv found at {} — metric sort fields will be 0", path.display());
            return HashMap::new();
        }
    };
    let reader = std::io::BufReader::new(file);
    let mut map = HashMap::new();
    let mut count = 0u64;
    for line in reader.split(b'\n') {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line.is_empty() {
            continue;
        }
        // TSV: id\treactionCount\tcommentCount\tcollectedCount
        let parts: Vec<&[u8]> = line.split(|&b| b == b'\t').collect();
        if parts.len() < 4 {
            continue;
        }
        let id = fast_parse_u32(parts[0]);
        let reaction = fast_parse_u32(parts[1]);
        let comment = fast_parse_u32(parts[2]);
        let collected = fast_parse_u32(parts[3]);
        if id > 0 {
            map.insert(id, (reaction, comment, collected));
            count += 1;
            if count % 10_000_000 == 0 {
                eprintln!("  metrics: {} rows loaded", count);
            }
        }
    }
    map
}

fn fast_parse_u32(bytes: &[u8]) -> u32 {
    let s = std::str::from_utf8(bytes).unwrap_or("0");
    s.trim().parse::<u32>().unwrap_or(0)
}

/// Download all-time aggregate metrics from ClickHouse to a TSV file.
/// Query: entityMetricDailyAgg grouped by entityId for entityType='Image'.
/// Output: metrics.csv in stage_dir (id\treactionCount\tcommentCount\tcollectedCount).
pub async fn download_metrics_from_clickhouse(
    stage_dir: &std::path::Path,
    ch_url: &str,
    ch_username: Option<&str>,
    ch_password: Option<&str>,
) -> Result<u64, String> {
    let done_path = stage_dir.join("metrics.csv.done");
    if done_path.exists() {
        eprintln!("metrics.csv already downloaded (found .done marker)");
        return Ok(0);
    }

    let csv_path = stage_dir.join("metrics.csv");
    eprintln!("Downloading ClickHouse metrics to {} ...", csv_path.display());

    let query = r#"SELECT
        entityId,
        sumIf(total, metricType IN ('ReactionLike','ReactionHeart','ReactionLaugh','ReactionCry')) as reactionCount,
        sumIf(total, metricType = 'Comment') as commentCount,
        sumIf(total, metricType = 'Collection') as collectedCount
    FROM entityMetricDailyAgg
    WHERE entityType = 'Image'
    GROUP BY entityId
    FORMAT TSV"#;

    let http = reqwest::Client::new();
    let mut req = http.post(ch_url).body(query.to_string());

    if let Some(username) = ch_username {
        let password = ch_password.unwrap_or("");
        req = req.basic_auth(username, Some(password));
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

    // Stream response body to file
    let mut file = std::fs::File::create(&csv_path)
        .map_err(|e| format!("create metrics.csv: {e}"))?;
    let body = resp.bytes().await.map_err(|e| format!("read CH body: {e}"))?;
    std::io::Write::write_all(&mut file, &body)
        .map_err(|e| format!("write metrics.csv: {e}"))?;

    let row_count = body.iter().filter(|&&b| b == b'\n').count() as u64;
    eprintln!("Downloaded {} metric rows from ClickHouse", row_count);

    // Write .done marker
    std::fs::write(&done_path, format!("{row_count}"))
        .map_err(|e| format!("write .done marker: {e}"))?;

    Ok(row_count)
}

/// Fast inline tag CSV parser — two integers, no quoting.
#[inline]
fn parse_tag_line(line: &[u8]) -> Option<(i64, i64)> {
    let comma = line.iter().position(|&b| b == b',')?;
    let tag_id = parse_i64_fast(&line[..comma])?;
    let rest = &line[comma + 1..];
    let end = if rest.last() == Some(&b'\r') { rest.len() - 1 } else { rest.len() };
    let image_id = parse_i64_fast(&rest[..end])?;
    Some((tag_id, image_id))
}

#[inline]
fn parse_i64_fast(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() { return None; }
    let (negative, start) = if bytes[0] == b'-' { (true, 1) } else { (false, 0) };
    if start >= bytes.len() { return None; }
    let mut val: i64 = 0;
    for &b in &bytes[start..] {
        if b < b'0' || b > b'9' { return None; }
        val = val.wrapping_mul(10).wrapping_add((b - b'0') as i64);
    }
    if negative { Some(-val) } else { Some(val) }
}
