//! Single-pass bulk loader: CSV → bitmaps + docstore tuples in one pass per CSV.
//!
//! Eliminates the scatter-gather pipeline by processing each CSV directly:
//!   1. Rayon block reader parses CSV rows
//!   2. Each row simultaneously builds bitmaps AND appends V2 docstore tuples
//!   3. After each CSV: stream bitmap saves to BitmapFs, drop from memory
//!   4. Enrichment maps loaded only when needed, dropped after
//!
//! Processing order (largest first to free memory sooner):
//!   1. Tags (63 GB, 5.4B rows) — tagIds filter bitmap + docstore tuples
//!   2. Images (14 GB, 107M rows) — scalar fields, sort fields, filter fields, alive bitmap
//!   3. Resources (777 MB) — baseModel, modelVersionIds, poi
//!   4. Tools (50 MB) — toolIds
//!   5. Techniques (71 MB) — techniqueIds
//!   6. Metrics (from metrics.csv) — sort bitmaps + docstore tuples

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::bitmap_fs::BitmapFs;
use crate::concurrent_engine::ConcurrentEngine;

use super::bulk_loader::BulkLoadStats;
use super::config::IndexDefinition;
use super::copy_queries::{
    parse_image_row, parse_model_row, parse_model_version_row, parse_post_row, parse_resource_row,
    parse_technique_row, parse_tool_row,
};
use super::progress::LoadProgress;

const LOG_INTERVAL: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the single-pass bulk loader.
///
/// Each CSV is processed in one pass: rows are parsed, bitmaps are built, and
/// V2 docstore tuples are appended simultaneously. After each CSV completes,
/// the bitmaps built during that CSV are saved to BitmapFs and dropped.
pub fn run_single_pass_v2(
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

    // Get BitmapFs for streaming saves
    let bitmap_fs = engine
        .bitmap_store()
        .ok_or_else(|| "no bitmap_path configured; cannot stream saves".to_string())?
        .clone();

    // Prepare BulkWriter for V2 docstore tuple appends
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

    // Enter loading mode — skip snapshot publishing during bulk inserts
    engine.enter_loading_mode();

    progress.set_phase(1);
    eprintln!("\n=== Single-Pass V2: CSV → bitmaps + docstore (no scratch shards) ===");

    // Assigned in step 2 (images CSV)
    let total_images: u64;

    // ===================================================================
    // Step 1: Tags (63 GB, 5.4B rows)
    // ===================================================================
    {
        eprintln!("\n--- Step 1: Tags CSV (tagIds filter bitmaps + docstore tuples) ---");
        let t = Instant::now();
        let tag_bitmaps = process_tags_csv(stage_dir, &bulk_writer, &progress)?;
        let tag_count = tag_bitmaps.len();
        eprintln!(
            "  Tags: {} distinct values built in {:.1}s",
            tag_count,
            t.elapsed().as_secs_f64()
        );

        // Save tagIds filter bitmaps to disk
        let t = Instant::now();
        let tag_map: HashMap<u64, RoaringBitmap> = tag_bitmaps;
        let saved = save_filter_field_to_disk(&bitmap_fs, "tagIds", &tag_map)?;
        eprintln!(
            "  Saved tagIds: {} values ({:.1} MB) in {:.1}s",
            tag_count,
            saved as f64 / (1024.0 * 1024.0),
            t.elapsed().as_secs_f64()
        );
        // tag_map dropped here — memory freed
    }

    // ===================================================================
    // Step 2: Images (14 GB, 107M rows)
    // ===================================================================
    {
        eprintln!("\n--- Step 2: Images CSV (scalar fields, sort fields, alive bitmap) ---");

        // Load Post enrichment map
        let t = Instant::now();
        let post_map = load_post_map(stage_dir)?;
        eprintln!(
            "  Post map: {} rows in {:.1}s",
            post_map.len(),
            t.elapsed().as_secs_f64()
        );

        let t = Instant::now();
        let img_result = process_images_csv(
            stage_dir,
            &bulk_writer,
            &progress,
            &post_map,
            schema,
            &filter_set,
            &sort_bits,
            &filter_names,
            &sort_configs,
        )?;
        total_images = img_result.row_count;
        eprintln!(
            "  Images: {} rows, {} filter fields, {} sort fields in {:.1}s ({:.0}/s)",
            img_result.row_count,
            img_result.filter_maps.len(),
            img_result.sort_maps.len(),
            t.elapsed().as_secs_f64(),
            img_result.row_count as f64 / t.elapsed().as_secs_f64().max(0.001)
        );
        drop(post_map);

        // Save filter bitmaps (except tagIds, already saved)
        let t = Instant::now();
        for (field_name, values) in &img_result.filter_maps {
            if values.is_empty() {
                continue;
            }
            let saved = save_filter_field_to_disk(&bitmap_fs, field_name, values)?;
            eprintln!(
                "  Saved filter {}: {} values ({:.1} MB)",
                field_name,
                values.len(),
                saved as f64 / (1024.0 * 1024.0)
            );
        }
        // Save sort bitmaps
        for (field_name, bits) in &sort_configs {
            if let Some(bit_map) = img_result.sort_maps.get(field_name) {
                if bit_map.is_empty() {
                    continue;
                }
                save_sort_field_to_disk(&bitmap_fs, field_name, bit_map, *bits)?;
                eprintln!("  Saved sort {}: {} layers", field_name, bits);
            }
        }
        // Save alive bitmap
        let max_slot = img_result.alive.max().unwrap_or(0);
        bitmap_fs
            .write_alive(&img_result.alive)
            .map_err(|e| format!("write_alive: {e}"))?;
        eprintln!(
            "  Saved alive bitmap: {} bits",
            img_result.alive.len()
        );
        // Save slot counter
        let slot_counter = max_slot.saturating_add(1);
        bitmap_fs
            .write_slot_counter(slot_counter)
            .map_err(|e| format!("write_slot_counter: {e}"))?;
        eprintln!(
            "  Image bitmaps saved in {:.1}s, slot_counter={}",
            t.elapsed().as_secs_f64(),
            slot_counter
        );
        // img_result dropped — memory freed
    }

    // ===================================================================
    // Step 3: Resources (777 MB)
    // ===================================================================
    {
        eprintln!("\n--- Step 3: Resources CSV (modelVersionIds, baseModel, poi) ---");

        let t = Instant::now();
        let mv_map = load_mv_map(stage_dir)?;
        eprintln!(
            "  MV map: {} rows in {:.1}s",
            mv_map.len(),
            t.elapsed().as_secs_f64()
        );
        let t = Instant::now();
        let model_map = load_model_map(stage_dir)?;
        eprintln!(
            "  Model map: {} rows in {:.1}s",
            model_map.len(),
            t.elapsed().as_secs_f64()
        );

        let t = Instant::now();
        let res_result = process_resources_csv(
            stage_dir,
            &bulk_writer,
            &progress,
            schema,
            &filter_set,
            &mv_map,
            &model_map,
        )?;
        eprintln!(
            "  Resources: {} rows in {:.1}s",
            res_result.row_count,
            t.elapsed().as_secs_f64()
        );
        drop(mv_map);
        drop(model_map);

        // Save resource filter bitmaps
        let t = Instant::now();
        for (field_name, values) in &res_result.filter_maps {
            if values.is_empty() {
                continue;
            }
            let saved = save_filter_field_to_disk(&bitmap_fs, field_name, values)?;
            eprintln!(
                "  Saved filter {}: {} values ({:.1} MB)",
                field_name,
                values.len(),
                saved as f64 / (1024.0 * 1024.0)
            );
        }
        eprintln!(
            "  Resource bitmaps saved in {:.1}s",
            t.elapsed().as_secs_f64()
        );
    }

    // ===================================================================
    // Step 4: Tools (50 MB)
    // ===================================================================
    {
        eprintln!("\n--- Step 4: Tools CSV (toolIds filter bitmaps) ---");
        let t = Instant::now();
        let tool_bitmaps = process_multi_value_csv(
            stage_dir,
            "tools.csv",
            parse_tool_row,
            &bulk_writer,
            "toolIds",
        )?;
        let tool_count = tool_bitmaps.len();
        eprintln!(
            "  Tools: {} distinct values in {:.1}s",
            tool_count,
            t.elapsed().as_secs_f64()
        );

        let saved = save_filter_field_to_disk(&bitmap_fs, "toolIds", &tool_bitmaps)?;
        eprintln!(
            "  Saved toolIds: {} values ({:.1} MB)",
            tool_count,
            saved as f64 / (1024.0 * 1024.0)
        );
    }

    // ===================================================================
    // Step 5: Techniques (71 MB)
    // ===================================================================
    {
        eprintln!("\n--- Step 5: Techniques CSV (techniqueIds filter bitmaps) ---");
        let t = Instant::now();
        let tech_bitmaps = process_multi_value_csv(
            stage_dir,
            "techniques.csv",
            parse_technique_row,
            &bulk_writer,
            "techniqueIds",
        )?;
        let tech_count = tech_bitmaps.len();
        eprintln!(
            "  Techniques: {} distinct values in {:.1}s",
            tech_count,
            t.elapsed().as_secs_f64()
        );

        let saved = save_filter_field_to_disk(&bitmap_fs, "techniqueIds", &tech_bitmaps)?;
        eprintln!(
            "  Saved techniqueIds: {} values ({:.1} MB)",
            tech_count,
            saved as f64 / (1024.0 * 1024.0)
        );
    }

    // ===================================================================
    // Step 6: Metrics (from metrics.csv)
    // ===================================================================
    {
        eprintln!("\n--- Step 6: Metrics CSV (reactionCount/commentCount/collectedCount sort bitmaps) ---");
        {
            let t = Instant::now();
            let metrics_sort = process_metrics_csv(
                stage_dir,
                &bulk_writer,
                &sort_bits,
                &sort_configs,
            )?;
            eprintln!(
                "  Built metrics sort bitmaps in {:.1}s",
                t.elapsed().as_secs_f64()
            );

            // Save metrics sort bitmaps
            for (field_name, bits) in &sort_configs {
                if let Some(bit_map) = metrics_sort.get(field_name) {
                    if bit_map.is_empty() {
                        continue;
                    }
                    save_sort_field_to_disk(&bitmap_fs, field_name, bit_map, *bits)?;
                    eprintln!("  Saved sort {}: {} layers", field_name, bits);
                }
            }
        }
    }

    // ===================================================================
    // Finalize: exit loading mode
    // ===================================================================
    progress.set_phase(3);
    eprintln!("\n=== Finalizing: exit loading mode ===");
    let t = Instant::now();
    engine.exit_loading_mode();
    eprintln!("Loading mode exited in {:.1}s", t.elapsed().as_secs_f64());

    progress.set_phase(6);
    let elapsed = wall_start.elapsed();
    eprintln!(
        "\nSingle-pass V2 complete: {} images in {:.1}s ({:.0}/s)",
        total_images,
        elapsed.as_secs_f64(),
        total_images as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok(BulkLoadStats {
        records_loaded: total_images,
        errors: 0,
        elapsed,
    })
}

// ---------------------------------------------------------------------------
// Per-CSV result types
// ---------------------------------------------------------------------------

struct ImageCsvResult {
    row_count: u64,
    filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
    sort_maps: HashMap<String, HashMap<usize, RoaringBitmap>>,
    alive: RoaringBitmap,
}

struct ResourceCsvResult {
    row_count: u64,
    filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
}

// ---------------------------------------------------------------------------
// Tags CSV processor — block reader + rayon fold+reduce
// ---------------------------------------------------------------------------

/// Process tags.csv: build tagIds filter bitmaps + append docstore tuples.
/// Returns HashMap<tag_id_u64, RoaringBitmap>.
fn process_tags_csv(
    stage_dir: &Path,
    _bulk_writer: &Arc<crate::docstore::BulkWriter>,
    progress: &Arc<LoadProgress>,
) -> Result<HashMap<u64, RoaringBitmap>, String> {
    // mmap the entire 63 GB file — zero-copy, OS handles paging.
    // Split into N chunks (one per rayon thread), each builds a local
    // Vec<RoaringBitmap> (direct index by tag_id, no hashing).
    // One merge pass at the end: OR all Vecs together.
    const MAX_TAG_ID: usize = 300_000; // tag IDs fit in 0..300K

    let file = std::fs::File::open(stage_dir.join("tags.csv"))
        .map_err(|e| format!("open tags.csv: {e}"))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap tags.csv: {e}"))?;
    let data = &mmap[..];
    let file_len = data.len();
    eprintln!("  tags: mmap'd {} ({:.1} GB)", file_len, file_len as f64 / (1024.0 * 1024.0 * 1024.0));

    // Split file into N equal-ish byte ranges, align to newlines.
    let num_threads = rayon::current_num_threads();
    let chunk_size = file_len / num_threads;
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(num_threads);
    let mut start = 0;
    for i in 0..num_threads {
        let mut end = if i == num_threads - 1 {
            file_len
        } else {
            let tentative = start + chunk_size;
            // Find next newline after tentative end
            match data[tentative..].iter().position(|&b| b == b'\n') {
                Some(offset) => tentative + offset + 1,
                None => file_len,
            }
        };
        if end > file_len { end = file_len; }
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }

    let total = AtomicU64::new(0);
    let total_ref = &total;

    // Rayon parallel: each thread processes its byte range independently.
    // Vec<RoaringBitmap> indexed by tag_id — direct index, no hashing.
    let thread_results: Vec<Vec<RoaringBitmap>> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &data[range_start..range_end];
            let mut bitmaps: Vec<RoaringBitmap> = (0..MAX_TAG_ID)
                .map(|_| RoaringBitmap::new())
                .collect();
            let mut count = 0u64;
            let mut line_start = 0;

            for i in 0..chunk.len() {
                if chunk[i] == b'\n' {
                    let line = &chunk[line_start..i];
                    line_start = i + 1;
                    if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                        continue;
                    }
                    if let Some((tag_id, image_id)) = parse_tag_line(line) {
                        let tid = tag_id as usize;
                        if tid < MAX_TAG_ID {
                            bitmaps[tid].insert(image_id as u32);
                        }
                        count += 1;
                    }
                }
            }
            total_ref.fetch_add(count, Ordering::Relaxed);
            let t = total_ref.load(Ordering::Relaxed);
            eprintln!("  tags: thread done, {}M total so far", t / 1_000_000);
            bitmaps
        })
        .collect();

    // Merge: OR all thread-local Vecs into the first one.
    let mut merged_vec = thread_results.into_iter().reduce(|mut dst, src| {
        for (i, bm) in src.into_iter().enumerate() {
            if !bm.is_empty() {
                dst[i] |= bm;
            }
        }
        dst
    }).unwrap_or_else(|| vec![]);

    // Convert Vec<RoaringBitmap> to HashMap<u64, RoaringBitmap> (only non-empty)
    let mut result: HashMap<u64, RoaringBitmap> = HashMap::new();
    for (i, bm) in merged_vec.drain(..).enumerate() {
        if !bm.is_empty() {
            result.insert(i as u64, bm);
        }
    }

    let t = total.load(Ordering::Relaxed);
    progress.tag_rows.store(t, Ordering::Release);
    eprintln!("  Tags total: {} rows, {} distinct tag IDs", t, result.len());
    Ok(result)
}

// ---------------------------------------------------------------------------
// Multi-value CSV processor (tools, techniques)
// ---------------------------------------------------------------------------

/// Process a two-column (value_id, image_id) CSV: build filter bitmaps + docstore tuples.
/// Uses mmap + parallel byte-range splitting.
fn process_multi_value_csv(
    stage_dir: &Path,
    filename: &str,
    parse_fn: fn(&[u8]) -> Option<(i64, i64)>,
    bulk_writer: &Arc<crate::docstore::BulkWriter>,
    field_name: &str,
) -> Result<HashMap<u64, RoaringBitmap>, String> {
    let file = std::fs::File::open(stage_dir.join(filename))
        .map_err(|e| format!("open {filename}: {e}"))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap {filename}: {e}"))?;
    let data = &mmap[..];

    let field_idx = bulk_writer.field_to_idx().get(field_name).copied();
    let ranges = split_mmap_ranges(data, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;

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
                    if let Some((value_id, image_id)) = parse_fn(line) {
                        let slot = image_id as u32;
                        let val = value_id as u64;
                        bitmaps.entry(val).or_insert_with(RoaringBitmap::new).insert(slot);
                        if let Some(fidx) = field_idx {
                            let value = rmp_serde::to_vec(&(value_id as i64)).unwrap_or_default();
                            bulk_writer.append_tuple_raw(slot, fidx, &value);
                        }
                        count += 1;
                    }
                }
            }
            total_ref.fetch_add(count, Ordering::Relaxed);
            bitmaps
        })
        .collect();

    // Merge
    let mut merged: HashMap<u64, RoaringBitmap> = HashMap::new();
    for bitmaps in thread_results {
        for (val, bm) in bitmaps {
            merged.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
        }
    }

    eprintln!("  {filename}: {} rows processed", total.load(Ordering::Relaxed));
    Ok(merged)
}

// ---------------------------------------------------------------------------
// Images CSV processor
// ---------------------------------------------------------------------------

/// Process images.csv: build all scalar filter bitmaps, sort bitmaps, alive bitmap,
/// and append docstore tuples for every field.
///
/// Uses mmap + parallel byte-range splitting (same pattern as tags).
/// Each thread builds thread-local bitmaps and appends docstore tuples concurrently.
fn process_images_csv(
    stage_dir: &Path,
    bulk_writer: &Arc<crate::docstore::BulkWriter>,
    progress: &Arc<LoadProgress>,
    post_map: &HashMap<i64, (Option<i64>, String, Option<i64>)>,
    schema: &crate::config::DataSchema,
    filter_set: &HashSet<String>,
    sort_bits: &HashMap<String, u8>,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
) -> Result<ImageCsvResult, String> {
    let file = std::fs::File::open(stage_dir.join("images.csv"))
        .map_err(|e| format!("open images.csv: {e}"))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap images.csv: {e}"))?;
    let data = &mmap[..];
    let file_len = data.len();
    eprintln!("  images: mmap'd {} ({:.1} GB)", file_len, file_len as f64 / (1024.0 * 1024.0 * 1024.0));

    // Fields to process (exclude multi-value and metrics fields)
    let img_filter_names: Vec<String> = filter_names
        .iter()
        .filter(|n| !["tagIds", "toolIds", "techniqueIds", "modelVersionIds", "baseModel"].contains(&n.as_str()))
        .cloned()
        .collect();
    let img_sort_configs: Vec<(String, u8)> = sort_configs
        .iter()
        .filter(|(n, _)| !["reactionCount", "commentCount", "collectedCount"].contains(&n.as_str()))
        .cloned()
        .collect();

    // Split file into byte ranges (one per thread), align to newlines.
    let num_threads = rayon::current_num_threads();
    let chunk_size = file_len / num_threads;
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
        let end = end.min(file_len);
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }

    let total = AtomicU64::new(0);
    let total_ref = &total;

    // Thread-local result: (filter_maps, sort_maps, alive, count)
    type ThreadResult = (
        HashMap<String, HashMap<u64, RoaringBitmap>>,
        HashMap<String, HashMap<usize, RoaringBitmap>>,
        RoaringBitmap,
        u64,
    );

    let thread_results: Vec<ThreadResult> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &data[range_start..range_end];

            // Thread-local bitmap accumulators
            let mut filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>> =
                img_filter_names.iter().map(|n| (n.clone(), HashMap::new())).collect();
            let mut sort_maps: HashMap<String, HashMap<usize, RoaringBitmap>> =
                img_sort_configs.iter().map(|(n, _)| (n.clone(), HashMap::new())).collect();
            let mut alive = RoaringBitmap::new();
            let mut count = 0u64;
            let mut line_start = 0;

            for i in 0..chunk.len() {
                if chunk[i] == b'\n' {
                    let line = &chunk[line_start..i];
                    line_start = i + 1;
                    if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                        continue;
                    }

                    let mut row = match parse_image_row(line) {
                        Some(r) => r,
                        None => continue,
                    };

                    // Enrich from Post lookup (read-only, shared across threads)
                    if let Some(post_id) = row.post_id {
                        if let Some((pub_secs, avail, mv_id)) = post_map.get(&post_id) {
                            row.published_at_secs = *pub_secs;
                            row.availability = avail.clone();
                            row.posted_to_id = *mv_id;
                        }
                    }

                    let slot = row.id as u32;

                    alive.insert(slot);
                    build_image_filter_bitmaps(&row, slot, filter_set, schema, &mut filter_maps);
                    build_image_sort_bitmaps(&row, slot, sort_bits, &mut sort_maps);
                    append_image_docstore_tuples(&row, slot, bulk_writer);

                    count += 1;
                    if count % LOG_INTERVAL == 0 {
                        let t = total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed) + LOG_INTERVAL;
                        eprintln!("  images: {}M rows...", t / 1_000_000);
                    }
                }
            }
            // Flush remaining count
            total_ref.fetch_add(count % LOG_INTERVAL, Ordering::Relaxed);

            (filter_maps, sort_maps, alive, count)
        })
        .collect();

    // Merge all thread-local results
    let mut merged_filters: HashMap<String, HashMap<u64, RoaringBitmap>> =
        img_filter_names.iter().map(|n| (n.clone(), HashMap::new())).collect();
    let mut merged_sorts: HashMap<String, HashMap<usize, RoaringBitmap>> =
        img_sort_configs.iter().map(|(n, _)| (n.clone(), HashMap::new())).collect();
    let mut merged_alive = RoaringBitmap::new();
    let mut merged_count = 0u64;

    for (filter_maps, sort_maps, alive, count) in thread_results {
        merged_alive |= alive;
        merged_count += count;

        for (field, values) in filter_maps {
            let dest = merged_filters.entry(field).or_default();
            for (val, bm) in values {
                dest.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
            }
        }
        for (field, layers) in sort_maps {
            let dest = merged_sorts.entry(field).or_default();
            for (bit, bm) in layers {
                dest.entry(bit).and_modify(|e| *e |= &bm).or_insert(bm);
            }
        }
    }

    progress.image_rows.store(merged_count, Ordering::Release);

    Ok(ImageCsvResult {
        row_count: merged_count,
        filter_maps: merged_filters,
        sort_maps: merged_sorts,
        alive: merged_alive,
    })
}

/// Build filter bitmaps for a single image row.
fn build_image_filter_bitmaps(
    row: &super::copy_queries::CopyImageRow,
    slot: u32,
    filter_set: &HashSet<String>,
    schema: &crate::config::DataSchema,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    if filter_set.contains("nsfwLevel") {
        insert_filter(filter_maps, "nsfwLevel", row.nsfw_level as u64, slot);
    }
    if filter_set.contains("userId") {
        insert_filter(filter_maps, "userId", row.user_id as u64, slot);
    }
    if filter_set.contains("type") {
        let key = resolve_type_key(&row.image_type, schema);
        insert_filter(filter_maps, "type", key, slot);
    }
    if filter_set.contains("availability") {
        let key = resolve_availability_key(&row.availability, schema);
        insert_filter(filter_maps, "availability", key, slot);
    }
    if filter_set.contains("blockedFor") && row.blocked_for.is_some() {
        let key = resolve_blocked_for_key(schema);
        insert_filter(filter_maps, "blockedFor", key, slot);
    }
    if filter_set.contains("postId") {
        insert_filter(
            filter_maps,
            "postId",
            row.post_id.unwrap_or(0) as u64,
            slot,
        );
    }
    if filter_set.contains("postedToId") {
        insert_filter(
            filter_maps,
            "postedToId",
            row.posted_to_id.unwrap_or(0) as u64,
            slot,
        );
    }
    if filter_set.contains("hasMeta") {
        insert_filter(
            filter_maps,
            "hasMeta",
            if row.has_meta() { 1 } else { 0 },
            slot,
        );
    }
    if filter_set.contains("onSite") {
        insert_filter(
            filter_maps,
            "onSite",
            if row.on_site() { 1 } else { 0 },
            slot,
        );
    }
    if filter_set.contains("poi") {
        insert_filter(
            filter_maps,
            "poi",
            if row.poi() { 1 } else { 0 },
            slot,
        );
    }
    if filter_set.contains("minor") {
        insert_filter(
            filter_maps,
            "minor",
            if row.minor() { 1 } else { 0 },
            slot,
        );
    }
    if filter_set.contains("isPublished") {
        let published = row.published_at_secs.unwrap_or(0) > 0;
        insert_filter(
            filter_maps,
            "isPublished",
            if published { 1 } else { 0 },
            slot,
        );
    }
    if filter_set.contains("isRemix") {
        insert_filter(filter_maps, "isRemix", 0, slot);
    }
}

/// Build sort bitmaps for a single image row.
fn build_image_sort_bitmaps(
    row: &super::copy_queries::CopyImageRow,
    slot: u32,
    sort_bits: &HashMap<String, u8>,
    sort_maps: &mut HashMap<String, HashMap<usize, RoaringBitmap>>,
) {
    if let Some(&bits) = sort_bits.get("sortAt") {
        let sort_at = row.sort_at_secs() as u32;
        insert_sort_bits(sort_maps, "sortAt", sort_at, bits, slot);
    }
    if let Some(&bits) = sort_bits.get("publishedAt") {
        let pub_secs = row.published_at_secs.unwrap_or(0) as u32;
        insert_sort_bits(sort_maps, "publishedAt", pub_secs, bits, slot);
    }
    if let Some(&bits) = sort_bits.get("id") {
        insert_sort_bits(sort_maps, "id", slot, bits, slot);
    }
}

/// Append docstore tuples for all image scalar fields.
fn append_image_docstore_tuples(
    row: &super::copy_queries::CopyImageRow,
    slot: u32,
    bulk_writer: &Arc<crate::docstore::BulkWriter>,
) {
    let field_idx = bulk_writer.field_to_idx();

    // Helper macro to reduce boilerplate
    macro_rules! append_field {
        ($name:expr, $value:expr) => {
            if let Some(&fidx) = field_idx.get($name) {
                let value = rmp_serde::to_vec(&$value).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &value);
            }
        };
    }

    append_field!("id", (row.id as i64));
    append_field!("nsfwLevel", (row.nsfw_level as i64));
    append_field!("userId", (row.user_id as i64));
    append_field!("type", row.image_type.as_str());
    append_field!("postId", (row.post_id.unwrap_or(0) as i64));
    append_field!("postedToId", (row.posted_to_id.unwrap_or(0) as i64));
    append_field!("availability", row.availability.as_str());
    append_field!("hasMeta", row.has_meta());
    append_field!("onSite", row.on_site());
    append_field!("poi", row.poi());
    append_field!("minor", row.minor());

    // Sort fields
    let sort_at = row.sort_at_secs() as i64;
    append_field!("sortAt", sort_at);
    append_field!("sortAtUnix", (sort_at * 1000));
    let published_at_ms = row.published_at_secs.unwrap_or(0) * 1000;
    append_field!("publishedAtUnix", published_at_ms);

    // isPublished
    let published = row.published_at_secs.unwrap_or(0) > 0;
    append_field!("isPublished", published);

    // Blocked
    if let Some(ref bf) = row.blocked_for {
        append_field!("blockedFor", bf.as_str());
    }

    // URL and hash
    if let Some(ref url) = row.url {
        append_field!("url", url.as_str());
    }
    if let Some(ref hash) = row.hash {
        append_field!("hash", hash.as_str());
    }
}

// ---------------------------------------------------------------------------
// Resources CSV processor
// ---------------------------------------------------------------------------

/// Process resources.csv: build modelVersionIds + baseModel filter bitmaps,
/// OR resource-level poi into existing poi filter bitmap docstore tuples.
/// Uses mmap + parallel byte-range splitting.
fn process_resources_csv(
    stage_dir: &Path,
    bulk_writer: &Arc<crate::docstore::BulkWriter>,
    progress: &Arc<LoadProgress>,
    schema: &crate::config::DataSchema,
    filter_set: &HashSet<String>,
    mv_map: &HashMap<i64, (Option<String>, i64)>,
    model_map: &HashMap<i64, (bool, String)>,
) -> Result<ResourceCsvResult, String> {
    let file = std::fs::File::open(stage_dir.join("resources.csv"))
        .map_err(|e| format!("open resources.csv: {e}"))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap resources.csv: {e}"))?;
    let data = &mmap[..];

    let has_mv = filter_set.contains("modelVersionIds");
    let has_bm = filter_set.contains("baseModel");
    let has_poi = filter_set.contains("poi");
    let mv_field_idx = bulk_writer.field_to_idx().get("modelVersionIds").copied();
    let base_model_field_idx = bulk_writer.field_to_idx().get("baseModel").copied();

    let ranges = split_mmap_ranges(data, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;

    let thread_results: Vec<HashMap<String, HashMap<u64, RoaringBitmap>>> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &data[range_start..range_end];
            let mut filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
            if has_mv { filter_maps.insert("modelVersionIds".into(), HashMap::new()); }
            if has_bm { filter_maps.insert("baseModel".into(), HashMap::new()); }
            if has_poi { filter_maps.insert("poi".into(), HashMap::new()); }
            let mut count = 0u64;
            let mut line_start = 0;

            for i in 0..chunk.len() {
                if chunk[i] == b'\n' {
                    let line = &chunk[line_start..i];
                    line_start = i + 1;
                    if line.is_empty() || (line.len() == 1 && line[0] == b'\r') { continue; }

                    let row = match parse_resource_row(line) {
                        Some(r) => r,
                        None => continue,
                    };
                    let slot = row.image_id as u32;

                    if has_mv {
                        filter_maps.get_mut("modelVersionIds").unwrap()
                            .entry(row.model_version_id as u64)
                            .or_insert_with(RoaringBitmap::new)
                            .insert(slot);
                    }
                    if let Some(fidx) = mv_field_idx {
                        let value = rmp_serde::to_vec(&(row.model_version_id as i64)).unwrap_or_default();
                        bulk_writer.append_tuple_raw(slot, fidx, &value);
                    }

                    if let Some((mv_base_model, model_id)) = mv_map.get(&row.model_version_id) {
                        if let Some((poi, model_type)) = model_map.get(model_id) {
                            if model_type == "Checkpoint" {
                                if let Some(ref bm_str) = mv_base_model {
                                    let key = resolve_base_model_key_str(bm_str, schema);
                                    if key > 0 {
                                        insert_filter(&mut filter_maps, "baseModel", key, slot);
                                    }
                                    if let Some(fidx) = base_model_field_idx {
                                        let value = rmp_serde::to_vec(&bm_str.as_str()).unwrap_or_default();
                                        bulk_writer.append_tuple_raw(slot, fidx, &value);
                                    }
                                }
                            }
                            if *poi {
                                insert_filter(&mut filter_maps, "poi", 1, slot);
                            }
                        }
                    }
                    count += 1;
                }
            }
            total_ref.fetch_add(count, Ordering::Relaxed);
            filter_maps
        })
        .collect();

    // Merge
    let mut merged: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
    for fm in thread_results {
        merge_filter_maps(&mut merged, fm);
    }

    let t = total.load(Ordering::Relaxed);
    progress.resource_rows.store(t, Ordering::Release);

    Ok(ResourceCsvResult {
        row_count: t,
        filter_maps: merged,
    })
}

// ---------------------------------------------------------------------------
// Metrics processor
// ---------------------------------------------------------------------------

/// Process metrics.csv directly: mmap + parallel, build sort bitmaps + docstore tuples.
/// TSV format: entityId\treactionCount\tcommentCount\tcollectedCount
/// No intermediate HashMap — parse line → bitmaps + docstore in one pass.
fn process_metrics_csv(
    stage_dir: &Path,
    bulk_writer: &Arc<crate::docstore::BulkWriter>,
    sort_bits: &HashMap<String, u8>,
    sort_configs: &[(String, u8)],
) -> Result<HashMap<String, HashMap<usize, RoaringBitmap>>, String> {
    let path = stage_dir.join("metrics.csv");
    if !path.exists() {
        eprintln!("  No metrics.csv found — metric sort fields will be 0");
        return Ok(HashMap::new());
    }

    let file = std::fs::File::open(&path)
        .map_err(|e| format!("open metrics.csv: {e}"))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| format!("mmap metrics.csv: {e}"))?;
    let data = &mmap[..];
    eprintln!("  metrics: mmap'd {} ({:.1} MB)", data.len(), data.len() as f64 / (1024.0 * 1024.0));

    let metric_sort_configs: Vec<(String, u8)> = sort_configs
        .iter()
        .filter(|(n, _)| ["reactionCount", "commentCount", "collectedCount"].contains(&n.as_str()))
        .cloned()
        .collect();

    let reaction_bits = sort_bits.get("reactionCount").copied();
    let comment_bits = sort_bits.get("commentCount").copied();
    let collected_bits = sort_bits.get("collectedCount").copied();
    let reaction_field_idx = bulk_writer.field_to_idx().get("reactionCount").copied();
    let comment_field_idx = bulk_writer.field_to_idx().get("commentCount").copied();
    let collected_field_idx = bulk_writer.field_to_idx().get("collectedCount").copied();

    let ranges = split_mmap_ranges(data, rayon::current_num_threads());
    let total = AtomicU64::new(0);
    let total_ref = &total;

    let thread_results: Vec<HashMap<String, HashMap<usize, RoaringBitmap>>> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &data[range_start..range_end];
            let mut sort_maps: HashMap<String, HashMap<usize, RoaringBitmap>> =
                metric_sort_configs.iter().map(|(n, _)| (n.clone(), HashMap::new())).collect();
            let mut count = 0u64;
            let mut line_start = 0;

            for i in 0..chunk.len() {
                if chunk[i] == b'\n' {
                    let line = &chunk[line_start..i];
                    line_start = i + 1;
                    if line.is_empty() || (line.len() == 1 && line[0] == b'\r') { continue; }

                    // Parse TSV: id\treaction\tcomment\tcollected
                    let parts: Vec<&[u8]> = line.splitn(5, |&b| b == b'\t').collect();
                    if parts.len() < 4 { continue; }

                    let slot = fast_parse_u32(parts[0]);
                    if slot == 0 { continue; }
                    let reaction = fast_parse_u32(parts[1]);
                    let comment = fast_parse_u32(parts[2]);
                    let collected = fast_parse_u32(parts[3]);

                    // Sort bitmaps
                    if let Some(bits) = reaction_bits {
                        insert_sort_bits(&mut sort_maps, "reactionCount", reaction, bits, slot);
                    }
                    if let Some(bits) = comment_bits {
                        insert_sort_bits(&mut sort_maps, "commentCount", comment, bits, slot);
                    }
                    if let Some(bits) = collected_bits {
                        insert_sort_bits(&mut sort_maps, "collectedCount", collected, bits, slot);
                    }

                    // Docstore tuples
                    if let Some(fidx) = reaction_field_idx {
                        let value = rmp_serde::to_vec(&(reaction as i64)).unwrap_or_default();
                        bulk_writer.append_tuple_raw(slot, fidx, &value);
                    }
                    if let Some(fidx) = comment_field_idx {
                        let value = rmp_serde::to_vec(&(comment as i64)).unwrap_or_default();
                        bulk_writer.append_tuple_raw(slot, fidx, &value);
                    }
                    if let Some(fidx) = collected_field_idx {
                        let value = rmp_serde::to_vec(&(collected as i64)).unwrap_or_default();
                        bulk_writer.append_tuple_raw(slot, fidx, &value);
                    }

                    count += 1;
                }
            }
            total_ref.fetch_add(count, Ordering::Relaxed);
            sort_maps
        })
        .collect();

    // Merge
    let mut merged: HashMap<String, HashMap<usize, RoaringBitmap>> = HashMap::new();
    for sm in thread_results {
        merge_sort_maps(&mut merged, sm);
    }

    eprintln!("  metrics: {} rows processed", total.load(Ordering::Relaxed));
    Ok(merged)
}

// ---------------------------------------------------------------------------
// Shared mmap + parallel helpers
// ---------------------------------------------------------------------------

/// Split an mmap'd file into byte ranges aligned to newlines (one per thread).
fn split_mmap_ranges(data: &[u8], num_threads: usize) -> Vec<(usize, usize)> {
    let file_len = data.len();
    let chunk_size = file_len / num_threads;
    let mut ranges = Vec::with_capacity(num_threads);
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
        }
        .min(file_len);
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

/// Merge thread-local filter maps into a destination (OR bitmaps).
fn merge_filter_maps(
    dest: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
    src: HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    for (field, values) in src {
        let d = dest.entry(field).or_default();
        for (val, bm) in values {
            d.entry(val).and_modify(|e| *e |= &bm).or_insert(bm);
        }
    }
}

/// Merge thread-local sort maps into a destination (OR bitmaps).
fn merge_sort_maps(
    dest: &mut HashMap<String, HashMap<usize, RoaringBitmap>>,
    src: HashMap<String, HashMap<usize, RoaringBitmap>>,
) {
    for (field, layers) in src {
        let d = dest.entry(field).or_default();
        for (bit, bm) in layers {
            d.entry(bit).and_modify(|e| *e |= &bm).or_insert(bm);
        }
    }
}

/// Fast u32 parse from byte slice (no allocation).
#[inline]
fn fast_parse_u32(bytes: &[u8]) -> u32 {
    let mut n: u32 = 0;
    for &b in bytes {
        if b >= b'0' && b <= b'9' {
            n = n * 10 + (b - b'0') as u32;
        } else if b == b'\r' || b == b' ' {
            break;
        }
    }
    n
}

// ---------------------------------------------------------------------------
// Shared bitmap helpers
// ---------------------------------------------------------------------------

#[inline]
fn insert_filter(
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
    field: &str,
    value: u64,
    slot: u32,
) {
    if let Some(fm) = filter_maps.get_mut(field) {
        fm.entry(value)
            .or_insert_with(RoaringBitmap::new)
            .insert(slot);
    }
}

#[inline]
fn insert_sort_bits(
    sort_maps: &mut HashMap<String, HashMap<usize, RoaringBitmap>>,
    field: &str,
    value: u32,
    bits: u8,
    slot: u32,
) {
    if let Some(sm) = sort_maps.get_mut(field) {
        for bit in 0..(bits as usize) {
            if (value >> bit) & 1 == 1 {
                sm.entry(bit)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }
        }
    }
}

/// Save a single filter field's bitmaps to BitmapFs using write_filter_bucket.
fn save_filter_field_to_disk(
    fs: &BitmapFs,
    field_name: &str,
    values: &HashMap<u64, RoaringBitmap>,
) -> Result<u64, String> {
    let mut by_bucket: HashMap<u8, Vec<(u64, &RoaringBitmap)>> = HashMap::new();
    for (value, bm) in values {
        let bucket = ((*value >> 8) & 0xFF) as u8;
        by_bucket
            .entry(bucket)
            .or_default()
            .push((*value, bm));
    }
    let mut total_bytes = 0u64;
    for (bucket, entries) in &by_bucket {
        for (_, bm) in entries {
            total_bytes += bm.serialized_size() as u64;
        }
        fs.write_filter_bucket(field_name, *bucket, entries)
            .map_err(|e| format!("write_filter_bucket({field_name}, {bucket:02x}): {e}"))?;
    }
    Ok(total_bytes)
}

/// Save sort field bitmaps to BitmapFs.
fn save_sort_field_to_disk(
    fs: &BitmapFs,
    field_name: &str,
    bit_map: &HashMap<usize, RoaringBitmap>,
    num_bits: u8,
) -> Result<(), String> {
    let empty = RoaringBitmap::new();
    let mut layers: Vec<&RoaringBitmap> = Vec::with_capacity(num_bits as usize);
    for bit in 0..(num_bits as usize) {
        layers.push(bit_map.get(&bit).unwrap_or(&empty));
    }
    fs.write_sort_layers(field_name, &layers)
        .map_err(|e| format!("write_sort_layers({field_name}): {e}"))
}

// ---------------------------------------------------------------------------
// String map resolution helpers
// ---------------------------------------------------------------------------

fn resolve_type_key(image_type: &str, schema: &crate::config::DataSchema) -> u64 {
    schema
        .fields
        .iter()
        .find(|f| f.target == "type")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| {
            map.get(&image_type.to_lowercase())
                .or_else(|| map.get(image_type))
                .copied()
        })
        .unwrap_or(0) as u64
}

fn resolve_availability_key(availability: &str, schema: &crate::config::DataSchema) -> u64 {
    schema
        .fields
        .iter()
        .find(|f| f.target == "availability")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| {
            map.get(&availability.to_lowercase())
                .or_else(|| map.get(availability))
                .copied()
        })
        .unwrap_or(0) as u64
}

fn resolve_blocked_for_key(schema: &crate::config::DataSchema) -> u64 {
    schema
        .fields
        .iter()
        .find(|f| f.target == "blockedFor")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| map.get("blocked").or_else(|| map.values().next()).copied())
        .unwrap_or(1) as u64
}

fn resolve_base_model_key_str(base_model: &str, schema: &crate::config::DataSchema) -> u64 {
    schema
        .fields
        .iter()
        .find(|f| f.target == "baseModel")
        .and_then(|f| f.string_map.as_ref())
        .and_then(|map| {
            map.get(&base_model.to_lowercase())
                .or_else(|| map.get(base_model))
                .copied()
        })
        .unwrap_or(0) as u64
}

// ---------------------------------------------------------------------------
// Enrichment helpers (reused from scatter_gather)
// ---------------------------------------------------------------------------

fn load_post_map(
    stage_dir: &Path,
) -> Result<HashMap<i64, (Option<i64>, String, Option<i64>)>, String> {
    let mut map = HashMap::new();
    let file = std::io::BufReader::new(
        std::fs::File::open(stage_dir.join("posts.csv"))
            .map_err(|e| format!("open posts.csv: {e}"))?,
    );
    for line in file.split(b'\n') {
        let line = line.map_err(|e| format!("read posts.csv: {e}"))?;
        if let Some(row) = parse_post_row(&line) {
            map.insert(
                row.id,
                (row.published_at_secs, row.availability, row.model_version_id),
            );
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

// ---------------------------------------------------------------------------
// Fast inline CSV parsers (reused from scatter_gather)
// ---------------------------------------------------------------------------

/// Fast inline tag CSV parser — two integers, no quoting.
#[inline]
fn parse_tag_line(line: &[u8]) -> Option<(i64, i64)> {
    let comma = line.iter().position(|&b| b == b',')?;
    let tag_id = parse_i64_fast(&line[..comma])?;
    let rest = &line[comma + 1..];
    let end = if rest.last() == Some(&b'\r') {
        rest.len() - 1
    } else {
        rest.len()
    };
    let image_id = parse_i64_fast(&rest[..end])?;
    Some((tag_id, image_id))
}

#[inline]
fn parse_i64_fast(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (negative, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };
    if start >= bytes.len() {
        return None;
    }
    let mut val: i64 = 0;
    for &b in &bytes[start..] {
        if b < b'0' || b > b'9' {
            return None;
        }
        val = val.wrapping_mul(10).wrapping_add((b - b'0') as i64);
    }
    if negative {
        Some(-val)
    } else {
        Some(val)
    }
}
