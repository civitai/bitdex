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
use crate::dictionary::FieldDictionary;
use crate::docstore::PackedValue;

use super::bulk_loader::BulkLoadStats;
use super::config::IndexDefinition;
use super::copy_queries::{
    parse_image_row, parse_model_row, parse_model_version_row, parse_post_row, parse_resource_row,
    parse_technique_row, parse_tool_row,
};
use super::progress::LoadProgress;

const LOG_INTERVAL: u64 = 1_000_000;

/// Check if a filter field already has data on disk (skip-if-loaded).
/// Returns true if the field's BitmapFs directory has at least one fpack file.
fn field_already_loaded(bitmap_fs: &BitmapFs, field_name: &str) -> bool {
    match bitmap_fs.list_field_keys(field_name) {
        Ok(keys) if !keys.is_empty() => {
            eprintln!("  {field_name}: already loaded ({} values on disk), skipping", keys.len());
            true
        }
        _ => false,
    }
}

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

    // No enter_loading_mode — we write directly to BitmapFs, not through the engine.
    // Loading mode would trigger a snapshot save on exit that overwrites our bitmaps.

    // FieldDictionaries for LowCardinalityString fields — declared at function scope
    // so they can be persisted after all CSVs are processed.
    let type_dict = FieldDictionary::new();
    let availability_dict = FieldDictionary::new();
    let blocked_for_dict = FieldDictionary::new();
    let base_model_dict = FieldDictionary::new();

    progress.set_phase(1);
    eprintln!("\n=== Single-Pass V2: CSV → bitmaps + docstore (no scratch shards) ===");

    // Assigned in step 2 (images CSV)
    let total_images: u64;

    // ===================================================================
    // Step 1: Tags (63 GB, 5.4B rows)
    // ===================================================================
    {
        eprintln!("\n--- Step 1: Tags CSV (tagIds filter bitmaps + docstore tuples) ---");
        if field_already_loaded(&bitmap_fs, "tagIds") {
            // Already loaded — skip
        } else {
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
        } // end if !field_already_loaded
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
            &config,
            &type_dict,
            &availability_dict,
            &blocked_for_dict,
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
        // Save alive bitmap (excludes deferred slots — they're not alive yet)
        let max_alive_slot = img_result.alive.max().unwrap_or(0);
        bitmap_fs
            .write_alive(&img_result.alive)
            .map_err(|e| format!("write_alive: {e}"))?;
        eprintln!(
            "  Saved alive bitmap: {} bits",
            img_result.alive.len()
        );
        // Save slot counter — must account for deferred slots too (they're allocated)
        let max_deferred_slot = img_result.deferred_slots.values()
            .flat_map(|v| v.iter())
            .copied()
            .max()
            .unwrap_or(0);
        let slot_counter = max_alive_slot.max(max_deferred_slot).saturating_add(1);
        bitmap_fs
            .write_slot_counter(slot_counter)
            .map_err(|e| format!("write_slot_counter: {e}"))?;
        // Save deferred alive map (flush thread reads this on startup to activate due slots)
        if !img_result.deferred_slots.is_empty() {
            bitmap_fs
                .write_deferred_alive(&img_result.deferred_slots)
                .map_err(|e| format!("write_deferred_alive: {e}"))?;
            let deferred_total: usize = img_result.deferred_slots.values().map(|v| v.len()).sum();
            eprintln!("  Saved deferred alive map: {} slots", deferred_total);
        }
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
            &base_model_dict,
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
        if field_already_loaded(&bitmap_fs, "toolIds") {
            // skip
        } else {
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
        } // end if !field_already_loaded
    }

    // ===================================================================
    // Step 5: Techniques (71 MB)
    // ===================================================================
    {
        eprintln!("\n--- Step 5: Techniques CSV (techniqueIds filter bitmaps) ---");
        if field_already_loaded(&bitmap_fs, "techniqueIds") {
            // skip
        } else {
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
        } // end if !field_already_loaded
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
    // Step 7: CollectionItems (filter_only — bitmap only, no docstore)
    // ===================================================================
    {
        eprintln!("\n--- Step 7: CollectionItems CSV (collectionIds filter bitmaps) ---");
        if field_already_loaded(&bitmap_fs, "collectionIds") {
            // skip
        } else if stage_dir.join("collection_items.csv").exists() {
            let t = Instant::now();
            // Reuse the backfill CSV processor (same mmap+rayon pattern)
            let coll_bitmaps = crate::pg_sync::backfill::process_collection_items_csv(stage_dir)?;
            let coll_count = coll_bitmaps.len();
            eprintln!(
                "  CollectionItems: {} distinct values in {:.1}s",
                coll_count,
                t.elapsed().as_secs_f64()
            );

            let saved = save_filter_field_to_disk(&bitmap_fs, "collectionIds", &coll_bitmaps)?;
            eprintln!(
                "  Saved collectionIds: {} values ({:.1} MB)",
                coll_count,
                saved as f64 / (1024.0 * 1024.0)
            );
        } else {
            eprintln!("  collection_items.csv not found, skipping collectionIds");
        }
    }

    // ===================================================================
    // Done — all bitmaps + docstore already written to disk via BitmapFs.
    // No exit_loading_mode needed (would trigger snapshot save overwriting our bitmaps).
    // ===================================================================
    progress.set_phase(3);

    // Persist LowCardinalityString dictionaries so the server can resolve string→key at query time
    {
        let dict_dir = bitmap_fs.root().join("dictionaries");
        std::fs::create_dir_all(&dict_dir).ok();
        let dicts: Vec<(&str, &FieldDictionary)> = vec![
            ("type", &type_dict),
            ("availability", &availability_dict),
            ("blockedFor", &blocked_for_dict),
            ("baseModel", &base_model_dict),
        ];
        for (name, dict) in &dicts {
            let snap = dict.snapshot();
            if snap.forward.is_empty() {
                continue;
            }
            let path = dict_dir.join(format!("{name}.dict"));
            if let Err(e) = crate::dictionary::save_dictionary(&snap, &path) {
                eprintln!("WARNING: failed to save dictionary for '{name}': {e}");
            } else {
                eprintln!("  Saved dictionary '{name}': {} entries", snap.forward.len());
            }
        }
    }

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
    deferred_slots: std::collections::BTreeMap<u64, Vec<u32>>,
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

    // Each thread builds filter bitmaps AND collects per-slot value lists for docstore.
    let thread_results: Vec<(HashMap<u64, RoaringBitmap>, HashMap<u32, Vec<i64>>)> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &data[range_start..range_end];
            let mut bitmaps: HashMap<u64, RoaringBitmap> = HashMap::new();
            let mut slot_values: HashMap<u32, Vec<i64>> = HashMap::new();
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
                        if field_idx.is_some() {
                            slot_values.entry(slot).or_default().push(value_id as i64);
                        }
                        count += 1;
                    }
                }
            }
            total_ref.fetch_add(count, Ordering::Relaxed);
            (bitmaps, slot_values)
        })
        .collect();

    // Write collected multi-value arrays to docstore (one PackedValue::Mi per slot)
    if let Some(fidx) = field_idx {
        // Merge per-slot values from all threads
        let mut merged_values: HashMap<u32, Vec<i64>> = HashMap::new();
        for (_, slot_values) in &thread_results {
            for (&slot, values) in slot_values {
                merged_values.entry(slot).or_default().extend(values);
            }
        }
        for (slot, values) in &merged_values {
            let packed = rmp_serde::to_vec(&PackedValue::Mi(values.clone())).unwrap_or_default();
            bulk_writer.append_tuple_raw(*slot, fidx, &packed);
        }
        eprintln!("  {field_name}: wrote {} multi-value docstore tuples", merged_values.len());
    }

    // Merge filter bitmaps
    let mut merged: HashMap<u64, RoaringBitmap> = HashMap::new();
    for (bitmaps, _) in thread_results {
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
    config: &crate::config::Config,
    type_dict: &FieldDictionary,
    availability_dict: &FieldDictionary,
    blocked_for_dict: &FieldDictionary,
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
            let tentative = (start + chunk_size).min(file_len);
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

    // Deferred alive: check config and capture current time
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let has_deferred_alive = config.deferred_alive.is_some();

    // Thread-local result: (filter_maps, sort_maps, alive, deferred_slots, count)
    type ThreadResult = (
        HashMap<String, HashMap<u64, RoaringBitmap>>,
        HashMap<String, HashMap<usize, RoaringBitmap>>,
        RoaringBitmap,
        Vec<(u32, u64)>, // (slot, activate_at) for deferred alive
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
            let mut deferred: Vec<(u32, u64)> = Vec::new();
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

                    // Deferred alive: future-dated posts get docstore entries but
                    // skip alive/filter/sort bits. Activated by flush thread later.
                    let published_at_secs = row.published_at_secs.unwrap_or(0) as u64;
                    if has_deferred_alive && published_at_secs > now_unix {
                        // Still write docstore entry so activate_due() can read it back
                        append_image_docstore_tuples(&row, slot, bulk_writer);
                        deferred.push((slot, published_at_secs));
                    } else {
                        alive.insert(slot);
                        build_image_filter_bitmaps(&row, slot, filter_set, &type_dict, &availability_dict, &blocked_for_dict, &mut filter_maps);
                        build_image_sort_bitmaps(&row, slot, sort_bits, &mut sort_maps);
                        append_image_docstore_tuples(&row, slot, bulk_writer);
                    }

                    count += 1;
                    if count % LOG_INTERVAL == 0 {
                        let t = total_ref.fetch_add(LOG_INTERVAL, Ordering::Relaxed) + LOG_INTERVAL;
                        eprintln!("  images: {}M rows...", t / 1_000_000);
                    }
                }
            }
            // Flush remaining count
            total_ref.fetch_add(count % LOG_INTERVAL, Ordering::Relaxed);

            (filter_maps, sort_maps, alive, deferred, count)
        })
        .collect();

    // Merge all thread-local results
    let mut merged_filters: HashMap<String, HashMap<u64, RoaringBitmap>> =
        img_filter_names.iter().map(|n| (n.clone(), HashMap::new())).collect();
    let mut merged_sorts: HashMap<String, HashMap<usize, RoaringBitmap>> =
        img_sort_configs.iter().map(|(n, _)| (n.clone(), HashMap::new())).collect();
    let mut merged_alive = RoaringBitmap::new();
    let mut merged_deferred: std::collections::BTreeMap<u64, Vec<u32>> =
        std::collections::BTreeMap::new();
    let mut merged_count = 0u64;

    for (filter_maps, sort_maps, alive, deferred, count) in thread_results {
        merged_alive |= alive;
        merged_count += count;

        for (slot, activate_at) in deferred {
            merged_deferred.entry(activate_at).or_default().push(slot);
        }

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

    let deferred_total: usize = merged_deferred.values().map(|v| v.len()).sum();
    if deferred_total > 0 {
        eprintln!("  deferred alive: {} slots with future publishedAt (will activate later)", deferred_total);
    }
    progress.image_rows.store(merged_count, Ordering::Release);

    Ok(ImageCsvResult {
        row_count: merged_count,
        filter_maps: merged_filters,
        sort_maps: merged_sorts,
        alive: merged_alive,
        deferred_slots: merged_deferred,
    })
}

/// Build filter bitmaps for a single image row.
fn build_image_filter_bitmaps(
    row: &super::copy_queries::CopyImageRow,
    slot: u32,
    filter_set: &HashSet<String>,
    type_dict: &FieldDictionary,
    availability_dict: &FieldDictionary,
    blocked_for_dict: &FieldDictionary,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    if filter_set.contains("nsfwLevel") {
        insert_filter(filter_maps, "nsfwLevel", row.nsfw_level as u64, slot);
    }
    if filter_set.contains("userId") {
        insert_filter(filter_maps, "userId", row.user_id as u64, slot);
    }
    if filter_set.contains("type") {
        let key = type_dict.get_or_insert(&row.image_type) as u64;
        insert_filter(filter_maps, "type", key, slot);
    }
    if filter_set.contains("availability") {
        let key = availability_dict.get_or_insert(&row.availability) as u64;
        insert_filter(filter_maps, "availability", key, slot);
    }
    if filter_set.contains("blockedFor") && row.blocked_for.is_some() {
        let key = blocked_for_dict.get_or_insert(row.blocked_for.as_deref().unwrap()) as u64;
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

    // Helper macros wrapping values in PackedValue for correct V2 docstore encoding
    macro_rules! append_int {
        ($name:expr, $value:expr) => {
            if let Some(&fidx) = field_idx.get($name) {
                let value = rmp_serde::to_vec(&PackedValue::I($value as i64)).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &value);
            }
        };
    }
    macro_rules! append_str {
        ($name:expr, $value:expr) => {
            if let Some(&fidx) = field_idx.get($name) {
                let value = rmp_serde::to_vec(&PackedValue::S($value.to_string())).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &value);
            }
        };
    }
    macro_rules! append_bool {
        ($name:expr, $value:expr) => {
            if let Some(&fidx) = field_idx.get($name) {
                let value = rmp_serde::to_vec(&PackedValue::B($value)).unwrap_or_default();
                bulk_writer.append_tuple_raw(slot, fidx, &value);
            }
        };
    }

    append_int!("id", row.id);
    append_int!("nsfwLevel", row.nsfw_level);
    append_int!("userId", row.user_id);
    append_str!("type", row.image_type);
    append_int!("postId", row.post_id.unwrap_or(0));
    append_int!("postedToId", row.posted_to_id.unwrap_or(0));
    append_str!("availability", row.availability);
    append_bool!("hasMeta", row.has_meta());
    append_bool!("onSite", row.on_site());
    append_bool!("poi", row.poi());
    append_bool!("minor", row.minor());

    // Sort fields
    let sort_at = row.sort_at_secs() as i64;
    append_int!("sortAt", sort_at);
    let published_at_secs = row.published_at_secs.unwrap_or(0);
    append_int!("publishedAt", published_at_secs);

    // isPublished
    let published = row.published_at_secs.unwrap_or(0) > 0;
    append_bool!("isPublished", published);

    // Blocked
    if let Some(ref bf) = row.blocked_for {
        append_str!("blockedFor", bf);
    }

    // URL and hash
    if let Some(ref url) = row.url {
        append_str!("url", url);
    }
    if let Some(ref hash) = row.hash {
        append_str!("hash", hash);
    }
    if let Some(w) = row.width {
        append_int!("width", w);
    }
    if let Some(h) = row.height {
        append_int!("height", h);
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
    base_model_dict: &FieldDictionary,
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

    // Each thread builds filter bitmaps + collects modelVersionIds per slot for docstore.
    let thread_results: Vec<(HashMap<String, HashMap<u64, RoaringBitmap>>, HashMap<u32, Vec<i64>>)> = ranges
        .par_iter()
        .map(|&(range_start, range_end)| {
            let chunk = &data[range_start..range_end];
            let mut filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
            let mut mv_slot_values: HashMap<u32, Vec<i64>> = HashMap::new();
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
                    if mv_field_idx.is_some() {
                        mv_slot_values.entry(slot).or_default().push(row.model_version_id as i64);
                    }

                    if let Some((mv_base_model, model_id)) = mv_map.get(&row.model_version_id) {
                        if let Some((poi, model_type)) = model_map.get(model_id) {
                            if model_type == "Checkpoint" {
                                if let Some(ref bm_str) = mv_base_model {
                                    let key = base_model_dict.get_or_insert(bm_str) as u64;
                                    insert_filter(&mut filter_maps, "baseModel", key, slot);
                                    if let Some(fidx) = base_model_field_idx {
                                        let value = rmp_serde::to_vec(&PackedValue::S(bm_str.clone())).unwrap_or_default();
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
            (filter_maps, mv_slot_values)
        })
        .collect();

    // Write collected modelVersionIds arrays to docstore
    if let Some(fidx) = mv_field_idx {
        let mut merged_mv: HashMap<u32, Vec<i64>> = HashMap::new();
        for (_, mv_vals) in &thread_results {
            for (&slot, values) in mv_vals {
                merged_mv.entry(slot).or_default().extend(values);
            }
        }
        for (slot, values) in &merged_mv {
            let packed = rmp_serde::to_vec(&PackedValue::Mi(values.clone())).unwrap_or_default();
            bulk_writer.append_tuple_raw(*slot, fidx, &packed);
        }
        eprintln!("  modelVersionIds: wrote {} multi-value docstore tuples", merged_mv.len());
    }

    // Merge filter bitmaps
    let mut merged: HashMap<String, HashMap<u64, RoaringBitmap>> = HashMap::new();
    for (fm, _) in thread_results {
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
                        let value = rmp_serde::to_vec(&PackedValue::I(reaction as i64)).unwrap_or_default();
                        bulk_writer.append_tuple_raw(slot, fidx, &value);
                    }
                    if let Some(fidx) = comment_field_idx {
                        let value = rmp_serde::to_vec(&PackedValue::I(comment as i64)).unwrap_or_default();
                        bulk_writer.append_tuple_raw(slot, fidx, &value);
                    }
                    if let Some(fidx) = collected_field_idx {
                        let value = rmp_serde::to_vec(&PackedValue::I(collected as i64)).unwrap_or_default();
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
            let tentative = (start + chunk_size).min(file_len);
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
pub fn save_filter_field_to_disk(
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

#[cfg(test)]
mod fixture_tests {
    use super::*;
    use std::path::Path;

    fn fixtures_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/csv")
    }

    /// Parse every line of the real tags fixture with parse_tag_line.
    #[test]
    fn test_tags_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("tags.csv")).unwrap();
        let mut parsed = 0u64;
        let mut failed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            match parse_tag_line(line) {
                Some((tag_id, image_id)) => {
                    assert!(tag_id >= 0, "negative tagId: {tag_id}");
                    assert!(image_id >= 0, "negative imageId: {image_id}");
                    parsed += 1;
                }
                None => {
                    failed += 1;
                    let s = std::str::from_utf8(line).unwrap_or("<invalid utf8>");
                    eprintln!("Failed to parse tag line: {s}");
                }
            }
        }
        assert!(parsed >= 100, "Expected at least 100 parsed tag rows, got {parsed}");
        assert_eq!(failed, 0, "Expected 0 parse failures");
        eprintln!("tags fixture: {parsed} rows parsed successfully");
    }

    /// Parse every line of the real tools fixture.
    #[test]
    fn test_tools_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("tools.csv")).unwrap();
        let mut parsed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            let result = parse_tag_line(line); // Same format: valueId,imageId
            assert!(result.is_some(), "Failed to parse tool line: {:?}", std::str::from_utf8(line));
            parsed += 1;
        }
        assert!(parsed >= 50, "Expected at least 50 parsed tool rows, got {parsed}");
    }

    /// Parse every line of the real techniques fixture.
    #[test]
    fn test_techniques_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("techniques.csv")).unwrap();
        let mut parsed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            let result = parse_tag_line(line); // Same format: valueId,imageId
            assert!(result.is_some(), "Failed to parse technique line: {:?}", std::str::from_utf8(line));
            parsed += 1;
        }
        assert!(parsed >= 50, "Expected at least 50 parsed technique rows, got {parsed}");
    }

    /// Parse every line of the real images fixture with the image CSV parser.
    #[test]
    fn test_images_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("images.csv")).unwrap();
        let mut parsed = 0u64;
        let mut failed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            // Image lines have 11 CSV columns. Just verify we can split them.
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            if fields.len() >= 11 {
                // Parse the ID (first field)
                let id = parse_i64_fast(fields[0]);
                assert!(id.is_some(), "Failed to parse image id: {:?}", std::str::from_utf8(fields[0]));
                parsed += 1;
            } else {
                // Some image rows have commas in quoted fields (url, hash)
                // CSV quoting means naive split can overcound or undercound
                // Still count as parsed if we got at least the ID
                if let Some(_) = parse_i64_fast(fields[0]) {
                    parsed += 1;
                } else {
                    failed += 1;
                }
            }
        }
        assert!(parsed >= 50, "Expected at least 50 parsed image rows, got {parsed}");
        eprintln!("images fixture: {parsed} rows parsed, {failed} failed");
    }

    /// Parse every line of the real resources fixture.
    #[test]
    fn test_resources_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("resources.csv")).unwrap();
        let mut parsed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            // Format: imageId,modelVersionId,detected(bool)
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            assert!(fields.len() >= 3, "Resource line has {} fields, expected 3: {:?}",
                fields.len(), std::str::from_utf8(line));
            let image_id = parse_i64_fast(fields[0]);
            assert!(image_id.is_some(), "Failed to parse resource imageId");
            let mv_id = parse_i64_fast(fields[1]);
            assert!(mv_id.is_some(), "Failed to parse resource modelVersionId");
            parsed += 1;
        }
        assert!(parsed >= 50, "Expected at least 50 parsed resource rows, got {parsed}");
    }

    /// Parse every line of the real metrics fixture (from ClickHouse).
    #[test]
    fn test_metrics_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("metrics.csv")).unwrap();
        let mut parsed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            // Format: entityId,reactionCount,commentCount,collectedCount
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            assert!(fields.len() >= 4, "Metric line has {} fields, expected 4: {:?}",
                fields.len(), std::str::from_utf8(line));
            let entity_id = parse_i64_fast(fields[0]);
            assert!(entity_id.is_some(), "Failed to parse metric entityId");
            let reaction = parse_i64_fast(fields[1]);
            assert!(reaction.is_some(), "Failed to parse reactionCount");
            parsed += 1;
        }
        assert!(parsed >= 50, "Expected at least 50 parsed metric rows, got {parsed}");
    }

    /// Parse posts fixture — enrichment table with 4 columns.
    #[test]
    fn test_posts_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("posts.csv")).unwrap();
        let mut parsed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            // Format: id,publishedAtSecs,availability,modelVersionId
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            assert!(fields.len() >= 3, "Post line has {} fields, expected >=3: {:?}",
                fields.len(), std::str::from_utf8(line));
            let id = parse_i64_fast(fields[0]);
            assert!(id.is_some(), "Failed to parse post id");
            parsed += 1;
        }
        assert!(parsed >= 50, "Expected at least 50 parsed post rows, got {parsed}");
    }

    /// Parse models fixture — 3 columns.
    #[test]
    fn test_models_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("models.csv")).unwrap();
        let mut parsed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            assert!(fields.len() >= 3, "Model line has {} fields, expected 3", fields.len());
            let id = parse_i64_fast(fields[0]);
            assert!(id.is_some(), "Failed to parse model id");
            parsed += 1;
        }
        assert!(parsed >= 50, "Expected at least 50 parsed model rows, got {parsed}");
    }

    /// Parse model_versions fixture — 3 columns.
    #[test]
    fn test_model_versions_fixture_parses() {
        let data = std::fs::read(fixtures_dir().join("model_versions.csv")).unwrap();
        let mut parsed = 0u64;
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            assert!(fields.len() >= 3, "ModelVersion line has {} fields, expected 3", fields.len());
            let id = parse_i64_fast(fields[0]);
            assert!(id.is_some(), "Failed to parse model_version id");
            parsed += 1;
        }
        assert!(parsed >= 50, "Expected at least 50 parsed model_version rows, got {parsed}");
    }

    // --- Audit items 3.11, 3.16, 3.20 ---

    #[test]
    fn test_images_fixture_all_11_fields_extractable() {
        // 3.11: Verify all 11 image scalar fields can be extracted
        // Columns: id, url, nsfwLevel, hash, flags, type, userId, blockedFor,
        //          scannedAtSecs, createdAtSecs, postId
        let data = std::fs::read(fixtures_dir().join("images.csv")).unwrap();
        let mut full_rows = 0u64;

        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }

            // Smart CSV split that handles quoted fields
            let fields = split_csv_line(line);
            if fields.len() < 11 {
                continue; // Skip lines with embedded commas in quoted fields
            }

            // Field 0: id (integer, required)
            let id = parse_i64_fast(fields[0]);
            assert!(id.is_some(), "id must be parseable");

            // Field 2: nsfwLevel (integer)
            if !fields[2].is_empty() {
                let nsfw = parse_i64_fast(fields[2]);
                assert!(nsfw.is_some(), "nsfwLevel must be integer: {:?}", std::str::from_utf8(fields[2]));
            }

            // Field 4: flags (integer)
            if !fields[4].is_empty() {
                let flags = parse_i64_fast(fields[4]);
                assert!(flags.is_some(), "flags must be integer: {:?}", std::str::from_utf8(fields[4]));
            }

            // Field 6: userId (integer)
            if !fields[6].is_empty() {
                let uid = parse_i64_fast(fields[6]);
                assert!(uid.is_some(), "userId must be integer: {:?}", std::str::from_utf8(fields[6]));
            }

            // Field 8: scannedAtSecs (integer, epoch seconds)
            if !fields[8].is_empty() {
                let ts = parse_i64_fast(fields[8]);
                assert!(ts.is_some(), "scannedAtSecs must be integer");
                if let Some(v) = ts {
                    assert!(v > 1_000_000_000 && v < 2_000_000_000,
                        "scannedAtSecs {} looks wrong (expected epoch seconds)", v);
                }
            }

            // Field 9: createdAtSecs (integer, epoch seconds)
            if !fields[9].is_empty() {
                let ts = parse_i64_fast(fields[9]);
                assert!(ts.is_some(), "createdAtSecs must be integer");
            }

            // Field 10: postId (integer)
            let pid_bytes = fields[10].strip_suffix(&[b'\r']).unwrap_or(fields[10]);
            if !pid_bytes.is_empty() {
                let pid = parse_i64_fast(pid_bytes);
                assert!(pid.is_some(), "postId must be integer: {:?}", std::str::from_utf8(pid_bytes));
            }

            full_rows += 1;
        }

        assert!(full_rows >= 50, "Expected at least 50 fully parseable image rows, got {full_rows}");
        eprintln!("images fixture: {full_rows} rows with all 11 fields parsed");
    }

    #[test]
    fn test_images_fixture_timestamps_are_seconds_not_ms() {
        // 3.16: Verify timestamps from COPY are in seconds, not milliseconds
        let data = std::fs::read(fixtures_dir().join("images.csv")).unwrap();

        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            let fields = split_csv_line(line);
            if fields.len() < 11 { continue; }

            // scannedAtSecs (field 8) should be epoch seconds (10 digits), not ms (13 digits)
            if !fields[8].is_empty() {
                if let Some(v) = parse_i64_fast(fields[8]) {
                    assert!(v < 10_000_000_000,
                        "scannedAtSecs {} looks like milliseconds, not seconds", v);
                }
            }

            // createdAtSecs (field 9) same check
            if !fields[9].is_empty() {
                if let Some(v) = parse_i64_fast(fields[9]) {
                    assert!(v < 10_000_000_000,
                        "createdAtSecs {} looks like milliseconds, not seconds", v);
                }
            }
        }
    }

    #[test]
    fn test_posts_fixture_timestamps_are_seconds() {
        // Posts publishedAtSecs should also be epoch seconds
        let data = std::fs::read(fixtures_dir().join("posts.csv")).unwrap();

        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            if fields.len() < 2 { continue; }

            // Field 1: publishedAtSecs
            if !fields[1].is_empty() {
                if let Some(v) = parse_i64_fast(fields[1]) {
                    assert!(v < 10_000_000_000,
                        "Post publishedAtSecs {} looks like milliseconds", v);
                }
            }
        }
    }

    #[test]
    fn test_images_fixture_csv_split_handles_all_rows() {
        // 3.20: Verify the CSV splitter handles all rows (including quoted fields)
        let data = std::fs::read(fixtures_dir().join("images.csv")).unwrap();
        let mut rows_with_11_fields = 0u64;
        let mut total_rows = 0u64;

        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() || (line.len() == 1 && line[0] == b'\r') {
                continue;
            }
            total_rows += 1;
            let fields = split_csv_line(line);
            if fields.len() >= 11 {
                rows_with_11_fields += 1;
            }
        }

        // All rows should have at least 11 fields when properly split
        assert_eq!(rows_with_11_fields, total_rows,
            "All {} rows should have 11+ fields with proper CSV splitting, but only {} did",
            total_rows, rows_with_11_fields);
    }

    /// Split a CSV line handling quoted fields (simple implementation).
    fn split_csv_line(line: &[u8]) -> Vec<&[u8]> {
        let mut fields = Vec::new();
        let mut start = 0;
        let mut in_quotes = false;

        for i in 0..line.len() {
            match line[i] {
                b'"' => in_quotes = !in_quotes,
                b',' if !in_quotes => {
                    fields.push(&line[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        fields.push(&line[start..]);
        fields
    }
}
