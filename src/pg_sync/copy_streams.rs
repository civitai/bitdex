//! Per-table COPY stream processing for bulk loading.
//!
//! Each function consumes a Postgres COPY CSV byte stream, parses rows,
//! writes fields to the SlotArena, and builds BitmapAccum for the engine.
//!
//! Compared to the range-batched `table_streams`, COPY streams avoid
//! per-batch query overhead and transfer data in a single pass.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use futures_util::TryStreamExt;
use roaring::RoaringBitmap;
use sqlx::PgPool;

use crate::config::DataSchema;
use crate::loader::{extract_bitmaps, BitmapAccum};

use super::copy_queries::{
    self, parse_image_row, parse_resource_row, parse_tag_row, parse_technique_row, parse_tool_row,
    CopyParser, CopyResourceRow,
};
use super::progress::LoadProgress;
use super::slot_arena::{self, SlotArena};
use super::table_streams::StreamStats;

// Progress update interval (rows)
const PROGRESS_INTERVAL: u64 = 100_000;
// Log interval (rows)
const LOG_INTERVAL: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Image+Post COPY stream
// ---------------------------------------------------------------------------

/// Stream the Image+Post table via COPY: writes scalars to arena, builds
/// filter/sort bitmaps via `extract_bitmaps`.
pub(crate) async fn stream_images_copy(
    pool: &PgPool,
    arena: &SlotArena,
    schema: &DataSchema,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    filter_set: &HashSet<String>,
    sort_bits: &HashMap<String, u8>,
    progress: &Arc<LoadProgress>,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut last_log = 0u64;

    let mut stream = copy_queries::copy_images(pool)
        .await
        .map_err(|e| format!("copy_images: {e}"))?;
    let mut parser = CopyParser::new();

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("copy_images stream: {e}"))?
    {
        let lines = parser.feed(&chunk);
        for line in lines {
            let row = match parse_image_row(&line) {
                Some(r) => r,
                None => continue,
            };
            let slot = row.id as u32;

            // Compute derived fields
            let sort_at = row.sort_at_secs();
            let has_meta = row.has_meta();
            let on_site = row.on_site();
            let poi = row.poi();
            let minor = row.minor();
            let published_at_ms = (row.published_at_secs.unwrap_or(0) * 1000) as u64;

            // Write scalar fields to arena
            arena.write_scalars(
                slot,
                row.id as u64,
                row.nsfw_level as u8,
                row.user_id as u64,
                slot_arena::encode_image_type(Some(&row.image_type)),
                sort_at,
                poi,
                minor,
                row.url.as_deref().map(|s| s.as_bytes()),
                row.hash.as_deref().map(|s| s.as_bytes()),
                has_meta,
                on_site,
                row.post_id.unwrap_or(0) as u64,
                row.posted_to_id.unwrap_or(0) as u64,
                slot_arena::encode_availability(Some(row.availability.as_str())),
                slot_arena::encode_blocked_for(row.blocked_for.as_deref()),
                published_at_ms,
            );

            // Build a minimal JSON doc for bitmap extraction.
            // extract_bitmaps handles all schema-aware filter/sort field logic
            // including string_map lookups, exists-booleans, bit decomposition, etc.
            let mut doc = serde_json::json!({
                "id": row.id,
                "nsfwLevel": row.nsfw_level,
                "userId": row.user_id,
                "postId": row.post_id.unwrap_or(0),
                "postedToId": row.posted_to_id.unwrap_or(0),
                "type": &row.image_type,
                "availability": &row.availability,
                "blockedFor": row.blocked_for.as_deref(),
                "reactionCount": 0,
                "commentCount": 0,
                "collectedCount": 0,
                "sortAt": sort_at,
                "publishedAtUnix": published_at_ms as i64,
            });

            // Exists-boolean fields: only set when true (matching extract_bitmaps behavior)
            if let Some(obj) = doc.as_object_mut() {
                if has_meta {
                    obj.insert("hasMeta".into(), serde_json::json!(true));
                }
                if on_site {
                    obj.insert("onSite".into(), serde_json::json!(true));
                }
                if poi {
                    obj.insert("poi".into(), serde_json::json!(true));
                }
                if minor {
                    obj.insert("minor".into(), serde_json::json!(true));
                }
            }

            accum.alive.insert(slot);
            extract_bitmaps(
                &doc,
                schema,
                filter_set,
                sort_bits,
                slot,
                &mut accum.filter_maps,
                &mut accum.sort_maps,
            );

            total += 1;

            // Progress updates
            if total % PROGRESS_INTERVAL == 0 {
                progress.image_rows.store(total, Ordering::Release);
            }
            if total % LOG_INTERVAL == 0 && total > last_log {
                last_log = total;
                let elapsed = start.elapsed().as_secs_f64();
                let rate = total as f64 / elapsed;
                eprintln!(
                    "  stream_images_copy: {} rows ({:.0}/s, {:.1}s)",
                    total, rate, elapsed
                );
            }
        }
    }

    // Final progress update
    progress.image_rows.store(total, Ordering::Release);
    progress
        .streams_done
        .fetch_add(1, Ordering::Release);

    let elapsed = start.elapsed();
    eprintln!(
        "stream_images_copy: complete — {} images in {:.1}s ({:.0}/s)",
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Tag COPY stream
// ---------------------------------------------------------------------------

/// Stream TagsOnImageDetails via COPY, ordered by tagId.
///
/// Groups rows by tagId and flushes batches of image IDs to the arena
/// and filter bitmaps — same pattern as `table_streams::stream_tags`.
pub(crate) async fn stream_tags_copy(
    pool: &PgPool,
    arena: &SlotArena,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    progress: &Arc<LoadProgress>,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut last_log = 0u64;

    let mut stream = copy_queries::copy_tags(pool)
        .await
        .map_err(|e| format!("copy_tags: {e}"))?;
    let mut parser = CopyParser::new();

    // Group-by-tagId state
    let mut current_tag: Option<i64> = None;
    let mut current_images: Vec<u32> = Vec::with_capacity(4096);

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("copy_tags stream: {e}"))?
    {
        let lines = parser.feed(&chunk);
        for line in lines {
            let (tag_id, image_id) = match parse_tag_row(&line) {
                Some(r) => r,
                None => continue,
            };

            if current_tag != Some(tag_id) {
                // Flush previous tag batch
                if let Some(prev_tag_id) = current_tag {
                    flush_tag_batch(prev_tag_id as u64, &current_images, arena, &mut accum.filter_maps);
                }
                current_tag = Some(tag_id);
                current_images.clear();
            }

            current_images.push(image_id as u32);
            total += 1;

            if total % PROGRESS_INTERVAL == 0 {
                progress.tag_rows.store(total, Ordering::Release);
            }
            if total % LOG_INTERVAL == 0 && total > last_log {
                last_log = total;
                let elapsed = start.elapsed().as_secs_f64();
                let rate = total as f64 / elapsed;
                eprintln!(
                    "  stream_tags_copy: {} rows ({:.0}/s, {:.1}s)",
                    total, rate, elapsed
                );
            }
        }
    }

    // Flush final tag batch
    if let Some(tag_id) = current_tag {
        flush_tag_batch(tag_id as u64, &current_images, arena, &mut accum.filter_maps);
    }

    progress.tag_rows.store(total, Ordering::Release);
    progress
        .streams_done
        .fetch_add(1, Ordering::Release);

    let elapsed = start.elapsed();
    eprintln!(
        "stream_tags_copy: complete — {} tag rows in {:.1}s ({:.0}/s)",
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Tool COPY stream
// ---------------------------------------------------------------------------

/// Stream ImageTool via COPY, ordered by toolId.
pub(crate) async fn stream_tools_copy(
    pool: &PgPool,
    arena: &SlotArena,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    progress: &Arc<LoadProgress>,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut last_log = 0u64;

    let mut stream = copy_queries::copy_tools(pool)
        .await
        .map_err(|e| format!("copy_tools: {e}"))?;
    let mut parser = CopyParser::new();

    let mut current_tool: Option<i64> = None;
    let mut current_images: Vec<u32> = Vec::with_capacity(256);

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("copy_tools stream: {e}"))?
    {
        let lines = parser.feed(&chunk);
        for line in lines {
            let (tool_id, image_id) = match parse_tool_row(&line) {
                Some(r) => r,
                None => continue,
            };

            if current_tool != Some(tool_id) {
                if let Some(prev_tool_id) = current_tool {
                    flush_id_batch(
                        "toolIds",
                        prev_tool_id as u64,
                        &current_images,
                        |img_id| arena.write_tools(img_id, &[prev_tool_id as u32]),
                        &mut accum.filter_maps,
                    );
                }
                current_tool = Some(tool_id);
                current_images.clear();
            }

            current_images.push(image_id as u32);
            total += 1;

            if total % PROGRESS_INTERVAL == 0 {
                progress.tool_rows.store(total, Ordering::Release);
            }
            if total % LOG_INTERVAL == 0 && total > last_log {
                last_log = total;
                let elapsed = start.elapsed().as_secs_f64();
                eprintln!(
                    "  stream_tools_copy: {} rows ({:.0}/s, {:.1}s)",
                    total,
                    total as f64 / elapsed,
                    elapsed
                );
            }
        }
    }

    // Flush final batch
    if let Some(tool_id) = current_tool {
        flush_id_batch(
            "toolIds",
            tool_id as u64,
            &current_images,
            |img_id| arena.write_tools(img_id, &[tool_id as u32]),
            &mut accum.filter_maps,
        );
    }

    progress.tool_rows.store(total, Ordering::Release);
    progress
        .streams_done
        .fetch_add(1, Ordering::Release);

    let elapsed = start.elapsed();
    eprintln!(
        "stream_tools_copy: complete — {} rows in {:.1}s ({:.0}/s)",
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Technique COPY stream
// ---------------------------------------------------------------------------

/// Stream ImageTechnique via COPY, ordered by techniqueId.
pub(crate) async fn stream_techniques_copy(
    pool: &PgPool,
    arena: &SlotArena,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    progress: &Arc<LoadProgress>,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut last_log = 0u64;

    let mut stream = copy_queries::copy_techniques(pool)
        .await
        .map_err(|e| format!("copy_techniques: {e}"))?;
    let mut parser = CopyParser::new();

    let mut current_tech: Option<i64> = None;
    let mut current_images: Vec<u32> = Vec::with_capacity(256);

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("copy_techniques stream: {e}"))?
    {
        let lines = parser.feed(&chunk);
        for line in lines {
            let (technique_id, image_id) = match parse_technique_row(&line) {
                Some(r) => r,
                None => continue,
            };

            if current_tech != Some(technique_id) {
                if let Some(tech_id) = current_tech {
                    flush_id_batch(
                        "techniqueIds",
                        tech_id as u64,
                        &current_images,
                        |img_id| arena.write_techniques(img_id, &[tech_id as u32]),
                        &mut accum.filter_maps,
                    );
                }
                current_tech = Some(technique_id);
                current_images.clear();
            }

            current_images.push(image_id as u32);
            total += 1;

            if total % PROGRESS_INTERVAL == 0 {
                progress.technique_rows.store(total, Ordering::Release);
            }
            if total % LOG_INTERVAL == 0 && total > last_log {
                last_log = total;
                let elapsed = start.elapsed().as_secs_f64();
                eprintln!(
                    "  stream_techniques_copy: {} rows ({:.0}/s, {:.1}s)",
                    total,
                    total as f64 / elapsed,
                    elapsed
                );
            }
        }
    }

    // Flush final batch
    if let Some(tech_id) = current_tech {
        flush_id_batch(
            "techniqueIds",
            tech_id as u64,
            &current_images,
            |img_id| arena.write_techniques(img_id, &[tech_id as u32]),
            &mut accum.filter_maps,
        );
    }

    progress.technique_rows.store(total, Ordering::Release);
    progress
        .streams_done
        .fetch_add(1, Ordering::Release);

    let elapsed = start.elapsed();
    eprintln!(
        "stream_techniques_copy: complete — {} rows in {:.1}s ({:.0}/s)",
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Resource COPY stream
// ---------------------------------------------------------------------------

/// Stream ImageResourceNew + ModelVersion + Model via COPY, ordered by imageId.
///
/// Accumulates all resource rows per image, then flushes:
/// - Detected MVs to `arena.write_model_version_ids`
/// - Manual MVs to `arena.write_model_version_ids_manual`
/// - Base model (first Checkpoint-type model) to `arena.write_base_model`
/// - Resource POI (any model.poi=true) to `arena.set_resource_poi`
/// - Filter bitmaps for modelVersionIds and baseModel
pub(crate) async fn stream_resources_copy(
    pool: &PgPool,
    arena: &SlotArena,
    schema: &DataSchema,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    progress: &Arc<LoadProgress>,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut last_log = 0u64;

    let mut stream = copy_queries::copy_resources(pool)
        .await
        .map_err(|e| format!("copy_resources: {e}"))?;
    let mut parser = CopyParser::new();

    // Per-image accumulation state
    let mut current_image_id: Option<i64> = None;
    let mut pending_resources: Vec<CopyResourceRow> = Vec::with_capacity(16);

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| format!("copy_resources stream: {e}"))?
    {
        let lines = parser.feed(&chunk);
        for line in lines {
            let row = match parse_resource_row(&line) {
                Some(r) => r,
                None => continue,
            };

            if current_image_id != Some(row.image_id) {
                // Flush accumulated resources for previous image
                if let Some(img_id) = current_image_id {
                    flush_resources(
                        img_id,
                        &pending_resources,
                        arena,
                        schema,
                        &mut accum.filter_maps,
                    );
                }
                current_image_id = Some(row.image_id);
                pending_resources.clear();
            }

            pending_resources.push(row);
            total += 1;

            if total % PROGRESS_INTERVAL == 0 {
                progress.resource_rows.store(total, Ordering::Release);
            }
            if total % LOG_INTERVAL == 0 && total > last_log {
                last_log = total;
                let elapsed = start.elapsed().as_secs_f64();
                eprintln!(
                    "  stream_resources_copy: {} rows ({:.0}/s, {:.1}s)",
                    total,
                    total as f64 / elapsed,
                    elapsed
                );
            }
        }
    }

    // Flush final image's resources
    if let Some(img_id) = current_image_id {
        flush_resources(
            img_id,
            &pending_resources,
            arena,
            schema,
            &mut accum.filter_maps,
        );
    }

    progress.resource_rows.store(total, Ordering::Release);
    progress
        .streams_done
        .fetch_add(1, Ordering::Release);

    let elapsed = start.elapsed();
    eprintln!(
        "stream_resources_copy: complete — {} resources in {:.1}s ({:.0}/s)",
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

/// Flush accumulated resource rows for a single image.
///
/// Partitions MVs into detected vs manual, picks the first Checkpoint base model,
/// checks for resource POI, and builds filter bitmaps.
fn flush_resources(
    image_id: i64,
    resources: &[CopyResourceRow],
    arena: &SlotArena,
    schema: &DataSchema,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    let slot = image_id as u32;

    let mut detected_mvs: Vec<u32> = Vec::new();
    let mut manual_mvs: Vec<u32> = Vec::new();
    let mut base_model_str: Option<&str> = None;
    let mut has_resource_poi = false;

    for res in resources {
        let mv_id = res.model_version_id as u32;

        if res.detected {
            detected_mvs.push(mv_id);
        } else {
            manual_mvs.push(mv_id);
        }

        // Base model: first Checkpoint-type model wins
        if base_model_str.is_none() && res.model_type == "Checkpoint" {
            base_model_str = res.base_model.as_deref();
        }

        // Resource POI: any model with poi=true
        if res.model_poi {
            has_resource_poi = true;
        }
    }

    // Write to arena
    if !detected_mvs.is_empty() {
        arena.write_model_version_ids(slot, &detected_mvs);
    }
    if !manual_mvs.is_empty() {
        arena.write_model_version_ids_manual(slot, &manual_mvs);
    }
    if let Some(bm) = base_model_str {
        arena.write_base_model(slot, slot_arena::encode_base_model(Some(bm)));
    }
    if has_resource_poi {
        arena.set_resource_poi(slot);
    }

    // Build filter bitmaps — modelVersionIds (both detected + manual in same bitmap)
    let mv_bitmap = filter_maps
        .entry("modelVersionIds".to_string())
        .or_default();
    for &mv_id in detected_mvs.iter().chain(manual_mvs.iter()) {
        mv_bitmap
            .entry(mv_id as u64)
            .or_insert_with(RoaringBitmap::new)
            .insert(slot);
    }

    // baseModel filter bitmap via schema string_map
    if let Some(bm_str) = base_model_str {
        if !bm_str.is_empty() {
            let key = schema
                .fields
                .iter()
                .find(|f| f.target == "baseModel")
                .and_then(|f| f.string_map.as_ref())
                .and_then(|map| {
                    let lower = bm_str.to_lowercase();
                    map.get(&lower).or_else(|| map.get(bm_str)).copied()
                })
                .unwrap_or(0) as u64;

            filter_maps
                .entry("baseModel".to_string())
                .or_default()
                .entry(key)
                .or_insert_with(RoaringBitmap::new)
                .insert(slot);
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Flush a batch of images for a single tagId: write to arena + build bitmap.
fn flush_tag_batch(
    tag_id: u64,
    image_ids: &[u32],
    arena: &SlotArena,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    let tag_u32 = tag_id as u32;
    for &image_id in image_ids {
        arena.write_tags(image_id, &[tag_u32]);
    }

    let bm = filter_maps
        .entry("tagIds".to_string())
        .or_default()
        .entry(tag_id)
        .or_insert_with(RoaringBitmap::new);
    for &image_id in image_ids {
        bm.insert(image_id);
    }
}

/// Generic flush for ID-based streams (tools, techniques).
fn flush_id_batch(
    field_name: &str,
    id_value: u64,
    image_ids: &[u32],
    mut arena_write: impl FnMut(u32),
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    for &image_id in image_ids {
        arena_write(image_id);
    }

    let bm = filter_maps
        .entry(field_name.to_string())
        .or_default()
        .entry(id_value)
        .or_insert_with(RoaringBitmap::new);
    for &image_id in image_ids {
        bm.insert(image_id);
    }
}
