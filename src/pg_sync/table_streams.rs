//! Per-table streaming functions for bulk loading.
//!
//! Each function streams one Postgres table in range batches, writes fields to
//! the SlotArena, and builds bitmap accumulators for the engine.
//!
//! Table streams:
//!   - `stream_images`: Image + Post → scalars + derived booleans + filter/sort bitmaps
//!   - `stream_tags`: TagsOnImageDetails → tagIds (ordered by tagId for bitmap efficiency)
//!   - `stream_tools`: ImageTool → toolIds
//!   - `stream_techniques`: ImageTechnique → techniqueIds
//!   - `stream_resources`: ImageResourceNew + MV + Model → MVs, baseModel, poi

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use roaring::RoaringBitmap;
use sqlx::PgPool;

use crate::config::DataSchema;
use crate::loader::{extract_bitmaps, BitmapAccum};

use super::queries;
use super::row_assembler;
use super::slot_arena::{self, SlotArena};

/// Statistics for a single table stream.
#[derive(Debug)]
pub struct StreamStats {
    pub rows_processed: u64,
    pub elapsed: std::time::Duration,
}

// ---------------------------------------------------------------------------
// Image+Post stream
// ---------------------------------------------------------------------------

/// Stream the Image+Post table: writes scalars to slots, builds filter/sort bitmaps.
///
/// This is the primary stream — it populates all Image scalar fields, derived
/// booleans (hasMeta, onSite), and produces the alive bitmap.
pub(crate) async fn stream_images(
    pool: &PgPool,
    arena: &SlotArena,
    schema: &DataSchema,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    filter_set: &HashSet<String>,
    sort_bits: &HashMap<String, u8>,
    max_id: i64,
    batch_size: i64,
    progress: &Arc<AtomicU64>,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut range_start: i64 = 0;

    while range_start <= max_id {
        let range_end = range_start + batch_size;

        let images = queries::fetch_images_by_range(pool, range_start, range_end)
            .await
            .map_err(|e| format!("stream_images: fetch range [{range_start}, {range_end}): {e}"))?;

        for image in &images {
            let slot = image.id as u32;

            // Compute derived fields
            let sort_at_secs = image.sort_at.map(|dt| dt.timestamp() as u64).unwrap_or(0);
            let published_at_ms = image.published_at.map(|dt| dt.timestamp_millis() as u64).unwrap_or(0);

            let has_meta = match &image.meta {
                Some(v) if !v.is_null() => !image.hide_meta.unwrap_or(false),
                _ => false,
            };
            let on_site = match &image.meta {
                Some(serde_json::Value::Object(map)) => map.contains_key("civitaiResources"),
                _ => false,
            };

            // Write scalar fields to arena
            arena.write_scalars(
                slot,
                image.id as u64,
                image.nsfw_level.unwrap_or(0) as u8,
                image.user_id.unwrap_or(0) as u64,
                slot_arena::encode_image_type(image.image_type.as_deref()),
                sort_at_secs,
                image.poi.unwrap_or(false),
                image.minor.unwrap_or(false),
                image.url.as_deref().map(|s| s.as_bytes()),
                image.hash.as_deref().map(|s| s.as_bytes()),
                has_meta,
                on_site,
                image.post_id as u64,
                image.posted_to_id.unwrap_or(0) as u64,
                slot_arena::encode_availability(image.availability.as_deref()),
                slot_arena::encode_blocked_for(image.blocked_for.as_deref()),
                published_at_ms,
            );

            // Build bitmaps via extract_bitmaps (reuses the full JSON path)
            // Assemble a minimal JSON doc with only the indexed fields
            let doc = row_assembler::assemble_document(
                image,
                &row_assembler::EnrichmentData {
                    tags: HashMap::new(),
                    tools: HashMap::new(),
                    techniques: HashMap::new(),
                    resources: HashMap::new(),
                    metrics: HashMap::new(),
                },
            );

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
        }

        progress.store(total, Ordering::Release);

        if !images.is_empty() {
            let elapsed = start.elapsed();
            let rate = total as f64 / elapsed.as_secs_f64();
            eprintln!(
                "  stream_images: range [{range_start}, {range_end}) → {} rows, {} total ({:.0}/s)",
                images.len(), total, rate
            );
        }

        range_start = range_end;
    }

    let elapsed = start.elapsed();
    eprintln!(
        "stream_images: complete — {} images in {:.1}s ({:.0}/s)",
        total, elapsed.as_secs_f64(), total as f64 / elapsed.as_secs_f64()
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Tag stream
// ---------------------------------------------------------------------------

/// Stream TagsOnImageDetails ordered by tagId for bitmap-efficient insertion.
///
/// This produces ~360x faster bitmap insertion than per-image ordering because
/// all images for one tagId are inserted into a single bitmap consecutively.
pub(crate) async fn stream_tags(
    pool: &PgPool,
    arena: &SlotArena,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    batch_size: i64,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut range_start: i64 = 0;

    // Get max tag ID for range iteration
    let max_tag_id = queries::get_max_tag_id(pool)
        .await
        .map_err(|e| format!("stream_tags: get_max_tag_id: {e}"))?;

    if max_tag_id == 0 {
        return Ok((accum, StreamStats { rows_processed: 0, elapsed: start.elapsed() }));
    }

    while range_start <= max_tag_id {
        let range_end = range_start + batch_size;

        let rows = queries::fetch_tags_by_tag_range(pool, range_start, range_end)
            .await
            .map_err(|e| format!("stream_tags: fetch range [{range_start}, {range_end}): {e}"))?;

        // Process rows: they arrive ordered by tagId, imageId.
        // Group by tagId for bitmap batch insertion.
        let mut current_tag: Option<i64> = None;
        let mut current_images: Vec<u32> = Vec::with_capacity(1000);

        for row in &rows {
            if current_tag != Some(row.tag_id) {
                // Flush previous tag
                if let Some(tag_id) = current_tag {
                    flush_tag_batch(
                        tag_id as u64,
                        &current_images,
                        arena,
                        &mut accum.filter_maps,
                    );
                }
                current_tag = Some(row.tag_id);
                current_images.clear();
            }
            current_images.push(row.image_id as u32);
            total += 1;
        }

        // Flush last tag in batch
        if let Some(tag_id) = current_tag {
            flush_tag_batch(
                tag_id as u64,
                &current_images,
                arena,
                &mut accum.filter_maps,
            );
        }

        if !rows.is_empty() {
            let elapsed = start.elapsed();
            let rate = total as f64 / elapsed.as_secs_f64();
            eprintln!(
                "  stream_tags: tagId range [{range_start}, {range_end}) → {} rows, {} total ({:.0}/s)",
                rows.len(), total, rate
            );
        }

        range_start = range_end;
    }

    let elapsed = start.elapsed();
    eprintln!(
        "stream_tags: complete — {} tag rows in {:.1}s ({:.0}/s)",
        total, elapsed.as_secs_f64(), total as f64 / elapsed.as_secs_f64()
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

/// Flush a batch of images for a single tagId: write to arena + build bitmap.
fn flush_tag_batch(
    tag_id: u64,
    image_ids: &[u32],
    arena: &SlotArena,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
) {
    // Write tag to each slot's arena
    let tag_u32 = tag_id as u32;
    for &image_id in image_ids {
        arena.write_tags(image_id, &[tag_u32]);
    }

    // Build filter bitmap: insert all images into this tagId's bitmap at once
    let bm = filter_maps
        .entry("tagIds".to_string())
        .or_default()
        .entry(tag_id)
        .or_insert_with(RoaringBitmap::new);
    for &image_id in image_ids {
        bm.insert(image_id);
    }
}

// ---------------------------------------------------------------------------
// Tool stream
// ---------------------------------------------------------------------------

/// Stream ImageTool ordered by toolId.
pub(crate) async fn stream_tools(
    pool: &PgPool,
    arena: &SlotArena,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    batch_size: i64,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;

    let max_tool_id = queries::get_max_tool_id(pool)
        .await
        .map_err(|e| format!("stream_tools: get_max_tool_id: {e}"))?;

    if max_tool_id == 0 {
        return Ok((accum, StreamStats { rows_processed: 0, elapsed: start.elapsed() }));
    }

    let mut range_start: i64 = 0;
    while range_start <= max_tool_id {
        let range_end = range_start + batch_size;

        let rows = queries::fetch_tools_by_tool_range(pool, range_start, range_end)
            .await
            .map_err(|e| format!("stream_tools: fetch range [{range_start}, {range_end}): {e}"))?;

        // Group by toolId
        let mut current_tool: Option<i64> = None;
        let mut current_images: Vec<u32> = Vec::with_capacity(100);

        for row in &rows {
            if current_tool != Some(row.tool_id as i64) {
                if let Some(tool_id) = current_tool {
                    flush_id_batch(
                        "toolIds",
                        tool_id as u64,
                        &current_images,
                        |img_id| arena.write_tools(img_id, &[tool_id as u32]),
                        &mut accum.filter_maps,
                    );
                }
                current_tool = Some(row.tool_id as i64);
                current_images.clear();
            }
            current_images.push(row.image_id as u32);
            total += 1;
        }

        if let Some(tool_id) = current_tool {
            flush_id_batch(
                "toolIds",
                tool_id as u64,
                &current_images,
                |img_id| arena.write_tools(img_id, &[tool_id as u32]),
                &mut accum.filter_maps,
            );
        }

        range_start = range_end;
    }

    let elapsed = start.elapsed();
    eprintln!(
        "stream_tools: complete — {} rows in {:.1}s ({:.0}/s)",
        total, elapsed.as_secs_f64(), total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Technique stream
// ---------------------------------------------------------------------------

/// Stream ImageTechnique ordered by techniqueId.
pub(crate) async fn stream_techniques(
    pool: &PgPool,
    arena: &SlotArena,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    batch_size: i64,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;

    let max_tech_id = queries::get_max_technique_id(pool)
        .await
        .map_err(|e| format!("stream_techniques: get_max_technique_id: {e}"))?;

    if max_tech_id == 0 {
        return Ok((accum, StreamStats { rows_processed: 0, elapsed: start.elapsed() }));
    }

    let mut range_start: i64 = 0;
    while range_start <= max_tech_id {
        let range_end = range_start + batch_size;

        let rows = queries::fetch_techniques_by_technique_range(pool, range_start, range_end)
            .await
            .map_err(|e| format!("stream_techniques: fetch [{range_start}, {range_end}): {e}"))?;

        let mut current_tech: Option<i64> = None;
        let mut current_images: Vec<u32> = Vec::with_capacity(100);

        for row in &rows {
            if current_tech != Some(row.technique_id as i64) {
                if let Some(tech_id) = current_tech {
                    flush_id_batch(
                        "techniqueIds",
                        tech_id as u64,
                        &current_images,
                        |img_id| arena.write_techniques(img_id, &[tech_id as u32]),
                        &mut accum.filter_maps,
                    );
                }
                current_tech = Some(row.technique_id as i64);
                current_images.clear();
            }
            current_images.push(row.image_id as u32);
            total += 1;
        }

        if let Some(tech_id) = current_tech {
            flush_id_batch(
                "techniqueIds",
                tech_id as u64,
                &current_images,
                |img_id| arena.write_techniques(img_id, &[tech_id as u32]),
                &mut accum.filter_maps,
            );
        }

        range_start = range_end;
    }

    let elapsed = start.elapsed();
    eprintln!(
        "stream_techniques: complete — {} rows in {:.1}s ({:.0}/s)",
        total, elapsed.as_secs_f64(), total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Resource stream
// ---------------------------------------------------------------------------

/// Stream ImageResourceNew + ModelVersion + Model.
///
/// Produces: modelVersionIds, modelVersionIdsManual, baseModel, resourcePoi.
/// Resources are fetched by imageId range (same ordering as images) since
/// the resource table is much smaller than tags.
pub(crate) async fn stream_resources(
    pool: &PgPool,
    arena: &SlotArena,
    schema: &DataSchema,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    max_image_id: i64,
    batch_size: i64,
) -> Result<(BitmapAccum, StreamStats), String> {
    let start = Instant::now();
    let mut accum = BitmapAccum::new(filter_names, sort_configs);
    let mut total = 0u64;
    let mut range_start: i64 = 0;

    while range_start <= max_image_id {
        let range_end = range_start + batch_size;

        let rows = queries::fetch_resources_by_range(pool, range_start, range_end)
            .await
            .map_err(|e| format!("stream_resources: fetch [{range_start}, {range_end}): {e}"))?;

        for row in &rows {
            let slot = row.image_id as u32;

            // Write MV IDs to arena
            let mv_ids: Vec<u32> = row.model_version_ids.iter().map(|&id| id as u32).collect();
            arena.write_model_version_ids(slot, &mv_ids);

            let mv_manual: Vec<u32> = row.model_version_ids_manual.iter().map(|&id| id as u32).collect();
            arena.write_model_version_ids_manual(slot, &mv_manual);

            // Base model
            let base_model_enum = slot_arena::encode_base_model(row.base_model.as_deref());
            arena.write_base_model(slot, base_model_enum);

            // Resource POI — OR into slot
            if row.resource_poi.unwrap_or(false) {
                arena.set_resource_poi(slot);
            }

            // Build filter bitmaps
            // modelVersionIds
            for &mv_id in &row.model_version_ids {
                accum.filter_maps
                    .entry("modelVersionIds".to_string())
                    .or_default()
                    .entry(mv_id as u64)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }

            // baseModel (string → mapped integer via schema's string_map)
            if let Some(ref bm_str) = row.base_model {
                if !bm_str.is_empty() {
                    // Find the baseModel field mapping in schema to use its string_map
                    let key = schema.fields.iter()
                        .find(|f| f.target == "baseModel")
                        .and_then(|f| f.string_map.as_ref())
                        .and_then(|map| {
                            // Try case-insensitive lookup (matching extract_filter_value behavior)
                            let lower = bm_str.to_lowercase();
                            map.get(&lower).or_else(|| map.get(bm_str.as_str())).copied()
                        })
                        .unwrap_or(0) as u64;

                    accum.filter_maps
                        .entry("baseModel".to_string())
                        .or_default()
                        .entry(key)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(slot);
                }
            }

            total += 1;
        }

        if !rows.is_empty() {
            let elapsed = start.elapsed();
            let rate = total as f64 / elapsed.as_secs_f64();
            eprintln!(
                "  stream_resources: range [{range_start}, {range_end}) → {} rows, {} total ({:.0}/s)",
                rows.len(), total, rate
            );
        }

        range_start = range_end;
    }

    let elapsed = start.elapsed();
    eprintln!(
        "stream_resources: complete — {} resources in {:.1}s ({:.0}/s)",
        total, elapsed.as_secs_f64(), total as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok((accum, StreamStats { rows_processed: total, elapsed }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generic flush for ID-based streams (tools, techniques).
/// Writes to arena + builds filter bitmap.
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
