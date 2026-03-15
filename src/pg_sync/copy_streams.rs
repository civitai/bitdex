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
use rayon::prelude::*;
use roaring::RoaringBitmap;
use sqlx::PgPool;

use crate::config::DataSchema;
use crate::dictionary::FieldDictionary;
use crate::loader::BitmapAccum;

use super::copy_queries::{
    self, parse_image_row, parse_resource_row, parse_tag_row, parse_technique_row, parse_tool_row,
    CopyParser, CopyImageRow, CopyResourceRow,
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
/// filter/sort bitmaps directly from parsed CSV fields (no JSON allocation).
///
/// Uses Rayon fold+reduce for parallel bitmap construction across chunks.
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
    let mut total = 0u64;
    let mut last_log = 0u64;
    let mut accum = BitmapAccum::new(filter_names, sort_configs);

    // Pre-resolve string_maps for MappedString filter fields (type, availability, blockedFor).
    // These are looked up once instead of per-row.
    let type_map = resolve_string_map(schema, "type");
    let availability_map = resolve_string_map(schema, "availability");
    let blocked_for_map = resolve_string_map(schema, "blockedFor");

    // Pre-resolve which sort fields exist and their bit counts
    let sort_at_bits = sort_bits.get("sortAt").copied();
    let published_at_bits = sort_bits.get("publishedAt").copied();
    let reaction_count_bits = sort_bits.get("reactionCount").copied();
    let comment_count_bits = sort_bits.get("commentCount").copied();
    let collected_count_bits = sort_bits.get("collectedCount").copied();
    let id_sort_bits = sort_bits.get("id").copied();

    // Pre-check which filter fields are active
    let has_nsfw = filter_set.contains("nsfwLevel");
    let has_user = filter_set.contains("userId");
    let has_type = filter_set.contains("type");
    let has_avail = filter_set.contains("availability");
    let has_blocked = filter_set.contains("blockedFor");
    let has_post = filter_set.contains("postId");
    let has_posted_to = filter_set.contains("postedToId");
    let has_remix_of = filter_set.contains("remixOfId");
    let has_has_meta = filter_set.contains("hasMeta");
    let has_on_site = filter_set.contains("onSite");
    let has_poi = filter_set.contains("poi");
    let has_minor = filter_set.contains("minor");
    let has_is_published = filter_set.contains("isPublished");
    let has_is_remix = filter_set.contains("isRemix");

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
        let chunk_len = lines.len() as u64;
        if chunk_len == 0 {
            continue;
        }

        // Parse all lines in this chunk (fast, mostly integer parsing)
        let rows: Vec<CopyImageRow> = lines
            .into_iter()
            .filter_map(|line| parse_image_row(&line))
            .collect();

        // Write scalars to arena sequentially (mmap writes, fast)
        for row in &rows {
            let slot = row.id as u32;
            let sort_at = row.sort_at_secs();
            let published_at_ms = (row.published_at_secs.unwrap_or(0) * 1000) as u64;
            arena.write_scalars(
                slot,
                row.id as u64,
                row.nsfw_level as u8,
                row.user_id as u64,
                slot_arena::encode_image_type(Some(&row.image_type)),
                sort_at,
                row.poi(),
                row.minor(),
                row.url.as_deref().map(|s| s.as_bytes()),
                row.hash.as_deref().map(|s| s.as_bytes()),
                row.has_meta(),
                row.on_site(),
                row.post_id.unwrap_or(0) as u64,
                row.posted_to_id.unwrap_or(0) as u64,
                slot_arena::encode_availability(Some(row.availability.as_str())),
                slot_arena::encode_blocked_for(row.blocked_for.as_deref()),
                published_at_ms,
            );
        }

        // Build bitmaps in parallel via Rayon fold+reduce
        let chunk_accum = rows
            .par_iter()
            .fold(
                || BitmapAccum::new(filter_names, sort_configs),
                |mut acc, row| {
                    let slot = row.id as u32;
                    let sort_at = row.sort_at_secs();
                    let has_meta = row.has_meta();
                    let on_site = row.on_site();
                    let poi = row.poi();
                    let minor = row.minor();
                    let published_at_secs = row.published_at_secs.unwrap_or(0);

                    acc.alive.insert(slot);

                    // --- Filter bitmaps (direct, no JSON) ---
                    if has_nsfw {
                        insert_filter(&mut acc.filter_maps, "nsfwLevel", row.nsfw_level as u64, slot);
                    }
                    if has_user {
                        insert_filter(&mut acc.filter_maps, "userId", row.user_id as u64, slot);
                    }
                    if has_type {
                        let key = lookup_string_map(&type_map, &row.image_type);
                        insert_filter(&mut acc.filter_maps, "type", key, slot);
                    }
                    if has_avail {
                        let key = lookup_string_map(&availability_map, &row.availability);
                        insert_filter(&mut acc.filter_maps, "availability", key, slot);
                    }
                    if has_blocked {
                        if let Some(ref bf) = row.blocked_for {
                            let key = lookup_string_map(&blocked_for_map, bf);
                            insert_filter(&mut acc.filter_maps, "blockedFor", key, slot);
                        }
                    }
                    if has_post {
                        insert_filter(&mut acc.filter_maps, "postId", row.post_id.unwrap_or(0) as u64, slot);
                    }
                    if has_posted_to {
                        insert_filter(&mut acc.filter_maps, "postedToId", row.posted_to_id.unwrap_or(0) as u64, slot);
                    }
                    if has_remix_of {
                        // remixOfId not in images COPY — skip (enriched from resources)
                    }
                    // Boolean filters
                    if has_has_meta {
                        insert_filter(&mut acc.filter_maps, "hasMeta", if has_meta { 1 } else { 0 }, slot);
                    }
                    if has_on_site {
                        insert_filter(&mut acc.filter_maps, "onSite", if on_site { 1 } else { 0 }, slot);
                    }
                    if has_poi {
                        insert_filter(&mut acc.filter_maps, "poi", if poi { 1 } else { 0 }, slot);
                    }
                    if has_minor {
                        insert_filter(&mut acc.filter_maps, "minor", if minor { 1 } else { 0 }, slot);
                    }
                    // Exists-boolean filters
                    if has_is_published {
                        insert_filter(&mut acc.filter_maps, "isPublished", if published_at_secs != 0 { 1 } else { 0 }, slot);
                    }
                    if has_is_remix {
                        // isRemix derived from remixOfId — not in images COPY
                        insert_filter(&mut acc.filter_maps, "isRemix", 0, slot);
                    }

                    // --- Sort bitmaps (bit decomposition, no JSON) ---
                    if let Some(bits) = sort_at_bits {
                        insert_sort_bits(&mut acc.sort_maps, "sortAt", sort_at as u32, bits, slot);
                    }
                    if let Some(bits) = published_at_bits {
                        insert_sort_bits(&mut acc.sort_maps, "publishedAt", published_at_secs.max(0) as u32, bits, slot);
                    }
                    if let Some(bits) = reaction_count_bits {
                        insert_sort_bits(&mut acc.sort_maps, "reactionCount", 0, bits, slot);
                    }
                    if let Some(bits) = comment_count_bits {
                        insert_sort_bits(&mut acc.sort_maps, "commentCount", 0, bits, slot);
                    }
                    if let Some(bits) = collected_count_bits {
                        insert_sort_bits(&mut acc.sort_maps, "collectedCount", 0, bits, slot);
                    }
                    if let Some(bits) = id_sort_bits {
                        insert_sort_bits(&mut acc.sort_maps, "id", slot, bits, slot);
                    }

                    acc
                },
            )
            .reduce(
                || BitmapAccum::new(filter_names, sort_configs),
                |a, b| a.merge(b),
            );

        accum = accum.merge(chunk_accum);
        total += rows.len() as u64;

        // Progress + logging
        if total / PROGRESS_INTERVAL != (total - rows.len() as u64) / PROGRESS_INTERVAL {
            progress.image_rows.store(total, Ordering::Release);
        }
        if total / LOG_INTERVAL != last_log / LOG_INTERVAL {
            last_log = total;
            let elapsed = start.elapsed().as_secs_f64();
            let rate = total as f64 / elapsed;
            eprintln!(
                "  stream_images_copy: {} rows ({:.0}/s, {:.1}s)",
                total, rate, elapsed
            );
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
// Public bitmap builder for use from bulk_loader (file-based Phase 2)
// ---------------------------------------------------------------------------

/// Build filter and sort bitmaps for a single image row.
/// Called from bulk_loader when building from local CSV files.
pub(crate) fn build_image_bitmaps(
    row: &CopyImageRow,
    slot: u32,
    sort_at: u64,
    _schema: &DataSchema,
    type_dict: &FieldDictionary,
    availability_dict: &FieldDictionary,
    blocked_for_dict: &FieldDictionary,
    filter_set: &HashSet<String>,
    sort_bits: &HashMap<String, u8>,
    filter_maps: &mut HashMap<String, HashMap<u64, RoaringBitmap>>,
    sort_maps: &mut HashMap<String, HashMap<usize, RoaringBitmap>>,
) {
    let published_at_secs = row.published_at_secs.unwrap_or(0);

    // Filter bitmaps
    if filter_set.contains("nsfwLevel") { insert_filter(filter_maps, "nsfwLevel", row.nsfw_level as u64, slot); }
    if filter_set.contains("userId") { insert_filter(filter_maps, "userId", row.user_id as u64, slot); }
    if filter_set.contains("type") {
        let key = type_dict.get_or_insert(&row.image_type) as u64;
        insert_filter(filter_maps, "type", key, slot);
    }
    if filter_set.contains("availability") {
        let key = availability_dict.get_or_insert(&row.availability) as u64;
        insert_filter(filter_maps, "availability", key, slot);
    }
    if filter_set.contains("blockedFor") {
        if let Some(ref bf) = row.blocked_for {
            let key = blocked_for_dict.get_or_insert(bf) as u64;
            insert_filter(filter_maps, "blockedFor", key, slot);
        }
    }
    if filter_set.contains("postId") { insert_filter(filter_maps, "postId", row.post_id.unwrap_or(0) as u64, slot); }
    if filter_set.contains("postedToId") { insert_filter(filter_maps, "postedToId", row.posted_to_id.unwrap_or(0) as u64, slot); }
    if filter_set.contains("hasMeta") { insert_filter(filter_maps, "hasMeta", if row.has_meta() { 1 } else { 0 }, slot); }
    if filter_set.contains("onSite") { insert_filter(filter_maps, "onSite", if row.on_site() { 1 } else { 0 }, slot); }
    if filter_set.contains("poi") { insert_filter(filter_maps, "poi", if row.poi() { 1 } else { 0 }, slot); }
    if filter_set.contains("minor") { insert_filter(filter_maps, "minor", if row.minor() { 1 } else { 0 }, slot); }
    if filter_set.contains("isPublished") { insert_filter(filter_maps, "isPublished", if published_at_secs != 0 { 1 } else { 0 }, slot); }
    if filter_set.contains("isRemix") { insert_filter(filter_maps, "isRemix", 0, slot); }

    // Sort bitmaps
    if let Some(&bits) = sort_bits.get("sortAt") { insert_sort_bits(sort_maps, "sortAt", sort_at as u32, bits, slot); }
    if let Some(&bits) = sort_bits.get("publishedAt") { insert_sort_bits(sort_maps, "publishedAt", published_at_secs.max(0) as u32, bits, slot); }
    if let Some(&bits) = sort_bits.get("reactionCount") { insert_sort_bits(sort_maps, "reactionCount", 0, bits, slot); }
    if let Some(&bits) = sort_bits.get("commentCount") { insert_sort_bits(sort_maps, "commentCount", 0, bits, slot); }
    if let Some(&bits) = sort_bits.get("collectedCount") { insert_sort_bits(sort_maps, "collectedCount", 0, bits, slot); }
    if let Some(&bits) = sort_bits.get("id") { insert_sort_bits(sort_maps, "id", slot, bits, slot); }
}

// ---------------------------------------------------------------------------
// Direct bitmap helpers (no JSON allocation)
// ---------------------------------------------------------------------------

/// Insert a value into a filter bitmap map.
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

/// Decompose a u32 value into bit-layer sort bitmaps.
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

/// Pre-resolve a string_map from schema for a given field target name.
/// Returns a Vec of (lowercase_key, mapped_value) pairs for fast lookup.
fn resolve_string_map(schema: &DataSchema, target: &str) -> Vec<(String, u64)> {
    schema
        .fields
        .iter()
        .find(|f| f.target == target)
        .and_then(|f| f.string_map.as_ref())
        .map(|map| {
            map.iter()
                .map(|(k, &v)| (k.to_lowercase(), v as u64))
                .collect()
        })
        .unwrap_or_default()
}

/// Look up a string value in a pre-resolved string_map. Returns 0 if not found.
#[inline]
fn lookup_string_map(map: &[(String, u64)], value: &str) -> u64 {
    let lower = value.to_lowercase();
    map.iter()
        .find(|(k, _)| k == &lower)
        .map(|(_, v)| *v)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tag COPY stream
// ---------------------------------------------------------------------------

/// Stream TagsOnImageDetails via COPY (unordered).
///
/// Inserts directly into bitmaps per-row — no sorting or grouping needed.
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

            let slot = image_id as u32;
            arena.write_tags(slot, &[tag_id as u32]);

            // Direct bitmap insert (no grouping needed)
            if let Some(fm) = accum.filter_maps.get_mut("tagIds") {
                fm.entry(tag_id as u64)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }

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

/// Stream ImageTool via COPY (unordered). Direct per-row bitmap inserts.
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

            let slot = image_id as u32;
            arena.write_tools(slot, &[tool_id as u32]);

            if let Some(fm) = accum.filter_maps.get_mut("toolIds") {
                fm.entry(tool_id as u64)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }

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

/// Stream ImageTechnique via COPY (unordered). Direct per-row bitmap inserts.
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

            let slot = image_id as u32;
            arena.write_techniques(slot, &[technique_id as u32]);

            if let Some(fm) = accum.filter_maps.get_mut("techniqueIds") {
                fm.entry(technique_id as u64)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(slot);
            }

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

/// Stream ImageResourceNew via COPY (no JOINs), enriched with MV/Model lookup maps.
///
/// Accumulates all resource rows per image, then flushes:
/// - Detected MVs to `arena.write_model_version_ids`
/// - Manual MVs to `arena.write_model_version_ids_manual`
/// - Base model (first Checkpoint-type model) to `arena.write_base_model`
/// - Resource POI (any model.poi=true) to `arena.set_resource_poi`
/// - Filter bitmaps for modelVersionIds and baseModel
///
/// `mv_lookup`: MV.id → (baseModel, modelId)
/// `model_lookup`: Model.id → (poi, model_type)
pub(crate) async fn stream_resources_copy(
    pool: &PgPool,
    arena: &SlotArena,
    schema: &DataSchema,
    filter_names: &[String],
    sort_configs: &[(String, u8)],
    progress: &Arc<LoadProgress>,
    mv_lookup: &HashMap<i64, (Option<String>, i64)>,
    model_lookup: &HashMap<i64, (bool, String)>,
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
                        mv_lookup,
                        model_lookup,
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
            mv_lookup,
            model_lookup,
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
    mv_lookup: &HashMap<i64, (Option<String>, i64)>,
    model_lookup: &HashMap<i64, (bool, String)>,
) {
    let slot = image_id as u32;

    let mut detected_mvs: Vec<u32> = Vec::new();
    let mut manual_mvs: Vec<u32> = Vec::new();
    let mut base_model_str: Option<String> = None;
    let mut has_resource_poi = false;

    for res in resources {
        let mv_id = res.model_version_id as u32;

        if res.detected {
            detected_mvs.push(mv_id);
        } else {
            manual_mvs.push(mv_id);
        }

        // Look up MV → Model enrichment data
        if let Some((mv_base_model, model_id)) = mv_lookup.get(&res.model_version_id) {
            if let Some((poi, model_type)) = model_lookup.get(model_id) {
                // Base model: first Checkpoint-type model wins
                if base_model_str.is_none() && model_type == "Checkpoint" {
                    base_model_str = mv_base_model.clone();
                }
                // Resource POI: any model with poi=true
                if *poi {
                    has_resource_poi = true;
                }
            }
        }
    }

    // Write to arena
    if !detected_mvs.is_empty() {
        arena.write_model_version_ids(slot, &detected_mvs);
    }
    if !manual_mvs.is_empty() {
        arena.write_model_version_ids_manual(slot, &manual_mvs);
    }
    if let Some(ref bm) = base_model_str {
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
    if let Some(ref bm_str) = base_model_str {
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

