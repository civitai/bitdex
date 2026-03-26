//! WAL ops processor — translates ops from WAL files into bitmap mutations.
//!
//! Two processing modes per the Sync V2 design:
//!
//! - **Steady-state**: Ops → BitmapSink (CoalescerSink) → coalescer channel → flush thread.
//!   Used by the WAL reader thread during normal operation.
//!
//! - **Dump mode**: Ops → BitmapSink (AccumSink) → direct bitmap accumulation.
//!   Used during initial load. Bypasses coalescer, snapshot publishing, and cache.
//!
//! Both paths use the same `process_entity_ops()` core that translates Op variants
//! into BitmapSink calls using the engine Config for field awareness and
//! `value_to_bitmap_key()` / `value_to_sort_u32()` for value conversion.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::concurrent_engine::ConcurrentEngine;
use crate::config::Config;
use crate::filter::FilterFieldType;
use crate::ingester::BitmapSink;
use crate::mutation::{value_to_bitmap_key, value_to_sort_u32, FieldRegistry};
use crate::pg_sync::op_dedup::dedup_ops;
use crate::pg_sync::ops::{EntityOps, Op};
use crate::query::{BitdexQuery, FilterClause, Value as QValue};

/// Convert a serde_json::Value to a query::Value for bitmap key conversion.
fn json_to_qvalue(v: &JsonValue) -> QValue {
    match v {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                QValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                QValue::Float(f)
            } else {
                QValue::Integer(0)
            }
        }
        JsonValue::Bool(b) => QValue::Bool(*b),
        JsonValue::String(s) => QValue::String(s.clone()),
        JsonValue::Null => QValue::Integer(0),
        _ => QValue::String(v.to_string()),
    }
}

/// Configuration for the ops processor.
pub struct OpsProcessorConfig {
    /// Max records to read per WAL batch
    pub batch_size: usize,
    /// How long to sleep when no new records are available
    pub poll_interval: Duration,
    /// Path to persist the cursor position
    pub cursor_path: PathBuf,
}

impl Default for OpsProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            poll_interval: Duration::from_millis(50),
            cursor_path: PathBuf::from("wal_cursor"),
        }
    }
}

/// Info about a computed sort field: which source fields feed it and the operation.
#[derive(Clone)]
struct ComputedSortInfo {
    /// The computed sort field name (e.g., "sortAt")
    target: String,
    /// Arc<str> for the target field
    target_arc: Arc<str>,
    /// Number of bits for the target sort field
    target_bits: usize,
    /// The computation operation
    op: crate::config::ComputedOp,
    /// Source field names (e.g., ["existedAt", "publishedAt"])
    source_fields: Vec<String>,
}

/// Precomputed field metadata from Config, used during ops processing.
/// Built once, reused across all batches.
pub struct FieldMeta {
    /// Filter field name → (Arc<str>, FilterFieldType)
    filter_fields: HashMap<String, (Arc<str>, FilterFieldType)>,
    /// Sort field name → (Arc<str>, num_bits)
    sort_fields: HashMap<String, (Arc<str>, usize)>,
    /// Reverse map: source_field → computed sort fields that depend on it.
    /// When a source field is set, all computed fields referencing it must be recomputed.
    computed_deps: HashMap<String, Vec<ComputedSortInfo>>,
    /// Field registry for Arc<str> interning (kept for future DocSink use)
    #[allow(dead_code)]
    registry: FieldRegistry,
}

impl FieldMeta {
    /// Build FieldMeta from engine config.
    pub fn from_config(config: &Config) -> Self {
        let registry = FieldRegistry::from_config(config);
        let mut filter_fields = HashMap::new();
        for fc in &config.filter_fields {
            filter_fields.insert(
                fc.name.clone(),
                (registry.get(&fc.name), fc.field_type.clone()),
            );
        }
        let mut sort_fields = HashMap::new();
        for sc in &config.sort_fields {
            sort_fields.insert(
                sc.name.clone(),
                (registry.get(&sc.name), sc.bits as usize),
            );
        }

        // Build reverse dependency map for computed sort fields
        let mut computed_deps: HashMap<String, Vec<ComputedSortInfo>> = HashMap::new();
        for sc in &config.sort_fields {
            if let Some(ref computed) = sc.computed {
                let info = ComputedSortInfo {
                    target: sc.name.clone(),
                    target_arc: registry.get(&sc.name),
                    target_bits: sc.bits as usize,
                    op: computed.op.clone(),
                    source_fields: computed.source_fields.clone(),
                };
                for source in &computed.source_fields {
                    computed_deps
                        .entry(source.clone())
                        .or_default()
                        .push(info.clone());
                }
            }
        }

        Self {
            filter_fields,
            sort_fields,
            computed_deps,
            registry,
        }
    }

    /// Check if a sort field is a source for any computed field.
    fn has_computed_deps(&self, field: &str) -> bool {
        self.computed_deps.contains_key(field)
    }
}

/// Process a batch of entity ops, translating them into BitmapSink calls.
///
/// This is the core function used by both steady-state (CoalescerSink) and
/// dump (AccumSink) paths. The sink determines where mutations go.
///
/// For queryOpSet resolution, an engine reference is needed to execute queries.
/// Pass `None` during dump mode (queryOpSets are only used in steady-state).
///
/// Returns (applied, skipped, errors).
pub fn apply_ops_batch<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    batch: &mut Vec<EntityOps>,
    engine: Option<&ConcurrentEngine>,
) -> (usize, usize, usize) {
    dedup_ops(batch);

    let mut applied = 0usize;
    let mut skipped = 0usize;
    let mut errors = 0usize;

    for entry in batch.iter() {
        let entity_id = entry.entity_id;
        if entity_id < 0 || entity_id > u32::MAX as i64 {
            skipped += 1;
            continue;
        }
        let slot = entity_id as u32;

        // Delete absorbs everything — clear all bitmaps for this slot.
        if entry.ops.iter().any(|op| matches!(op, Op::Delete)) {
            match process_delete(sink, meta, slot, engine) {
                Ok(()) => applied += 1,
                Err(e) => {
                    tracing::warn!("ops processor: delete slot {slot} failed: {e}");
                    errors += 1;
                }
            }
            continue;
        }

        // Handle queryOpSets (steady-state only — needs engine for query resolution)
        for op in &entry.ops {
            if let Op::QueryOpSet { query, ops } = op {
                if let Some(eng) = engine {
                    match apply_query_op_set(sink, meta, eng, query, ops) {
                        Ok(count) => applied += count,
                        Err(e) => {
                            tracing::warn!("ops processor: queryOpSet '{query}' failed: {e}");
                            errors += 1;
                        }
                    }
                } else {
                    tracing::warn!("ops processor: queryOpSet skipped (no engine in dump mode)");
                    skipped += 1;
                }
            }
        }

        // Process set/remove/add ops → direct bitmap mutations.
        // Track sort field values for computed field recomputation.
        let mut has_any_ops = false;
        let mut sort_values: HashMap<&str, u32> = HashMap::new();
        for op in &entry.ops {
            match op {
                Op::Set { field, value } => {
                    process_set_op(sink, meta, slot, field, value);
                    // Track sort value for computed field deps
                    if meta.has_computed_deps(field) || meta.sort_fields.contains_key(field.as_str()) {
                        let qval = json_to_qvalue(value);
                        if let Some(sv) = value_to_sort_u32(&qval) {
                            sort_values.insert(field.as_str(), sv);
                        }
                    }
                    has_any_ops = true;
                }
                Op::Remove { field, value } => {
                    process_remove_op(sink, meta, slot, field, value);
                    has_any_ops = true;
                }
                Op::Add { field, value } => {
                    process_add_op(sink, meta, slot, field, value);
                    has_any_ops = true;
                }
                Op::Delete | Op::QueryOpSet { .. } => {
                    // Already handled above
                }
            }
        }

        // Recompute any computed sort fields whose source fields were set.
        // For GREATEST(existedAt, publishedAt): if existedAt was set, compute
        // sortAt = max(existedAt, current_publishedAt). If only one source is
        // available, use it directly (the other defaults to 0).
        if !meta.computed_deps.is_empty() {
            for (source_field, deps) in &meta.computed_deps {
                if let Some(&source_val) = sort_values.get(source_field.as_str()) {
                    for dep in deps {
                        // Gather all source values — use tracked values from this
                        // batch, or 0 if not available (source not in this entity's ops)
                        let values: Vec<u32> = dep.source_fields.iter()
                            .map(|sf| sort_values.get(sf.as_str()).copied().unwrap_or(0))
                            .collect();

                        let computed_val = match dep.op {
                            crate::config::ComputedOp::Greatest => *values.iter().max().unwrap_or(&0),
                            crate::config::ComputedOp::Least => *values.iter().min().unwrap_or(&0),
                        };

                        // Set sort layers for the computed field
                        for bit in 0..dep.target_bits {
                            if (computed_val >> bit) & 1 == 1 {
                                sink.sort_set(dep.target_arc.clone(), bit, slot);
                            }
                            // Note: we don't clear old bits here because during dump,
                            // the computed field starts at 0 (all bits clear).
                            // For steady-state, the remove op on the source field
                            // should be paired with clearing the old computed value.
                        }
                    }
                }
            }
        }

        // Set alive only if creates_slot is true (primary entity table).
        // Join tables (tags, tools) set creates_slot=false — they only
        // add multi-value bitmaps to existing slots.
        if entry.creates_slot {
            sink.alive_insert(slot);
        }

        if has_any_ops {
            applied += 1;
        }
    }

    // Flush buffered operations
    if let Err(e) = sink.flush() {
        tracing::error!("ops processor: sink flush failed: {e}");
        errors += 1;
    }

    (applied, skipped, errors)
}

/// Process a `set` op: set the new value's bitmap bit for this slot.
fn process_set_op<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    slot: u32,
    field: &str,
    value: &JsonValue,
) {
    let qval = json_to_qvalue(value);

    // Check if this is a filter field
    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if let Some(key) = value_to_bitmap_key(&qval) {
            sink.filter_insert(arc_name.clone(), key, slot);
        }
    }

    // Check if this is a sort field
    if let Some((arc_name, num_bits)) = meta.sort_fields.get(field) {
        if let Some(sort_val) = value_to_sort_u32(&qval) {
            for bit in 0..*num_bits {
                if (sort_val >> bit) & 1 == 1 {
                    sink.sort_set(arc_name.clone(), bit, slot);
                }
            }
        }
    }
}

/// Process a `remove` op: clear the old value's bitmap bit for this slot.
fn process_remove_op<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    slot: u32,
    field: &str,
    value: &JsonValue,
) {
    let qval = json_to_qvalue(value);

    // Check if this is a filter field
    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if let Some(key) = value_to_bitmap_key(&qval) {
            sink.filter_remove(arc_name.clone(), key, slot);
        }
    }

    // Check if this is a sort field
    if let Some((arc_name, num_bits)) = meta.sort_fields.get(field) {
        if let Some(sort_val) = value_to_sort_u32(&qval) {
            for bit in 0..*num_bits {
                if (sort_val >> bit) & 1 == 1 {
                    sink.sort_clear(arc_name.clone(), bit, slot);
                }
            }
        }
    }
}

/// Process an `add` op: set a multi-value bitmap bit.
/// Same as `set` for bitmap purposes — adds the value's bit.
fn process_add_op<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    slot: u32,
    field: &str,
    value: &JsonValue,
) {
    let qval = json_to_qvalue(value);

    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if let Some(key) = value_to_bitmap_key(&qval) {
            sink.filter_insert(arc_name.clone(), key, slot);
        }
    }
    // Multi-value fields don't have sort layers, but handle it generically
    if let Some((arc_name, num_bits)) = meta.sort_fields.get(field) {
        if let Some(sort_val) = value_to_sort_u32(&qval) {
            for bit in 0..*num_bits {
                if (sort_val >> bit) & 1 == 1 {
                    sink.sort_set(arc_name.clone(), bit, slot);
                }
            }
        }
    }
}

/// Process a delete: read stored doc from engine to know which bitmaps to clear
/// (clean delete principle), then clear all filter/sort bits + alive bit.
///
/// Per design doc H1: deletes are the one op type that requires a docstore read.
fn process_delete<S: BitmapSink>(
    sink: &mut S,
    _meta: &FieldMeta,
    slot: u32,
    engine: Option<&ConcurrentEngine>,
) -> std::result::Result<(), String> {
    // If we have an engine, read stored doc to clear filter/sort bitmaps cleanly.
    // Without engine (dump mode), we can only clear alive — filter bitmaps may be stale.
    if let Some(eng) = engine {
        // Use the engine's delete method which handles clean delete internally.
        eng.delete(slot).map_err(|e| format!("engine delete failed: {e}"))?;
        return Ok(());
    }

    // Dump mode fallback: just clear alive bit (no stored doc to read)
    sink.alive_remove(slot);
    Ok(())
}

/// Resolve a queryOpSet: execute the query to get matching slots,
/// then apply the nested ops to each matching slot via the BitmapSink.
fn apply_query_op_set<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    engine: &ConcurrentEngine,
    query_str: &str,
    ops: &[Op],
) -> std::result::Result<usize, String> {
    let filters = parse_filter_from_query_str(query_str)?;

    let query = BitdexQuery {
        filters,
        sort: None,
        limit: usize::MAX,
        offset: None,
        cursor: None,
        skip_cache: true,
    };

    let result = engine
        .execute_query(&query)
        .map_err(|e| format!("queryOpSet query failed: {e}"))?;

    let slot_ids = &result.ids;
    if slot_ids.is_empty() {
        return Ok(0);
    }

    // Apply nested ops to each matching slot
    let mut applied = 0;
    for &slot_id in slot_ids {
        if slot_id < 0 || slot_id > u32::MAX as i64 {
            continue;
        }
        let slot = slot_id as u32;

        for op in ops {
            match op {
                Op::Set { field, value } => {
                    process_set_op(sink, meta, slot, field, value);
                }
                Op::Remove { field, value } => {
                    process_remove_op(sink, meta, slot, field, value);
                }
                Op::Add { field, value } => {
                    process_add_op(sink, meta, slot, field, value);
                }
                Op::Delete => {
                    // Delete within queryOpSet clears alive for each matched slot
                    sink.alive_remove(slot);
                }
                Op::QueryOpSet { .. } => {
                    // Nested queryOpSets not supported
                    tracing::warn!("nested queryOpSet ignored");
                }
            }
        }
        applied += 1;
    }

    Ok(applied)
}

/// Parse a simple filter string like "modelVersionIds eq 456" or "postId eq 789"
/// into filter clauses.
fn parse_filter_from_query_str(query_str: &str) -> std::result::Result<Vec<FilterClause>, String> {
    let clauses: Vec<&str> = query_str.split(" AND ").collect();
    let mut filters = Vec::new();

    for clause in clauses {
        let parts: Vec<&str> = clause.trim().splitn(3, ' ').collect();
        if parts.len() < 3 {
            return Err(format!("Invalid filter clause: '{clause}'"));
        }

        let field = parts[0].to_string();
        let op = parts[1].to_lowercase();
        let value_str = parts[2];

        let filter = match op.as_str() {
            "eq" => {
                let value = parse_query_value(value_str)?;
                FilterClause::Eq(field, value)
            }
            "in" => {
                let values = parse_query_values_array(value_str)?;
                FilterClause::In(field, values)
            }
            _ => {
                return Err(format!("Unsupported filter op '{op}' in queryOpSet"));
            }
        };
        filters.push(filter);
    }

    Ok(filters)
}

/// Parse a single query value from a string.
fn parse_query_value(s: &str) -> std::result::Result<QValue, String> {
    if let Ok(n) = s.parse::<i64>() {
        return Ok(QValue::Integer(n));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(QValue::Float(f));
    }
    if s == "true" {
        return Ok(QValue::Bool(true));
    }
    if s == "false" {
        return Ok(QValue::Bool(false));
    }
    let stripped = s.trim_matches('"').trim_matches('\'');
    Ok(QValue::String(stripped.to_string()))
}

/// Parse an array of query values like "[101, 102, 103]".
fn parse_query_values_array(s: &str) -> std::result::Result<Vec<QValue>, String> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(format!("Expected array for 'in' filter, got: '{s}'"));
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    let mut values = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if !part.is_empty() {
            values.push(parse_query_value(part)?);
        }
    }
    Ok(values)
}

/// Process a batch of entity ops in dump mode using AccumSink.
///
/// This is the bulk-loading path that bypasses the coalescer entirely.
/// Ops are accumulated directly into bitmaps (like the single-pass loader).
///
/// Returns (applied, skipped, errors).
pub(crate) fn apply_ops_batch_dump(
    accum: &mut crate::loader::BitmapAccum,
    meta: &FieldMeta,
    batch: &mut Vec<EntityOps>,
) -> (usize, usize, usize) {
    let mut sink = crate::ingester::AccumSink::new(accum);
    apply_ops_batch(&mut sink, meta, batch, None)
}

/// Process all WAL entries in dump mode: reads WAL, accumulates bitmaps, applies to engine.
///
/// This is the high-level dump pipeline entry point. It:
/// 1. Creates a BitmapAccum from the engine config
/// 2. Reads all WAL entries, processes via AccumSink
/// 3. Applies accumulated bitmaps directly to engine staging
///
/// Returns (total_applied, total_errors, elapsed_secs).
pub fn process_wal_dump(
    engine: &ConcurrentEngine,
    wal_path: &Path,
    batch_size: usize,
) -> (u64, u64, f64) {
    use crate::loader::BitmapAccum;
    use crate::ops_wal::WalReader;
    use std::time::Instant;

    let config = engine.config();
    let meta = FieldMeta::from_config(config);

    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config.sort_fields.iter().map(|s| (s.name.clone(), s.bits)).collect();
    let mut accum = BitmapAccum::new(&filter_names, &sort_configs);

    let start = Instant::now();
    let mut reader = WalReader::new(wal_path, 0);
    let mut total_applied = 0u64;
    let mut total_errors = 0u64;

    loop {
        let batch = match reader.read_batch(batch_size) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("WAL read error in dump mode: {e}");
                total_errors += 1;
                break;
            }
        };
        if batch.entries.is_empty() {
            break;
        }
        let mut entries = batch.entries;
        let (applied, _skipped, errors) = apply_ops_batch_dump(&mut accum, &meta, &mut entries);
        total_applied += applied as u64;
        total_errors += errors as u64;
    }

    // Apply accumulated bitmaps to engine staging
    engine.apply_accum(&accum);

    (total_applied, total_errors, start.elapsed().as_secs_f64())
}

/// Direct dump pipeline: CSV → chunked reader → rayon parallel parse → BitmapAccum → apply.
///
/// Bypasses WAL entirely. Uses a reader thread + rayon fold+reduce for parallel
/// CSV parsing, matching the single-pass loader's throughput pattern. Memory-safe
/// at any scale — reads in ~300MB blocks, never loads the full file.
///
/// Returns (total_applied, total_errors, elapsed_secs).
pub fn process_csv_dump_direct(
    engine: &ConcurrentEngine,
    csv_dir: &Path,
    _batch_size: usize,
    limit: Option<u64>,
) -> (u64, u64, f64) {
    use crate::loader::BitmapAccum;
    use crate::pg_sync::copy_queries::{parse_image_row, parse_tag_row, parse_tool_row};
    use rayon::prelude::*;
    use std::io::BufRead;
    use std::time::Instant;

    let config = engine.config();
    let meta = FieldMeta::from_config(config);

    let filter_names: Vec<String> = config.filter_fields.iter().map(|f| f.name.clone()).collect();
    let sort_configs: Vec<(String, u8)> = config.sort_fields.iter().map(|s| (s.name.clone(), s.bits)).collect();

    let start = Instant::now();
    let mut total_applied = 0u64;
    let mut total_errors = 0u64;
    let record_limit = limit.map(|l| l as usize).unwrap_or(usize::MAX);

    // Enter loading mode ONCE for the entire dump — avoids Arc clone cascade.
    engine.enter_loading_mode();

    // Chunk size for reading CSV lines. 10M lines per chunk keeps memory bounded
    // (~1-2GB per chunk) while giving rayon enough work for parallelism.
    const CHUNK_SIZE: usize = 10_000_000;

    /// Helper: read up to `chunk` lines from a BufReader, returns lines read.
    fn read_chunk(
        reader: &mut impl BufRead,
        chunk: usize,
        buf: &mut Vec<Vec<u8>>,
    ) -> usize {
        buf.clear();
        let mut count = 0;
        let mut line_buf = Vec::new();
        while count < chunk {
            line_buf.clear();
            match reader.read_until(b'\n', &mut line_buf) {
                Ok(0) => break, // EOF
                Ok(_) => {
                    // Trim trailing newline
                    if line_buf.last() == Some(&b'\n') { line_buf.pop(); }
                    if line_buf.last() == Some(&b'\r') { line_buf.pop(); }
                    if !line_buf.is_empty() {
                        buf.push(std::mem::take(&mut line_buf));
                        line_buf = Vec::new();
                        count += 1;
                    }
                }
                Err(_) => break,
            }
        }
        count
    }

    // Phase 1: Images (creates alive slots) — chunked
    let images_csv = csv_dir.join("images.csv");
    if images_csv.exists() {
        let img_start = Instant::now();
        let file = std::fs::File::open(&images_csv).expect("open images.csv");
        let mut reader = std::io::BufReader::with_capacity(8 * 1024 * 1024, file);
        let mut phase_total = 0usize;
        let mut phase_errors = 0u64;
        let mut chunk_buf = Vec::with_capacity(CHUNK_SIZE);

        let f_names = &filter_names;
        let s_configs = &sort_configs;
        let meta_ref = &meta;

        loop {
            let remaining = record_limit.saturating_sub(phase_total);
            if remaining == 0 { break; }
            let n = read_chunk(&mut reader, remaining.min(CHUNK_SIZE), &mut chunk_buf);
            if n == 0 { break; }

            let accum = chunk_buf
                .par_iter()
                .fold(
                    || BitmapAccum::new(f_names, s_configs),
                    |mut acc, line| {
                        let row = match parse_image_row(line) {
                            Some(r) => r,
                            None => { acc.errors += 1; return acc; }
                        };
                        let slot = row.id as u32;
                        acc.alive.insert(slot);

                        let ops = crate::pg_sync::csv_ops::image_row_to_ops_pub(&row);
                        for op in &ops {
                            if let Op::Set { field, value } = op {
                                let qval = json_to_qvalue(value);
                                if let Some((_, _)) = meta_ref.filter_fields.get(field.as_str()) {
                                    if let Some(key) = value_to_bitmap_key(&qval) {
                                        if let Some(m) = acc.filter_maps.get_mut(field.as_str()) {
                                            m.entry(key).or_insert_with(roaring::RoaringBitmap::new).insert(slot);
                                        }
                                    }
                                }
                                if let Some((_, num_bits)) = meta_ref.sort_fields.get(field.as_str()) {
                                    if let Some(sort_val) = value_to_sort_u32(&qval) {
                                        if let Some(m) = acc.sort_maps.get_mut(field.as_str()) {
                                            for bit in 0..*num_bits {
                                                if (sort_val >> bit) & 1 == 1 {
                                                    m.entry(bit).or_insert_with(roaring::RoaringBitmap::new).insert(slot);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        acc.count += 1;
                        acc
                    },
                )
                .reduce(
                    || BitmapAccum::new(f_names, s_configs),
                    |a, b| a.merge(b),
                );

            phase_total += accum.count;
            phase_errors += accum.errors;
            engine.apply_accum(&accum);

            eprintln!("  images: chunk {}..{} ({:.0}/s)",
                phase_total - accum.count, phase_total,
                accum.count as f64 / img_start.elapsed().as_secs_f64().max(0.001));
        }
        total_applied += phase_total as u64;
        total_errors += phase_errors;

        eprintln!("  images: {} rows total, {:.1}s ({:.0}/s)",
            phase_total,
            img_start.elapsed().as_secs_f64(),
            phase_total as f64 / img_start.elapsed().as_secs_f64().max(0.001));
    }

    // Phase 2: Tags (chunked rayon)
    let tags_csv = csv_dir.join("tags.csv");
    if tags_csv.exists() {
        let tag_start = Instant::now();
        let file = std::fs::File::open(&tags_csv).expect("open tags.csv");
        let mut reader = std::io::BufReader::with_capacity(8 * 1024 * 1024, file);
        let mut phase_total = 0usize;
        let mut phase_errors = 0u64;
        let mut chunk_buf = Vec::with_capacity(CHUNK_SIZE);

        let f_names = &filter_names;
        let s_configs = &sort_configs;

        loop {
            let remaining = record_limit.saturating_sub(phase_total);
            if remaining == 0 { break; }
            let n = read_chunk(&mut reader, remaining.min(CHUNK_SIZE), &mut chunk_buf);
            if n == 0 { break; }

            let accum = chunk_buf
                .par_iter()
                .fold(
                    || BitmapAccum::new(f_names, s_configs),
                    |mut acc, line| {
                        let (tag_id, image_id) = match parse_tag_row(line) {
                            Some(pair) => pair,
                            None => { acc.errors += 1; return acc; }
                        };
                        let slot = image_id as u32;
                        if let Some(m) = acc.filter_maps.get_mut("tagIds") {
                            m.entry(tag_id as u64)
                                .or_insert_with(roaring::RoaringBitmap::new)
                                .insert(slot);
                        }
                        acc.count += 1;
                        acc
                    },
                )
                .reduce(
                    || BitmapAccum::new(f_names, s_configs),
                    |a, b| a.merge(b),
                );

            phase_total += accum.count;
            phase_errors += accum.errors;
            engine.apply_accum(&accum);
        }
        total_applied += phase_total as u64;
        total_errors += phase_errors;

        eprintln!("  tags: {} rows, {:.1}s ({:.0}/s)",
            phase_total,
            tag_start.elapsed().as_secs_f64(),
            phase_total as f64 / tag_start.elapsed().as_secs_f64().max(0.001));
    }

    // Phase 3: Tools (chunked rayon)
    let tools_csv = csv_dir.join("tools.csv");
    if tools_csv.exists() {
        let tool_start = Instant::now();
        let file = std::fs::File::open(&tools_csv).expect("open tools.csv");
        let mut reader = std::io::BufReader::with_capacity(8 * 1024 * 1024, file);
        let mut phase_total = 0usize;
        let mut phase_errors = 0u64;
        let mut chunk_buf = Vec::with_capacity(CHUNK_SIZE);

        let f_names = &filter_names;
        let s_configs = &sort_configs;

        loop {
            let remaining = record_limit.saturating_sub(phase_total);
            if remaining == 0 { break; }
            let n = read_chunk(&mut reader, remaining.min(CHUNK_SIZE), &mut chunk_buf);
            if n == 0 { break; }

            let accum = chunk_buf
                .par_iter()
                .fold(
                    || BitmapAccum::new(f_names, s_configs),
                    |mut acc, line| {
                        let (tool_id, image_id) = match parse_tool_row(line) {
                            Some(pair) => pair,
                            None => { acc.errors += 1; return acc; }
                        };
                        let slot = image_id as u32;
                        if let Some(m) = acc.filter_maps.get_mut("toolIds") {
                            m.entry(tool_id as u64)
                                .or_insert_with(roaring::RoaringBitmap::new)
                                .insert(slot);
                        }
                        acc.count += 1;
                        acc
                    },
                )
                .reduce(
                    || BitmapAccum::new(f_names, s_configs),
                    |a, b| a.merge(b),
                );

            phase_total += accum.count;
            phase_errors += accum.errors;
            engine.apply_accum(&accum);
        }
        total_applied += phase_total as u64;
        total_errors += phase_errors;

        eprintln!("  tools: {} rows, {:.1}s ({:.0}/s)",
            phase_total,
            tool_start.elapsed().as_secs_f64(),
            phase_total as f64 / tool_start.elapsed().as_secs_f64().max(0.001));
    }

    // Exit loading mode. On headless, this will timeout (no flush thread) but
    // that's OK — the warning is harmless and the flag gets cleared.
    engine.exit_loading_mode();

    eprintln!("  Total: {total_applied} ops in {:.1}s ({:.0}/s)",
        start.elapsed().as_secs_f64(),
        total_applied as f64 / start.elapsed().as_secs_f64().max(0.001));

    (total_applied, total_errors, start.elapsed().as_secs_f64())
}

/// Persist cursor position to disk.
pub fn save_cursor(path: &Path, cursor: u64) -> std::io::Result<()> {
    std::fs::write(path, cursor.to_string())
}

/// Load cursor position from disk. Returns 0 if file doesn't exist.
pub fn load_cursor(path: &Path) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::config::{Config, FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::ingester::BitmapSink;

    /// A test sink that records all operations for verification.
    struct RecordingSink {
        filter_inserts: Vec<(String, u64, u32)>,
        filter_removes: Vec<(String, u64, u32)>,
        sort_sets: Vec<(String, usize, u32)>,
        sort_clears: Vec<(String, usize, u32)>,
        alive_inserts: Vec<u32>,
        alive_removes: Vec<u32>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self {
                filter_inserts: Vec::new(),
                filter_removes: Vec::new(),
                sort_sets: Vec::new(),
                sort_clears: Vec::new(),
                alive_inserts: Vec::new(),
                alive_removes: Vec::new(),
            }
        }
    }

    impl BitmapSink for RecordingSink {
        fn filter_insert(&mut self, field: Arc<str>, value: u64, slot: u32) {
            self.filter_inserts.push((field.to_string(), value, slot));
        }
        fn filter_remove(&mut self, field: Arc<str>, value: u64, slot: u32) {
            self.filter_removes.push((field.to_string(), value, slot));
        }
        fn sort_set(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
            self.sort_sets.push((field.to_string(), bit_layer, slot));
        }
        fn sort_clear(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
            self.sort_clears.push((field.to_string(), bit_layer, slot));
        }
        fn alive_insert(&mut self, slot: u32) {
            self.alive_inserts.push(slot);
        }
        fn alive_remove(&mut self, slot: u32) {
            self.alive_removes.push(slot);
        }
        fn flush(&mut self) -> crate::error::Result<()> {
            Ok(())
        }
    }

    fn test_config() -> Config {
        let mut config = Config::default();
        config.filter_fields = vec![
            FilterFieldConfig {
                name: "nsfwLevel".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
            FilterFieldConfig {
                name: "type".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
            FilterFieldConfig {
                name: "tagIds".into(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
            FilterFieldConfig {
                name: "hasMeta".into(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
            },
        ];
        config.sort_fields = vec![SortFieldConfig {
            name: "existedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        }];
        config
    }

    #[test]
    fn test_set_op_filter_insert() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "nsfwLevel".into(),
                value: json!(16),
            }],
        }];

        let (applied, skipped, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None);
        assert_eq!(applied, 1);
        assert_eq!(skipped, 0);
        assert_eq!(errors, 0);

        assert_eq!(sink.filter_inserts.len(), 1);
        assert_eq!(sink.filter_inserts[0], ("nsfwLevel".to_string(), 16, 42));
        assert_eq!(sink.alive_inserts, vec![42]);
    }

    #[test]
    fn test_remove_then_set_scalar_update() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![
                Op::Remove {
                    field: "nsfwLevel".into(),
                    value: json!(8),
                },
                Op::Set {
                    field: "nsfwLevel".into(),
                    value: json!(16),
                },
            ],
        }];

        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);

        // Should have one remove (old value 8) and one insert (new value 16)
        assert_eq!(sink.filter_removes.len(), 1);
        assert_eq!(sink.filter_removes[0], ("nsfwLevel".to_string(), 8, 42));
        assert_eq!(sink.filter_inserts.len(), 1);
        assert_eq!(sink.filter_inserts[0], ("nsfwLevel".to_string(), 16, 42));
    }

    #[test]
    fn test_add_multi_value() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: 100,
            creates_slot: false,
            ops: vec![
                Op::Add {
                    field: "tagIds".into(),
                    value: json!(42),
                },
                Op::Add {
                    field: "tagIds".into(),
                    value: json!(99),
                },
            ],
        }];

        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);

        assert_eq!(sink.filter_inserts.len(), 2);
        // Order after dedup is nondeterministic (HashMap iteration)
        let mut values: Vec<u64> = sink.filter_inserts.iter().map(|(_, v, _)| *v).collect();
        values.sort();
        assert_eq!(values, vec![42, 99]);
        // Add-only ops should NOT set alive (only Set ops do)
        assert!(sink.alive_inserts.is_empty());
    }

    #[test]
    fn test_sort_field_bit_decomposition() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        // existedAt = 5 = 0b101 → bits 0 and 2 set
        let mut batch = vec![EntityOps {
            entity_id: 10,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "existedAt".into(),
                value: json!(5),
            }],
        }];

        apply_ops_batch(&mut sink, &meta, &mut batch, None);

        // Should have sort_set for bits 0 and 2
        let sort_bits: Vec<usize> = sink.sort_sets.iter().map(|(_, bit, _)| *bit).collect();
        assert!(sort_bits.contains(&0));
        assert!(sort_bits.contains(&2));
        assert!(!sort_bits.contains(&1)); // bit 1 not set for value 5
    }

    #[test]
    fn test_sort_field_remove_clears_bits() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        // Remove old sort value 5 = 0b101, set new value 6 = 0b110
        let mut batch = vec![EntityOps {
            entity_id: 10,
            creates_slot: true,
            ops: vec![
                Op::Remove {
                    field: "existedAt".into(),
                    value: json!(5),
                },
                Op::Set {
                    field: "existedAt".into(),
                    value: json!(6),
                },
            ],
        }];

        apply_ops_batch(&mut sink, &meta, &mut batch, None);

        // Clears: bits 0, 2 (from value 5)
        let clear_bits: Vec<usize> = sink.sort_clears.iter().map(|(_, bit, _)| *bit).collect();
        assert!(clear_bits.contains(&0));
        assert!(clear_bits.contains(&2));

        // Sets: bits 1, 2 (from value 6)
        let set_bits: Vec<usize> = sink.sort_sets.iter().map(|(_, bit, _)| *bit).collect();
        assert!(set_bits.contains(&1));
        assert!(set_bits.contains(&2));
    }

    #[test]
    fn test_boolean_field() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: 50,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "hasMeta".into(),
                value: json!(true),
            }],
        }];

        apply_ops_batch(&mut sink, &meta, &mut batch, None);

        // true → bitmap key 1
        assert_eq!(sink.filter_inserts.len(), 1);
        assert_eq!(sink.filter_inserts[0], ("hasMeta".to_string(), 1, 50));
    }

    #[test]
    fn test_unknown_field_ignored() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: 1,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "unknownField".into(),
                value: json!(42),
            }],
        }];

        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None);
        assert_eq!(applied, 1); // still counts as applied (alive set)
        assert_eq!(errors, 0);

        // No filter or sort ops emitted for unknown field
        assert!(sink.filter_inserts.is_empty());
        assert!(sink.sort_sets.is_empty());
    }

    #[test]
    fn test_delete_without_engine() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: false,
            ops: vec![Op::Delete],
        }];

        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);

        // In dump mode (no engine), delete only clears alive
        assert_eq!(sink.alive_removes, vec![42]);
    }

    #[test]
    fn test_image_insert_all_fields() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: 1000,
            creates_slot: true,
            ops: vec![
                Op::Set {
                    field: "nsfwLevel".into(),
                    value: json!(1),
                },
                Op::Set {
                    field: "type".into(),
                    value: json!(0), // "image" mapped to 0
                },
                Op::Set {
                    field: "hasMeta".into(),
                    value: json!(true),
                },
                Op::Set {
                    field: "existedAt".into(),
                    value: json!(1711234567u64),
                },
            ],
        }];

        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);

        // 3 filter inserts (nsfwLevel, type, hasMeta) + sort bits for existedAt
        assert_eq!(sink.filter_inserts.len(), 3);
        assert!(!sink.sort_sets.is_empty()); // existedAt bit layers
        assert_eq!(sink.alive_inserts, vec![1000]);
    }

    #[test]
    fn test_negative_entity_id_skipped() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();

        let mut batch = vec![EntityOps {
            entity_id: -1,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "nsfwLevel".into(),
                value: json!(1),
            }],
        }];

        let (_, skipped, _) = apply_ops_batch(&mut sink, &meta, &mut batch, None);
        assert_eq!(skipped, 1);
        assert!(sink.filter_inserts.is_empty());
    }

    #[test]
    fn test_parse_filter_eq() {
        let filters = parse_filter_from_query_str("modelVersionIds eq 456").unwrap();
        assert_eq!(filters.len(), 1);
        assert!(matches!(
            &filters[0],
            FilterClause::Eq(f, QValue::Integer(456)) if f == "modelVersionIds"
        ));
    }

    #[test]
    fn test_parse_filter_in() {
        let filters =
            parse_filter_from_query_str("modelVersionIds in [101, 102, 103]").unwrap();
        assert_eq!(filters.len(), 1);
        if let FilterClause::In(f, vals) = &filters[0] {
            assert_eq!(f, "modelVersionIds");
            assert_eq!(vals.len(), 3);
        } else {
            panic!("Expected In clause");
        }
    }

    #[test]
    fn test_parse_query_value_types() {
        assert!(matches!(
            parse_query_value("42").unwrap(),
            QValue::Integer(42)
        ));
        assert!(matches!(
            parse_query_value("true").unwrap(),
            QValue::Bool(true)
        ));
        assert!(matches!(
            parse_query_value("\"hello\"").unwrap(),
            QValue::String(s) if s == "hello"
        ));
    }

    #[test]
    fn test_cursor_persistence() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        assert_eq!(load_cursor(&path), 0);
        save_cursor(&path, 12345).unwrap();
        assert_eq!(load_cursor(&path), 12345);
    }
}
