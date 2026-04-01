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
use crate::dictionary::FieldDictionary;
use crate::shard_store_doc::PackedValue;
use crate::shard_store_doc::DocStoreV3;
use crate::filter::FilterFieldType;
use crate::ingester::BitmapSink;
use crate::mutation::{value_to_bitmap_key, value_to_sort_u32, FieldRegistry};
use crate::pg_sync::op_dedup::dedup_ops;
use crate::pg_sync::ops::{EntityOps, Op};
use crate::query::{BitdexQuery, FilterClause, Value as QValue};
// ---------------------------------------------------------------------------
// DocWriter — writes field values to docstore alongside bitmap mutations
// ---------------------------------------------------------------------------
/// Writes individual field-value tuples to the docstore during WAL processing.
/// Wraps the engine's docstore with a cached field dictionary snapshot.
///
/// Thread safety: DocWriter is used exclusively by the WAL reader thread,
/// which is single-threaded. The read-modify-write in write_add/write_remove
/// is safe because no concurrent writer can modify the same slot's doc between
/// the read and write within a single WAL batch cycle.
pub struct DocWriter {
    docstore: Arc<parking_lot::Mutex<DocStoreV3>>,
    field_dict: HashMap<String, u16>,
    pending: Vec<(u32, u16, Vec<u8>)>,
}
impl DocWriter {
    /// Create a DocWriter from the engine's docstore.
    pub fn new(docstore: Arc<parking_lot::Mutex<DocStoreV3>>) -> Self {
        let field_dict = docstore.lock().field_dict_snapshot();
        Self {
            docstore,
            field_dict,
            pending: Vec::new(),
        }
    }
    /// Write a single-value field update to the docstore.
    /// Clamps negative integers to 0 — sort fields (reactionCount, etc.) are
    /// unsigned in bitmaps; storing negatives in docstore would diverge from
    /// the bitmap value and confuse shadow-mode comparisons.
    fn write_set(&mut self, slot: u32, field: &str, value: &JsonValue) {
        let idx = match self.resolve_field(field) {
            Some(idx) => idx,
            None => return,
        };
        // Clamp negative integers to 0 before docstore write
        let clamped;
        let effective = if let Some(n) = value.as_i64() {
            if n < 0 {
                clamped = serde_json::json!(0);
                &clamped
            } else {
                value
            }
        } else {
            value
        };
        if let Some(packed) = json_to_packed(effective) {
            if let Ok(bytes) = rmp_serde::to_vec(&packed) {
                self.pending.push((slot, idx, bytes));
            }
        }
    }
    /// Write a multi-value add: read current list, append value, write back.
    fn write_add(&mut self, slot: u32, field: &str, value: &JsonValue) {
        let idx = match self.resolve_field(field) {
            Some(idx) => idx,
            None => return,
        };
        let add_val = match value.as_i64() {
            Some(v) => v,
            None => return,
        };
        // Read current doc and get existing multi-value list
        let mut current = self.read_multi_value(slot, field);
        if !current.contains(&add_val) {
            current.push(add_val);
        }
        if let Ok(bytes) = rmp_serde::to_vec(&PackedValue::Mi(current)) {
            self.pending.push((slot, idx, bytes));
        }
    }
    /// Write a multi-value remove: read current list, remove value, write back.
    fn write_remove(&mut self, slot: u32, field: &str, value: &JsonValue) {
        let idx = match self.resolve_field(field) {
            Some(idx) => idx,
            None => return,
        };
        let remove_val = match value.as_i64() {
            Some(v) => v,
            None => return,
        };
        let mut current = self.read_multi_value(slot, field);
        current.retain(|&v| v != remove_val);
        if let Ok(bytes) = rmp_serde::to_vec(&PackedValue::Mi(current)) {
            self.pending.push((slot, idx, bytes));
        }
    }
    /// Flush pending tuples to the docstore.
    fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let tuples = std::mem::take(&mut self.pending);
        if let Err(e) = self.docstore.lock().append_tuples_batch(tuples) {
            tracing::warn!("DocWriter flush failed: {e}");
        }
    }
    fn resolve_field(&mut self, field: &str) -> Option<u16> {
        if let Some(&idx) = self.field_dict.get(field) {
            return Some(idx);
        }
        // Field not in snapshot — try to ensure it exists
        match self.docstore.lock().ensure_field_index(field) {
            Ok(idx) => {
                self.field_dict.insert(field.to_string(), idx);
                Some(idx)
            }
            Err(e) => {
                tracing::warn!("DocWriter: failed to ensure field '{field}': {e}");
                None
            }
        }
    }
    fn read_multi_value(&self, slot: u32, field: &str) -> Vec<i64> {
        let doc = match self.docstore.lock().get(slot) {
            Ok(Some(doc)) => doc,
            _ => return Vec::new(),
        };
        match doc.fields.get(field) {
            Some(crate::mutation::FieldValue::Multi(vals)) => {
                vals.iter().filter_map(|v| {
                    if let QValue::Integer(i) = v { Some(*i) } else { None }
                }).collect()
            }
            _ => Vec::new(),
        }
    }
}
// ---------------------------------------------------------------------------
// Document → Ops decomposition (for PUT/PATCH → WAL refactor, task 2.7)
// ---------------------------------------------------------------------------
/// Convert a FieldValue to a serde_json::Value for Op serialization.
pub fn field_value_to_json(fv: &crate::mutation::FieldValue) -> JsonValue {
    match fv {
        crate::mutation::FieldValue::Single(v) => qvalue_to_json(v),
        crate::mutation::FieldValue::Multi(vals) => {
            JsonValue::Array(vals.iter().map(qvalue_to_json).collect())
        }
    }
}
/// Convert a query::Value to a serde_json::Value.
fn qvalue_to_json(v: &QValue) -> JsonValue {
    match v {
        QValue::Integer(i) => JsonValue::Number(serde_json::Number::from(*i)),
        QValue::Float(f) => {
            serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        QValue::Bool(b) => JsonValue::Bool(*b),
        QValue::String(s) => JsonValue::String(s.clone()),
    }
}
/// Decompose a Document into `Vec<Op>` for WAL writing.
///
/// For fresh inserts (old_doc is None): emits Op::Set for each field.
/// For upserts (old_doc is Some): emits Op::Remove for old values + Op::Set for
/// new values on changed fields. Unchanged fields are skipped.
///
/// Multi-value fields are decomposed into individual Op::Add/Op::Remove per value.
///
/// `is_patch`: when true (PATCH semantics), fields absent from new_doc are left
/// untouched — no Op::Remove emitted. When false (PUT semantics), absent fields
/// are treated as deletions and their old bitmap bits are cleared.
pub fn document_to_ops(
    new_doc: &crate::mutation::Document,
    old_doc: Option<&crate::shard_store_doc::StoredDoc>,
    config: &crate::config::Config,
    is_patch: bool,
) -> Vec<Op> {
    let mut ops = Vec::new();
    let empty_fields = std::collections::HashMap::new();
    let old_fields = old_doc.map_or(&empty_fields, |d| &d.fields);
    // Process all fields in the new document
    for (field_name, new_val) in &new_doc.fields {
        let old_val = old_fields.get(field_name);
        // Check if this is a multi-value field (tagIds, toolIds, etc.)
        let is_multi_value = config.filter_fields.iter()
            .any(|f| f.name == *field_name && f.field_type == crate::filter::FilterFieldType::MultiValue);
        if is_multi_value {
            // Multi-value: compute add/remove sets
            let old_ints = extract_multi_ints(old_val);
            let new_ints = extract_multi_ints(Some(new_val));
            // Remove values that were in old but not in new
            for v in &old_ints {
                if !new_ints.contains(v) {
                    ops.push(Op::Remove {
                        field: field_name.clone(),
                        value: JsonValue::Number(serde_json::Number::from(*v)),
                    });
                }
            }
            // Add values that are in new but not in old
            for v in &new_ints {
                if !old_ints.contains(v) {
                    ops.push(Op::Add {
                        field: field_name.clone(),
                        value: JsonValue::Number(serde_json::Number::from(*v)),
                    });
                }
            }
        } else {
            // Single-value field: remove old + set new if changed
            if let Some(old) = old_val {
                if old != new_val {
                    ops.push(Op::Remove {
                        field: field_name.clone(),
                        value: field_value_to_json(old),
                    });
                    ops.push(Op::Set {
                        field: field_name.clone(),
                        value: field_value_to_json(new_val),
                    });
                }
                // else: unchanged, skip
            } else {
                // New field (not in old doc)
                ops.push(Op::Set {
                    field: field_name.clone(),
                    value: field_value_to_json(new_val),
                });
            }
        }
    }
    // For PUT upsert: handle fields that were in old doc but removed in new doc.
    // PATCH skips this — absent fields are left untouched (partial update semantics).
    if old_doc.is_some() && !is_patch {
        for (field_name, old_val) in old_fields {
            if !new_doc.fields.contains_key(field_name) {
                // Field was removed
                let is_multi_value = config.filter_fields.iter()
                    .any(|f| f.name == *field_name && f.field_type == crate::filter::FilterFieldType::MultiValue);
                if is_multi_value {
                    for v in extract_multi_ints(Some(old_val)) {
                        ops.push(Op::Remove {
                            field: field_name.clone(),
                            value: JsonValue::Number(serde_json::Number::from(v)),
                        });
                    }
                } else {
                    ops.push(Op::Remove {
                        field: field_name.clone(),
                        value: field_value_to_json(old_val),
                    });
                }
            }
        }
    }
    ops
}
/// Extract integer values from a multi-value FieldValue.
fn extract_multi_ints(fv: Option<&crate::mutation::FieldValue>) -> Vec<i64> {
    match fv {
        Some(crate::mutation::FieldValue::Multi(vals)) => {
            vals.iter().filter_map(|v| {
                if let QValue::Integer(i) = v { Some(*i) } else { None }
            }).collect()
        }
        Some(crate::mutation::FieldValue::Single(QValue::Integer(i))) => vec![*i],
        _ => Vec::new(),
    }
}
/// Convert a JSON value to a PackedValue for docstore storage.
fn json_to_packed(v: &JsonValue) -> Option<PackedValue> {
    match v {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(PackedValue::I(i))
            } else if let Some(f) = n.as_f64() {
                Some(PackedValue::F(f))
            } else {
                None
            }
        }
        JsonValue::Bool(b) => Some(PackedValue::B(*b)),
        JsonValue::String(s) => Some(PackedValue::S(s.clone())),
        JsonValue::Null => None,
        JsonValue::Array(arr) => {
            let ints: Vec<i64> = arr.iter().filter_map(|v| v.as_i64()).collect();
            if ints.len() == arr.len() {
                Some(PackedValue::Mi(ints))
            } else {
                // Mixed arrays: store as Mm (multi-packed)
                let packed: Vec<PackedValue> = arr.iter()
                    .filter_map(|v| json_to_packed(v))
                    .collect();
                Some(PackedValue::Mm(packed))
            }
        }
        JsonValue::Object(_) => None,
    }
}
// ---------------------------------------------------------------------------
// Enrichment types for dump processing
// ---------------------------------------------------------------------------
/// Post enrichment data, keyed by post_id.
struct PostEnrichment {
    published_at_secs: Option<i64>,
    availability: String,
    // postedToId is derived from Post.modelVersionId — not directly available
    // We use post_id itself as postedToId (Post table's ID is the posted-to entity)
}
/// ModelVersion enrichment data, keyed by model_version_id.
struct MvEnrichment {
    base_model: Option<String>,
    model_id: i64,
}
/// Model enrichment data, keyed by model_id.
struct ModelEnrichment {
    poi: bool,
}
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
    /// Filter fields that accept null values (null Set/Remove = no-op).
    nullable_fields: std::collections::HashSet<String>,
    /// Sort field name → (Arc<str>, num_bits)
    sort_fields: HashMap<String, (Arc<str>, usize)>,
    /// Reverse map: source_field → computed sort fields that depend on it.
    /// When a source field is set, all computed fields referencing it must be recomputed.
    computed_deps: HashMap<String, Vec<ComputedSortInfo>>,
    /// Deferred alive config: if present, the source_field name whose future timestamps
    /// trigger deferred alive instead of immediate alive. ms_to_seconds indicates
    /// whether the field value is in milliseconds (needs /1000 for epoch comparison).
    deferred_alive_field: Option<(String, bool)>,
    /// Field registry for Arc<str> interning (kept for future DocSink use)
    #[allow(dead_code)]
    registry: FieldRegistry,
}
impl FieldMeta {
    /// Build FieldMeta from engine config.
    pub fn from_config(config: &Config) -> Self {
        let registry = FieldRegistry::from_config(config);
        let mut filter_fields = HashMap::new();
        let mut nullable_fields = std::collections::HashSet::new();
        for fc in &config.filter_fields {
            filter_fields.insert(
                fc.name.clone(),
                (registry.get(&fc.name), fc.field_type.clone()),
            );
        }
        // Read nullable from data_schema field mappings
        for fm in &config.data_schema.fields {
            if fm.nullable && filter_fields.contains_key(&fm.target) {
                nullable_fields.insert(fm.target.clone());
            }
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
        // Deferred alive config
        let deferred_alive_field = config.deferred_alive.as_ref().map(|da| {
            (da.source_field.clone(), da.ms_to_seconds)
        });
        Self {
            filter_fields,
            nullable_fields,
            sort_fields,
            computed_deps,
            deferred_alive_field,
            registry,
        }
    }
    /// Check if a sort field is a source for any computed field.
    fn has_computed_deps(&self, field: &str) -> bool {
        self.computed_deps.contains_key(field)
    }
}
// ---------------------------------------------------------------------------
// Enrichment loading — small tables loaded into memory as HashMaps
// ---------------------------------------------------------------------------
/// Load posts.csv into a HashMap<post_id, PostEnrichment>.
/// Posts: id, publishedAtSecs, availability, modelVersionId (4 columns CSV)
fn load_posts_enrichment(csv_dir: &Path) -> HashMap<i64, PostEnrichment> {
    use crate::pg_sync::copy_queries::parse_post_row;
    use std::io::BufRead;
    let path = csv_dir.join("posts.csv");
    if !path.exists() {
        eprintln!("  posts.csv not found, skipping post enrichment");
        return HashMap::new();
    }
    let start = std::time::Instant::now();
    let file = std::fs::File::open(&path).expect("open posts.csv");
    let reader = std::io::BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut map = HashMap::new();
    let mut count = 0u64;
    for line in reader.split(b'\n') {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line.is_empty() { continue; }
        if let Some(row) = parse_post_row(&line) {
            map.insert(row.id, PostEnrichment {
                published_at_secs: row.published_at_secs,
                availability: row.availability,
            });
            count += 1;
        }
    }
    eprintln!("  posts enrichment: {} rows in {:.1}s", count, start.elapsed().as_secs_f64());
    map
}
/// Load model_versions.csv into a HashMap<mv_id, MvEnrichment>.
/// ModelVersions: id, baseModel, modelId (3 columns CSV)
fn load_mv_enrichment(csv_dir: &Path) -> HashMap<i64, MvEnrichment> {
    use crate::pg_sync::copy_queries::parse_model_version_row;
    use std::io::BufRead;
    let path = csv_dir.join("model_versions.csv");
    if !path.exists() {
        eprintln!("  model_versions.csv not found, skipping MV enrichment");
        return HashMap::new();
    }
    let start = std::time::Instant::now();
    let file = std::fs::File::open(&path).expect("open model_versions.csv");
    let reader = std::io::BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut map = HashMap::new();
    let mut count = 0u64;
    for line in reader.split(b'\n') {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line.is_empty() { continue; }
        if let Some(row) = parse_model_version_row(&line) {
            map.insert(row.id, MvEnrichment {
                base_model: row.base_model,
                model_id: row.model_id,
            });
            count += 1;
        }
    }
    eprintln!("  model_versions enrichment: {} rows in {:.1}s", count, start.elapsed().as_secs_f64());
    map
}
/// Load models.csv into a HashMap<model_id, ModelEnrichment>.
/// Models: id, poi, type (3 columns CSV)
fn load_model_enrichment(csv_dir: &Path) -> HashMap<i64, ModelEnrichment> {
    use crate::pg_sync::copy_queries::parse_model_row;
    use std::io::BufRead;
    let path = csv_dir.join("models.csv");
    if !path.exists() {
        eprintln!("  models.csv not found, skipping model enrichment");
        return HashMap::new();
    }
    let start = std::time::Instant::now();
    let file = std::fs::File::open(&path).expect("open models.csv");
    let reader = std::io::BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut map = HashMap::new();
    let mut count = 0u64;
    for line in reader.split(b'\n') {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line.is_empty() { continue; }
        if let Some(row) = parse_model_row(&line) {
            map.insert(row.id, ModelEnrichment {
                poi: row.poi,
            });
            count += 1;
        }
    }
    eprintln!("  models enrichment: {} rows in {:.1}s", count, start.elapsed().as_secs_f64());
    map
}
/// Resolve a string value through the field dictionary, returning the u64 bitmap key.
#[inline]
fn resolve_string_dict(
    dicts: &HashMap<String, FieldDictionary>,
    field: &str,
    value: &str,
) -> Option<u64> {
    dicts.get(field).map(|dict| dict.get_or_insert(value) as u64)
}
/// Set sort layers for a u32 value on a slot in a BitmapAccum.
#[inline]
fn accum_set_sort(
    sort_maps: &mut HashMap<String, HashMap<usize, roaring::RoaringBitmap>>,
    field: &str,
    num_bits: usize,
    value: u32,
    slot: u32,
) {
    if let Some(m) = sort_maps.get_mut(field) {
        for bit in 0..num_bits {
            if (value >> bit) & 1 == 1 {
                m.entry(bit)
                    .or_insert_with(roaring::RoaringBitmap::new)
                    .insert(slot);
            }
        }
    }
}
/// Check if an entity's ops contain a deferred alive condition (future publishedAt).
fn check_deferred_alive(meta: &FieldMeta, ops: &[Op]) -> bool {
    if let Some((ref da_field, ms_to_secs)) = meta.deferred_alive_field {
        for op in ops {
            if let Op::Set { field, value } = op {
                if field == da_field {
                    if let Some(ts) = value.as_i64() {
                        let secs = if ms_to_secs { ts / 1000 } else { ts };
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;
                        return secs > now;
                    }
                }
            }
        }
    }
    false
}
/// Extract the deferred alive timestamp (seconds since epoch) from ops.
fn get_deferred_timestamp(meta: &FieldMeta, ops: &[Op]) -> Option<u64> {
    if let Some((ref da_field, ms_to_secs)) = meta.deferred_alive_field {
        for op in ops {
            if let Op::Set { field, value } = op {
                if field == da_field {
                    if let Some(ts) = value.as_i64() {
                        let secs = if ms_to_secs { ts / 1000 } else { ts };
                        return Some(secs as u64);
                    }
                }
            }
        }
    }
    None
}
/// Process a batch of entity ops, translating them into BitmapSink calls
/// and optionally writing field values to the docstore via DocWriter.
///
/// This is the core function used by both steady-state (CoalescerSink) and
/// dump (AccumSink) paths. The sink determines where mutations go.
///
/// For queryOpSet resolution, an engine reference is needed to execute queries.
/// Pass `None` during dump mode (queryOpSets are only used in steady-state).
///
/// Pass a `DocWriter` for steady-state to keep the docstore in sync with bitmap
/// changes. Pass `None` during dump mode (dump processor handles docs separately).
///
/// Returns (applied, skipped, errors).
pub fn apply_ops_batch<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    batch: &mut Vec<EntityOps>,
    engine: Option<&ConcurrentEngine>,
    mut doc_writer: Option<&mut DocWriter>,
) -> (usize, usize, usize) {
    dedup_ops(batch);
    // One-time diagnostic: log computed_deps status
    if !batch.is_empty() && !meta.computed_deps.is_empty() {
        eprintln!(
            "apply_ops_batch: {} entries, computed_deps has {} source fields: {:?}",
            batch.len(),
            meta.computed_deps.len(),
            meta.computed_deps.keys().collect::<Vec<_>>(),
        );
    } else if !batch.is_empty() && meta.computed_deps.is_empty() {
        eprintln!("apply_ops_batch: {} entries, computed_deps is EMPTY", batch.len());
    }
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
        // [2.10] Drop ops on non-alive slots (except creates_slot=true or queryOpSet).
        // In steady-state, ops arriving for non-existent slots are stale or
        // out-of-order — silently skip them. queryOpSet entries are exempt because
        // their entity_id is the source entity (e.g., Post.id), not the target slot.
        //
        // Safety net: if the slot is beyond the current high-water mark (slot_counter),
        // it's a genuinely new entity — auto-promote to creates_slot behavior.
        // This handles the case where ops_poller doesn't know which table sets alive.
        let has_query_op_set = entry.ops.iter().any(|op| matches!(op, Op::QueryOpSet { .. }));
        let mut creates_slot = entry.creates_slot;
        if !creates_slot && !has_query_op_set {
            if let Some(eng) = engine {
                if !eng.is_slot_alive(slot) {
                    if slot >= eng.slot_counter() {
                        // Slot is beyond high-water mark — this is a new entity,
                        // not a stale op for a deleted slot. Auto-promote.
                        creates_slot = true;
                        tracing::info!(
                            "ops processor: auto-promoting slot {slot} to creates_slot \
                             (entity_id={entity_id}, beyond slot_counter={})",
                            eng.slot_counter()
                        );
                    } else {
                        skipped += 1;
                        continue;
                    }
                }
            }
        }
        // [2.4] Check deferred alive BEFORE processing any ops.
        // If creates_slot=true and publishedAt is in the future, skip ALL bitmaps
        // (alive + filter + sort). Only write docstore so activate_due() can
        // rebuild bitmaps later.
        let is_deferred = if creates_slot {
            check_deferred_alive(meta, &entry.ops)
        } else {
            false
        };
        if is_deferred {
            // Schedule deferred alive + write docstore only (no bitmap ops)
            let da_secs = get_deferred_timestamp(meta, &entry.ops).unwrap_or(0);
            sink.deferred_alive(slot, da_secs);
            if let Some(ref mut dw) = doc_writer {
                for op in &entry.ops {
                    match op {
                        Op::Set { field, value } => dw.write_set(slot, field, value),
                        Op::Add { field, value } => dw.write_add(slot, field, value),
                        _ => {}
                    }
                }
                // Still flush since we're skipping bitmap processing
            }
            applied += 1;
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
        // Process set/remove/add ops → direct bitmap mutations + docstore writes.
        // Track sort field values for computed field recomputation.
        // old_sort_values tracks removed values, sort_values tracks new (set) values.
        let mut has_any_ops = false;
        let mut sort_values: HashMap<&str, u32> = HashMap::new();
        let mut old_sort_values: HashMap<&str, u32> = HashMap::new();
        for op in &entry.ops {
            match op {
                Op::Set { field, value } => {
                    process_set_op(sink, meta, slot, field, value);
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_set(slot, field, value);
                    }
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
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_remove(slot, field, value);
                    }
                    // [2.3] Track old sort values for computed field recomputation
                    if meta.has_computed_deps(field) || meta.sort_fields.contains_key(field.as_str()) {
                        let qval = json_to_qvalue(value);
                        if let Some(sv) = value_to_sort_u32(&qval) {
                            old_sort_values.insert(field.as_str(), sv);
                        }
                    }
                    has_any_ops = true;
                }
                Op::Add { field, value } => {
                    process_add_op(sink, meta, slot, field, value);
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_add(slot, field, value);
                    }
                    has_any_ops = true;
                }
                Op::Delete | Op::QueryOpSet { .. } | Op::Alive => {
                    // Already handled above (Delete/QueryOpSet) or signal-only (Alive)
                }
            }
        }
        // [2.3] Recompute computed sort fields when source fields change.
        // First clear old computed value bits, then set new ones.
        // PG triggers emit remove+set pairs, so we have both old and new values.
        if !meta.computed_deps.is_empty() {
            // Diagnostic: log computed_deps trigger for first batch entry with sort changes
            if !sort_values.is_empty() || !old_sort_values.is_empty() {
                eprintln!(
                    "  computed_deps: slot={} sort_vals={:?} old_sort_vals={:?} deps_keys={:?}",
                    slot, sort_values.keys().collect::<Vec<_>>(),
                    old_sort_values.keys().collect::<Vec<_>>(),
                    meta.computed_deps.keys().collect::<Vec<_>>(),
                );
            }
            // Determine which source fields changed (have either old or new value)
            let mut changed_sources: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for k in sort_values.keys() {
                if meta.computed_deps.contains_key(*k) {
                    changed_sources.insert(k);
                }
            }
            for k in old_sort_values.keys() {
                if meta.computed_deps.contains_key(*k) {
                    changed_sources.insert(k);
                }
            }
            // Read stored doc to get current values for source fields NOT in this ops batch.
            // Without this, missing sources default to 0, breaking GREATEST/LEAST.
            let stored_sort_values: HashMap<&str, u32> = if let Some(eng) = engine {
                let mut stored = HashMap::new();
                if let Ok(Some(doc)) = eng.get_document(slot) {
                    for source_field in changed_sources.iter().flat_map(|sf| {
                        meta.computed_deps.get(*sf).into_iter().flat_map(|deps| {
                            deps.iter().flat_map(|d| d.source_fields.iter().map(|s| s.as_str()))
                        })
                    }) {
                        if !sort_values.contains_key(source_field) && !old_sort_values.contains_key(source_field) {
                            if let Some(fv) = doc.fields.get(source_field) {
                                if let crate::mutation::FieldValue::Single(ref v) = fv {
                                    if let Some(sv) = value_to_sort_u32(v) {
                                        stored.insert(source_field, sv);
                                    }
                                }
                            }
                        }
                    }
                }
                stored
            } else {
                HashMap::new()
            };
            for source_field in &changed_sources {
                if let Some(deps) = meta.computed_deps.get(*source_field) {
                    for dep in deps {
                        // Clear old computed value (using old source values, falling back to stored).
                        // Do NOT fall through to sort_values — it has the NEW values, which would
                        // make old_computed = new_computed and corrupt the bitmap clear.
                        let old_values: Vec<u32> = dep.source_fields.iter()
                            .map(|sf| old_sort_values.get(sf.as_str())
                                .or_else(|| stored_sort_values.get(sf.as_str()))
                                .copied()
                                .unwrap_or(0))
                            .collect();
                        let old_computed = match dep.op {
                            crate::config::ComputedOp::Greatest => *old_values.iter().max().unwrap_or(&0),
                            crate::config::ComputedOp::Least => *old_values.iter().min().unwrap_or(&0),
                        };
                        for bit in 0..dep.target_bits {
                            if (old_computed >> bit) & 1 == 1 {
                                sink.sort_clear(dep.target_arc.clone(), bit, slot);
                            }
                        }
                        // Set new computed value (using new source values, falling back to stored).
                        // Do NOT fall through to old_sort_values — for unchanged fields, the
                        // stored value IS the current (and thus new) value.
                        let new_values: Vec<u32> = dep.source_fields.iter()
                            .map(|sf| sort_values.get(sf.as_str())
                                .or_else(|| stored_sort_values.get(sf.as_str()))
                                .copied()
                                .unwrap_or(0))
                            .collect();
                        let new_computed = match dep.op {
                            crate::config::ComputedOp::Greatest => *new_values.iter().max().unwrap_or(&0),
                            crate::config::ComputedOp::Least => *new_values.iter().min().unwrap_or(&0),
                        };
                        eprintln!(
                            "  computed sort recomp: target={} slot={} old_vals={:?}→{} new_vals={:?}→{} stored={:?}",
                            dep.target, slot, old_values, old_computed, new_values, new_computed,
                            stored_sort_values.keys().collect::<Vec<_>>(),
                        );
                        for bit in 0..dep.target_bits {
                            if (new_computed >> bit) & 1 == 1 {
                                sink.sort_set(dep.target_arc.clone(), bit, slot);
                            }
                        }
                        // Write computed sort value to docstore so future reads
                        // (and GET /documents) reflect the recomputed value.
                        if let Some(ref mut dw) = doc_writer {
                            dw.write_set(slot, &dep.target, &serde_json::json!(new_computed));
                        }
                    }
                }
            }
        }
        // Set alive if creates_slot is true and not deferred (deferred handled above).
        if creates_slot {
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
    // Flush docstore writes
    if let Some(dw) = doc_writer {
        dw.flush();
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
    let is_null_nullable = value.is_null() && meta.nullable_fields.contains(field);
    let qval = json_to_qvalue(value);
    // Check if this is a filter field — skip insert for nullable null values
    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if !is_null_nullable {
            if let Some(key) = value_to_bitmap_key(&qval) {
                sink.filter_insert(arc_name.clone(), key, slot);
            }
        }
    }
    // Check if this is a sort field.
    // Clear ALL bits first, then set new ones. Without clearing, a value decrease
    // (e.g. reactionCount 100→50) would leave stale high bits from the old value.
    // This is essential for the CH metrics poller which sends Set-only ops (no Remove).
    if let Some((arc_name, num_bits)) = meta.sort_fields.get(field) {
        if let Some(sort_val) = value_to_sort_u32(&qval) {
            for bit in 0..*num_bits {
                if (sort_val >> bit) & 1 == 1 {
                    sink.sort_set(arc_name.clone(), bit, slot);
                } else {
                    sink.sort_clear(arc_name.clone(), bit, slot);
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
    let is_null_nullable = value.is_null() && meta.nullable_fields.contains(field);
    let qval = json_to_qvalue(value);
    // Check if this is a filter field — skip remove for nullable null values
    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if !is_null_nullable {
            if let Some(key) = value_to_bitmap_key(&qval) {
                sink.filter_remove(arc_name.clone(), key, slot);
            }
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
    // Nullable fields: null value = no-op
    if value.is_null() && meta.nullable_fields.contains(field) {
        return;
    }
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
                Op::Alive => {} // Signal-only, handled at EntityOps level
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
    doc_writer: Option<&mut DocWriter>,
) -> (usize, usize, usize) {
    let mut sink = crate::ingester::AccumSink::new(accum);
    apply_ops_batch(&mut sink, meta, batch, None, doc_writer)
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
    let mut reader = WalReader::from_legacy(wal_path, 0);
    let mut total_applied = 0u64;
    let mut total_errors = 0u64;
    // Create DocWriter so computed sort fields (sortAt = GREATEST) are written
    // to docstore during dump. Without this, only bitmaps get the computed value.
    let mut doc_writer = DocWriter::new(engine.docstore_arc());
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
        let (applied, _skipped, errors) = apply_ops_batch_dump(&mut accum, &meta, &mut entries, Some(&mut doc_writer));
        total_applied += applied as u64;
        total_errors += errors as u64;
    }
    // Flush any pending docstore writes
    doc_writer.flush();
    // Apply accumulated bitmaps to engine staging
    engine.apply_accum(&accum);
    (total_applied, total_errors, start.elapsed().as_secs_f64())
}
// V1 dump functions removed: apply_accum_to_staging, process_multi_value_csv,
// process_csv_dump_direct. Use V2 ops pipeline (ops_poller + /ops endpoint) instead.
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
    use crate::config::{Config, DataSchema, FieldMapping, FieldValueType, FilterFieldConfig, SortFieldConfig};
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
        deferred_alive: Vec<(u32, u64)>,
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
                deferred_alive: Vec::new(),
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
        fn deferred_alive(&mut self, slot: u32, activate_at: u64) {
            self.deferred_alive.push((slot, activate_at));
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
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "type".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "tagIds".into(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "hasMeta".into(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
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
        let (applied, skipped, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
        let (_, skipped, _) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
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
    #[test]
    fn test_deferred_alive_future_publishedat() {
        use crate::config::DeferredAliveConfig;
        let mut config = test_config();
        config.deferred_alive = Some(DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
        });
        // Add publishedAt as a sort field so it appears in meta
        config.sort_fields.push(SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Future timestamp (year 2050)
        let future_ts = 2524608000i64;
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "nsfwLevel".into(), value: json!(16) },
                Op::Set { field: "publishedAt".into(), value: json!(future_ts) },
            ],
        }];
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);
        // Should NOT have alive_insert (deferred instead)
        assert!(sink.alive_inserts.is_empty(), "future publishedAt should NOT set alive");
        assert_eq!(sink.deferred_alive.len(), 1);
        assert_eq!(sink.deferred_alive[0], (42, future_ts as u64));
        // [2.4] ALL bitmaps should be skipped for deferred alive —
        // filter/sort bitmaps are NOT set. Only docstore gets written.
        // activate_due() rebuilds bitmaps from stored doc when the time comes.
        assert!(sink.filter_inserts.is_empty(), "deferred should skip ALL bitmaps including filter");
        assert!(sink.sort_sets.is_empty(), "deferred should skip ALL bitmaps including sort");
    }
    #[test]
    fn test_deferred_alive_past_publishedat() {
        use crate::config::DeferredAliveConfig;
        let mut config = test_config();
        config.deferred_alive = Some(DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Past timestamp (year 2024)
        let past_ts = 1704067200i64;
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "nsfwLevel".into(), value: json!(16) },
                Op::Set { field: "publishedAt".into(), value: json!(past_ts) },
            ],
        }];
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);
        // Past timestamp should set alive immediately
        assert_eq!(sink.alive_inserts, vec![42]);
        assert!(sink.deferred_alive.is_empty(), "past publishedAt should NOT defer alive");
    }
    // -----------------------------------------------------------------------
    // DocWriter tests
    // -----------------------------------------------------------------------
    #[test]
    fn test_doc_writer_write_set() {
        use crate::shard_store_doc::PackedValue;
        use crate::shard_store_doc::DocStoreV3;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStoreV3::open(&docs_dir).unwrap();
        store.ensure_field_index("nsfwLevel").unwrap();
        store.ensure_field_index("userId").unwrap();
        let store = Arc::new(parking_lot::Mutex::new(store));
        let mut dw = DocWriter::new(Arc::clone(&store));
        dw.write_set(10, "nsfwLevel", &json!(16));
        dw.write_set(10, "userId", &json!(42));
        dw.flush();

        let doc = store.lock().get(10).unwrap().unwrap();
        match &doc.fields["nsfwLevel"] {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(16)) => {}
            other => panic!("expected nsfwLevel=16, got: {:?}", other),
        }
        match &doc.fields["userId"] {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(42)) => {}
            other => panic!("expected userId=42, got: {:?}", other),
        }
    }
    #[test]
    fn test_doc_writer_write_add_remove() {
        use crate::shard_store_doc::PackedValue;
        use crate::shard_store_doc::DocStoreV3;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStoreV3::open(&docs_dir).unwrap();
        store.ensure_field_index("tagIds").unwrap();
        let store = Arc::new(parking_lot::Mutex::new(store));
        // First write an initial value
        {
            let _dw = DocWriter::new(Arc::clone(&store));
            let initial = rmp_serde::to_vec(&PackedValue::Mi(vec![100, 200])).unwrap();
            let idx = store.lock().field_index("tagIds").unwrap();
            store.lock().append_tuple(5, idx, &initial).unwrap();
        }
        // Add a value
        {
            let mut dw = DocWriter::new(Arc::clone(&store));
            dw.write_add(5, "tagIds", &json!(300));
            dw.flush();
        }

        let doc = store.lock().get(5).unwrap().unwrap();
        match &doc.fields["tagIds"] {
            crate::mutation::FieldValue::Multi(vals) => {
                let ints: Vec<i64> = vals.iter().filter_map(|v| {
                    if let crate::query::Value::Integer(i) = v { Some(*i) } else { None }
                }).collect();
                assert!(ints.contains(&100));
                assert!(ints.contains(&200));
                assert!(ints.contains(&300));
            }
            other => panic!("expected multi-value tagIds, got: {:?}", other),
        }
        // Remove a value
        {
            let mut dw = DocWriter::new(Arc::clone(&store));
            dw.write_remove(5, "tagIds", &json!(200));
            dw.flush();
        }

        let doc = store.lock().get(5).unwrap().unwrap();
        match &doc.fields["tagIds"] {
            crate::mutation::FieldValue::Multi(vals) => {
                let ints: Vec<i64> = vals.iter().filter_map(|v| {
                    if let crate::query::Value::Integer(i) = v { Some(*i) } else { None }
                }).collect();
                assert!(ints.contains(&100));
                assert!(!ints.contains(&200), "200 should have been removed");
                assert!(ints.contains(&300));
            }
            other => panic!("expected multi-value tagIds, got: {:?}", other),
        }
    }

    /// E2E: DocWriter writes scalar fields through DocStoreV3 and reads them back.
    /// Validates the production ops pipeline docstore write path.
    #[test]
    fn test_docstore_v3_doc_writer_e2e_roundtrip() {
        use crate::shard_store_doc::DocStoreV3;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStoreV3::open(&docs_dir).unwrap();
        store.ensure_field_index("sortAt").unwrap();
        store.ensure_field_index("nsfwLevel").unwrap();

        let store = Arc::new(parking_lot::Mutex::new(store));
        let mut dw = DocWriter::new(Arc::clone(&store));

        // Write scalar fields via DocWriter (simulates WAL ops processor path)
        dw.write_set(100, "sortAt", &json!(1711900000));
        dw.write_set(100, "nsfwLevel", &json!(5));
        dw.flush();

        // Read back via DocStoreV3 and verify
        let doc = store.lock().get(100).unwrap();
        assert!(doc.is_some(), "doc should exist after DocWriter writes");
        let doc = doc.unwrap();
        match &doc.fields["sortAt"] {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(v)) => {
                assert_eq!(*v, 1711900000, "sortAt value should roundtrip");
            }
            other => panic!("expected sortAt=1711900000, got: {:?}", other),
        }
        match &doc.fields["nsfwLevel"] {
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(v)) => {
                assert_eq!(*v, 5, "nsfwLevel value should roundtrip");
            }
            other => panic!("expected nsfwLevel=5, got: {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Non-alive slot filtering tests (2.10)
    // -----------------------------------------------------------------------
    #[test]
    fn test_non_alive_slot_ops_dropped() {
        // Without an engine, non-alive check is skipped (dump mode).
        // This test verifies that creates_slot=false entries are processed
        // when no engine is provided (dump mode behavior).
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 99,
            creates_slot: false,
            ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(8) }],
        }];
        let (applied, skipped, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        // Without engine, non-alive check is bypassed — ops are applied
        assert_eq!(applied, 1);
        assert_eq!(skipped, 0);
        assert_eq!(errors, 0);
        assert!(!sink.filter_inserts.is_empty());
    }
    #[test]
    fn test_creates_slot_bypasses_alive_check() {
        // creates_slot=true should always process, even without alive status
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 55,
            creates_slot: true,
            ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(4) }],
        }];
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);
        assert_eq!(sink.alive_inserts, vec![55]);
        assert!(!sink.filter_inserts.is_empty());
    }
    // -----------------------------------------------------------------------
    // Computed sort old-value clearing tests (2.3)
    // -----------------------------------------------------------------------
    #[test]
    fn test_computed_sort_remove_set_clears_old_bits() {
        use crate::config::{ComputedField, ComputedOp};
        let mut config = test_config();
        // Add existedAt and publishedAt as sort fields
        config.sort_fields.push(SortFieldConfig {
            name: "existedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        // Add sortAt as computed = GREATEST(existedAt, publishedAt)
        config.sort_fields.push(SortFieldConfig {
            name: "sortAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: Some(ComputedField {
                op: ComputedOp::Greatest,
                source_fields: vec!["existedAt".into(), "publishedAt".into()],
            }),
        });
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Simulate a publishedAt change: remove old (1000), set new (2000)
        // existedAt stays at 500 (only in set, no remove)
        let mut batch = vec![EntityOps {
            entity_id: 10,
            creates_slot: false,
            ops: vec![
                Op::Remove { field: "publishedAt".into(), value: json!(1000) },
                Op::Set { field: "publishedAt".into(), value: json!(2000) },
                Op::Set { field: "existedAt".into(), value: json!(500) },
            ],
        }];
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);
        // sortAt should have sort_clears (old computed = max(500,1000) = 1000)
        // and sort_sets (new computed = max(500,2000) = 2000)
        let sort_at_clears: Vec<_> = sink.sort_clears.iter()
            .filter(|(f, _, _)| f == "sortAt")
            .collect();
        let sort_at_sets: Vec<_> = sink.sort_sets.iter()
            .filter(|(f, _, _)| f == "sortAt")
            .collect();
        assert!(!sort_at_clears.is_empty(), "should clear old sortAt bits");
        assert!(!sort_at_sets.is_empty(), "should set new sortAt bits");
        // Verify old value 1000 bits were cleared
        let old_val: u32 = 1000;
        for bit in 0..32 {
            if (old_val >> bit) & 1 == 1 {
                assert!(
                    sort_at_clears.iter().any(|(_, b, s)| *b == bit && *s == 10),
                    "should clear bit {bit} of old sortAt value {old_val}"
                );
            }
        }
        // Verify new value 2000 bits were set
        let new_val: u32 = 2000;
        for bit in 0..32 {
            if (new_val >> bit) & 1 == 1 {
                assert!(
                    sort_at_sets.iter().any(|(_, b, s)| *b == bit && *s == 10),
                    "should set bit {bit} of new sortAt value {new_val}"
                );
            }
        }
    }
    #[test]
    fn test_computed_deps_from_real_config() {
        // Load the actual production config (IndexDefinition wrapper) and verify computed_deps
        let config_json = std::fs::read_to_string("deploy/configs/civitai-index.json")
            .expect("civitai-index.json should exist");
        let idx_def: serde_json::Value = serde_json::from_str(&config_json)
            .expect("should parse JSON");
        let config: crate::config::Config = serde_json::from_value(idx_def["config"].clone())
            .expect("should parse config section");
        let meta = FieldMeta::from_config(&config);
        // Verify computed_deps has entries for both source fields of sortAt
        assert!(
            meta.computed_deps.contains_key("publishedAt"),
            "computed_deps should have 'publishedAt' as source for sortAt. \
             Keys: {:?}", meta.computed_deps.keys().collect::<Vec<_>>()
        );
        assert!(
            meta.computed_deps.contains_key("existedAt"),
            "computed_deps should have 'existedAt' as source for sortAt. \
             Keys: {:?}", meta.computed_deps.keys().collect::<Vec<_>>()
        );
        // Verify the dep targets sortAt
        let deps = &meta.computed_deps["publishedAt"];
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].target, "sortAt");
        assert_eq!(deps[0].source_fields, vec!["existedAt", "publishedAt"]);
        // Test that a publishedAt-only ops batch triggers sortAt recomputation
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 100,
            creates_slot: false,
            ops: vec![
                Op::Set { field: "publishedAt".into(), value: json!(1700000000) },
            ],
        }];
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);
        // sortAt should have sort_sets for the new computed value
        // Without engine (stored doc fallback), existedAt defaults to 0
        // So sortAt = GREATEST(0, 1700000000) = 1700000000
        let sort_at_sets: Vec<_> = sink.sort_sets.iter()
            .filter(|(f, _, _)| f == "sortAt")
            .collect();
        assert!(!sort_at_sets.is_empty(),
            "publishedAt-only op should trigger sortAt recomputation. \
             sort_sets: {:?}", sink.sort_sets);
    }
    // -----------------------------------------------------------------------
    // json_to_packed tests
    // -----------------------------------------------------------------------
    #[test]
    fn test_json_to_packed_types() {
        use crate::shard_store_doc::PackedValue;

        assert_eq!(json_to_packed(&json!(42)), Some(PackedValue::I(42)));
        assert_eq!(json_to_packed(&json!(3.14)), Some(PackedValue::F(3.14)));
        assert_eq!(json_to_packed(&json!(true)), Some(PackedValue::B(true)));
        assert_eq!(json_to_packed(&json!("hello")), Some(PackedValue::S("hello".into())));
        assert_eq!(json_to_packed(&json!(null)), None);
        assert_eq!(json_to_packed(&json!([1, 2, 3])), Some(PackedValue::Mi(vec![1, 2, 3])));
    }
    // -----------------------------------------------------------------------
    // document_to_ops tests (2.7)
    // -----------------------------------------------------------------------
    #[test]
    fn test_document_to_ops_fresh_insert() {
        use crate::mutation::{Document, FieldValue};
        use crate::query::Value as QValue;
        let config = test_config();
        let mut fields = std::collections::HashMap::new();
        fields.insert("nsfwLevel".into(), FieldValue::Single(QValue::Integer(16)));
        let doc = Document { fields };
        let ops = document_to_ops(&doc, None, &config, false);
        // Should have a Set op for nsfwLevel
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            Op::Set { field, value } => {
                assert_eq!(field, "nsfwLevel");
                assert_eq!(value, &json!(16));
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }
    #[test]
    fn test_document_to_ops_upsert_changed_field() {
        use crate::mutation::{Document, FieldValue};
        use crate::query::Value as QValue;
        let config = test_config();
        // Old doc: nsfwLevel=8
        let mut old_fields = std::collections::HashMap::new();
        old_fields.insert("nsfwLevel".into(), FieldValue::Single(QValue::Integer(8)));
        let old_doc = crate::shard_store_doc::StoredDoc { fields: old_fields, schema_version: 0 };

        // New doc: nsfwLevel=16
        let mut new_fields = std::collections::HashMap::new();
        new_fields.insert("nsfwLevel".into(), FieldValue::Single(QValue::Integer(16)));
        let new_doc = Document { fields: new_fields };
        let ops = document_to_ops(&new_doc, Some(&old_doc), &config, false);
        // Should have Remove(old=8) + Set(new=16)
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().any(|op| matches!(op, Op::Remove { field, value } if field == "nsfwLevel" && value == &json!(8))));
        assert!(ops.iter().any(|op| matches!(op, Op::Set { field, value } if field == "nsfwLevel" && value == &json!(16))));
    }
    #[test]
    fn test_document_to_ops_unchanged_field_skipped() {
        use crate::mutation::{Document, FieldValue};
        use crate::query::Value as QValue;
        let config = test_config();
        let mut fields = std::collections::HashMap::new();
        fields.insert("nsfwLevel".into(), FieldValue::Single(QValue::Integer(8)));

        let old_doc = crate::shard_store_doc::StoredDoc { fields: fields.clone(), schema_version: 0 };
        let new_doc = Document { fields };
        let ops = document_to_ops(&new_doc, Some(&old_doc), &config, false);
        assert!(ops.is_empty(), "unchanged fields should produce no ops");
    }
    #[test]
    fn test_document_to_ops_patch_preserves_absent_fields() {
        use crate::mutation::{Document, FieldValue};
        use crate::query::Value as QValue;
        let config = test_config();
        // Old doc has nsfwLevel=8 AND reactionCount sort field
        let mut old_fields = std::collections::HashMap::new();
        old_fields.insert("nsfwLevel".into(), FieldValue::Single(QValue::Integer(8)));
        let old_doc = crate::shard_store_doc::StoredDoc { fields: old_fields, schema_version: 0 };

        // PATCH only sends userId=42 (nsfwLevel absent from patch)
        let mut new_fields = std::collections::HashMap::new();
        new_fields.insert("userId".into(), FieldValue::Single(QValue::Integer(42)));
        let new_doc = Document { fields: new_fields };
        // is_patch=true: absent fields should NOT generate Remove ops
        let ops = document_to_ops(&new_doc, Some(&old_doc), &config, true);
        let has_remove_nsfw = ops.iter().any(|op| matches!(op, Op::Remove { field, .. } if field == "nsfwLevel"));
        assert!(!has_remove_nsfw, "PATCH should NOT remove absent fields (nsfwLevel)");
        // Should have Set for userId (new field)
        let has_set_user = ops.iter().any(|op| matches!(op, Op::Set { field, .. } if field == "userId"));
        assert!(has_set_user, "PATCH should set provided fields (userId)");
        // is_patch=false (PUT): absent fields SHOULD generate Remove ops
        let ops_put = document_to_ops(&new_doc, Some(&old_doc), &config, false);
        let has_remove_nsfw_put = ops_put.iter().any(|op| matches!(op, Op::Remove { field, .. } if field == "nsfwLevel"));
        assert!(has_remove_nsfw_put, "PUT should remove absent fields (nsfwLevel)");
    }
    fn test_config_with_nullable() -> Config {
        let mut config = test_config();
        config.filter_fields.push(FilterFieldConfig {
            name: "blockedFor".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        // Mark blockedFor as nullable via data_schema FieldMapping so that
        // null Set/Remove ops are no-ops rather than mapping to zero.
        config.data_schema = DataSchema {
            id_field: String::new(),
            schema_version: 1,
            fields: vec![FieldMapping {
                source: "blockedFor".into(),
                target: "blockedFor".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: true,
            }],
        };
        config
    }
    #[test]
    fn test_nullable_field_null_set_is_noop() {
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Set blockedFor to null — should be a no-op (no bitmap insert)
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "blockedFor".into(),
                value: json!(null),
            }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert!(
            sink.filter_inserts.iter().all(|(f, _, _)| f != "blockedFor"),
            "null set on nullable field should not insert any bitmap bit"
        );
    }
    #[test]
    fn test_nullable_field_non_null_set_works() {
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Set blockedFor to a real value — should insert bitmap bit
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "blockedFor".into(),
                value: json!("TOS violation"),
            }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert!(
            sink.filter_inserts.iter().any(|(f, _, _)| f == "blockedFor"),
            "non-null set on nullable field should insert bitmap bit"
        );
    }
    #[test]
    fn test_nullable_field_null_remove_is_noop() {
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Remove blockedFor with null value — should be a no-op
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![Op::Remove {
                field: "blockedFor".into(),
                value: json!(null),
            }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert!(
            sink.filter_removes.iter().all(|(f, _, _)| f != "blockedFor"),
            "null remove on nullable field should not remove any bitmap bit"
        );
    }
    #[test]
    fn test_non_nullable_field_null_maps_to_zero() {
        // nsfwLevel is NOT nullable — null should map to 0
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "nsfwLevel".into(),
                value: json!(null),
            }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert!(
            sink.filter_inserts.iter().any(|(f, v, _)| f == "nsfwLevel" && *v == 0),
            "null on non-nullable field should map to 0"
        );
    }
    #[test]
    fn test_nullable_transition_old_to_null() {
        // Simulate blockedFor changing from "copyright" to null:
        // Remove old value, then Set null — old bitmap removed, no new bitmap created
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![
                Op::Remove {
                    field: "blockedFor".into(),
                    value: json!("copyright"),
                },
                Op::Set {
                    field: "blockedFor".into(),
                    value: json!(null),
                },
            ],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        // Old value should be removed
        assert!(
            sink.filter_removes.iter().any(|(f, _, _)| f == "blockedFor"),
            "old blockedFor value should be removed from bitmap"
        );
        // No new bitmap entry should be created
        assert!(
            sink.filter_inserts.iter().all(|(f, _, _)| f != "blockedFor"),
            "null set on nullable field should not create new bitmap entry"
        );
    }
    #[test]
    fn test_nullable_add_null_is_noop() {
        // Add op with null value on nullable field should be a no-op
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![Op::Add {
                field: "blockedFor".into(),
                value: json!(null),
            }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert!(
            sink.filter_inserts.iter().all(|(f, _, _)| f != "blockedFor"),
            "null add on nullable field should not insert any bitmap bit"
        );
    }
}
