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
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use serde_json::Value as JsonValue;
use crate::concurrent_engine::ConcurrentEngine;
use crate::config::Config;
use crate::dictionary::FieldDictionary;
use crate::shard_store_doc::PackedValue;
use crate::shard_store_doc::DocStoreV3;
use crate::filter::{FilterFieldType, NULL_BITMAP_KEY};
use crate::ingester::BitmapSink;

/// queryOpSet fan-out cap (issue #60).
///
/// Default `usize::MAX` (effectively no cap) — ships first so the new
/// `bitdex_query_op_set_fanout_size` histogram gathers prod data. Once we have
/// a fan-out distribution from real traffic, set a finite cap via the
/// `BITDEX_QUERY_OP_SET_MAX_FANOUT` env var. Reads on every apply (microsecond
/// cost dwarfed by `execute_query`'s ms-scale work) so operators can tune
/// without restart by editing the manifest and rolling pods.
const DEFAULT_MAX_FANOUT: usize = usize::MAX;
fn max_fanout() -> usize {
    std::env::var("BITDEX_QUERY_OP_SET_MAX_FANOUT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_FANOUT)
}
use crate::mutation::{value_to_bitmap_key, value_to_sort_u32, FieldRegistry};
use crate::pg_sync::op_dedup::dedup_ops;
use crate::pg_sync::ops::{EntityOps, Op};
use crate::query::{BitdexQuery, FilterClause, Value as QValue};

/// Resolve a Value to a u64 filter bitmap key, consulting the per-field
/// `FieldDictionary` for `LowCardinalityString` values when the direct
/// integer/bool conversion fails.
///
/// `for_set` controls dictionary write behavior:
/// - `true` (set/add path): unknown strings are auto-assigned a new key via
///   `get_or_insert`. Required so newly-seen LCS values written through the
///   steady-state ops path get queryable bitmap entries.
/// - `false` (remove path): unknown strings return `None` so the clear is a
///   no-op. Removing a string that was never inserted is harmless.
fn resolve_filter_key(
    qval: &QValue,
    field: &str,
    dictionaries: Option<&HashMap<String, FieldDictionary>>,
    for_set: bool,
) -> Option<u64> {
    if let Some(key) = value_to_bitmap_key(qval) {
        return Some(key);
    }
    if let QValue::String(s) = qval {
        if let Some(dicts) = dictionaries {
            if let Some(dict) = dicts.get(field) {
                if for_set {
                    return Some(dict.get_or_insert(s) as u64);
                }
                return dict.get(s).map(|v| v as u64);
            }
        }
    }
    None
}
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
    docstore: Arc<parking_lot::RwLock<DocStoreV3>>,
    field_dict: HashMap<String, u16>,
    pending: Vec<(u32, u16, Vec<u8>)>,
    pending_append: Vec<(u32, u16, PackedValue)>,
    pending_remove: Vec<(u32, u16, PackedValue)>,
}
impl DocWriter {
    /// Create a DocWriter from the engine's docstore.
    pub fn new(docstore: Arc<parking_lot::RwLock<DocStoreV3>>) -> Self {
        let field_dict = docstore.read().field_dict_snapshot();
        Self {
            docstore,
            field_dict,
            pending: Vec::new(),
            pending_append: Vec::new(),
            pending_remove: Vec::new(),
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
    /// Write a multi-value add by emitting DocOp::Append. The apply path
    /// unions with existing Mi values and dedups, so no read-modify-write
    /// is needed — eliminates the batch race where two adds for the same
    /// slot in one batch would each emit a Set and clobber each other.
    fn write_add(&mut self, slot: u32, field: &str, value: &JsonValue) {
        let idx = match self.resolve_field(field) {
            Some(idx) => idx,
            None => return,
        };
        let add_val = match value.as_i64() {
            Some(v) => v,
            None => return,
        };
        self.pending_append.push((slot, idx, PackedValue::I(add_val)));
    }
    /// Write a multi-value remove by emitting DocOp::Remove.
    fn write_remove(&mut self, slot: u32, field: &str, value: &JsonValue) {
        let idx = match self.resolve_field(field) {
            Some(idx) => idx,
            None => return,
        };
        let remove_val = match value.as_i64() {
            Some(v) => v,
            None => return,
        };
        self.pending_remove.push((slot, idx, PackedValue::I(remove_val)));
    }
    /// Flush all pending ops to the docstore in a single pass.
    /// Groups Set + Append + Remove ops by shard, writing once per shard
    /// instead of twice (halves file-open count for multi-field entity updates).
    pub fn flush(&mut self) {
        let has_sets = !self.pending.is_empty();
        let has_multi = !self.pending_append.is_empty() || !self.pending_remove.is_empty();
        if !has_sets && !has_multi {
            return;
        }
        let sets = std::mem::take(&mut self.pending);
        let appends = std::mem::take(&mut self.pending_append);
        let removes = std::mem::take(&mut self.pending_remove);
        if let Err(e) = self.docstore.write().append_mixed_batch(sets, appends, removes) {
            tracing::warn!("DocWriter flush failed: {e}");
        }
    }
    fn resolve_field(&mut self, field: &str) -> Option<u16> {
        if let Some(&idx) = self.field_dict.get(field) {
            return Some(idx);
        }
        // Field not in snapshot — try to ensure it exists
        match self.docstore.write().ensure_field_index(field) {
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
    let empty_fields = HashMap::new();
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
    nullable_fields: HashSet<String>,
    /// Sort field name → (Arc<str>, num_bits)
    sort_fields: HashMap<String, (Arc<str>, usize)>,
    /// Reverse map: source_field → computed sort fields that depend on it.
    /// When a source field is set, all computed fields referencing it must be recomputed.
    computed_deps: HashMap<String, Vec<ComputedSortInfo>>,
    /// `data_schema`-driven shadow updates for `exists_boolean` filter targets.
    ///
    /// Key: a field name that the steady-state trigger may emit ops for. Value:
    /// the `exists_boolean` filter targets that share the same `data_schema`
    /// source as that field, paired with the Arc'd field name used by the sink.
    ///
    /// Example: data_schema declares `publishedAtUnix → publishedAt` (sort) and
    /// `publishedAtUnix → isPublished` (exists_boolean filter). The Post fan-out
    /// trigger emits ops keyed by the sort target name (`publishedAt`). The
    /// shadow map fans `Set publishedAt` out to also write the `isPublished`
    /// bitmap so the trigger config doesn't have to declare every derived
    /// target manually. The source name (`publishedAtUnix`) is also keyed in
    /// case a trigger ever emits source-named ops.
    exists_boolean_shadows: HashMap<String, Vec<Arc<str>>>,
    /// Deferred alive config: if present, the source_field name whose future timestamps
    /// trigger deferred alive instead of immediate alive. ms_to_seconds indicates
    /// whether the field value is in milliseconds (needs /1000 for epoch comparison).
    deferred_alive_field: Option<(String, bool)>,
    /// Document field name to populate with the slot ID on `creates_slot=true`.
    /// The dump path extracts `data_schema.id_field` from source JSON and stores
    /// it as a Document field (`loader.rs:780`); steady-state ops never carry a
    /// column-level Set for the PG primary key (it lives in `entity_id`). Without
    /// a synthetic write here, every steady-state-inserted slot's docstore is
    /// missing `id`. Empty string disables the synthetic write.
    id_field: String,
    /// Field registry for Arc<str> interning (kept for future DocSink use)
    #[allow(dead_code)]
    registry: FieldRegistry,
}
impl FieldMeta {
    /// Build FieldMeta from engine config.
    pub fn from_config(config: &Config) -> Self {
        let registry = FieldRegistry::from_config(config);
        let mut filter_fields = HashMap::new();
        let mut nullable_fields = HashSet::new();
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
        // Build the exists_boolean shadow map. For each `exists_boolean`
        // filter target, the underlying source feeds one or more other
        // targets (typically a sort field or a same-named filter). When the
        // trigger emits ops for any of those siblings, the exists_boolean
        // target needs to flip true/false based on whether the new value is
        // null. Without this, the steady-state path leaves the
        // exists_boolean bitmap stuck at whatever the dump computed.
        let mut exists_boolean_shadows: HashMap<String, Vec<Arc<str>>> = HashMap::new();
        for fm in &config.data_schema.fields {
            if !matches!(fm.value_type, crate::config::FieldValueType::ExistsBoolean) {
                continue;
            }
            // The exists_boolean target must be a registered filter field —
            // otherwise the bitmap update has nowhere to go.
            let (eb_arc, _) = match filter_fields.get(&fm.target) {
                Some(pair) => pair,
                None => continue,
            };
            let eb_arc = eb_arc.clone();
            // Add a shadow trigger keyed by the source name (covers triggers
            // that ever emit source-keyed ops) and by every sibling target
            // that shares the same source (covers the common case where the
            // trigger emits target-keyed ops, e.g. `publishedAt`).
            let mut keys: Vec<String> = vec![fm.source.clone()];
            for sibling in &config.data_schema.fields {
                if sibling.source == fm.source && sibling.target != fm.target {
                    keys.push(sibling.target.clone());
                }
            }
            for k in keys {
                let entry = exists_boolean_shadows.entry(k).or_default();
                if !entry.iter().any(|a| Arc::ptr_eq(a, &eb_arc)) {
                    entry.push(eb_arc.clone());
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
            exists_boolean_shadows,
            deferred_alive_field,
            id_field: config.data_schema.id_field.clone(),
            registry,
        }
    }
    /// Check if a sort field is a source for any computed field.
    fn has_computed_deps(&self, field: &str) -> bool {
        self.computed_deps.contains_key(field)
    }
}

/// Mirror the `process_set_op` / `process_remove_op` exists_boolean shadow
/// (lines ~1115 and ~1167) into the docstore. The bitmap update flips the
/// derived target's bit; this writes the corresponding bool into the
/// document so `GET /documents/{slot}` agrees with the bitmap state.
///
/// `is_null_or_remove` true → field is being cleared (Remove or null Set);
/// the exists_boolean target stores `false`. False → non-null Set; target
/// stores `true`.
///
/// The lookup key is whatever `field` name the trigger emitted. The shadow
/// map is built (`from_config`) so that BOTH the data_schema source name
/// (e.g. `publishedAtUnix`) AND every sibling target sharing that source
/// (e.g. `publishedAt`) resolve to the same `exists_boolean` target arc.
/// Production triggers emit target-keyed payloads (`publishedAt` already
/// in seconds via `extract(epoch from ...)`) — this covers them — and the
/// source-keyed path stays correct for any future trigger that emits raw
/// source values.
///
/// Latent gap (not in production today): a source-keyed op whose target is
/// a numeric sort field with `ms_to_seconds: true` will write the raw
/// source value to the docstore via `dw.write_set` (no derivation). The
/// fix would parallel this helper for sort targets; deferred because the
/// current Civitai trigger config never emits source-keyed sort ops.
fn write_shadow_target_docs(
    dw: &mut DocWriter,
    meta: &FieldMeta,
    slot: u32,
    field: &str,
    is_null_or_remove: bool,
) {
    if let Some(targets) = meta.exists_boolean_shadows.get(field) {
        let bool_val = JsonValue::Bool(!is_null_or_remove);
        for arc_target in targets {
            dw.write_set(slot, arc_target.as_ref(), &bool_val);
        }
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
    // 11c CPU floor attribution (2026-04-30): time the apply path so we can
    // attribute the WAL reader's contribution. Bridge handle is None in tests
    // and dump-only contexts — observation is best-effort. Server-feature
    // gated because metrics_bridge_handle is server-only.
    #[cfg(feature = "server")]
    let _apply_timer = engine.and_then(|e| e.metrics_bridge_handle()).map(|b| {
        b.wal_apply_batch_seconds
            .with_label_values(&[&b.index_name])
            .start_timer()
    });
    dedup_ops(batch);
    // Per-batch diagnostic at trace level (was eprintln, hot-path cost).
    if !batch.is_empty() {
        tracing::trace!(
            "apply_ops_batch: {} entries, computed_deps sources: {}",
            batch.len(),
            meta.computed_deps.len(),
        );
    }
    // LowCardinalityString filter ops carry their value as JSON string
    // (e.g. {"field":"type","value":"image"}). Without dictionary resolution
    // the filter bitmap update silently no-ops because `value_to_bitmap_key`
    // returns `None` for `Value::String`. The engine owns the live per-field
    // FieldDictionary; dump callers pass `None` here and rely on the
    // dump_processor's own dictionary resolution path.
    let dictionaries = engine.map(|e| e.dictionaries());
    let mut applied = 0usize;
    let mut skipped = 0usize;
    let mut errors = 0usize;
    for entry in batch.iter() {
        let entity_id = entry.entity_id;
        if entity_id < 0 || entity_id > u32::MAX as i64 {
            if skipped < 3 {
                eprintln!("ops processor: SKIP entity_id={entity_id} out of u32 range");
            }
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
                        // Diagnostic: log first few skips to help debug WAL reader stall
                        if skipped < 3 {
                            eprintln!(
                                "ops processor: SKIP slot={slot} entity_id={entity_id} !alive slot_counter={} creates_slot={} ops={:?}",
                                eng.slot_counter(), entry.creates_slot,
                                entry.ops.iter().map(|o| match o {
                                    Op::Set { field, .. } => format!("set:{field}"),
                                    Op::Remove { field, .. } => format!("rm:{field}"),
                                    Op::Add { field, .. } => format!("add:{field}"),
                                    Op::Delete => "delete".into(),
                                    Op::Alive => "alive".into(),
                                    Op::QueryOpSet { .. } => "queryOpSet".into(),
                                }).collect::<Vec<_>>()
                            );
                        }
                        skipped += 1;
                        continue;
                    }
                }
            }
        }
        // §3a (bug #16): persist `id == slot` on creates_slot. The dump path
        // does this from source JSON via `loader.rs:780`; ops never carry a
        // column-level Set for the PG primary key, so without this every
        // steady-state-inserted slot is missing `id` in its stored doc.
        // Skip if any op in this batch already sets meta.id_field (defensive —
        // no known trigger does, but preserves the contract).
        if creates_slot && !meta.id_field.is_empty() {
            if let Some(ref mut dw) = doc_writer {
                let id_already_set = entry.ops.iter().any(|op| {
                    matches!(op, Op::Set { field, .. } if field == &meta.id_field)
                });
                if !id_already_set {
                    dw.write_set(slot, &meta.id_field, &serde_json::json!(slot));
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
                let query_str = match query {
                    Some(q) => q,
                    None => {
                        // Null query: trigger couldn't resolve join condition — skip silently
                        skipped += 1;
                        continue;
                    }
                };
                if let Some(eng) = engine {
                    // §3b (bug #16): pass doc_writer so fan-out ops update the
                    // docstore alongside bitmaps. Without this, queryOpSet
                    // (e.g. Post → Image fan-out for publishedAt) updates the
                    // bitmap shadow but leaves the doc at default values.
                    match apply_query_op_set(sink, meta, eng, query_str, ops, doc_writer.as_deref_mut()) {
                        Ok(count) => applied += count,
                        Err(e) => {
                            tracing::warn!("ops processor: queryOpSet '{query_str}' failed: {e}");
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
                    process_set_op(sink, meta, slot, field, value, dictionaries);
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_set(slot, field, value);
                        // §3b (bug #16): mirror the bitmap shadow update at
                        // process_set_op:1115 into the docstore. Without this,
                        // exists_boolean targets (e.g. isPublished) only
                        // appear in the bitmap; reads return the docstore
                        // default and disagree with query results.
                        write_shadow_target_docs(dw, meta, slot, field, value.is_null());
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
                    process_remove_op(sink, meta, slot, field, value, dictionaries);
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_remove(slot, field, value);
                        // Mirror of process_remove_op:1167 shadow → docstore.
                        write_shadow_target_docs(dw, meta, slot, field, true);
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
                    process_add_op(sink, meta, slot, field, value, dictionaries);
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
            // Per-op diagnostic was costing ~100µs/op in stderr allocation + flush.
            // Left as tracing::trace so it's compiled out unless explicitly enabled.
            tracing::trace!(
                "computed_deps: slot={} sort_vals={:?} old_sort_vals={:?} deps_keys={:?}",
                slot, sort_values.keys().collect::<Vec<_>>(),
                old_sort_values.keys().collect::<Vec<_>>(),
                meta.computed_deps.keys().collect::<Vec<_>>(),
            );
            // Determine which source fields changed (have either old or new value)
            let mut changed_sources: HashSet<&str> = HashSet::new();
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
                        // Per-op diagnostic at trace level — was eprintln, ~100µs/op
                        // stderr allocation + flush dominated the inner loop under
                        // computed-sort bursts. Matches the conversion pattern at
                        // :644 and :825 per `docs/_in/core-path-review-2026-04-25.md`
                        // drift-hygiene-audit.
                        tracing::trace!(
                            "computed sort recomp: target={} slot={} old_vals={:?}→{} new_vals={:?}→{} stored={:?}",
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
/// IMPORTANT: null detection happens on raw JsonValue BEFORE json_to_qvalue(),
/// because json_to_qvalue maps null → Integer(0), losing null information.
fn process_set_op<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    slot: u32,
    field: &str,
    value: &JsonValue,
    dictionaries: Option<&HashMap<String, FieldDictionary>>,
) {
    let is_nullable = meta.nullable_fields.contains(field);
    let is_null = value.is_null();
    let qval = json_to_qvalue(value);

    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if is_null && is_nullable {
            // Null value on nullable field: set the null bitmap key
            sink.filter_insert(arc_name.clone(), NULL_BITMAP_KEY, slot);
        } else {
            if is_nullable {
                // Non-null value on nullable field: clear the null bitmap key
                sink.filter_remove(arc_name.clone(), NULL_BITMAP_KEY, slot);
            }
            if let Some(key) = resolve_filter_key(&qval, field, dictionaries, true) {
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
    // Shadow updates for `exists_boolean` filter targets that share a
    // data_schema source with this field. The trigger emits ops keyed by
    // sort/filter target name; the exists_boolean target gets derived here
    // from the value's null-ness so the trigger config doesn't have to
    // declare every derived target manually.
    //
    // Boolean filters use bitmap key 0 (false) and 1 (true). We always clear
    // the opposite bit before inserting the new bit so the bitmap state is
    // consistent on transitions (slot was previously stored under the old
    // boolean, no companion Remove op is emitted for shadow updates).
    if let Some(shadows) = meta.exists_boolean_shadows.get(field) {
        let exists_key: u64 = if is_null { 0 } else { 1 };
        let opposite_key: u64 = 1 - exists_key;
        for arc_name in shadows {
            sink.filter_remove(arc_name.clone(), opposite_key, slot);
            sink.filter_insert(arc_name.clone(), exists_key, slot);
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
    dictionaries: Option<&HashMap<String, FieldDictionary>>,
) {
    let is_null = value.is_null();
    let is_nullable = meta.nullable_fields.contains(field);
    let qval = json_to_qvalue(value);

    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if is_null && is_nullable {
            // Removing a null value: clear the null bitmap key
            sink.filter_remove(arc_name.clone(), NULL_BITMAP_KEY, slot);
        } else {
            // Non-null remove, or null on non-nullable (null→0 via json_to_qvalue).
            // Must clear the 0 bit that was set when the null was originally stored.
            // for_set=false: unknown LCS strings return None so the clear becomes a no-op.
            if let Some(key) = resolve_filter_key(&qval, field, dictionaries, false) {
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
    // Mirror of the `process_set_op` shadow: a Remove op signals that the
    // sibling source no longer holds a value, so any `exists_boolean` filter
    // target derived from that source must flip to false. The Set that
    // typically follows in an UPDATE pair (Remove(old) + Set(new)) overrides
    // this back to true if the new value is non-null. Without this, a bare
    // Remove (or any path that emits Remove without a paired Set) would
    // leave the exists_boolean bitmap stuck at its previous true state.
    if let Some(shadows) = meta.exists_boolean_shadows.get(field) {
        for arc_name in shadows {
            sink.filter_remove(arc_name.clone(), 1, slot);
            sink.filter_insert(arc_name.clone(), 0, slot);
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
    dictionaries: Option<&HashMap<String, FieldDictionary>>,
) {
    // Nullable fields: null value = no-op
    if value.is_null() && meta.nullable_fields.contains(field) {
        return;
    }
    let qval = json_to_qvalue(value);
    if let Some((arc_name, _field_type)) = meta.filter_fields.get(field) {
        if let Some(key) = resolve_filter_key(&qval, field, dictionaries, true) {
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
///
/// `doc_writer` is the same writer used by `apply_ops_batch` direct ops; passing
/// it through keeps fan-out doc writes in sync with bitmap mutations. Without it
/// (the pre-bug-#16 behavior) Post→Image fan-out updates the `publishedAt` /
/// `isPublished` bitmaps for matching slots but leaves the docstore at defaults,
/// so `GET /documents/{slot}` disagrees with query results.
fn apply_query_op_set<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    engine: &ConcurrentEngine,
    query_str: &str,
    ops: &[Op],
    mut doc_writer: Option<&mut DocWriter>,
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
    // Mission #77: bump query_total so the existing query-rate dashboard reflects
    // the WAL-reader's queryOpSet fan-out queries, not just /api/.../query.
    #[cfg(feature = "server")]
    let metrics = engine.metrics_bridge_handle();
    #[cfg(feature = "server")]
    if let Some(ref bridge) = metrics {
        bridge
            .query_total
            .with_label_values(&[&bridge.index_name])
            .inc();
    }

    let result = engine
        .execute_query(&query)
        .map_err(|e| format!("queryOpSet query failed: {e}"))?;
    let slot_ids = &result.ids;

    // Issue #60: observe fan-out size BEFORE the cap check so the histogram captures
    // the would-be-rejected upper tail too. The bridge handle is `None` in tests
    // and dump-only contexts — observation is best-effort.
    #[cfg(feature = "server")]
    if let Some(ref bridge) = metrics {
        bridge
            .query_op_set_fanout_size
            .with_label_values(&[&bridge.index_name])
            .observe(slot_ids.len() as f64);
    }

    let cap = max_fanout();
    if slot_ids.len() > cap {
        tracing::warn!(
            target: "ops_processor",
            "queryOpSet '{}' matches {} slots, exceeds cap {} — op skipped (data drift)",
            query_str,
            slot_ids.len(),
            cap,
        );
        #[cfg(feature = "server")]
        if let Some(ref bridge) = metrics {
            bridge
                .query_op_set_rejected_total
                .with_label_values(&[&bridge.index_name, "fanout_too_wide"])
                .inc();
        }
        return Ok(0);
    }

    if slot_ids.is_empty() {
        return Ok(0);
    }
    let dictionaries = Some(engine.dictionaries());
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
                    process_set_op(sink, meta, slot, field, value, dictionaries);
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_set(slot, field, value);
                        write_shadow_target_docs(dw, meta, slot, field, value.is_null());
                    }
                }
                Op::Remove { field, value } => {
                    process_remove_op(sink, meta, slot, field, value, dictionaries);
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_remove(slot, field, value);
                        write_shadow_target_docs(dw, meta, slot, field, true);
                    }
                }
                Op::Add { field, value } => {
                    process_add_op(sink, meta, slot, field, value, dictionaries);
                    if let Some(ref mut dw) = doc_writer {
                        dw.write_add(slot, field, value);
                    }
                }
                Op::Delete => {
                    // Delete within queryOpSet clears alive for each matched slot.
                    // Docstore cleanup is owned by autovac (no per-slot delete here);
                    // bitmap clean-delete is handled at the EntityOps level.
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

    #[cfg(feature = "server")]
    if let Some(ref bridge) = metrics {
        bridge
            .query_op_set_applied_slots_total
            .with_label_values(&[&bridge.index_name])
            .inc_by(applied as u64);
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
                per_value_lazy: false, max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "type".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false, max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "tagIds".into(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false, max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "hasMeta".into(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false, max_range_scan_values: None,
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
    /// Regression: LCS string values must be dictionary-resolved before
    /// `filter_insert`. Prior to this fix, `Set type="image"` (the shape
    /// emitted by the Image steady-state trigger) silently dropped the
    /// bitmap update because `value_to_bitmap_key(Value::String(_))` returns
    /// `None`. The Image filter bitmap diverged from the docstore, so any
    /// `Eq("type", "image")` query that AND'd with a small candidate set
    /// (e.g. a time bucket) collapsed to zero results.
    #[test]
    fn test_set_op_lcs_string_resolves_via_dictionary() {
        let config = test_config();
        // The production schema registers `type` as `single_value` at the
        // filter level; the LCS-ness lives in `data_schema.value_type` and
        // surfaces at query time via the per-field `FieldDictionary`. The
        // resolver in `process_set_op` consults the dictionary regardless of
        // FilterFieldType, so we can use the existing `type` SingleValue
        // entry from `test_config()` and just inject a dictionary.
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut dictionaries: HashMap<String, FieldDictionary> = HashMap::new();
        dictionaries.insert("type".to_string(), FieldDictionary::new());

        process_set_op(
            &mut sink,
            &meta,
            42,
            "type",
            &json!("image"),
            Some(&dictionaries),
        );

        // First write of "image" auto-assigns key 1 (FieldDictionary starts at 1).
        let dict_key = dictionaries.get("type").unwrap().get("image").unwrap() as u64;
        assert_eq!(dict_key, 1);
        assert_eq!(sink.filter_inserts.len(), 1, "Set type=\"image\" must emit a filter_insert");
        assert_eq!(sink.filter_inserts[0], ("type".to_string(), dict_key, 42));
    }

    /// Build a config that mirrors the production Civitai relationship
    /// between `publishedAtUnix → publishedAt` (sort target) and
    /// `publishedAtUnix → isPublished` (exists_boolean filter target).
    /// Used by the shadow-update tests to verify ops keyed by the sort
    /// target name fan out to the filter target.
    fn shadow_config() -> Config {
        let mut config = Config::default();
        config.filter_fields = vec![FilterFieldConfig {
            name: "isPublished".into(),
            field_type: FilterFieldType::Boolean,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        }];
        config.sort_fields = vec![SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        }];
        config.data_schema.fields = vec![
            FieldMapping {
                source: "publishedAtUnix".into(),
                target: "publishedAt".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: true,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: false,
            },
            FieldMapping {
                source: "publishedAtUnix".into(),
                target: "isPublished".into(),
                value_type: FieldValueType::ExistsBoolean,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: false,
            },
        ];
        config
    }

    /// Regression: an `exists_boolean` filter target must flip true on a
    /// non-null Set op for any sibling target sharing the same data_schema
    /// source. This is the root cause of doc 129087101 reading
    /// `isPublished=false` in prod — the Post fan-out trigger emits ops
    /// keyed by `publishedAt` (sort target) and the data_schema-driven
    /// `isPublished` derivation never fired in steady state.
    #[test]
    fn test_set_op_shadows_exists_boolean_target() {
        let config = shadow_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Trigger emits Set publishedAt=<seconds> when a Post is published.
        process_set_op(&mut sink, &meta, 42, "publishedAt", &json!(1_777_581_167i64), None);

        // Sort field gets the standard bit decomposition (existence-agnostic).
        assert!(!sink.sort_sets.is_empty(), "publishedAt sort bits must still be written");
        // Shadow flips isPublished=true: removes the false bit, inserts true.
        let removes: Vec<&(String, u64, u32)> = sink.filter_removes.iter()
            .filter(|(f, _, _)| f == "isPublished").collect();
        let inserts: Vec<&(String, u64, u32)> = sink.filter_inserts.iter()
            .filter(|(f, _, _)| f == "isPublished").collect();
        assert_eq!(removes, vec![&("isPublished".to_string(), 0u64, 42)],
            "shadow must clear the false bit so prior state doesn't linger");
        assert_eq!(inserts, vec![&("isPublished".to_string(), 1u64, 42)],
            "shadow must insert the true bit on non-null Set");
    }

    /// Regression: a null Set op (Post unpublished) must flip the shadow
    /// `exists_boolean` target to false. Without the shadow, isPublished
    /// stays true even after the publishedAt clears.
    #[test]
    fn test_set_op_shadows_exists_boolean_null_to_false() {
        let config = shadow_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        process_set_op(&mut sink, &meta, 42, "publishedAt", &json!(null), None);

        let removes: Vec<&(String, u64, u32)> = sink.filter_removes.iter()
            .filter(|(f, _, _)| f == "isPublished").collect();
        let inserts: Vec<&(String, u64, u32)> = sink.filter_inserts.iter()
            .filter(|(f, _, _)| f == "isPublished").collect();
        assert_eq!(removes, vec![&("isPublished".to_string(), 1u64, 42)],
            "shadow must clear the true bit on null Set");
        assert_eq!(inserts, vec![&("isPublished".to_string(), 0u64, 42)],
            "shadow must insert the false bit on null Set");
    }

    /// Regression: a Remove op on the sibling source must also flip the
    /// exists_boolean shadow target to false. UPDATE pairs (Remove(old) +
    /// Set(new)) still end up correct because the Set that follows
    /// overrides this when the new value is non-null. The case this guards
    /// is bare Remove without a companion Set — flagged by external review
    /// as a defensive gap in the original Set-only design.
    #[test]
    fn test_remove_op_shadows_exists_boolean_to_false() {
        let config = shadow_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        process_remove_op(&mut sink, &meta, 42, "publishedAt", &json!(1_777_581_167i64), None);

        let removes: Vec<&(String, u64, u32)> = sink.filter_removes.iter()
            .filter(|(f, _, _)| f == "isPublished").collect();
        let inserts: Vec<&(String, u64, u32)> = sink.filter_inserts.iter()
            .filter(|(f, _, _)| f == "isPublished").collect();
        assert_eq!(removes, vec![&("isPublished".to_string(), 1u64, 42)],
            "Remove must clear the true bit on the shadow target");
        assert_eq!(inserts, vec![&("isPublished".to_string(), 0u64, 42)],
            "Remove must insert the false bit on the shadow target");
    }

    /// Source-name and target-name shadow keys both fire. A trigger that
    /// (hypothetically) emits ops keyed by the data_schema source name
    /// (`publishedAtUnix`) must produce the same shadow as one that emits
    /// ops keyed by the sibling target (`publishedAt`).
    #[test]
    fn test_shadow_triggers_on_source_name_too() {
        let config = shadow_config();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        process_set_op(&mut sink, &meta, 42, "publishedAtUnix", &json!(1_777_581_167_000i64), None);

        let inserts: Vec<&(String, u64, u32)> = sink.filter_inserts.iter()
            .filter(|(f, _, _)| f == "isPublished").collect();
        assert_eq!(inserts, vec![&("isPublished".to_string(), 1u64, 42)]);
    }

    /// Regression: the time-bucket leak fix in concurrent_engine reads
    /// `coalescer.mutated_sort_slots()` keyed by the bucket's tracked sort
    /// field name (e.g., `sortAt`). External review questioned whether
    /// computed sort recomputation actually populates that map for the
    /// computed target — the fix only works if it does.
    ///
    /// Verify here by emitting `Set publishedAt=<seconds>` and asserting
    /// the sink sees `sort_set` / `sort_clear` calls keyed by the
    /// COMPUTED target Arc (`sortAt`), not just by `publishedAt`. The
    /// coalescer's `mutated_sort_slots()` aggregates sort_sets+sort_clears
    /// per field, so as long as the recompute path emits sink calls with
    /// the computed-target field name, the time-bucket fix downstream
    /// will see the slot under the right key.
    #[test]
    fn test_computed_sort_recompute_emits_target_field_sort_ops() {
        // sortAt = greatest(existedAt, publishedAt). publishedAt acts as
        // both a tracked sort source AND the trigger-emitted op field;
        // recompute should produce sort_set/sort_clear on `sortAt`.
        let mut config = Config::default();
        config.filter_fields = Vec::new();
        config.sort_fields = vec![
            SortFieldConfig {
                name: "publishedAt".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: false,
                computed: None,
            },
            SortFieldConfig {
                name: "existedAt".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: false,
                computed: None,
            },
            SortFieldConfig {
                name: "sortAt".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: false,
                computed: Some(crate::config::ComputedField {
                    op: crate::config::ComputedOp::Greatest,
                    source_fields: vec!["existedAt".into(), "publishedAt".into()],
                }),
            },
        ];
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![
                Op::Remove { field: "publishedAt".into(), value: json!(1_000_000i64) },
                Op::Set { field: "publishedAt".into(), value: json!(2_000_000i64) },
            ],
        }];
        let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert_eq!(applied, 1);
        assert_eq!(errors, 0);

        // The computed-sort recompute path must emit sink ops keyed by
        // the computed target name `sortAt` — not just by the source
        // `publishedAt`. This is the property the time-bucket leak fix
        // relies on via `coalescer.mutated_sort_slots()`.
        let sort_at_writes: Vec<&(String, usize, u32)> = sink.sort_sets.iter()
            .chain(sink.sort_clears.iter())
            .filter(|(f, _, _)| f == "sortAt").collect();
        assert!(!sort_at_writes.is_empty(),
            "computed-sort recompute must emit sink ops on the `sortAt` target \
             so coalescer.mutated_sort_slots() picks up the slot for time-bucket re-eval");
        // And of course the source field gets its own sort writes.
        let published_at_writes: Vec<&(String, usize, u32)> = sink.sort_sets.iter()
            .chain(sink.sort_clears.iter())
            .filter(|(f, _, _)| f == "publishedAt").collect();
        assert!(!published_at_writes.is_empty(), "source sort field writes must still happen");
    }

    /// Companion to the LCS-set test: removing a previously-inserted LCS
    /// value must resolve through the dictionary too. Unknown strings are
    /// no-ops on the remove path (no key was ever assigned, nothing to clear).
    #[test]
    fn test_remove_op_lcs_string_resolves_via_dictionary() {
        let config = test_config();
        let meta = FieldMeta::from_config(&config);
        let mut dictionaries: HashMap<String, FieldDictionary> = HashMap::new();
        let dict = FieldDictionary::new();
        let image_key = dict.get_or_insert("image") as u64;
        dictionaries.insert("type".to_string(), dict);

        let mut sink = RecordingSink::new();
        process_remove_op(
            &mut sink,
            &meta,
            42,
            "type",
            &json!("image"),
            Some(&dictionaries),
        );
        assert_eq!(sink.filter_removes.len(), 1);
        assert_eq!(sink.filter_removes[0], ("type".to_string(), image_key, 42));

        // Unknown string on remove → no-op (no key was assigned for "video").
        let mut sink2 = RecordingSink::new();
        process_remove_op(
            &mut sink2,
            &meta,
            42,
            "type",
            &json!("video"),
            Some(&dictionaries),
        );
        assert!(sink2.filter_removes.is_empty(), "remove of unseen LCS value must be a no-op");
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
        let store = Arc::new(parking_lot::RwLock::new(store));
        let mut dw = DocWriter::new(Arc::clone(&store));
        dw.write_set(10, "nsfwLevel", &json!(16));
        dw.write_set(10, "userId", &json!(42));
        dw.flush();

        let doc = store.read().get(10).unwrap().unwrap();
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
        let store = Arc::new(parking_lot::RwLock::new(store));
        // First write an initial value
        {
            let _dw = DocWriter::new(Arc::clone(&store));
            let initial = rmp_serde::to_vec(&PackedValue::Mi(vec![100, 200])).unwrap();
            let idx = store.read().field_index("tagIds").unwrap();
            store.write().append_tuple(5, idx, &initial).unwrap();
        }
        // Add a value
        {
            let mut dw = DocWriter::new(Arc::clone(&store));
            dw.write_add(5, "tagIds", &json!(300));
            dw.flush();
        }

        let doc = store.read().get(5).unwrap().unwrap();
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

        let doc = store.read().get(5).unwrap().unwrap();
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

    #[test]
    fn test_doc_writer_batch_add_race() {
        use crate::shard_store_doc::PackedValue;
        use crate::shard_store_doc::DocStoreV3;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStoreV3::open(&docs_dir).unwrap();
        store.ensure_field_index("tagIds").unwrap();
        let store = Arc::new(parking_lot::RwLock::new(store));

        // Initial value
        {
            let initial = rmp_serde::to_vec(&PackedValue::Mi(vec![100, 200])).unwrap();
            let idx = store.read().field_index("tagIds").unwrap();
            store.write().append_tuple(5, idx, &initial).unwrap();
        }

        // Two adds for the same slot in ONE DocWriter batch (the race scenario).
        let mut dw = DocWriter::new(Arc::clone(&store));
        dw.write_add(5, "tagIds", &json!(300));
        dw.write_add(5, "tagIds", &json!(400));
        dw.flush();

        let doc = store.read().get(5).unwrap().unwrap();
        match &doc.fields["tagIds"] {
            crate::mutation::FieldValue::Multi(vals) => {
                let ints: Vec<i64> = vals.iter().filter_map(|v| {
                    if let crate::query::Value::Integer(i) = v { Some(*i) } else { None }
                }).collect();
                assert!(ints.contains(&100), "100 missing: {:?}", ints);
                assert!(ints.contains(&200), "200 missing: {:?}", ints);
                assert!(ints.contains(&300), "300 missing (batch race): {:?}", ints);
                assert!(ints.contains(&400), "400 missing (batch race): {:?}", ints);
            }
            other => panic!("expected multi-value tagIds, got: {:?}", other),
        }
    }

    #[test]
    fn test_doc_writer_batch_add_dedup() {
        use crate::shard_store_doc::PackedValue;
        use crate::shard_store_doc::DocStoreV3;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStoreV3::open(&docs_dir).unwrap();
        store.ensure_field_index("tagIds").unwrap();
        let store = Arc::new(parking_lot::RwLock::new(store));

        {
            let initial = rmp_serde::to_vec(&PackedValue::Mi(vec![100])).unwrap();
            let idx = store.read().field_index("tagIds").unwrap();
            store.write().append_tuple(5, idx, &initial).unwrap();
        }

        let mut dw = DocWriter::new(Arc::clone(&store));
        dw.write_add(5, "tagIds", &json!(100)); // re-add existing
        dw.write_add(5, "tagIds", &json!(200)); // add new
        dw.flush();

        let doc = store.read().get(5).unwrap().unwrap();
        match &doc.fields["tagIds"] {
            crate::mutation::FieldValue::Multi(vals) => {
                let ints: Vec<i64> = vals.iter().filter_map(|v| {
                    if let crate::query::Value::Integer(i) = v { Some(*i) } else { None }
                }).collect();
                let mut sorted = ints.clone();
                sorted.sort();
                assert_eq!(sorted, vec![100, 200], "expected exactly [100, 200], got: {:?}", ints);
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

        let store = Arc::new(parking_lot::RwLock::new(store));
        let mut dw = DocWriter::new(Arc::clone(&store));

        // Write scalar fields via DocWriter (simulates WAL ops processor path)
        dw.write_set(100, "sortAt", &json!(1711900000));
        dw.write_set(100, "nsfwLevel", &json!(5));
        dw.flush();

        // Read back via DocStoreV3 and verify
        let doc = store.read().get(100).unwrap();
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
        let mut fields = HashMap::new();
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
        let mut old_fields = HashMap::new();
        old_fields.insert("nsfwLevel".into(), FieldValue::Single(QValue::Integer(8)));
        let old_doc = crate::shard_store_doc::StoredDoc { fields: old_fields, schema_version: 0 };

        // New doc: nsfwLevel=16
        let mut new_fields = HashMap::new();
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
        let mut fields = HashMap::new();
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
        let mut old_fields = HashMap::new();
        old_fields.insert("nsfwLevel".into(), FieldValue::Single(QValue::Integer(8)));
        let old_doc = crate::shard_store_doc::StoredDoc { fields: old_fields, schema_version: 0 };

        // PATCH only sends userId=42 (nsfwLevel absent from patch)
        let mut new_fields = HashMap::new();
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
            per_value_lazy: false, max_range_scan_values: None,
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
    fn test_nullable_field_null_set_inserts_sentinel() {
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Set blockedFor to null — should insert NULL_BITMAP_KEY sentinel
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
            sink.filter_inserts.iter().any(|(f, v, _)| f == "blockedFor" && *v == crate::filter::NULL_BITMAP_KEY),
            "null set on nullable field should insert NULL_BITMAP_KEY sentinel"
        );
    }
    #[test]
    fn test_nullable_field_non_null_set_works() {
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Set blockedFor to a real value — should insert bitmap bit AND remove null sentinel
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "blockedFor".into(),
                value: json!(42), // use integer since blockedFor is SingleValue in test config
            }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
        assert!(
            sink.filter_inserts.iter().any(|(f, v, _)| f == "blockedFor" && *v != crate::filter::NULL_BITMAP_KEY),
            "non-null set on nullable field should insert value bitmap bit"
        );
        assert!(
            sink.filter_removes.iter().any(|(f, v, _)| f == "blockedFor" && *v == crate::filter::NULL_BITMAP_KEY),
            "non-null set on nullable field should remove NULL_BITMAP_KEY sentinel"
        );
    }
    #[test]
    fn test_nullable_field_null_remove_clears_sentinel() {
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        // Remove blockedFor with null value — should remove NULL_BITMAP_KEY sentinel
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
            sink.filter_removes.iter().any(|(f, v, _)| f == "blockedFor" && *v == crate::filter::NULL_BITMAP_KEY),
            "null remove on nullable field should remove NULL_BITMAP_KEY sentinel"
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
        // Simulate blockedFor changing from a value to null:
        // Remove old value, then Set null — old bitmap removed, null sentinel inserted
        let config = test_config_with_nullable();
        let meta = FieldMeta::from_config(&config);
        let mut sink = RecordingSink::new();
        let mut batch = vec![EntityOps {
            entity_id: 42,
            creates_slot: true,
            ops: vec![
                Op::Remove {
                    field: "blockedFor".into(),
                    value: json!(42), // use integer since blockedFor is SingleValue
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
            sink.filter_removes.iter().any(|(f, v, _)| f == "blockedFor" && *v != crate::filter::NULL_BITMAP_KEY),
            "old blockedFor value should be removed from bitmap"
        );
        // Null sentinel should be inserted
        assert!(
            sink.filter_inserts.iter().any(|(f, v, _)| f == "blockedFor" && *v == crate::filter::NULL_BITMAP_KEY),
            "null set should insert NULL_BITMAP_KEY sentinel"
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

    // ── #60: queryOpSet fan-out cap ─────────────────────────────────────────
    //
    // Env-mutating tests must be `#[serial]` because `BITDEX_QUERY_OP_SET_MAX_FANOUT`
    // is process-global. Each test sets its own value, asserts, restores prior state.

    #[test]
    #[serial_test::serial]
    fn test_max_fanout_default_is_usize_max() {
        std::env::remove_var("BITDEX_QUERY_OP_SET_MAX_FANOUT");
        assert_eq!(
            max_fanout(),
            usize::MAX,
            "unset env should yield DEFAULT_MAX_FANOUT (usize::MAX)"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_max_fanout_env_override_parses() {
        std::env::set_var("BITDEX_QUERY_OP_SET_MAX_FANOUT", "100000");
        assert_eq!(max_fanout(), 100_000);
        std::env::remove_var("BITDEX_QUERY_OP_SET_MAX_FANOUT");
    }

    #[test]
    #[serial_test::serial]
    fn test_max_fanout_invalid_env_falls_back_to_default() {
        std::env::set_var("BITDEX_QUERY_OP_SET_MAX_FANOUT", "not-a-number");
        assert_eq!(
            max_fanout(),
            usize::MAX,
            "non-numeric env value should fall back to DEFAULT_MAX_FANOUT"
        );
        std::env::remove_var("BITDEX_QUERY_OP_SET_MAX_FANOUT");
    }
}
