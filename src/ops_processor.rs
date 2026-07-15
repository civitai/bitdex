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
/// Warn when a single publish fan-out's deferred-reach pass scans more than
/// this many deferred candidates (one docstore read each). Healthy steady
/// state keeps only a handful of scheduled slots deferred; prod once grew to
/// ~49.7k during the reschedule-drop bug, so this bounds surprise.
const DEFERRED_REACH_WARN_THRESHOLD: usize = 10_000;
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
    pub(crate) fn write_set(&mut self, slot: u32, field: &str, value: &JsonValue) {
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
        // Explicit null clears the field. DocOp::Set apply removes the field
        // from the doc snapshot when the value is Null. Without this, scalar
        // nullable transitions (e.g. blockedFor "X"→null) leave the prior tuple
        // as the LIFO winner and reads return the stale value.
        JsonValue::Null => Some(PackedValue::Null),
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
#[allow(dead_code)]
struct PostEnrichment {
    published_at_secs: Option<i64>,
    availability: String,
}
/// ModelVersion enrichment data, keyed by model_version_id.
#[allow(dead_code)]
struct MvEnrichment {
    base_model: Option<String>,
    model_id: i64,
}
/// Model enrichment data, keyed by model_id.
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
fn resolve_string_dict(
    dicts: &HashMap<String, FieldDictionary>,
    field: &str,
    value: &str,
) -> Option<u64> {
    dicts.get(field).map(|dict| dict.get_or_insert(value) as u64)
}
/// Set sort layers for a u32 value on a slot in a BitmapAccum.
#[inline]
#[allow(dead_code)]
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
    check_deferred_alive_secs(meta, ops).is_some()
}

/// Same as `check_deferred_alive` but returns the activation timestamp (seconds
/// since epoch) when deferral applies. Used by `apply_query_op_set` to register
/// deferred fan-out slots without a second scan of the ops.
fn check_deferred_alive_secs(meta: &FieldMeta, ops: &[Op]) -> Option<u64> {
    let (ref da_field, ms_to_secs) = meta.deferred_alive_field.as_ref()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    for op in ops {
        if let Op::Set { field, value } = op {
            if field == da_field.as_str() {
                if let Some(ts) = value.as_i64() {
                    let secs = if *ms_to_secs { ts / 1000 } else { ts };
                    if secs > now {
                        return Some(secs as u64);
                    }
                }
            }
        }
    }
    None
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
/// Process-wide memo of slots confirmed DELETED (not alive, below the
/// high-water mark, stored doc present). Bounds the cost of the doc-presence
/// probe in the stale-op skip path: late updates for deleted entities can
/// repeat, and each probe decodes a docstore shard. Cleared wholesale if it
/// ever grows past the cap (stale entries are harmless — a re-probe just
/// re-confirms; a recycled slot becomes ALIVE again and never reaches this
/// path).
fn confirmed_deleted_slots() -> &'static parking_lot::Mutex<ahash::AHashSet<u32>> {
    static MEMO: std::sync::OnceLock<parking_lot::Mutex<ahash::AHashSet<u32>>> =
        std::sync::OnceLock::new();
    MEMO.get_or_init(|| parking_lot::Mutex::new(ahash::AHashSet::new()))
}

fn is_confirmed_deleted(slot: u32) -> bool {
    confirmed_deleted_slots().lock().contains(&slot)
}

fn remember_confirmed_deleted(slot: u32) {
    let mut memo = confirmed_deleted_slots().lock();
    if memo.len() >= 1_000_000 {
        memo.clear();
    }
    memo.insert(slot);
}

/// Tests share the process-wide memo; clear it so one test's deleted slots
/// can't leak skip decisions into another's.
#[cfg(test)]
pub(crate) fn clear_confirmed_deleted_memo() {
    confirmed_deleted_slots().lock().clear();
}

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
    // Push fan-out entries to the end of the batch. `dedup_ops` reassembles
    // entries from an AHashMap, which iterates in non-deterministic order —
    // so a fan-out can land BEFORE the entries whose filter writes it
    // depends on (e.g., a Post fan-out querying `postId eq P` arriving
    // before the Image entity that sets `postId=P`). Even with the
    // flush+force_publish barrier inside `apply_query_op_set`, there is
    // nothing to flush when the fan-out runs first. Sorting fan-outs last
    // makes the in-batch dependency order well-defined and lets the
    // barrier do its job. Repro: `tests/sortat_fanout_race.rs::t2b`.
    batch.sort_by_key(|e| {
        e.ops.iter().any(|op| matches!(op, Op::QueryOpSet { .. }))
    });
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

    // Same-batch fan-out visibility barrier — runs ONCE per batch, just
    // before the FIRST QueryOpSet entry is processed (not before the loop
    // starts — at that point nothing's in CoalescerSink yet). Because we
    // sorted all QueryOpSet entries to the end of the batch above, by the
    // time we hit the first fan-out, every non-fan-out write (filter
    // inserts, sort layer writes, doc writes) has already been emitted to
    // the sink/doc_writer and just needs to be flushed → published →
    // visible to `engine.execute_query` inside `apply_query_op_set`.
    //
    // Previous implementation called the barrier per-fan-out from inside
    // `apply_query_op_set`. With many fan-outs per batch (e.g. a Post
    // update fanning to 100+ Images, or a Model update fanning to
    // thousands), that produced O(N) * 100ms latency on the WAL reader
    // hot path and could back up the ops poller under sustained load.
    // Hoisting to one call cuts worst-case to O(1) per batch.
    //
    // Timeout: 5s (vs the original 100ms inside apply_query_op_set). The
    // flush thread normally publishes in well under 50ms; 5s is for
    // pathological cases like back-pressure during lazy load. On true
    // timeout we count an error per fan-out we're about to skip and
    // suppress the fan-outs themselves rather than execute them against
    // a known-stale snapshot — that's the original Bug B failure mode.
    // The skipped ops are NOT recovered automatically (WAL cursor still
    // advances) — operator response is to watch the
    // `bitdex_fanout_barrier_skips_total` metric and trigger a fresh
    // dump if it spikes.
    let mut fanout_barrier_done = false;
    let mut fanout_barrier_failed = false;

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
                Ok(()) => {
                    // A deferred (not-yet-alive) slot has no bitmap bits to
                    // clear, but its deferred-map entry must go too — a later
                    // activate_due would otherwise resurrect the deleted slot
                    // by replaying its stored doc. Emitted unconditionally:
                    // is_slot_deferred lags the published snapshot, and a
                    // cancel for a never-deferred slot is a cheap no-op.
                    if meta.deferred_alive_field.is_some() {
                        sink.deferred_cancel(slot);
                    }
                    applied += 1;
                }
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
                    // Deferred slots are allocated (below the high-water mark)
                    // but not alive — the plain not-alive skip used to drop
                    // EVERY follow-up op for them: publish-date reschedules,
                    // unpublishes, tag/metric updates, all silently lost until
                    // activation replayed a stale doc (audit 2026-07-07, §3.1).
                    // While deferred: persist all field writes to the docstore
                    // (activation replays the doc, so they surface then), and
                    // if the deferred source field itself changed, re-schedule
                    // the activation — schedule_alive dedupes the old key, and
                    // a now-past timestamp activates on the next flush cycle.
                    if eng.is_slot_deferred(slot) {
                        if let Some(ref mut dw) = doc_writer {
                            for op in &entry.ops {
                                match op {
                                    Op::Set { field, value } => dw.write_set(slot, field, value),
                                    Op::Add { field, value } => dw.write_add(slot, field, value),
                                    Op::Remove { field, value } => {
                                        dw.write_remove(slot, field, value)
                                    }
                                    _ => {}
                                }
                            }
                        }
                        // Does this batch touch the deferred source field at all?
                        // (Set to null and Remove yield no timestamp, but they DO
                        // change the schedule — they unschedule it.)
                        let da_field = meta
                            .deferred_alive_field
                            .as_ref()
                            .map(|(f, _)| f.as_str())
                            .unwrap_or_default();
                        let modifies_schedule = entry.ops.iter().any(|op| match op {
                            Op::Set { field, .. } | Op::Remove { field, .. } => {
                                field == da_field
                            }
                            _ => false,
                        });
                        if let Some(new_at) = get_deferred_timestamp(meta, &entry.ops) {
                            // Reschedule (a past timestamp activates next cycle).
                            sink.deferred_alive(slot, new_at);
                            tracing::info!(
                                "ops processor: rescheduled deferred slot {slot} to {new_at}"
                            );
                        } else if modifies_schedule {
                            // Unschedule (publishedAt → null/removed): the entity
                            // reverts to a plain draft, which is ALIVE. Schedule an
                            // immediate activation — activate_due replays the full
                            // stored doc, rebuilding every bitmap (not just this
                            // op's fields), so the draft becomes fully queryable.
                            // Leaving it deferred would keep it invisible until the
                            // stale schedule fired.
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            sink.deferred_alive(slot, now);
                            tracing::info!(
                                "ops processor: unscheduled deferred slot {slot} — \
                                 activating as draft"
                            );
                        }
                        applied += 1;
                        continue;
                    }
                    if slot >= eng.slot_counter() {
                        // Slot is beyond high-water mark — this is a new entity,
                        // not a stale op for a deleted slot. Auto-promote.
                        creates_slot = true;
                        tracing::info!(
                            "ops processor: auto-promoting slot {slot} to creates_slot \
                             (entity_id={entity_id}, beyond slot_counter={})",
                            eng.slot_counter()
                        );
                    } else if is_confirmed_deleted(slot) {
                        // Memoized: this slot already proved doc-present
                        // (deleted). Skip without re-reading the docstore —
                        // late stale updates for deleted entities can repeat
                        // (metrics refreshers, retries) and the doc read
                        // decodes a whole shard.
                        skipped += 1;
                        continue;
                    } else if eng
                        .docstore_arc()
                        .read()
                        .get(slot)
                        .ok()
                        .flatten()
                        .is_none()
                    {
                        // Below the high-water mark, not alive, and NO stored doc:
                        // this slot was never inserted — the op IS the insert.
                        // The high-water heuristic alone is unsafe here: a bulk
                        // load's later phases (metrics dumps at a later wall-clock
                        // cut than the images CSV) push slot_counter past slots
                        // whose insert ops exist only in the replay stream, and
                        // the plain skip silently dropped them (2026-07-13 nuke:
                        // 9,614 skipped ops / 285 posts missing images on the
                        // pod that dumped mid-churn). Deleted slots are the case
                        // the skip exists for, and they KEEP their stored doc
                        // until autovac — so doc-absent discriminates
                        // never-inserted from deleted exactly.
                        creates_slot = true;
                        tracing::info!(
                            "ops processor: auto-promoting slot {slot} to creates_slot \
                             (entity_id={entity_id}, below slot_counter={} but no stored doc)",
                            eng.slot_counter()
                        );
                    } else {
                        remember_confirmed_deleted(slot);
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
                // First fan-out reached: drain CoalescerSink + DocWriter and
                // wait for engine to publish a fresh snapshot containing
                // every preceding non-fan-out write in this batch.
                if !fanout_barrier_done {
                    fanout_barrier_done = true;
                    if let Some(eng) = engine {
                        if let Some(ref mut dw) = doc_writer {
                            dw.flush();
                        }
                        if let Err(e) = sink.flush() {
                            tracing::warn!(
                                "ops processor: sink.flush before fan-out failed: {e}"
                            );
                        }
                        if !eng.force_publish_blocking(Duration::from_secs(5)) {
                            tracing::error!(
                                "ops processor: force_publish_blocking timed out \
                                 after 5s — fan-outs in this batch will be SKIPPED \
                                 to avoid stale-snapshot fan-out misses. WAL cursor \
                                 still advances; operator must watch fanout_barrier \
                                 skip metric."
                            );
                            fanout_barrier_failed = true;
                        }
                    }
                }
                if fanout_barrier_failed {
                    // Barrier timed out: snapshot the fan-out resolves against
                    // is known-stale. Skip rather than execute against it —
                    // that's the Bug B failure mode we just fixed.
                    tracing::warn!(
                        "ops processor: queryOpSet '{query_str}' SKIPPED — pre-batch barrier timed out"
                    );
                    errors += 1;
                } else if let Some(eng) = engine {
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
        recompute_computed_sorts_for_slot(
            sink,
            meta,
            engine,
            slot,
            &sort_values,
            &old_sort_values,
            doc_writer.as_deref_mut(),
        );
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
/// Recompute every computed sort field whose source fields appear in
/// `sort_values` (new) or `old_sort_values` (old) for a single slot. Writes a
/// definitive set or clear op per bit based on the new computed value (full
/// overwrite — independent of any prior bitmap state). Falls back to the
/// stored doc for source fields not present in the ops batch so partial
/// updates compute against the current persisted value, not 0.
///
/// Called from both:
/// - `apply_ops_batch` per directly-addressed entity slot (each entity carries
///   its own remove+set pairs that populate sort_values/old_sort_values).
/// - `apply_query_op_set` per fan-out matched slot (the shared ops vector
///   populates sort_values/old_sort_values once before the per-slot loop).
fn recompute_computed_sorts_for_slot<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    engine: Option<&ConcurrentEngine>,
    slot: u32,
    sort_values: &HashMap<&str, u32>,
    old_sort_values: &HashMap<&str, u32>,
    mut doc_writer: Option<&mut DocWriter>,
) {
    if meta.computed_deps.is_empty() {
        return;
    }
    tracing::trace!(
        "computed_deps: slot={} sort_vals={:?} old_sort_vals={:?} deps_keys={:?}",
        slot,
        sort_values.keys().collect::<Vec<_>>(),
        old_sort_values.keys().collect::<Vec<_>>(),
        meta.computed_deps.keys().collect::<Vec<_>>(),
    );
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
    if changed_sources.is_empty() {
        return;
    }
    // Read stored doc to fill in source fields not present in this ops batch.
    // Without this, missing sources default to 0 and break GREATEST/LEAST.
    //
    // Deferred-source skip: if the stored value of the configured
    // `deferred_alive` source field (e.g. `publishedAt`) is in the FUTURE,
    // the slot is in deferred-alive state — the doc holds the scheduled
    // future timestamp as the raw PG truth (via the deferred branch of
    // `apply_query_op_set`), but that value must NOT leak into the
    // computed-sort bitmap. Otherwise any later op that touches another
    // source field (e.g. an Image re-scan emitting `Set existedAt=new`)
    // triggers a recompute whose `max(existedAt, T_FUTURE) = T_FUTURE`
    // bakes the scheduled value into the sort layer — exactly the prod
    // symptom from 2026-05-12 (bitmap.sortAt 50+ days ahead of now for
    // post-redump-inserted slots). Repro:
    // `tests/sortat_fanout_race.rs::d4_*`.
    //
    // Excluding the field from `stored_sort_values` makes the lookup
    // unwrap_or(0) below, so the computed value derives from the
    // currently-visible source fields only. `activate_due` doesn't go
    // through this helper — it calls `diff_document` directly with the
    // current (now-past-or-equal) publishedAt, so activation still
    // writes the correct bitmap value at the right moment.
    let now_secs_for_deferred = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0);
    let deferred_field: Option<(&str, bool)> = meta
        .deferred_alive_field
        .as_ref()
        .map(|(name, ms)| (name.as_str(), *ms));
    let stored_sort_values: HashMap<&str, u32> = if let Some(eng) = engine {
        let mut stored = HashMap::new();
        if let Ok(Some(doc)) = eng.get_document(slot) {
            for source_field in changed_sources.iter().flat_map(|sf| {
                meta.computed_deps
                    .get(*sf)
                    .into_iter()
                    .flat_map(|deps| {
                        deps.iter()
                            .flat_map(|d| d.source_fields.iter().map(|s| s.as_str()))
                    })
            }) {
                if !sort_values.contains_key(source_field)
                    && !old_sort_values.contains_key(source_field)
                {
                    if let Some(fv) = doc.fields.get(source_field) {
                        if let crate::mutation::FieldValue::Single(ref v) = fv {
                            if let Some(sv) = value_to_sort_u32(v) {
                                // Deferred-alive guard: if this stored field
                                // is the deferred trigger AND it's in the
                                // future, omit it from stored_sort_values.
                                let skip_deferred = deferred_field
                                    .map(|(name, _ms)| {
                                        name == source_field && sv > now_secs_for_deferred
                                    })
                                    .unwrap_or(false);
                                if !skip_deferred {
                                    stored.insert(source_field, sv);
                                } else {
                                    tracing::trace!(
                                        "computed sort recomp: slot={} skipping deferred stored {}={} (now={})",
                                        slot, source_field, sv, now_secs_for_deferred
                                    );
                                }
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
                let new_values: Vec<u32> = dep
                    .source_fields
                    .iter()
                    .map(|sf| {
                        sort_values
                            .get(sf.as_str())
                            .or_else(|| stored_sort_values.get(sf.as_str()))
                            .copied()
                            .unwrap_or(0)
                    })
                    .collect();
                let new_computed = match dep.op {
                    crate::config::ComputedOp::Greatest => {
                        *new_values.iter().max().unwrap_or(&0)
                    }
                    crate::config::ComputedOp::Least => {
                        *new_values.iter().min().unwrap_or(&0)
                    }
                };
                tracing::trace!(
                    "computed sort recomp: target={} slot={} new_vals={:?}→{} stored={:?}",
                    dep.target,
                    slot,
                    new_values,
                    new_computed,
                    stored_sort_values.keys().collect::<Vec<_>>(),
                );
                // Full overwrite: write set OR clear for every bit based on
                // new_computed alone. See `process_set_op` for the same
                // pattern on direct sort fields. Independence from prior
                // bitmap state is what stops the OR-accumulation corruption
                // documented in src/ops_processor.rs history.
                for bit in 0..dep.target_bits {
                    if (new_computed >> bit) & 1 == 1 {
                        sink.sort_set(dep.target_arc.clone(), bit, slot);
                    } else {
                        sink.sort_clear(dep.target_arc.clone(), bit, slot);
                    }
                }
                if let Some(ref mut dw) = doc_writer {
                    dw.write_set(slot, &dep.target, &serde_json::json!(new_computed));
                }
            }
        }
    }

    // --- Safety net: complete a lost deferred activation (2026-07-03) ---
    // A scheduled-ahead slot is deferred (doc.publishedAt = future, bitmap +
    // isPublished shadow untouched) and is meant to be activated by the flush
    // thread's `activate_due` when its time arrives. Prod audit found ~49.7k
    // slots whose activation was never applied (deferred-map scheduling lost):
    // they went live (publishedAt now in the past) but the `isPublished`
    // exists_boolean shadow stayed false and the `publishedAt` source sort
    // layer stayed 0, so they are excluded from `isPublished=true` feeds. The
    // computed `sortAt` heals on its own here (it derives from the stored
    // publishedAt once past), which masked the real defect.
    //
    // Whenever a recompute touches such a slot, finish the job the lost
    // activation should have done: write the `publishedAt` source sort layer
    // and flip the `isPublished` shadow to true. Both are idempotent, so
    // already-activated slots are unaffected. A slot still legitimately
    // deferred (publishedAt in the future) or a genuine draft (no stored
    // publishedAt) is skipped by the `> 0 && <= now` guard. This does NOT
    // remove the need to fix the deferred-map scheduling itself — it bounds
    // the blast radius so live-but-unflipped slots self-correct on the next
    // source-field op. See docs/_in/deferred-publish-isPublished-corrected-diagnosis-2026-07-03.md.
    if let (Some(eng), Some((deferred_name, ms_to_secs))) = (engine, deferred_field) {
        if let Ok(Some(doc)) = eng.get_document(slot) {
            if let Some(crate::mutation::FieldValue::Single(ref v)) =
                doc.fields.get(deferred_name)
            {
                // Extract the raw integer BEFORE any u32 narrowing — with
                // ms_to_seconds configs the stored value is milliseconds and
                // would wrap u32 first, corrupting both the comparison and
                // the healed sort layer.
                let raw: Option<i64> = match v {
                    crate::types::Value::Integer(i) => Some(*i),
                    other => value_to_sort_u32(other).map(|u| u as i64),
                };
                if let Some(raw) = raw {
                    let secs = if ms_to_secs { raw / 1000 } else { raw };
                    let pub_val = secs.clamp(0, u32::MAX as i64) as u32;
                    if pub_val > 0 && pub_val <= now_secs_for_deferred {
                        // publishedAt source sort layer — full overwrite.
                        if let Some((arc_name, num_bits)) = meta.sort_fields.get(deferred_name) {
                            for bit in 0..*num_bits {
                                if (pub_val >> bit) & 1 == 1 {
                                    sink.sort_set(arc_name.clone(), bit, slot);
                                } else {
                                    sink.sort_clear(arc_name.clone(), bit, slot);
                                }
                            }
                            if let Some(ref mut dw) = doc_writer {
                                dw.write_set(slot, deferred_name, &serde_json::json!(pub_val));
                            }
                        }
                        // isPublished exists_boolean shadow → true (key 1).
                        if let Some(shadows) = meta.exists_boolean_shadows.get(deferred_name) {
                            for arc_name in shadows {
                                sink.filter_remove(arc_name.clone(), 0, slot);
                                sink.filter_insert(arc_name.clone(), 1, slot);
                                if let Some(ref mut dw) = doc_writer {
                                    dw.write_set(slot, arc_name.as_ref(), &serde_json::json!(true));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
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
/// Overdue-deferred sweep (audit 2026-07-07, fix A4): heal slots whose
/// deferred activation was lost before the durability fixes, or slips through
/// any residual gap. Entirely config-driven — every field name is derived
/// from `deferred_alive.source_field` (exists_boolean shadow registry +
/// computed-sort deps); nothing is hardcoded.
///
/// Queries alive slots whose shadow (e.g. `isPublished`) is still false,
/// most-feed-relevant first (computed sort target descending, capped at
/// `limit`), doc-checks that the stored source timestamp is past, and
/// re-emits the activation state via `recompute_computed_sorts_for_slot`
/// (which writes the source sort layer, flips the shadow, and recomputes the
/// computed target). Idempotent: healthy drafts (no stored source value) and
/// legitimately deferred slots (future value) are skipped by the doc check.
///
/// Runs on the WAL reader thread between batches — never on the flush thread.
///
/// Pagination (page-cap fix, 2026-07-14): `execute_query` clamps `limit` to
/// `max_page_size` (200 in prod), so a single query can never see more than
/// one page of the shadow-false candidate space. Skipped candidates (drafts,
/// legitimately-deferred) stay shadow-false, so without pagination every
/// cycle re-scans the SAME head and stuck slots below it are never reached
/// (45 victims sat 15h on 2026-07-14). This fn now pages via keyset cursor
/// until `limit` candidates are checked or the space is exhausted, and
/// returns the cursor so the caller can resume the rotation next cycle —
/// every candidate is reached within ceil(population / limit) cycles.
///
/// Returns (candidates_checked, healed_slots, resume_cursor). A `None`
/// resume_cursor means the full candidate space was covered; pass the
/// returned cursor back in as `resume` on the next cycle.
pub fn overdue_deferred_sweep<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    engine: &ConcurrentEngine,
    doc_writer: &mut DocWriter,
    limit: usize,
    resume: Option<crate::query::CursorPosition>,
) -> (usize, Vec<u32>, Option<crate::query::CursorPosition>) {
    let Some((source_field, ms_to_secs)) = meta.deferred_alive_field.clone() else {
        return (0, Vec::new(), None);
    };
    let Some(shadow) = meta
        .exists_boolean_shadows
        .get(source_field.as_str())
        .and_then(|s| s.first())
        .cloned()
    else {
        // No shadow configured — nothing observable to sweep against.
        return (0, Vec::new(), None);
    };
    // Feed-relevance ordering: the computed target derived from the source
    // (e.g. sortAt from publishedAt); fall back to the source sort field.
    let sort_field = meta
        .computed_deps
        .get(source_field.as_str())
        .and_then(|deps| deps.first().map(|d| d.target.clone()))
        .unwrap_or_else(|| source_field.clone());
    // The engine clamps per-query results to max_page_size, so page through
    // the candidate space with the keyset cursor instead of issuing one
    // over-clamped query.
    let page_size = engine.config().max_page_size.min(limit).max(1);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut cursor = resume;
    let mut exhausted = false;
    let mut checked = 0usize;
    let mut healed: Vec<u32> = Vec::new();
    while checked < limit {
        let query = BitdexQuery {
            filters: vec![crate::query::FilterClause::Eq(
                shadow.to_string(),
                crate::query::Value::Bool(false),
            )],
            sort: Some(crate::query::SortClause {
                field: sort_field.clone(),
                direction: crate::query::SortDirection::Desc,
            }),
            limit: page_size,
            cursor: cursor.clone(),
            offset: None,
            skip_cache: true,
        };
        let result = match engine.execute_query(&query) {
            Ok(result) => result,
            Err(e) => {
                // Return the last-good cursor, not None: a deterministic
                // error at page N must not reset the rotation to the top
                // every cycle (that would starve the tail forever — the
                // exact failure mode this fn exists to prevent).
                tracing::warn!("overdue-deferred sweep: query failed: {e}");
                return (checked, healed, cursor);
            }
        };
        let page_len = result.ids.len();
        if page_len == 0 {
            exhausted = true;
            break;
        }
        for id in result.ids {
            if id < 0 || id > u32::MAX as i64 {
                continue;
            }
            let slot = id as u32;
            checked += 1;
            let Ok(Some(doc)) = engine.get_document(slot) else {
                continue;
            };
            let Some(crate::mutation::FieldValue::Single(v)) =
                doc.fields.get(source_field.as_str())
            else {
                continue; // genuine draft — no stored source value
            };
            // Extract the raw integer BEFORE any u32 narrowing — a milliseconds
            // source would wrap u32 first and corrupt the comparison.
            let raw: i64 = match v {
                crate::types::Value::Integer(i) => *i,
                other => match crate::mutation::value_to_sort_u32(other) {
                    Some(u) => u as i64,
                    None => continue,
                },
            };
            let secs = if ms_to_secs { raw / 1000 } else { raw };
            if secs <= 0 || secs > now {
                continue; // draft (0) or legitimately deferred (future)
            }
            let mut sort_values: HashMap<&str, u32> = HashMap::new();
            sort_values.insert(source_field.as_str(), secs as u32);
            let empty: HashMap<&str, u32> = HashMap::new();
            recompute_computed_sorts_for_slot(
                sink,
                meta,
                Some(engine),
                slot,
                &sort_values,
                &empty,
                Some(doc_writer),
            );
            healed.push(slot);
        }
        cursor = result.cursor;
        if page_len < page_size || cursor.is_none() {
            exhausted = true;
            break;
        }
    }
    if exhausted {
        cursor = None;
        tracing::info!(
            "overdue-deferred sweep: full candidate pass complete (checked {checked}, healed {})",
            healed.len()
        );
    }
    if !healed.is_empty() {
        tracing::info!(
            "overdue-deferred sweep: healed {} of {checked} shadow-false candidates",
            healed.len()
        );
    }
    (checked, healed, cursor)
}

/// What the verifier concluded about an apparent orphan once the post-publish
/// barrier re-read came back. Only `PublishLag` is benign — it means the slot
/// was present all along and the first read was early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanVerdict {
    /// Present after the barrier: the activation batch was applied and
    /// published late. Nothing was lost, so nothing needs re-driving.
    PublishLag,
    /// Absent, but the barrier never completed, so "absent" was never
    /// established against a published snapshot: this is a genuine drop and a
    /// publish lag longer than the barrier, indistinguishable. Re-driven like
    /// any unproven slot, but NOT counted as a confirmed drop.
    Inconclusive,
    /// Absent after a COMPLETED barrier with no remove ops against the slot:
    /// the batch's field ops never reached the published snapshot.
    NonApply,
    /// Absent after a COMPLETED barrier, and remove ops hit the slot:
    /// something undid the activation.
    Revert,
}

impl OrphanVerdict {
    /// `barrier_ok` is whether `force_publish_blocking` actually COMPLETED.
    /// It gates only the absent branch: a positive re-read proves the slot
    /// present however the barrier went, but a negative one proves nothing
    /// unless a publish is known to have landed first. The barrier times out
    /// often enough in prod (observed 1-in-2 in one window) that treating a
    /// timed-out absent read as a confirmed drop would make
    /// `redriven_total` — the alarm-worthy counter — cry wolf on lag.
    ///
    /// ── THE TRADE: a false POSITIVE swapped for a false NEGATIVE ──
    /// This does NOT make the verifier better at finding drops. It makes
    /// `redriven_total` SOUND (every count is a real drop — no phantoms) at
    /// the cost of SENSITIVITY (roughly half of real drops are missed). A
    /// REAL drop that happens during a slow promote has `barrier_ok == false`
    /// and lands in `inconclusive`, NOT `redriven` — so an alarm wired to
    /// `redriven_total` stays SILENT on it. Do not read `redriven_total` as
    /// "all real drops"; read it as "drops we can prove". `inconclusive` is
    /// the bucket where the unproven ones — real and benign alike — pile up.
    ///
    /// That trade is only acceptable because `Inconclusive` still re-drives:
    /// the data is repaired either way, so what is lost is DETECTION, not
    /// safety. An alarm that fires on every long promote gets muted, and a
    /// muted alarm detects nothing at all.
    ///
    /// ── Do NOT "fix" this by raising the barrier cap ──
    /// Barrier tuning is an ASYMPTOTE, not a solution. 100ms fails ~95% of
    /// promotes; 500ms fails ~50%; the promote is a CoW clone-cascade with no
    /// fixed ceiling, so no cap makes `barrier_ok == false` go away. Each
    /// increment only buys more WAL-reader stall in exchange for a diminishing
    /// slice of that tail. The structural answer is a LATER PASS (re-check the
    /// slot once the publish has certainly landed, off this thread) rather
    /// than a longer WAIT — that is v1.1.48 work, tracked in FOLLOWUP.md.
    pub fn classify(barrier_ok: bool, reread_present: bool, has_recent_removes: bool) -> Self {
        match (barrier_ok, reread_present, has_recent_removes) {
            (_, true, _) => Self::PublishLag,
            (false, false, _) => Self::Inconclusive,
            (true, false, false) => Self::NonApply,
            (true, false, true) => Self::Revert,
        }
    }
    /// Whether the slot must be re-driven. Every verdict that does not PROVE
    /// the slot present re-drives — a genuine orphan can never be suppressed.
    pub fn redrives(self) -> bool {
        !matches!(self, Self::PublishLag)
    }
    /// Whether a re-drive under this verdict is a CONFIRMED drop, i.e. counts
    /// toward `redriven_total`. `Inconclusive` re-drives too, but its verdict
    /// is unknown, so it routes to `inconclusive_total` instead — keeping
    /// `redriven_total` a signal an operator can alarm on.
    pub fn is_confirmed_drop(self) -> bool {
        matches!(self, Self::NonApply | Self::Revert)
    }
    pub fn description(self) -> &'static str {
        match self {
            Self::PublishLag => {
                "PUBLISH-LAG (present after re-read; batch applied, published late — not a true drop, re-drive skipped)"
            }
            Self::Inconclusive => {
                "INCONCLUSIVE (absent, but barrier timed out — drop vs over-long publish lag unproven; re-driven anyway)"
            }
            Self::NonApply => "NON-APPLY (still absent after completed barrier, no revert ops seen)",
            Self::Revert => {
                "REVERT (still absent after completed barrier, remove ops hit this slot)"
            }
        }
    }
}

/// What one `verify_recent_activations` pass did.
///
/// `redriven`, `publish_lag` and `inconclusive` are mutually exclusive and
/// together account for every checked slot whose membership was successfully
/// determined and found absent. A slot whose re-read fails is requeued, not
/// counted — `checked` is incremented before the read, so it can exceed the
/// sum of the three counters on any given pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VerifyOutcome {
    /// Activated slots examined (had a membership value to verify).
    pub checked: usize,
    /// Confirmed orphans re-driven from their stored doc: absent after a
    /// COMPLETED publish barrier. The alarm-worthy count.
    pub redriven: usize,
    /// Apparent orphans the re-read proved present — published late, skipped.
    pub publish_lag: usize,
    /// Absent slots re-driven without a completed barrier — drop vs over-long
    /// publish lag unproven. Watch, don't alarm.
    pub inconclusive: usize,
}

/// Post-activation verifier (deferred activation-miss backstop, 2026-07-14).
///
/// Drains recently-activated slots (queued by the flush thread after replay)
/// and confirms each is actually indexed under its own `postId` — the exact
/// orphan signature: a slot that `activate_due` fired but whose `postId` filter
/// insert was dropped, leaving it ALIVE yet absent from every postId-scoped
/// feed and invisible to the `isPublished=false` overdue sweep. On a miss it
/// re-drives the full stored doc via `diff_document(None, doc)` — the same
/// replay activation performs — sent through the mutation channel. Idempotent:
/// full-overwrite sort layers, idempotent filter inserts, so re-driving a
/// healthy slot (or one that a concurrent organic op already fixed) is a no-op.
///
/// An apparent orphan is not taken at face value: the first read can simply be
/// ahead of the publish, so the slot is re-read after a barrier and only
/// re-driven if it is still absent (see `OrphanVerdict`).
///
/// Runs on the WAL reader between batches (query + doc reads stay off the flush
/// thread).
pub fn verify_recent_activations(engine: &ConcurrentEngine, limit: usize) -> VerifyOutcome {
    // The membership field — the filter field the activation fan-out groups on
    // (e.g. postId) — is config-driven; no field name is baked into the logic.
    // Absent (or not a configured filter) → the verifier is disabled.
    let verify_field = match engine.config().activation_verify.membership_field.clone() {
        Some(f) if !f.is_empty() => f,
        _ => return VerifyOutcome::default(),
    };
    if !engine
        .config()
        .filter_fields
        .iter()
        .any(|f| f.name == verify_field)
    {
        return VerifyOutcome::default();
    }
    let slots = engine.drain_activation_verify(limit);
    if slots.is_empty() {
        return VerifyOutcome::default();
    }
    let registry = FieldRegistry::from_config(engine.config());
    let mut checked = 0usize;
    let mut redriven = 0usize;
    let mut publish_lag = 0usize;
    let mut inconclusive = 0usize;
    let mut requeue: Vec<u32> = Vec::new();
    for slot in slots {
        let doc = match engine.get_document(slot) {
            Ok(Some(d)) => d,
            Ok(None) => continue,             // deleted/absent — nothing to verify
            Err(_) => {
                requeue.push(slot); // transient read failure — re-check next pass
                continue;
            }
        };
        // Only alive slots are activations we care about; a re-deferred or
        // deleted slot is not an orphan.
        if !engine.is_slot_alive(slot) {
            continue;
        }
        let pid = match doc.fields.get(verify_field.as_str()) {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(p))) => *p,
            _ => continue, // no grouping value — can't verify membership
        };
        checked += 1;
        let query = BitdexQuery {
            filters: vec![FilterClause::Eq(
                verify_field.clone(),
                crate::query::Value::Integer(pid),
            )],
            sort: None,
            limit: usize::MAX,
            offset: None,
            cursor: None,
            skip_cache: true,
        };
        let indexed = match engine.execute_query(&query) {
            Ok(r) => r.ids.iter().any(|&id| id == slot as i64),
            Err(_) => {
                requeue.push(slot); // transient query failure — re-check next pass
                continue;
            }
        };
        if indexed {
            continue; // healthy — the slot is in its postId bitmap
        }
        // VB-state diagnostic (residual activation-miss hunt): capture the exact
        // membership-bitmap state at the orphan moment, BEFORE the re-drive
        // rebuilds it. Static tracing + deterministic repros ruled out the
        // per-value-lazy load clobber; this pins WHICH runtime state produced
        // the miss on the next prod specimen. Low volume (only on a real
        // re-drive, ~sub-per-minute). Cross-checks (all config-derived, no
        // hardcoded field names): alive bit (rules out "never activated"); the
        // EAGER exists_boolean shadow (e.g. isPublished) vs the LAZY membership
        // field — if the slot has the eager field but not the lazy one, the drop
        // is lazy-field-specific; and the deferred source SORT layer (e.g.
        // publishedAt). "has every field except postId" ⇒ postId-specific;
        // "missing multiple" ⇒ broader emit/publish loss.
        let alive = engine.is_slot_alive(slot);
        let shadow_field = engine
            .config()
            .data_schema
            .fields
            .iter()
            .find(|fm| matches!(fm.value_type, crate::config::FieldValueType::ExistsBoolean))
            .map(|fm| fm.target.clone());
        let shadow_state = shadow_field
            .as_ref()
            .map(|f| engine.filter_value_state_debug(f, 1, slot))
            .unwrap_or_else(|| "none".to_string());
        let sort_field = engine
            .config()
            .deferred_alive
            .as_ref()
            .map(|d| d.source_field.clone());
        let sort_present = sort_field
            .as_ref()
            .and_then(|f| engine.sort_slot_present_debug(f, slot));
        tracing::warn!(
            target: "activation",
            "verify: orphan slot {slot} {verify_field}={pid} alive={alive} \
             membership[{}] shadow[{shadow_state}] source_sort_present={sort_present:?}",
            engine.filter_value_state_debug(&verify_field, pid as u64, slot),
        );
        // ── Post-publish re-read: is this a real orphan, or publish lag? ──
        // The first query above is not proof of a drop. It runs
        // execute_query → ensure_fields_loaded, whose ForcePublish barrier is
        // capped at 100ms, while a sort promote routinely runs longer than
        // that in prod — so the barrier times out on the large majority of
        // promotes (93-98% observed) and the query reads PRE-publish state.
        // (Directly observed: the 500ms barrier below succeeds while the
        // 100ms one fails on the same slot in the same window. Do NOT quote
        // a precise promote duration here — [flush-slow]'s fields do not sum,
        // so that field is not a trustworthy per-cycle cost. See FOLLOWUP.md.) A slot whose activation batch was
        // applied and is merely publishing late then looks identical to one
        // that was dropped.
        //
        // So barrier properly (`publish_barrier_ms`, default 2s — sized over a
        // promote, unlike the 100ms query-path cap) and re-read. (a) PRESENT
        // ⇒ publish lag: the batch landed, nothing was ever lost, and the
        // re-drive would be a no-op re-apply of data already arriving — count
        // it and skip. This holds whether or not the barrier completed: a set
        // bit is proof on its own. (b) ABSENT after a COMPLETED barrier ⇒ a
        // confirmed orphan: re-drive, and the recent remove-op ring tells us
        // which kind — a FilterRemove/SortClear/AliveRemove hit is a REVERT
        // signal; an empty ring points at NON-APPLY. (c) ABSENT but the
        // barrier TIMED OUT ⇒ INCONCLUSIVE: this barrier is not immune to the
        // problem it was added to solve — prod shows it completing (+54ms) and
        // timing out (+501ms, the full cap) at roughly 1-in-2 in one window —
        // and an absent read behind a timed-out barrier is a drop and a lag
        // longer than the barrier, indistinguishable. Re-drive (see below) but
        // keep it out of `redriven_total` so that counter stays alarm-worthy.
        //
        // The suppression cannot mask a real orphan by construction: a dropped
        // slot is still absent after the barrier (no publish can produce a bit
        // nothing ever set), so it still gets re-driven — as does an
        // inconclusive one. Only slots PROVEN present — i.e. never lost — are
        // skipped. Prod v1.1.46 classified 2/2 apparent orphans as publish lag.
        //
        // Barrier safety: `force_publish_blocking` runs on the WAL-reader thread
        // (this verifier), sends a FlushCommand to the flush thread and waits on
        // a bounded channel — no lock is held across the wait, it targets a
        // different thread, and the verifier processes orphans sequentially, so
        // it cannot self-deadlock or stack. It returns as soon as the publish
        // lands (the cap is a bound, not a cost) and only the rare (~0.3%)
        // apparent-orphan path pays it.
        let diag_now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let publish_barrier = engine.force_publish_blocking(std::time::Duration::from_millis(
            engine.config().activation_verify.publish_barrier_ms,
        ));
        let reread_present = match engine.execute_query(&query) {
            Ok(r) => r.ids.iter().any(|&id| id == slot as i64),
            // A failed re-read is UNKNOWN, not absent. Collapsing it to `false`
            // would classify it NonApply behind a completed barrier and bump
            // `redriven_total` — an unknown counted as a confirmed drop, which
            // is the exact conflation this verdict split exists to remove. The
            // first query (above) already treats its own Err as unknown and
            // requeues; do the same here rather than let a transient read
            // failure page an operator with a phantom drop.
            Err(_) => {
                requeue.push(slot); // transient read failure — re-check next pass
                continue;
            }
        };
        let recent_removes =
            engine.recent_removes_for_slot(slot, diag_now_ms.saturating_sub(5_000));
        let verdict = OrphanVerdict::classify(
            publish_barrier,
            reread_present,
            !recent_removes.is_empty(),
        );
        tracing::warn!(
            target: "activation",
            "verify-diag: slot {slot} {verify_field}={pid} barrier_ok={publish_barrier} reread_present={reread_present} class=\"{}\" recent_removes={recent_removes:?}",
            verdict.description(),
        );
        if !verdict.redrives() {
            publish_lag += 1;
            continue;
        }
        // Not proven present: re-drive the full doc via the activation replay
        // path — confirmed orphan or inconclusive alike, since re-driving is
        // idempotent and only the counter routing differs.
        // Sent through the mutation channel (the flush thread is the sole
        // ArcSwap writer — never mutate staging directly here).
        let document = crate::mutation::Document {
            fields: doc.fields.clone(),
        };
        let ops = crate::mutation::diff_document(
            slot,
            None,
            &document,
            engine.config(),
            false,
            &registry,
        );
        for op in ops {
            let _ = engine.mutation_sender().send(op);
        }
        // Doc/bitmap coherence: diff_document flips the exists_boolean shadow
        // (e.g. isPublished) BITMAP, but the stored doc's shadow may be stale
        // (post the dump fix, deferred-seeded docs store it false). Write the
        // derived shadow into the doc to match the bitmap — the same coherence
        // the normal activation replay does — or GET /documents would serve the
        // wrong published state until a later op (the #291-era stale-doc class).
        let derived = crate::mutation::derive_exists_boolean_shadows(
            engine.config(),
            &document.fields,
        );
        if !derived.is_empty() {
            let mut dw = DocWriter::new(engine.docstore_arc());
            for (name, val) in &derived {
                dw.write_set(slot, name, &serde_json::json!(val));
            }
            dw.flush();
        }
        engine.evict_doc_cache(slot);
        tracing::warn!(
            target: "activation",
            "verify: slot {slot} activated but ABSENT from {verify_field} {pid} — re-drove from stored doc"
        );
        if verdict.is_confirmed_drop() {
            redriven += 1;
        } else {
            inconclusive += 1;
        }
    }
    if !requeue.is_empty() {
        engine.requeue_activation_verify(&requeue);
    }
    #[cfg(feature = "server")]
    if let Some(bridge) = engine.metrics_bridge_handle() {
        // Ring depth, sampled EVERY pass — including passes that drain nothing,
        // which is the case worth seeing. Unconditional on purpose: every other
        // signal here is a counter that reports a strand by going quiet, and
        // quiet is what a clean result looks like too. Depth is the only one
        // where a strand is a rising line rather than an absence, so it must not
        // be gated behind "did anything happen".
        bridge
            .activation_verify_pending
            .with_label_values(&[&bridge.index_name])
            .set(engine.activation_verify_len() as i64);
        bridge
            .activation_verify_ready
            .with_label_values(&[&bridge.index_name])
            .set(engine.activation_verify_ready_len() as i64);
        if checked > 0 {
            bridge
                .activation_verify_checked_total
                .with_label_values(&[&bridge.index_name])
                .inc_by(checked as u64);
        }
        if redriven > 0 {
            bridge
                .activation_verify_redriven_total
                .with_label_values(&[&bridge.index_name])
                .inc_by(redriven as u64);
        }
        if publish_lag > 0 {
            bridge
                .activation_verify_publish_lag_total
                .with_label_values(&[&bridge.index_name])
                .inc_by(publish_lag as u64);
        }
        if inconclusive > 0 {
            bridge
                .activation_verify_inconclusive_total
                .with_label_values(&[&bridge.index_name])
                .inc_by(inconclusive as u64);
        }
    }
    VerifyOutcome {
        checked,
        redriven,
        publish_lag,
        inconclusive,
    }
}

/// Field-name label for the zero-match fan-out counter. Low-cardinality by
/// construction: the field NAME of the clause (never the value), recursing
/// into the first leaf of compound clauses.
#[cfg(feature = "server")]
fn zero_match_field_label(clause: &crate::query::FilterClause) -> String {
    use crate::query::FilterClause as FC;
    match clause {
        FC::Eq(f, _)
        | FC::NotEq(f, _)
        | FC::In(f, _)
        | FC::NotIn(f, _)
        | FC::Gt(f, _)
        | FC::Lt(f, _)
        | FC::Gte(f, _)
        | FC::Lte(f, _)
        | FC::IsNull(f)
        | FC::IsNotNull(f) => f.clone(),
        FC::BucketBitmap { field, .. } => field.clone(),
        FC::Not(inner) => zero_match_field_label(inner),
        FC::And(cs) | FC::Or(cs) => cs
            .first()
            .map(zero_match_field_label)
            .unwrap_or_else(|| "none".to_string()),
    }
}

/// Whether a stored doc satisfies a fan-out's filter predicate, evaluated by
/// reading the doc's stored field values. Used to reach deferred slots, which
/// carry no bitmap bits and so can't be found by the bitmap query.
///
/// `parse_filter_from_query_str` only ever produces `Eq` / `In` clauses, so
/// those are the only shapes handled; any other clause conservatively fails to
/// match (the deferred reach is best-effort, never a correctness-critical
/// universe — a missed match degrades to the pre-fix behavior for that slot).
fn doc_matches_filters(doc: &crate::shard_store_doc::StoredDoc, filters: &[FilterClause]) -> bool {
    filters.iter().all(|clause| match clause {
        FilterClause::Eq(field, val) => doc_field_has_value(doc, field, val),
        FilterClause::In(field, vals) => vals.iter().any(|v| doc_field_has_value(doc, field, v)),
        _ => false,
    })
}

/// Whether `doc.fields[field]` contains `val` (single equality or multi-value
/// membership).
fn doc_field_has_value(
    doc: &crate::shard_store_doc::StoredDoc,
    field: &str,
    val: &QValue,
) -> bool {
    match doc.fields.get(field) {
        Some(crate::mutation::FieldValue::Single(v)) => v == val,
        Some(crate::mutation::FieldValue::Multi(vs)) => vs.iter().any(|v| v == val),
        None => false,
    }
}

/// Apply a publish/schedule-shaped fan-out to the DEFERRED slots it targets —
/// the ones a queryOpSet's bitmap query can't see because deferred slots have
/// no filter bits set. For each deferred slot whose stored doc matches the
/// fan-out's filter, persist the fan-out's field writes to the docstore (so
/// activation replay reconstructs correct bitmaps) and re-drive its deferred
/// schedule, exactly mirroring the direct-op deferred branch in
/// `apply_ops_batch`:
///   - a future timestamp on the source field → reschedule to it,
///   - a now/past timestamp → reschedule to it (activates next flush cycle),
///   - source field cleared (Remove, no Set) → activate immediately as a draft.
///
/// Returns the number of deferred slots rescheduled/activated. Self-contained:
/// flushes and evicts the doc cache for the slots it touched, because the
/// caller's coherence block only covers bitmap-matched slots and several of the
/// caller's exit paths (cap exceeded, zero bitmap matches) return before it.
fn apply_fanout_to_deferred_slots<S: BitmapSink>(
    sink: &mut S,
    meta: &FieldMeta,
    engine: &ConcurrentEngine,
    filters: &[FilterClause],
    ops: &[Op],
    mut doc_writer: Option<&mut DocWriter>,
) -> usize {
    // Gate: only publishes touch the deferred schedule. A fan-out that doesn't
    // write the deferred source field can't change a deferred slot's schedule,
    // so skip the deferred-map scan + per-candidate docstore reads entirely.
    let da_field = match meta.deferred_alive_field.as_ref() {
        Some((f, _)) => f.as_str(),
        None => return 0,
    };
    let modifies_schedule = ops.iter().any(|op| match op {
        Op::Set { field, .. } | Op::Remove { field, .. } => field == da_field,
        _ => false,
    });
    if !modifies_schedule {
        return 0;
    }

    let candidates = engine.deferred_slots();
    if candidates.is_empty() {
        return 0;
    }
    // Cost signal: this pass reads one doc per candidate. The deferred map is
    // small in healthy steady state, but prod once accumulated ~49.7k deferred
    // slots (the bug this fix repairs) — warn so a regrown backlog + high
    // publish rate is visible before it becomes a WAL-reader CPU problem.
    if candidates.len() > DEFERRED_REACH_WARN_THRESHOLD {
        tracing::warn!(
            target: "ops_processor",
            "deferred-reach: scanning {} deferred slots for publish fan-out (> {} threshold) — \
             deferred backlog may be growing",
            candidates.len(),
            DEFERRED_REACH_WARN_THRESHOLD,
        );
    }
    #[cfg(feature = "server")]
    let metrics = engine.metrics_bridge_handle();
    #[cfg(feature = "server")]
    if let Some(ref bridge) = metrics {
        bridge
            .deferred_fanout_scanned_total
            .with_label_values(&[&bridge.index_name])
            .inc_by(candidates.len() as u64);
    }
    // A now/past Set reschedules to that instant (activates next flush cycle);
    // a Remove-only (source cleared) activates immediately as a plain draft.
    let new_at = get_deferred_timestamp(meta, ops);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut touched: Vec<u32> = Vec::new();
    for slot in candidates {
        // A slot that is already alive can't be deferred; if a stale snapshot
        // still lists it (or a re-defer landed and reverted within the window)
        // skip it — the bitmap-matched path above already handles alive slots,
        // and applying here would double-count and redundantly reschedule.
        if engine.is_slot_alive(slot) {
            continue;
        }
        let doc = match engine.get_document(slot) {
            Ok(Some(d)) => d,
            _ => continue,
        };
        if !doc_matches_filters(&doc, filters) {
            continue;
        }
        // Persist the fan-out field writes so activation replay
        // (diff_document over the stored doc) rebuilds every bitmap. Shadow
        // doc writes are intentionally skipped — the deferred doc must not
        // carry a premature isPublished=true; the shadow derives correctly
        // from the source field at activation time.
        if let Some(ref mut dw) = doc_writer {
            for op in ops {
                match op {
                    Op::Set { field, value } => dw.write_set(slot, field, value),
                    Op::Add { field, value } => dw.write_add(slot, field, value),
                    Op::Remove { field, value } => dw.write_remove(slot, field, value),
                    _ => {}
                }
            }
        }
        sink.deferred_alive(slot, new_at.unwrap_or(now));
        tracing::info!(
            target: "ops_processor",
            "queryOpSet reached deferred slot {slot} — rescheduled to {}",
            new_at.unwrap_or(now),
        );
        touched.push(slot);
    }

    if !touched.is_empty() {
        // Flush the buffered deferred doc writes, then evict so a GET between
        // flush and eviction can't repopulate the cache from stale disk state
        // (same ordering rationale as the alive-slot coherence block below).
        if let Some(ref mut dw) = doc_writer {
            dw.flush();
        }
        for &slot in &touched {
            engine.evict_doc_cache(slot);
        }
    }
    #[cfg(feature = "server")]
    if let Some(ref bridge) = metrics {
        bridge
            .deferred_fanout_reached_total
            .with_label_values(&[&bridge.index_name])
            .inc_by(touched.len() as u64);
    }
    touched.len()
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
    // Field label for the zero-match counter, captured before `filters` moves
    // into the query. Metric labels must be low-cardinality: field NAME only.
    #[cfg(feature = "server")]
    let zero_match_field: String = filters
        .first()
        .map(zero_match_field_label)
        .unwrap_or_else(|| "none".to_string());
    // Keep a copy for the deferred-slot reach below — `filters` moves into the
    // query, but deferred slots are matched against the same predicate by
    // reading their stored docs (they have no bitmap bits to query against).
    let deferred_filters = filters.clone();
    let query = BitdexQuery {
        filters,
        sort: None,
        limit: usize::MAX,
        offset: None,
        cursor: None,
        skip_cache: true,
    };
    // Same-batch fan-out visibility barrier is now hoisted to
    // `apply_ops_batch` and runs ONCE per batch, just before the first
    // QueryOpSet entry (which is sorted to the end of the batch). That
    // means by the time we reach this fn, the engine snapshot already
    // reflects every preceding non-fan-out write in the batch. See
    // `apply_ops_batch` for rationale + timeout handling.

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

    // --- Deferred-slot fan-out reach (reschedule-drop fix, 2026-07-14) ---
    // The query above resolves fan-out targets against the LIVE bitmaps.
    // Deferred (scheduled, not-yet-alive) slots have NO filter bits set — the
    // deferred insert / re-defer path skips every bitmap — so a `postId eq P`
    // fan-out CANNOT see them. When a scheduled Post is published EARLY
    // (publishedAt moved to now), the Post→Image fan-out that must reschedule
    // or activate its deferred images matches zero of them and the reschedule
    // is silently lost: the images stay deferred at the stale future time,
    // missing from feeds for days (prod specimens 28898846, 29761599).
    //
    // Reach those slots directly through the deferred-alive map. Gated on a
    // publish/schedule-shaped fan-out (a Set/Remove on the deferred source
    // field) so the deferred-map scan + per-candidate docstore read is paid
    // only by publishes — tag/metric fan-outs skip it entirely. Runs before
    // the cap and empty-match early returns because an all-deferred post
    // matches zero bitmap slots and would otherwise return here having done
    // nothing. Mirrors the direct-op deferred branch in `apply_ops_batch`.
    let deferred_applied = apply_fanout_to_deferred_slots(
        sink,
        meta,
        engine,
        &deferred_filters,
        ops,
        doc_writer.as_deref_mut(),
    );

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
        return Ok(deferred_applied);
    }

    if slot_ids.is_empty() {
        // Zero BITMAP matches. Indistinguishable here from a legitimately
        // empty target (post with no images yet), so it must not fail — but
        // it is also the exact signature of the silent no-op class (specimen
        // 136063341: a fan-out that matched nothing on a freshly-dumped pod
        // while PG had matching rows; suspected per-value lazy-load
        // shadowing sync-created diffs — see FOLLOWUP.md). Count it, labeled
        // by filter field, and log at info so a post-boot spike is
        // attributable to a query shape without extra instrumentation.
        //
        // But NOT when the deferred-reach pass above already handled slots:
        // an all-deferred post (every image scheduled) matches zero bitmap
        // slots yet is NOT a no-op — counting it here would falsely inflate
        // the very counter used to detect the drop this fix repairs.
        if deferred_applied == 0 {
            tracing::info!(
                target: "ops_processor",
                "queryOpSet '{}' matched 0 slots — applied nothing",
                query_str,
            );
            #[cfg(feature = "server")]
            if let Some(ref bridge) = metrics {
                bridge
                    .query_op_set_zero_match_total
                    .with_label_values(&[&bridge.index_name, &zero_match_field])
                    .inc();
            }
        }
        return Ok(deferred_applied);
    }
    let dictionaries = Some(engine.dictionaries());
    // Pre-compute sort_values / old_sort_values from the shared ops vector
    // once for the fan-out — they're identical across every matched slot, so
    // there's no benefit to rebuilding them per iteration. The per-slot
    // computed-sort recompute below uses these plus a per-slot stored-doc
    // fallback to materialize the full new value.
    let mut fanout_sort_values: HashMap<&str, u32> = HashMap::new();
    let mut fanout_old_sort_values: HashMap<&str, u32> = HashMap::new();
    if !meta.computed_deps.is_empty() {
        for op in ops {
            match op {
                Op::Set { field, value } => {
                    if meta.has_computed_deps(field)
                        || meta.sort_fields.contains_key(field.as_str())
                    {
                        let qval = json_to_qvalue(value);
                        if let Some(sv) = value_to_sort_u32(&qval) {
                            fanout_sort_values.insert(field.as_str(), sv);
                        }
                    }
                }
                Op::Remove { field, value } => {
                    if meta.has_computed_deps(field)
                        || meta.sort_fields.contains_key(field.as_str())
                    {
                        let qval = json_to_qvalue(value);
                        if let Some(sv) = value_to_sort_u32(&qval) {
                            fanout_old_sort_values.insert(field.as_str(), sv);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    // Per-slot deferred-alive gate for fan-out (e.g. Post→Image scheduled posts).
    // The top-level apply_ops_batch deferred check fires on creates_slot for the
    // entity_id, which for a fan-out is the source row (Post.id), not the matched
    // image slot. Without this check, fan-out from a future-scheduled Post
    // immediately writes publishedAt sort layers and flips the isPublished
    // shadow on already-alive image slots, leaking scheduled posts into queries.
    //
    // Detection mirrors check_deferred_alive: if any nested Set targets the
    // configured deferred_alive source field with a value > now, route every
    // matched slot through the deferred path (write doc, skip bitmap, schedule
    // activation). activate_due replays from the docstore at activation time —
    // the doc already carries the future field values from the writes below.
    let deferred_at = check_deferred_alive_secs(meta, ops);
    // Apply nested ops to each matching slot. Seed with the deferred-slot reach
    // count so the caller's applied/skipped accounting includes rescheduled
    // deferred slots (they carry no bitmap bits, so they never appear in
    // `slot_ids`).
    let mut applied = deferred_applied;
    for &slot_id in slot_ids {
        if slot_id < 0 || slot_id > u32::MAX as i64 {
            continue;
        }
        let slot = slot_id as u32;
        if let Some(activate_at) = deferred_at {
            // Deferred fan-out: persist source field values to the docstore so
            // activate_due's diff_document replay reconstructs the correct
            // bitmap state, but skip every bitmap mutation AND skip the shadow
            // doc writes. write_shadow_target_docs would prematurely flip
            // shadow targets like isPublished=true in the doc — making
            // GET /documents/{slot} disagree with the deferred bitmap state
            // until activation. The shadow gets derived correctly from the
            // source field at activation time via diff_document.
            if let Some(ref mut dw) = doc_writer {
                for op in ops {
                    match op {
                        Op::Set { field, value } => {
                            dw.write_set(slot, field, value);
                        }
                        Op::Remove { field, value } => {
                            dw.write_remove(slot, field, value);
                        }
                        Op::Add { field, value } => {
                            dw.write_add(slot, field, value);
                        }
                        _ => {}
                    }
                }
            }
            sink.deferred_alive(slot, activate_at);
            applied += 1;
            continue;
        }
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
        // Per-slot computed-sort recompute. Without this, fan-out updates to
        // a source field (e.g. publishedAt on Post → Image fan-out) leave the
        // computed target (sortAt = GREATEST(existedAt, publishedAt)) stale
        // for every matched slot. Stale sortAt then cascades through:
        //   - mutated_sort_slots[sortAt] never lists the slot, so the flush
        //     thread's time-bucket maintenance skips it (slot stuck in old
        //     bucket bitmap),
        //   - the unified cache's invalidation skips entries that key on the
        //     unchanged-from-its-perspective sortAt,
        //   - BoundStore live maintenance never sees the slot, so paginated
        //     cursors anchored to sortAt return wrong results.
        // The full-overwrite shape inside the helper guarantees the bitmap
        // state equals the freshly-computed value bit-exact, regardless of
        // any prior corruption or eager-load timing.
        recompute_computed_sorts_for_slot(
            sink,
            meta,
            Some(engine),
            slot,
            &fanout_sort_values,
            &fanout_old_sort_values,
            doc_writer.as_deref_mut(),
        );
        applied += 1;
    }

    // Doc-cache coherence for fan-outs (stakeout 2026-07-08, specimen post
    // 29660803): the WAL-reader loop evicts the DocCache by entry.entity_id,
    // which for a queryOpSet is the SOURCE entity (e.g. Post.id) — the
    // MATCHED slots' cached docs were never evicted, so GET /documents kept
    // serving the stale pre-fan-out doc (publishedAt=0 / isPublished=false)
    // for ~20+ minutes (LRU) on ~a third of recently-published posts' images,
    // while bitmaps/search were already correct. Flush this fan-out's
    // buffered doc writes FIRST, then evict exactly the matched slots —
    // evict-before-flush would let a read between the two repopulate the
    // cache from the still-old disk state. Per-fan-out eviction keeps the
    // working set bounded (≤ max_fanout slots) instead of accumulating
    // across the batch.
    if applied > 0 {
        if let Some(ref mut dw) = doc_writer {
            dw.flush();
        }
        for &slot_id in slot_ids {
            if (0..=u32::MAX as i64).contains(&slot_id) {
                engine.evict_doc_cache(slot_id as u32);
            }
        }
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
        deferred_cancels: Vec<u32>,
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
                deferred_cancels: Vec::new(),
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
        fn deferred_cancel(&mut self, slot: u32) {
            self.deferred_cancels.push(slot);
        }
        fn flush(&mut self) -> crate::error::Result<()> {
            Ok(())
        }
    }
    /// Regression (nuke 2026-07-13, boot-replay skip-watermark): a bulk load's
    /// LATER phases (metrics, dumped at a later wall-clock cut) push
    /// slot_counter past slots whose insert ops exist only in the replay
    /// stream — the images CSV was snapshotted BEFORE those images existed.
    /// The old "!alive && below high-water ⇒ stale, skip" heuristic silently
    /// dropped those inserts (bitdex-1: 9,614 skipped ops, 285 posts missing
    /// images). A never-inserted slot has NO stored doc, while a deleted slot
    /// keeps its doc until autovac — so doc-absence must auto-promote the op
    /// to an insert, and doc-presence must keep the stale-op skip.
    #[test]
    fn test_below_hwm_insert_op_auto_promotes_when_no_doc() {
        clear_confirmed_deleted_memo();
        let dir = tempfile::TempDir::new().unwrap();
        let docstore_path = dir.path().join("docs");
        let mut config = test_config();
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        let mut engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        let meta = FieldMeta::from_config(engine.config());

        // Raise the high-water mark: insert slot 100 the normal way.
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: 100,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(1) }],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        assert_eq!((applied, errors), (1, 0));
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        wait_for_alive_slot(&engine, 100, 5_000);
        assert!(engine.slot_counter() > 50, "slot 100 must raise the high-water mark");

        // Replay-shaped insert: slot 50 is BELOW the high-water mark, not
        // alive, creates_slot=false (triggers can't mark inserts), and has no
        // stored doc. Must auto-promote and insert, not skip.
        let mut sink2 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let mut batch2 = vec![EntityOps {
            entity_id: 50,
            creates_slot: false,
            ops: vec![Op::Set { field: "postId".into(), value: json!(2) }],
        }];
        let (applied2, skipped2, errors2) =
            apply_ops_batch(&mut sink2, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
        assert_eq!(
            (applied2, skipped2, errors2),
            (1, 0, 0),
            "below-HWM insert op with no stored doc must auto-promote, not skip"
        );
        crate::ingester::BitmapSink::flush(&mut sink2).unwrap();
        dw2.flush();
        wait_for_alive_slot(&engine, 50, 5_000);
        assert!(
            engine.docstore_arc().read().get(50).unwrap().is_some(),
            "auto-promoted insert must write the doc"
        );

        // Deleted-slot case must STILL skip: delete slot 100 (doc stays on
        // disk until autovac), then send it a stale Set.
        let mut sink3 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut batch3 = vec![EntityOps {
            entity_id: 100,
            creates_slot: false,
            ops: vec![Op::Delete],
        }];
        let (applied3, _, errors3) =
            apply_ops_batch(&mut sink3, &meta, &mut batch3, Some(&engine), None);
        assert_eq!((applied3, errors3), (1, 0));
        crate::ingester::BitmapSink::flush(&mut sink3).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5_000);
        while engine.is_slot_alive(100) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(!engine.is_slot_alive(100), "slot 100 must be deleted");
        assert!(
            engine.docstore_arc().read().get(100).unwrap().is_some(),
            "precondition: deleted slot keeps its stored doc (autovac not run)"
        );

        let mut sink4 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut batch4 = vec![EntityOps {
            entity_id: 100,
            creates_slot: false,
            ops: vec![Op::Set { field: "postId".into(), value: json!(3) }],
        }];
        let (applied4, skipped4, _) =
            apply_ops_batch(&mut sink4, &meta, &mut batch4, Some(&engine), None);
        assert_eq!(
            (applied4, skipped4),
            (0, 1),
            "stale op for a deleted slot (doc present) must still skip"
        );
        assert!(!engine.is_slot_alive(100), "deleted slot must stay dead");
        assert!(
            is_confirmed_deleted(100),
            "deleted-slot probe must be memoized to bound repeat docstore reads"
        );

        // Memoized repeat: same stale op again must skip via the memo
        // (behaviorally identical; pins the fast path exists).
        let mut sink5 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut batch5 = vec![EntityOps {
            entity_id: 100,
            creates_slot: false,
            ops: vec![Op::Set { field: "postId".into(), value: json!(4) }],
        }];
        let (applied5, skipped5, _) =
            apply_ops_batch(&mut sink5, &meta, &mut batch5, Some(&engine), None);
        assert_eq!((applied5, skipped5), (0, 1), "memoized deleted slot must skip");

        // Beyond-HWM fast path must promote WITHOUT consulting the docstore
        // memo/probe (pins the `slot >= slot_counter` branch on its own —
        // review #310 F4: the collapsed concurrent_engine test would pass
        // via the doc probe even if this branch were deleted).
        let beyond = engine.slot_counter() + 500;
        let mut sink6 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw6 = DocWriter::new(engine.docstore_arc());
        let mut batch6 = vec![EntityOps {
            entity_id: beyond as i64,
            creates_slot: false,
            ops: vec![Op::Set { field: "postId".into(), value: json!(5) }],
        }];
        let (applied6, skipped6, _) =
            apply_ops_batch(&mut sink6, &meta, &mut batch6, Some(&engine), Some(&mut dw6));
        assert_eq!(
            (applied6, skipped6),
            (1, 0),
            "beyond-HWM slot must auto-promote via the high-water branch"
        );
        clear_confirmed_deleted_memo();
        engine.shutdown();
    }

    /// Regression (stakeout 2026-07-08, specimen post 29660803): a fan-out
    /// mutates the MATCHED slots' docs, but the WAL-reader loop only evicts
    /// the DocCache for the SOURCE entity_id (the Post) — so a previously
    /// cached image doc kept serving the stale pre-fan-out state
    /// (publishedAt=0/isPublished=false) for the LRU lifetime while
    /// bitmaps/search were already correct. apply_query_op_set must flush its
    /// doc writes and evict the matched slots itself.
    #[test]
    fn test_fanout_evicts_doc_cache_for_matched_slots() {
        let dir = tempfile::TempDir::new().unwrap();
        let bitmap_path = dir.path().join("bitmaps"); // enables the DocCache
        let docstore_path = dir.path().join("docs");

        let mut config = test_config();
        config.storage.bitmap_path = Some(bitmap_path);
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        let engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let image_slot: u32 = 7;
        let post_id: i64 = 100;
        let t_pub: i64 = 1_783_522_973;

        // Insert the image (draft: no publishedAt), server-shaped.
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: image_slot as i64,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(post_id) }],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        assert_eq!((applied, errors), (1, 0));
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        wait_for_alive_slot(&engine, image_slot, 5_000);

        // Cache the stale doc (cache-on-read), exactly like GET /documents.
        let stale = engine.get_document(image_slot).unwrap().unwrap();
        assert!(stale.fields.get("publishedAt").is_none());

        // Post publishes → fan-out. Mimic the WAL-reader loop faithfully,
        // including its (insufficient) source-entity eviction.
        let mut sink2 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let mut batch2 = vec![EntityOps {
            entity_id: post_id,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {post_id}")),
                ops: vec![Op::Set { field: "publishedAt".into(), value: json!(t_pub) }],
            }],
        }];
        let (applied2, _, errors2) =
            apply_ops_batch(&mut sink2, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
        assert_eq!((applied2, errors2), (1, 0), "fan-out must match and apply");
        crate::ingester::BitmapSink::flush(&mut sink2).unwrap();
        dw2.flush();
        engine.evict_doc_cache(post_id as u32); // the old loop's only eviction

        // The very next read must see the fan-out's write — not the cached
        // stale doc. (Fails before the fix: cache still holds the draft doc.)
        let fresh = engine.get_document(image_slot).unwrap().unwrap();
        assert_eq!(
            fresh.fields.get("publishedAt"),
            Some(&crate::mutation::FieldValue::Single(crate::types::Value::Integer(t_pub))),
            "fan-out-mutated slot must serve the updated doc immediately"
        );
    }

    /// Poll until the slot's alive bit is published.
    fn wait_for_alive_slot(engine: &ConcurrentEngine, slot: u32, timeout_ms: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        while std::time::Instant::now() < deadline {
            if engine.is_slot_alive(slot) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        panic!("slot {slot} never became alive within {timeout_ms}ms");
    }

    /// PROD REPRO (2026-07-10, layer-zero hunt): an ALIVE draft image re-deferred
    /// by a future-publishedAt fan-out, then activated at Tf. The re-defer path
    /// (mutation.rs [2.5]) clears every filter/sort bit from the old doc;
    /// activation replay (diff_document(None, stored_doc)) must restore ALL of
    /// them. Prod victims (23 of Day's top-200, e.g. slot 136273661 / post
    /// 29693453, fan-out [remove publishedAt(null), set publishedAt(future)])
    /// ended with filters restored but sortAt/publishedAt/existedAt layers = 0.
    #[test]
    fn test_redefer_then_activation_restores_sort_layers() {
        let dir = tempfile::TempDir::new().unwrap();
        let docstore_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");

        let mut config = test_config();
        config.storage.bitmap_path = Some(bitmap_path);
        // Keep the merge thread from persisting snapshots before the restart
        // phase: the opslog must be the ONLY durability path — exactly the
        // prod window where activation replay ops lived only in (dead)
        // legacy per-layer appends. 45s > test runtime-to-drop; NOT huge,
        // because engine shutdown joins the merge thread mid-sleep (a 1h
        // interval hangs drop for an hour).
        config.merge_interval_ms = 45_000;
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "sortAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: Some(crate::config::ComputedField {
                op: crate::config::ComputedOp::Greatest,
                source_fields: vec!["existedAt".into(), "publishedAt".into()],
            }),
        });
        config.deferred_alive = Some(crate::config::DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
            sweep_interval_secs: 0,
            sweep_limit: 20_000,
        });
        let engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let image_slot: u32 = 7;
        let post_id: i64 = 100;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let t_existed = now - 3600;
        let t_pub_future = now + 3; // activates ~3s in

        // 1) Draft image insert (server-shaped): alive, existedAt set, no pub.
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: image_slot as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(post_id) },
                Op::Set { field: "nsfwLevel".into(), value: json!(1) },
                Op::Set { field: "existedAt".into(), value: json!(t_existed) },
            ],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        assert_eq!((applied, errors), (1, 0));
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        wait_for_alive_slot(&engine, image_slot, 5_000);
        assert_eq!(
            sort_layer_value(&engine, "sortAt", image_slot),
            Some(t_existed as u32),
            "draft insert must set sortAt layers to existedAt"
        );

        // 2) Post schedules publish in the future → fan-out re-defers the
        //    ALIVE image (exact captured prod shape: remove(null) + set(T)).
        let mut sink2 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let mut batch2 = vec![EntityOps {
            entity_id: post_id,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {post_id}")),
                ops: vec![
                    Op::Remove { field: "publishedAt".into(), value: serde_json::Value::Null },
                    Op::Set { field: "publishedAt".into(), value: json!(t_pub_future) },
                ],
            }],
        }];
        let (applied2, _, errors2) =
            apply_ops_batch(&mut sink2, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
        assert_eq!((applied2, errors2), (1, 0), "fan-out must match the image");
        crate::ingester::BitmapSink::flush(&mut sink2).unwrap();
        dw2.flush();

        // 3) Wait until PAST Tf, then for the activation replay to land: the
        //    publishedAt layer flipping to T is the replay's own write, so
        //    polling it (not sortAt, which holds existedAt bits from the
        //    insert) is the correct convergence signal.
        std::thread::sleep(std::time::Duration::from_secs((t_pub_future - now + 2) as u64));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if sort_layer_value(&engine, "publishedAt", image_slot).unwrap_or(0) != 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(engine.is_slot_alive(image_slot), "slot must be alive after Tf");

        // 4) Post-activation: filters AND all sort layers must be restored.
        let expected_sort = t_pub_future.max(t_existed) as u32;
        assert_eq!(
            sort_layer_value(&engine, "publishedAt", image_slot),
            Some(t_pub_future as u32),
            "activation replay must restore publishedAt layers"
        );
        assert_eq!(
            sort_layer_value(&engine, "existedAt", image_slot),
            Some(t_existed as u32),
            "activation replay must restore existedAt layers"
        );
        assert_eq!(
            sort_layer_value(&engine, "sortAt", image_slot),
            Some(expected_sort),
            "activation replay must restore computed sortAt layers (prod victims read 0)"
        );

        // 5) RESTART: activation-replay ops have NO WAL entry — the shard
        //    opslog is their ONLY durability. Pre-fix, sort ops were appended
        //    to legacy per-layer files that `load_sort_layers` never reads
        //    when the packed shard exists, so every pod restart permanently
        //    zeroed the layers of everything activated since the last merge
        //    persist (prod 2026-07-10: whole activation cohorts, 8/8 at
        //    Tf=11:26Z; post-restart cohorts 5/5 clean).
        let config2 = engine.config().clone();
        drop(engine);
        let engine2 = ConcurrentEngine::new_with_path(config2, &docstore_path).unwrap();
        assert!(engine2.is_slot_alive(image_slot), "slot alive after restart");
        assert_eq!(
            sort_layer_value(&engine2, "publishedAt", image_slot),
            Some(t_pub_future as u32),
            "publishedAt layers must survive restart (pre-fix: zeroed)"
        );
        assert_eq!(
            sort_layer_value(&engine2, "existedAt", image_slot),
            Some(t_existed as u32),
            "existedAt layers must survive restart (pre-fix: zeroed)"
        );
        assert_eq!(
            sort_layer_value(&engine2, "sortAt", image_slot),
            Some(expected_sort),
            "computed sortAt layers must survive restart (pre-fix: zeroed)"
        );
    }

    /// PROD REPRO (2026-07-14, reschedule-drop): an image inserted DEFERRED
    /// (scheduled for a future publishedAt, so it carries NO bitmap bits) is
    /// then published EARLY via a Post→Image fan-out. The fan-out resolves its
    /// targets against the live bitmaps by `postId eq P`, but the deferred
    /// image has no postId bit, so it matches ZERO slots — the reschedule is
    /// silently dropped and the image stays deferred at the stale future time
    /// (prod specimens 28898846, 29761599: images missing from feeds for days).
    /// The fix reaches deferred slots through the deferred-alive map + a
    /// docstore match. Pre-fix, the fan-out no-ops and the slot never activates,
    /// so `wait_for_alive_slot` times out.
    #[test]
    fn test_deferred_slot_publish_fanout_reaches_and_activates() {
        let dir = tempfile::TempDir::new().unwrap();
        let docstore_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");

        let mut config = test_config();
        config.storage.bitmap_path = Some(bitmap_path);
        config.merge_interval_ms = 45_000;
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "sortAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: Some(crate::config::ComputedField {
                op: crate::config::ComputedOp::Greatest,
                source_fields: vec!["existedAt".into(), "publishedAt".into()],
            }),
        });
        config.deferred_alive = Some(crate::config::DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
            sweep_interval_secs: 0,
            sweep_limit: 20_000,
        });
        let engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let image_slot: u32 = 7;
        let post_id: i64 = 100;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let t_existed = now - 3600;
        let t_pub_future = now + 3600; // scheduled an hour out — won't fire on its own

        // 1) Image inserted SCHEDULED (creates_slot + future publishedAt) →
        //    deferred: doc written, NO bitmaps set (postId bit absent).
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: image_slot as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(post_id) },
                Op::Set { field: "nsfwLevel".into(), value: json!(1) },
                Op::Set { field: "existedAt".into(), value: json!(t_existed) },
                Op::Set { field: "publishedAt".into(), value: json!(t_pub_future) },
            ],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        assert_eq!((applied, errors), (1, 0));
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        engine.force_publish_blocking(std::time::Duration::from_secs(5));

        // Deferred, not alive, and invisible to a postId query (no bitmap bit).
        assert!(!engine.is_slot_alive(image_slot), "scheduled image must not be alive yet");
        assert!(engine.is_slot_deferred(image_slot), "scheduled image must be deferred");
        assert_eq!(
            sort_layer_value(&engine, "publishedAt", image_slot),
            None,
            "deferred image must not be findable by postId (no bitmap bit)"
        );

        // 2) User publishes EARLY: Post→Image fan-out moves publishedAt to the
        //    past. Captured prod shape: remove(null) + set(now-ish).
        let t_pub_now = now - 5; // past → activates next flush cycle
        let mut sink2 = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let mut batch2 = vec![EntityOps {
            entity_id: post_id,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {post_id}")),
                ops: vec![
                    Op::Remove { field: "publishedAt".into(), value: serde_json::Value::Null },
                    Op::Set { field: "publishedAt".into(), value: json!(t_pub_now) },
                ],
            }],
        }];
        let (applied2, _, errors2) =
            apply_ops_batch(&mut sink2, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
        assert_eq!(errors2, 0, "fan-out must not error");
        assert_eq!(
            applied2, 1,
            "fan-out must reach the deferred image (pre-fix: 0 — bitmap query can't see it)"
        );
        crate::ingester::BitmapSink::flush(&mut sink2).unwrap();
        dw2.flush();

        // 3) Rescheduled to the past → activates on the next flush cycle.
        wait_for_alive_slot(&engine, image_slot, 10_000);

        // 4) Post-activation: findable by postId, publishedAt layer = the early
        //    time, computed sortAt = GREATEST(existedAt, publishedAt).
        assert_eq!(
            sort_layer_value(&engine, "publishedAt", image_slot),
            Some(t_pub_now as u32),
            "activation must set publishedAt layer to the early-publish time"
        );
        assert_eq!(
            sort_layer_value(&engine, "sortAt", image_slot),
            Some(t_pub_now.max(t_existed) as u32),
            "activation must recompute sortAt"
        );
    }

    /// Engine for the deferred-reach tests: `test_config` fields + postId
    /// filter, publishedAt sort (also the deferred source), computed sortAt.
    /// post_id is fixed at 100 so `sort_layer_value` can query it.
    fn deferred_reach_engine(dir: &tempfile::TempDir) -> ConcurrentEngine {
        let docstore_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");
        let mut config = test_config();
        config.storage.bitmap_path = Some(bitmap_path);
        config.merge_interval_ms = 45_000;
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        config.sort_fields.push(SortFieldConfig {
            name: "sortAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: Some(crate::config::ComputedField {
                op: crate::config::ComputedOp::Greatest,
                source_fields: vec!["existedAt".into(), "publishedAt".into()],
            }),
        });
        config.deferred_alive = Some(crate::config::DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
            sweep_interval_secs: 0,
            sweep_limit: 20_000,
        });
        ConcurrentEngine::new_with_path(config, &docstore_path).unwrap()
    }

    /// Insert one image as DEFERRED (creates_slot + future publishedAt): doc
    /// written, no bitmaps set. Returns after flush + publish.
    fn insert_deferred_image(
        engine: &ConcurrentEngine,
        meta: &FieldMeta,
        slot: u32,
        post_id: i64,
        existed_at: i64,
        pub_future: i64,
    ) {
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(post_id) },
                Op::Set { field: "nsfwLevel".into(), value: json!(1) },
                Op::Set { field: "existedAt".into(), value: json!(existed_at) },
                Op::Set { field: "publishedAt".into(), value: json!(pub_future) },
            ],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut sink, meta, &mut batch, Some(engine), Some(&mut dw));
        assert_eq!((applied, errors), (1, 0), "deferred insert");
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        engine.force_publish_blocking(std::time::Duration::from_secs(5));
        assert!(engine.is_slot_deferred(slot), "image {slot} must be deferred");
        assert!(!engine.is_slot_alive(slot), "image {slot} must not be alive");
    }

    /// Read a single integer doc field, or None if absent/non-integer.
    fn doc_int_field(engine: &ConcurrentEngine, slot: u32, field: &str) -> Option<i64> {
        let doc = engine.get_document(slot).ok()??;
        match doc.fields.get(field) {
            Some(crate::mutation::FieldValue::Single(crate::query::Value::Integer(v))) => Some(*v),
            _ => None,
        }
    }

    /// Apply a single fan-out batch (Post entity → images) through a
    /// RecordingSink so the emitted deferred/bitmap ops can be asserted
    /// directly — the deterministic pattern the direct-op deferred tests use,
    /// avoiding a race on the flush thread's activation replay. The engine is
    /// still real (execute_query resolves the fan-out; deferred_slots() + the
    /// docstore drive the deferred-reach pass). Returns (applied, sink).
    fn apply_fanout_recording(
        engine: &ConcurrentEngine,
        meta: &FieldMeta,
        post_id: i64,
        inner_ops: Vec<Op>,
    ) -> (usize, RecordingSink) {
        let mut rec = RecordingSink::new();
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: post_id,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {post_id}")),
                ops: inner_ops,
            }],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut rec, meta, &mut batch, Some(engine), Some(&mut dw));
        assert_eq!(errors, 0, "fan-out must not error");
        dw.flush();
        (applied, rec)
    }

    /// (a) A fan-out that moves publishedAt from one FUTURE time to ANOTHER
    /// future time must reschedule the deferred slot in place: a single
    /// deferred_alive at the NEW future key, no bitmap writes, doc updated.
    #[test]
    fn test_deferred_fanout_future_to_future_stays_deferred() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = deferred_reach_engine(&dir);
        let meta = FieldMeta::from_config(engine.config());
        let slot: u32 = 7;
        let post_id: i64 = 100;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        insert_deferred_image(&engine, &meta, slot, post_id, now - 3600, now + 3600);

        let t2 = now + 7200; // a DIFFERENT future time
        let (applied, rec) = apply_fanout_recording(
            &engine,
            &meta,
            post_id,
            vec![
                Op::Remove { field: "publishedAt".into(), value: serde_json::Value::Null },
                Op::Set { field: "publishedAt".into(), value: json!(t2) },
            ],
        );
        assert_eq!(applied, 1, "fan-out must reach the deferred slot");
        // Rescheduled to the NEW future key, and only that.
        assert_eq!(
            rec.deferred_alive,
            vec![(slot, t2 as u64)],
            "must reschedule the deferred slot to t2"
        );
        // Deferred arm writes NO bitmaps (that's what keeps it out of feeds).
        assert!(
            rec.filter_inserts.is_empty() && rec.sort_sets.is_empty(),
            "deferred reschedule must not touch bitmaps"
        );
        // Doc carries the new schedule so activation later replays t2.
        assert_eq!(doc_int_field(&engine, slot, "publishedAt"), Some(t2));
    }

    /// (b) A fan-out that CLEARS publishedAt (unpublish → revert to draft) must
    /// schedule the deferred slot for IMMEDIATE activation (deferred_alive at
    /// ~now), mirroring the direct-op unschedule path.
    #[test]
    fn test_deferred_fanout_unpublish_activates_as_draft() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = deferred_reach_engine(&dir);
        let meta = FieldMeta::from_config(engine.config());
        let slot: u32 = 7;
        let post_id: i64 = 100;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        insert_deferred_image(&engine, &meta, slot, post_id, now - 3600, now + 3600);

        // Unpublish: source cleared, no future timestamp.
        let (applied, rec) = apply_fanout_recording(
            &engine,
            &meta,
            post_id,
            vec![Op::Set { field: "publishedAt".into(), value: serde_json::Value::Null }],
        );
        assert_eq!(applied, 1, "fan-out must reach the deferred slot");
        assert_eq!(rec.deferred_alive.len(), 1, "must emit one (immediate) activation");
        let (s, at) = rec.deferred_alive[0];
        assert_eq!(s, slot);
        assert!(
            (at as i64) >= now - 5 && (at as i64) <= now + 5,
            "unpublish must activate immediately (~now), got {at}"
        );
        assert!(
            rec.filter_inserts.is_empty() && rec.sort_sets.is_empty(),
            "deferred activation defers bitmap rebuild to replay, writes none here"
        );
    }

    /// (c) A NON-publish fan-out (touches no deferred source field) must NOT
    /// reach deferred slots — the cost gate. No deferred_alive emitted, and the
    /// deferred slot's doc is left untouched (the gate returns before any scan
    /// or docstore write).
    #[test]
    fn test_nonpublish_fanout_does_not_touch_deferred() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = deferred_reach_engine(&dir);
        let meta = FieldMeta::from_config(engine.config());
        let slot: u32 = 7;
        let post_id: i64 = 100;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        insert_deferred_image(&engine, &meta, slot, post_id, now - 3600, now + 3600);

        // A tag fan-out — no write to publishedAt (the deferred source).
        let (applied, rec) = apply_fanout_recording(
            &engine,
            &meta,
            post_id,
            vec![Op::Add { field: "tagIds".into(), value: json!(42) }],
        );
        // No alive images in this post, and the deferred slot is gated out.
        assert_eq!(applied, 0, "non-publish fan-out must apply nothing here");
        assert!(rec.deferred_alive.is_empty(), "gate: no deferred reschedule");
        assert!(engine.is_slot_deferred(slot), "deferred slot must remain deferred");
        // Gate proof: had the deferred-reach pass run, it would have written
        // tagIds=42 into the deferred slot's doc. It must not.
        let doc = engine.get_document(slot).unwrap().unwrap();
        let has_tag = matches!(
            doc.fields.get("tagIds"),
            Some(crate::mutation::FieldValue::Multi(v))
                if v.contains(&crate::query::Value::Integer(42))
        );
        assert!(!has_tag, "non-publish fan-out must not reach the deferred slot's doc");
    }

    /// (d) A mixed post — one alive image, one deferred image — early-published.
    /// Both arms apply exactly once: the alive image via the bitmap match
    /// (publishedAt sort write), the deferred image via the deferred-reach pass
    /// (a single deferred_alive). No double application of either slot.
    #[test]
    fn test_mixed_post_publish_fanout_both_arms() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = deferred_reach_engine(&dir);
        let meta = FieldMeta::from_config(engine.config());
        let post_id: i64 = 100;
        let alive_slot: u32 = 7;
        let deferred_slot: u32 = 8;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let existed = now - 3600;

        // Alive draft image (no publishedAt → alive, postId bit set).
        {
            let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
            let mut dw = DocWriter::new(engine.docstore_arc());
            let mut batch = vec![EntityOps {
                entity_id: alive_slot as i64,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "postId".into(), value: json!(post_id) },
                    Op::Set { field: "nsfwLevel".into(), value: json!(1) },
                    Op::Set { field: "existedAt".into(), value: json!(existed) },
                ],
            }];
            let (applied, _, errors) =
                apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
            assert_eq!((applied, errors), (1, 0));
            crate::ingester::BitmapSink::flush(&mut sink).unwrap();
            dw.flush();
        }
        wait_for_alive_slot(&engine, alive_slot, 5_000);
        // Deferred image scheduled for the future.
        insert_deferred_image(&engine, &meta, deferred_slot, post_id, existed, now + 3600);

        // Early-publish the whole post.
        let t_pub = now - 5;
        let (applied, rec) = apply_fanout_recording(
            &engine,
            &meta,
            post_id,
            vec![
                Op::Remove { field: "publishedAt".into(), value: serde_json::Value::Null },
                Op::Set { field: "publishedAt".into(), value: json!(t_pub) },
            ],
        );
        // Exactly two applied: one bitmap-matched (alive) + one deferred-reached.
        assert_eq!(applied, 2, "both arms apply exactly once");
        // Deferred arm: exactly the deferred slot rescheduled to the past time
        // (activates next flush cycle) — never the alive slot.
        assert_eq!(
            rec.deferred_alive,
            vec![(deferred_slot, t_pub as u64)],
            "only the deferred image is rescheduled, at t_pub"
        );
        // Bitmap arm: the alive slot's publishedAt sort layers were written; the
        // deferred slot never touches bitmaps.
        assert!(
            rec.sort_sets.iter().any(|(f, _, s)| f == "publishedAt" && *s == alive_slot),
            "alive image published via the bitmap arm"
        );
        assert!(
            !rec.sort_sets.iter().any(|(_, _, s)| *s == deferred_slot)
                && !rec.filter_inserts.iter().any(|(_, _, s)| *s == deferred_slot),
            "deferred image must not get bitmap writes (no double application)"
        );
    }

    /// Read a slot's sort-layer value the way queries see it: a sorted
    /// single-slot query whose page cursor carries the reconstructed value.
    fn sort_layer_value(engine: &ConcurrentEngine, field: &str, slot: u32) -> Option<u32> {
        let query = crate::query::BitdexQuery {
            filters: vec![crate::query::FilterClause::Eq(
                "postId".to_string(),
                crate::query::Value::Integer(100),
            )],
            sort: Some(crate::query::SortClause {
                field: field.to_string(),
                direction: crate::query::SortDirection::Asc,
            }),
            limit: 50,
            offset: None,
            cursor: None,
            skip_cache: true,
        };
        let mut cursor = None;
        for _ in 0..10 {
            let mut q = query.clone();
            q.limit = 1;
            q.cursor = cursor;
            let res = engine.execute_query(&q).ok()?;
            if res.ids.is_empty() {
                return None;
            }
            let cur = res.cursor?;
            if res.ids[0] as u32 == slot {
                return Some(cur.sort_value as u32);
            }
            cursor = Some(crate::query::CursorPosition {
                sort_value: cur.sort_value,
                slot_id: cur.slot_id,
            });
        }
        None
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
    /// Build a config exercising the deferred-publish safety net: a computed
    /// `sortAt = GREATEST(existedAt, publishedAt)`, `publishedAt` as both a
    /// sort field and the `deferred_alive` source, and an `isPublished`
    /// exists_boolean shadow sharing the `publishedAtUnix` source.
    fn safety_net_config() -> Config {
        use crate::config::{ComputedField, ComputedOp, DeferredAliveConfig};
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
        config.sort_fields = vec![
            SortFieldConfig { name: "existedAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false, computed: None },
            SortFieldConfig { name: "publishedAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false, computed: None },
            SortFieldConfig {
                name: "sortAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: false,
                computed: Some(ComputedField { op: ComputedOp::Greatest, source_fields: vec!["existedAt".into(), "publishedAt".into()] }),
            },
        ];
        config.deferred_alive = Some(DeferredAliveConfig { source_field: "publishedAt".into(), ms_to_seconds: false, sweep_interval_secs: 0, sweep_limit: 20_000, });
        config.data_schema.fields = vec![
            FieldMapping { source: "publishedAtUnix".into(), target: "publishedAt".into(), value_type: FieldValueType::Integer, fallback: None, string_map: None, doc_only: false, filter_only: false, ms_to_seconds: true, truncate_u32: false, case_sensitive: false, default_value: None, nullable: false },
            FieldMapping { source: "publishedAtUnix".into(), target: "isPublished".into(), value_type: FieldValueType::ExistsBoolean, fallback: None, string_map: None, doc_only: false, filter_only: false, ms_to_seconds: false, truncate_u32: false, case_sensitive: false, default_value: None, nullable: false },
        ];
        config
    }

    fn unit_now_secs() -> u32 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as u32
    }

    /// Regression (2026-07-03): the recompute safety net must complete a LOST
    /// deferred activation. A scheduled-ahead slot whose `activate_due` replay
    /// was never applied is stuck with `doc.publishedAt` in the past but its
    /// `isPublished` shadow false and `publishedAt` sort layer at 0 — excluded
    /// from `isPublished=true` feeds. When any recompute touches such a slot,
    /// the net must flip `isPublished=true` and write the `publishedAt` layer.
    /// Activation-orphan diagnostic ring: `recent_removes_for_slot` must filter
    /// by slot AND by the `since_ms` window, and format alive vs filter/sort
    /// records. This backs the non-apply-vs-revert probe in the verifier.
    #[test]
    fn recent_removes_ring_filters_by_slot_and_window() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        // Empty ring → no records.
        assert!(engine.recent_removes_for_slot(42, 0).is_empty());
        engine.push_remove_record_for_test(42, "filter", "postId", 1_000);
        engine.push_remove_record_for_test(42, "sort", "publishedAt", 1_100);
        engine.push_remove_record_for_test(7, "alive", "", 1_050);
        // Slot 42 within window → both its records, not slot 7's.
        let recs = engine.recent_removes_for_slot(42, 1_000);
        assert_eq!(recs.len(), 2, "got {recs:?}");
        assert!(recs.iter().any(|r| r == "filter:postId@1000"));
        assert!(recs.iter().any(|r| r == "sort:publishedAt@1100"));
        // `since` excludes older records.
        let recs2 = engine.recent_removes_for_slot(42, 1_050);
        assert_eq!(recs2, vec!["sort:publishedAt@1100".to_string()]);
        // alive record for slot 7 formats without a field.
        assert_eq!(
            engine.recent_removes_for_slot(7, 0),
            vec!["alive@1050".to_string()]
        );
    }

    #[test]
    fn test_recompute_safety_net_completes_lost_activation() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let slot = 42u32;
        let t_pub = unit_now_secs() - 100; // published in the past
        let new_existed = t_pub - 50; // publishedAt is the max → sortAt = t_pub

        // Forge the stuck state: doc carries a past publishedAt, but no op ever
        // set the bitmap/shadow (as the deferred branch leaves it).
        let mut dw = DocWriter::new(engine.docstore_arc());
        dw.write_set(slot, "publishedAt", &json!(t_pub as i64));
        dw.flush();

        // A later existedAt re-scan fires recompute for this slot.
        let mut sort_values: HashMap<&str, u32> = HashMap::new();
        sort_values.insert("existedAt", new_existed);
        let old_sort_values: HashMap<&str, u32> = HashMap::new();

        let mut sink = RecordingSink::new();
        recompute_computed_sorts_for_slot(&mut sink, &meta, Some(&engine), slot, &sort_values, &old_sort_values, None);

        // isPublished shadow flipped to true (clear false bit, set true bit).
        assert!(
            sink.filter_inserts.contains(&("isPublished".to_string(), 1u64, slot)),
            "safety net must insert isPublished=true, got inserts={:?}", sink.filter_inserts
        );
        assert!(
            sink.filter_removes.contains(&("isPublished".to_string(), 0u64, slot)),
            "safety net must clear isPublished=false, got removes={:?}", sink.filter_removes
        );

        // publishedAt sort layer written from the stored doc value (set bits only).
        let pub_bits: Vec<usize> = sink.sort_sets.iter().filter(|(f, _, _)| f == "publishedAt").map(|(_, b, _)| *b).collect();
        let expected_bits: Vec<usize> = (0..32).filter(|b| (t_pub >> b) & 1 == 1).collect();
        assert_eq!(pub_bits, expected_bits, "safety net must write publishedAt sort-layer set-bits for the stored value");
    }

    /// The safety net must NOT force-activate a slot that is still legitimately
    /// deferred (publishedAt in the future) — that would leak scheduled posts.
    #[test]
    fn test_recompute_safety_net_skips_future_publishedat() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let slot = 43u32;
        let t_future = unit_now_secs() + 86_400; // 1 day out
        let new_existed = unit_now_secs() - 100;

        let mut dw = DocWriter::new(engine.docstore_arc());
        dw.write_set(slot, "publishedAt", &json!(t_future as i64));
        dw.flush();

        let mut sort_values: HashMap<&str, u32> = HashMap::new();
        sort_values.insert("existedAt", new_existed);
        let old_sort_values: HashMap<&str, u32> = HashMap::new();

        let mut sink = RecordingSink::new();
        recompute_computed_sorts_for_slot(&mut sink, &meta, Some(&engine), slot, &sort_values, &old_sort_values, None);

        // No publishedAt sort-layer writes and no isPublished flip — the slot
        // stays deferred until activate_due handles it.
        assert!(
            !sink.sort_sets.iter().any(|(f, _, _)| f == "publishedAt"),
            "safety net must not write publishedAt layer for a future value, got {:?}", sink.sort_sets
        );
        assert!(
            !sink.filter_inserts.iter().any(|(f, v, _)| f == "isPublished" && *v == 1),
            "safety net must not flip isPublished for a future value, got {:?}", sink.filter_inserts
        );
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

    /// Reduce a sequence of (bit, set|clear) ops into the final bitmap value.
    /// Returns Some(value) if every bit 0..bits got at least one definitive
    /// write (set or clear). Returns None for any bit where no op was emitted
    /// — that means the recompute path is leaving prior bitmap state untouched
    /// for that bit position, which is exactly the corruption vector this
    /// regression targets.
    fn reconstruct_from_sink_ops(
        sink: &RecordingSink,
        target_field: &str,
        slot: u32,
        bits: usize,
    ) -> Option<u32> {
        // Walk the recorded ops in order and track each bit's final state.
        let mut state: Vec<Option<bool>> = vec![None; bits];
        let mut events: Vec<(usize, usize, bool)> = Vec::new();
        for (idx, (f, b, s)) in sink.sort_sets.iter().enumerate() {
            if f == target_field && *s == slot && *b < bits {
                events.push((idx, *b, true));
            }
        }
        for (idx, (f, b, s)) in sink.sort_clears.iter().enumerate() {
            if f == target_field && *s == slot && *b < bits {
                // Use a separate id space; clears appear after sets in the
                // recorded order, but we want to honor the actual emission
                // order. Tag clears with offset to keep them stable; the
                // current recompute path emits set OR clear per bit per
                // recompute call, never both, so no real ordering ambiguity
                // exists in practice.
                events.push((idx + 1_000_000_000, *b, false));
            }
        }
        events.sort_by_key(|(idx, _, _)| *idx);
        for (_, bit, set) in events {
            state[bit] = Some(set);
        }
        let mut value: u32 = 0;
        for (bit, s) in state.iter().enumerate() {
            match s {
                Some(true) => value |= 1u32 << bit,
                Some(false) => {} // bit explicitly cleared
                None => return None, // no definitive write — corruption vector
            }
        }
        Some(value)
    }

    /// REGRESSION: computed-sort recompute must write a definitive set OR
    /// clear for every bit in `target_bits` on every call, so the resulting
    /// bitmap state equals new_computed exactly — no dependence on prior
    /// state.
    ///
    /// Pre-fix behavior: only emitted sort_clear when old_computed had the
    /// bit set, only emitted sort_set when new_computed had the bit set.
    /// Bits where both old_computed and new_computed had 0 received NO op —
    /// any bits the bitmap previously carried in those positions stayed set,
    /// and subsequent updates OR'd new bits on top. Production symptom: slot
    /// 32136507's reconstructed sortAt was multi-year shifted from the doc
    /// because the same OR'd-superset accumulated over many trigger-driven
    /// recomputes.
    #[test]
    fn test_computed_sort_recompute_full_overwrite_all_bits() {
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
        // Mirrors the prod symptom: a slot lives a long time, sees many
        // publishedAt updates, and the bitmap accumulates if recompute ever
        // skipped a bit. We cycle through values whose bit patterns differ
        // significantly so any stale bits would be visible.
        let test_values: &[(u32, u32)] = &[
            (0x66F6_E6E3, 0x69F0_2766), // close to slot 32136507's source values
            (0xFFFF_0000, 0x0000_FFFF), // disjoint halves
            (0x0F0F_0F0F, 0xF0F0_F0F0), // alternating nibbles
            (0xFFFF_FFFF, 0x0000_0000), // all-set then all-zero (max worst case)
            (0x1234_5678, 0x8765_4321), // arbitrary
            (0x0000_0001, 0x0000_0002), // tiny values — most bits MUST be cleared
        ];
        let mut sink = RecordingSink::new();
        // First op creates the slot; subsequent ops are pure recomputes.
        for (i, (a, b)) in test_values.iter().enumerate() {
            let prev_a = if i == 0 { 0u32 } else { test_values[i - 1].0 };
            let prev_b = if i == 0 { 0u32 } else { test_values[i - 1].1 };
            let mut batch = vec![EntityOps {
                entity_id: 7,
                creates_slot: i == 0,
                ops: vec![
                    Op::Remove { field: "existedAt".into(), value: json!(prev_a as i64) },
                    Op::Set { field: "existedAt".into(), value: json!(*a as i64) },
                    Op::Remove { field: "publishedAt".into(), value: json!(prev_b as i64) },
                    Op::Set { field: "publishedAt".into(), value: json!(*b as i64) },
                ],
            }];
            let prev_sets = sink.sort_sets.len();
            let prev_clears = sink.sort_clears.len();
            let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
            assert_eq!(applied, 1);
            assert_eq!(errors, 0);
            // Carve out the ops emitted by THIS recompute alone.
            let mut cycle_sink = RecordingSink::new();
            cycle_sink.sort_sets = sink.sort_sets[prev_sets..].to_vec();
            cycle_sink.sort_clears = sink.sort_clears[prev_clears..].to_vec();
            // Every bit must have a definitive write — set or clear, never absent.
            let reconstructed = reconstruct_from_sink_ops(&cycle_sink, "sortAt", 7, 32);
            let expected = (*a).max(*b);
            assert_eq!(
                reconstructed,
                Some(expected),
                "computed sort recompute iter {}: every bit must be definitively written. \
                 a=0x{:08x} b=0x{:08x} expected=0x{:08x} reconstructed={:?}",
                i, a, b, expected, reconstructed.map(|v| format!("0x{:08x}", v)),
            );
        }
    }

    /// REGRESSION: queryOpSet fan-out previously called `process_set_op` for
    /// each matched slot but never invoked the computed-sort recompute, so
    /// `sortAt = GREATEST(existedAt, publishedAt)` stayed stale on fan-out
    /// targets. This test exercises the helper directly to confirm full
    /// overwrite of the target field for any new_computed value, and
    /// implicitly validates the call from the fan-out path which now invokes
    /// the same helper.
    #[test]
    fn test_recompute_helper_full_overwrite_for_fanout_path() {
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
        // Mirror a fan-out call: shared sort_values from a publishedAt update
        // op, no engine (so no stored fallback — every source field must be
        // present in sort_values for the test to be deterministic).
        let mut sort_values: HashMap<&str, u32> = HashMap::new();
        sort_values.insert("existedAt", 1727723939);
        sort_values.insert("publishedAt", 1778184486);
        let old_sort_values: HashMap<&str, u32> = HashMap::new();
        let mut sink = RecordingSink::new();
        let slot: u32 = 99;
        recompute_computed_sorts_for_slot(
            &mut sink,
            &meta,
            None,
            slot,
            &sort_values,
            &old_sort_values,
            None,
        );
        // sortAt must receive a definitive write for every bit.
        let new_computed: u32 = 1727723939u32.max(1778184486u32);
        for bit in 0..32 {
            if (new_computed >> bit) & 1 == 1 {
                assert!(
                    sink.sort_sets.iter().any(|(f, b, s)| f == "sortAt" && *b == bit && *s == slot),
                    "sortAt bit {bit} (set in new_computed=0x{:08x}) must be sort_set on fan-out target slot {slot}",
                    new_computed
                );
                assert!(
                    !sink.sort_clears.iter().any(|(f, b, s)| f == "sortAt" && *b == bit && *s == slot),
                    "sortAt bit {bit} must not be sort_cleared when new_computed has it set"
                );
            } else {
                assert!(
                    sink.sort_clears.iter().any(|(f, b, s)| f == "sortAt" && *b == bit && *s == slot),
                    "sortAt bit {bit} (zero in new_computed=0x{:08x}) must be sort_cleared so prior bitmap state cannot leak through (this is the OR-accumulation guard for fan-out targets)",
                    new_computed
                );
                assert!(
                    !sink.sort_sets.iter().any(|(f, b, s)| f == "sortAt" && *b == bit && *s == slot),
                    "sortAt bit {bit} must not be sort_set when new_computed has it zero"
                );
            }
        }
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
    /// Regression (FOLLOWUP.md "LCS dictionary durability hole", 2026-07-08):
    /// a key minted on the OPS path must survive a crash — persisted where
    /// boot's `load_dictionaries` actually reads (`<bitmap_path>/dictionaries`,
    /// not the old dead `shardstore/dictionaries` target), so a reopened
    /// dictionary never re-issues an on-disk-referenced key to a different
    /// string.
    #[test]
    fn test_ops_minted_dict_key_survives_crash_and_is_not_reissued() {
        use crate::config::{DataSchema, FieldMapping, FieldValueType};
        let dir = tempfile::TempDir::new().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");

        let mut config = test_config();
        config.storage.bitmap_path = Some(bitmap_path.clone());
        config.data_schema = DataSchema {
            fields: vec![FieldMapping {
                source: "type".into(),
                target: "type".into(),
                value_type: FieldValueType::LowCardinalityString,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: false,
            }],
            ..Default::default()
        };
        let schema = config.data_schema.clone();

        let minted_key;
        {
            // Boot dance as server.rs does it: engine + load_dictionaries +
            // set_dictionaries.
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            let dicts = ConcurrentEngine::load_dictionaries(&schema, &bitmap_path).unwrap();
            engine.set_dictionaries(dicts);

            // Mint a key via the ops path (WAL-reader shape).
            let meta = FieldMeta::from_config(engine.config());
            let mut sink = RecordingSink::new();
            let mut batch = vec![EntityOps {
                entity_id: 4242,
                creates_slot: true,
                ops: vec![Op::Set { field: "type".into(), value: json!("HoloCine") }],
            }];
            let (applied, _, errors) =
                apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), None);
            assert_eq!((applied, errors), (1, 0));
            minted_key = engine.dictionaries().get("type").unwrap().get("holocine").unwrap();

            // What the WAL-reader loop now does after every batch.
            engine.persist_dirty_dictionaries().unwrap();
            // Crash: engine dropped without any HTTP-upsert/save_snapshot path.
        }

        // Reboot: the minted key must be present and never re-issued.
        let reloaded = ConcurrentEngine::load_dictionaries(&schema, &bitmap_path).unwrap();
        let dict = reloaded.get("type").expect("dictionary must exist after reboot");
        assert_eq!(
            dict.get("holocine"),
            Some(minted_key),
            "ops-minted key must survive the crash"
        );
        let k_new = dict.get_or_insert("SomethingElse");
        assert_ne!(
            k_new, minted_key,
            "reopened dictionary must not re-issue the minted key"
        );
    }

    /// Legacy-dir fallback: keys stranded in the old dead persist target
    /// (`<bitmap_path>/shardstore/dictionaries`) are still picked up when the
    /// canonical file is missing or older.
    #[test]
    fn test_load_dictionaries_legacy_shardstore_fallback() {
        use crate::config::{DataSchema, FieldMapping, FieldValueType};
        let dir = tempfile::TempDir::new().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let schema = DataSchema {
            fields: vec![FieldMapping {
                source: "type".into(),
                target: "type".into(),
                value_type: FieldValueType::LowCardinalityString,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: false,
            }],
            ..Default::default()
        };

        // Stranded legacy copy with 2 keys; no canonical copy.
        let legacy = crate::dictionary::FieldDictionary::new();
        legacy.get_or_insert("alpha");
        legacy.get_or_insert("beta");
        crate::dictionary::save_dictionary(
            &legacy.snapshot(),
            &bitmap_path.join("shardstore").join("dictionaries").join("type.dict"),
        )
        .unwrap();

        let loaded = ConcurrentEngine::load_dictionaries(&schema, &bitmap_path).unwrap();
        let dict = loaded.get("type").unwrap();
        assert_eq!(dict.get("alpha"), legacy.get("alpha"));
        assert_eq!(dict.get("beta"), legacy.get("beta"));

        // Canonical newer than legacy → canonical wins.
        let canonical = crate::dictionary::FieldDictionary::new();
        canonical.get_or_insert("alpha");
        canonical.get_or_insert("beta");
        canonical.get_or_insert("gamma");
        crate::dictionary::save_dictionary(
            &canonical.snapshot(),
            &bitmap_path.join("dictionaries").join("type.dict"),
        )
        .unwrap();
        let loaded2 = ConcurrentEngine::load_dictionaries(&schema, &bitmap_path).unwrap();
        assert!(loaded2.get("type").unwrap().get("gamma").is_some());
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
        sweep_interval_secs: 0,
        sweep_limit: 20_000,
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
    /// Poll until the engine's published snapshot shows the slot as deferred.
    fn wait_for_deferred(engine: &ConcurrentEngine, slot: u32, timeout_ms: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        while std::time::Instant::now() < deadline {
            if engine.is_slot_deferred(slot) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        panic!("slot {slot} never became deferred within {timeout_ms}ms");
    }

    /// Regression (audit 2026-07-07 §3.1): ops for a deferred, not-yet-alive
    /// slot were silently dropped by the not-alive guard — a publish-date
    /// reschedule while deferred was lost, so the slot activated at the stale
    /// time. While deferred, a Set on the deferred source field must
    /// re-schedule the activation (and persist to the doc), not be skipped.
    #[test]
    fn test_deferred_slot_reschedule_not_dropped() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let slot: u32 = 77;
        let now = unit_now_secs() as i64;
        let t1 = now + 3_600; // initial schedule: +1h
        let t2 = now + 7_200; // rescheduled: +2h

        // Fresh insert carrying a future publishedAt → deferred branch.
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "existedAt".into(), value: json!(now - 100) },
                Op::Set { field: "publishedAt".into(), value: json!(t1) },
            ],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        assert_eq!((applied, errors), (1, 0));
        sink.flush().unwrap();
        dw.flush();
        wait_for_deferred(&engine, slot, 2_000);

        // Follow-up op (creates_slot=false): reschedule publishedAt. Recorded
        // via RecordingSink so we can assert exactly what the guard emits.
        let mut rec = RecordingSink::new();
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let mut batch2 = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: false,
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(t2) }],
        }];
        let (applied2, skipped2, errors2) =
            apply_ops_batch(&mut rec, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
        dw2.flush();
        assert_eq!(
            (applied2, skipped2, errors2),
            (1, 0, 0),
            "reschedule for a deferred slot must be applied, not skipped"
        );
        assert_eq!(
            rec.deferred_alive,
            vec![(slot, t2 as u64)],
            "guard must re-schedule the deferred activation at the new time"
        );
        assert!(
            rec.alive_inserts.is_empty() && rec.filter_inserts.is_empty(),
            "no bitmap mutations while still deferred"
        );
        // Doc must carry the new publishedAt for the eventual activation replay.
        let doc = engine.get_document(slot).unwrap().unwrap();
        assert_eq!(
            doc.fields.get("publishedAt"),
            Some(&crate::mutation::FieldValue::Single(crate::types::Value::Integer(t2))),
        );
    }

    /// Unscheduling while deferred (external review 2026-07-07, GPT #3 /
    /// Gemini #1): a Set publishedAt=null on a deferred slot must not leave
    /// it invisible until the stale schedule fires — it reverts to a draft,
    /// which is alive. The guard must emit an immediate activation.
    #[test]
    fn test_deferred_slot_unschedule_activates_as_draft() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let slot: u32 = 79;
        let now = unit_now_secs() as i64;

        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "existedAt".into(), value: json!(now - 100) },
                Op::Set { field: "publishedAt".into(), value: json!(now + 3_600) },
            ],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        sink.flush().unwrap();
        dw.flush();
        wait_for_deferred(&engine, slot, 2_000);

        // Unschedule: publishedAt → null.
        let mut rec = RecordingSink::new();
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let mut batch2 = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: false,
            ops: vec![Op::Set { field: "publishedAt".into(), value: serde_json::Value::Null }],
        }];
        let (applied2, skipped2, _) =
            apply_ops_batch(&mut rec, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
        dw2.flush();
        assert_eq!((applied2, skipped2), (1, 0));
        assert_eq!(
            rec.deferred_alive.len(),
            1,
            "unschedule must emit an immediate activation, got {:?}",
            rec.deferred_alive
        );
        let (s, at) = rec.deferred_alive[0];
        assert_eq!(s, slot);
        assert!(
            (at as i64) <= now + 5,
            "activation must be scheduled at ~now (immediate), got {at}"
        );
    }

    /// A follow-up op that does NOT touch the schedule (e.g. a tag update)
    /// must keep the slot deferred — doc write only, no activation.
    #[test]
    fn test_deferred_slot_unrelated_op_stays_deferred() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let slot: u32 = 80;
        let now = unit_now_secs() as i64;

        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(now + 3_600) }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        sink.flush().unwrap();
        dw.flush();
        wait_for_deferred(&engine, slot, 2_000);

        let mut rec = RecordingSink::new();
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let mut batch2 = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: false,
            ops: vec![Op::Set { field: "existedAt".into(), value: json!(now - 50) }],
        }];
        let (applied2, _, _) =
            apply_ops_batch(&mut rec, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
        dw2.flush();
        assert_eq!(applied2, 1, "doc write for a deferred slot must be applied");
        assert!(
            rec.deferred_alive.is_empty() && rec.alive_inserts.is_empty(),
            "an op that doesn't touch the schedule must not activate or reschedule"
        );
    }

    /// PROD REPRO (deferred activation-miss, 2026-07-14): a deferred slot whose
    /// stored doc can't be read at activation must be RE-DEFERRED for retry, not
    /// activated blind. `activate_due` sets the alive bit and removes the slot
    /// from the deferred map before the flush thread reads the doc; the pre-fix
    /// code, on a doc-read miss, pushed AliveInsert-only — stranding the slot
    /// ALIVE with zero bitmaps (invisible to every filtered query and to the
    /// isPublished=false overdue sweep) permanently. The fix undoes the
    /// activation and re-schedules for a short retry. Here we schedule a slot
    /// deferred-due-now WITHOUT writing its doc, so the flush thread's
    /// activation read misses; the slot must NOT go alive and must stay
    /// deferred. Pre-fix this asserts fails: the slot is alive with no bitmaps.
    /// The VB-state diagnostic reports the membership bitmap's true state for a
    /// slot: present + slot_in_fused=true after an insert, and VB=ABSENT for an
    /// unknown value. This is the lens for the next prod orphan specimen.
    #[test]
    fn test_filter_value_state_debug() {
        let mut config = safety_net_config();
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(), field_type: FilterFieldType::SingleValue,
            behaviors: None, eviction: None, eager_load: false,
            per_value_lazy: true, max_range_scan_values: None,
        });
        let engine = ConcurrentEngine::new(config).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let post_id: i64 = 4242;
        let slot: u32 = 90;
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: slot as i64, creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(post_id) },
                Op::Set { field: "existedAt".into(), value: json!(1000) },
            ],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        wait_for_alive_slot(&engine, slot, 5_000);

        let present = engine.filter_value_state_debug("postId", post_id as u64, slot);
        assert!(present.contains("slot_in_fused=true"), "present state should show fused=true: {present}");
        let absent = engine.filter_value_state_debug("postId", 999_999, slot);
        assert!(absent.contains("slot_in_fused=false"), "absent value should show fused=false: {absent}");
    }

    #[test]
    fn test_per_value_lazy_insert_survives_lazy_load() {
        // Isolates the lead's per_value_lazy hypothesis: an insert to a
        // per_value_lazy field for a value NEVER on disk (like an activation
        // replay's postId insert for a dump-deferred slot) creates an UNLOADED
        // VersionedBitmap with the slot in its diff. A subsequent query on that
        // value triggers the per-value lazy load. Assert the slot survives.
        let dir = tempfile::TempDir::new().unwrap();
        let docstore_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");
        let mut config = Config::default();
        config.storage.bitmap_path = Some(bitmap_path);
        config.filter_fields = vec![
            FilterFieldConfig {
                name: "nsfwLevel".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None, eviction: None, eager_load: true,
                per_value_lazy: false, max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "postId".into(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None, eviction: None, eager_load: false,
                per_value_lazy: true, max_range_scan_values: None,
            },
        ];
        config.sort_fields = vec![SortFieldConfig {
            name: "existedAt".into(), source_type: "uint32".into(),
            encoding: "linear".into(), bits: 32, eager_load: false, computed: None,
        }];
        let engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let post_id: i64 = 29783080;
        let slot: u32 = 500;

        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(post_id) },
                Op::Set { field: "nsfwLevel".into(), value: json!(1) },
                Op::Set { field: "existedAt".into(), value: json!(1000) },
            ],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        wait_for_alive_slot(&engine, slot, 5_000);

        // Query postId=P → per-value lazy load engages. Slot must survive.
        let q = crate::query::BitdexQuery {
            filters: vec![crate::query::FilterClause::Eq(
                "postId".into(),
                crate::query::Value::Integer(post_id),
            )],
            sort: None, limit: 100, offset: None, cursor: None, skip_cache: true,
        };
        let res = engine.execute_query(&q).unwrap();
        assert!(
            res.ids.contains(&(slot as i64)),
            "per_value_lazy postId insert must survive the lazy load, got {:?}",
            res.ids
        );
    }

    /// The lead's exact specimen shape: postId=P is PARTIALLY on disk (an
    /// earlier alive image of the post), then the engine restarts (P becomes
    /// per_value_lazy unloaded, on disk), then a NEW slot's postId=P is inserted
    /// in-memory (the activation replay for a dump-deferred image), then a query
    /// on postId=P triggers the per-value lazy load FROM DISK (load_base). If
    /// that load drops the in-memory-inserted slot, the new slot is absent =
    /// the orphan. Assert BOTH the on-disk slot and the new in-memory slot are
    /// present.
    #[test]
    fn test_per_value_lazy_ondisk_load_preserves_inmemory_insert() {
        let dir = tempfile::TempDir::new().unwrap();
        let docstore_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");
        let mut config = Config::default();
        config.storage.bitmap_path = Some(bitmap_path);
        config.merge_interval_ms = 100;
        config.filter_fields = vec![
            FilterFieldConfig {
                name: "nsfwLevel".into(), field_type: FilterFieldType::SingleValue,
                behaviors: None, eviction: None, eager_load: true,
                per_value_lazy: false, max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "postId".into(), field_type: FilterFieldType::SingleValue,
                behaviors: None, eviction: None, eager_load: false,
                per_value_lazy: true, max_range_scan_values: None,
            },
        ];
        config.sort_fields = vec![SortFieldConfig {
            name: "existedAt".into(), source_type: "uint32".into(),
            encoding: "linear".into(), bits: 32, eager_load: false, computed: None,
        }];
        let post_id: i64 = 29783080;
        let on_disk_slot: u32 = 100; // earlier alive image, persisted with postId=P
        let new_slot: u32 = 200;     // dump-deferred image, activation inserts postId=P

        let postid_ids = |engine: &ConcurrentEngine| -> Vec<i64> {
            let q = crate::query::BitdexQuery {
                filters: vec![crate::query::FilterClause::Eq(
                    "postId".into(), crate::query::Value::Integer(post_id))],
                sort: None, limit: 100, offset: None, cursor: None, skip_cache: true,
            };
            engine.execute_query(&q).map(|r| r.ids).unwrap_or_default()
        };

        // --- Engine 1: persist an alive image with postId=P to disk ---
        {
            let engine = ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            let meta = FieldMeta::from_config(engine.config());
            let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
            let mut dw = DocWriter::new(engine.docstore_arc());
            let mut batch = vec![EntityOps {
                entity_id: on_disk_slot as i64, creates_slot: true,
                ops: vec![
                    Op::Set { field: "postId".into(), value: json!(post_id) },
                    Op::Set { field: "nsfwLevel".into(), value: json!(1) },
                    Op::Set { field: "existedAt".into(), value: json!(1000) },
                ],
            }];
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
            crate::ingester::BitmapSink::flush(&mut sink).unwrap();
            dw.flush();
            wait_for_alive_slot(&engine, on_disk_slot, 5_000);
            assert!(postid_ids(&engine).contains(&(on_disk_slot as i64)));
            // Let the merge thread persist filter bitmaps to disk, then drop
            // (drop joins the merge thread → final persist).
            std::thread::sleep(std::time::Duration::from_millis(600));
            drop(engine);
        }

        // --- Engine 2: reopen (postId=P on disk, lazy/unloaded) ---
        let engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        // Insert the NEW slot's postId=P in-memory (mimics activation replay).
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: new_slot as i64, creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(post_id) },
                Op::Set { field: "nsfwLevel".into(), value: json!(1) },
                Op::Set { field: "existedAt".into(), value: json!(2000) },
            ],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();
        dw.flush();
        wait_for_alive_slot(&engine, new_slot, 5_000);

        // Query postId=P → triggers the per-value lazy load of P FROM DISK.
        let ids = postid_ids(&engine);
        assert!(
            ids.contains(&(on_disk_slot as i64)),
            "on-disk slot must be present after lazy load, got {ids:?}"
        );
        assert!(
            ids.contains(&(new_slot as i64)),
            "in-memory-inserted slot must SURVIVE the disk lazy-load, got {ids:?}"
        );
    }

    #[test]
    fn test_activation_read_miss_redefers_not_orphans() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let now = unit_now_secs();
        let test_slot: u32 = 55; // NO stored doc → read miss at activation
        let ctrl_slot: u32 = 56; // HAS a doc → activates normally (proves the cycle ran)

        // Control slot's doc: publishedAt in the past so activation replays it.
        let mut dw = DocWriter::new(engine.docstore_arc());
        dw.write_set(ctrl_slot, "publishedAt", &json!(now as i64 - 10));
        dw.write_set(ctrl_slot, "existedAt", &json!(now as i64 - 100));
        dw.flush();

        // Schedule both deferred, due immediately.
        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        crate::ingester::BitmapSink::deferred_alive(&mut sink, test_slot, now as u64);
        crate::ingester::BitmapSink::deferred_alive(&mut sink, ctrl_slot, now as u64);
        crate::ingester::BitmapSink::flush(&mut sink).unwrap();

        // The control MUST activate — this proves the flush thread ran a real
        // activate_due cycle (so the test slot's assertions aren't vacuous).
        wait_for_alive_slot(&engine, ctrl_slot, 5_000);

        // The test slot's doc read missed: it must be re-deferred, NOT activated
        // blind. Pre-fix it is ALIVE with zero bitmaps (the orphan); the fix
        // undoes the activation and re-schedules it.
        assert!(
            !engine.is_slot_alive(test_slot),
            "a doc-read miss must NOT activate the slot blind (would orphan it)"
        );
        assert!(
            engine.is_slot_deferred(test_slot),
            "a doc-read miss must re-defer the slot for retry"
        );
    }

    // NOTE — there is deliberately NO test here pinning "the seq must not
    // advance before the snapshot it describes." That invariant existed in an
    // earlier design that kept the publish count in an atomic BESIDE the
    // ArcSwap, ordered against the store by hand. A test was written for it and
    // then mutated (increment moved above the store): the test stayed GREEN,
    // because the violation window is nanoseconds and any observational test
    // polls straight past it. A test that cannot fail is worse than no test —
    // it is the reason the next person stops looking.
    //
    // The invariant is now carried by `InnerEngine::publish_seq`: the seq is
    // published by the same atomic store as the state it certifies, so the
    // mutation is not expressible and there is nothing left to pin.

    /// A test engine that has published at least once, so slots tagged 0 are
    /// drainable. An IDLE engine never publishes — nothing is dirty, so the
    /// count legitimately stays at 0 — which is itself the design working: with
    /// no publish, nothing is provably visible and nothing may be judged.
    #[cfg(test)]
    fn engine_past_first_publish(config: crate::config::Config) -> ConcurrentEngine {
        let engine = ConcurrentEngine::new(config).unwrap();
        let mut fields = HashMap::new();
        fields.insert(
            "id".to_string(),
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(1)),
        );
        engine
            .put(1, &crate::mutation::Document { fields })
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while engine.published_seq() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "engine never published after a write"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        engine
    }

    /// THE GATE, tested at the only value that discriminates: `published == c`.
    ///
    /// Every live slot sits at exactly this value between being enqueued and its
    /// own batch publishing — the flush thread tags it with the seq that is
    /// CURRENTLY published, and the batch lands at `c + 1`. So `published == c`
    /// means "my batch has NOT published yet", and the gate must be CLOSED.
    ///
    /// This is the assertion the earlier version of this test lacked, while
    /// carrying this name. It gated at `AfterPublish(u64::MAX)` (closed under
    /// both `>` and `>=`) and `AfterPublish(0)` with `published > 0` (open under
    /// both) — two values that agree, and never the one that disagrees. Relaxing
    /// the gate to `>=` therefore left 79 tests green while making it a total
    /// no-op: every slot drainable the instant it is queued, verifier reading
    /// pre-publish state, false orphans restored in one character. A test named
    /// for an invariant it does not pin is worse than no test, because it is
    /// trusted — this one fails under `>=`.
    #[test]
    fn test_gate_is_closed_at_the_slots_own_publish_seq() {
        let engine = engine_past_first_publish(safety_net_config());

        // The live-slot state, reproduced exactly: tag == currently-published seq.
        let live = engine.published_seq();
        engine.push_activation_verify_gated_for_test(
            &[42],
            crate::concurrent_engine::VerifyGate::AfterPublish(live),
        );

        // THE DISCRIMINATING ASSERTION. `>` keeps this closed; `>=` opens it.
        assert_eq!(
            engine.drain_activation_verify(100),
            Vec::<u32>::new(),
            "a slot tagged with the CURRENTLY published seq has not had its own \
             batch published yet — the gate must be CLOSED. If this returns the \
             slot, the gate is a no-op: it hands the verifier pre-publish state \
             and manufactures the false orphans it exists to prevent."
        );
        assert_eq!(engine.activation_verify_len(), 1, "still queued, not dropped");
        assert_eq!(engine.activation_verify_ready_len(), 0, "and not counted ready");

        // Now let its batch publish. The gate must OPEN — it delays judgement,
        // it never prevents it.
        let mut fields = HashMap::new();
        fields.insert(
            "id".to_string(),
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(43)),
        );
        engine
            .put(43, &crate::mutation::Document { fields })
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while engine.published_seq() <= live {
            assert!(
                std::time::Instant::now() < deadline,
                "engine never published after a write"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            engine.drain_activation_verify(100),
            vec![42],
            "once the seq passes the tag the batch is provably live, so the slot \
             MUST be released — a gate that never opens is a stranded slot"
        );
    }

    /// The publish seq must never RUN BACKWARDS — pinning bug 3.
    ///
    /// The two `InnerEngine` rebuild sites (field unload) construct a fresh
    /// struct, so they must carry `publish_seq` across. Writing `publish_seq: 0`
    /// there compiles, reads perfectly well, and rewinds the gate. Unlike the
    /// seq-vs-snapshot ordering — which this design made *inexpressible* — this
    /// mistake IS expressible, so it gets a test rather than a comment.
    ///
    /// The damage is transient, not permanent: `flush_cycle` keeps climbing, so
    /// the gate recovers within a cycle. The cost is the window — while the seq
    /// is behind, nothing drains, the ring keeps taking activations, and past
    /// `ring_cap` the front is evicted, silently dropping unverified slots.
    #[test]
    fn test_unload_does_not_rewind_publish_seq() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = safety_net_config();
        config.storage.bitmap_path = Some(dir.path().join("bitmaps"));
        config.merge_interval_ms = 200;
        let docstore_path = dir.path().join("docs");
        let engine = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();

        // Get the seq off the floor so a rewind to 0 is actually a decrease.
        let mut fields = HashMap::new();
        fields.insert(
            "id".to_string(),
            crate::mutation::FieldValue::Single(crate::query::Value::Integer(11)),
        );
        engine
            .put(11, &crate::mutation::Document { fields })
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while engine.published_seq() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "engine never published after a write"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let before = engine.published_seq();
        assert!(before > 0, "precondition: seq is off the floor");

        // Unload rebuilds InnerEngine — the seq must survive the rebuild.
        engine.save_and_unload().unwrap();

        // Watch across the rebuild's publish. Monotonic means monotonic: not
        // "recovers shortly", not "usually" — never lower, at any observation.
        let watch = std::time::Instant::now() + std::time::Duration::from_millis(600);
        while std::time::Instant::now() < watch {
            let now = engine.published_seq();
            assert!(
                now >= before,
                "publish seq went BACKWARDS across an unload ({now} < {before}). \
                 An unload changes what is resident, not which publish we are on. \
                 While the seq is behind, the gate opens for nothing: the ring \
                 fills with activations that never drain, and past ring_cap the \
                 oldest are evicted — unverified slots dropped with no log line, \
                 no counter and no alarm."
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// The ring's ordering is STRUCTURAL, and the drain relies on it: it stops at
    /// the first unpublished entry, which is only sound if nothing behind that
    /// entry can be ready. Tags come from a single flush thread reading a
    /// monotonic counter, so the ring is always non-decreasing — no config knob
    /// can perturb it (unlike a time-based delay, where a hot-reloaded interval
    /// could). Cheap to hold, so hold it: otherwise the short-circuit silently
    /// becomes "drop slots on the floor" or gets "fixed" into a full scan on
    /// every WAL batch.
    #[test]
    fn test_drain_stops_at_first_unpublished_and_keeps_the_rest() {
        let engine = engine_past_first_publish(safety_net_config());

        // Ready, ready, NOT-ready, ready — in ring order.
        use crate::concurrent_engine::VerifyGate::AfterPublish;
        engine.push_activation_verify_gated_for_test(&[1, 2], AfterPublish(0));
        engine.push_activation_verify_gated_for_test(&[3], AfterPublish(u64::MAX));
        engine.push_activation_verify_gated_for_test(&[4], AfterPublish(0));

        let drained = engine.drain_activation_verify(100);
        assert_eq!(
            drained,
            vec![1, 2],
            "the drain must stop at the first unpublished slot rather than skip it"
        );
        assert_eq!(
            engine.activation_verify_len(),
            2,
            "the unpublished slot AND everything behind it stay queued — a slot \
             must never be silently discarded by the short-circuit"
        );
    }

    /// Post-activation verifier (deferred activation-miss backstop): an ALIVE
    /// slot whose stored doc carries a postId but whose postId FILTER bitmap was
    /// never set (the orphan signature — activated but the postId insert dropped)
    /// must be detected and re-driven from its doc so it becomes queryable under
    /// its postId. Healthy slots (already indexed) are checked but not re-driven.
    #[test]
    fn test_verify_recent_activations_redrives_postid_orphan() {
        let mut config = safety_net_config();
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        // Membership field is config-driven (no baked-in "postId").
        config.activation_verify.membership_field = Some("postId".into());
        let engine = ConcurrentEngine::new(config).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let now = unit_now_secs() as i64;
        let post_id: i64 = 4242;
        let orphan: u32 = 70;
        let healthy: u32 = 71;

        let postid_ids = |engine: &ConcurrentEngine| -> Vec<i64> {
            let q = crate::query::BitdexQuery {
                filters: vec![crate::query::FilterClause::Eq(
                    "postId".into(),
                    crate::query::Value::Integer(post_id),
                )],
                sort: None,
                limit: 1000,
                offset: None,
                cursor: None,
                skip_cache: true,
            };
            engine.execute_query(&q).map(|r| r.ids).unwrap_or_default()
        };

        // Healthy slot: a normal insert — postId set in BOTH bitmap and doc.
        {
            let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
            let mut dw = DocWriter::new(engine.docstore_arc());
            let mut batch = vec![EntityOps {
                entity_id: healthy as i64,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "postId".into(), value: json!(post_id) },
                    Op::Set { field: "existedAt".into(), value: json!(now - 100) },
                    Op::Set { field: "publishedAt".into(), value: json!(now - 10) },
                ],
            }];
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
            crate::ingester::BitmapSink::flush(&mut sink).unwrap();
            dw.flush();
        }
        wait_for_alive_slot(&engine, healthy, 5_000);

        // Orphan slot: alive with existedAt+publishedAt but NO postId bitmap;
        // postId written to the DOC only — the activation-miss orphan state.
        {
            let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
            let mut dw = DocWriter::new(engine.docstore_arc());
            let mut batch = vec![EntityOps {
                entity_id: orphan as i64,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "existedAt".into(), value: json!(now - 100) },
                    Op::Set { field: "publishedAt".into(), value: json!(now - 10) },
                ],
            }];
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
            crate::ingester::BitmapSink::flush(&mut sink).unwrap();
            dw.write_set(orphan, "postId", &json!(post_id));
            // Simulate the stale-doc divergence: doc shadow FALSE while the
            // (insert-set) isPublished bitmap is true — the state the Q3
            // coherence write must repair on re-drive.
            dw.write_set(orphan, "isPublished", &json!(false));
            dw.flush();
        }
        wait_for_alive_slot(&engine, orphan, 5_000);
        // Precondition: the orphan's stored doc shadow is the stale false.
        let orphan_doc_before = engine.get_document(orphan).unwrap().unwrap();
        assert!(
            matches!(
                orphan_doc_before.fields.get("isPublished"),
                Some(crate::mutation::FieldValue::Single(crate::query::Value::Bool(false)))
            ),
            "setup: orphan doc isPublished should be stale-false"
        );

        // Setup precondition: only the healthy slot is in postId; orphan is not.
        let before = postid_ids(&engine);
        assert!(before.contains(&(healthy as i64)), "healthy must be in postId, got {before:?}");
        assert!(!before.contains(&(orphan as i64)), "orphan must NOT be in postId yet, got {before:?}");

        // Queue both and run the verifier.
        engine.push_activation_verify_for_test(&[orphan, healthy]);
        let outcome = verify_recent_activations(&engine, 100);
        assert_eq!(outcome.checked, 2, "both slots have a postId doc value → both checked");
        assert_eq!(
            outcome.redriven, 1,
            "only the orphan (absent from postId) is re-driven"
        );
        // SAFETY PROPERTY: a slot genuinely absent after the barrier is a real
        // orphan and must never be mistaken for publish lag and skipped.
        assert_eq!(
            outcome.publish_lag, 0,
            "a slot still absent after the barrier is a real orphan, not publish lag"
        );
        // ...and it is re-driven exactly once, under whichever absent verdict:
        // the three counters partition the not-already-indexed slots.
        assert_eq!(
            outcome.redriven + outcome.inconclusive,
            1,
            "the absent orphan is re-driven exactly once, and counted exactly once"
        );
        assert_eq!(
            outcome.inconclusive, 0,
            "the 2s barrier completes on an idle test engine, so the absent orphan \
             is a CONFIRMED drop rather than inconclusive"
        );

        // The re-drive was sent through the mutation channel; wait for apply.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            if postid_ids(&engine).contains(&(orphan as i64)) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "orphan never re-indexed under postId after verify re-drive"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        // Q3 doc/bitmap coherence: the re-drive must have written the derived
        // isPublished shadow into the stored doc to match the bitmap (which the
        // insert set true). GET /documents must no longer serve the stale false.
        let orphan_doc_after = engine.get_document(orphan).unwrap().unwrap();
        assert!(
            matches!(
                orphan_doc_after.fields.get("isPublished"),
                Some(crate::mutation::FieldValue::Single(crate::query::Value::Bool(true)))
            ),
            "re-drive must repair the doc shadow to true (doc/bitmap agreement), got {:?}",
            orphan_doc_after.fields.get("isPublished")
        );
        // And the bitmap agrees: orphan is in isPublished=true.
        let pub_true = {
            let q = crate::query::BitdexQuery {
                filters: vec![crate::query::FilterClause::And(vec![
                    crate::query::FilterClause::Eq("postId".into(), crate::query::Value::Integer(post_id)),
                    crate::query::FilterClause::Eq("isPublished".into(), crate::query::Value::Bool(true)),
                ])],
                sort: None,
                limit: 1000,
                offset: None,
                cursor: None,
                skip_cache: true,
            };
            engine.execute_query(&q).map(|r| r.ids).unwrap_or_default()
        };
        assert!(
            pub_true.contains(&(orphan as i64)),
            "orphan must be in postId AND isPublished=true after re-drive, got {pub_true:?}"
        );
    }

    /// The suppression's safety property, at the decision level: ONLY a re-read
    /// that PROVES the slot present skips the re-drive. Every other verdict —
    /// including the ambiguous ones — re-drives, so a genuine orphan cannot be
    /// mistaken for publish lag.
    ///
    /// Covers the full (barrier_ok × reread_present) square, since the barrier
    /// is NOT reliably longer than a publish (prod: completes at +54ms, times
    /// out at +501ms, ~1-in-2 in one window) and its outcome changes what an
    /// absent read is allowed to mean.
    #[test]
    fn test_orphan_verdict_only_suppresses_proven_present() {
        // ── present ⇒ PublishLag, however the barrier went ────────────────
        // A set bit is proof on its own: nothing can publish a bit that was
        // never set, so a present slot was never lost.
        assert_eq!(
            OrphanVerdict::classify(true, true, false),
            OrphanVerdict::PublishLag
        );
        assert_eq!(
            OrphanVerdict::classify(true, true, true),
            OrphanVerdict::PublishLag,
            "present after the barrier is present — remove ops don't make a live slot an orphan"
        );
        assert_eq!(
            OrphanVerdict::classify(false, true, false),
            OrphanVerdict::PublishLag,
            "a positive re-read proves presence even when the barrier timed out"
        );
        assert_eq!(
            OrphanVerdict::classify(false, true, true),
            OrphanVerdict::PublishLag
        );

        // ── absent + barrier COMPLETED ⇒ confirmed drop, kind by removes ──
        assert_eq!(
            OrphanVerdict::classify(true, false, false),
            OrphanVerdict::NonApply
        );
        assert_eq!(
            OrphanVerdict::classify(true, false, true),
            OrphanVerdict::Revert
        );

        // ── absent + barrier TIMED OUT ⇒ Inconclusive, either way ─────────
        // Absent was never established against a published snapshot, so a drop
        // and an over-long publish lag are indistinguishable — and the remove
        // ring can't break the tie (a Revert conclusion needs the same proof of
        // publish that NonApply does).
        assert_eq!(
            OrphanVerdict::classify(false, false, false),
            OrphanVerdict::Inconclusive,
            "absent behind a timed-out barrier is not proof of a drop"
        );
        assert_eq!(
            OrphanVerdict::classify(false, false, true),
            OrphanVerdict::Inconclusive
        );

        // ── SAFETY: everything not proven present still re-drives ─────────
        assert!(
            !OrphanVerdict::PublishLag.redrives(),
            "a slot proven present was never lost — re-driving it is wasted work"
        );
        assert!(OrphanVerdict::NonApply.redrives());
        assert!(OrphanVerdict::Revert.redrives());
        assert!(
            OrphanVerdict::Inconclusive.redrives(),
            "an unproven slot must still be re-driven — the re-drive is idempotent, \
             so the cost of being wrong is a no-op, not a lost post"
        );
    }

    /// Counter routing: `redriven_total` must mean "a confirmed drop appeared"
    /// so an operator can alarm on it. An Inconclusive re-drives, but it is not
    /// proof of a drop, so it must route to `inconclusive_total` and leave
    /// `redriven_total` alone — otherwise a publish lag longer than the barrier
    /// (routinely half of them, in prod) would fire that alarm falsely.
    #[test]
    fn test_verdict_counter_routing_is_exclusive_and_exhaustive() {
        // Exactly one counter per verdict, and each is the intended one.
        let route = |v: OrphanVerdict| -> (bool, bool, bool) {
            // (publish_lag, redriven, inconclusive) — mirrors the increment
            // arms in verify_recent_activations.
            if !v.redrives() {
                (true, false, false)
            } else if v.is_confirmed_drop() {
                (false, true, false)
            } else {
                (false, false, true)
            }
        };
        assert_eq!(route(OrphanVerdict::PublishLag), (true, false, false));
        assert_eq!(route(OrphanVerdict::NonApply), (false, true, false));
        assert_eq!(route(OrphanVerdict::Revert), (false, true, false));
        assert_eq!(
            route(OrphanVerdict::Inconclusive),
            (false, false, true),
            "an inconclusive re-drive must NOT touch redriven_total"
        );

        for v in [
            OrphanVerdict::PublishLag,
            OrphanVerdict::NonApply,
            OrphanVerdict::Revert,
            OrphanVerdict::Inconclusive,
        ] {
            let (lag, red, inc) = route(v);
            assert_eq!(
                u8::from(lag) + u8::from(red) + u8::from(inc),
                1,
                "{v:?} must land in exactly one counter"
            );
            // The counters are consistent with the safety property: everything
            // counted as anything other than publish lag was re-driven.
            assert_eq!(red || inc, v.redrives(), "{v:?}: counted-as vs re-driven disagree");
        }

        // Only a CONFIRMED drop is alarm-worthy.
        assert!(!OrphanVerdict::Inconclusive.is_confirmed_drop());
        assert!(!OrphanVerdict::PublishLag.is_confirmed_drop());
        assert!(OrphanVerdict::NonApply.is_confirmed_drop());
        assert!(OrphanVerdict::Revert.is_confirmed_drop());
    }

    /// FALSE ORPHAN (prod v1.1.46: 2/2 classified apparent orphans were this):
    /// a slot whose activation batch HAS been applied but is not yet published
    /// reads as absent, and must NOT be re-driven — the post-publish barrier
    /// proves it present, so it counts as publish lag instead.
    ///
    /// The unpublished-but-applied state is staged the way prod produces it: a
    /// mutation sits in the channel while the flush thread has yet to publish
    /// it. The flush cycle is widened so that window is observable (at the 50µs
    /// default it closes before the verifier can read), and the attempt is
    /// retried if the flush thread publishes first and leaves no lag to see.
    #[test]
    fn test_verify_publish_lag_slot_is_not_redriven() {
        let mut config = safety_net_config();
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        config.activation_verify.membership_field = Some("postId".into());
        // Hold the flush thread's publish long enough to read ahead of it.
        config.flush_interval_us = 100_000;
        // The barrier must outlast the widened cycle (prod's 2s default is
        // sized against a ~200ms promote, not a 100ms-1s test flush loop).
        config.activation_verify.publish_barrier_ms = 5_000;
        let engine = ConcurrentEngine::new(config).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let now = unit_now_secs() as i64;
        let post_id: i64 = 5150;
        let slot: u32 = 80;

        let postid_ids = |engine: &ConcurrentEngine| -> Vec<i64> {
            let q = crate::query::BitdexQuery {
                filters: vec![crate::query::FilterClause::Eq(
                    "postId".into(),
                    crate::query::Value::Integer(post_id),
                )],
                sort: None,
                limit: 1000,
                offset: None,
                cursor: None,
                skip_cache: true,
            };
            engine.execute_query(&q).map(|r| r.ids).unwrap_or_default()
        };

        // Alive, doc carries the postId, postId bitmap not yet set — the state
        // an activated slot is in until its batch publishes.
        {
            let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
            let mut dw = DocWriter::new(engine.docstore_arc());
            let mut batch = vec![EntityOps {
                entity_id: slot as i64,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "existedAt".into(), value: json!(now - 100) },
                    Op::Set { field: "publishedAt".into(), value: json!(now - 10) },
                ],
            }];
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
            crate::ingester::BitmapSink::flush(&mut sink).unwrap();
            dw.write_set(slot, "postId", &json!(post_id));
            dw.flush();
        }
        wait_for_alive_slot(&engine, slot, 5_000);
        assert!(
            !postid_ids(&engine).contains(&(slot as i64)),
            "setup: postId must not be published yet"
        );

        let filter_op = |slots: Vec<u32>, insert: bool| {
            if insert {
                crate::write_coalescer::MutationOp::FilterInsert {
                    field: "postId".into(),
                    value: post_id as u64,
                    slots,
                }
            } else {
                crate::write_coalescer::MutationOp::FilterRemove {
                    field: "postId".into(),
                    value: post_id as u64,
                    slots,
                }
            }
        };

        let mut lag_observed = false;
        for _ in 0..5 {
            // The activation batch's postId insert: in the channel, applied by
            // the flush thread, not yet published.
            engine
                .mutation_sender()
                .send(filter_op(vec![slot], true))
                .unwrap();
            engine.push_activation_verify_for_test(&[slot]);
            let outcome = verify_recent_activations(&engine, 100);
            assert_eq!(
                outcome.redriven, 0,
                "a slot the barrier proves present was never lost — re-driving it is the \
                 false-orphan wolf-cry this fix removes"
            );
            if outcome.publish_lag == 1 {
                lag_observed = true;
                break;
            }
            // The flush thread published before the verifier's first read, so
            // there was no lag to observe (outcome: healthy, checked only).
            // Undo the insert and try again.
            engine
                .mutation_sender()
                .send(filter_op(vec![slot], false))
                .unwrap();
            assert!(engine.force_publish_blocking(std::time::Duration::from_secs(5)));
        }
        assert!(
            lag_observed,
            "never caught the verifier reading ahead of the publish — the test staged \
             no publish-lag window, so it proved nothing"
        );
        // And the batch did land on its own: no re-drive was needed.
        assert!(
            postid_ids(&engine).contains(&(slot as i64)),
            "the applied batch must be published after the barrier"
        );
    }

    /// The post-activation verify ring must survive restart: a slot queued for
    /// verification, persisted, then reloaded on boot must still be re-checked
    /// and re-driven. This closes the in-memory ring's boot-gap — an orphan
    /// created just before a crash would otherwise never be re-checked (the
    /// overdue sweep can't see an activation orphan).
    #[test]
    fn test_activation_verify_ring_survives_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let docstore_path = dir.path().join("docs");
        let bitmap_path = dir.path().join("bitmaps");
        let mut config = safety_net_config();
        config.storage.bitmap_path = Some(bitmap_path);
        config.merge_interval_ms = 200;
        config.filter_fields.push(FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        });
        config.activation_verify.membership_field = Some("postId".into());

        let post_id: i64 = 909;
        let orphan: u32 = 88;
        let now = unit_now_secs() as i64;

        let postid_ids = |engine: &ConcurrentEngine| -> Vec<i64> {
            let q = crate::query::BitdexQuery {
                filters: vec![crate::query::FilterClause::Eq(
                    "postId".into(),
                    crate::query::Value::Integer(post_id),
                )],
                sort: None,
                limit: 1000,
                offset: None,
                cursor: None,
                skip_cache: true,
            };
            engine.execute_query(&q).map(|r| r.ids).unwrap_or_default()
        };

        // --- First engine: manufacture the orphan, queue + persist the ring ---
        {
            let engine = ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            let meta = FieldMeta::from_config(engine.config());
            let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
            let mut dw = DocWriter::new(engine.docstore_arc());
            let mut batch = vec![EntityOps {
                entity_id: orphan as i64,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "existedAt".into(), value: json!(now - 100) },
                    Op::Set { field: "publishedAt".into(), value: json!(now - 10) },
                ],
            }];
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
            crate::ingester::BitmapSink::flush(&mut sink).unwrap();
            dw.write_set(orphan, "postId", &json!(post_id));
            dw.flush();
            wait_for_alive_slot(&engine, orphan, 5_000);
            assert!(
                !postid_ids(&engine).contains(&(orphan as i64)),
                "setup: orphan must not be in postId"
            );
            // Queue for verification and persist the ring (what the flush thread
            // does on an activation cycle), then let a merge persist bitmaps.
            //
            // Tagged with a HIGH publish count, the way a real slot enqueued
            // after thousands of flush cycles would be. Realistic, but NOT what
            // makes this test able to fail — Mutation H hardcodes the tag at the
            // LOAD site, so the test would fail at AfterPublish(0) too. What
            // makes it able to fail is the re-stamp on load being the only thing
            // that clears a tag the new process cannot reach; the fixture just
            // looks like production while doing it.
            engine.push_activation_verify_gated_for_test(
                &[orphan],
                crate::concurrent_engine::VerifyGate::AfterPublish(5_000),
            );
            engine.persist_activation_verify_for_test();
            std::thread::sleep(std::time::Duration::from_millis(400));
            drop(engine);
        }

        // --- Second engine: boot re-seeds the ring; verifier re-drives ---
        let engine2 = ConcurrentEngine::new_with_path(config, &docstore_path).unwrap();
        assert!(engine2.is_slot_alive(orphan), "orphan must be alive after restart");
        assert!(
            !postid_ids(&engine2).contains(&(orphan as i64)),
            "orphan still not in postId after restart (bitmap was never set)"
        );

        // Deliberately NOT waiting for a publish: a restored slot has no
        // in-flight batch, so it must be verifiable immediately. Requiring a
        // publish here would strand it on any index quiet enough not to write.
        assert_eq!(
            engine2.published_seq(),
            0,
            "precondition: a freshly booted, idle engine has published nothing — \
             which is exactly the state a restored slot must be verifiable in"
        );

        let outcome = verify_recent_activations(&engine2, 100);
        // THE REHYDRATE SEAM: the slot was enqueued at count 5000, but this
        // process's counter restarted at 0. It is verifiable only because the
        // loader re-stamped the tag. Persist the tag verbatim instead and this
        // assertion reads checked=0 — the slot stranded in the ring forever,
        // silently, which is the failure with no signal attached.
        assert_eq!(
            outcome.checked, 1,
            "a slot restored from the persisted ring must still be verified — if this \
             is 0, its publish-count tag survived the restart and now names a count \
             this process will never reach, so the slot is stranded unverified with \
             no log, no counter and no alarm"
        );
        assert_eq!(outcome.redriven, 1, "the reloaded orphan must be re-driven");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            if postid_ids(&engine2).contains(&(orphan as i64)) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "orphan never re-indexed under postId after restart re-drive"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// Overdue-deferred sweep (fix A4): a slot that is alive with its shadow
    /// stuck false and a stored past publishedAt (the lost-activation state)
    /// must be healed; genuine drafts (no stored publishedAt) and legitimately
    /// deferred slots (future publishedAt) must be left alone.
    #[test]
    fn test_overdue_deferred_sweep_heals_stuck_slots_only() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let now = unit_now_secs() as i64;
        let stuck: u32 = 101; // past publishedAt, shadow false → heal
        let draft: u32 = 102; // no publishedAt → skip
        let future: u32 = 103; // future publishedAt → skip

        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        // All three inserted alive with an explicit shadow=false bit (as the
        // dump path's enrichment writes it for unpublished images).
        let mut batch: Vec<EntityOps> = [stuck, draft, future]
            .iter()
            .map(|&s| EntityOps {
                entity_id: s as i64,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "existedAt".into(), value: json!(now - 500) },
                    Op::Set { field: "isPublished".into(), value: json!(false) },
                ],
            })
            .collect();
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        // Forge the lost-activation docs (deferred branch writes the doc only).
        dw.write_set(stuck, "publishedAt", &json!(now - 100));
        dw.write_set(future, "publishedAt", &json!(now + 3_600));
        sink.flush().unwrap();
        dw.flush();
        // Wait for the flush thread to publish alive + shadow-false bits.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(2_000);
        while std::time::Instant::now() < deadline {
            if engine.is_slot_alive(future) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(engine.is_slot_alive(stuck), "test rig: stuck slot must be alive");
        // The forged writes bypass DocCache and the flush thread's
        // write-through can repopulate stale entries; evict AFTER the flush
        // thread has settled so the sweep's get_document reads the disk state.
        engine.evict_doc_cache(stuck);
        engine.evict_doc_cache(future);

        let mut rec = RecordingSink::new();
        let mut dw2 = DocWriter::new(engine.docstore_arc());
        let (checked, healed, cursor) =
            overdue_deferred_sweep(&mut rec, &meta, &engine, &mut dw2, 1_000, None);
        dw2.flush();
        assert!(checked >= 3, "all three shadow-false slots are candidates, got {checked}");
        assert!(cursor.is_none(), "candidate space fits in one pass — no resume cursor");
        assert_eq!(healed, vec![stuck], "exactly the stuck slot must be healed");
        // Heal emits the shadow flip and the publishedAt sort layer for `stuck`.
        assert!(
            rec.filter_inserts.contains(&("isPublished".to_string(), 1u64, stuck)),
            "sweep must flip isPublished=true for the stuck slot"
        );
        assert!(
            rec.sort_sets.iter().any(|(f, _, s)| f == "publishedAt" && *s == stuck),
            "sweep must write the publishedAt sort layer for the stuck slot"
        );
        assert!(
            !rec.filter_inserts.iter().any(|(f, v, s)| f == "isPublished" && *v == 1 && (*s == draft || *s == future)),
            "draft and future slots must not be flipped"
        );
    }

    /// Shared rig for the page-cap regression tests: `n_decoys` shadow-false
    /// drafts with RECENT existedAt (high sortAt — they own the head of the
    /// Desc-sorted candidate list and are never healed, drafts have no stored
    /// publishedAt) plus one stuck slot with OLD existedAt (tail of the list)
    /// and a forged past publishedAt doc (the lost-activation state).
    /// Returns (engine, meta, stuck_slot).
    fn page_cap_rig(n_decoys: u32) -> (ConcurrentEngine, FieldMeta, u32) {
        let mut config = safety_net_config();
        config.max_page_size = 5;
        let engine = ConcurrentEngine::new(config).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let now = unit_now_secs() as i64;
        let stuck: u32 = 1;
        let decoys: Vec<u32> = (2..2 + n_decoys).collect();

        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch: Vec<EntityOps> = Vec::new();
        // Stuck slot: oldest existedAt → LOWEST sortAt → last in Desc order.
        batch.push(EntityOps {
            entity_id: stuck as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "existedAt".into(), value: json!(now - 100_000) },
                Op::Set { field: "isPublished".into(), value: json!(false) },
            ],
        });
        for (i, &d) in decoys.iter().enumerate() {
            batch.push(EntityOps {
                entity_id: d as i64,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "existedAt".into(), value: json!(now - 10 - i as i64) },
                    Op::Set { field: "isPublished".into(), value: json!(false) },
                ],
            });
        }
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        // Forge the lost activation: doc carries a past publishedAt but the
        // shadow/sort bitmaps were never written.
        dw.write_set(stuck, "publishedAt", &json!(now - 50));
        sink.flush().unwrap();
        dw.flush();
        let last_decoy = *decoys.last().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(2_000);
        while std::time::Instant::now() < deadline {
            if engine.is_slot_alive(last_decoy) && engine.is_slot_alive(stuck) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(engine.is_slot_alive(stuck), "test rig: stuck slot must be alive");
        assert!(engine.is_slot_alive(last_decoy), "test rig: decoys must be alive");
        // The forged write bypasses DocCache and the flush thread's
        // write-through can repopulate a stale entry; evict AFTER the flush
        // thread has settled so the sweep's get_document reads the disk state.
        engine.evict_doc_cache(stuck);
        (engine, meta, stuck)
    }

    /// Regression (2026-07-14 page-cap): `execute_query` clamps limit to
    /// `max_page_size`, so the old single-query sweep only ever saw the head
    /// of the Desc-sorted candidate list. A stuck slot ranked below the first
    /// page was never reached (45 posts sat 15h in prod). One sweep pass with
    /// `limit` > population must paginate past the clamp and heal it.
    #[test]
    fn test_overdue_deferred_sweep_paginates_past_page_cap() {
        // 12 decoys ahead of the stuck slot; page cap 5 → stuck is on page 3.
        let (engine, meta, stuck) = page_cap_rig(12);
        let mut rec = RecordingSink::new();
        let mut dw = DocWriter::new(engine.docstore_arc());
        let (checked, healed, cursor) =
            overdue_deferred_sweep(&mut rec, &meta, &engine, &mut dw, 1_000, None);
        dw.flush();
        assert!(
            checked >= 13,
            "sweep must page through ALL 13 candidates, not stop at the 5-cap, got {checked}"
        );
        assert_eq!(healed, vec![stuck], "the stuck slot beyond page 1 must be healed");
        assert!(cursor.is_none(), "space exhausted in one pass — no resume cursor");
    }

    /// Regression (2026-07-14 page-cap): when `sweep_limit` is smaller than
    /// the candidate population, the returned cursor must rotate the scan
    /// window forward so successive cycles reach the tail — instead of
    /// re-scanning the same head forever (the prod failure mode).
    #[test]
    fn test_overdue_deferred_sweep_cursor_rotates_across_cycles() {
        let (engine, meta, stuck) = page_cap_rig(12);
        let mut cursor: Option<crate::query::CursorPosition> = None;
        let mut healed_all: Vec<u32> = Vec::new();
        let mut cycles = 0usize;
        // limit=5 per cycle over 13 candidates → stuck must heal within
        // ceil(13/5)=3 cycles. Allow one extra for the wrap.
        for _ in 0..4 {
            cycles += 1;
            let mut rec = RecordingSink::new();
            let mut dw = DocWriter::new(engine.docstore_arc());
            let (_, healed, next) =
                overdue_deferred_sweep(&mut rec, &meta, &engine, &mut dw, 5, cursor.take());
            dw.flush();
            healed_all.extend(healed);
            cursor = next;
            if healed_all.contains(&stuck) {
                break;
            }
            assert!(
                cursor.is_some() || cycles >= 3,
                "mid-rotation cycles must return a resume cursor (cycle {cycles})"
            );
        }
        assert_eq!(
            healed_all,
            vec![stuck],
            "rotating cursor must reach the stuck slot within {cycles} cycles"
        );
        assert!(cycles <= 3, "coverage must be bounded: healed in {cycles} cycles, expected <=3");
    }

    /// Regression (audit 2026-07-07 §3.1): deleting a deferred slot must
    /// cancel the pending activation — otherwise activate_due resurrects the
    /// deleted slot by replaying its stored doc.
    #[test]
    fn test_deferred_slot_delete_cancels_activation() {
        let engine = ConcurrentEngine::new(safety_net_config()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let slot: u32 = 78;
        let now = unit_now_secs() as i64;

        let mut sink = crate::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: true,
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(now + 3_600) }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        sink.flush().unwrap();
        dw.flush();
        wait_for_deferred(&engine, slot, 2_000);

        let mut rec = RecordingSink::new();
        let mut batch2 = vec![EntityOps {
            entity_id: slot as i64,
            creates_slot: false,
            ops: vec![Op::Delete],
        }];
        let (applied2, _, errors2) =
            apply_ops_batch(&mut rec, &meta, &mut batch2, Some(&engine), None);
        assert_eq!((applied2, errors2), (1, 0));
        assert_eq!(
            rec.deferred_cancels,
            vec![slot],
            "delete of a deferred slot must cancel its pending activation"
        );
    }

    #[test]
    fn test_deferred_alive_past_publishedat() {
        use crate::config::DeferredAliveConfig;
        let mut config = test_config();
        config.deferred_alive = Some(DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
        sweep_interval_secs: 0,
        sweep_limit: 20_000,
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
        // sortAt should have a definitive write (set OR clear) for EVERY bit
        // based on new_computed = max(500, 2000) = 2000. The recompute path no
        // longer relies on old_computed for clearing — full overwrite ensures
        // the bitmap state equals new_computed exactly regardless of history.
        let sort_at_clears: Vec<_> = sink.sort_clears.iter()
            .filter(|(f, _, _)| f == "sortAt")
            .collect();
        let sort_at_sets: Vec<_> = sink.sort_sets.iter()
            .filter(|(f, _, _)| f == "sortAt")
            .collect();
        let new_val: u32 = 2000;
        for bit in 0..32 {
            if (new_val >> bit) & 1 == 1 {
                assert!(
                    sort_at_sets.iter().any(|(_, b, s)| *b == bit && *s == 10),
                    "should set bit {bit} of new sortAt value {new_val}"
                );
                assert!(
                    !sort_at_clears.iter().any(|(_, b, s)| *b == bit && *s == 10),
                    "must NOT clear bit {bit} of new sortAt value {new_val}"
                );
            } else {
                assert!(
                    sort_at_clears.iter().any(|(_, b, s)| *b == bit && *s == 10),
                    "should clear bit {bit} (zero in new sortAt value {new_val})"
                );
                assert!(
                    !sort_at_sets.iter().any(|(_, b, s)| *b == bit && *s == 10),
                    "must NOT set bit {bit} (zero in new sortAt value {new_val})"
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
        assert_eq!(json_to_packed(&json!(null)), Some(PackedValue::Null));
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
