use std::collections::HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::config::{ComputedOp, ComputedField, Config};
use crate::docstore::{DocStore, StoredDoc};
use crate::error::{BitdexError, Result};
use crate::filter::FilterIndex;
use crate::query::Value;
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;
use crate::write_coalescer::MutationOp;

/// A document mutation payload for PUT operations.
/// Contains field name -> value mappings for both filter and sort fields.
/// Bitdex does NOT store these values; they are consumed to set bitmap bits.
#[derive(Debug, Clone)]
pub struct Document {
    pub fields: HashMap<String, FieldValue>,
}

/// A field value in a mutation payload.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FieldValue {
    /// Single value for single_value and boolean fields.
    Single(Value),
    /// Multiple values for multi_value fields (e.g., tags).
    Multi(Vec<Value>),
}

/// A partial update payload for PATCH operations.
/// Contains only the changed fields with old and new values.
#[derive(Debug, Clone)]
pub struct PatchPayload {
    pub fields: HashMap<String, PatchField>,
}

/// A single field change in a PATCH operation.
/// Both old and new values come from the WAL event -- we never look up stored state.
#[derive(Debug, Clone)]
pub struct PatchField {
    pub old: FieldValue,
    pub new: FieldValue,
}

/// Registry of interned field names. Built once from Config at engine construction.
/// Cloning an Arc<str> is just an atomic increment -- essentially free.
#[derive(Debug, Clone)]
pub struct FieldRegistry {
    fields: HashMap<String, Arc<str>>,
}

impl FieldRegistry {
    /// Build a FieldRegistry from a Config, interning all filter and sort field names.
    pub fn from_config(config: &Config) -> Self {
        let mut fields = HashMap::new();
        for fc in &config.filter_fields {
            fields
                .entry(fc.name.clone())
                .or_insert_with(|| Arc::from(fc.name.as_str()));
        }
        for sc in &config.sort_fields {
            fields
                .entry(sc.name.clone())
                .or_insert_with(|| Arc::from(sc.name.as_str()));
        }
        Self { fields }
    }

    /// Get the interned Arc<str> for a field name, or create one on the fly.
    pub fn get(&self, name: &str) -> Arc<str> {
        self.fields
            .get(name)
            .cloned()
            .unwrap_or_else(|| Arc::from(name))
    }
}

/// Pure diff function: given old doc (if any), new doc, config, field registry, and slot ID,
/// returns the list of MutationOps needed to update bitmaps.
///
/// This does NOT touch any bitmap state -- it only computes what mutations
/// are needed. Used by ConcurrentEngine to send ops to the coalescer channel.
pub fn diff_document(
    slot: u32,
    old_doc: Option<&StoredDoc>,
    new_doc: &Document,
    config: &Config,
    is_upsert: bool,
    registry: &FieldRegistry,
) -> Vec<MutationOp> {
    // Deferred alive check FIRST: if the document should be deferred, return
    // only the DeferredAlive op (plus cleanup ops if upsert) with NO new bitmap
    // mutations. The document remains completely invisible (no filter/sort bits)
    // until activation, at which point the full mutation pipeline is replayed
    // from the stored doc.
    if let Some(ref da_config) = config.deferred_alive {
        if let Some(fv) = new_doc.fields.get(&da_config.source_field) {
            if let FieldValue::Single(Value::Integer(ts)) = fv {
                let mut activate_at = *ts as u64;
                if da_config.ms_to_seconds {
                    activate_at /= 1000;
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if activate_at > now {
                    let mut ops = Vec::new();

                    // If this is an upsert (old doc exists with live bitmaps),
                    // we must clear all old filter/sort bits and the alive bit.
                    // Otherwise the document stays visible with stale data until
                    // activation replays the full mutation pipeline.
                    if let Some(old) = old_doc {
                        for filter_config in &config.filter_fields {
                            if let Some(old_val) = old.fields.get(&filter_config.name) {
                                let arc_name = registry.get(&filter_config.name);
                                collect_filter_remove_ops(&mut ops, &arc_name, slot, old_val);
                            }
                        }
                        for sort_config in &config.sort_fields {
                            if let Some(old_val) = old.fields.get(&sort_config.name) {
                                if let FieldValue::Single(val) = old_val {
                                    if let Some(old_s) = value_to_sort_u32(val) {
                                        let arc_name = registry.get(&sort_config.name);
                                        let num_bits = sort_config.bits as usize;
                                        for bit in 0..num_bits {
                                            if (old_s >> bit) & 1 == 1 {
                                                ops.push(MutationOp::SortClear {
                                                    field: arc_name.clone(),
                                                    bit_layer: bit,
                                                    slots: vec![slot],
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        ops.push(MutationOp::AliveRemove { slots: vec![slot] });
                    }

                    ops.push(MutationOp::DeferredAlive {
                        slot,
                        activate_at,
                    });
                    return ops;
                }
            }
        }
    }

    let mut ops = Vec::new();

    if is_upsert {
        // Upsert: diff old vs new, only emit ops for changed fields
        let empty_fields = HashMap::new();
        let old_fields = old_doc.map_or(&empty_fields, |d| &d.fields);

        for filter_config in &config.filter_fields {
            let field_name = &filter_config.name;
            let old_val = old_fields.get(field_name);
            let new_val = new_doc.fields.get(field_name);

            if field_values_equal(old_val, new_val) {
                continue;
            }

            let arc_name = registry.get(field_name);

            // Clear old filter bits
            if let Some(old) = old_val {
                collect_filter_remove_ops(&mut ops, &arc_name, slot, old);
            }
            // Set new filter bits
            if let Some(new) = new_val {
                collect_filter_insert_ops(&mut ops, &arc_name, slot, new);
            }
        }

        for sort_config in &config.sort_fields {
            let (old_sort, new_sort) = if let Some(ref computed) = sort_config.computed {
                // Computed sort field: resolve value from source fields
                resolve_computed_sort(computed, old_fields, &new_doc.fields)
            } else {
                // Direct sort field: read value from document
                let field_name = &sort_config.name;
                let old_val = old_fields.get(field_name);
                let new_val = new_doc.fields.get(field_name);

                if field_values_equal(old_val, new_val) {
                    continue;
                }

                let old_s = old_val.and_then(|v| match v {
                    FieldValue::Single(val) => value_to_sort_u32(val),
                    _ => None,
                });
                let new_s = new_val.and_then(|v| match v {
                    FieldValue::Single(val) => value_to_sort_u32(val),
                    _ => None,
                });
                (old_s, new_s)
            };

            if old_sort == new_sort {
                continue;
            }

            let arc_name = registry.get(&sort_config.name);
            let num_bits = sort_config.bits as usize;
            emit_sort_diff_ops(&mut ops, &arc_name, num_bits, slot, old_sort, new_sort);
        }
    } else {
        // Fresh insert: set all bitmaps, but first clear stale bits if old doc exists
        if let Some(old) = old_doc {
            for filter_config in &config.filter_fields {
                if let Some(old_val) = old.fields.get(&filter_config.name) {
                    let arc_name = registry.get(&filter_config.name);
                    collect_filter_remove_ops(&mut ops, &arc_name, slot, old_val);
                }
            }
            for sort_config in &config.sort_fields {
                if let Some(old_val) = old.fields.get(&sort_config.name) {
                    if let FieldValue::Single(val) = old_val {
                        if let Some(old_s) = value_to_sort_u32(val) {
                            let arc_name = registry.get(&sort_config.name);
                            let num_bits = sort_config.bits as usize;
                            for bit in 0..num_bits {
                                if (old_s >> bit) & 1 == 1 {
                                    ops.push(MutationOp::SortClear {
                                        field: arc_name.clone(),
                                        bit_layer: bit,
                                        slots: vec![slot],
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Set all new bitmaps
        for filter_config in &config.filter_fields {
            if let Some(field_value) = new_doc.fields.get(&filter_config.name) {
                let arc_name = registry.get(&filter_config.name);
                collect_filter_insert_ops(&mut ops, &arc_name, slot, field_value);
            }
        }
        for sort_config in &config.sort_fields {
            let sort_val = if let Some(ref computed) = sort_config.computed {
                // Computed: resolve from source fields in the new doc
                let values: Vec<u32> = computed.source_fields.iter()
                    .filter_map(|f| field_to_sort_u32(&new_doc.fields, f))
                    .collect();
                if values.is_empty() { None } else { Some(apply_computed_op(&computed.op, &values)) }
            } else {
                // Direct: read from document
                new_doc.fields.get(&sort_config.name).and_then(|fv| match fv {
                    FieldValue::Single(val) => value_to_sort_u32(val),
                    _ => None,
                })
            };

            if let Some(sort_val) = sort_val {
                let arc_name = registry.get(&sort_config.name);
                let num_bits = sort_config.bits as usize;
                for bit in 0..num_bits {
                    if (sort_val >> bit) & 1 == 1 {
                        ops.push(MutationOp::SortSet {
                            field: arc_name.clone(),
                            bit_layer: bit,
                            slots: vec![slot],
                        });
                    }
                }
            }
        }
    }

    // Alive insert (for both fresh insert and upsert -- idempotent).
    // If we reach here, the document is not deferred (checked at top of function).
    ops.push(MutationOp::AliveInsert { slots: vec![slot] });

    ops
}

/// Pure diff for partial update (PATCH): like diff_document upsert path,
/// but ONLY processes fields present in new_doc. Missing fields are skipped
/// entirely — they are NOT treated as deletions. This is the key difference
/// from diff_document which treats missing fields as "change to None."
pub fn diff_document_partial(
    slot: u32,
    old_doc: Option<&StoredDoc>,
    new_doc: &Document,
    config: &Config,
    registry: &FieldRegistry,
) -> Vec<MutationOp> {
    // [2.5] Deferred alive check: if the PATCH changes publishedAt to a future
    // date, defer this slot. Same logic as diff_document — clear old bitmaps
    // and defer the alive bit.
    if let Some(ref da_config) = config.deferred_alive {
        if let Some(fv) = new_doc.fields.get(&da_config.source_field) {
            if let FieldValue::Single(Value::Integer(ts)) = fv {
                let mut activate_at = *ts as u64;
                if da_config.ms_to_seconds {
                    activate_at /= 1000;
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if activate_at > now {
                    let mut ops = Vec::new();
                    let empty_fields = HashMap::new();
                    let old_fields = old_doc.map_or(&empty_fields, |d| &d.fields);

                    // Clear old bitmaps if this was a live slot
                    for filter_config in &config.filter_fields {
                        if let Some(old_val) = old_fields.get(&filter_config.name) {
                            let arc_name = registry.get(&filter_config.name);
                            collect_filter_remove_ops(&mut ops, &arc_name, slot, old_val);
                        }
                    }
                    for sort_config in &config.sort_fields {
                        if let Some(old_val) = old_fields.get(&sort_config.name) {
                            if let FieldValue::Single(val) = old_val {
                                if let Some(old_s) = value_to_sort_u32(val) {
                                    let arc_name = registry.get(&sort_config.name);
                                    let num_bits = sort_config.bits as usize;
                                    for bit in 0..num_bits {
                                        if (old_s >> bit) & 1 == 1 {
                                            ops.push(MutationOp::SortClear {
                                                field: arc_name.clone(),
                                                bit_layer: bit,
                                                slots: vec![slot],
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    ops.push(MutationOp::AliveRemove { slots: vec![slot] });
                    ops.push(MutationOp::DeferredAlive { slot, activate_at });
                    return ops;
                }
            }
        }
    }

    let mut ops = Vec::new();
    let empty_fields = HashMap::new();
    let old_fields = old_doc.map_or(&empty_fields, |d| &d.fields);

    for filter_config in &config.filter_fields {
        let field_name = &filter_config.name;
        // PATCH semantics: skip fields not in the new doc
        let new_val = match new_doc.fields.get(field_name) {
            Some(v) => Some(v),
            None => continue,
        };
        let old_val = old_fields.get(field_name);

        if field_values_equal(old_val, new_val) {
            continue;
        }

        let arc_name = registry.get(field_name);
        if let Some(old) = old_val {
            collect_filter_remove_ops(&mut ops, &arc_name, slot, old);
        }
        if let Some(new) = new_val {
            collect_filter_insert_ops(&mut ops, &arc_name, slot, new);
        }
    }

    for sort_config in &config.sort_fields {
        let (old_sort, new_sort) = if let Some(ref computed) = sort_config.computed {
            // Computed sort field: check if any source field is in the PATCH
            let any_source_in_patch = computed.source_fields.iter()
                .any(|f| new_doc.fields.contains_key(f));
            if !any_source_in_patch {
                continue; // PATCH semantics: skip if no source field is being patched
            }
            resolve_computed_sort(computed, old_fields, &new_doc.fields)
        } else {
            let field_name = &sort_config.name;
            // PATCH semantics: skip fields not in the new doc
            let new_val = match new_doc.fields.get(field_name) {
                Some(v) => Some(v),
                None => continue,
            };
            let old_val = old_fields.get(field_name);

            if field_values_equal(old_val, new_val) {
                continue;
            }

            let old_s = old_val.and_then(|v| match v {
                FieldValue::Single(val) => value_to_sort_u32(val),
                _ => None,
            });
            let new_s = new_val.and_then(|v| match v {
                FieldValue::Single(val) => value_to_sort_u32(val),
                _ => None,
            });
            (old_s, new_s)
        };

        if old_sort == new_sort {
            continue;
        }

        let arc_name = registry.get(&sort_config.name);
        let num_bits = sort_config.bits as usize;
        emit_sort_diff_ops(&mut ops, &arc_name, num_bits, slot, old_sort, new_sort);
    }

    ops
}

/// Pure diff for PATCH: given old/new field values, returns MutationOps.
pub fn diff_patch(
    slot: u32,
    patch: &PatchPayload,
    config: &Config,
    registry: &FieldRegistry,
) -> Vec<MutationOp> {
    let mut ops = Vec::new();

    for (field_name, change) in &patch.fields {
        let arc_name = registry.get(field_name);

        // Check if this is a filter field
        let is_filter = config.filter_fields.iter().any(|f| f.name == *field_name);
        if is_filter {
            collect_filter_remove_ops(&mut ops, &arc_name, slot, &change.old);
            collect_filter_insert_ops(&mut ops, &arc_name, slot, &change.new);
        }

        // Check if this is a sort field
        if let Some(sort_config) = config.sort_fields.iter().find(|s| s.name == *field_name) {
            if let (FieldValue::Single(old_val), FieldValue::Single(new_val)) =
                (&change.old, &change.new)
            {
                if let (Some(old_sort), Some(new_sort)) =
                    (value_to_sort_u32(old_val), value_to_sort_u32(new_val))
                {
                    let diff = old_sort ^ new_sort;
                    let num_bits = sort_config.bits as usize;
                    for bit in 0..num_bits {
                        if (diff >> bit) & 1 == 1 {
                            if (new_sort >> bit) & 1 == 1 {
                                ops.push(MutationOp::SortSet {
                                    field: arc_name.clone(),
                                    bit_layer: bit,
                                    slots: vec![slot],
                                });
                            } else {
                                ops.push(MutationOp::SortClear {
                                    field: arc_name.clone(),
                                    bit_layer: bit,
                                    slots: vec![slot],
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    ops
}

/// Collect FilterRemove ops for a field value.
pub fn collect_filter_remove_ops(
    ops: &mut Vec<MutationOp>,
    field_name: &Arc<str>,
    slot: u32,
    val: &FieldValue,
) {
    match val {
        FieldValue::Single(v) => {
            if let Some(key) = value_to_bitmap_key(v) {
                ops.push(MutationOp::FilterRemove {
                    field: field_name.clone(),
                    value: key,
                    slots: vec![slot],
                });
            }
        }
        FieldValue::Multi(vals) => {
            for v in vals {
                if let Some(key) = value_to_bitmap_key(v) {
                    ops.push(MutationOp::FilterRemove {
                        field: field_name.clone(),
                        value: key,
                        slots: vec![slot],
                    });
                }
            }
        }
    }
}

/// Collect FilterInsert ops for a field value.
fn collect_filter_insert_ops(
    ops: &mut Vec<MutationOp>,
    field_name: &Arc<str>,
    slot: u32,
    val: &FieldValue,
) {
    match val {
        FieldValue::Single(v) => {
            if let Some(key) = value_to_bitmap_key(v) {
                ops.push(MutationOp::FilterInsert {
                    field: field_name.clone(),
                    value: key,
                    slots: vec![slot],
                });
            }
        }
        FieldValue::Multi(vals) => {
            for v in vals {
                if let Some(key) = value_to_bitmap_key(v) {
                    ops.push(MutationOp::FilterInsert {
                        field: field_name.clone(),
                        value: key,
                        slots: vec![slot],
                    });
                }
            }
        }
    }
}

/// Compare two optional FieldValues for equality (public for reuse).
fn field_values_equal(a: Option<&FieldValue>, b: Option<&FieldValue>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(FieldValue::Single(va)), Some(FieldValue::Single(vb))) => values_equal(va, vb),
        (Some(FieldValue::Multi(va)), Some(FieldValue::Multi(vb))) => {
            va.len() == vb.len()
                && va.iter().zip(vb.iter()).all(|(a, b)| values_equal(a, b))
        }
        _ => false,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        _ => false,
    }
}

/// Convert a Value to a u64 bitmap key for filter indexing.
pub fn value_to_bitmap_key(val: &Value) -> Option<u64> {
    match val {
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        Value::Integer(v) => Some(*v as u64),
        Value::Float(_) | Value::String(_) => None,
    }
}

/// Convert a Value to a u32 for sort layer bit decomposition.
pub fn value_to_sort_u32(val: &Value) -> Option<u32> {
    match val {
        Value::Integer(v) => Some((*v).max(0) as u32),
        _ => None,
    }
}

/// Extract a u32 sort value from a field in a field map.
fn field_to_sort_u32(fields: &HashMap<String, FieldValue>, name: &str) -> Option<u32> {
    fields.get(name).and_then(|fv| match fv {
        FieldValue::Single(val) => value_to_sort_u32(val),
        _ => None,
    })
}

/// Resolve old and new computed sort values from source fields.
/// For each source field, reads from new_fields if present, else old_fields.
fn resolve_computed_sort(
    computed: &ComputedField,
    old_fields: &HashMap<String, FieldValue>,
    new_fields: &HashMap<String, FieldValue>,
) -> (Option<u32>, Option<u32>) {
    // Check if any source field actually changed
    let any_changed = computed.source_fields.iter().any(|f| {
        let old_val = old_fields.get(f);
        let new_val = new_fields.get(f);
        // Changed if new_fields has this field and it differs from old
        new_val.is_some() && !field_values_equal(old_val, new_val)
    });

    if !any_changed {
        return (None, None); // Caller will see equal values and skip
    }

    // Compute old value from old_fields
    let old_values: Vec<u32> = computed.source_fields.iter()
        .filter_map(|f| field_to_sort_u32(old_fields, f))
        .collect();
    let old_computed = if old_values.is_empty() {
        None
    } else {
        Some(apply_computed_op(&computed.op, &old_values))
    };

    // Compute new value: use new_fields if field is present, else fall back to old_fields
    let new_values: Vec<u32> = computed.source_fields.iter()
        .filter_map(|f| {
            field_to_sort_u32(new_fields, f)
                .or_else(|| field_to_sort_u32(old_fields, f))
        })
        .collect();
    let new_computed = if new_values.is_empty() {
        None
    } else {
        Some(apply_computed_op(&computed.op, &new_values))
    };

    (old_computed, new_computed)
}

/// Apply a computed operation to a set of u32 values.
pub fn apply_computed_op(op: &ComputedOp, values: &[u32]) -> u32 {
    match op {
        ComputedOp::Greatest => values.iter().copied().max().unwrap_or(0),
        ComputedOp::Least => values.iter().copied().min().unwrap_or(0),
    }
}

/// Emit sort layer set/clear ops for a value change on a single slot.
fn emit_sort_diff_ops(
    ops: &mut Vec<MutationOp>,
    field: &Arc<str>,
    num_bits: usize,
    slot: u32,
    old_sort: Option<u32>,
    new_sort: Option<u32>,
) {
    match (old_sort, new_sort) {
        (Some(old_s), Some(new_s)) => {
            let diff = old_s ^ new_s;
            for bit in 0..num_bits {
                if (diff >> bit) & 1 == 1 {
                    if (new_s >> bit) & 1 == 1 {
                        ops.push(MutationOp::SortSet {
                            field: field.clone(),
                            bit_layer: bit,
                            slots: vec![slot],
                        });
                    } else {
                        ops.push(MutationOp::SortClear {
                            field: field.clone(),
                            bit_layer: bit,
                            slots: vec![slot],
                        });
                    }
                }
            }
        }
        (Some(_), None) => {
            for bit in 0..num_bits {
                ops.push(MutationOp::SortClear {
                    field: field.clone(),
                    bit_layer: bit,
                    slots: vec![slot],
                });
            }
        }
        (None, Some(new_s)) => {
            for bit in 0..num_bits {
                if (new_s >> bit) & 1 == 1 {
                    ops.push(MutationOp::SortSet {
                        field: field.clone(),
                        bit_layer: bit,
                        slots: vec![slot],
                    });
                }
            }
        }
        (None, None) => {}
    }
}

/// The core mutation engine. Applies PUT/PATCH/DELETE/DELETE WHERE to bitmaps.
pub struct MutationEngine<'a> {
    slots: &'a mut SlotAllocator,
    filters: &'a mut FilterIndex,
    sorts: &'a mut SortIndex,
    config: &'a Config,
    docstore: &'a mut DocStore,
}

impl<'a> MutationEngine<'a> {
    pub fn new(
        slots: &'a mut SlotAllocator,
        filters: &'a mut FilterIndex,
        sorts: &'a mut SortIndex,
        config: &'a Config,
        docstore: &'a mut DocStore,
    ) -> Self {
        Self {
            slots,
            filters,
            sorts,
            config,
            docstore,
        }
    }

    /// PUT(id, document) -- full replace with upsert semantics.
    ///
    /// If slot is alive (upsert): read old doc from docstore, diff each field,
    /// update only the bitmaps that actually changed. O(changed fields).
    ///
    /// If slot is NOT alive (fresh insert): set all bitmaps directly. No diff needed.
    ///
    /// Always writes the new doc to docstore after bitmap updates.
    pub fn put(&mut self, id: u32, doc: &Document) -> Result<()> {
        let is_upsert = self.slots.is_alive(id);

        if is_upsert {
            // Upsert: read old doc from docstore and diff
            let old_doc = self.docstore.get(id)?;
            self.diff_and_update(id, old_doc.as_ref(), doc)?;
        } else {
            // Fresh insert (or re-insert of dead slot with stale bits):
            // If slot was ever allocated, it may have stale bits from before deletion.
            // The docstore tells us exactly what those old bits were.
            if self.slots.was_ever_allocated(id) {
                let old_doc = self.docstore.get(id)?;
                if let Some(old) = &old_doc {
                    // Clear stale bits using the old doc (targeted, not scan-all)
                    self.clear_old_bitmaps(id, old);
                }
            }

            // Set all bitmaps for the new document
            self.set_all_bitmaps(id, doc);
        }

        // Allocate slot (sets alive bit) -- idempotent for upserts
        self.slots.allocate(id)?;

        // Write new doc to docstore
        let stored = StoredDoc {
            fields: doc.fields.clone(),
            schema_version: 0,
        };
        self.docstore.put(id, &stored)?;

        // Eager merge: sort diffs and alive must be compacted before readers see them
        for (_name, field) in self.sorts.fields_mut() {
            field.merge_dirty();
        }
        self.slots.merge_alive();

        Ok(())
    }

    /// Diff old vs new doc and update only changed bitmaps. Used for upserts.
    fn diff_and_update(
        &mut self,
        id: u32,
        old_doc: Option<&StoredDoc>,
        new_doc: &Document,
    ) -> Result<()> {
        let empty_fields = HashMap::new();
        let old_fields = old_doc.map_or(&empty_fields, |d| &d.fields);

        // Process filter fields
        for filter_config in &self.config.filter_fields {
            let field_name = &filter_config.name;
            let old_val = old_fields.get(field_name);
            let new_val = new_doc.fields.get(field_name);

            // Skip if both are identical
            if Self::field_values_equal(old_val, new_val) {
                continue;
            }

            if let Some(filter_field) = self.filters.get_field_mut(field_name) {
                // Clear old bitmap bits
                if let Some(old) = old_val {
                    Self::clear_filter_bits(filter_field, id, old);
                }
                // Set new bitmap bits
                if let Some(new) = new_val {
                    Self::set_filter_bits(filter_field, id, new);
                }
            }
        }

        // Process sort fields
        for sort_config in &self.config.sort_fields {
            let field_name = &sort_config.name;
            let old_val = old_fields.get(field_name);
            let new_val = new_doc.fields.get(field_name);

            if Self::field_values_equal(old_val, new_val) {
                continue;
            }

            if let Some(sort_field) = self.sorts.get_field_mut(field_name) {
                let old_sort = old_val.and_then(|v| {
                    if let FieldValue::Single(val) = v {
                        value_to_sort_u32(val)
                    } else {
                        None
                    }
                });
                let new_sort = new_val.and_then(|v| {
                    if let FieldValue::Single(val) = v {
                        value_to_sort_u32(val)
                    } else {
                        None
                    }
                });

                match (old_sort, new_sort) {
                    (Some(old_s), Some(new_s)) => {
                        sort_field.update(id, old_s, new_s);
                    }
                    (Some(_), None) => {
                        sort_field.remove(id);
                    }
                    (None, Some(new_s)) => {
                        sort_field.insert(id, new_s);
                    }
                    (None, None) => {}
                }
            }
        }

        Ok(())
    }

    /// Clear stale bitmaps for a dead slot being re-inserted, using the old stored doc.
    fn clear_old_bitmaps(&mut self, id: u32, old_doc: &StoredDoc) {
        for filter_config in &self.config.filter_fields {
            if let Some(old_val) = old_doc.fields.get(&filter_config.name) {
                if let Some(filter_field) = self.filters.get_field_mut(&filter_config.name) {
                    Self::clear_filter_bits(filter_field, id, old_val);
                }
            }
        }
        for sort_config in &self.config.sort_fields {
            if let Some(old_val) = old_doc.fields.get(&sort_config.name) {
                if let Some(sort_field) = self.sorts.get_field_mut(&sort_config.name) {
                    if let FieldValue::Single(val) = old_val {
                        if value_to_sort_u32(val).is_some() {
                            sort_field.remove(id);
                        }
                    }
                }
            }
        }
    }

    /// Set all bitmaps for a fresh insert (no diffing).
    fn set_all_bitmaps(&mut self, id: u32, doc: &Document) {
        for filter_config in &self.config.filter_fields {
            if let Some(field_value) = doc.fields.get(&filter_config.name) {
                if let Some(filter_field) = self.filters.get_field_mut(&filter_config.name) {
                    Self::set_filter_bits(filter_field, id, field_value);
                }
            }
        }
        for sort_config in &self.config.sort_fields {
            if let Some(field_value) = doc.fields.get(&sort_config.name) {
                if let Some(sort_field) = self.sorts.get_field_mut(&sort_config.name) {
                    if let FieldValue::Single(val) = field_value {
                        if let Some(sort_val) = value_to_sort_u32(val) {
                            sort_field.insert(id, sort_val);
                        }
                    }
                }
            }
        }
    }

    /// Compare two optional FieldValues for equality.
    fn field_values_equal(a: Option<&FieldValue>, b: Option<&FieldValue>) -> bool {
        match (a, b) {
            (None, None) => true,
            (Some(FieldValue::Single(va)), Some(FieldValue::Single(vb))) => {
                Self::values_equal(va, vb)
            }
            (Some(FieldValue::Multi(va)), Some(FieldValue::Multi(vb))) => {
                va.len() == vb.len()
                    && va.iter().zip(vb.iter()).all(|(a, b)| Self::values_equal(a, b))
            }
            _ => false,
        }
    }

    /// Compare two Values for equality.
    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }

    /// Clear filter bitmap bits for a field value.
    fn clear_filter_bits(
        filter_field: &mut crate::filter::FilterField,
        id: u32,
        val: &FieldValue,
    ) {
        match val {
            FieldValue::Single(v) => {
                if let Some(key) = value_to_bitmap_key(v) {
                    filter_field.remove(key, id);
                }
            }
            FieldValue::Multi(vals) => {
                for v in vals {
                    if let Some(key) = value_to_bitmap_key(v) {
                        filter_field.remove(key, id);
                    }
                }
            }
        }
    }

    /// Set filter bitmap bits for a field value.
    fn set_filter_bits(
        filter_field: &mut crate::filter::FilterField,
        id: u32,
        val: &FieldValue,
    ) {
        match val {
            FieldValue::Single(v) => {
                if let Some(key) = value_to_bitmap_key(v) {
                    filter_field.insert(key, id);
                }
            }
            FieldValue::Multi(vals) => {
                for v in vals {
                    if let Some(key) = value_to_bitmap_key(v) {
                        filter_field.insert(key, id);
                    }
                }
            }
        }
    }

    /// PATCH(id, partial_fields) -- merge only provided fields.
    ///
    /// For each changed filter field: clear old bitmap bit, set new bitmap bit.
    /// For each changed sort field: XOR old and new values, flip only changed bit layers.
    /// The WAL event provides old and new values -- we do NOT look up old values.
    pub fn patch(&mut self, id: u32, patch: &PatchPayload) -> Result<()> {
        if !self.slots.is_alive(id) {
            return Err(BitdexError::SlotNotFound(id));
        }

        for (field_name, change) in &patch.fields {
            // Update filter bitmaps
            if let Some(filter_field) = self.filters.get_field_mut(field_name) {
                // Clear old values
                match &change.old {
                    FieldValue::Single(val) => {
                        if let Some(key) = value_to_bitmap_key(val) {
                            filter_field.remove(key, id);
                        }
                    }
                    FieldValue::Multi(vals) => {
                        for val in vals {
                            if let Some(key) = value_to_bitmap_key(val) {
                                filter_field.remove(key, id);
                            }
                        }
                    }
                }
                // Set new values
                match &change.new {
                    FieldValue::Single(val) => {
                        if let Some(key) = value_to_bitmap_key(val) {
                            filter_field.insert(key, id);
                        }
                    }
                    FieldValue::Multi(vals) => {
                        for val in vals {
                            if let Some(key) = value_to_bitmap_key(val) {
                                filter_field.insert(key, id);
                            }
                        }
                    }
                }
            }

            // Update sort layer bitmaps
            if let Some(sort_field) = self.sorts.get_field_mut(field_name) {
                if let (FieldValue::Single(old_val), FieldValue::Single(new_val)) =
                    (&change.old, &change.new)
                {
                    if let (Some(old_sort), Some(new_sort)) =
                        (value_to_sort_u32(old_val), value_to_sort_u32(new_val))
                    {
                        sort_field.update(id, old_sort, new_sort);
                    }
                }
            }
        }

        // Eager merge: sort diffs must be compacted before readers see them
        for (_name, field) in self.sorts.fields_mut() {
            field.merge_dirty();
        }

        Ok(())
    }

    /// DELETE(id) -- clean delete: clear filter/sort bitmaps then alive bit.
    ///
    /// Reads the doc from the docstore to determine which bitmaps need clearing.
    /// This keeps filter bitmaps clean, eliminating the alive AND from queries.
    pub fn delete(&mut self, id: u32) -> Result<()> {
        // Read old doc to know which bitmaps to clear
        let old_doc = self.docstore.get(id)?;
        if let Some(doc) = &old_doc {
            self.clear_old_bitmaps(id, doc);
            // Merge sort diffs from the clears
            for (_name, field) in self.sorts.fields_mut() {
                field.merge_dirty();
            }
        }
        self.slots.delete(id)?;
        self.slots.merge_alive();
        Ok(())
    }

    /// DELETE WHERE(predicate) -- resolve predicate, clean-delete all matches.
    ///
    /// Takes a pre-computed bitmap of matching slots (the caller resolves the predicate
    /// using the query engine). For each slot, reads the doc and clears filter/sort bitmaps.
    pub fn delete_where(&mut self, matching_slots: &RoaringBitmap) -> Result<u64> {
        let mut count = 0u64;
        for slot in matching_slots.iter() {
            if self.slots.is_alive(slot) {
                // Clean-delete: clear filter/sort bitmaps using stored doc
                if let Ok(Some(doc)) = self.docstore.get(slot) {
                    self.clear_old_bitmaps(slot, &doc);
                }
                self.slots.delete(slot)?;
                count += 1;
            }
        }
        // Merge sort diffs and alive
        for (_name, field) in self.sorts.fields_mut() {
            field.merge_dirty();
        }
        self.slots.merge_alive();
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;

    fn test_config() -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
                computed: None,
            }],
            ..Default::default()
        }
    }

    fn setup() -> (SlotAllocator, FilterIndex, SortIndex, Config, DocStore) {
        let config = test_config();
        let slots = SlotAllocator::new();
        let mut filters = FilterIndex::new();
        let mut sorts = SortIndex::new();
        let docstore = DocStore::open_temp().unwrap();

        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        (slots, filters, sorts, config, docstore)
    }

    fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
        Document {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    #[test]
    fn test_put_insert() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);

        let doc = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            (
                "tagIds",
                FieldValue::Multi(vec![Value::Integer(456), Value::Integer(789)]),
            ),
            ("onSite", FieldValue::Single(Value::Bool(true))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ]);

        engine.put(100, &doc).unwrap();

        assert!(slots.is_alive(100));
        assert_eq!(slots.alive_count(), 1);

        // Merge filter diffs before reading (Engine::put does this; MutationEngine does not)
        for (_name, field) in filters.fields_mut() {
            field.merge_dirty();
        }

        assert!(filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(1)
            .unwrap()
            .contains(100));
        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(456)
            .unwrap()
            .contains(100));
        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(789)
            .unwrap()
            .contains(100));
        assert!(filters
            .get_field("onSite")
            .unwrap()
            .get(1)
            .unwrap()
            .contains(100));

        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(100),
            42
        );
    }

    #[test]
    fn test_put_upsert_replaces_old_values() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);

        let doc1 = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(10))),
        ]);
        engine.put(100, &doc1).unwrap();

        let doc2 = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("reactionCount", FieldValue::Single(Value::Integer(99))),
        ]);
        engine.put(100, &doc2).unwrap();

        // Merge filter diffs before reading
        for (_name, field) in filters.fields_mut() {
            field.merge_dirty();
        }

        // Old filter value gone
        assert!(filters.get_field("nsfwLevel").unwrap().get(1).is_none()
            || !filters
                .get_field("nsfwLevel")
                .unwrap()
                .get(1)
                .unwrap()
                .contains(100));

        // New filter value set
        assert!(filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(2)
            .unwrap()
            .contains(100));

        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(100),
            99
        );
    }

    #[test]
    fn test_patch_filter_field() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);

        let doc = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(10))),
        ]);
        engine.put(100, &doc).unwrap();

        let patch = PatchPayload {
            fields: vec![(
                "nsfwLevel".to_string(),
                PatchField {
                    old: FieldValue::Single(Value::Integer(1)),
                    new: FieldValue::Single(Value::Integer(28)),
                },
            )]
            .into_iter()
            .collect(),
        };
        engine.patch(100, &patch).unwrap();

        // Merge filter diffs before reading
        for (_name, field) in filters.fields_mut() {
            field.merge_dirty();
        }

        assert!(filters.get_field("nsfwLevel").unwrap().get(1).is_none()
            || !filters
                .get_field("nsfwLevel")
                .unwrap()
                .get(1)
                .unwrap()
                .contains(100));

        assert!(filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(28)
            .unwrap()
            .contains(100));
    }

    #[test]
    fn test_patch_sort_field_uses_xor() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);

        let doc = make_doc(vec![
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]);
        engine.put(10, &doc).unwrap();

        let patch = PatchPayload {
            fields: vec![(
                "reactionCount".to_string(),
                PatchField {
                    old: FieldValue::Single(Value::Integer(100)),
                    new: FieldValue::Single(Value::Integer(200)),
                },
            )]
            .into_iter()
            .collect(),
        };
        engine.patch(10, &patch).unwrap();

        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(10),
            200
        );
    }

    #[test]
    fn test_patch_multi_value_field() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);

        let doc = make_doc(vec![(
            "tagIds",
            FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
        )]);
        engine.put(10, &doc).unwrap();

        let patch = PatchPayload {
            fields: vec![(
                "tagIds".to_string(),
                PatchField {
                    old: FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                    new: FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)]),
                },
            )]
            .into_iter()
            .collect(),
        };
        engine.patch(10, &patch).unwrap();

        // Merge filter diffs before reading
        for (_name, field) in filters.fields_mut() {
            field.merge_dirty();
        }

        assert!(filters.get_field("tagIds").unwrap().get(100).is_none()
            || !filters
                .get_field("tagIds")
                .unwrap()
                .get(100)
                .unwrap()
                .contains(10));

        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(200)
            .unwrap()
            .contains(10));

        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(300)
            .unwrap()
            .contains(10));
    }

    #[test]
    fn test_delete_cleans_all_bitmaps() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);

        let doc = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ]);
        engine.put(100, &doc).unwrap();
        engine.delete(100).unwrap();

        assert!(!slots.is_alive(100));

        // Merge filter diffs before reading
        for (_name, field) in filters.fields_mut() {
            field.merge_dirty();
        }

        // Filter bitmap is clean — stale bit removed on delete
        assert!(
            filters.get_field("nsfwLevel").unwrap().get(1).is_none()
                || !filters
                    .get_field("nsfwLevel")
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .contains(100)
        );

        // Sort bitmap is clean — stale bits removed on delete
        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(100),
            0
        );
    }

    #[test]
    fn test_delete_nonexistent() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);
        assert!(engine.delete(999).is_err());
    }

    #[test]
    fn test_patch_nonexistent() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);
        let patch = PatchPayload {
            fields: HashMap::new(),
        };
        assert!(engine.patch(999, &patch).is_err());
    }

    #[test]
    fn test_delete_where() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup();

        // Insert docs
        {
            let mut engine =
                MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);
            for i in 0..10u32 {
                let doc = make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(if i < 5 { 1 } else { 2 })),
                )]);
                engine.put(i, &doc).unwrap();
            }
        }

        // Merge filter diffs before reading
        for (_name, field) in filters.fields_mut() {
            field.merge_dirty();
        }

        // Get matching bitmap, then delete
        let matching = filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(1)
            .unwrap()
            .clone();
        let mut engine =
            MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config, &mut docstore);
        let deleted = engine.delete_where(&matching).unwrap();
        assert_eq!(deleted, 5);
        assert_eq!(slots.alive_count(), 5);

        for i in 0..5 {
            assert!(!slots.is_alive(i));
        }
        for i in 5..10 {
            assert!(slots.is_alive(i));
        }
    }

    // -----------------------------------------------------------------------
    // Computed sort field tests
    // -----------------------------------------------------------------------

    fn computed_config() -> Config {
        use crate::config::{ComputedField, ComputedOp};
        Config {
            filter_fields: vec![],
            sort_fields: vec![
                SortFieldConfig {
                    name: "existedAt".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: None,
                },
                SortFieldConfig {
                    name: "publishedAt".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: None,
                },
                SortFieldConfig {
                    name: "sortAt".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: false,
                    computed: Some(ComputedField {
                        op: ComputedOp::Greatest,
                        source_fields: vec!["existedAt".to_string(), "publishedAt".to_string()],
                    }),
                },
            ],
            ..Default::default()
        }
    }

    fn setup_computed() -> (SlotAllocator, FilterIndex, SortIndex, Config, DocStore) {
        let config = computed_config();
        let slots = SlotAllocator::new();
        let mut filters = FilterIndex::new();
        let mut sorts = SortIndex::new();
        let docstore = DocStore::open_temp().unwrap();

        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }
        (slots, filters, sorts, config, docstore)
    }

    #[test]
    fn test_computed_sort_fresh_insert() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup_computed();
        let registry = FieldRegistry::from_config(&config);
        let slot = 0u32;

        let mut fields = HashMap::new();
        fields.insert("existedAt".into(), FieldValue::Single(Value::Integer(100)));
        fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(200)));
        let doc = Document { fields };

        let ops = diff_document(slot, None, &doc, &config, false, &registry);

        // Should have sort ops for existedAt=100, publishedAt=200, and sortAt=200 (GREATEST)
        let sort_at_sets: Vec<_> = ops.iter().filter(|op| matches!(op,
            MutationOp::SortSet { field, .. } if field.as_ref() == "sortAt"
        )).collect();
        assert!(!sort_at_sets.is_empty(), "Should have sortAt set ops");

        // Verify the computed value is 200 (max of 100, 200) by checking bit pattern
        let mut reconstructed: u32 = 0;
        for op in &ops {
            if let MutationOp::SortSet { field, bit_layer, .. } = op {
                if field.as_ref() == "sortAt" {
                    reconstructed |= 1 << bit_layer;
                }
            }
        }
        assert_eq!(reconstructed, 200, "sortAt should be GREATEST(100, 200) = 200");
    }

    #[test]
    fn test_computed_sort_upsert_source_changes() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup_computed();
        let registry = FieldRegistry::from_config(&config);
        let slot = 0u32;

        // Old doc: existedAt=100, publishedAt=200 → sortAt=200
        let mut old_fields = HashMap::new();
        old_fields.insert("existedAt".into(), FieldValue::Single(Value::Integer(100)));
        old_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(200)));
        old_fields.insert("sortAt".into(), FieldValue::Single(Value::Integer(200)));
        let old_doc = StoredDoc { fields: old_fields, schema_version: 0 };

        // New doc: existedAt=300 (changed), publishedAt=200 → sortAt should become 300
        let mut new_fields = HashMap::new();
        new_fields.insert("existedAt".into(), FieldValue::Single(Value::Integer(300)));
        new_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(200)));
        let new_doc = Document { fields: new_fields };

        let ops = diff_document(slot, Some(&old_doc), &new_doc, &config, true, &registry);

        // Reconstruct sortAt from ops: should have clears for old value (200) and sets for new (300)
        let mut set_bits: u32 = 0;
        let mut clear_bits: u32 = 0;
        for op in &ops {
            match op {
                MutationOp::SortSet { field, bit_layer, .. } if field.as_ref() == "sortAt" => {
                    set_bits |= 1 << bit_layer;
                }
                MutationOp::SortClear { field, bit_layer, .. } if field.as_ref() == "sortAt" => {
                    clear_bits |= 1 << bit_layer;
                }
                _ => {}
            }
        }
        // The XOR diff between 200 and 300 should produce the right set/clear pattern
        let diff = 200u32 ^ 300u32;
        assert_ne!(diff, 0, "Values differ so diff should be nonzero");
        // set_bits | clear_bits should equal the diff (all changed bits accounted for)
        assert_eq!(set_bits | clear_bits, diff, "All changed bits should have ops");
    }

    #[test]
    fn test_computed_sort_no_change_when_sources_unchanged() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup_computed();
        let registry = FieldRegistry::from_config(&config);
        let slot = 0u32;

        let mut old_fields = HashMap::new();
        old_fields.insert("existedAt".into(), FieldValue::Single(Value::Integer(100)));
        old_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(200)));
        let old_doc = StoredDoc { fields: old_fields, schema_version: 0 };

        // Same values in new doc
        let mut new_fields = HashMap::new();
        new_fields.insert("existedAt".into(), FieldValue::Single(Value::Integer(100)));
        new_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(200)));
        let new_doc = Document { fields: new_fields };

        let ops = diff_document(slot, Some(&old_doc), &new_doc, &config, true, &registry);

        // Should have no sortAt ops since sources didn't change
        let sort_at_ops: Vec<_> = ops.iter().filter(|op| match op {
            MutationOp::SortSet { field, .. } | MutationOp::SortClear { field, .. } => field.as_ref() == "sortAt",
            _ => false,
        }).collect();
        assert!(sort_at_ops.is_empty(), "No sortAt ops when sources unchanged");
    }

    #[test]
    fn test_computed_sort_patch_updates_computed() {
        let (mut slots, mut filters, mut sorts, config, mut docstore) = setup_computed();
        let registry = FieldRegistry::from_config(&config);
        let slot = 0u32;

        // Old doc with both source fields
        let mut old_fields = HashMap::new();
        old_fields.insert("existedAt".into(), FieldValue::Single(Value::Integer(100)));
        old_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(200)));
        let old_doc = StoredDoc { fields: old_fields, schema_version: 0 };

        // PATCH only changes publishedAt to 50
        let mut new_fields = HashMap::new();
        new_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(50)));
        let new_doc = Document { fields: new_fields };

        let ops = diff_document_partial(slot, Some(&old_doc), &new_doc, &config, &registry);

        // sortAt should change from GREATEST(100,200)=200 to GREATEST(100,50)=100
        let has_sort_at_ops = ops.iter().any(|op| match op {
            MutationOp::SortSet { field, .. } | MutationOp::SortClear { field, .. } => field.as_ref() == "sortAt",
            _ => false,
        });
        assert!(has_sort_at_ops, "PATCH changing publishedAt should update sortAt");
    }

    #[test]
    fn test_apply_computed_op_greatest() {
        assert_eq!(apply_computed_op(&crate::config::ComputedOp::Greatest, &[10, 20, 5]), 20);
        assert_eq!(apply_computed_op(&crate::config::ComputedOp::Greatest, &[0]), 0);
        assert_eq!(apply_computed_op(&crate::config::ComputedOp::Greatest, &[]), 0);
    }

    #[test]
    fn test_apply_computed_op_least() {
        assert_eq!(apply_computed_op(&crate::config::ComputedOp::Least, &[10, 20, 5]), 5);
        assert_eq!(apply_computed_op(&crate::config::ComputedOp::Least, &[0]), 0);
        assert_eq!(apply_computed_op(&crate::config::ComputedOp::Least, &[]), 0);
    }

    #[test]
    fn test_diff_document_partial_deferred_alive() {
        use crate::config::{DeferredAliveConfig, FilterFieldConfig, SortFieldConfig};
        use crate::filter::FilterFieldType;
        use crate::write_coalescer::MutationOp;

        let mut config = Config::default();
        config.filter_fields = vec![FilterFieldConfig {
            name: "nsfwLevel".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
        }];
        config.sort_fields = vec![SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        }];
        config.deferred_alive = Some(DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
        });

        let registry = FieldRegistry::from_config(&config);

        // Old doc has nsfwLevel=16 and publishedAt=1000 (alive)
        let mut old_fields = std::collections::HashMap::new();
        old_fields.insert("nsfwLevel".into(), FieldValue::Single(Value::Integer(16)));
        old_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(1000)));
        let old_doc = crate::docstore::StoredDoc { fields: old_fields, schema_version: 0 };

        // PATCH changes publishedAt to far future (year 2050)
        let future_ts = 2524608000i64;
        let mut new_fields = std::collections::HashMap::new();
        new_fields.insert("publishedAt".into(), FieldValue::Single(Value::Integer(future_ts)));
        let new_doc = Document { fields: new_fields };

        let ops = diff_document_partial(42, Some(&old_doc), &new_doc, &config, &registry);

        // Should contain: filter removes (clear old nsfwLevel), sort clears,
        // alive remove, and deferred alive
        let has_deferred = ops.iter().any(|op| matches!(op, MutationOp::DeferredAlive { .. }));
        let has_alive_remove = ops.iter().any(|op| matches!(op, MutationOp::AliveRemove { .. }));

        assert!(has_deferred, "PATCH to future publishedAt should defer alive");
        assert!(has_alive_remove, "PATCH to future should remove alive bit");

        // Should NOT have any filter inserts or sort sets (all bitmaps cleared)
        let has_filter_insert = ops.iter().any(|op| matches!(op, MutationOp::FilterInsert { .. }));
        assert!(!has_filter_insert, "deferred should not insert any filter bitmaps");
    }
}
