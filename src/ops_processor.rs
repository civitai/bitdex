//! WAL ops processor — reads ops from WAL files and applies them as engine mutations.
//!
//! The processor runs as a dedicated thread, tailing WAL files and converting ops
//! into engine mutations (put/patch/delete). It handles:
//! - Regular ops (set/remove/add) via PatchPayload
//! - queryOpSet via query resolution + bulk bitmap ops
//! - Delete via engine.delete()
//! - Deduplication via shared dedup helper

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::concurrent_engine::ConcurrentEngine;
use crate::mutation::{FieldValue, PatchField, PatchPayload};
use crate::pg_sync::op_dedup::dedup_ops;
use crate::pg_sync::ops::{EntityOps, Op};
use crate::query::{BitdexQuery, FilterClause, Value as QValue};

/// Convert a serde_json::Value to a query::Value.
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
        JsonValue::Null => QValue::Integer(0), // Null → zero for bitmap purposes
        _ => QValue::String(v.to_string()), // Arrays/objects → string representation
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

/// Process a single batch of entity ops against the engine.
/// Returns (applied, skipped, errors).
pub fn apply_ops_batch(
    engine: &ConcurrentEngine,
    batch: &mut Vec<EntityOps>,
) -> (usize, usize, usize) {
    // Dedup first
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

        for op in &entry.ops {
            match op {
                Op::Delete => {
                    match engine.delete(slot) {
                        Ok(()) => applied += 1,
                        Err(e) => {
                            tracing::warn!("ops processor: delete slot {slot} failed: {e}");
                            errors += 1;
                        }
                    }
                }

                Op::QueryOpSet { query, ops } => {
                    match apply_query_op_set(engine, query, ops) {
                        Ok(count) => applied += count,
                        Err(e) => {
                            tracing::warn!("ops processor: queryOpSet '{query}' failed: {e}");
                            errors += 1;
                        }
                    }
                }

                // Accumulate set/remove/add ops per entity, then apply as a patch
                _ => {
                    // Collect all non-delete, non-queryOpSet ops for this entity
                    // and apply as a single patch
                }
            }
        }

        // Build a PatchPayload from the set/remove/add ops for this entity
        let patch = build_patch_from_ops(&entry.ops);
        if !patch.fields.is_empty() {
            match engine.patch(slot, &patch) {
                Ok(()) => applied += 1,
                Err(e) => {
                    tracing::warn!("ops processor: patch slot {slot} failed: {e}");
                    errors += 1;
                }
            }
        }
    }

    (applied, skipped, errors)
}

/// Build a PatchPayload from a list of ops for a single entity.
/// Pairs remove/set ops on the same field into PatchField { old, new }.
/// Add ops become multi-value inserts.
fn build_patch_from_ops(ops: &[Op]) -> PatchPayload {
    let mut fields: HashMap<String, PatchField> = HashMap::new();

    // First pass: collect removes (old values) and sets (new values) per field
    let mut old_values: HashMap<&str, &JsonValue> = HashMap::new();
    let mut new_values: HashMap<&str, &JsonValue> = HashMap::new();
    let mut add_values: HashMap<&str, Vec<&JsonValue>> = HashMap::new();
    let mut remove_values: HashMap<&str, Vec<&JsonValue>> = HashMap::new();

    for op in ops {
        match op {
            Op::Remove { field, value } => {
                // Check if there's a corresponding Set for this field (scalar update)
                let has_set = ops.iter().any(|o| matches!(o, Op::Set { field: f, .. } if f == field));
                if has_set {
                    old_values.insert(field, value);
                } else {
                    // Multi-value remove
                    remove_values.entry(field).or_default().push(value);
                }
            }
            Op::Set { field, value } => {
                new_values.insert(field, value);
            }
            Op::Add { field, value } => {
                add_values.entry(field).or_default().push(value);
            }
            Op::Delete | Op::QueryOpSet { .. } => {
                // Handled separately
            }
        }
    }

    // Build PatchFields for scalar set/remove pairs
    for (field, new_val) in &new_values {
        let old = old_values
            .get(*field)
            .map(|v| FieldValue::Single(json_to_qvalue(v)))
            .unwrap_or(FieldValue::Single(QValue::Integer(0)));
        let new = FieldValue::Single(json_to_qvalue(new_val));
        fields.insert(field.to_string(), PatchField { old, new });
    }

    // Build PatchFields for multi-value adds
    for (field, vals) in &add_values {
        let new_multi: Vec<QValue> = vals.iter().map(|v| json_to_qvalue(v)).collect();
        let existing = fields.entry(field.to_string()).or_insert_with(|| PatchField {
            old: FieldValue::Multi(vec![]),
            new: FieldValue::Multi(vec![]),
        });
        if let FieldValue::Multi(ref mut m) = existing.new {
            m.extend(new_multi);
        } else {
            *existing = PatchField {
                old: FieldValue::Multi(vec![]),
                new: FieldValue::Multi(vals.iter().map(|v| json_to_qvalue(v)).collect()),
            };
        }
    }

    // Build PatchFields for multi-value removes
    for (field, vals) in &remove_values {
        let removed: Vec<QValue> = vals.iter().map(|v| json_to_qvalue(v)).collect();
        let existing = fields.entry(field.to_string()).or_insert_with(|| PatchField {
            old: FieldValue::Multi(vec![]),
            new: FieldValue::Multi(vec![]),
        });
        if let FieldValue::Multi(ref mut m) = existing.old {
            m.extend(removed);
        } else {
            *existing = PatchField {
                old: FieldValue::Multi(vals.iter().map(|v| json_to_qvalue(v)).collect()),
                new: FieldValue::Multi(vec![]),
            };
        }
    }

    PatchPayload { fields }
}

/// Resolve a queryOpSet: execute the query to get matching slots, then apply
/// the nested ops to each slot.
fn apply_query_op_set(
    engine: &ConcurrentEngine,
    query_str: &str,
    ops: &[Op],
) -> Result<usize, String> {
    // Parse the query string into filter clauses
    let filters = parse_filter_from_query_str(query_str)?;

    let query = BitdexQuery {
        filters,
        sort: None,
        limit: usize::MAX, // Get all matching slots
        offset: None,
        cursor: None,
        skip_cache: true, // Don't pollute cache with internal queries
    };

    // Execute query to get matching slot IDs
    let result = engine
        .execute_query(&query)
        .map_err(|e| format!("queryOpSet query failed: {e}"))?;

    let slot_ids = &result.ids;
    if slot_ids.is_empty() {
        return Ok(0);
    }

    // Build the patch from nested ops
    let patch = build_patch_from_ops(ops);
    if patch.fields.is_empty() {
        return Ok(0);
    }

    // Apply patch to each matching slot
    let mut applied = 0;
    for &slot_id in slot_ids {
        if slot_id < 0 {
            continue;
        }
        let slot = slot_id as u32;
        match engine.patch(slot, &patch) {
            Ok(()) => applied += 1,
            Err(e) => {
                tracing::warn!("queryOpSet: patch slot {slot} failed: {e}");
            }
        }
    }

    Ok(applied)
}

/// Parse a simple filter string like "modelVersionIds eq 456" or "postId eq 789"
/// into filter clauses.
fn parse_filter_from_query_str(query_str: &str) -> Result<Vec<FilterClause>, String> {
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
fn parse_query_value(s: &str) -> Result<QValue, String> {
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
fn parse_query_values_array(s: &str) -> Result<Vec<QValue>, String> {
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

    #[test]
    fn test_build_patch_from_scalar_update() {
        let ops = vec![
            Op::Remove { field: "nsfwLevel".into(), value: json!(8) },
            Op::Set { field: "nsfwLevel".into(), value: json!(16) },
        ];
        let patch = build_patch_from_ops(&ops);
        assert_eq!(patch.fields.len(), 1);
        let field = &patch.fields["nsfwLevel"];
        assert_eq!(field.old, FieldValue::Single(QValue::Integer(8)));
        assert_eq!(field.new, FieldValue::Single(QValue::Integer(16)));
    }

    #[test]
    fn test_build_patch_from_insert_no_old() {
        let ops = vec![
            Op::Set { field: "nsfwLevel".into(), value: json!(16) },
            Op::Set { field: "type".into(), value: json!("image") },
        ];
        let patch = build_patch_from_ops(&ops);
        assert_eq!(patch.fields.len(), 2);
        assert_eq!(patch.fields["nsfwLevel"].old, FieldValue::Single(QValue::Integer(0)));
        assert_eq!(patch.fields["nsfwLevel"].new, FieldValue::Single(QValue::Integer(16)));
    }

    #[test]
    fn test_build_patch_from_add() {
        let ops = vec![
            Op::Add { field: "tagIds".into(), value: json!(42) },
            Op::Add { field: "tagIds".into(), value: json!(99) },
        ];
        let patch = build_patch_from_ops(&ops);
        assert_eq!(patch.fields.len(), 1);
        if let FieldValue::Multi(ref vals) = patch.fields["tagIds"].new {
            assert_eq!(vals.len(), 2);
        } else {
            panic!("Expected Multi");
        }
    }

    #[test]
    fn test_build_patch_from_multi_remove() {
        let ops = vec![
            Op::Remove { field: "tagIds".into(), value: json!(42) },
        ];
        let patch = build_patch_from_ops(&ops);
        assert_eq!(patch.fields.len(), 1);
        if let FieldValue::Multi(ref vals) = patch.fields["tagIds"].old {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0], QValue::Integer(42));
        } else {
            panic!("Expected Multi for old");
        }
    }

    #[test]
    fn test_build_patch_skips_delete_and_query() {
        let ops = vec![
            Op::Delete,
            Op::QueryOpSet { query: "x eq 1".into(), ops: vec![] },
            Op::Set { field: "a".into(), value: json!(1) },
        ];
        let patch = build_patch_from_ops(&ops);
        assert_eq!(patch.fields.len(), 1);
        assert!(patch.fields.contains_key("a"));
    }

    #[test]
    fn test_parse_filter_eq() {
        let filters = parse_filter_from_query_str("modelVersionIds eq 456").unwrap();
        assert_eq!(filters.len(), 1);
        assert!(matches!(&filters[0], FilterClause::Eq(f, QValue::Integer(456)) if f == "modelVersionIds"));
    }

    #[test]
    fn test_parse_filter_in() {
        let filters = parse_filter_from_query_str("modelVersionIds in [101, 102, 103]").unwrap();
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
        assert!(matches!(parse_query_value("42").unwrap(), QValue::Integer(42)));
        assert!(matches!(parse_query_value("true").unwrap(), QValue::Bool(true)));
        assert!(matches!(parse_query_value("\"hello\"").unwrap(), QValue::String(s) if s == "hello"));
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
