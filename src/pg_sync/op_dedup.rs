//! Op deduplication and compression.
//!
//! Shared helper used by both pg-sync (before sending) and the WAL reader
//! (before applying). Two layers of dedup catch duplicates at both stages.
//!
//! Rules:
//! - LIFO per (entity_id, field): last op wins for set/remove pairs
//! - Add/remove cancellation: add X then remove X = net zero, dropped
//! - QueryOpSet dedup: by (entity_id, query string), last wins
//! - Delete absorbs all prior ops for the same entity_id

use ahash::{AHashMap as HashMap, AHashSet as HashSet};

use super::ops::{EntityOps, Op};

/// Deduplicate a batch of entity ops in-place.
///
/// Processes ops in order (oldest first), applying LIFO semantics:
/// for each (entity_id, field), only the last op survives.
/// Add/remove cancellation eliminates net-zero multi-value ops.
/// A delete op absorbs all prior ops for that entity.
pub fn dedup_ops(batch: &mut Vec<EntityOps>) {
    // Phase 1: Merge all ops per entity_id, preserving creates_slot (OR across sources)
    let mut entity_map: HashMap<i64, Vec<Op>> = HashMap::new();
    let mut creates_slot_map: HashMap<i64, bool> = HashMap::new();
    for entry in batch.drain(..) {
        entity_map
            .entry(entry.entity_id)
            .or_default()
            .extend(entry.ops);
        // If ANY source for this entity sets creates_slot, preserve it
        if entry.creates_slot {
            creates_slot_map.insert(entry.entity_id, true);
        }
    }

    // Phase 2: Dedup ops within each entity
    for (_entity_id, ops) in &mut entity_map {
        dedup_entity_ops(ops);
    }

    // Phase 3: Rebuild batch, dropping empty entries
    *batch = entity_map
        .into_iter()
        .filter(|(_, ops)| !ops.is_empty())
        .map(|(entity_id, ops)| EntityOps {
            entity_id,
            ops,
            creates_slot: creates_slot_map.get(&entity_id).copied().unwrap_or(false),
        })
        .collect();
}

/// Dedup ops for a single entity. Mutates the vec in place.
fn dedup_entity_ops(ops: &mut Vec<Op>) {
    if ops.is_empty() {
        return;
    }

    // If there's a Delete, it absorbs everything — only keep the delete
    if ops.iter().any(|op| matches!(op, Op::Delete)) {
        ops.clear();
        ops.push(Op::Delete);
        return;
    }

    // First pass: collect all ops, tracking which fields have Set ops
    let all_ops: Vec<Op> = ops.drain(..).collect();
    let mut set_fields: HashSet<String> = HashSet::new();
    for op in &all_ops {
        if let Op::Set { field, .. } = op {
            set_fields.insert(field.clone());
        }
    }

    // LIFO for set/remove on scalar fields (paired with Set = old value cleanup)
    let mut last_set: HashMap<String, serde_json::Value> = HashMap::new();
    let mut last_remove: HashMap<String, serde_json::Value> = HashMap::new();

    // Track add/remove for multi-value fields (net operations)
    // Key: (field, value_as_string), Value: net count (+1 for add, -1 for remove)
    let mut multi_value_net: HashMap<(String, String), i64> = HashMap::new();

    // Track queryOpSet by query string (last wins)
    let mut query_ops: HashMap<Option<String>, Vec<Op>> = HashMap::new();

    for op in all_ops {
        match op {
            Op::Set { ref field, ref value } => {
                last_set.insert(field.clone(), value.clone());
            }
            Op::Remove { ref field, ref value } => {
                if set_fields.contains(field) {
                    // Scalar field: this remove is paired with a set (old value cleanup)
                    last_remove.insert(field.clone(), value.clone());
                } else {
                    // Multi-value field: track net operations
                    let key = (field.clone(), value.to_string());
                    *multi_value_net.entry(key).or_insert(0) -= 1;
                }
            }
            Op::Add { ref field, ref value } => {
                let key = (field.clone(), value.to_string());
                *multi_value_net.entry(key).or_insert(0) += 1;
            }
            Op::QueryOpSet { ref query, ops: ref nested_ops } => {
                query_ops.insert(query.clone(), nested_ops.clone());
            }
            Op::Delete => unreachable!("handled above"),
            Op::Alive => {} // Signal-only, no dedup needed
        }
    }

    // Rebuild: remove ops first, then set ops (order matters for bitmap updates)
    for (field, value) in &last_remove {
        ops.push(Op::Remove {
            field: field.clone(),
            value: value.clone(),
        });
    }

    for (field, value) in last_set {
        ops.push(Op::Set { field, value });
    }

    // Multi-value: emit net operations
    for ((field, value_str), net) in multi_value_net {
        if net == 0 {
            continue; // Cancelled out
        }
        let value: serde_json::Value = serde_json::from_str(&value_str)
            .unwrap_or(serde_json::Value::String(value_str));
        if net > 0 {
            ops.push(Op::Add { field, value });
        } else {
            ops.push(Op::Remove { field, value });
        }
    }

    // QueryOpSets: last query string wins
    for (query, nested) in query_ops {
        ops.push(Op::QueryOpSet { query, ops: nested });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entity(id: i64, ops: Vec<Op>) -> EntityOps {
        EntityOps { entity_id: id, ops, creates_slot: false }
    }

    #[test]
    fn test_lifo_set_same_field() {
        let mut batch = vec![
            entity(1, vec![
                Op::Set { field: "nsfwLevel".into(), value: json!(8) },
            ]),
            entity(1, vec![
                Op::Set { field: "nsfwLevel".into(), value: json!(16) },
            ]),
        ];
        dedup_ops(&mut batch);
        assert_eq!(batch.len(), 1);
        let ops = &batch[0].ops;
        // Last set wins
        let set_op = ops.iter().find(|op| matches!(op, Op::Set { field, .. } if field == "nsfwLevel")).unwrap();
        if let Op::Set { value, .. } = set_op {
            assert_eq!(*value, json!(16));
        }
    }

    #[test]
    fn test_different_fields_preserved() {
        let mut batch = vec![entity(1, vec![
            Op::Set { field: "nsfwLevel".into(), value: json!(16) },
            Op::Set { field: "type".into(), value: json!("video") },
        ])];
        dedup_ops(&mut batch);
        assert_eq!(batch[0].ops.len(), 2);
    }

    #[test]
    fn test_add_remove_cancellation() {
        let mut batch = vec![entity(1, vec![
            Op::Add { field: "tagIds".into(), value: json!(42) },
            Op::Remove { field: "tagIds".into(), value: json!(42) },
        ])];
        dedup_ops(&mut batch);
        // Net zero — entity should be dropped entirely
        assert!(batch.is_empty() || batch[0].ops.is_empty());
    }

    #[test]
    fn test_add_survives_when_no_cancel() {
        let mut batch = vec![entity(1, vec![
            Op::Add { field: "tagIds".into(), value: json!(42) },
            Op::Add { field: "tagIds".into(), value: json!(99) },
        ])];
        dedup_ops(&mut batch);
        assert_eq!(batch.len(), 1);
        let adds: Vec<_> = batch[0].ops.iter()
            .filter(|op| matches!(op, Op::Add { .. }))
            .collect();
        assert_eq!(adds.len(), 2);
    }

    #[test]
    fn test_delete_absorbs_all() {
        let mut batch = vec![
            entity(1, vec![
                Op::Set { field: "nsfwLevel".into(), value: json!(16) },
                Op::Add { field: "tagIds".into(), value: json!(42) },
            ]),
            entity(1, vec![Op::Delete]),
        ];
        dedup_ops(&mut batch);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].ops.len(), 1);
        assert!(matches!(&batch[0].ops[0], Op::Delete));
    }

    #[test]
    fn test_different_entities_independent() {
        let mut batch = vec![
            entity(1, vec![Op::Set { field: "nsfwLevel".into(), value: json!(16) }]),
            entity(2, vec![Op::Set { field: "nsfwLevel".into(), value: json!(32) }]),
        ];
        dedup_ops(&mut batch);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn test_query_op_set_last_wins() {
        let mut batch = vec![entity(456, vec![
            Op::QueryOpSet {
                query: Some("modelVersionIds eq 456".into()),
                ops: vec![Op::Set { field: "baseModel".into(), value: json!("SD 1.5") }],
            },
            Op::QueryOpSet {
                query: Some("modelVersionIds eq 456".into()),
                ops: vec![Op::Set { field: "baseModel".into(), value: json!("SDXL") }],
            },
        ])];
        dedup_ops(&mut batch);
        let qops: Vec<_> = batch[0].ops.iter()
            .filter(|op| matches!(op, Op::QueryOpSet { .. }))
            .collect();
        assert_eq!(qops.len(), 1);
        if let Op::QueryOpSet { ops, .. } = &qops[0] {
            if let Op::Set { value, .. } = &ops[0] {
                assert_eq!(*value, json!("SDXL"));
            }
        }
    }

    #[test]
    fn test_remove_set_pair_preserved() {
        // An update: remove old value, set new value — both should survive
        let mut batch = vec![entity(1, vec![
            Op::Remove { field: "nsfwLevel".into(), value: json!(8) },
            Op::Set { field: "nsfwLevel".into(), value: json!(16) },
        ])];
        dedup_ops(&mut batch);
        assert_eq!(batch.len(), 1);
        let has_remove = batch[0].ops.iter().any(|op| matches!(op, Op::Remove { field, .. } if field == "nsfwLevel"));
        let has_set = batch[0].ops.iter().any(|op| matches!(op, Op::Set { field, .. } if field == "nsfwLevel"));
        assert!(has_remove, "remove should survive");
        assert!(has_set, "set should survive");
    }

    #[test]
    fn test_empty_batch() {
        let mut batch: Vec<EntityOps> = vec![];
        dedup_ops(&mut batch);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_multiple_adds_same_value_collapse() {
        // Adding tag 42 three times should still produce one add
        let mut batch = vec![entity(1, vec![
            Op::Add { field: "tagIds".into(), value: json!(42) },
            Op::Add { field: "tagIds".into(), value: json!(42) },
            Op::Add { field: "tagIds".into(), value: json!(42) },
        ])];
        dedup_ops(&mut batch);
        let adds: Vec<_> = batch[0].ops.iter()
            .filter(|op| matches!(op, Op::Add { field, .. } if field == "tagIds"))
            .collect();
        assert_eq!(adds.len(), 1);
    }
}
