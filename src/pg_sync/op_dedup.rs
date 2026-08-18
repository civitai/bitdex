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
    // First-arrival order of entity ids. Phase 3 used to rebuild the batch by
    // consuming the map, so the ORDER ENTITIES ARE APPLIED IN was per-process
    // hash order — and two entities can carry ops for the same slot in one
    // batch (a Post fan-out's queryOpSet and a direct Image op both write
    // publishedAt for the same image). Whichever landed last won, by hash.
    // Same failure shape as the one inside `dedup_entity_ops`, one level up.
    let mut entity_order: Vec<i64> = Vec::new();
    for entry in batch.drain(..) {
        if !entity_map.contains_key(&entry.entity_id) {
            entity_order.push(entry.entity_id);
        }
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

    // Phase 3: Rebuild batch in first-arrival entity order, dropping empties
    *batch = entity_order
        .into_iter()
        .filter_map(|entity_id| {
            let ops = entity_map.remove(&entity_id)?;
            if ops.is_empty() {
                return None;
            }
            Some(EntityOps {
                entity_id,
                ops,
                creates_slot: creates_slot_map.get(&entity_id).copied().unwrap_or(false),
            })
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

    // Which op ARRIVED last for each scalar field, and the order fields were
    // first seen. Both exist so the rebuilt vec is a function of arrival order
    // only — never of hash iteration order.
    //
    // The kind matters on its own: a field can end on a Remove even though the
    // batch also contains a Set for it (publish at T, then reschedule to T+n
    // inside one batch). Emitting the Set regardless would apply the superseded
    // op — deterministically wrong rather than randomly wrong. The last op is
    // the state PG ended in, so it is the one that survives.
    let mut last_scalar_is_set: HashMap<String, bool> = HashMap::new();
    let mut scalar_field_order: Vec<String> = Vec::new();

    // Track add/remove for multi-value fields (net operations)
    // Key: (field, value_as_string), Value: net count (+1 for add, -1 for remove)
    // `multi_value_order` keeps first-arrival order for the rebuild: the net
    // counts are order-independent, but the ops emitted from them are not.
    // A schedule field carrying two value-bearing removes lands here (no Set on
    // the field ⇒ classified multi-value), and the engine reads the LAST one —
    // so hash order used to decide which schedule won.
    let mut multi_value_net: HashMap<(String, String), i64> = HashMap::new();
    let mut multi_value_order: Vec<(String, String)> = Vec::new();

    // Track queryOpSet by query string — MERGE nested ops, never last-wins.
    //
    // Every Post fan-out carries the identical query string ("postId eq X"),
    // and one user action (e.g. scheduling a post) can update the Post row
    // more than once inside a single poller/WAL batch. The old wholesale
    // last-wins here silently DISCARDED the earlier fan-out's nested ops —
    // if the first update carried `Set publishedAt=Tf` and the second only
    // `Set availability` (publishedAt unchanged emits nothing under
    // IS DISTINCT FROM), the publish op evaporated before the engine ever
    // saw it: total per-post publish no-op, ~7% of scheduled posts, the
    // last surviving member of the 2026-07 fan-out loss family. Nested ops
    // are concatenated in arrival order and deduped with the same per-field
    // LIFO rules as entity ops, so same-field conflicts still resolve
    // last-wins per FIELD — never per fan-out.
    let mut query_ops: HashMap<Option<String>, Vec<Op>> = HashMap::new();
    let mut query_order: Vec<Option<String>> = Vec::new();

    for op in all_ops {
        match op {
            Op::Set { ref field, ref value } => {
                if last_set.insert(field.clone(), value.clone()).is_none()
                    && !last_remove.contains_key(field)
                {
                    scalar_field_order.push(field.clone());
                }
                last_scalar_is_set.insert(field.clone(), true);
            }
            Op::Remove { ref field, ref value } => {
                if set_fields.contains(field) {
                    // Scalar field: this remove is paired with a set (old value cleanup)
                    if last_remove.insert(field.clone(), value.clone()).is_none()
                        && !last_set.contains_key(field)
                    {
                        scalar_field_order.push(field.clone());
                    }
                    last_scalar_is_set.insert(field.clone(), false);
                } else {
                    // Multi-value field: track net operations
                    let key = (field.clone(), value.to_string());
                    if !multi_value_net.contains_key(&key) {
                        multi_value_order.push(key.clone());
                    }
                    *multi_value_net.entry(key).or_insert(0) -= 1;
                }
            }
            Op::Add { ref field, ref value } => {
                let key = (field.clone(), value.to_string());
                if !multi_value_net.contains_key(&key) {
                    multi_value_order.push(key.clone());
                }
                *multi_value_net.entry(key).or_insert(0) += 1;
            }
            Op::QueryOpSet { ref query, ops: ref nested_ops } => {
                if !query_ops.contains_key(query) {
                    query_order.push(query.clone());
                }
                query_ops
                    .entry(query.clone())
                    .or_default()
                    .extend(nested_ops.iter().cloned());
            }
            Op::Delete => unreachable!("handled above"),
            Op::Alive => {} // Signal-only, no dedup needed
        }
    }

    // Rebuild: remove ops first, then set ops (order matters for bitmap
    // updates), each in first-arrival field order.
    for field in &scalar_field_order {
        if let Some(value) = last_remove.get(field) {
            ops.push(Op::Remove {
                field: field.clone(),
                value: value.clone(),
            });
        }
    }

    for field in &scalar_field_order {
        // A field whose LAST arriving op was a Remove keeps only that remove:
        // its Set was superseded within this batch.
        if !last_scalar_is_set.get(field).copied().unwrap_or(false) {
            continue;
        }
        if let Some(value) = last_set.get(field) {
            ops.push(Op::Set {
                field: field.clone(),
                value: value.clone(),
            });
        }
    }

    // Multi-value: emit net operations, in first-arrival order
    for key in multi_value_order {
        let net = multi_value_net.get(&key).copied().unwrap_or(0);
        if net == 0 {
            continue; // Cancelled out
        }
        let (field, value_str) = key;
        let value: serde_json::Value = serde_json::from_str(&value_str)
            .unwrap_or(serde_json::Value::String(value_str));
        if net > 0 {
            ops.push(Op::Add { field, value });
        } else {
            ops.push(Op::Remove { field, value });
        }
    }

    // QueryOpSets: one per query string, nested ops merged + field-deduped,
    // emitted in first-arrival order.
    for query in query_order {
        let Some(mut nested) = query_ops.remove(&query) else {
            continue;
        };
        dedup_entity_ops(&mut nested);
        if !nested.is_empty() {
            ops.push(Op::QueryOpSet { query, ops: nested });
        }
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

    /// Regression (2026-07-09, the last fan-out loss survivor): two fan-outs
    /// for the SAME entity + query in one batch must MERGE their nested ops —
    /// the old wholesale last-wins discarded the earlier fan-out's ops. Kill
    /// shape: schedule action = Post update A (Set publishedAt) then update B
    /// (Set availability, publishedAt unchanged so not re-emitted) in one
    /// poller batch → publishedAt evaporated → total per-post publish no-op
    /// (~7% of scheduled posts; specimens 29651562/29651617/29651221/
    /// 29666515/29666636/29666669).
    #[test]
    fn test_query_op_set_same_query_merges_disjoint_fields() {
        let mut batch = vec![entity(100, vec![
            Op::QueryOpSet {
                query: Some("postId eq 100".into()),
                ops: vec![Op::Set { field: "publishedAt".into(), value: json!(1783571340) }],
            },
            Op::QueryOpSet {
                query: Some("postId eq 100".into()),
                ops: vec![Op::Set { field: "availability".into(), value: json!("Public") }],
            },
        ])];
        dedup_ops(&mut batch);
        let qops: Vec<_> = batch[0].ops.iter()
            .filter(|op| matches!(op, Op::QueryOpSet { .. }))
            .collect();
        assert_eq!(qops.len(), 1, "same query string must collapse to ONE fan-out");
        if let Op::QueryOpSet { ops, .. } = &qops[0] {
            let has_pub = ops.iter().any(|o| matches!(o, Op::Set { field, .. } if field == "publishedAt"));
            let has_avail = ops.iter().any(|o| matches!(o, Op::Set { field, .. } if field == "availability"));
            assert!(has_pub, "publishedAt Set must SURVIVE the merge (was discarded pre-fix)");
            assert!(has_avail, "availability Set must survive too");
        }
    }

    /// Same-field conflicts across merged fan-outs still resolve last-wins —
    /// per FIELD, not per fan-out (preserves the old test's intent).
    #[test]
    fn test_query_op_set_same_query_same_field_last_wins() {
        let mut batch = vec![entity(100, vec![
            Op::QueryOpSet {
                query: Some("postId eq 100".into()),
                ops: vec![Op::Set { field: "publishedAt".into(), value: json!(111) }],
            },
            Op::QueryOpSet {
                query: Some("postId eq 100".into()),
                ops: vec![Op::Set { field: "publishedAt".into(), value: json!(222) }],
            },
        ])];
        dedup_ops(&mut batch);
        if let Some(Op::QueryOpSet { ops, .. }) = batch[0].ops.iter().find(|op| matches!(op, Op::QueryOpSet { .. })) {
            let sets: Vec<_> = ops.iter().filter(|o| matches!(o, Op::Set { field, .. } if field == "publishedAt")).collect();
            assert_eq!(sets.len(), 1);
            if let Op::Set { value, .. } = sets[0] {
                assert_eq!(*value, json!(222), "later fan-out's value wins per-field");
            }
        } else { panic!("queryOpSet missing"); }
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

    /// Value-bearing removes on a field with no Set are classified as
    /// multi-value ops and ALL survive — so the order they are re-emitted in is
    /// the order a last-op-wins consumer reads. It must be arrival order, never
    /// hash order.
    ///
    /// This is the second half of the 2026-08-18 scheduled-publish defect: the
    /// engine reads the last remove on the schedule field to decide when to
    /// activate a slot. With hash ordering it took an arbitrary one of them.
    /// The emission fix stops a single ROW carrying two; a batch that merges two
    /// rows (schedule, then reschedule) still delivers both, and the later one
    /// has to win.
    #[test]
    fn test_multi_value_removes_keep_arrival_order() {
        // Enough distinct values that landing in arrival order by luck is not a
        // plausible explanation for a pass.
        let values: Vec<i64> = (1..=20).map(|i| 1_800_000_000 + i).collect();
        let mut batch = vec![entity(
            1,
            values
                .iter()
                .map(|v| Op::Remove { field: "publishedAt".into(), value: json!(v) })
                .collect(),
        )];
        dedup_ops(&mut batch);

        let seen: Vec<i64> = batch[0]
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Remove { field, value } if field == "publishedAt" => value.as_i64(),
                _ => None,
            })
            .collect();
        assert_eq!(
            seen, values,
            "value-bearing removes must be re-emitted in arrival order — a \
             last-op-wins consumer reads the last one as the current value"
        );
    }

    /// A Set superseded by a later Remove on the same field must NOT be
    /// re-emitted: within one batch the last arriving op is the state PG ended
    /// in. Publish-then-reschedule produces exactly this shape (set publishedAt
    /// = now, then remove publishedAt = future schedule), and re-emitting the
    /// Set publishes content PG considers scheduled.
    #[test]
    fn test_later_remove_supersedes_earlier_set() {
        let future = 1_900_000_000_i64;
        let mut batch = vec![entity(
            1,
            vec![
                Op::Remove { field: "publishedAt".into(), value: json!(1_700_000_000_i64) },
                Op::Set { field: "publishedAt".into(), value: json!(1_800_000_000_i64) },
                Op::Remove { field: "publishedAt".into(), value: json!(future) },
            ],
        )];
        dedup_ops(&mut batch);

        let ops = &batch[0].ops;
        assert!(
            !ops.iter().any(|op| matches!(op, Op::Set { field, .. } if field == "publishedAt")),
            "the superseded Set must not survive: {ops:?}"
        );
        let removes: Vec<i64> = ops
            .iter()
            .filter_map(|op| match op {
                Op::Remove { field, value } if field == "publishedAt" => value.as_i64(),
                _ => None,
            })
            .collect();
        assert_eq!(
            removes,
            vec![future],
            "only the last arriving op survives, carrying its value: {ops:?}"
        );
    }

    /// Entities come back in first-arrival order too, not hash order.
    ///
    /// The batch is applied in vec order, and two entities can carry ops for the
    /// same slot in one batch — a Post fan-out (entity = post id) and a direct
    /// Image op (entity = image id) both write `publishedAt` for that image.
    /// Rebuilding the batch by consuming the entity map made which one landed
    /// last a per-process coin flip: the same failure shape as the one inside
    /// `dedup_entity_ops`, one level up.
    #[test]
    fn test_entities_keep_arrival_order() {
        let ids: Vec<i64> = (1..=24).map(|i| i * 7).collect();
        let mut batch: Vec<EntityOps> = ids
            .iter()
            .map(|id| entity(*id, vec![Op::Set { field: "nsfwLevel".into(), value: json!(1) }]))
            .collect();
        dedup_ops(&mut batch);
        let seen: Vec<i64> = batch.iter().map(|e| e.entity_id).collect();
        assert_eq!(
            seen, ids,
            "entities must be re-emitted in arrival order — the batch is applied \
             in this order and two entities can write the same slot"
        );
    }

    /// The ordinary scalar shape is unaffected: a remove of the OLD value
    /// followed by a set of the NEW one still yields both, remove first.
    #[test]
    fn test_remove_then_set_keeps_both_remove_first() {
        let mut batch = vec![entity(
            1,
            vec![
                Op::Remove { field: "availability".into(), value: json!("Private") },
                Op::Set { field: "availability".into(), value: json!("Public") },
            ],
        )];
        dedup_ops(&mut batch);
        assert_eq!(
            batch[0].ops,
            vec![
                Op::Remove { field: "availability".into(), value: json!("Private") },
                Op::Set { field: "availability".into(), value: json!("Public") },
            ]
        );
    }
}
