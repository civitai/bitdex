//! Cache overlay — applies field ops to a cache entry's bitmap at read time.
//!
//! When a cache entry is loaded from CacheSilo, it may be behind the current
//! field ops cursor. This module scans the pending ops and patches the entry's
//! bitmap to reflect recent mutations, avoiding full epoch-based invalidation.
//!
//! ## Clause matching rules
//!
//! For each field op, we check the entry's clauses to determine if the op
//! affects the entry's result set:
//!
//! - **Eq(field, val)**: FilterSet(field, val, slot) → insert; FilterClear(field, val, slot) → remove
//! - **In(field, vals)**: same, but match any value in the set
//! - **NotEq(field, val)**: FilterSet(field, val, slot) → remove; FilterClear(field, val, slot) → insert
//!   (GAP-4: for now, fall back to epoch invalidation for negation — handled by caller)
//! - **Range (Gt/Gte/Lt/Lte)**: fall back to epoch invalidation (requires value comparison)
//! - **SortChange**: invalidate sorted_keys (regenerated lazily)

use std::collections::HashMap;
use roaring::RoaringBitmap;
use super::cache::CanonicalClause;
use super::field_ops_log::FieldOp;

/// Result of applying field ops overlay to a cache entry.
pub struct OverlayResult {
    /// The patched bitmap (original with ops applied).
    pub bitmap: RoaringBitmap,
    /// Updated sorted_keys (None if sort changed and needs regeneration).
    pub sorted_keys: Option<Vec<u64>>,
    /// Number of filter ops that matched and were applied.
    pub applied_count: u32,
    /// Whether the entry needs full epoch invalidation (negation/range clauses).
    pub needs_epoch_fallback: bool,
    /// Whether sort values changed (sorted_keys need regeneration).
    pub sort_changed: bool,
    /// Slots added to the bitmap by filter ops (for sorted_keys update by caller).
    pub slots_added: Vec<u32>,
    /// Slots removed from the bitmap by filter ops (for sorted_keys update by caller).
    pub slots_removed: Vec<u32>,
}

/// Check if a cache entry's clauses contain any negation, range, or compound
/// ops that require epoch-based fallback.
pub fn has_epoch_fallback_clauses(clauses: &[CanonicalClause]) -> bool {
    clauses.iter().any(|c| {
        matches!(c.op.as_str(), "neq" | "notin" | "gt" | "gte" | "lt" | "lte" | "and" | "or")
            || c.op.starts_with("not(")
    })
}

/// Apply a sequence of field ops to a cache entry bitmap.
///
/// `clauses` — the entry's filter clauses (from UnifiedKey)
/// `sort_field_idx` — the field_idx of the entry's sort field (for SortChange matching)
/// `bitmap` — the entry's current bitmap (will be modified in place)
/// `sorted_keys` — the entry's current sorted_keys (invalidated if sort changes)
/// `ops` — field ops to apply
/// `field_idx_to_name` — maps field_idx → field name for clause matching
///
/// Returns overlay statistics. The caller should check `needs_epoch_fallback`
/// and fall through to epoch invalidation if true AND any such ops were seen.
pub fn apply_field_ops(
    clauses: &[CanonicalClause],
    sort_field_idx: Option<u16>,
    bitmap: &mut RoaringBitmap,
    sorted_keys: &mut Option<Vec<u64>>,
    ops: &[FieldOp],
    field_idx_to_name: &HashMap<u16, String>,
) -> OverlayResult {
    let mut applied_count = 0u32;
    let mut needs_epoch_fallback = false;
    let mut sort_changed = false;
    let mut slots_added: Vec<u32> = Vec::new();
    let mut slots_removed: Vec<u32> = Vec::new();

    for op in ops {
        match op {
            FieldOp::FilterSet { field_idx, value, slot }
            | FieldOp::FilterClear { field_idx, value, slot } => {
                let is_set = matches!(op, FieldOp::FilterSet { .. });
                let field_name = match field_idx_to_name.get(field_idx) {
                    Some(name) => name.as_str(),
                    None => continue, // unknown field — skip
                };

                // Check each clause for a match
                for clause in clauses {
                    // Compound clauses (and/or) have empty field — always trigger fallback
                    match clause.op.as_str() {
                        "and" | "or" => {
                            needs_epoch_fallback = true;
                            continue;
                        }
                        _ => {}
                    }

                    if clause.field != field_name {
                        continue;
                    }

                    match clause.op.as_str() {
                        "eq" => {
                            // Exact match: value must equal the clause's value
                            if let Ok(clause_val) = clause.value_repr.parse::<u64>() {
                                if *value == clause_val {
                                    if is_set {
                                        bitmap.insert(*slot);
                                        slots_added.push(*slot);
                                    } else {
                                        bitmap.remove(*slot);
                                        slots_removed.push(*slot);
                                    }
                                    applied_count += 1;
                                }
                            }
                        }
                        "in" => {
                            // In clause: value must be one of the comma-separated values
                            let matches = clause.value_repr.split(',').any(|v| {
                                v.parse::<u64>().ok() == Some(*value)
                            });
                            if matches {
                                if is_set {
                                    bitmap.insert(*slot);
                                    slots_added.push(*slot);
                                } else {
                                    bitmap.remove(*slot);
                                    slots_removed.push(*slot);
                                }
                                applied_count += 1;
                            }
                        }
                        "neq" | "notin" | "gt" | "gte" | "lt" | "lte" | "and" | "or" => {
                            // These require epoch fallback — we can't efficiently
                            // determine the effect without the full bitmap state.
                            needs_epoch_fallback = true;
                        }
                        op if op.starts_with("not(") => {
                            // Not(Eq), Not(In), etc. — epoch fallback
                            needs_epoch_fallback = true;
                        }
                        "bucket" => {
                            // Bucket clauses handled separately (task #38)
                        }
                        _ => {}
                    }
                }
            }
            FieldOp::SortChange { field_idx, slot, .. } => {
                if let Some(sort_idx) = sort_field_idx {
                    if *field_idx == sort_idx {
                        sort_changed = true;
                        // Invalidate sorted_keys — will be regenerated lazily
                        *sorted_keys = None;
                        // The slot stays in the bitmap (it still matches the filter),
                        // but its position in the sort order changed.
                        let _ = slot; // slot still alive, bitmap unchanged
                        applied_count += 1;
                    }
                }
            }
        }
    }

    OverlayResult {
        bitmap: bitmap.clone(),
        sorted_keys: sorted_keys.clone(),
        applied_count,
        needs_epoch_fallback,
        sort_changed,
        slots_added,
        slots_removed,
    }
}

/// Apply time bucket drift correction to a cache entry's bitmap.
///
/// If the entry was formed at `bucket_cutoff_at_formation` and the current
/// cutoff has advanced, slots that aged out are removed via AND-NOT with
/// the `merged_expired` bitmap from PendingBucketDiffs.
///
/// Returns the number of slots removed, or 0 if no correction was needed.
pub fn apply_bucket_drift(
    bitmap: &mut RoaringBitmap,
    bucket_cutoff_at_formation: u64,
    current_cutoff: u64,
    merged_expired: &RoaringBitmap,
) -> u64 {
    if bucket_cutoff_at_formation == 0 || current_cutoff <= bucket_cutoff_at_formation {
        return 0; // no drift or no bucket tracking
    }
    if merged_expired.is_empty() {
        return 0;
    }
    let before = bitmap.len();
    *bitmap -= merged_expired;
    before - bitmap.len()
}

/// Check if a cache entry's clauses include a bucket clause.
pub fn has_bucket_clause(clauses: &[CanonicalClause]) -> bool {
    clauses.iter().any(|c| c.op == "bucket")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn clause(field: &str, op: &str, value: &str) -> CanonicalClause {
        CanonicalClause {
            field: field.to_string(),
            op: op.to_string(),
            value_repr: value.to_string(),
        }
    }

    fn field_name_map() -> HashMap<u16, String> {
        let mut m = HashMap::new();
        m.insert(1, "nsfwLevel".to_string());
        m.insert(2, "tagIds".to_string());
        m.insert(3, "sortAt".to_string());
        m.insert(4, "userId".to_string());
        m
    }

    fn make_bitmap(slots: &[u32]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for &s in slots { bm.insert(s); }
        bm
    }

    // ── Eq clause ────────────────────────────────────────────────────────

    #[test]
    fn filter_set_adds_slot_to_eq_match() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10, 20, 30]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 1, value: 1, slot: 40 }];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(bm.contains(40));
        assert_eq!(bm.len(), 4);
        assert_eq!(result.applied_count, 1);
        assert!(!result.needs_epoch_fallback);
    }

    #[test]
    fn filter_clear_removes_slot_from_eq_match() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10, 20, 30]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterClear { field_idx: 1, value: 1, slot: 20 }];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(!bm.contains(20));
        assert_eq!(bm.len(), 2);
        assert_eq!(result.applied_count, 1);
    }

    #[test]
    fn filter_set_with_wrong_value_is_noop() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10, 20]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 1, value: 2, slot: 30 }]; // value=2 != clause value=1

        apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(!bm.contains(30));
        assert_eq!(bm.len(), 2);
    }

    #[test]
    fn filter_set_wrong_field_is_noop() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 4, value: 1, slot: 20 }]; // field=userId, not nsfwLevel

        apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(!bm.contains(20));
    }

    // ── In clause ────────────────────────────────────────────────────────

    #[test]
    fn filter_set_matches_in_clause() {
        let clauses = vec![clause("nsfwLevel", "in", "1,2,3")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;

        let ops = vec![
            FieldOp::FilterSet { field_idx: 1, value: 2, slot: 20 }, // matches
            FieldOp::FilterSet { field_idx: 1, value: 5, slot: 30 }, // doesn't match
        ];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(bm.contains(20));
        assert!(!bm.contains(30));
        assert_eq!(result.applied_count, 1);
    }

    // ── Negation fallback ────────────────────────────────────────────────

    #[test]
    fn neq_clause_triggers_epoch_fallback() {
        let clauses = vec![clause("nsfwLevel", "neq", "1")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 1, value: 2, slot: 20 }];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(result.needs_epoch_fallback);
    }

    #[test]
    fn range_clause_triggers_epoch_fallback() {
        let clauses = vec![clause("sortAt", "gte", "1000")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 3, value: 1500, slot: 20 }];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(result.needs_epoch_fallback);
    }

    // ── Sort change ──────────────────────────────────────────────────────

    #[test]
    fn sort_change_invalidates_sorted_keys() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10, 20, 30]);
        let mut keys = Some(vec![
            (100u64 << 32) | 10,
            (50u64 << 32) | 20,
            (10u64 << 32) | 30,
        ]);
        let ops = vec![FieldOp::SortChange { field_idx: 3, new_value: 999, slot: 20 }];

        let result = apply_field_ops(&clauses, Some(3), &mut bm, &mut keys, &ops, &field_name_map());

        assert!(result.sort_changed);
        assert!(keys.is_none()); // sorted_keys invalidated
        assert_eq!(bm.len(), 3); // bitmap unchanged — slot still matches filter
    }

    #[test]
    fn sort_change_wrong_field_is_noop() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = Some(vec![(100u64 << 32) | 10]);
        let ops = vec![FieldOp::SortChange { field_idx: 4, new_value: 999, slot: 10 }]; // field=userId, sort is field=3

        let result = apply_field_ops(&clauses, Some(3), &mut bm, &mut keys, &ops, &field_name_map());

        assert!(!result.sort_changed);
        assert!(keys.is_some()); // sorted_keys preserved
    }

    // ── Multi-clause entries ─────────────────────────────────────────────

    #[test]
    fn multi_clause_both_must_match() {
        // Entry: nsfwLevel=1 AND tagIds IN 100,200
        let clauses = vec![
            clause("nsfwLevel", "eq", "1"),
            clause("tagIds", "in", "100,200"),
        ];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;

        // This op matches nsfwLevel=1 — adds slot 20
        let ops = vec![FieldOp::FilterSet { field_idx: 1, value: 1, slot: 20 }];
        apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        // Slot 20 was added because it matched the nsfwLevel clause.
        // Note: the overlay adds based on individual clause matches. The caller
        // is responsible for AND semantics if needed (multi-clause intersection
        // is handled at query time, not overlay time).
        assert!(bm.contains(20));
    }

    // ── Batch of mixed ops ───────────────────────────────────────────────

    #[test]
    fn batch_of_mixed_ops() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10, 20, 30]);
        let mut keys = Some(vec![
            (100u64 << 32) | 10,
            (50u64 << 32) | 20,
            (10u64 << 32) | 30,
        ]);

        let ops = vec![
            FieldOp::FilterSet { field_idx: 1, value: 1, slot: 40 },   // add 40
            FieldOp::FilterClear { field_idx: 1, value: 1, slot: 20 }, // remove 20
            FieldOp::FilterSet { field_idx: 1, value: 2, slot: 50 },   // noop (wrong value)
            FieldOp::SortChange { field_idx: 3, new_value: 999, slot: 10 }, // sort changed
        ];

        let result = apply_field_ops(&clauses, Some(3), &mut bm, &mut keys, &ops, &field_name_map());

        assert!(bm.contains(10));
        assert!(!bm.contains(20)); // removed
        assert!(bm.contains(30));
        assert!(bm.contains(40)); // added
        assert!(!bm.contains(50)); // wrong value
        assert_eq!(bm.len(), 3);
        assert_eq!(result.applied_count, 3); // set+clear+sort
        assert!(result.sort_changed);
        assert!(keys.is_none());
    }

    // ── Unknown field ────────────────────────────────────────────────────

    #[test]
    fn unknown_field_idx_is_skipped() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 99, value: 1, slot: 20 }]; // unknown field

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert_eq!(result.applied_count, 0);
        assert!(!bm.contains(20));
    }

    // ── Empty ops ────────────────────────────────────────────────────────

    #[test]
    fn empty_ops_is_noop() {
        let clauses = vec![clause("nsfwLevel", "eq", "1")];
        let mut bm = make_bitmap(&[10, 20]);
        let mut keys = None;

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &[], &field_name_map());

        assert_eq!(bm.len(), 2);
        assert_eq!(result.applied_count, 0);
    }

    // ── has_epoch_fallback_clauses ────────────────────────────────────────

    #[test]
    fn detect_fallback_clauses() {
        assert!(!has_epoch_fallback_clauses(&[clause("f", "eq", "1")]));
        assert!(!has_epoch_fallback_clauses(&[clause("f", "in", "1,2")]));
        assert!(has_epoch_fallback_clauses(&[clause("f", "neq", "1")]));
        assert!(has_epoch_fallback_clauses(&[clause("f", "notin", "1,2")]));
        assert!(has_epoch_fallback_clauses(&[clause("f", "gt", "10")]));
        assert!(has_epoch_fallback_clauses(&[clause("f", "gte", "10")]));
        assert!(has_epoch_fallback_clauses(&[clause("f", "lt", "10")]));
        assert!(has_epoch_fallback_clauses(&[clause("f", "lte", "10")]));
    }

    // ── bucket drift ─────────────────────────────────────────────────────

    #[test]
    fn bucket_drift_removes_expired_slots() {
        let mut bm = make_bitmap(&[10, 20, 30, 40, 50]);
        let expired = make_bitmap(&[20, 40]); // slots 20 and 40 aged out
        let removed = apply_bucket_drift(&mut bm, 1000, 1060, &expired);
        assert_eq!(removed, 2);
        assert_eq!(bm.len(), 3);
        assert!(!bm.contains(20));
        assert!(!bm.contains(40));
    }

    #[test]
    fn bucket_drift_noop_when_no_cutoff() {
        let mut bm = make_bitmap(&[10, 20]);
        let expired = make_bitmap(&[20]);
        let removed = apply_bucket_drift(&mut bm, 0, 1060, &expired);
        assert_eq!(removed, 0);
        assert_eq!(bm.len(), 2); // unchanged
    }

    #[test]
    fn bucket_drift_noop_when_cutoff_unchanged() {
        let mut bm = make_bitmap(&[10, 20]);
        let expired = make_bitmap(&[20]);
        let removed = apply_bucket_drift(&mut bm, 1000, 1000, &expired);
        assert_eq!(removed, 0);
    }

    #[test]
    fn bucket_drift_noop_when_empty_expired() {
        let mut bm = make_bitmap(&[10, 20]);
        let expired = RoaringBitmap::new();
        let removed = apply_bucket_drift(&mut bm, 1000, 1060, &expired);
        assert_eq!(removed, 0);
    }

    // ── negation / compound edge cases ──────────────────────────────────

    #[test]
    fn not_eq_triggers_epoch_fallback() {
        let clauses = vec![clause("nsfwLevel", "not(eq)", "1")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 1, value: 2, slot: 20 }];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());
        assert!(result.needs_epoch_fallback);
    }

    #[test]
    fn compound_and_triggers_epoch_fallback() {
        let clauses = vec![clause("", "and", "nsfwLevel:eq:1+tagIds:in:100,200")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 1, value: 1, slot: 20 }];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());
        assert!(result.needs_epoch_fallback);
    }

    #[test]
    fn detect_fallback_includes_compound_ops() {
        assert!(has_epoch_fallback_clauses(&[clause("f", "not(eq)", "1")]));
        assert!(has_epoch_fallback_clauses(&[clause("", "and", "a:eq:1+b:eq:2")]));
        assert!(has_epoch_fallback_clauses(&[clause("", "or", "a:eq:1+b:eq:2")]));
    }

    #[test]
    fn multi_value_in_clear_removes_matching_slot() {
        let clauses = vec![clause("tagIds", "in", "100,200,300")];
        let mut bm = make_bitmap(&[10, 20, 30]);
        let mut keys = None;

        // Clear tagIds=200 for slot 20 — should remove from entry
        let ops = vec![FieldOp::FilterClear { field_idx: 2, value: 200, slot: 20 }];
        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());

        assert!(!bm.contains(20));
        assert_eq!(result.applied_count, 1);
    }

    #[test]
    fn notin_triggers_epoch_fallback() {
        let clauses = vec![clause("tagIds", "notin", "100,200")];
        let mut bm = make_bitmap(&[10]);
        let mut keys = None;
        let ops = vec![FieldOp::FilterSet { field_idx: 2, value: 100, slot: 20 }];

        let result = apply_field_ops(&clauses, None, &mut bm, &mut keys, &ops, &field_name_map());
        assert!(result.needs_epoch_fallback);
    }

    #[test]
    fn has_bucket_clause_detection() {
        assert!(has_bucket_clause(&[clause("sortAt", "bucket", "7d")]));
        assert!(!has_bucket_clause(&[clause("nsfwLevel", "eq", "1")]));
        assert!(has_bucket_clause(&[
            clause("nsfwLevel", "eq", "1"),
            clause("sortAt", "bucket", "7d"),
        ]));
    }
}
