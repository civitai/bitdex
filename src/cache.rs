// Cache key canonicalization for filter clauses.
//
// Provides canonical key generation for cache lookups. Filter clauses are
// converted into a sorted sequence of CanonicalClause structs, ensuring
// that equivalent filter sets always produce the same cache key regardless
// of clause ordering.

use serde::{Deserialize, Serialize};

use crate::query::{FilterClause, Value};

/// A single canonicalized filter clause key component.
/// Clauses are sorted by field name, then by string representation of value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CanonicalClause {
    pub field: String,
    pub op: String,
    pub value_repr: String,
}

impl CanonicalClause {
    pub(crate) fn from_filter(clause: &FilterClause) -> Option<Self> {
        match clause {
            FilterClause::Eq(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "eq".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::NotEq(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "neq".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::In(field, values) => {
                let mut sorted_values: Vec<String> = values.iter().map(value_to_string).collect();
                sorted_values.sort();
                Some(CanonicalClause {
                    field: field.clone(),
                    op: "in".to_string(),
                    value_repr: sorted_values.join(","),
                })
            }
            FilterClause::NotIn(field, values) => {
                let mut sorted_values: Vec<String> = values.iter().map(value_to_string).collect();
                sorted_values.sort();
                Some(CanonicalClause {
                    field: field.clone(),
                    op: "notin".to_string(),
                    value_repr: sorted_values.join(","),
                })
            }
            FilterClause::Gt(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "gt".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::Gte(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "gte".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::Lt(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "lt".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::Lte(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "lte".to_string(),
                value_repr: value_to_string(value),
            }),
            // BucketBitmap uses a stable key: op="bucket", value_repr=bucket_name.
            // This ensures queries snapped to "sortAt:7d" always produce the same cache key
            // regardless of the raw timestamp value supplied by the caller (C5).
            FilterClause::BucketBitmap { field, bucket_name, .. } => Some(CanonicalClause {
                field: field.clone(),
                op: "bucket".to_string(),
                value_repr: bucket_name.clone(),
            }),
            // Compound clauses: recursively canonicalize sub-clauses
            FilterClause::Not(inner) => {
                let inner_key = Self::from_filter(inner)?;
                Some(CanonicalClause {
                    field: inner_key.field,
                    op: format!("not({})", inner_key.op),
                    value_repr: inner_key.value_repr,
                })
            }
            FilterClause::And(inner) => {
                let mut parts: Vec<String> = Vec::new();
                for c in inner {
                    let k = Self::from_filter(c)?;
                    parts.push(format!("{}:{}:{}", k.field, k.op, k.value_repr));
                }
                parts.sort();
                Some(CanonicalClause {
                    field: "".to_string(),
                    op: "and".to_string(),
                    value_repr: parts.join("+"),
                })
            }
            FilterClause::Or(inner) => {
                let mut parts: Vec<String> = Vec::new();
                for c in inner {
                    let k = Self::from_filter(c)?;
                    parts.push(format!("{}:{}:{}", k.field, k.op, k.value_repr));
                }
                parts.sort();
                Some(CanonicalClause {
                    field: "".to_string(),
                    op: "or".to_string(),
                    value_repr: parts.join("|"),
                })
            }
            FilterClause::IsNull(field) => Some(CanonicalClause {
                field: field.clone(),
                op: "isnull".to_string(),
                value_repr: String::new(),
            }),
            FilterClause::IsNotNull(field) => Some(CanonicalClause {
                field: field.clone(),
                op: "isnotnull".to_string(),
                value_repr: String::new(),
            }),
        }
    }

    /// Convert a canonical clause back to a FilterClause.
    ///
    /// This is a best-effort reverse mapping used by the prefetch worker.
    /// Compound clauses (And, Or, Not) and BucketBitmap are not supported
    /// and return None. The prefetch worker skips entries with unconvertible clauses.
    pub fn to_filter_clause(cc: &CanonicalClause) -> Option<FilterClause> {
        let parse_value = |s: &str| -> Value {
            if let Ok(i) = s.parse::<i64>() {
                Value::Integer(i)
            } else if s == "true" {
                Value::Bool(true)
            } else if s == "false" {
                Value::Bool(false)
            } else if let Ok(f) = s.parse::<f64>() {
                Value::Float(f)
            } else {
                Value::String(s.to_string())
            }
        };

        let parse_values = |s: &str| -> Vec<Value> {
            s.split(',').map(|v| parse_value(v)).collect()
        };

        match cc.op.as_str() {
            "eq" => Some(FilterClause::Eq(cc.field.clone(), parse_value(&cc.value_repr))),
            "neq" => Some(FilterClause::NotEq(cc.field.clone(), parse_value(&cc.value_repr))),
            "in" => Some(FilterClause::In(cc.field.clone(), parse_values(&cc.value_repr))),
            "notin" => Some(FilterClause::NotIn(cc.field.clone(), parse_values(&cc.value_repr))),
            "gt" => Some(FilterClause::Gt(cc.field.clone(), parse_value(&cc.value_repr))),
            "gte" => Some(FilterClause::Gte(cc.field.clone(), parse_value(&cc.value_repr))),
            "lt" => Some(FilterClause::Lt(cc.field.clone(), parse_value(&cc.value_repr))),
            "lte" => Some(FilterClause::Lte(cc.field.clone(), parse_value(&cc.value_repr))),
            // Compound ops (and, or, not) and bucket are not reversible — skip
            _ => None,
        }
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Bool(b) => b.to_string(),
        Value::String(s) => s.clone(),
    }
}

/// Canonical key for the cache: a sorted sequence of filter clause keys.
pub type CacheKey = Vec<CanonicalClause>;

/// Convert a set of filter clauses into a canonical cache key.
/// Returns None if any clause cannot be canonicalized (compound clauses).
/// Flattens top-level clauses and sorts by (field, op, value).
pub fn canonicalize(clauses: &[FilterClause]) -> Option<CacheKey> {
    let mut key_parts: Vec<CanonicalClause> = Vec::new();
    for clause in clauses {
        match clause {
            // Flatten top-level And into individual clauses
            FilterClause::And(inner) => {
                for c in inner {
                    key_parts.push(CanonicalClause::from_filter(c)?);
                }
            }
            _ => {
                key_parts.push(CanonicalClause::from_filter(clause)?);
            }
        }
    }
    key_parts.sort();
    Some(key_parts)
}

/// Produce a stable canonical clause for a bucket-snapped range filter (C5).
/// Used by `canonicalize_with_buckets` to ensure range queries on bucketed fields
/// produce deterministic cache keys independent of the raw timestamp value.
pub fn snap_clause_key(
    clause: &FilterClause,
    tb: &crate::time_buckets::TimeBucketManager,
    now_unix: u64,
) -> Option<CanonicalClause> {
    match clause {
        FilterClause::Gt(field, value) | FilterClause::Gte(field, value)
            if field == tb.field_name() =>
        {
            let threshold = match value {
                // Unit-normalized (#305 review F2): raw ms saturated the
                // duration to 0 here. Dead code today (live cache keys come
                // from post-snap BucketBitmap bucket_name), fixed so it can't
                // be resurrected with the bug intact.
                Value::Integer(v) => Some(crate::query::ts_secs_u64(*v as u64)),
                _ => None,
            }?;
            let duration = now_unix.saturating_sub(threshold);
            let bucket_name = tb.snap_duration(duration, 0.10)?;
            Some(CanonicalClause {
                field: field.clone(),
                op: "bucket_gte".to_string(),
                value_repr: bucket_name.to_string(),
            })
        }
        FilterClause::Lt(field, value) | FilterClause::Lte(field, value)
            if field == tb.field_name() =>
        {
            let threshold = match value {
                // Unit-normalized (#305 review F2) — see the Gte arm.
                Value::Integer(v) => Some(crate::query::ts_secs_u64(*v as u64)),
                _ => None,
            }?;
            let duration = threshold.saturating_sub(now_unix);
            let bucket_name = tb.snap_duration(duration, 0.10)?;
            Some(CanonicalClause {
                field: field.clone(),
                op: "bucket_lte".to_string(),
                value_repr: bucket_name.to_string(),
            })
        }
        _ => None,
    }
}

/// Canonicalize filter clauses with optional bucket snapping (C5).
/// Bucket-eligible range clauses produce stable keys (e.g., "sortAt:bucket_gte:7d")
/// instead of raw timestamps that change every second.
pub fn canonicalize_with_buckets(
    clauses: &[FilterClause],
    tb: Option<&crate::time_buckets::TimeBucketManager>,
    now_unix: u64,
) -> Option<CacheKey> {
    let mut canonical_clauses: Vec<CanonicalClause> = Vec::new();
    for clause in clauses {
        // Try bucket snapping first
        if let Some(tb) = tb {
            if let Some(snapped) = snap_clause_key(clause, tb, now_unix) {
                canonical_clauses.push(snapped);
                continue;
            }
        }
        // Fall through to normal canonicalization
        match clause {
            // Flatten top-level And into individual clauses
            FilterClause::And(inner) => {
                for c in inner {
                    canonical_clauses.push(CanonicalClause::from_filter(c)?);
                }
            }
            _ => {
                canonical_clauses.push(CanonicalClause::from_filter(clause)?);
            }
        }
    }
    canonical_clauses.sort();
    Some(canonical_clauses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::FilterClause;
    use roaring::RoaringBitmap;

    fn eq_clause(field: &str, value: i64) -> FilterClause {
        FilterClause::Eq(field.to_string(), Value::Integer(value))
    }

    #[test]
    fn test_canonicalize_simple() {
        let clauses = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("userId", 42),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 2);
        // Should be sorted by field name: nsfwLevel < userId
        assert_eq!(key[0].field, "nsfwLevel");
        assert_eq!(key[1].field, "userId");
    }

    #[test]
    fn test_canonicalize_order_independent() {
        let clauses_a = vec![
            eq_clause("userId", 42),
            eq_clause("nsfwLevel", 1),
        ];
        let clauses_b = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("userId", 42),
        ];
        let key_a = canonicalize(&clauses_a).unwrap();
        let key_b = canonicalize(&clauses_b).unwrap();
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn test_canonicalize_flattens_and() {
        let clauses = vec![
            FilterClause::And(vec![
                eq_clause("nsfwLevel", 1),
                eq_clause("userId", 42),
            ]),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 2);
    }

    #[test]
    fn test_canonicalize_handles_compound_not() {
        let clauses = vec![
            FilterClause::Not(Box::new(eq_clause("nsfwLevel", 28))),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(key[0].field, "nsfwLevel");
        assert_eq!(key[0].op, "not(eq)");
        assert_eq!(key[0].value_repr, "28");
    }

    #[test]
    fn test_canonicalize_handles_compound_or() {
        let clauses = vec![
            FilterClause::Or(vec![
                eq_clause("nsfwLevel", 1),
                eq_clause("userId", 42),
            ]),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(key[0].op, "or");
    }

    #[test]
    fn test_canonicalize_handles_deeply_nested() {
        // Not(Or(...)) produces a stable key
        let clauses = vec![
            FilterClause::Not(Box::new(FilterClause::Or(vec![
                eq_clause("a", 1),
                eq_clause("b", 2),
            ]))),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(key[0].op, "not(or)");
    }

    #[test]
    fn test_canonicalize_in_clause_sorts_values() {
        let clauses = vec![
            FilterClause::In(
                "tagIds".to_string(),
                vec![Value::Integer(300), Value::Integer(100), Value::Integer(200)],
            ),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key[0].value_repr, "100,200,300");
    }

    #[test]
    fn test_not_eq_clause_in_key() {
        let clauses = vec![
            FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(28)),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(key[0].op, "neq");
    }

    #[test]
    fn test_range_clauses_in_key() {
        let clauses = vec![
            FilterClause::Gte("nsfwLevel".to_string(), Value::Integer(1)),
            FilterClause::Lt("nsfwLevel".to_string(), Value::Integer(28)),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 2);
    }

    fn make_bucket_bitmap(field: &str, bucket_name: &str) -> FilterClause {
        FilterClause::BucketBitmap {
            field: field.to_string(),
            bucket_name: bucket_name.to_string(),
            bitmap: std::sync::Arc::new(RoaringBitmap::new()),
        }
    }

    #[test]
    fn test_bucket_bitmap_produces_stable_key() {
        let clause_a = make_bucket_bitmap("sortAt", "7d");
        let clause_b = FilterClause::BucketBitmap {
            field: "sortAt".to_string(),
            bucket_name: "7d".to_string(),
            bitmap: std::sync::Arc::new({
                let mut bm = RoaringBitmap::new();
                bm.insert(42);
                bm
            }),
        };

        let key_a = canonicalize(&[clause_a]).unwrap();
        let key_b = canonicalize(&[clause_b]).unwrap();

        assert_eq!(key_a, key_b);
        assert_eq!(key_a[0].op, "bucket");
        assert_eq!(key_a[0].value_repr, "7d");
    }

    #[test]
    fn test_bucket_bitmap_key_field_name() {
        let clause = make_bucket_bitmap("sortAt", "24h");
        let key = canonicalize(&[clause]).unwrap();
        assert_eq!(key[0].field, "sortAt");
        assert_eq!(key[0].op, "bucket");
        assert_eq!(key[0].value_repr, "24h");
    }

    #[test]
    fn test_bucket_bitmap_different_bucket_names_different_keys() {
        let key_24h = canonicalize(&[make_bucket_bitmap("sortAt", "24h")]).unwrap();
        let key_7d = canonicalize(&[make_bucket_bitmap("sortAt", "7d")]).unwrap();
        assert_ne!(key_24h, key_7d);
    }

    #[test]
    fn test_bucket_bitmap_combined_with_eq_in_key() {
        let bucket = make_bucket_bitmap("sortAt", "7d");
        let eq = eq_clause("nsfwLevel", 1);
        let key = canonicalize(&[bucket, eq]).unwrap();
        assert_eq!(key.len(), 2);
        // Sorted by field: "nsfwLevel" < "sortAt"
        assert_eq!(key[0].field, "nsfwLevel");
        assert_eq!(key[1].field, "sortAt");
        assert_eq!(key[1].op, "bucket");
    }

    fn make_bucket_manager(now: u64) -> crate::time_buckets::TimeBucketManager {
        use crate::config::BucketConfig;
        let configs = vec![
            BucketConfig { name: "24h".to_string(), duration_secs: 86400, refresh_interval_secs: 300 },
            BucketConfig { name: "7d".to_string(), duration_secs: 604800, refresh_interval_secs: 3600 },
        ];
        let mut mgr = crate::time_buckets::TimeBucketManager::new("sortAt".to_string(), configs);
        mgr.rebuild_bucket("24h", std::iter::empty(), now);
        mgr.rebuild_bucket("7d", std::iter::empty(), now);
        mgr
    }

    #[test]
    fn test_snap_clause_key_gte_snaps_to_24h() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let ts = (now - 86400) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        let snapped = snap_clause_key(&clause, &mgr, now).unwrap();

        assert_eq!(snapped.field, "sortAt");
        assert_eq!(snapped.op, "bucket_gte");
        assert_eq!(snapped.value_repr, "24h");
    }

    #[test]
    fn test_snap_clause_key_gt_snaps_to_7d() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let ts = (now - 590000) as i64;
        let clause = FilterClause::Gt("sortAt".to_string(), Value::Integer(ts));
        let snapped = snap_clause_key(&clause, &mgr, now).unwrap();

        assert_eq!(snapped.op, "bucket_gte");
        assert_eq!(snapped.value_repr, "7d");
    }

    #[test]
    fn test_snap_clause_key_outside_tolerance_returns_none() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let ts = (now - 200000) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        assert!(snap_clause_key(&clause, &mgr, now).is_none());
    }

    #[test]
    fn test_snap_clause_key_non_bucketed_field_returns_none() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let clause = FilterClause::Gte("nsfwLevel".to_string(), Value::Integer(1));
        assert!(snap_clause_key(&clause, &mgr, now).is_none());
    }

    #[test]
    fn test_snap_clause_key_non_range_clause_returns_none() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let clause = FilterClause::Eq("sortAt".to_string(), Value::Integer(1_700_000_000));
        assert!(snap_clause_key(&clause, &mgr, now).is_none());
    }

    #[test]
    fn test_canonicalize_with_buckets_snaps_gte() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let ts = (now - 86400) as i64;
        let clauses = vec![
            FilterClause::Gte("sortAt".to_string(), Value::Integer(ts)),
            eq_clause("nsfwLevel", 1),
        ];
        let key = canonicalize_with_buckets(&clauses, Some(&mgr), now).unwrap();

        assert_eq!(key.len(), 2);
        assert_eq!(key[0].field, "nsfwLevel");
        assert_eq!(key[0].op, "eq");
        assert_eq!(key[1].field, "sortAt");
        assert_eq!(key[1].op, "bucket_gte");
        assert_eq!(key[1].value_repr, "24h");
    }

    #[test]
    fn test_canonicalize_with_buckets_stable_key_across_different_timestamps() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let ts1 = (now - 86400) as i64;
        let ts2 = (now - 83000) as i64;

        let key1 = canonicalize_with_buckets(
            &[FilterClause::Gte("sortAt".to_string(), Value::Integer(ts1))],
            Some(&mgr),
            now,
        ).unwrap();
        let key2 = canonicalize_with_buckets(
            &[FilterClause::Gte("sortAt".to_string(), Value::Integer(ts2))],
            Some(&mgr),
            now,
        ).unwrap();

        assert_eq!(key1, key2);
        assert_eq!(key1[0].value_repr, "24h");
    }

    #[test]
    fn test_canonicalize_with_buckets_falls_through_for_eq() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize_with_buckets(&clauses, Some(&mgr), now).unwrap();

        assert_eq!(key.len(), 1);
        assert_eq!(key[0].op, "eq");
    }

    #[test]
    fn test_canonicalize_with_buckets_none_tb_behaves_like_canonicalize() {
        let clauses = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("userId", 42),
        ];
        let key_plain = canonicalize(&clauses).unwrap();
        let key_buckets = canonicalize_with_buckets(&clauses, None, 0).unwrap();
        assert_eq!(key_plain, key_buckets);
    }
}
