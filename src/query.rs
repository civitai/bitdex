use std::fmt;
use std::sync::Arc;

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

/// A parsed query ready for execution by the bitmap engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitdexQuery {
    pub filters: Vec<FilterClause>,
    pub sort: Option<SortClause>,
    pub limit: usize,
    pub cursor: Option<CursorPosition>,
    /// Offset-based pagination: skip the first N results before returning.
    /// Used for shadow mode compatibility with Meilisearch's offset pagination.
    /// When both cursor and offset are set, cursor takes precedence.
    #[serde(default)]
    pub offset: Option<usize>,
    /// When true, bypass the unified cache entirely (no lookup, no insert).
    /// Useful for debugging to isolate whether cache maintenance is injecting
    /// incorrect data vs the underlying bitmaps being wrong.
    #[serde(default)]
    pub skip_cache: bool,
}

/// A filter clause representing a predicate on indexed data.
///
/// The engine evaluates these against roaring bitmaps:
/// - `Eq`/`NotEq`/`In`/`Gt`/`Lt`/`Gte`/`Lte` operate on individual field bitmaps
/// - `And`/`Or`/`Not` compose sub-clauses via bitmap intersection/union/complement
/// - `BucketBitmap` is an internal variant used by bucket snapping (C3/C5): a pre-computed
///   bitmap for a named time bucket, never constructed by the user or serialized from input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterClause {
    Eq(String, Value),
    NotEq(String, Value),
    In(String, Vec<Value>),
    NotIn(String, Vec<Value>),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
    Not(Box<FilterClause>),
    And(Vec<FilterClause>),
    Or(Vec<FilterClause>),
    /// Field value is null (uses NULL_BITMAP_KEY sentinel in filter bitmaps).
    IsNull(String),
    /// Field value is not null.
    IsNotNull(String),
    /// Pre-computed bucket bitmap produced by range-to-bucket snapping.
    /// - `field` — the filter field name (e.g. "sortAt")
    /// - `bucket_name` — the human-readable bucket name (e.g. "7d"), used as the stable cache key
    /// - `bitmap` — the pre-computed roaring bitmap for the bucket
    ///
    /// This variant is produced internally by `snap_range_clauses` and is never serialized
    /// to or from user-facing query input.
    #[serde(skip)]
    BucketBitmap {
        field: String,
        bucket_name: String,
        bitmap: Arc<RoaringBitmap>,
    },
}

/// A dynamically-typed value used in filter predicates and cursors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Bool(bool),
    String(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
        }
    }
}

impl fmt::Display for FilterClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterClause::Eq(field, val) => write!(f, "{field} = {val}"),
            FilterClause::NotEq(field, val) => write!(f, "{field} != {val}"),
            FilterClause::In(field, vals) => {
                let vals_str: Vec<String> = vals.iter().map(|v| v.to_string()).collect();
                write!(f, "{field} IN [{}]", vals_str.join(", "))
            }
            FilterClause::NotIn(field, vals) => {
                let vals_str: Vec<String> = vals.iter().map(|v| v.to_string()).collect();
                write!(f, "{field} NOT IN [{}]", vals_str.join(", "))
            }
            FilterClause::Gt(field, val) => write!(f, "{field} > {val}"),
            FilterClause::Lt(field, val) => write!(f, "{field} < {val}"),
            FilterClause::Gte(field, val) => write!(f, "{field} >= {val}"),
            FilterClause::Lte(field, val) => write!(f, "{field} <= {val}"),
            FilterClause::Not(inner) => write!(f, "NOT({inner})"),
            FilterClause::And(clauses) => {
                let parts: Vec<String> = clauses.iter().map(|c| c.to_string()).collect();
                write!(f, "({})", parts.join(" AND "))
            }
            FilterClause::Or(clauses) => {
                let parts: Vec<String> = clauses.iter().map(|c| c.to_string()).collect();
                write!(f, "({})", parts.join(" OR "))
            }
            FilterClause::BucketBitmap { field, bucket_name, .. } => {
                write!(f, "{field} ~bucket({bucket_name})")
            }
            FilterClause::IsNull(field) => write!(f, "{field} IS NULL"),
            FilterClause::IsNotNull(field) => write!(f, "{field} IS NOT NULL"),
        }
    }
}

impl fmt::Display for BitdexQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Filters
        if self.filters.is_empty() {
            write!(f, "WHERE *")?;
        } else {
            let parts: Vec<String> = self.filters.iter().map(|c| c.to_string()).collect();
            write!(f, "WHERE {}", parts.join(" AND "))?;
        }
        // Sort
        if let Some(ref sort) = self.sort {
            write!(f, " ORDER BY {} {:?}", sort.field, sort.direction)?;
        }
        // Limit
        write!(f, " LIMIT {}", self.limit)?;
        // Offset
        if let Some(offset) = self.offset {
            write!(f, " OFFSET {offset}")?;
        }
        // Cursor
        if let Some(ref cursor) = self.cursor {
            write!(f, " CURSOR(val={}, slot={})", cursor.sort_value, cursor.slot_id)?;
        }
        Ok(())
    }
}

/// Sort specification for query results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SortClause {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Cursor position for keyset pagination.
///
/// The cursor encodes the last seen sort value and slot ID, enabling
/// the engine to resume traversal from the exact position.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorPosition {
    pub sort_value: u64,
    pub slot_id: u32,
}

/// Errors produced by query parsers.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct ParseError {
    pub message: String,
}

impl ParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Trait for query parsers that convert raw input into `BitdexQuery`.
///
/// V2 ships with a JSON parser. Future plugins (OpenSearch DSL,
/// Meilisearch syntax) will implement this same trait.
pub trait QueryParser: Send + Sync {
    fn parse(&self, input: &[u8]) -> Result<BitdexQuery, ParseError>;
    fn content_type(&self) -> &str;
}

/// Context for bucket snapping: maps field names to their `TimeBucketManager`.
///
/// Passed to `snap_range_clauses` so the executor can resolve range filters against
/// pre-computed time buckets (C3).
pub struct BucketSnapContext<'a> {
    /// Map from field name → TimeBucketManager for that field.
    pub managers: &'a std::collections::HashMap<String, &'a crate::time_buckets::TimeBucketManager>,
    /// Current unix timestamp in seconds (used to compute `now - duration` for Gt/Gte).
    pub now_secs: u64,
    /// Snap tolerance as a fraction (e.g. 0.10 for 10%).
    pub tolerance_pct: f64,
    /// If true, queries outside tolerance snap to the nearest bucket instead of returning empty.
    /// Default: true (always snap for safety).
    pub always_snap: bool,
}

/// Pre-process filter clauses: replace range filters on bucketed timestamp fields with
/// pre-computed `BucketBitmap` clauses (C3).
///
/// For each `Gt(field, Integer(ts))` or `Gte(field, Integer(ts))` where `field` has a
/// `TimeBucketManager` registered in `ctx`, compute `duration = now - ts` and try to snap
/// to the nearest bucket within tolerance. If snapping succeeds, replace the clause with
/// `BucketBitmap { field, bucket_name, bitmap }`.
///
/// `Lt` / `Lte` are not snapped — time buckets are "newer than X" windows.
///
/// Returns the transformed clause list. Clauses that don't snap are returned unchanged.
pub fn snap_range_clauses(
    clauses: &[FilterClause],
    ctx: &BucketSnapContext<'_>,
) -> Vec<FilterClause> {
    clauses
        .iter()
        .map(|clause| snap_clause(clause, ctx))
        .collect()
}

fn snap_clause(clause: &FilterClause, ctx: &BucketSnapContext<'_>) -> FilterClause {
    match clause {
        FilterClause::Gt(field, Value::Integer(ts)) | FilterClause::Gte(field, Value::Integer(ts)) => {
            if let Some(snapped) = try_snap_to_bucket(field, *ts, ctx) {
                snapped
            } else if let Some(manager) = ctx.managers.get(field.as_str()) {
                if ctx.always_snap {
                    // Always snap to nearest bucket for safety — returns the smallest
                    // bucket that covers the requested duration, or the largest bucket.
                    let duration_secs = ctx.now_secs.saturating_sub(*ts as u64);
                    let bucket_name = manager.snap_nearest(duration_secs);
                    if let Some(bucket) = manager.get_bucket(bucket_name) {
                        FilterClause::BucketBitmap {
                            field: field.clone(),
                            bucket_name: bucket_name.to_string(),
                            bitmap: Arc::clone(bucket.bitmap()),
                        }
                    } else {
                        FilterClause::BucketBitmap {
                            field: field.clone(),
                            bucket_name: "_none".to_string(),
                            bitmap: Arc::new(RoaringBitmap::new()),
                        }
                    }
                } else {
                    // Unsnapped queries allowed — return empty bitmap for out-of-range.
                    FilterClause::BucketBitmap {
                        field: field.clone(),
                        bucket_name: "_none".to_string(),
                        bitmap: Arc::new(RoaringBitmap::new()),
                    }
                }
            } else {
                clause.clone()
            }
        }

        // Recurse into And/Or — but not Not (semantics differ)
        FilterClause::And(inner) => {
            FilterClause::And(inner.iter().map(|c| snap_clause(c, ctx)).collect())
        }
        FilterClause::Or(inner) => {
            FilterClause::Or(inner.iter().map(|c| snap_clause(c, ctx)).collect())
        }

        // All other variants pass through unchanged.
        other => other.clone(),
    }
}

/// Try to snap a `Gt/Gte(field, ts)` to a bucket bitmap.
/// Returns `Some(BucketBitmap { ... })` if snapping succeeds, `None` otherwise.
fn try_snap_to_bucket(
    field: &str,
    ts: i64,
    ctx: &BucketSnapContext<'_>,
) -> Option<FilterClause> {
    let manager = ctx.managers.get(field)?;
    // duration = now - ts (the window the filter requests)
    let duration_secs = ctx.now_secs.saturating_sub(ts as u64);
    let bucket_name = manager.snap_duration(duration_secs, ctx.tolerance_pct)?;
    let bucket = manager.get_bucket(bucket_name)?;
    Some(FilterClause::BucketBitmap {
        field: field.to_string(),
        bucket_name: bucket_name.to_string(),
        bitmap: Arc::clone(bucket.bitmap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BucketConfig;
    use crate::time_buckets::TimeBucketManager;
    use std::collections::HashMap;

    /// Build a BucketSnapContext with a single field and two buckets (24h, 7d).
    fn make_ctx<'a>(
        managers: &'a HashMap<String, &'a TimeBucketManager>,
        now_secs: u64,
    ) -> BucketSnapContext<'a> {
        BucketSnapContext {
            managers,
            now_secs,
            tolerance_pct: 0.10,
            always_snap: true,
        }
    }

    fn make_manager_with_data(now: u64) -> TimeBucketManager {
        let configs = vec![
            BucketConfig { name: "24h".to_string(), duration_secs: 86400, refresh_interval_secs: 300 },
            BucketConfig { name: "7d".to_string(), duration_secs: 604800, refresh_interval_secs: 3600 },
        ];
        let mut mgr = TimeBucketManager::new("sortAt".to_string(), configs);
        // Slots 1-3 within 24h, slots 4-6 within 7d but outside 24h, slot 7 outside 7d.
        let data: Vec<(u32, u64)> = vec![
            (1, now - 3600),
            (2, now - 7200),
            (3, now - 43200),
            (4, now - 90000),
            (5, now - 200000),
            (6, now - 500000),
            (7, now - 700000), // outside 7d
        ];
        mgr.rebuild_bucket("24h", data.iter().copied(), now);
        mgr.rebuild_bucket("7d", data.iter().copied(), now);
        mgr
    }

    #[test]
    fn test_snap_gt_exact_24h() {
        let now: u64 = 1_700_000_000;
        let mgr = make_manager_with_data(now);
        let mut managers = HashMap::new();
        managers.insert("sortAt".to_string(), &mgr);
        let ctx = make_ctx(&managers, now);

        // Gt("sortAt", now - 86400) → exactly 24h duration → snap to "24h"
        let ts = (now - 86400) as i64;
        let clauses = vec![FilterClause::Gt("sortAt".to_string(), Value::Integer(ts))];
        let snapped = snap_range_clauses(&clauses, &ctx);

        assert_eq!(snapped.len(), 1);
        match &snapped[0] {
            FilterClause::BucketBitmap { field, bucket_name, bitmap } => {
                assert_eq!(field, "sortAt");
                assert_eq!(bucket_name, "24h");
                assert_eq!(bitmap.len(), 3); // slots 1, 2, 3
            }
            other => panic!("expected BucketBitmap, got {:?}", other),
        }
    }

    #[test]
    fn test_snap_gte_7d_within_tolerance() {
        let now: u64 = 1_700_000_000;
        let mgr = make_manager_with_data(now);
        let mut managers = HashMap::new();
        managers.insert("sortAt".to_string(), &mgr);
        let ctx = make_ctx(&managers, now);

        // Gte("sortAt", now - 590000) → duration=590000, 7d=604800, delta=14800, threshold=60480 → snap
        let ts = (now - 590000) as i64;
        let clauses = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts))];
        let snapped = snap_range_clauses(&clauses, &ctx);

        match &snapped[0] {
            FilterClause::BucketBitmap { bucket_name, .. } => {
                assert_eq!(bucket_name, "7d");
            }
            other => panic!("expected BucketBitmap, got {:?}", other),
        }
    }

    #[test]
    fn test_snap_nearest_outside_tolerance() {
        let now: u64 = 1_700_000_000;
        let mgr = make_manager_with_data(now);
        let mut managers = HashMap::new();
        managers.insert("sortAt".to_string(), &mgr);
        let ctx = make_ctx(&managers, now);

        // Duration = 200000s. Neither 24h (86400) nor 7d (604800) is within 10% tolerance.
        // With always_snap=true, should snap to 7d (smallest bucket >= 200000s).
        let ts = (now - 200000) as i64;
        let clauses = vec![FilterClause::Gt("sortAt".to_string(), Value::Integer(ts))];
        let snapped = snap_range_clauses(&clauses, &ctx);

        assert!(matches!(&snapped[0], FilterClause::BucketBitmap { bucket_name, .. } if bucket_name == "7d"));
    }

    #[test]
    fn test_snap_nearest_with_always_snap_false() {
        let now: u64 = 1_700_000_000;
        let mgr = make_manager_with_data(now);
        let mut managers = HashMap::new();
        managers.insert("sortAt".to_string(), &mgr);
        let ctx = BucketSnapContext {
            managers: &managers,
            now_secs: now,
            tolerance_pct: 0.10,
            always_snap: false,
        };

        // Duration = 200000s, outside tolerance, always_snap=false → empty bitmap
        let ts = (now - 200000) as i64;
        let clauses = vec![FilterClause::Gt("sortAt".to_string(), Value::Integer(ts))];
        let snapped = snap_range_clauses(&clauses, &ctx);

        assert!(matches!(&snapped[0], FilterClause::BucketBitmap { bitmap, .. } if bitmap.is_empty()));
    }

    #[test]
    fn test_no_snap_unknown_field() {
        let now: u64 = 1_700_000_000;
        let managers: HashMap<String, &TimeBucketManager> = HashMap::new();
        let ctx = make_ctx(&managers, now);

        let ts = (now - 86400) as i64;
        let clauses = vec![FilterClause::Gt("nsfwLevel".to_string(), Value::Integer(ts))];
        let snapped = snap_range_clauses(&clauses, &ctx);

        assert!(matches!(&snapped[0], FilterClause::Gt(_, _)));
    }

    #[test]
    fn test_lt_lte_not_snapped() {
        let now: u64 = 1_700_000_000;
        let mgr = make_manager_with_data(now);
        let mut managers = HashMap::new();
        managers.insert("sortAt".to_string(), &mgr);
        let ctx = make_ctx(&managers, now);

        let ts = (now - 86400) as i64;
        let lt = FilterClause::Lt("sortAt".to_string(), Value::Integer(ts));
        let lte = FilterClause::Lte("sortAt".to_string(), Value::Integer(ts));
        let snapped = snap_range_clauses(&[lt, lte], &ctx);

        assert!(matches!(&snapped[0], FilterClause::Lt(_, _)));
        assert!(matches!(&snapped[1], FilterClause::Lte(_, _)));
    }

    #[test]
    fn test_snap_inside_and_clause() {
        let now: u64 = 1_700_000_000;
        let mgr = make_manager_with_data(now);
        let mut managers = HashMap::new();
        managers.insert("sortAt".to_string(), &mgr);
        let ctx = make_ctx(&managers, now);

        let ts = (now - 86400) as i64;
        let clauses = vec![FilterClause::And(vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
            FilterClause::Gt("sortAt".to_string(), Value::Integer(ts)),
        ])];
        let snapped = snap_range_clauses(&clauses, &ctx);

        match &snapped[0] {
            FilterClause::And(inner) => {
                assert!(matches!(&inner[0], FilterClause::Eq(_, _)));
                assert!(matches!(&inner[1], FilterClause::BucketBitmap { .. }));
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn test_non_integer_value_not_snapped() {
        let now: u64 = 1_700_000_000;
        let mgr = make_manager_with_data(now);
        let mut managers = HashMap::new();
        managers.insert("sortAt".to_string(), &mgr);
        let ctx = make_ctx(&managers, now);

        // Float value — should not snap
        let clauses = vec![FilterClause::Gt("sortAt".to_string(), Value::Float(1.0))];
        let snapped = snap_range_clauses(&clauses, &ctx);
        assert!(matches!(&snapped[0], FilterClause::Gt(_, _)));
    }

    #[test]
    fn test_filter_clause_construction() {
        let clause = FilterClause::And(vec![
            FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
            FilterClause::Or(vec![
                FilterClause::Eq("type".into(), Value::String("image".into())),
                FilterClause::Eq("type".into(), Value::String("video".into())),
            ]),
        ]);
        match clause {
            FilterClause::And(clauses) => assert_eq!(clauses.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::String("hello".into()).to_string(), "hello");
    }

    #[test]
    fn test_parse_error_display() {
        let err = ParseError::new("bad input");
        assert_eq!(err.to_string(), "bad input");
    }

    #[test]
    fn test_bitdex_query_serde_roundtrip() {
        let query = BitdexQuery {
            filters: vec![FilterClause::Eq("x".into(), Value::Integer(1))],
            sort: Some(SortClause {
                field: "score".into(),
                direction: SortDirection::Desc,
            }),
            limit: 50,
            cursor: Some(CursorPosition {
                sort_value: 100,
                slot_id: 42,
            }),
            offset: None,
            skip_cache: false,
        };
        let json = serde_json::to_string(&query).unwrap();
        let roundtrip: BitdexQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.limit, 50);
        assert_eq!(roundtrip.cursor.unwrap().slot_id, 42);
    }
}
