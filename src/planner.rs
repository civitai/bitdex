use std::collections::HashMap;

use crate::filter::FilterIndex;
use crate::query::{FilterClause, Value};
use crate::slot::SlotAllocator;

/// Threshold below which we skip bitmap sort traversal and use a simple in-memory sort.
/// For very small result sets, extracting IDs and sorting is faster than walking 32 bit layers.
const SORT_FIRST_THRESHOLD: u64 = 1000;

/// Optional context for resolving string values to bitmap keys during cardinality estimation.
pub struct PlannerContext<'a> {
    /// String maps: field_name → (string_value → integer_key).
    pub string_maps: Option<&'a HashMap<String, HashMap<String, i64>>>,
    /// Live dictionaries: field_name → FieldDictionary for LCS fields.
    pub dictionaries: Option<&'a HashMap<String, crate::dictionary::FieldDictionary>>,
}

/// Estimates the cardinality of a filter clause using bitmap metadata.
/// Returns the estimated number of matching documents.
fn estimate_cardinality(clause: &FilterClause, filters: &FilterIndex, alive_count: u64, ctx: Option<&PlannerContext<'_>>) -> u64 {
    match clause {
        FilterClause::Eq(field, value) => {
            if let Some(ff) = filters.get_field(field) {
                if let Some(key) = resolve_value_key(field, value, ctx) {
                    return ff.cardinality(key);
                }
            }
            // Unknown field or unconvertible value: assume worst case
            alive_count
        }

        FilterClause::NotEq(field, value) => {
            if let Some(ff) = filters.get_field(field) {
                if let Some(key) = resolve_value_key(field, value, ctx) {
                    return alive_count.saturating_sub(ff.cardinality(key));
                }
            }
            alive_count
        }

        FilterClause::In(field, values) => {
            if let Some(ff) = filters.get_field(field) {
                let mut total = 0u64;
                for v in values {
                    if let Some(key) = resolve_value_key(field, v, ctx) {
                        total += ff.cardinality(key);
                    }
                }
                // Union can't exceed alive_count; this is an upper bound (may overcount overlaps)
                return total.min(alive_count);
            }
            alive_count
        }

        FilterClause::NotIn(field, values) => {
            if let Some(ff) = filters.get_field(field) {
                let mut total = 0u64;
                for v in values {
                    if let Some(key) = resolve_value_key(field, v, ctx) {
                        total += ff.cardinality(key);
                    }
                }
                return alive_count.saturating_sub(total.min(alive_count));
            }
            alive_count
        }

        FilterClause::Not(inner) => {
            let inner_card = estimate_cardinality(inner, filters, alive_count, ctx);
            alive_count.saturating_sub(inner_card)
        }

        FilterClause::And(clauses) => {
            // Estimate as the minimum of child cardinalities (upper bound on intersection)
            clauses
                .iter()
                .map(|c| estimate_cardinality(c, filters, alive_count, ctx))
                .min()
                .unwrap_or(0)
        }

        FilterClause::Or(clauses) => {
            // Estimate as the sum of child cardinalities, capped at alive_count
            let total: u64 = clauses
                .iter()
                .map(|c| estimate_cardinality(c, filters, alive_count, ctx))
                .sum();
            total.min(alive_count)
        }

        // Range filters: we don't have exact stats, estimate as half alive
        FilterClause::Gt(_, _)
        | FilterClause::Lt(_, _)
        | FilterClause::Gte(_, _)
        | FilterClause::Lte(_, _) => alive_count / 2,

        // Pre-computed bucket bitmap: use the actual bitmap length as cardinality.
        FilterClause::BucketBitmap { bitmap, .. } => bitmap.len(),
    }
}

/// Resolve a Value to a bitmap key, using string maps/dictionaries for String values.
fn resolve_value_key(field: &str, val: &Value, ctx: Option<&PlannerContext<'_>>) -> Option<u64> {
    // Try direct conversion first (Integer, Bool)
    if let Some(key) = value_to_bitmap_key(val) {
        return Some(key);
    }
    // For String values, consult string maps and dictionaries
    if let Value::String(s) = val {
        if let Some(ctx) = ctx {
            // Try string maps first
            if let Some(maps) = ctx.string_maps {
                if let Some(field_map) = maps.get(field) {
                    let lookup = s.to_lowercase();
                    if let Some(&v) = field_map.get(&lookup) {
                        return Some(v as u64);
                    }
                }
            }
            // Fallback: live dictionaries
            if let Some(dicts) = ctx.dictionaries {
                if let Some(dict) = dicts.get(field) {
                    return dict.get(s).map(|v| v as u64);
                }
            }
        }
    }
    None
}

/// Convert a Value to a u64 bitmap key for cardinality lookups.
fn value_to_bitmap_key(val: &Value) -> Option<u64> {
    match val {
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        Value::Integer(v) => Some(*v as u64),
        Value::Float(_) | Value::String(_) => None,
    }
}

/// A planned query with filter clauses reordered for optimal execution.
pub struct QueryPlan {
    /// Filter clauses reordered by estimated cardinality (smallest first).
    pub ordered_clauses: Vec<FilterClause>,
    /// Whether to use simple in-memory sort instead of bitmap sort traversal.
    pub use_simple_sort: bool,
    /// Estimated result size after all filters.
    pub estimated_result_size: u64,
}

/// Plans query execution by reordering filter clauses by cardinality.
///
/// The no-sort decision is handled by the executor via `sort: Option<&SortClause>`.
/// When sort is `None`, results are returned in descending slot order (newest first).
pub fn plan_query(
    clauses: &[FilterClause],
    filters: &FilterIndex,
    slots: &SlotAllocator,
) -> QueryPlan {
    plan_query_with_context(clauses, filters, slots, None)
}

pub fn plan_query_with_context(
    clauses: &[FilterClause],
    filters: &FilterIndex,
    slots: &SlotAllocator,
    ctx: Option<&PlannerContext<'_>>,
) -> QueryPlan {
    let alive_count = slots.alive_count();

    if clauses.is_empty() {
        return QueryPlan {
            ordered_clauses: Vec::new(),
            use_simple_sort: alive_count < SORT_FIRST_THRESHOLD,
            estimated_result_size: alive_count,
        };
    }

    // Estimate cardinality for each clause and sort by it (ascending)
    let mut clause_estimates: Vec<(FilterClause, u64)> = clauses
        .iter()
        .map(|c| {
            let est = estimate_cardinality(c, filters, alive_count, ctx);
            (c.clone(), est)
        })
        .collect();

    clause_estimates.sort_by_key(|(_, est)| *est);

    // Estimated result size is the smallest clause estimate (since top-level is implicit AND)
    let estimated_result_size = clause_estimates
        .first()
        .map(|(_, est)| *est)
        .unwrap_or(alive_count);

    let ordered_clauses: Vec<FilterClause> = clause_estimates
        .into_iter()
        .map(|(c, _)| c)
        .collect();

    QueryPlan {
        ordered_clauses,
        use_simple_sort: estimated_result_size < SORT_FIRST_THRESHOLD,
        estimated_result_size,
    }
}

/// Reorder clauses within an And node by cardinality.
/// Returns a new And clause with children sorted by estimated cardinality.
pub fn optimize_and_clause(
    clauses: &[FilterClause],
    filters: &FilterIndex,
    alive_count: u64,
) -> Vec<FilterClause> {
    let mut clause_estimates: Vec<(FilterClause, u64)> = clauses
        .iter()
        .map(|c| {
            let est = estimate_cardinality(c, filters, alive_count, None);
            (c.clone(), est)
        })
        .collect();

    clause_estimates.sort_by_key(|(_, est)| *est);
    clause_estimates.into_iter().map(|(c, _)| c).collect()
}

/// Check if a NOT clause has a small negated set, making andnot the better strategy.
/// Returns true if the inner set is estimated to be small relative to the alive count.
pub fn should_use_andnot(clause: &FilterClause, filters: &FilterIndex, alive_count: u64) -> bool {
    match clause {
        FilterClause::Not(inner) => {
            let inner_card = estimate_cardinality(inner, filters, alive_count, None);
            inner_card < alive_count / 10
        }
        FilterClause::NotEq(field, value) => {
            if let Some(ff) = filters.get_field(field) {
                if let Some(key) = value_to_bitmap_key(value) {
                    let negated_card = ff.cardinality(key);
                    return negated_card < alive_count / 10;
                }
            }
            false
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::{Document, FieldValue, MutationEngine};
    use crate::sort::SortIndex;

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
                    name: "userId".to_string(),
                    field_type: FilterFieldType::SingleValue,
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
            }],
            max_page_size: 100,
            ..Default::default()
        }
    }

    struct TestHarness {
        slots: SlotAllocator,
        filters: FilterIndex,
        sorts: SortIndex,
        config: Config,
        docstore: crate::docstore::DocStore,
    }

    impl TestHarness {
        fn new() -> Self {
            let config = test_config();
            let slots = SlotAllocator::new();
            let mut filters = FilterIndex::new();
            let mut sorts = SortIndex::new();
            let docstore = crate::docstore::DocStore::open_temp().unwrap();

            for fc in &config.filter_fields {
                filters.add_field(fc.clone());
            }
            for sc in &config.sort_fields {
                sorts.add_field(sc.clone());
            }

            Self { slots, filters, sorts, config, docstore }
        }

        fn put(&mut self, id: u32, doc: &Document) {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
                &mut self.docstore,
            );
            engine.put(id, doc).unwrap();
            // Eager merge: mirror Engine::put() behavior
            for (_name, field) in self.sorts.fields_mut() {
                field.merge_dirty();
            }
            for (_name, field) in self.filters.fields_mut() {
                field.merge_dirty();
            }
            self.slots.merge_alive();
        }
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
    fn test_plan_empty_clauses() {
        let h = TestHarness::new();
        let plan = plan_query(&[], &h.filters, &h.slots);
        assert!(plan.ordered_clauses.is_empty());
        assert!(plan.use_simple_sort); // 0 docs < threshold
    }

    #[test]
    fn test_plan_orders_by_cardinality() {
        let mut h = TestHarness::new();

        // Insert 100 docs: all have nsfwLevel=1, only 5 have userId=42
        for i in 1..=100u32 {
            let user_id = if i <= 5 { 42 } else { i as i64 + 100 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("userId", FieldValue::Single(Value::Integer(user_id))),
            ]));
        }

        let clauses = vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),  // 100 matches
            FilterClause::Eq("userId".to_string(), Value::Integer(42)),     // 5 matches
        ];

        let plan = plan_query(&clauses, &h.filters, &h.slots);

        // userId should come first (lower cardinality)
        assert_eq!(plan.ordered_clauses.len(), 2);
        match &plan.ordered_clauses[0] {
            FilterClause::Eq(field, _) => assert_eq!(field, "userId"),
            _ => panic!("expected Eq clause for userId first"),
        }
        match &plan.ordered_clauses[1] {
            FilterClause::Eq(field, _) => assert_eq!(field, "nsfwLevel"),
            _ => panic!("expected Eq clause for nsfwLevel second"),
        }
    }

    #[test]
    fn test_plan_small_result_uses_simple_sort() {
        let mut h = TestHarness::new();

        // Insert 50 docs -- well below threshold
        for i in 1..=50u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]));
        }

        let clauses = vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
        ];

        let plan = plan_query(&clauses, &h.filters, &h.slots);
        assert!(plan.use_simple_sort);
        assert_eq!(plan.estimated_result_size, 50);
    }

    #[test]
    fn test_plan_large_result_uses_bitmap_sort() {
        let mut h = TestHarness::new();

        // Insert 2000 docs -- above threshold
        for i in 1..=2000u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]));
        }

        let clauses = vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
        ];

        let plan = plan_query(&clauses, &h.filters, &h.slots);
        assert!(!plan.use_simple_sort);
        assert_eq!(plan.estimated_result_size, 2000);
    }

    #[test]
    fn test_estimate_not_clause() {
        let mut h = TestHarness::new();

        for i in 1..=100u32 {
            let level = if i <= 5 { 28 } else { 1 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
            ]));
        }

        // NOT nsfwLevel=28 should estimate ~95
        let clause = FilterClause::Not(Box::new(
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(28)),
        ));

        let est = estimate_cardinality(&clause, &h.filters, h.slots.alive_count(), None);
        assert_eq!(est, 95);
    }

    #[test]
    fn test_estimate_in_clause() {
        let mut h = TestHarness::new();

        for i in 1..=30u32 {
            let level = (i % 3) as i64;
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
            ]));
        }

        // IN(0, 1) should estimate ~20 (10 for each)
        let clause = FilterClause::In(
            "nsfwLevel".to_string(),
            vec![Value::Integer(0), Value::Integer(1)],
        );

        let est = estimate_cardinality(&clause, &h.filters, h.slots.alive_count(), None);
        assert_eq!(est, 20);
    }

    #[test]
    fn test_estimate_and_clause() {
        let mut h = TestHarness::new();

        // i=1..=10 get userId=42, i=11..=100 get userId=i+1000 (no collision with 42)
        for i in 1..=100u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("userId", FieldValue::Single(Value::Integer(if i <= 10 { 42 } else { i as i64 + 1000 }))),
            ]));
        }

        let clause = FilterClause::And(vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),  // 100
            FilterClause::Eq("userId".to_string(), Value::Integer(42)),     // 10
        ]);

        let est = estimate_cardinality(&clause, &h.filters, h.slots.alive_count(), None);
        assert_eq!(est, 10); // min of children
    }

    #[test]
    fn test_estimate_or_clause() {
        let mut h = TestHarness::new();

        for i in 1..=100u32 {
            let level = if i <= 30 { 1 } else { 2 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
            ]));
        }

        let clause = FilterClause::Or(vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),  // 30
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(2)),  // 70
        ]);

        let est = estimate_cardinality(&clause, &h.filters, h.slots.alive_count(), None);
        assert_eq!(est, 100); // sum, capped at alive_count
    }

    #[test]
    fn test_should_use_andnot_small_set() {
        let mut h = TestHarness::new();

        // 100 docs, only 5 with nsfwLevel=28
        for i in 1..=100u32 {
            let level = if i <= 5 { 28 } else { 1 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
            ]));
        }

        let clause = FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(28));
        assert!(should_use_andnot(&clause, &h.filters, h.slots.alive_count()));
    }

    #[test]
    fn test_should_not_use_andnot_large_set() {
        let mut h = TestHarness::new();

        // 100 docs, 50 with nsfwLevel=1 -- negated set is too large
        for i in 1..=100u32 {
            let level = if i <= 50 { 1 } else { 2 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
            ]));
        }

        let clause = FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(1));
        assert!(!should_use_andnot(&clause, &h.filters, h.slots.alive_count()));
    }

    #[test]
    fn test_optimize_and_clause_ordering() {
        let mut h = TestHarness::new();

        for i in 1..=1000u32 {
            let nsfw = if i <= 900 { 1 } else { 2 };
            let user = if i <= 10 { 42 } else { i as i64 };
            let on_site = i <= 500;
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
                ("userId", FieldValue::Single(Value::Integer(user))),
                ("onSite", FieldValue::Single(Value::Bool(on_site))),
            ]));
        }

        let clauses = vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),  // ~900
            FilterClause::Eq("onSite".to_string(), Value::Bool(true)),      // ~500
            FilterClause::Eq("userId".to_string(), Value::Integer(42)),     // ~10
        ];

        let optimized = optimize_and_clause(&clauses, &h.filters, h.slots.alive_count());

        // Should be: userId (10), onSite (500), nsfwLevel (900)
        match &optimized[0] {
            FilterClause::Eq(field, _) => assert_eq!(field, "userId"),
            _ => panic!("expected userId first"),
        }
        match &optimized[1] {
            FilterClause::Eq(field, _) => assert_eq!(field, "onSite"),
            _ => panic!("expected onSite second"),
        }
        match &optimized[2] {
            FilterClause::Eq(field, _) => assert_eq!(field, "nsfwLevel"),
            _ => panic!("expected nsfwLevel third"),
        }
    }

    #[test]
    fn test_plan_multiple_clause_types() {
        let mut h = TestHarness::new();

        for i in 1..=100u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("tagIds", FieldValue::Multi(vec![Value::Integer(100)])),
                ("userId", FieldValue::Single(Value::Integer(if i <= 3 { 42 } else { i as i64 }))),
            ]));
        }

        let clauses = vec![
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
            FilterClause::Eq("tagIds".to_string(), Value::Integer(100)),
            FilterClause::Eq("userId".to_string(), Value::Integer(42)),
        ];

        let plan = plan_query(&clauses, &h.filters, &h.slots);

        // userId=42 has cardinality 3, should be first
        match &plan.ordered_clauses[0] {
            FilterClause::Eq(field, _) => assert_eq!(field, "userId"),
            _ => panic!("expected userId first"),
        }
        assert!(plan.use_simple_sort); // estimated 3 < 1000
    }

    #[test]
    fn test_plan_query_no_sort_parameter() {
        // plan_query no longer takes a has_sort parameter. The no-sort decision
        // is handled by the executor via sort: Option<&SortClause>.
        let h = TestHarness::new();
        let plan = plan_query(&[], &h.filters, &h.slots);
        assert!(plan.ordered_clauses.is_empty());
        assert!(plan.use_simple_sort); // 0 docs < threshold
    }
}
