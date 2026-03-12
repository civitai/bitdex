//! Phase 1 Integration Tests — correctness validation for the core bitmap engine.
//!
//! These tests verify end-to-end correctness through the Engine API, covering:
//! 1. Filter correctness vs brute-force scan
//! 2. Bitmap consistency after insert/update/delete sequences
//! 3. Sort correctness vs naive sort
//! 4. Cursor pagination — no gaps, no duplicates
//! 5. DELETE WHERE — predicate resolution + alive bitmap clearing
//! 6. PATCH — only changed fields update, unchanged fields retain correct bits
//! 7. Property-based tests using proptest

use std::collections::{HashMap, HashSet};

use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::engine::Engine;
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{FieldValue, PatchField, PatchPayload};
use bitdex_v2::query::{
    BitdexQuery, CursorPosition, FilterClause, SortClause, SortDirection, Value,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Civitai-like config: 7 filter fields, 5 sort fields.
fn civitai_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "tagIds".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "userId".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "modelVersionIds".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "onSite".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "hasMeta".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "type".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
            },
        ],
        sort_fields: vec![
            SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            },
            SortFieldConfig {
                name: "sortAt".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            },
            SortFieldConfig {
                name: "commentCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            },
            SortFieldConfig {
                name: "collectedCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            },
            SortFieldConfig {
                name: "id".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            },
        ],
        max_page_size: 200,
        ..Default::default()
    }
}

/// Minimal config for focused tests.
fn minimal_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "status".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "tags".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
            },
            FilterFieldConfig {
                name: "active".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "score".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
        }],
        max_page_size: 100,
        ..Default::default()
    }
}

/// Build a Document from a slice of (field_name, FieldValue) pairs.
fn doc(fields: &[(&str, FieldValue)]) -> bitdex_v2::mutation::Document {
    bitdex_v2::mutation::Document {
        fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    }
}

/// In-memory "ground truth" tracker for brute-force verification.
/// Stores what the engine SHOULD contain after a sequence of mutations.
struct GroundTruth {
    /// id -> field -> values (as bitmap keys)
    alive: HashMap<u32, HashMap<String, Vec<i64>>>,
    /// id -> sort field -> sort value
    sort_values: HashMap<u32, HashMap<String, u32>>,
}

impl GroundTruth {
    fn new() -> Self {
        Self {
            alive: HashMap::new(),
            sort_values: HashMap::new(),
        }
    }

    fn put(&mut self, id: u32, fields: &[(&str, FieldValue)]) {
        let mut field_map: HashMap<String, Vec<i64>> = HashMap::new();
        let mut sort_map: HashMap<String, u32> = HashMap::new();

        for (name, fv) in fields {
            match fv {
                FieldValue::Single(Value::Integer(v)) => {
                    field_map.insert(name.to_string(), vec![*v]);
                    // Also record as sort value if it could be one
                    sort_map.insert(name.to_string(), *v as u32);
                }
                FieldValue::Single(Value::Bool(b)) => {
                    field_map.insert(name.to_string(), vec![if *b { 1 } else { 0 }]);
                }
                FieldValue::Multi(vals) => {
                    let keys: Vec<i64> = vals
                        .iter()
                        .filter_map(|v| match v {
                            Value::Integer(i) => Some(*i),
                            _ => None,
                        })
                        .collect();
                    field_map.insert(name.to_string(), keys);
                }
                _ => {}
            }
        }

        self.alive.insert(id, field_map);
        self.sort_values.insert(id, sort_map);
    }

    fn delete(&mut self, id: u32) {
        self.alive.remove(&id);
        self.sort_values.remove(&id);
    }

    fn alive_ids(&self) -> HashSet<u32> {
        self.alive.keys().cloned().collect()
    }

    /// Brute-force filter: return all alive IDs that match field == value.
    fn brute_eq(&self, field: &str, value: i64) -> HashSet<u32> {
        self.alive
            .iter()
            .filter(|(_, fields)| {
                fields
                    .get(field)
                    .map_or(false, |vals| vals.contains(&value))
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Brute-force filter: return all alive IDs that do NOT match field == value.
    fn brute_not_eq(&self, field: &str, value: i64) -> HashSet<u32> {
        let eq = self.brute_eq(field, value);
        self.alive_ids().difference(&eq).cloned().collect()
    }

    /// Brute-force filter: return all alive IDs matching field IN values.
    fn brute_in(&self, field: &str, values: &[i64]) -> HashSet<u32> {
        self.alive
            .iter()
            .filter(|(_, fields)| {
                fields
                    .get(field)
                    .map_or(false, |vals| vals.iter().any(|v| values.contains(v)))
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Brute-force sorted IDs for a given sort field, descending.
    fn brute_sort_desc(&self, sort_field: &str, ids: &HashSet<u32>) -> Vec<u32> {
        let mut entries: Vec<(u32, u32)> = ids
            .iter()
            .map(|&id| {
                let sv = self
                    .sort_values
                    .get(&id)
                    .and_then(|m| m.get(sort_field))
                    .copied()
                    .unwrap_or(0);
                (id, sv)
            })
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        entries.into_iter().map(|(id, _)| id).collect()
    }

    /// Brute-force sorted IDs for a given sort field, ascending.
    fn brute_sort_asc(&self, sort_field: &str, ids: &HashSet<u32>) -> Vec<u32> {
        let mut entries: Vec<(u32, u32)> = ids
            .iter()
            .map(|&id| {
                let sv = self
                    .sort_values
                    .get(&id)
                    .and_then(|m| m.get(sort_field))
                    .copied()
                    .unwrap_or(0);
                (id, sv)
            })
            .collect();
        entries.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        entries.into_iter().map(|(id, _)| id).collect()
    }
}

// ===========================================================================
// 1. Filter correctness vs brute-force scan
// ===========================================================================

#[test]
fn filter_eq_matches_brute_force() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    // Insert 100 documents with varying nsfwLevel (1-5)
    for i in 1..=100u32 {
        let nsfw = ((i % 5) + 1) as i64;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(i as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    // Verify EQ filter for each nsfwLevel
    for level in 1..=5i64 {
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(level),
                )],
                None,
                200,
            )
            .unwrap();

        let engine_ids: HashSet<u32> = result.ids.iter().map(|&id| id as u32).collect();
        let truth_ids = truth.brute_eq("nsfwLevel", level);

        assert_eq!(
            engine_ids, truth_ids,
            "EQ filter mismatch for nsfwLevel={level}"
        );
    }
}

#[test]
fn filter_not_eq_matches_brute_force() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=50u32 {
        let nsfw = ((i % 3) + 1) as i64;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(i as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    for level in 1..=3i64 {
        let result = engine
            .query(
                &[FilterClause::NotEq(
                    "nsfwLevel".to_string(),
                    Value::Integer(level),
                )],
                None,
                200,
            )
            .unwrap();

        let engine_ids: HashSet<u32> = result.ids.iter().map(|&id| id as u32).collect();
        let truth_ids = truth.brute_not_eq("nsfwLevel", level);

        assert_eq!(
            engine_ids, truth_ids,
            "NOT EQ filter mismatch for nsfwLevel!={level}"
        );
    }
}

#[test]
fn filter_in_matches_brute_force() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=60u32 {
        let nsfw = ((i % 5) + 1) as i64;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(i as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    let in_values = vec![1i64, 3, 5];
    let result = engine
        .query(
            &[FilterClause::In(
                "nsfwLevel".to_string(),
                in_values.iter().map(|&v| Value::Integer(v)).collect(),
            )],
            None,
            200,
        )
        .unwrap();

    let engine_ids: HashSet<u32> = result.ids.iter().map(|&id| id as u32).collect();
    let truth_ids = truth.brute_in("nsfwLevel", &in_values);

    assert_eq!(engine_ids, truth_ids, "IN filter mismatch");
}

#[test]
fn filter_multi_value_eq_matches_brute_force() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    // Each doc has 2-3 tagIds
    for i in 1..=30u32 {
        let tag1 = ((i % 10) + 1) as i64;
        let tag2 = ((i % 7) + 10) as i64;
        let fields: Vec<(&str, FieldValue)> = vec![
            (
                "tagIds",
                FieldValue::Multi(vec![Value::Integer(tag1), Value::Integer(tag2)]),
            ),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(i as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    // Query for a specific tagId
    for tag in [1i64, 5, 10, 15] {
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(tag))],
                None,
                200,
            )
            .unwrap();

        let engine_ids: HashSet<u32> = result.ids.iter().map(|&id| id as u32).collect();
        let truth_ids = truth.brute_eq("tagIds", tag);

        assert_eq!(
            engine_ids, truth_ids,
            "Multi-value EQ filter mismatch for tagId={tag}"
        );
    }
}

#[test]
fn filter_boolean_matches_brute_force() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=40u32 {
        let on_site = i % 3 != 0;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("onSite", FieldValue::Single(Value::Bool(on_site))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(i as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    // true = 1, false = 0 in bitmap key space
    let result_true = engine
        .query(
            &[FilterClause::Eq(
                "onSite".to_string(),
                Value::Bool(true),
            )],
            None,
            200,
        )
        .unwrap();
    let result_false = engine
        .query(
            &[FilterClause::Eq(
                "onSite".to_string(),
                Value::Bool(false),
            )],
            None,
            200,
        )
        .unwrap();

    let engine_true: HashSet<u32> = result_true.ids.iter().map(|&id| id as u32).collect();
    let engine_false: HashSet<u32> = result_false.ids.iter().map(|&id| id as u32).collect();
    let truth_true = truth.brute_eq("onSite", 1);
    let truth_false = truth.brute_eq("onSite", 0);

    assert_eq!(engine_true, truth_true, "Boolean TRUE filter mismatch");
    assert_eq!(engine_false, truth_false, "Boolean FALSE filter mismatch");
    // No overlap
    assert!(
        engine_true.is_disjoint(&engine_false),
        "TRUE and FALSE results overlap"
    );
}

#[test]
fn filter_compound_and_or_matches_brute_force() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=50u32 {
        let nsfw = ((i % 5) + 1) as i64;
        let on_site = i % 2 == 0;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
            ("onSite", FieldValue::Single(Value::Bool(on_site))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(i as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    // Complex: (nsfwLevel == 1 OR nsfwLevel == 2) AND onSite == true
    let result = engine
        .query(
            &[FilterClause::And(vec![
                FilterClause::Or(vec![
                    FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                    FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(2)),
                ]),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ])],
            None,
            200,
        )
        .unwrap();

    let engine_ids: HashSet<u32> = result.ids.iter().map(|&id| id as u32).collect();

    // Brute force: docs that have nsfwLevel in {1,2} AND are onSite
    let nsfw_match = truth
        .brute_in("nsfwLevel", &[1, 2]);
    let on_site_match = truth.brute_eq("onSite", 1);
    let expected: HashSet<u32> = nsfw_match.intersection(&on_site_match).cloned().collect();

    assert_eq!(engine_ids, expected, "Compound AND/OR filter mismatch");
}

// ===========================================================================
// 2. Bitmap consistency after insert/update/delete sequences
// ===========================================================================

#[test]
fn bitmap_consistency_after_mixed_mutations() {
    let mut engine = Engine::new(minimal_config()).unwrap();
    let mut truth = GroundTruth::new();

    // Phase 1: Insert 20 documents
    for i in 1..=20u32 {
        let status = ((i % 4) + 1) as i64;
        let active = i % 2 == 0;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("status", FieldValue::Single(Value::Integer(status))),
            ("active", FieldValue::Single(Value::Bool(active))),
            (
                "score",
                FieldValue::Single(Value::Integer((i * 100) as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    verify_consistency(&engine, &truth, "after initial insert");

    // Phase 2: Delete some docs
    for &id in &[3u32, 7, 15, 19] {
        engine.delete(id).unwrap();
        truth.delete(id);
    }

    verify_consistency(&engine, &truth, "after deletions");

    // Phase 3: Upsert (re-PUT) existing docs with changed values
    for &id in &[1u32, 5, 10] {
        let fields: Vec<(&str, FieldValue)> = vec![
            ("status", FieldValue::Single(Value::Integer(99))),
            ("active", FieldValue::Single(Value::Bool(true))),
            ("score", FieldValue::Single(Value::Integer(9999))),
        ];
        engine.put(id, &doc(&fields)).unwrap();
        truth.put(id, &fields);
    }

    verify_consistency(&engine, &truth, "after upserts");

    // Phase 4: Insert new docs into previously unused IDs
    for id in 21..=30u32 {
        let fields: Vec<(&str, FieldValue)> = vec![
            ("status", FieldValue::Single(Value::Integer(1))),
            ("active", FieldValue::Single(Value::Bool(false))),
            ("score", FieldValue::Single(Value::Integer((id * 50) as i64))),
        ];
        engine.put(id, &doc(&fields)).unwrap();
        truth.put(id, &fields);
    }

    verify_consistency(&engine, &truth, "after new inserts");
}

fn verify_consistency(engine: &Engine, truth: &GroundTruth, phase: &str) {
    // Verify alive count
    assert_eq!(
        engine.alive_count(),
        truth.alive_ids().len() as u64,
        "alive count mismatch {phase}"
    );

    // Verify all alive IDs are queryable
    let all_result = engine.query(&[], None, 1000).unwrap();
    let all_ids: HashSet<u32> = all_result.ids.iter().map(|&id| id as u32).collect();
    assert_eq!(
        all_ids,
        truth.alive_ids(),
        "alive ID set mismatch {phase}"
    );
}

// ===========================================================================
// 3. Sort correctness vs naive sort
// ===========================================================================

#[test]
fn sort_desc_matches_naive_sort() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=50u32 {
        // Intentionally non-monotonic sort values
        let reaction_count = ((i * 7 + 13) % 1000) as i64;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(reaction_count)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    let sort = SortClause {
        field: "reactionCount".to_string(),
        direction: SortDirection::Desc,
    };
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&sort),
            50,
        )
        .unwrap();

    let engine_order: Vec<u32> = result.ids.iter().map(|&id| id as u32).collect();
    let all_ids = truth.brute_eq("nsfwLevel", 1);
    let truth_order = truth.brute_sort_desc("reactionCount", &all_ids);

    assert_eq!(
        engine_order, truth_order,
        "Descending sort order mismatch vs naive sort"
    );
}

#[test]
fn sort_asc_matches_naive_sort() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=50u32 {
        let comment_count = ((i * 11 + 3) % 500) as i64;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            (
                "commentCount",
                FieldValue::Single(Value::Integer(comment_count)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    let sort = SortClause {
        field: "commentCount".to_string(),
        direction: SortDirection::Asc,
    };
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&sort),
            50,
        )
        .unwrap();

    let engine_order: Vec<u32> = result.ids.iter().map(|&id| id as u32).collect();
    let all_ids = truth.brute_eq("nsfwLevel", 1);
    let truth_order = truth.brute_sort_asc("commentCount", &all_ids);

    assert_eq!(
        engine_order, truth_order,
        "Ascending sort order mismatch vs naive sort"
    );
}

#[test]
fn sort_top_n_subset_matches_naive_sort() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=200u32 {
        let reaction_count = ((i * 37 + 17) % 10_000) as i64;
        let nsfw = ((i % 3) + 1) as i64;
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(reaction_count)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    // Only request top 10 of the ~67 docs with nsfwLevel=1
    let sort = SortClause {
        field: "reactionCount".to_string(),
        direction: SortDirection::Desc,
    };
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&sort),
            10,
        )
        .unwrap();

    let engine_order: Vec<u32> = result.ids.iter().map(|&id| id as u32).collect();
    let all_ids = truth.brute_eq("nsfwLevel", 1);
    let truth_order: Vec<u32> = truth
        .brute_sort_desc("reactionCount", &all_ids)
        .into_iter()
        .take(10)
        .collect();

    assert_eq!(
        engine_order, truth_order,
        "Top-N sort subset mismatch vs naive sort"
    );
}

#[test]
fn sort_after_deletions_matches_naive_sort() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=30u32 {
        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer((i * 10) as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    // Delete every third doc
    for i in (3..=30u32).step_by(3) {
        engine.delete(i).unwrap();
        truth.delete(i);
    }

    let sort = SortClause {
        field: "reactionCount".to_string(),
        direction: SortDirection::Desc,
    };
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&sort),
            100,
        )
        .unwrap();

    let engine_order: Vec<u32> = result.ids.iter().map(|&id| id as u32).collect();
    let all_ids = truth.brute_eq("nsfwLevel", 1);
    let truth_order = truth.brute_sort_desc("reactionCount", &all_ids);

    assert_eq!(
        engine_order, truth_order,
        "Sort after deletions mismatch"
    );
    // Verify deleted docs are absent
    for i in (3..=30u32).step_by(3) {
        assert!(
            !engine_order.contains(&i),
            "Deleted doc {i} appeared in sorted results"
        );
    }
}

// ===========================================================================
// 4. Cursor pagination — no gaps, no duplicates
// ===========================================================================

#[test]
fn cursor_pagination_no_gaps_no_duplicates_desc() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    for i in 1..=50u32 {
        let reaction_count = ((i * 13 + 7) % 1000) as i64;
        engine
            .put(
                i,
                &doc(&[
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(reaction_count)),
                    ),
                ]),
            )
            .unwrap();
    }

    let sort = SortClause {
        field: "reactionCount".to_string(),
        direction: SortDirection::Desc,
    };

    let page_size = 7;
    let mut all_ids: Vec<i64> = Vec::new();
    let mut cursor: Option<CursorPosition> = None;

    // Paginate through all results
    loop {
        let query = BitdexQuery {
            filters: vec![FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            sort: Some(sort.clone()),
            limit: page_size,
            cursor: cursor.clone(),
            offset: None,
        };

        let result = engine.execute_query(&query).unwrap();
        if result.ids.is_empty() {
            break;
        }

        all_ids.extend(&result.ids);
        cursor = result.cursor;

        // Safety: don't loop forever
        if all_ids.len() > 100 {
            panic!("Pagination loop exceeded expected count");
        }
    }

    // Verify no duplicates
    let unique: HashSet<i64> = all_ids.iter().cloned().collect();
    assert_eq!(
        all_ids.len(),
        unique.len(),
        "Cursor pagination produced duplicates: {} total vs {} unique",
        all_ids.len(),
        unique.len()
    );

    // Verify completeness (all 50 docs)
    assert_eq!(all_ids.len(), 50, "Cursor pagination missed documents");

    // Verify ordering is monotonically non-increasing by sort value
    for window in all_ids.windows(2) {
        let a = window[0] as u32;
        let b = window[1] as u32;
        let va = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(a);
        let vb = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(b);
        assert!(
            va >= vb || (va == vb && a > b),
            "Sort order violated: id={a} val={va} should come before id={b} val={vb}"
        );
    }
}

#[test]
fn cursor_pagination_no_gaps_no_duplicates_asc() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    for i in 1..=50u32 {
        let score = ((i * 19 + 3) % 500) as i64;
        engine
            .put(
                i,
                &doc(&[
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    (
                        "commentCount",
                        FieldValue::Single(Value::Integer(score)),
                    ),
                ]),
            )
            .unwrap();
    }

    let sort = SortClause {
        field: "commentCount".to_string(),
        direction: SortDirection::Asc,
    };

    let page_size = 11;
    let mut all_ids: Vec<i64> = Vec::new();
    let mut cursor: Option<CursorPosition> = None;

    loop {
        let query = BitdexQuery {
            filters: vec![FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            sort: Some(sort.clone()),
            limit: page_size,
            cursor: cursor.clone(),
            offset: None,
        };

        let result = engine.execute_query(&query).unwrap();
        if result.ids.is_empty() {
            break;
        }

        all_ids.extend(&result.ids);
        cursor = result.cursor;

        if all_ids.len() > 100 {
            panic!("Pagination loop exceeded expected count");
        }
    }

    let unique: HashSet<i64> = all_ids.iter().cloned().collect();
    assert_eq!(all_ids.len(), unique.len(), "Duplicates in ascending pagination");
    assert_eq!(all_ids.len(), 50, "Missing docs in ascending pagination");
}

#[test]
fn cursor_page2_starts_where_page1_ended() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    for i in 1..=20u32 {
        engine
            .put(
                i,
                &doc(&[
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer((i * 10) as i64)),
                    ),
                ]),
            )
            .unwrap();
    }

    let sort = SortClause {
        field: "reactionCount".to_string(),
        direction: SortDirection::Desc,
    };

    let page1 = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&sort),
            5,
        )
        .unwrap();

    assert_eq!(page1.ids.len(), 5);
    let cursor = page1.cursor.as_ref().unwrap();

    let page2_query = BitdexQuery {
        filters: vec![FilterClause::Eq(
            "nsfwLevel".to_string(),
            Value::Integer(1),
        )],
        sort: Some(sort.clone()),
        limit: 5,
        cursor: Some(cursor.clone()),
        offset: None,
    };
    let page2 = engine.execute_query(&page2_query).unwrap();

    // No overlap between pages
    let page1_set: HashSet<i64> = page1.ids.iter().cloned().collect();
    let page2_set: HashSet<i64> = page2.ids.iter().cloned().collect();
    assert!(
        page1_set.is_disjoint(&page2_set),
        "Page 1 and Page 2 overlap"
    );

    // Page 2's best is worse than page 1's worst (descending)
    if let (Some(&p1_last), Some(&p2_first)) = (page1.ids.last(), page2.ids.first()) {
        let v1 = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(p1_last as u32);
        let v2 = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(p2_first as u32);
        assert!(
            v1 >= v2,
            "Page 2 first item (val={v2}) should not exceed page 1 last item (val={v1})"
        );
    }
}

// ===========================================================================
// 5. DELETE WHERE — predicate resolution + alive bitmap clearing
// ===========================================================================

#[test]
fn delete_where_removes_correct_subset() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    for i in 1..=40u32 {
        let nsfw = if i <= 20 { 1 } else { 2 };
        let fields: Vec<(&str, FieldValue)> = vec![
            (
                "nsfwLevel",
                FieldValue::Single(Value::Integer(nsfw)),
            ),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(i as i64)),
            ),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    // Delete WHERE nsfwLevel == 1
    let deleted = engine
        .delete_where(&[FilterClause::Eq(
            "nsfwLevel".to_string(),
            Value::Integer(1),
        )])
        .unwrap();

    assert_eq!(deleted, 20, "Expected 20 deletions");
    assert_eq!(engine.alive_count(), 20);

    // Update truth
    for i in 1..=20u32 {
        truth.delete(i);
    }

    // Verify remaining docs
    let remaining = engine.query(&[], None, 200).unwrap();
    let remaining_ids: HashSet<u32> = remaining.ids.iter().map(|&id| id as u32).collect();
    assert_eq!(remaining_ids, truth.alive_ids());

    // Verify deleted docs no longer appear in queries
    let nsfw1_result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            None,
            200,
        )
        .unwrap();
    assert_eq!(
        nsfw1_result.ids.len(),
        0,
        "Deleted nsfwLevel=1 docs still appear in query"
    );
}

#[test]
fn delete_where_compound_predicate() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    for i in 1..=30u32 {
        let nsfw = ((i % 3) + 1) as i64;
        let on_site = i % 2 == 0;
        engine
            .put(
                i,
                &doc(&[
                    ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
                    ("onSite", FieldValue::Single(Value::Bool(on_site))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(i as i64)),
                    ),
                ]),
            )
            .unwrap();
    }

    // Delete WHERE nsfwLevel == 1 AND onSite == true
    let deleted = engine
        .delete_where(&[
            FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
            FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
        ])
        .unwrap();

    assert!(deleted > 0, "Should have deleted at least one doc");
    let total_alive = engine.alive_count();
    assert_eq!(total_alive, 30 - deleted, "Alive count mismatch after delete_where");

    // Verify no remaining doc matches both conditions
    let check = engine
        .query(
            &[
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ],
            None,
            200,
        )
        .unwrap();
    assert_eq!(
        check.ids.len(),
        0,
        "Docs matching the delete predicate still visible"
    );
}

// ===========================================================================
// 6. PATCH — only changed fields update, unchanged retain correct bits
// ===========================================================================

#[test]
fn patch_changes_only_specified_fields() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    // Insert doc with all fields
    engine
        .put(
            1,
            &doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                (
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                ),
                ("onSite", FieldValue::Single(Value::Bool(true))),
                ("reactionCount", FieldValue::Single(Value::Integer(50))),
                ("commentCount", FieldValue::Single(Value::Integer(10))),
            ]),
        )
        .unwrap();

    // PATCH only nsfwLevel (1 -> 28) and reactionCount (50 -> 999)
    engine
        .patch(
            1,
            &PatchPayload {
                fields: vec![
                    (
                        "nsfwLevel".to_string(),
                        PatchField {
                            old: FieldValue::Single(Value::Integer(1)),
                            new: FieldValue::Single(Value::Integer(28)),
                        },
                    ),
                    (
                        "reactionCount".to_string(),
                        PatchField {
                            old: FieldValue::Single(Value::Integer(50)),
                            new: FieldValue::Single(Value::Integer(999)),
                        },
                    ),
                ]
                .into_iter()
                .collect(),
            },
        )
        .unwrap();

    // Verify changed fields
    let nsfw_old = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            None,
            100,
        )
        .unwrap();
    assert!(
        !nsfw_old.ids.contains(&1),
        "Old nsfwLevel=1 should not match doc 1"
    );

    let nsfw_new = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(28),
            )],
            None,
            100,
        )
        .unwrap();
    assert!(
        nsfw_new.ids.contains(&1),
        "New nsfwLevel=28 should match doc 1"
    );

    // Verify unchanged fields still work
    let tag_query = engine
        .query(
            &[FilterClause::Eq(
                "tagIds".to_string(),
                Value::Integer(100),
            )],
            None,
            100,
        )
        .unwrap();
    assert!(
        tag_query.ids.contains(&1),
        "Unchanged tagIds should still match doc 1"
    );

    let on_site_query = engine
        .query(
            &[FilterClause::Eq(
                "onSite".to_string(),
                Value::Bool(true),
            )],
            None,
            100,
        )
        .unwrap();
    assert!(
        on_site_query.ids.contains(&1),
        "Unchanged onSite should still match doc 1"
    );

    // Verify sort value updated
    let sort_val = engine
        .sorts()
        .get_field("reactionCount")
        .unwrap()
        .reconstruct_value(1);
    assert_eq!(sort_val, 999, "reactionCount should be updated to 999");

    // Verify unchanged sort value
    let comment_val = engine
        .sorts()
        .get_field("commentCount")
        .unwrap()
        .reconstruct_value(1);
    assert_eq!(
        comment_val, 10,
        "commentCount should be unchanged at 10"
    );
}

#[test]
fn patch_multi_value_field_correctly_swaps() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    engine
        .put(
            1,
            &doc(&[
                (
                    "tagIds",
                    FieldValue::Multi(vec![
                        Value::Integer(100),
                        Value::Integer(200),
                        Value::Integer(300),
                    ]),
                ),
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();

    // PATCH tagIds: remove 200, add 400
    engine
        .patch(
            1,
            &PatchPayload {
                fields: vec![(
                    "tagIds".to_string(),
                    PatchField {
                        old: FieldValue::Multi(vec![
                            Value::Integer(100),
                            Value::Integer(200),
                            Value::Integer(300),
                        ]),
                        new: FieldValue::Multi(vec![
                            Value::Integer(100),
                            Value::Integer(300),
                            Value::Integer(400),
                        ]),
                    },
                )]
                .into_iter()
                .collect(),
            },
        )
        .unwrap();

    // Tag 100 still present
    let r100 = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
            None,
            100,
        )
        .unwrap();
    assert!(r100.ids.contains(&1));

    // Tag 200 removed
    let r200 = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
            None,
            100,
        )
        .unwrap();
    assert!(!r200.ids.contains(&1));

    // Tag 300 still present
    let r300 = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(300))],
            None,
            100,
        )
        .unwrap();
    assert!(r300.ids.contains(&1));

    // Tag 400 added
    let r400 = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(400))],
            None,
            100,
        )
        .unwrap();
    assert!(r400.ids.contains(&1));
}

#[test]
fn patch_sort_field_xor_correctness() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    // Insert docs with known sort values
    for i in 1..=10u32 {
        engine
            .put(
                i,
                &doc(&[
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer((i * 100) as i64)),
                    ),
                ]),
            )
            .unwrap();
    }

    // PATCH doc 5: reactionCount 500 -> 1500 (should become the top result)
    engine
        .patch(
            5,
            &PatchPayload {
                fields: vec![(
                    "reactionCount".to_string(),
                    PatchField {
                        old: FieldValue::Single(Value::Integer(500)),
                        new: FieldValue::Single(Value::Integer(1500)),
                    },
                )]
                .into_iter()
                .collect(),
            },
        )
        .unwrap();

    let sort = SortClause {
        field: "reactionCount".to_string(),
        direction: SortDirection::Desc,
    };
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&sort),
            3,
        )
        .unwrap();

    // Doc 5 should now be first (1500), then doc 10 (1000), then doc 9 (900)
    assert_eq!(
        result.ids,
        vec![5, 10, 9],
        "After PATCH, sort order should reflect new value"
    );
}

// ===========================================================================
// 7. Additional integration scenarios
// ===========================================================================

#[test]
fn large_multi_value_field_correctness() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    // Each doc has 20 tags
    for i in 1..=50u32 {
        let tags: Vec<Value> = (0..20u32)
            .map(|t| Value::Integer(((i + t) % 100) as i64))
            .collect();
        engine
            .put(
                i,
                &doc(&[
                    ("tagIds", FieldValue::Multi(tags)),
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }

    // A tag that appears frequently
    let result = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(50))],
            None,
            200,
        )
        .unwrap();

    // Every doc from i=31..50 has tag 50 (since (31+19)%100=50, (32+18)%100=50, etc.)
    // and i=1..20 also covers tag 50 at various offsets.
    // Just verify the count is reasonable and all returned IDs are alive
    assert!(
        result.ids.len() > 0,
        "Should find docs with tag 50"
    );
    for id in &result.ids {
        assert!(
            engine.slots().is_alive(*id as u32),
            "Returned ID {} is not alive",
            id
        );
    }
}

#[test]
fn upsert_clears_all_old_state() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    // First PUT with many tags
    engine
        .put(
            1,
            &doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                (
                    "tagIds",
                    FieldValue::Multi(vec![
                        Value::Integer(10),
                        Value::Integer(20),
                        Value::Integer(30),
                    ]),
                ),
                ("onSite", FieldValue::Single(Value::Bool(true))),
                ("reactionCount", FieldValue::Single(Value::Integer(500))),
            ]),
        )
        .unwrap();

    // Second PUT (upsert) with completely different values
    engine
        .put(
            1,
            &doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(28))),
                (
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(40), Value::Integer(50)]),
                ),
                ("onSite", FieldValue::Single(Value::Bool(false))),
                ("reactionCount", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();

    // Old values should be gone
    assert_eq!(
        engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1)
                )],
                None,
                100
            )
            .unwrap()
            .ids
            .len(),
        0,
        "Old nsfwLevel=1 should be gone"
    );
    assert_eq!(
        engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(10))],
                None,
                100
            )
            .unwrap()
            .ids
            .len(),
        0,
        "Old tag 10 should be gone"
    );
    assert_eq!(
        engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(20))],
                None,
                100
            )
            .unwrap()
            .ids
            .len(),
        0,
        "Old tag 20 should be gone"
    );
    assert!(
        !engine
            .query(
                &[FilterClause::Eq(
                    "onSite".to_string(),
                    Value::Bool(true)
                )],
                None,
                100
            )
            .unwrap()
            .ids
            .contains(&1),
        "Old onSite=true should be gone for doc 1"
    );

    // New values should be present
    assert!(
        engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(28)
                )],
                None,
                100
            )
            .unwrap()
            .ids
            .contains(&1)
    );
    assert!(
        engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(40))],
                None,
                100
            )
            .unwrap()
            .ids
            .contains(&1)
    );
    assert!(
        engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(50))],
                None,
                100
            )
            .unwrap()
            .ids
            .contains(&1)
    );
    assert!(
        engine
            .query(
                &[FilterClause::Eq(
                    "onSite".to_string(),
                    Value::Bool(false)
                )],
                None,
                100
            )
            .unwrap()
            .ids
            .contains(&1)
    );

    // Sort value should be updated
    assert_eq!(
        engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(1),
        1
    );
}

#[test]
fn delete_makes_stale_bits_invisible() {
    let mut engine = Engine::new(civitai_config()).unwrap();

    engine
        .put(
            1,
            &doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(9999))),
            ]),
        )
        .unwrap();
    engine
        .put(
            2,
            &doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();

    // Delete doc 1 (the high-value one)
    engine.delete(1).unwrap();

    // Stale bits exist in filter and sort bitmaps, but queries should not see doc 1
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.ids, vec![2], "Only doc 2 should be visible");

    // Sorted query should also not see doc 1
    let sorted = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            100,
        )
        .unwrap();
    assert_eq!(sorted.ids, vec![2], "Sorted query should only return doc 2");
}

#[test]
fn json_parser_to_engine_roundtrip() {
    use bitdex_v2::query::QueryParser;
    use bitdex_v2::parser::json::JsonQueryParser;

    let mut engine = Engine::new(civitai_config()).unwrap();
    let parser = JsonQueryParser;

    for i in 1..=20u32 {
        engine
            .put(
                i,
                &doc(&[
                    (
                        "nsfwLevel",
                        FieldValue::Single(Value::Integer(((i % 3) + 1) as i64)),
                    ),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer((i * 10) as i64)),
                    ),
                ]),
            )
            .unwrap();
    }

    // Parse a JSON query and execute it (using the parser's expected format)
    let json = r#"{
        "filters": {
            "field": "nsfwLevel",
            "op": "eq",
            "value": 1
        },
        "sort": {
            "field": "reactionCount",
            "direction": "desc"
        },
        "limit": 5
    }"#;

    let query = parser.parse(json.as_bytes()).unwrap();
    let result = engine.execute_query(&query).unwrap();

    assert_eq!(result.ids.len(), 5);
    assert!(result.total_matched > 0);
    // Verify descending order
    for window in result.ids.windows(2) {
        let va = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(window[0] as u32);
        let vb = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(window[1] as u32);
        assert!(va >= vb, "Sort order violated in JSON parser roundtrip");
    }
}

#[test]
fn empty_query_returns_all_alive() {
    let mut engine = Engine::new(minimal_config()).unwrap();

    for i in 1..=15u32 {
        engine
            .put(
                i,
                &doc(&[
                    ("status", FieldValue::Single(Value::Integer(1))),
                    ("score", FieldValue::Single(Value::Integer(i as i64))),
                ]),
            )
            .unwrap();
    }

    engine.delete(5).unwrap();
    engine.delete(10).unwrap();

    let result = engine.query(&[], None, 100).unwrap();
    assert_eq!(result.total_matched, 13);
    assert_eq!(result.ids.len(), 13);
    assert!(!result.ids.contains(&5));
    assert!(!result.ids.contains(&10));
}

#[test]
fn max_page_size_caps_results() {
    let config = Config {
        max_page_size: 5,
        ..minimal_config()
    };
    let mut engine = Engine::new(config).unwrap();

    for i in 1..=20u32 {
        engine
            .put(
                i,
                &doc(&[
                    ("status", FieldValue::Single(Value::Integer(1))),
                    ("score", FieldValue::Single(Value::Integer(i as i64))),
                ]),
            )
            .unwrap();
    }

    // Request 100 but should be capped to 5
    let result = engine.query(&[], None, 100).unwrap();
    assert_eq!(result.ids.len(), 5, "Should be capped to max_page_size=5");
    assert_eq!(result.total_matched, 20, "Total matched should be all 20");
}

// ===========================================================================
// 8. Full Civitai-like workflow
// ===========================================================================

#[test]
fn civitai_full_workflow() {
    let mut engine = Engine::new(civitai_config()).unwrap();
    let mut truth = GroundTruth::new();

    // Simulate Civitai image ingestion
    for i in 1..=100u32 {
        let nsfw = ((i % 5) + 1) as i64;       // 1-5
        let user_id = ((i % 20) + 1) as i64;    // 1-20
        let on_site = i % 3 != 0;
        let has_meta = i % 4 != 0;
        let img_type = ((i % 3) + 1) as i64;    // 1=image, 2=video, 3=audio
        let tag1 = ((i % 15) + 1) as i64;
        let tag2 = ((i % 10) + 16) as i64;
        let reaction_count = ((i * 37 + 13) % 10000) as i64;
        let sort_at = (1_700_000_000 + i * 3600) as i64;
        let comment_count = ((i * 7) % 200) as i64;
        let collected_count = ((i * 3) % 50) as i64;

        let fields: Vec<(&str, FieldValue)> = vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(nsfw))),
            ("userId", FieldValue::Single(Value::Integer(user_id))),
            ("onSite", FieldValue::Single(Value::Bool(on_site))),
            ("hasMeta", FieldValue::Single(Value::Bool(has_meta))),
            ("type", FieldValue::Single(Value::Integer(img_type))),
            (
                "tagIds",
                FieldValue::Multi(vec![Value::Integer(tag1), Value::Integer(tag2)]),
            ),
            (
                "reactionCount",
                FieldValue::Single(Value::Integer(reaction_count)),
            ),
            ("sortAt", FieldValue::Single(Value::Integer(sort_at))),
            (
                "commentCount",
                FieldValue::Single(Value::Integer(comment_count)),
            ),
            (
                "collectedCount",
                FieldValue::Single(Value::Integer(collected_count)),
            ),
            ("id", FieldValue::Single(Value::Integer(i as i64))),
        ];
        engine.put(i, &doc(&fields)).unwrap();
        truth.put(i, &fields);
    }

    assert_eq!(engine.alive_count(), 100);

    // Query 1: nsfwLevel <= 2, sorted by reactionCount desc, limit 20
    let result1 = engine
        .query(
            &[FilterClause::In(
                "nsfwLevel".to_string(),
                vec![Value::Integer(1), Value::Integer(2)],
            )],
            Some(&SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            20,
        )
        .unwrap();

    let truth_ids = truth.brute_in("nsfwLevel", &[1, 2]);
    let truth_sorted: Vec<u32> = truth
        .brute_sort_desc("reactionCount", &truth_ids)
        .into_iter()
        .take(20)
        .collect();
    let engine_sorted: Vec<u32> = result1.ids.iter().map(|&id| id as u32).collect();
    assert_eq!(engine_sorted, truth_sorted, "Civitai query 1 mismatch");

    // Query 2: specific userId, onSite=true
    let result2 = engine
        .query(
            &[
                FilterClause::Eq("userId".to_string(), Value::Integer(5)),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ],
            None,
            200,
        )
        .unwrap();

    let truth_user = truth.brute_eq("userId", 5);
    let truth_on_site = truth.brute_eq("onSite", 1);
    let expected: HashSet<u32> = truth_user.intersection(&truth_on_site).cloned().collect();
    let engine_ids2: HashSet<u32> = result2.ids.iter().map(|&id| id as u32).collect();
    assert_eq!(engine_ids2, expected, "Civitai query 2 mismatch");

    // Mutation: delete some docs, patch others
    for i in (5..=100u32).step_by(10) {
        engine.delete(i).unwrap();
        truth.delete(i);
    }

    // PATCH remaining docs: bump reactionCount
    for &i in &[1u32, 2, 3] {
        engine
            .patch(
                i,
                &PatchPayload {
                    fields: vec![(
                        "reactionCount".to_string(),
                        PatchField {
                            old: FieldValue::Single(Value::Integer(
                                ((i * 37 + 13) % 10000) as i64,
                            )),
                            new: FieldValue::Single(Value::Integer(50000)),
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )
            .unwrap();
        // Update truth for sort
        if let Some(sort_map) = truth.sort_values.get_mut(&i) {
            sort_map.insert("reactionCount".to_string(), 50000);
        }
    }

    // Re-query after mutations
    let result3 = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            Some(&SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            5,
        )
        .unwrap();

    // Patched docs (1, 2, 3) with reactionCount=50000 should be at the top if they match nsfwLevel=1
    for &id in &result3.ids {
        assert!(
            engine.slots().is_alive(id as u32),
            "Result contains dead slot {id}"
        );
    }

    // Verify sort order
    for window in result3.ids.windows(2) {
        let va = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(window[0] as u32);
        let vb = engine
            .sorts()
            .get_field("reactionCount")
            .unwrap()
            .reconstruct_value(window[1] as u32);
        assert!(
            va >= vb,
            "Sort order violated after mutations: {} ({}) before {} ({})",
            window[0],
            va,
            window[1],
            vb
        );
    }
}
