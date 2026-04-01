//! Property-based tests using proptest.
//!
//! Generates random documents, random mutations, and random queries.
//! After every operation, verifies that a brute-force scan produces the
//! same result as the query engine.

use std::collections::{HashMap, HashSet};

use proptest::prelude::*;

use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::engine::Engine;
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::FieldValue;
use bitdex_v2::query::{FilterClause, SortClause, SortDirection, Value};

fn test_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "category".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                nullable: false,
            },
            FilterFieldConfig {
                name: "tags".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                nullable: false,
            },
            FilterFieldConfig {
                name: "active".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                nullable: false,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "score".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
        }],
        max_page_size: 1000,
        ..Default::default()
    }
}

fn doc(fields: &[(&str, FieldValue)]) -> bitdex_v2::mutation::Document {
    bitdex_v2::mutation::Document {
        fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    }
}

/// Ground truth state that mirrors what the engine should contain.
#[derive(Clone, Debug)]
struct Truth {
    docs: HashMap<u32, TruthDoc>,
}

#[derive(Clone, Debug)]
struct TruthDoc {
    category: i64,
    tags: Vec<i64>,
    active: bool,
    score: u32,
}

impl Truth {
    fn new() -> Self {
        Self {
            docs: HashMap::new(),
        }
    }

    fn put(&mut self, id: u32, doc: TruthDoc) {
        self.docs.insert(id, doc);
    }

    fn delete(&mut self, id: u32) {
        self.docs.remove(&id);
    }

    fn alive_ids(&self) -> HashSet<u32> {
        self.docs.keys().cloned().collect()
    }

    fn query_eq_category(&self, cat: i64) -> HashSet<u32> {
        self.docs
            .iter()
            .filter(|(_, d)| d.category == cat)
            .map(|(&id, _)| id)
            .collect()
    }

    fn query_eq_tag(&self, tag: i64) -> HashSet<u32> {
        self.docs
            .iter()
            .filter(|(_, d)| d.tags.contains(&tag))
            .map(|(&id, _)| id)
            .collect()
    }

    fn query_active(&self, active: bool) -> HashSet<u32> {
        self.docs
            .iter()
            .filter(|(_, d)| d.active == active)
            .map(|(&id, _)| id)
            .collect()
    }

    fn sorted_desc(&self, ids: &HashSet<u32>, limit: usize) -> Vec<u32> {
        let mut entries: Vec<(u32, u32)> = ids
            .iter()
            .filter_map(|&id| self.docs.get(&id).map(|d| (id, d.score)))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        entries.into_iter().take(limit).map(|(id, _)| id).collect()
    }
}

/// Strategy for generating a random document.
fn arb_truth_doc() -> impl Strategy<Value = TruthDoc> {
    (
        1..=10i64,                      // category (1-10)
        prop::collection::vec(1..=50i64, 0..5), // tags (0-4 tags, values 1-50)
        any::<bool>(),                  // active
        0..100_000u32,                  // score
    )
        .prop_map(|(category, tags, active, score)| TruthDoc {
            category,
            tags,
            active,
            score,
        })
}

/// Strategy for generating a batch of documents with IDs.
fn arb_doc_batch(count: usize) -> impl Strategy<Value = Vec<(u32, TruthDoc)>> {
    prop::collection::vec(arb_truth_doc(), count).prop_map(|docs| {
        docs.into_iter()
            .enumerate()
            .map(|(i, d)| ((i + 1) as u32, d))
            .collect()
    })
}

fn put_truth_doc(engine: &mut Engine, id: u32, td: &TruthDoc) {
    let tag_values: Vec<Value> = td.tags.iter().map(|&t| Value::Integer(t)).collect();
    engine
        .put(
            id,
            &doc(&[
                (
                    "category",
                    FieldValue::Single(Value::Integer(td.category)),
                ),
                ("tags", FieldValue::Multi(tag_values)),
                ("active", FieldValue::Single(Value::Bool(td.active))),
                (
                    "score",
                    FieldValue::Single(Value::Integer(td.score as i64)),
                ),
            ]),
        )
        .unwrap();
}

fn engine_query_eq(engine: &Engine, field: &str, val: Value) -> HashSet<u32> {
    engine
        .query(&[FilterClause::Eq(field.to_string(), val)], None, 1000)
        .unwrap()
        .ids
        .iter()
        .map(|&id| id as u32)
        .collect()
}

// ===========================================================================
// Property tests
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    /// After inserting N random documents, every EQ filter query matches brute-force.
    #[test]
    fn prop_insert_then_filter_eq(docs in arb_doc_batch(50)) {
        let mut engine = Engine::new(test_config()).unwrap();
        let mut truth = Truth::new();

        for (id, td) in &docs {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        // Check alive count
        prop_assert_eq!(engine.alive_count(), truth.alive_ids().len() as u64);

        // Spot-check category filter for each category 1-10
        for cat in 1..=10i64 {
            let engine_ids = engine_query_eq(&engine, "category", Value::Integer(cat));
            let truth_ids = truth.query_eq_category(cat);
            prop_assert_eq!(engine_ids, truth_ids,
                "Category EQ mismatch for cat={}", cat);
        }

        // Spot-check active filter
        for active in [true, false] {
            let engine_ids = engine_query_eq(&engine, "active", Value::Bool(active));
            let truth_ids = truth.query_active(active);
            prop_assert_eq!(engine_ids, truth_ids,
                "Active EQ mismatch for active={}", active);
        }
    }

    /// Insert N docs, delete some, verify filters still match brute-force.
    #[test]
    fn prop_insert_delete_filter(
        docs in arb_doc_batch(40),
        delete_indices in prop::collection::vec(0..40usize, 5..15),
    ) {
        let mut engine = Engine::new(test_config()).unwrap();
        let mut truth = Truth::new();

        for (id, td) in &docs {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        // Delete some docs (deduplicate indices)
        let to_delete: HashSet<usize> = delete_indices.into_iter().collect();
        for &idx in &to_delete {
            if idx < docs.len() {
                let id = docs[idx].0;
                if truth.docs.contains_key(&id) {
                    engine.delete(id).unwrap();
                    truth.delete(id);
                }
            }
        }

        // Verify alive count
        prop_assert_eq!(engine.alive_count(), truth.alive_ids().len() as u64);

        // Verify all-alive query
        let all = engine.query(&[], None, 1000).unwrap();
        let all_ids: HashSet<u32> = all.ids.iter().map(|&id| id as u32).collect();
        prop_assert_eq!(all_ids, truth.alive_ids());

        // Verify category filters
        for cat in 1..=10i64 {
            let engine_ids = engine_query_eq(&engine, "category", Value::Integer(cat));
            let truth_ids = truth.query_eq_category(cat);
            prop_assert_eq!(engine_ids, truth_ids,
                "After deletion, category EQ mismatch for cat={}", cat);
        }
    }

    /// Insert, then upsert some docs, verify state is correct.
    #[test]
    fn prop_insert_upsert_filter(
        initial in arb_doc_batch(30),
        updates in arb_doc_batch(30), // same IDs, different values
    ) {
        let mut engine = Engine::new(test_config()).unwrap();
        let mut truth = Truth::new();

        // Initial insert
        for (id, td) in &initial {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        // Upsert with new values
        for (id, td) in &updates {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        // Verify
        prop_assert_eq!(engine.alive_count(), truth.alive_ids().len() as u64);

        for cat in 1..=10i64 {
            let engine_ids = engine_query_eq(&engine, "category", Value::Integer(cat));
            let truth_ids = truth.query_eq_category(cat);
            prop_assert_eq!(engine_ids, truth_ids,
                "After upsert, category EQ mismatch for cat={}", cat);
        }
    }

    /// Sort results must match naive sort for random data.
    #[test]
    fn prop_sort_matches_naive(docs in arb_doc_batch(50)) {
        let mut engine = Engine::new(test_config()).unwrap();
        let mut truth = Truth::new();

        for (id, td) in &docs {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        // Query all active=true docs, sorted by score desc, limit 20
        let result = engine
            .query(
                &[FilterClause::Eq("active".to_string(), Value::Bool(true))],
                Some(&SortClause {
                    field: "score".to_string(),
                    direction: SortDirection::Desc,
                }),
                20,
            )
            .unwrap();

        let engine_order: Vec<u32> = result.ids.iter().map(|&id| id as u32).collect();
        let truth_ids = truth.query_active(true);
        let truth_order = truth.sorted_desc(&truth_ids, 20);

        prop_assert_eq!(engine_order, truth_order,
            "Sort order mismatch for random data");
    }

    /// Cursor pagination must cover all results exactly once.
    #[test]
    fn prop_cursor_completeness(docs in arb_doc_batch(30)) {
        let mut engine = Engine::new(test_config()).unwrap();
        let mut truth = Truth::new();

        for (id, td) in &docs {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        let sort = SortClause {
            field: "score".to_string(),
            direction: SortDirection::Desc,
        };
        let page_size = 7;
        let mut all_ids: Vec<i64> = Vec::new();
        let mut cursor = None;

        loop {
            let query = bitdex_v2::query::BitdexQuery {
                filters: vec![],
                sort: Some(sort.clone()),
                limit: page_size,
                cursor,
                offset: None,
                skip_cache: false,
            };
            let result = engine.execute_query(&query).unwrap();
            if result.ids.is_empty() {
                break;
            }
            all_ids.extend(&result.ids);
            cursor = result.cursor;

            if all_ids.len() > 200 {
                prop_assert!(false, "Pagination loop exceeded expected count");
            }
        }

        let unique: HashSet<i64> = all_ids.iter().cloned().collect();
        prop_assert_eq!(all_ids.len(), unique.len(), "Duplicates in pagination");
        prop_assert_eq!(all_ids.len(), truth.alive_ids().len(), "Missing docs in pagination");
    }

    /// Mixed mutations: insert, delete, re-insert. State should always be consistent.
    #[test]
    fn prop_mixed_mutations_consistency(
        initial in arb_doc_batch(20),
        replacements in arb_doc_batch(20),
        delete_mask in prop::collection::vec(any::<bool>(), 20),
    ) {
        let mut engine = Engine::new(test_config()).unwrap();
        let mut truth = Truth::new();

        // Phase 1: insert all
        for (id, td) in &initial {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        // Phase 2: delete based on mask
        for (i, &should_delete) in delete_mask.iter().enumerate() {
            if should_delete && i < initial.len() {
                let id = initial[i].0;
                if truth.docs.contains_key(&id) {
                    engine.delete(id).unwrap();
                    truth.delete(id);
                }
            }
        }

        // Phase 3: re-insert (upsert) with new values
        for (id, td) in &replacements {
            put_truth_doc(&mut engine, *id, td);
            truth.put(*id, td.clone());
        }

        // Verify
        prop_assert_eq!(engine.alive_count(), truth.alive_ids().len() as u64);

        let all = engine.query(&[], None, 1000).unwrap();
        let all_ids: HashSet<u32> = all.ids.iter().map(|&id| id as u32).collect();
        prop_assert_eq!(all_ids, truth.alive_ids());

        for cat in 1..=10i64 {
            let engine_ids = engine_query_eq(&engine, "category", Value::Integer(cat));
            let truth_ids = truth.query_eq_category(cat);
            prop_assert_eq!(engine_ids, truth_ids);
        }
    }
}

// ===========================================================================
// S1.7: VersionedBitmap property tests
// ===========================================================================

use std::sync::Arc;
use bitdex_v2::versioned_bitmap::VersionedBitmap;
use roaring::RoaringBitmap;

/// Operation on a VersionedBitmap: insert or remove a bit.
#[derive(Clone, Debug)]
enum VbOp {
    Insert(u32),
    Remove(u32),
}

fn arb_vb_ops(max_ops: usize) -> impl Strategy<Value = Vec<VbOp>> {
    prop::collection::vec(
        prop::strategy::Union::new(vec![
            (0..1000u32).prop_map(VbOp::Insert).boxed(),
            (0..1000u32).prop_map(VbOp::Remove).boxed(),
        ]),
        0..max_ops,
    )
}

/// Apply ops to a VersionedBitmap and also to a reference HashSet, return both.
fn apply_ops(base_bits: &[u32], ops: &[VbOp]) -> (VersionedBitmap, HashSet<u32>) {
    let mut base = RoaringBitmap::new();
    let mut truth: HashSet<u32> = HashSet::new();
    for &b in base_bits {
        base.insert(b);
        truth.insert(b);
    }
    let mut vb = VersionedBitmap::new(base);
    for op in ops {
        match op {
            VbOp::Insert(bit) => {
                vb.insert(*bit);
                truth.insert(*bit);
            }
            VbOp::Remove(bit) => {
                vb.remove(*bit);
                truth.remove(bit);
            }
        }
    }
    (vb, truth)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// apply_diff(universe) produces the same bitmap as merge() then base().
    #[test]
    fn prop_vb_apply_diff_matches_merge(
        base_bits in prop::collection::vec(0..500u32, 0..50),
        ops in arb_vb_ops(100),
    ) {
        let (vb, _truth) = apply_ops(&base_bits, &ops);

        // Full universe: 0..1000
        let mut universe = RoaringBitmap::new();
        for i in 0..1000u32 {
            universe.insert(i);
        }
        let via_apply = vb.apply_diff(&universe);

        let mut merged = vb.clone();
        merged.merge();
        let via_merge = merged.base().as_ref().clone();

        prop_assert_eq!(via_apply, via_merge,
            "apply_diff(universe) != merge+base");
    }

    /// fused() produces the same result as merge() then base().
    #[test]
    fn prop_vb_fused_matches_merge(
        base_bits in prop::collection::vec(0..500u32, 0..50),
        ops in arb_vb_ops(100),
    ) {
        let (vb, _truth) = apply_ops(&base_bits, &ops);

        let via_fused = vb.fused();

        let mut merged = vb.clone();
        merged.merge();
        let via_merge = merged.base().as_ref().clone();

        prop_assert_eq!(via_fused, via_merge,
            "fused() != merge+base");
    }

    /// merge() is idempotent: merging twice yields the same result.
    #[test]
    fn prop_vb_merge_idempotent(
        base_bits in prop::collection::vec(0..500u32, 0..50),
        ops in arb_vb_ops(100),
    ) {
        let (vb, _truth) = apply_ops(&base_bits, &ops);

        let mut once = vb.clone();
        once.merge();
        let after_once = once.base().as_ref().clone();

        once.merge(); // second merge
        let after_twice = once.base().as_ref().clone();

        prop_assert_eq!(after_once, after_twice, "merge is not idempotent");
    }

    /// contains(bit) matches apply_diff({bit}) for every bit in range.
    #[test]
    fn prop_vb_contains_matches_apply(
        base_bits in prop::collection::vec(0..200u32, 0..30),
        ops in arb_vb_ops(50),
    ) {
        let (vb, truth) = apply_ops(&base_bits, &ops);

        for bit in 0..200u32 {
            let via_contains = vb.contains(bit);
            let expected = truth.contains(&bit);
            prop_assert_eq!(via_contains, expected,
                "contains({}) = {}, expected {}", bit, via_contains, expected);
        }
    }

    /// Last-write-wins: for the same bit, the final operation determines state.
    #[test]
    fn prop_vb_last_write_wins(
        base_bits in prop::collection::vec(0..100u32, 0..20),
        bit in 0..100u32,
        final_op in any::<bool>(), // true = insert, false = remove
    ) {
        let (mut vb, _truth) = apply_ops(&base_bits, &[]);

        // Random sequence of ops on the same bit
        vb.insert(bit);
        vb.remove(bit);
        vb.insert(bit);
        vb.remove(bit);

        // Final operation
        if final_op {
            vb.insert(bit);
            prop_assert!(vb.contains(bit), "bit should be present after final insert");
        } else {
            vb.remove(bit);
            prop_assert!(!vb.contains(bit), "bit should be absent after final remove");
        }
    }
}
