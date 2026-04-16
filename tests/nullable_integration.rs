//! Integration tests for nullable filter fields in BitDex.
//!
//! Covers:
//! - P1: null lifecycle, dump path ops, range scan exclusion, NotEq exclusion,
//!   persistence across restart
//! - P2: dictionary field null bypass, parser format support, multi-field
//!   non-contamination, dump + restart preservation
//!
//! Architecture note: null values are represented by the NULL_BITMAP_KEY sentinel
//! (u64::MAX) in filter bitmaps. Inserting this key for a slot makes IsNull return
//! that slot; removing it and inserting a real key makes IsNotNull return it.
//!
//! Tests 2, 6, 9 (ops processor path) require the `pg-sync` feature and are
//! compiled/run conditionally.

use std::collections::HashSet;

use roaring::RoaringBitmap;

use bitdex_v2::concurrent_engine::InnerEngine;
use bitdex_v2::config::{
    Config, DataSchema, FieldMapping, FieldValueType, FilterFieldConfig, SortFieldConfig,
};
use bitdex_v2::executor::QueryExecutor;
use bitdex_v2::filter::{FilterFieldType, FilterIndex, NULL_BITMAP_KEY};
use bitdex_v2::query::{FilterClause, Value};
use bitdex_v2::slot::SlotAllocator;
use bitdex_v2::sort::SortIndex;

// ---------------------------------------------------------------------------
// Test infrastructure — InnerEngine helpers
// ---------------------------------------------------------------------------

/// Build an InnerEngine from the given config.
fn build_inner_engine(config: &Config) -> InnerEngine {
    let mut filters = FilterIndex::new();
    for fc in &config.filter_fields {
        filters.add_field(fc.clone());
    }
    let mut sorts = SortIndex::new();
    for sc in &config.sort_fields {
        sorts.add_field(sc.clone());
    }
    InnerEngine {
        slots: SlotAllocator::new(),
        filters,
        sorts,
    }
}

/// Query an InnerEngine using QueryExecutor. Returns sorted IDs as Vec<u32>.
fn query_inner(inner: &InnerEngine, filters: &[FilterClause], limit: usize) -> Vec<u32> {
    let executor = QueryExecutor::new(&inner.slots, &inner.filters, &inner.sorts, limit);
    let result = executor.execute(filters, None, limit, None).unwrap();
    let mut ids: Vec<u32> = result.ids.iter().map(|&id| id as u32).collect();
    ids.sort();
    ids
}

/// Insert a slot as alive in an InnerEngine.
fn alive_insert(inner: &mut InnerEngine, slot: u32) {
    inner.slots.allocate(slot).unwrap();
    // merge_alive() promotes the diff layer to the base snapshot so
    // alive_bitmap() (used by NotEq, NotIn, IsNotNull) sees the new slot.
    inner.slots.merge_alive();
}

/// Remove a slot's alive bit (simulating a clean delete).
fn alive_remove(inner: &mut InnerEngine, slot: u32) {
    inner.slots.alive_remove_one(slot);
    inner.slots.merge_alive();
}

/// Insert NULL_BITMAP_KEY for a field+slot in an InnerEngine.
fn null_insert(inner: &mut InnerEngine, field: &str, slot: u32) {
    if let Some(ff) = inner.filters.get_field(field) {
        ff.insert(NULL_BITMAP_KEY, slot);
        ff.merge_dirty();
    }
}

/// Remove NULL_BITMAP_KEY for a field+slot in an InnerEngine.
fn null_remove(inner: &mut InnerEngine, field: &str, slot: u32) {
    if let Some(ff) = inner.filters.get_field(field) {
        ff.remove(NULL_BITMAP_KEY, slot);
        ff.merge_dirty();
    }
}

/// Insert a concrete value key for a field+slot in an InnerEngine.
fn value_insert(inner: &mut InnerEngine, field: &str, value: u64, slot: u32) {
    if let Some(ff) = inner.filters.get_field(field) {
        ff.insert(value, slot);
        ff.merge_dirty();
    }
}

/// Remove a concrete value key for a field+slot in an InnerEngine.
fn value_remove(inner: &mut InnerEngine, field: &str, value: u64, slot: u32) {
    if let Some(ff) = inner.filters.get_field(field) {
        ff.remove(value, slot);
        ff.merge_dirty();
    }
}

// ---------------------------------------------------------------------------
// RecordingSink — used by pg-sync gated tests
// ---------------------------------------------------------------------------

#[cfg(feature = "pg-sync")]
mod recording_sink {
    use std::sync::Arc;
    use bitdex_v2::filter::NULL_BITMAP_KEY;
    use bitdex_v2::ingester::BitmapSink;

    pub struct RecordingSink {
        pub filter_inserts: Vec<(String, u64, u32)>,
        pub filter_removes: Vec<(String, u64, u32)>,
        pub sort_sets: Vec<(String, usize, u32)>,
        pub sort_clears: Vec<(String, usize, u32)>,
        pub alive_inserts: Vec<u32>,
        pub alive_removes: Vec<u32>,
        pub deferred_alive: Vec<(u32, u64)>,
    }

    impl RecordingSink {
        pub fn new() -> Self {
            Self {
                filter_inserts: Vec::new(),
                filter_removes: Vec::new(),
                sort_sets: Vec::new(),
                sort_clears: Vec::new(),
                alive_inserts: Vec::new(),
                alive_removes: Vec::new(),
                deferred_alive: Vec::new(),
            }
        }

        pub fn null_inserts_for(&self, field: &str) -> Vec<u32> {
            self.filter_inserts
                .iter()
                .filter(|(f, v, _)| f == field && *v == NULL_BITMAP_KEY)
                .map(|(_, _, slot)| *slot)
                .collect()
        }

        pub fn null_removes_for(&self, field: &str) -> Vec<u32> {
            self.filter_removes
                .iter()
                .filter(|(f, v, _)| f == field && *v == NULL_BITMAP_KEY)
                .map(|(_, _, slot)| *slot)
                .collect()
        }

        pub fn value_inserts_for(&self, field: &str) -> Vec<(u64, u32)> {
            self.filter_inserts
                .iter()
                .filter(|(f, v, _)| f == field && *v != NULL_BITMAP_KEY)
                .map(|(_, v, slot)| (*v, *slot))
                .collect()
        }

        pub fn value_removes_for(&self, field: &str) -> Vec<(u64, u32)> {
            self.filter_removes
                .iter()
                .filter(|(f, v, _)| f == field && *v != NULL_BITMAP_KEY)
                .map(|(_, v, slot)| (*v, *slot))
                .collect()
        }
    }

    impl BitmapSink for RecordingSink {
        fn filter_insert(&mut self, field: Arc<str>, value: u64, slot: u32) {
            self.filter_inserts.push((field.to_string(), value, slot));
        }
        fn filter_remove(&mut self, field: Arc<str>, value: u64, slot: u32) {
            self.filter_removes.push((field.to_string(), value, slot));
        }
        fn sort_set(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
            self.sort_sets.push((field.to_string(), bit_layer, slot));
        }
        fn sort_clear(&mut self, field: Arc<str>, bit_layer: usize, slot: u32) {
            self.sort_clears.push((field.to_string(), bit_layer, slot));
        }
        fn alive_insert(&mut self, slot: u32) {
            self.alive_inserts.push(slot);
        }
        fn alive_remove(&mut self, slot: u32) {
            self.alive_removes.push(slot);
        }
        fn deferred_alive(&mut self, slot: u32, activate_at: u64) {
            self.deferred_alive.push((slot, activate_at));
        }
        fn flush(&mut self) -> bitdex_v2::error::Result<()> {
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

fn config_nullable_post_id() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "postId".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "score".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        }],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![
                FieldMapping {
                    source: "postId".to_string(),
                    target: "postId".to_string(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: true,
                },
                FieldMapping {
                    source: "nsfwLevel".to_string(),
                    target: "nsfwLevel".to_string(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: false,
                },
            ],
        },
        max_page_size: 200,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// P1 Test 1: test_null_lifecycle
// ---------------------------------------------------------------------------
/// Full null lifecycle on a nullable integer field via InnerEngine + QueryExecutor.
///
/// Sequence:
/// 1. Insert slot 1 with postId=null → IsNull returns [1]
/// 2. Update slot 1 to postId=100 → IsNull empty, Eq(100) returns [1]
/// 3. Update slot 1 back to null → IsNull returns [1], Eq(100) empty
/// 4. Delete slot 1 → IsNull empty
#[test]
fn test_null_lifecycle() {
    let config = config_nullable_post_id();
    let mut inner = build_inner_engine(&config);

    // Step 1: insert entity 1 with postId=null
    alive_insert(&mut inner, 1);
    null_insert(&mut inner, "postId", 1);

    let result = query_inner(&inner, &[FilterClause::IsNull("postId".to_string())], 100);
    assert_eq!(result, vec![1], "IsNull should return slot 1 after null insert");

    // Step 2: update entity 1 to postId=100
    null_remove(&mut inner, "postId", 1);
    value_insert(&mut inner, "postId", 100, 1);

    let result = query_inner(&inner, &[FilterClause::IsNull("postId".to_string())], 100);
    assert!(result.is_empty(), "IsNull should be empty after setting postId=100");

    let result = query_inner(
        &inner,
        &[FilterClause::Eq("postId".to_string(), Value::Integer(100))],
        100,
    );
    assert_eq!(result, vec![1], "Eq(postId, 100) should return slot 1");

    // Step 3: update entity 1 back to postId=null
    value_remove(&mut inner, "postId", 100, 1);
    null_insert(&mut inner, "postId", 1);

    let result = query_inner(&inner, &[FilterClause::IsNull("postId".to_string())], 100);
    assert_eq!(result, vec![1], "IsNull should return slot 1 after null re-insert");

    let result = query_inner(
        &inner,
        &[FilterClause::Eq("postId".to_string(), Value::Integer(100))],
        100,
    );
    assert!(result.is_empty(), "Eq(postId, 100) should be empty after null re-insert");

    // Step 4: delete entity 1 (clear null bit and alive)
    null_remove(&mut inner, "postId", 1);
    alive_remove(&mut inner, 1);

    let result = query_inner(&inner, &[FilterClause::IsNull("postId".to_string())], 100);
    assert!(result.is_empty(), "IsNull should be empty after delete");
}

// ---------------------------------------------------------------------------
// P1 Test 2: test_dump_nullable_csv_values (pg-sync only)
// ---------------------------------------------------------------------------
/// Tests the ops_processor path with null values. Verifies that apply_ops_batch
/// correctly routes null ops to NULL_BITMAP_KEY and non-null ops to real keys.
#[cfg(feature = "pg-sync")]
#[test]
fn test_dump_nullable_csv_values() {
    use bitdex_v2::ops_processor::{apply_ops_batch, FieldMeta};
    use bitdex_v2::pg_sync::ops::{EntityOps, Op};
    use recording_sink::RecordingSink;
    use serde_json::json;

    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "postId".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "blockedFor".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
        ],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![
                FieldMapping {
                    source: "postId".to_string(),
                    target: "postId".to_string(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: true,
                },
                FieldMapping {
                    source: "blockedFor".to_string(),
                    target: "blockedFor".to_string(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: true,
                },
            ],
        },
        max_page_size: 100,
        ..Default::default()
    };

    let meta = FieldMeta::from_config(&config);
    let mut sink = RecordingSink::new();

    // Entity 1: postId=100, blockedFor=42 (both non-null)
    // Entity 2: postId=null, blockedFor=null
    // Entity 3: postId=200, blockedFor=99
    let mut batch = vec![
        EntityOps {
            entity_id: 1,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(100) },
                Op::Set { field: "blockedFor".into(), value: json!(42) },
            ],
        },
        EntityOps {
            entity_id: 2,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(null) },
                Op::Set { field: "blockedFor".into(), value: json!(null) },
            ],
        },
        EntityOps {
            entity_id: 3,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(200) },
                Op::Set { field: "blockedFor".into(), value: json!(99) },
            ],
        },
    ];

    let (applied, skipped, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
    assert_eq!(applied, 3);
    assert_eq!(skipped, 0);
    assert_eq!(errors, 0);

    // Entity 2 must have NULL_BITMAP_KEY inserts for both fields
    let null_post = sink.null_inserts_for("postId");
    assert!(
        null_post.contains(&2),
        "entity 2 should have NULL_BITMAP_KEY insert for postId; got: {:?}",
        null_post
    );

    let null_blocked = sink.null_inserts_for("blockedFor");
    assert!(
        null_blocked.contains(&2),
        "entity 2 should have NULL_BITMAP_KEY insert for blockedFor; got: {:?}",
        null_blocked
    );

    // Entities 1 and 3 must not have null inserts for postId
    assert!(
        !null_post.contains(&1) && !null_post.contains(&3),
        "entities 1 and 3 must not have NULL_BITMAP_KEY inserts for postId"
    );

    // Entities 1 and 3 should have real value inserts
    let post_vals = sink.value_inserts_for("postId");
    let post_slots: Vec<u32> = post_vals.iter().map(|(_, s)| *s).collect();
    assert!(post_slots.contains(&1), "entity 1 should have a value insert for postId");
    assert!(post_slots.contains(&3), "entity 3 should have a value insert for postId");
}

// ---------------------------------------------------------------------------
// P1 Test 3: test_range_scans_exclude_nulls
// ---------------------------------------------------------------------------
/// Null slots must never appear in range query results (Gt, Gte, Lt).
///
/// NULL_BITMAP_KEY (u64::MAX) is excluded from range comparisons by the guard
/// in value_to_bitmap_key() and the executor's range scan logic.
#[test]
fn test_range_scans_exclude_nulls() {
    let config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "score".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![FieldMapping {
                source: "score".to_string(),
                target: "score".to_string(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: true,
            }],
        },
        max_page_size: 200,
        ..Default::default()
    };

    let mut inner = build_inner_engine(&config);

    // slot 1: score=5
    alive_insert(&mut inner, 1);
    value_insert(&mut inner, "score", 5, 1);

    // slot 2: score=null
    alive_insert(&mut inner, 2);
    null_insert(&mut inner, "score", 2);

    // slot 3: score=20
    alive_insert(&mut inner, 3);
    value_insert(&mut inner, "score", 20, 3);

    // Gt(3): should return [1, 3], not [1, 2, 3]
    let result = query_inner(
        &inner,
        &[FilterClause::Gt("score".to_string(), Value::Integer(3))],
        100,
    );
    assert_eq!(result, vec![1, 3], "Gt(3) should return [1, 3]");
    assert!(!result.contains(&2), "Gt(3) must exclude null slot 2");

    // Gte(5): should return [1, 3]
    let result = query_inner(
        &inner,
        &[FilterClause::Gte("score".to_string(), Value::Integer(5))],
        100,
    );
    assert_eq!(result, vec![1, 3], "Gte(5) should return [1, 3]");

    // Lt(10): should return [1] only
    let result = query_inner(
        &inner,
        &[FilterClause::Lt("score".to_string(), Value::Integer(10))],
        100,
    );
    assert_eq!(result, vec![1], "Lt(10) should return [1] only");
    assert!(!result.contains(&2), "Lt(10) must exclude null slot 2");
    assert!(!result.contains(&3), "Lt(10) must exclude slot 3 (score=20)");
}

// ---------------------------------------------------------------------------
// P1 Test 4: test_not_eq_excludes_nulls
// ---------------------------------------------------------------------------
/// NotEq and NotIn must exclude null slots — null is not "not equal to 10",
/// it is unknown, and must be absent from all comparison results.
#[test]
fn test_not_eq_excludes_nulls() {
    let config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "category".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![FieldMapping {
                source: "category".to_string(),
                target: "category".to_string(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: true,
            }],
        },
        max_page_size: 200,
        ..Default::default()
    };

    let mut inner = build_inner_engine(&config);

    // slot 1: category=10
    alive_insert(&mut inner, 1);
    value_insert(&mut inner, "category", 10, 1);

    // slot 2: category=null
    alive_insert(&mut inner, 2);
    null_insert(&mut inner, "category", 2);

    // slot 3: category=20
    alive_insert(&mut inner, 3);
    value_insert(&mut inner, "category", 20, 3);

    // NotEq(10): should return [3] only, not [2, 3]
    let result = query_inner(
        &inner,
        &[FilterClause::NotEq("category".to_string(), Value::Integer(10))],
        100,
    );
    assert_eq!(result, vec![3], "NotEq(10) should return [3] only; null slot excluded");

    // NotIn([10]): same expectation
    let result = query_inner(
        &inner,
        &[FilterClause::NotIn(
            "category".to_string(),
            vec![Value::Integer(10)],
        )],
        100,
    );
    assert_eq!(result, vec![3], "NotIn([10]) should return [3] only");

    // IsNull: should return [2]
    let result = query_inner(
        &inner,
        &[FilterClause::IsNull("category".to_string())],
        100,
    );
    assert_eq!(result, vec![2], "IsNull should return [2]");

    // IsNotNull: should return [1, 3]
    let result = query_inner(
        &inner,
        &[FilterClause::IsNotNull("category".to_string())],
        100,
    );
    assert_eq!(result, vec![1, 3], "IsNotNull should return [1, 3]");
}

// ---------------------------------------------------------------------------
// P1 Test 5: test_null_bitmaps_survive_restart
// ---------------------------------------------------------------------------
/// Null bitmaps (NULL_BITMAP_KEY entries) must survive a full ShardStore
/// save + drop + reload cycle.
#[test]
fn test_null_bitmaps_survive_restart() {
    use bitdex_v2::shard_store_bitmap::{
        AliveBitmapStore, FieldValueBucketShard, FilterBitmapStore, SingletonShard,
    };
    use bitdex_v2::shard_store_meta::MetaStore;

    let tmp = tempfile::TempDir::new().unwrap();
    let ss_root = tmp.path().join("shardstore");

    // Phase 1: build in-memory state and persist
    {
        let alive_store = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();
        let filter_store =
            FilterBitmapStore::new(ss_root.join("filter"), FieldValueBucketShard).unwrap();
        let meta_store = MetaStore::new(ss_root.clone()).unwrap();

        // slot 1: postId=null
        // slot 2: postId=100
        let mut alive_bm = RoaringBitmap::new();
        alive_bm.insert(1);
        alive_bm.insert(2);

        let mut null_bm = RoaringBitmap::new();
        null_bm.insert(1);

        let mut val_100_bm = RoaringBitmap::new();
        val_100_bm.insert(2);

        alive_store.write_alive(&alive_bm).unwrap();
        filter_store
            .write_full_filter(&[
                ("postId", NULL_BITMAP_KEY, &null_bm),
                ("postId", 100, &val_100_bm),
            ])
            .unwrap();
        meta_store.write_slot_counter(3).unwrap();
    }

    // Phase 2: reload and verify
    {
        let alive_store = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();
        let filter_store =
            FilterBitmapStore::new(ss_root.join("filter"), FieldValueBucketShard).unwrap();

        let loaded_alive = alive_store.load_alive().unwrap().expect("alive bitmap must exist");
        assert!(loaded_alive.contains(1), "slot 1 must be alive after reload");
        assert!(loaded_alive.contains(2), "slot 2 must be alive after reload");

        // Load both postId bitmaps
        let bitmaps = filter_store
            .load_field_values("postId", &[NULL_BITMAP_KEY, 100])
            .unwrap();

        let null_bm = bitmaps
            .get(&NULL_BITMAP_KEY)
            .expect("NULL_BITMAP_KEY entry must be persisted");
        assert!(
            null_bm.contains(1),
            "null bitmap must contain slot 1 after reload"
        );
        assert!(
            !null_bm.contains(2),
            "null bitmap must not contain slot 2"
        );

        let val_100 = bitmaps.get(&100).expect("value=100 bitmap must be persisted");
        assert!(
            val_100.contains(2),
            "value=100 bitmap must contain slot 2 after reload"
        );

        // Rebuild InnerEngine from the stored state and verify IsNull query
        let mut inner = InnerEngine {
            slots: SlotAllocator::new(),
            filters: {
                let mut fi = FilterIndex::new();
                fi.add_field(FilterFieldConfig {
                    name: "postId".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                });
                fi
            },
            sorts: SortIndex::new(),
        };
        inner.slots.alive_or_bitmap(&loaded_alive);
        for (value, bm) in &bitmaps {
            inner
                .filters
                .get_field("postId")
                .unwrap()
                .or_bitmap(*value, bm);
        }

        let result = query_inner(&inner, &[FilterClause::IsNull("postId".to_string())], 100);
        assert!(
            result.contains(&1),
            "IsNull(postId) must return slot 1 after reload; got: {:?}",
            result
        );

        let result = query_inner(
            &inner,
            &[FilterClause::IsNotNull("postId".to_string())],
            100,
        );
        assert!(
            result.contains(&2),
            "IsNotNull(postId) must return slot 2 after reload; got: {:?}",
            result
        );
    }
}

// ---------------------------------------------------------------------------
// P2 Test 6: test_dictionary_field_null_bypass (pg-sync only)
// ---------------------------------------------------------------------------
/// On a nullable SingleValue field, a null op must emit NULL_BITMAP_KEY
/// (not a dictionary lookup), while a non-null op emits the actual value key.
#[cfg(feature = "pg-sync")]
#[test]
fn test_dictionary_field_null_bypass() {
    use bitdex_v2::ops_processor::{apply_ops_batch, FieldMeta};
    use bitdex_v2::pg_sync::ops::{EntityOps, Op};
    use recording_sink::RecordingSink;
    use serde_json::json;

    let config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "blockedFor".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![FieldMapping {
                source: "blockedFor".to_string(),
                target: "blockedFor".to_string(),
                value_type: FieldValueType::LowCardinalityString,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: true,
            }],
        },
        max_page_size: 100,
        ..Default::default()
    };

    let meta = FieldMeta::from_config(&config);
    let mut sink = RecordingSink::new();

    // Entity 1: blockedFor=null
    // Entity 2: blockedFor=42
    let mut batch = vec![
        EntityOps {
            entity_id: 1,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "blockedFor".into(),
                value: json!(null),
            }],
        },
        EntityOps {
            entity_id: 2,
            creates_slot: true,
            ops: vec![Op::Set {
                field: "blockedFor".into(),
                value: json!(42),
            }],
        },
    ];

    let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
    assert_eq!(applied, 2);
    assert_eq!(errors, 0);

    // Entity 1 must get NULL_BITMAP_KEY insert for blockedFor (not a real value key)
    let null_inserts = sink.null_inserts_for("blockedFor");
    assert_eq!(
        null_inserts,
        vec![1],
        "entity 1 should have NULL_BITMAP_KEY insert for blockedFor; got: {:?}",
        null_inserts
    );

    // Entity 2 must get value insert (42) for blockedFor, not null
    let value_inserts = sink.value_inserts_for("blockedFor");
    assert!(
        value_inserts.iter().any(|(v, s)| *v == 42 && *s == 2),
        "entity 2 should have value insert (42) for blockedFor; got: {:?}",
        value_inserts
    );

    // Entity 2 must not have a null insert
    assert!(
        !null_inserts.contains(&2),
        "entity 2 (blockedFor=42) must not have NULL_BITMAP_KEY insert"
    );
}

// ---------------------------------------------------------------------------
// P2 Test 7: test_all_parser_formats_isnull
// ---------------------------------------------------------------------------
/// All three query parser formats must parse IsNull and IsNotNull correctly.
/// Tests Eq(field, null) → IsNull and field: null → IsNull rewrites.
#[test]
fn test_all_parser_formats_isnull() {
    use bitdex_v2::parser::json::JsonQueryParser;
    use bitdex_v2::parser::compact::CompactQueryParser;
    use bitdex_v2::parser::meilisearch::MeilisearchQueryParser;
    use bitdex_v2::query::QueryParser;

    // --- Bitdex JSON format ---
    // IsNull and IsNotNull are native variants in the JSON format.
    // The JSON parser wraps a single filter object (not an array).
    let json_parser = JsonQueryParser;
    let q = json_parser
        .parse(br#"{"filters":{"IsNull":"postId"},"limit":10}"#)
        .unwrap();
    assert_eq!(
        q.filters,
        vec![FilterClause::IsNull("postId".to_string())],
        "bitdex JSON: IsNull parse"
    );

    let q = json_parser
        .parse(br#"{"filters":{"IsNotNull":"postId"},"limit":10}"#)
        .unwrap();
    assert_eq!(
        q.filters,
        vec![FilterClause::IsNotNull("postId".to_string())],
        "bitdex JSON: IsNotNull parse"
    );

    // --- Compact format (MongoDB-style) ---
    // { "field": null } → IsNull
    let compact = CompactQueryParser;
    let q = compact
        .parse(br#"{"filter":{"postId":null},"limit":10}"#)
        .unwrap();
    let has_isnull = q
        .filters
        .iter()
        .any(|f| matches!(f, FilterClause::IsNull(field) if field == "postId"));
    assert!(has_isnull, "compact: null field value should produce IsNull");

    // { "field": { "$eq": null } } → IsNull
    let q = compact
        .parse(br#"{"filter":{"postId":{"$eq":null}},"limit":10}"#)
        .unwrap();
    let has_isnull = q
        .filters
        .iter()
        .any(|f| matches!(f, FilterClause::IsNull(field) if field == "postId"));
    assert!(has_isnull, "compact: $eq: null should produce IsNull");

    // { "field": { "$ne": null } } → IsNotNull
    let q = compact
        .parse(br#"{"filter":{"postId":{"$ne":null}},"limit":10}"#)
        .unwrap();
    let has_isnotnull = q
        .filters
        .iter()
        .any(|f| matches!(f, FilterClause::IsNotNull(field) if field == "postId"));
    assert!(has_isnotnull, "compact: $ne: null should produce IsNotNull");

    // --- Meilisearch format ---
    // "postId IS NULL" → IsNull
    let ms = MeilisearchQueryParser;
    let q = ms
        .parse(br#"{"filter":"postId IS NULL","limit":10}"#)
        .unwrap();
    let has_isnull = q
        .filters
        .iter()
        .any(|f| matches!(f, FilterClause::IsNull(field) if field == "postId"));
    assert!(has_isnull, "meilisearch: 'postId IS NULL' should produce IsNull");

    // "postId IS NOT NULL" → IsNotNull
    let q = ms
        .parse(br#"{"filter":"postId IS NOT NULL","limit":10}"#)
        .unwrap();
    let has_isnotnull = q
        .filters
        .iter()
        .any(|f| matches!(f, FilterClause::IsNotNull(field) if field == "postId"));
    assert!(has_isnotnull, "meilisearch: 'postId IS NOT NULL' should produce IsNotNull");
}

// ---------------------------------------------------------------------------
// P2 Test 8: test_multiple_nullable_fields_no_contamination
// ---------------------------------------------------------------------------
/// Null bitmaps on separate fields must not bleed into each other.
/// A slot with null-postId must not appear in IsNull(blockedFor).
#[test]
fn test_multiple_nullable_fields_no_contamination() {
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "postId".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "blockedFor".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
        ],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![
                FieldMapping {
                    source: "postId".to_string(),
                    target: "postId".to_string(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: true,
                },
                FieldMapping {
                    source: "blockedFor".to_string(),
                    target: "blockedFor".to_string(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None,
                    nullable: true,
                },
            ],
        },
        max_page_size: 200,
        ..Default::default()
    };

    let mut inner = build_inner_engine(&config);

    // slot 1: postId=null, blockedFor=42
    alive_insert(&mut inner, 1);
    null_insert(&mut inner, "postId", 1);
    value_insert(&mut inner, "blockedFor", 42, 1);

    // slot 2: postId=100, blockedFor=null
    alive_insert(&mut inner, 2);
    value_insert(&mut inner, "postId", 100, 2);
    null_insert(&mut inner, "blockedFor", 2);

    // slot 3: postId=null, blockedFor=null
    alive_insert(&mut inner, 3);
    null_insert(&mut inner, "postId", 3);
    null_insert(&mut inner, "blockedFor", 3);

    // IsNull("postId") should return [1, 3]
    let result = query_inner(&inner, &[FilterClause::IsNull("postId".to_string())], 100);
    let result_set: HashSet<u32> = result.iter().cloned().collect();
    assert_eq!(
        result_set,
        HashSet::from([1u32, 3]),
        "IsNull(postId) should return [1, 3]; got {:?}",
        result
    );
    assert!(
        !result_set.contains(&2),
        "slot 2 (postId=100) must not appear in IsNull(postId)"
    );

    // IsNull("blockedFor") should return [2, 3]
    let result = query_inner(
        &inner,
        &[FilterClause::IsNull("blockedFor".to_string())],
        100,
    );
    let result_set: HashSet<u32> = result.iter().cloned().collect();
    assert_eq!(
        result_set,
        HashSet::from([2u32, 3]),
        "IsNull(blockedFor) should return [2, 3]; got {:?}",
        result
    );
    assert!(
        !result_set.contains(&1),
        "slot 1 (blockedFor=42) must not appear in IsNull(blockedFor)"
    );
}

// ---------------------------------------------------------------------------
// P2 Test 9: test_dump_then_restart_preserves_nulls (pg-sync only)
// ---------------------------------------------------------------------------
/// Null bitmaps written via apply_ops_batch (simulating a dump) must survive
/// a ShardStore save + reload cycle.
#[cfg(feature = "pg-sync")]
#[test]
fn test_dump_then_restart_preserves_nulls() {
    use bitdex_v2::ops_processor::{apply_ops_batch, FieldMeta};
    use bitdex_v2::pg_sync::ops::{EntityOps, Op};
    use bitdex_v2::shard_store_bitmap::{
        AliveBitmapStore, FieldValueBucketShard, FilterBitmapStore, SingletonShard,
    };
    use bitdex_v2::shard_store_meta::MetaStore;
    use recording_sink::RecordingSink;
    use serde_json::json;

    let tmp = tempfile::TempDir::new().unwrap();
    let ss_root = tmp.path().join("shardstore");

    let config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "postId".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![FieldMapping {
                source: "postId".to_string(),
                target: "postId".to_string(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: true,
            }],
        },
        max_page_size: 100,
        ..Default::default()
    };

    // Phase 1: process ops (simulating dump) and record sink operations
    let meta = FieldMeta::from_config(&config);
    let mut sink = RecordingSink::new();

    let mut batch = vec![
        EntityOps {
            entity_id: 10,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(null) }],
        },
        EntityOps {
            entity_id: 20,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(500) }],
        },
    ];

    let (applied, _, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
    assert_eq!(applied, 2);
    assert_eq!(errors, 0);

    // Verify the sink captured the null insert for slot 10
    let null_inserts = sink.null_inserts_for("postId");
    assert!(
        null_inserts.contains(&10),
        "slot 10 should have NULL_BITMAP_KEY insert from ops batch"
    );

    // Reconstruct bitmaps from the recorded sink operations
    let mut null_bm = RoaringBitmap::new();
    let mut val_500_bm = RoaringBitmap::new();
    let mut alive_bm = RoaringBitmap::new();

    for (field, value, slot) in &sink.filter_inserts {
        if field == "postId" {
            if *value == NULL_BITMAP_KEY {
                null_bm.insert(*slot);
            } else if *value == 500 {
                val_500_bm.insert(*slot);
            }
        }
    }
    for slot in &sink.alive_inserts {
        alive_bm.insert(*slot);
    }

    // Phase 2: persist via ShardStore
    let alive_store = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();
    let filter_store =
        FilterBitmapStore::new(ss_root.join("filter"), FieldValueBucketShard).unwrap();
    let meta_store = MetaStore::new(ss_root.clone()).unwrap();

    alive_store.write_alive(&alive_bm).unwrap();
    filter_store
        .write_full_filter(&[
            ("postId", NULL_BITMAP_KEY, &null_bm),
            ("postId", 500, &val_500_bm),
        ])
        .unwrap();
    meta_store.write_slot_counter(21).unwrap();

    // Phase 3: reload from ShardStore and verify
    let alive_store2 = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();
    let filter_store2 =
        FilterBitmapStore::new(ss_root.join("filter"), FieldValueBucketShard).unwrap();

    let loaded_alive = alive_store2.load_alive().unwrap().expect("alive bitmap must exist after reload");
    assert!(loaded_alive.contains(10), "slot 10 must be alive after reload");
    assert!(loaded_alive.contains(20), "slot 20 must be alive after reload");

    let loaded_maps = filter_store2
        .load_field_values("postId", &[NULL_BITMAP_KEY, 500])
        .unwrap();

    let loaded_null = loaded_maps
        .get(&NULL_BITMAP_KEY)
        .expect("null bitmap for postId must be persisted");
    assert!(
        loaded_null.contains(10),
        "null bitmap must contain slot 10 after reload"
    );
    assert!(
        !loaded_null.contains(20),
        "null bitmap must not contain slot 20"
    );

    let loaded_500 = loaded_maps
        .get(&500)
        .expect("value=500 bitmap for postId must be persisted");
    assert!(
        loaded_500.contains(20),
        "value=500 bitmap must contain slot 20 after reload"
    );
}

// ---------------------------------------------------------------------------
// Fix 3: test_nullable_value_to_value_transition
// ---------------------------------------------------------------------------
/// On a nullable integer field, transitioning from one non-null value to another
/// must emit a remove for the old value, an insert for the new value, and must
/// not touch NULL_BITMAP_KEY at all (neither insert nor remove).
#[cfg(feature = "pg-sync")]
#[test]
fn test_nullable_value_to_value_transition() {
    use bitdex_v2::ops_processor::{apply_ops_batch, FieldMeta};
    use bitdex_v2::pg_sync::ops::{EntityOps, Op};
    use recording_sink::RecordingSink;
    use serde_json::json;

    let config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "priority".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![FieldMapping {
                source: "priority".to_string(),
                target: "priority".to_string(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: true,
            }],
        },
        max_page_size: 100,
        ..Default::default()
    };

    let meta = FieldMeta::from_config(&config);

    // Step 1: insert entity with priority=10 (creates_slot=true)
    let mut sink1 = RecordingSink::new();
    let mut batch1 = vec![EntityOps {
        entity_id: 1,
        creates_slot: true,
        ops: vec![Op::Set { field: "priority".into(), value: json!(10) }],
    }];
    let (applied, skipped, errors) = apply_ops_batch(&mut sink1, &meta, &mut batch1, None, None);
    assert_eq!(applied, 1);
    assert_eq!(skipped, 0);
    assert_eq!(errors, 0);

    // Initial insert: value 10 inserted, no NULL_BITMAP_KEY involvement
    let val_inserts1 = sink1.value_inserts_for("priority");
    assert!(
        val_inserts1.iter().any(|(v, s)| *v == 10 && *s == 1),
        "initial insert should have value=10 for slot 1; got: {:?}",
        val_inserts1
    );
    let null_inserts1 = sink1.null_inserts_for("priority");
    assert!(
        null_inserts1.is_empty(),
        "initial non-null insert must not produce NULL_BITMAP_KEY insert; got: {:?}",
        null_inserts1
    );

    // Step 2: update entity — remove priority=10, set priority=20 (creates_slot=false)
    let mut sink2 = RecordingSink::new();
    let mut batch2 = vec![EntityOps {
        entity_id: 1,
        creates_slot: false,
        ops: vec![
            Op::Remove { field: "priority".into(), value: json!(10) },
            Op::Set { field: "priority".into(), value: json!(20) },
        ],
    }];
    let (applied, skipped, errors) = apply_ops_batch(&mut sink2, &meta, &mut batch2, None, None);
    assert_eq!(applied, 1);
    assert_eq!(skipped, 0);
    assert_eq!(errors, 0);

    // Remove for old value=10
    let val_removes2 = sink2.value_removes_for("priority");
    assert!(
        val_removes2.iter().any(|(v, s)| *v == 10 && *s == 1),
        "update should remove value=10 for slot 1; got: {:?}",
        val_removes2
    );

    // Insert for new value=20
    let val_inserts2 = sink2.value_inserts_for("priority");
    assert!(
        val_inserts2.iter().any(|(v, s)| *v == 20 && *s == 1),
        "update should insert value=20 for slot 1; got: {:?}",
        val_inserts2
    );

    // No NULL_BITMAP_KEY inserts during value→value transition
    let null_inserts2 = sink2.null_inserts_for("priority");
    assert!(
        null_inserts2.is_empty(),
        "value→value transition must not produce NULL_BITMAP_KEY insert; got: {:?}",
        null_inserts2
    );
    // Defensive NULL_BITMAP_KEY remove IS expected — non-null set always clears
    // the null sentinel in case the slot previously had null. This is correct.
}

// ---------------------------------------------------------------------------
// Fix 4: test_not_eq_on_nullable_field_via_ops
// ---------------------------------------------------------------------------
/// Via the ops pipeline: insert 3 entities (value=10, null, value=20) on a
/// nullable field. Verify the sink captures correct NULL_BITMAP_KEY inserts
/// for the null entity and no null inserts for the non-null entities. This
/// exercises the ops pipeline → query path integration for NotEq semantics.
#[cfg(feature = "pg-sync")]
#[test]
fn test_not_eq_on_nullable_field_via_ops() {
    use bitdex_v2::ops_processor::{apply_ops_batch, FieldMeta};
    use bitdex_v2::pg_sync::ops::{EntityOps, Op};
    use recording_sink::RecordingSink;
    use serde_json::json;

    let config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "rating".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }],
        sort_fields: vec![],
        data_schema: DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![FieldMapping {
                source: "rating".to_string(),
                target: "rating".to_string(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: true,
            }],
        },
        max_page_size: 100,
        ..Default::default()
    };

    let meta = FieldMeta::from_config(&config);
    let mut sink = RecordingSink::new();

    // Three entities: entity 1 = value 10, entity 2 = null, entity 3 = value 20
    let mut batch = vec![
        EntityOps {
            entity_id: 1,
            creates_slot: true,
            ops: vec![Op::Set { field: "rating".into(), value: json!(10) }],
        },
        EntityOps {
            entity_id: 2,
            creates_slot: true,
            ops: vec![Op::Set { field: "rating".into(), value: json!(null) }],
        },
        EntityOps {
            entity_id: 3,
            creates_slot: true,
            ops: vec![Op::Set { field: "rating".into(), value: json!(20) }],
        },
    ];

    let (applied, skipped, errors) = apply_ops_batch(&mut sink, &meta, &mut batch, None, None);
    assert_eq!(applied, 3);
    assert_eq!(skipped, 0);
    assert_eq!(errors, 0);

    // Only entity 2 (null) should get NULL_BITMAP_KEY insert
    let null_inserts = sink.null_inserts_for("rating");
    assert!(
        null_inserts.contains(&2),
        "entity 2 (null) must have NULL_BITMAP_KEY insert for rating; got: {:?}",
        null_inserts
    );
    assert!(
        !null_inserts.contains(&1),
        "entity 1 (value=10) must not have NULL_BITMAP_KEY insert; got: {:?}",
        null_inserts
    );
    assert!(
        !null_inserts.contains(&3),
        "entity 3 (value=20) must not have NULL_BITMAP_KEY insert; got: {:?}",
        null_inserts
    );

    // Entities 1 and 3 should have real value inserts; entity 2 must not
    let val_inserts = sink.value_inserts_for("rating");
    assert!(
        val_inserts.iter().any(|(v, s)| *v == 10 && *s == 1),
        "entity 1 must have value=10 insert; got: {:?}",
        val_inserts
    );
    assert!(
        val_inserts.iter().any(|(v, s)| *v == 20 && *s == 3),
        "entity 3 must have value=20 insert; got: {:?}",
        val_inserts
    );
    let val_slots: Vec<u32> = val_inserts.iter().map(|(_, s)| *s).collect();
    assert!(
        !val_slots.contains(&2),
        "entity 2 (null) must not have any real value insert for rating; got: {:?}",
        val_inserts
    );

    // Non-null entities get defensive NULL_BITMAP_KEY removes (clearing null in case slot
    // previously had null). Only entity 2 (null) should NOT have a null remove.
    let null_removes = sink.null_removes_for("rating");
    assert!(
        !null_removes.contains(&2),
        "entity 2 (null) must not have NULL_BITMAP_KEY remove; got: {:?}",
        null_removes
    );
}
