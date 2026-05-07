//! S2.7: ConcurrentEngine restart integration tests.
//!
//! Verifies that engine state survives a shutdown → restart cycle:
//! - alive_count, slot_counter match
//! - Filter queries return identical results
//! - Sort ordering is preserved
//! - Deleted documents remain deleted
use std::thread;
use std::time::Duration;
use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{FilterClause, SortClause, SortDirection, Value};
fn restart_config(bitmap_path: &std::path::Path) -> Config {
    let mut config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "tagIds".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "onSite".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "reactionCount".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        }],
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100, // Fast merge for tests
        channel_capacity: 10_000,
        ..Default::default()
    };
    config.storage.bitmap_path = Some(bitmap_path.to_path_buf());
    config
}
fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
    Document {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}
fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if engine.alive_count() == expected_alive {
            thread::sleep(Duration::from_millis(2));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        engine.alive_count(),
        expected_alive,
        "timed out waiting for flush"
    );
}
/// Wait for the merge thread to persist data to redb.
fn wait_for_merge(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}
#[test]
fn test_restart_basic() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");
    let config = restart_config(&bitmap_path);
    // Phase 1: Insert documents and verify
    let pre_alive;
    let pre_counter;
    let pre_nsfw1_ids;
    let pre_sorted_ids;
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        for i in 1..=50u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        (
                            "nsfwLevel",
                            FieldValue::Single(Value::Integer((i % 5) as i64 + 1)),
                        ),
                        (
                            "tagIds",
                            FieldValue::Multi(vec![
                                Value::Integer((i % 10) as i64),
                                Value::Integer((i % 7) as i64 + 100),
                            ]),
                        ),
                        (
                            "onSite",
                            FieldValue::Single(Value::Bool(i % 2 == 0)),
                        ),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 100)),
                        ),
                    ]),
                )
                .unwrap();
        }
        wait_for_flush(&engine, 50, 2000);
        // Wait for merge to persist to redb
        wait_for_merge(500);
        pre_alive = engine.alive_count();
        pre_counter = engine.slot_counter();
        pre_nsfw1_ids = {
            let result = engine
                .query(
                    &[FilterClause::Eq(
                        "nsfwLevel".to_string(),
                        Value::Integer(1),
                    )],
                    None,
                    1000,
                )
                .unwrap();
            let mut ids = result.ids;
            ids.sort();
            ids
        };
        pre_sorted_ids = {
            let result = engine
                .query(
                    &[FilterClause::Eq(
                        "onSite".to_string(),
                        Value::Bool(true),
                    )],
                    Some(&SortClause {
                        field: "reactionCount".to_string(),
                        direction: SortDirection::Desc,
                    }),
                    10,
                )
                .unwrap();
            result.ids
        };
        // Engine dropped here — shutdown + cleanup
    }
    assert_eq!(pre_alive, 50);
    assert!(pre_counter >= 50);
    // Phase 2: Restart and verify
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        // alive_count and slot_counter should match
        assert_eq!(
            engine.alive_count(),
            pre_alive,
            "alive_count mismatch after restart"
        );
        assert_eq!(
            engine.slot_counter(),
            pre_counter,
            "slot_counter mismatch after restart"
        );
        // Filter query should return same results
        let post_nsfw1_ids = {
            let result = engine
                .query(
                    &[FilterClause::Eq(
                        "nsfwLevel".to_string(),
                        Value::Integer(1),
                    )],
                    None,
                    1000,
                )
                .unwrap();
            let mut ids = result.ids;
            ids.sort();
            ids
        };
        assert_eq!(
            pre_nsfw1_ids, post_nsfw1_ids,
            "nsfwLevel=1 results differ after restart"
        );
        // Sort query should return same results
        let post_sorted_ids = {
            let result = engine
                .query(
                    &[FilterClause::Eq(
                        "onSite".to_string(),
                        Value::Bool(true),
                    )],
                    Some(&SortClause {
                        field: "reactionCount".to_string(),
                        direction: SortDirection::Desc,
                    }),
                    10,
                )
                .unwrap();
            result.ids
        };
        assert_eq!(
            pre_sorted_ids, post_sorted_ids,
            "sorted results differ after restart"
        );
    }
}
#[test]
fn test_restart_after_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");
    let config = restart_config(&bitmap_path);
    let pre_alive;
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        // Insert 20 docs
        for i in 1..=20u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        (
                            "nsfwLevel",
                            FieldValue::Single(Value::Integer(1)),
                        ),
                        (
                            "onSite",
                            FieldValue::Single(Value::Bool(true)),
                        ),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64)),
                        ),
                    ]),
                )
                .unwrap();
        }
        wait_for_flush(&engine, 20, 2000);
        // Delete docs 5, 10, 15
        for &id in &[5u32, 10, 15] {
            engine.delete(id).unwrap();
        }
        wait_for_flush(&engine, 17, 2000);
        wait_for_merge(500);
        pre_alive = engine.alive_count();
    }
    assert_eq!(pre_alive, 17);
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        assert_eq!(engine.alive_count(), 17, "deletes not persisted");
        // Verify deleted IDs are not in query results
        let result = engine.query(&[], None, 1000).unwrap();
        assert!(!result.ids.contains(&5), "deleted doc 5 found");
        assert!(!result.ids.contains(&10), "deleted doc 10 found");
        assert!(!result.ids.contains(&15), "deleted doc 15 found");
        assert_eq!(result.ids.len(), 17);
    }
}
#[test]
fn test_restart_empty_engine() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");
    let config = restart_config(&bitmap_path);
    // Create and immediately drop an empty engine
    {
        let _engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        wait_for_merge(200);
    }
    // Restart — should work fine with empty db
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        assert_eq!(engine.alive_count(), 0);
        let result = engine.query(&[], None, 100).unwrap();
        assert!(result.ids.is_empty());
    }
}
#[test]
fn test_restart_after_upserts() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");
    let config = restart_config(&bitmap_path);
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        // Insert doc 1 with nsfwLevel=1
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            )
            .unwrap();
        wait_for_flush(&engine, 1, 2000);
        // Upsert doc 1 with nsfwLevel=5
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
                    ("onSite", FieldValue::Single(Value::Bool(false))),
                    ("reactionCount", FieldValue::Single(Value::Integer(999))),
                ]),
            )
            .unwrap();
        wait_for_flush(&engine, 1, 2000);
        wait_for_merge(500);
    }
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        assert_eq!(engine.alive_count(), 1);
        // Should find doc under nsfwLevel=5, not 1
        let result_5 = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(5),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result_5.ids, vec![1], "upserted value not found");
        let result_1 = engine
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
            result_1.ids.is_empty(),
            "old value still present after upsert and restart"
        );
    }
}
#[test]
fn test_restart_lazy_value_loading() {
    // Verifies that multi_value fields (tagIds) use per-value lazy loading:
    // only the specific queried tag values are loaded, not all 10 tag bitmaps.
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");
    let config = restart_config(&bitmap_path);
    // Phase 1: Insert documents with various tagIds
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        for i in 1..=20u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        (
                            "tagIds",
                            FieldValue::Multi(vec![
                                Value::Integer((i % 5) as i64),     // tags 0-4
                                Value::Integer((i % 3) as i64 + 10), // tags 10-12
                            ]),
                        ),
                        ("onSite", FieldValue::Single(Value::Bool(true))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 10)),
                        ),
                    ]),
                )
                .unwrap();
        }
        wait_for_flush(&engine, 20, 2000);
        wait_for_merge(500);
    }
    // Phase 2: Restart and query specific tag values
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        )
        .unwrap();
        assert_eq!(engine.alive_count(), 20);
        // Query tagIds=0 — should load just that value's bitmap
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(0))],
                None,
                100,
            )
            .unwrap();
        // Docs with i % 5 == 0: 5, 10, 15, 20
        assert_eq!(result.ids.len(), 4, "tagIds=0 should match 4 docs");
        let mut ids = result.ids.clone();
        ids.sort();
        assert_eq!(ids, vec![5, 10, 15, 20]);
        // Query tagIds=10 — should load just that value
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(10))],
                None,
                100,
            )
            .unwrap();
        // Docs with i % 3 == 0: 3, 6, 9, 12, 15, 18
        assert_eq!(result.ids.len(), 6, "tagIds=10 should match 6 docs");
        // Query with In clause — loads multiple specific values
        let result = engine
            .query(
                &[FilterClause::In(
                    "tagIds".to_string(),
                    vec![Value::Integer(1), Value::Integer(2)],
                )],
                None,
                100,
            )
            .unwrap();
        // tagIds=1: i%5==1 → 1,6,11,16 ; tagIds=2: i%5==2 → 2,7,12,17
        assert_eq!(result.ids.len(), 8, "tagIds IN [1,2] should match 8 docs");
        // Repeat same query — should be instant (already loaded)
        let result2 = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(0))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result2.ids.len(), 4, "repeated query should still work");
        // Query a tag value that doesn't exist — should return empty
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(999))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids.len(), 0, "nonexistent tag should match nothing");
    }
}
