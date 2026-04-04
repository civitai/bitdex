use super::*;
use crate::config::{FilterFieldConfig, SortFieldConfig};
use crate::filter::FilterFieldType;
use crate::mutation::FieldValue;
use crate::query::{BitdexQuery, FilterClause, SortClause, SortDirection, Value};
use std::sync::Arc;
use std::thread;
fn test_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "tagIds".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "onSite".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
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
        max_page_size: 100,
        flush_interval_us: 50, // Fast flush for tests
        channel_capacity: 10_000,
        ..Default::default()
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
/// Wait for the flush thread to apply all pending mutations.
fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if engine.alive_count() == expected_alive {
            // Give one more flush cycle to ensure everything is settled
            thread::sleep(Duration::from_millis(2));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    // Final check
    assert_eq!(
        engine.alive_count(),
        expected_alive,
        "timed out waiting for flush; alive_count={} expected={}",
        engine.alive_count(),
        expected_alive
    );
}
// ---- Basic correctness tests ----
#[test]
fn test_put_and_query() {
    let engine = ConcurrentEngine::new(test_config()).unwrap();
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(42))),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 1, 500);
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
    assert_eq!(result.ids, vec![1]);
}
#[test]
fn test_put_multiple_and_sorted_query() {
    let engine = ConcurrentEngine::new(test_config()).unwrap();
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(100))),
            ]),
        )
        .unwrap();
    engine
        .put(
            2,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(500))),
            ]),
        )
        .unwrap();
    engine
        .put(
            3,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(300))),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 3, 500);
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
    assert_eq!(result.ids, vec![2, 3, 1]); // 500, 300, 100
}
#[test]
fn test_delete() {
    let engine = ConcurrentEngine::new(test_config()).unwrap();
    engine
        .put(
            1,
            &make_doc(vec![(
                "nsfwLevel",
                FieldValue::Single(Value::Integer(1)),
            )]),
        )
        .unwrap();
    engine
        .put(
            2,
            &make_doc(vec![(
                "nsfwLevel",
                FieldValue::Single(Value::Integer(1)),
            )]),
        )
        .unwrap();
    wait_for_flush(&engine, 2, 500);
    engine.delete(1).unwrap();
    // Wait for delete to be flushed
    wait_for_flush(&engine, 1, 500);
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
    assert_eq!(result.ids, vec![2]);
}
#[test]
fn test_upsert_correctness() {
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();
    // Initial insert
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(10))),
            ]),
        )
        .unwrap();
    // Must wait for first put to be fully flushed (alive bit set)
    // before doing upsert, otherwise the second put won't detect is_alive=true
    wait_for_flush(&engine, 1, 500);
    // Verify first insert is visible
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
    assert_eq!(result.ids, vec![1]);
    // Upsert with new values — now the alive bit is set so diff will detect upsert
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                ("reactionCount", FieldValue::Single(Value::Integer(99))),
            ]),
        )
        .unwrap();
    // Wait for upsert flush. alive_count stays 1 so we need a different signal.
    // Shutdown ensures final flush completes.
    engine.shutdown();
    // Old value should not match
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
    assert!(result.ids.is_empty());
    // New value should match
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(2),
            )],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.ids, vec![1]);
}
#[test]
fn test_execute_query() {
    let engine = ConcurrentEngine::new(test_config()).unwrap();
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(42))),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 1, 500);
    let query = BitdexQuery {
        filters: vec![FilterClause::Eq(
            "nsfwLevel".to_string(),
            Value::Integer(1),
        )],
        sort: Some(SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        }),
        limit: 50,
        cursor: None,
        offset: None,
        skip_cache: false,
    };
    let result = engine.execute_query(&query).unwrap();
    assert_eq!(result.ids, vec![1]);
}
// ---- Concurrency tests ----
#[test]
fn test_concurrent_puts() {
    let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
    let num_threads = 4;
    let docs_per_thread = 50;
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..docs_per_thread {
                    let id = (t * docs_per_thread + i + 1) as u32;
                    engine
                        .put(
                            id,
                            &make_doc(vec![
                                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                                (
                                    "reactionCount",
                                    FieldValue::Single(Value::Integer(id as i64)),
                                ),
                            ]),
                        )
                        .unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let total = (num_threads * docs_per_thread) as u64;
    wait_for_flush(&engine, total, 2000);
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
    assert_eq!(result.total_matched, total);
}
#[test]
fn test_concurrent_reads_during_writes() {
    let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
    // Pre-populate some docs
    for i in 1..=10u32 {
        engine
            .put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(i as i64 * 10)),
                    ),
                ]),
            )
            .unwrap();
    }
    wait_for_flush(&engine, 10, 500);
    // Spawn writer threads adding more docs
    let writer_handles: Vec<_> = (0..2)
        .map(|t| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..25 {
                    let id = 100 + t * 25 + i;
                    engine
                        .put(
                            id as u32,
                            &make_doc(vec![
                                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                                (
                                    "reactionCount",
                                    FieldValue::Single(Value::Integer(id as i64)),
                                ),
                            ]),
                        )
                        .unwrap();
                }
            })
        })
        .collect();
    // Spawn reader threads querying concurrently
    let reader_handles: Vec<_> = (0..4)
        .map(|_| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let mut success_count = 0;
                for _ in 0..50 {
                    let result = engine.query(
                        &[FilterClause::Eq(
                            "nsfwLevel".to_string(),
                            Value::Integer(1),
                        )],
                        None,
                        100,
                    );
                    assert!(result.is_ok(), "query should not fail");
                    success_count += 1;
                    thread::yield_now();
                }
                success_count
            })
        })
        .collect();
    for h in writer_handles {
        h.join().unwrap();
    }
    for h in reader_handles {
        let count = h.join().unwrap();
        assert_eq!(count, 50, "all reader queries should succeed");
    }
}
#[test]
fn test_concurrent_mixed_read_write() {
    let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..20 {
                    if t % 2 == 0 {
                        // Writer
                        let id = (t * 20 + i + 1) as u32;
                        engine
                            .put(
                                id,
                                &make_doc(vec![(
                                    "nsfwLevel",
                                    FieldValue::Single(Value::Integer(1)),
                                )]),
                            )
                            .unwrap();
                    } else {
                        // Reader
                        let _ = engine.query(
                            &[FilterClause::Eq(
                                "nsfwLevel".to_string(),
                                Value::Integer(1),
                            )],
                            None,
                            100,
                        );
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    // No panics = success for concurrency safety
}
#[test]
fn test_shutdown_flushes_remaining() {
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();
    for i in 1..=5u32 {
        engine
            .put(
                i,
                &make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(1)),
                )]),
            )
            .unwrap();
    }
    // Shutdown triggers final flush
    engine.shutdown();
    assert_eq!(engine.alive_count(), 5);
}
#[test]
fn test_multi_value_filter() {
    let engine = ConcurrentEngine::new(test_config()).unwrap();
    engine
        .put(
            1,
            &make_doc(vec![(
                "tagIds",
                FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
            )]),
        )
        .unwrap();
    engine
        .put(
            2,
            &make_doc(vec![(
                "tagIds",
                FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)]),
            )]),
        )
        .unwrap();
    wait_for_flush(&engine, 2, 500);
    // Query for tag 200 - should match both
    let result = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.total_matched, 2);
    // Query for tag 100 - should match only doc 1
    let result = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.ids, vec![1]);
}
#[test]
fn test_merge_thread_starts_and_stops() {
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();
    // Just verify it starts and shuts down cleanly
    engine.shutdown();
}
#[test]
fn test_two_threads_independent() {
    let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
    // Insert a doc to exercise the flush thread
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(42))),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 1, 500);
    // Query to verify flush worked while merge thread is also running
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
    assert!(result.ids.contains(&1));
}
/// Filter queries return correct results across multiple flush cycles.
#[test]
fn test_filter_diffs_accumulate_across_flushes() {
    let engine = ConcurrentEngine::new(test_config()).unwrap();
    // Insert doc A
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
                ("onSite", FieldValue::Single(Value::Bool(true))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(10)),
                ),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 1, 500);
    // Insert doc B with same nsfwLevel
    engine
        .put(
            2,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
                ("onSite", FieldValue::Single(Value::Bool(false))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(20)),
                ),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 2, 500);
    // Query should return both docs
    let result = engine
        .query(
            &[FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(3),
            )],
            None,
            100,
        )
        .unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 2], "both docs should match nsfwLevel=3");
}
/// S1.8-5: Concurrent reads during mutations return correct results.
#[test]
fn test_concurrent_reads_during_mutations() {
    let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
    // Insert initial docs
    for i in 1..=20u32 {
        engine
            .put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64 + 1))),
                    ("onSite", FieldValue::Single(Value::Bool(i % 2 == 0))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(i as i64)),
                    ),
                ]),
            )
            .unwrap();
    }
    wait_for_flush(&engine, 20, 1000);
    // Spawn reader threads that query continuously
    let mut handles = Vec::new();
    for _ in 0..4 {
        let eng = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                // Query should never panic or return inconsistent results
                let result = eng
                    .query(
                        &[FilterClause::Eq(
                            "nsfwLevel".to_string(),
                            Value::Integer(1),
                        )],
                        None,
                        100,
                    )
                    .unwrap();
                // Results should be non-empty (we inserted docs with nsfwLevel=1)
                assert!(!result.ids.is_empty(), "query returned empty during concurrent reads");
                thread::sleep(Duration::from_micros(100));
            }
        }));
    }
    // Concurrently insert more docs
    for i in 21..=40u32 {
        engine
            .put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64 + 1))),
                    ("onSite", FieldValue::Single(Value::Bool(i % 2 == 0))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(i as i64)),
                    ),
                ]),
            )
            .unwrap();
        thread::sleep(Duration::from_micros(200));
    }
    // Wait for all readers to finish
    for h in handles {
        h.join().unwrap();
    }
    // Final verification
    wait_for_flush(&engine, 40, 1000);
    let result = engine.query(&[], None, 1000).unwrap();
    assert_eq!(result.ids.len(), 40, "all 40 docs should be alive");
}
// ---- Snapshot save/restore tests ----
fn test_config_with_bitmap_path(bitmap_path: std::path::PathBuf) -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "tagIds".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "onSite".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
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
        max_page_size: 100,
        flush_interval_us: 50,
        channel_capacity: 10_000,
        storage: crate::config::StorageConfig {
            bitmap_path: Some(bitmap_path),
            ..Default::default()
        },
        ..Default::default()
    }
}
#[test]
fn test_save_snapshot_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    let config = test_config_with_bitmap_path(bitmap_path.clone());
    // Phase 1: Create engine, insert data, save snapshot
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)])),
                    ("onSite", FieldValue::Single(Value::Bool(false))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            )
            .unwrap();
        engine
            .put(
                3,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(100)])),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    ("reactionCount", FieldValue::Single(Value::Integer(300))),
                ]),
            )
            .unwrap();
        // Shutdown to ensure all mutations are flushed and published
        engine.shutdown();
        // Verify data is visible before saving
        assert_eq!(engine.alive_count(), 3);
        // Save the snapshot
        engine.save_snapshot().unwrap();
    }
    // Phase 2: Create a NEW engine from the same config+paths and verify restoration
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        // Verify alive count restored
        assert_eq!(
            engine.alive_count(),
            3,
            "alive count should be restored from snapshot"
        );
        // Verify slot counter restored
        assert_eq!(
            engine.slot_counter(),
            4,
            "slot counter should be restored (next_slot = max_id + 1)"
        );
        // Verify filter queries work
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();
        let mut ids = result.ids.clone();
        ids.sort();
        assert_eq!(ids, vec![1, 3], "nsfwLevel=1 should match docs 1 and 3");
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(2))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![2], "nsfwLevel=2 should match doc 2");
        // Verify multi-value filter
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(
            result.total_matched, 2,
            "tagIds=200 should match docs 1 and 2"
        );
        // Verify boolean filter
        let result = engine
            .query(
                &[FilterClause::Eq("onSite".to_string(), Value::Bool(true))],
                None,
                100,
            )
            .unwrap();
        let mut ids = result.ids.clone();
        ids.sort();
        assert_eq!(ids, vec![1, 3], "onSite=true should match docs 1 and 3");
        // Verify sort works correctly (descending reactionCount)
        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                Some(&sort),
                10,
            )
            .unwrap();
        assert_eq!(
            result.ids,
            vec![1, 3],
            "sort desc should return 500 (doc 1) before 300 (doc 3)"
        );
    }
}
#[test]
fn test_save_snapshot_empty_engine() {
    let dir = tempfile::tempdir().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    let config = test_config_with_bitmap_path(bitmap_path.clone());
    // Save snapshot of empty engine
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        engine.save_snapshot().unwrap();
    }
    // Restore from empty snapshot
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        assert_eq!(engine.alive_count(), 0, "empty snapshot should restore to 0 alive");
        assert_eq!(engine.slot_counter(), 0, "empty snapshot should restore counter to 0");
    }
}
#[test]
fn test_save_snapshot_after_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    let config = test_config_with_bitmap_path(bitmap_path.clone());
    // Insert 3 docs, delete 1, then save and restore
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        for i in 1..=3u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 10))),
                    ]),
                )
                .unwrap();
        }
        wait_for_flush(&engine, 3, 500);
        // Delete doc 2
        engine.delete(2).unwrap();
        wait_for_flush(&engine, 2, 500);
        engine.shutdown();
        engine.save_snapshot().unwrap();
    }
    // Restore and verify
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        assert_eq!(engine.alive_count(), 2, "should have 2 alive after delete");
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();
        let mut ids = result.ids.clone();
        ids.sort();
        assert_eq!(ids, vec![1, 3], "deleted doc 2 should not appear");
    }
}
#[test]
fn test_save_snapshot_preserves_sort_values() {
    let dir = tempfile::tempdir().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    let config = test_config_with_bitmap_path(bitmap_path.clone());
    // Insert docs with specific sort values
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            )
            .unwrap();
        engine
            .put(
                3,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(300))),
                ]),
            )
            .unwrap();
        engine.shutdown();
        engine.save_snapshot().unwrap();
    }
    // Restore and verify sort order is preserved
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                Some(&sort),
                10,
            )
            .unwrap();
        assert_eq!(
            result.ids,
            vec![2, 3, 1],
            "descending sort should be 500, 300, 100 after restore"
        );
        let sort_asc = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Asc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                Some(&sort_asc),
                10,
            )
            .unwrap();
        assert_eq!(
            result.ids,
            vec![1, 3, 2],
            "ascending sort should be 100, 300, 500 after restore"
        );
    }
}
// ---- Named cursor tests ----
#[test]
fn test_cursor_set_and_get() {
    let engine = ConcurrentEngine::new(test_config()).unwrap();
    // No cursor initially
    assert!(engine.get_cursor("pg-sync-0").is_none());
    assert!(engine.get_all_cursors().is_empty());
    // Set a cursor
    engine.set_cursor("pg-sync-0".to_string(), "12345".to_string());
    assert_eq!(engine.get_cursor("pg-sync-0").unwrap(), "12345");
    // Set another
    engine.set_cursor("pg-sync-1".to_string(), "12300".to_string());
    let all = engine.get_all_cursors();
    assert_eq!(all.len(), 2);
    assert_eq!(all["pg-sync-0"], "12345");
    assert_eq!(all["pg-sync-1"], "12300");
    // Overwrite
    engine.set_cursor("pg-sync-0".to_string(), "12400".to_string());
    assert_eq!(engine.get_cursor("pg-sync-0").unwrap(), "12400");
}
// ---- Regression tests for reliability fixes ----
/// Regression test: delete() marks slots in-flight (just like put()),
/// preventing concurrent readers from seeing partially-applied delete
/// mutations.
#[test]
fn test_concurrent_put_delete_in_flight_race() {
    let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
    let num_docs = 20u32;
    for id in 1..=num_docs {
        engine
            .put(
                id,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer((id % 3 + 1) as i64))),
                    ("reactionCount", FieldValue::Single(Value::Integer(id as i64 * 10))),
                ]),
            )
            .unwrap();
    }
    wait_for_flush(&engine, num_docs as u64, 1000);
    let iterations = 100;
    let query_error_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let put_handles: Vec<_> = (0..4)
        .map(|t| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let base = 100 + t * iterations;
                for i in 0..iterations {
                    let id = (base + i) as u32;
                    let val = (i % 5 + 1) as i64;
                    engine
                        .put(
                            id,
                            &make_doc(vec![
                                ("nsfwLevel", FieldValue::Single(Value::Integer(val))),
                                ("reactionCount", FieldValue::Single(Value::Integer(val * 10))),
                            ]),
                        )
                        .ok();
                    thread::yield_now();
                }
            })
        })
        .collect();
    let delete_handles: Vec<_> = (0..4)
        .map(|t| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let start = t * 5 + 1;
                for id in start..start + 5 {
                    engine.delete(id as u32).ok();
                    thread::yield_now();
                }
            })
        })
        .collect();
    let reader_handles: Vec<_> = (0..4)
        .map(|_| {
            let engine = Arc::clone(&engine);
            let errors = Arc::clone(&query_error_count);
            thread::spawn(move || {
                for _ in 0..200 {
                    for val in 1..=5i64 {
                        match engine.query(
                            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(val))],
                            None,
                            1000,
                        ) {
                            Ok(_) => {}
                            Err(_) => { errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                        }
                    }
                    thread::yield_now();
                }
            })
        })
        .collect();
    for h in put_handles { h.join().unwrap(); }
    for h in delete_handles { h.join().unwrap(); }
    for h in reader_handles { h.join().unwrap(); }
    assert_eq!(query_error_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    let mut engine = Arc::try_unwrap(engine).ok().expect("refcount 1");
    engine.shutdown();
    let expected_alive = 400u64;
    assert_eq!(engine.alive_count(), expected_alive);
    let mut all_found: Vec<i64> = Vec::new();
    for val in 1..=5i64 {
        let result = engine
            .query(&[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(val))], None, 1000)
            .unwrap();
        all_found.extend_from_slice(&result.ids);
    }
    all_found.sort();
    all_found.dedup();
    assert_eq!(all_found.len(), expected_alive as usize);
    for id in 1..=num_docs as i64 {
        assert!(!all_found.contains(&id), "deleted slot {} found in filter query", id);
    }
}
#[test]
fn test_eager_load_fields_not_pending_after_restore() {
    let dir = tempfile::tempdir().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    // Config: nsfwLevel is eager_load=true, onSite is eager_load=false
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: true, // <-- eager
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "onSite".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: false, // <-- lazy (default)
                per_value_lazy: false,
            },
        ],
        sort_fields: vec![
            SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: true, // <-- eager
                computed: None,
            },
        ],
        max_page_size: 100,
        flush_interval_us: 50,
        channel_capacity: 10_000,
        storage: crate::config::StorageConfig {
            bitmap_path: Some(bitmap_path.clone()),
        },
        ..Default::default()
    };
    // Insert some data, save snapshot
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                    ("onSite", FieldValue::Single(Value::Bool(false))),
                    ("reactionCount", FieldValue::Single(Value::Integer(99))),
                ]),
            )
            .unwrap();
        engine.shutdown();
        engine.save_snapshot().unwrap();
    }
    // Restore — pending_filter_loads / pending_sort_loads removed (BitmapSilo handles lazy loading).
    // Fields are all queryable after restore via BitmapSilo mmap.
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                Some(&SortClause {
                    field: "reactionCount".to_string(),
                    direction: SortDirection::Desc,
                }),
                10,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
    }
}
/// Reproduce the WAL reader stall: ops for alive slots should be applied,
/// not silently skipped. This test exercises the exact code path used by
/// the server WAL reader thread.
#[cfg(feature = "pg-sync")]
#[test]
fn test_wal_reader_ops_alive_check() {
    use crate::pg_sync::ops::{EntityOps, Op};
    use crate::ops_processor::{FieldMeta, apply_ops_batch, DocWriter};
    use crate::ingester::CoalescerSink;
    use serde_json::json;

    let mut engine = ConcurrentEngine::new(test_config()).unwrap();

    // Insert doc to make slot 100 alive
    engine.put(100, &make_doc(vec![
        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
    ])).unwrap();
    wait_for_flush(&engine, 1, 500);
    assert!(engine.is_slot_alive(100), "slot 100 should be alive");

    // Build ops processor components (same as server WAL reader thread)
    let meta = FieldMeta::from_config(engine.config());
    let sender = engine.mutation_sender();
    let mut sink = CoalescerSink::new(sender);
    let mut doc_writer = DocWriter::new(engine.docstore_arc());

    // Apply ops for alive slot — should succeed
    let mut entries = vec![EntityOps {
        entity_id: 100,
        creates_slot: false,
        ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(16) }],
    }];
    let (applied, skipped, errors) = apply_ops_batch(
        &mut sink, &meta, &mut entries, Some(&engine), Some(&mut doc_writer),
    );
    assert_eq!(applied, 1, "op for alive slot must be applied");
    assert_eq!(skipped, 0, "no ops should be skipped");
    assert_eq!(errors, 0, "no errors expected");

    // Apply ops for non-alive slot below slot_counter — should be skipped
    let sc = engine.slot_counter();
    eprintln!("slot_counter = {sc}");
    let dead_slot: i64 = if sc > 50 { 50 } else { (sc + 100) as i64 };
    let mut entries2 = vec![EntityOps {
        entity_id: dead_slot,
        creates_slot: false,
        ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(8) }],
    }];
    let (applied2, skipped2, errors2) = apply_ops_batch(
        &mut sink, &meta, &mut entries2, Some(&engine), Some(&mut doc_writer),
    );
    if (dead_slot as u32) < sc {
        assert_eq!(skipped2, 1, "non-alive slot below slot_counter should be skipped");
        assert_eq!(applied2, 0);
    } else {
        // Auto-promoted because beyond slot_counter
        assert_eq!(applied2, 1, "slot beyond slot_counter should be auto-promoted");
    }
    assert_eq!(errors2, 0);

    // Apply ops with creates_slot=true for new entity — should succeed
    let new_slot = (sc + 1000) as i64;
    let mut entries3 = vec![EntityOps {
        entity_id: new_slot,
        creates_slot: true,
        ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(4) }],
    }];
    let (applied3, skipped3, errors3) = apply_ops_batch(
        &mut sink, &meta, &mut entries3, Some(&engine), Some(&mut doc_writer),
    );
    assert_eq!(applied3, 1, "creates_slot=true should always succeed");
    assert_eq!(skipped3, 0);
    assert_eq!(errors3, 0);

    engine.shutdown();
}
// --- Write path audit items 2.11, 2.15, 2.16, 2.17 ---
#[test]
fn test_delete_cleans_filter_and_sort_bits() {
    // 2.11: DELETE should clear all filter/sort bitmap bits before clearing alive
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();
    engine
        .put(
            1,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                ("reactionCount", FieldValue::Single(Value::Integer(42))),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 1, 500);
    // Verify it's queryable before delete
    let result = engine
        .query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.total_matched, 1);
    // Delete
    engine.delete(1).unwrap();
    thread::sleep(Duration::from_millis(50));
    // Verify alive is cleared
    assert_eq!(engine.alive_count(), 0);
    // Verify filter bitmaps are clean (no stale bits)
    let result = engine
        .query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.total_matched, 0, "nsfwLevel bitmap should be clean after delete");
    let result = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.total_matched, 0, "tagIds bitmap should be clean after delete");
    engine.shutdown();
}
#[test]
fn test_multi_value_diff_add_and_remove() {
    // 2.15: Upsert that changes multi-value field should add new values and remove old
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();
    // Insert with tagIds [100, 200]
    engine
        .put(
            1,
            &make_doc(vec![
                ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 1, 500);
    // Upsert with tagIds [200, 300] — should remove 100, keep 200, add 300
    engine
        .put(
            1,
            &make_doc(vec![
                ("tagIds", FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)])),
            ]),
        )
        .unwrap();
    thread::sleep(Duration::from_millis(50));
    // Tag 100 should be gone
    let result = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.total_matched, 0, "tag 100 should be removed after upsert");
    // Tag 200 should still be there
    let result = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.ids, vec![1]);
    // Tag 300 should be added
    let result = engine
        .query(
            &[FilterClause::Eq("tagIds".to_string(), Value::Integer(300))],
            None,
            100,
        )
        .unwrap();
    assert_eq!(result.ids, vec![1]);
    engine.shutdown();
}
#[test]
fn test_sort_bitmap_updates_on_value_change() {
    // 2.16: Changing a sort field value should update sort layer bitmaps
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();
    // Insert two docs with different reactionCounts
    engine
        .put(1, &make_doc(vec![
            ("reactionCount", FieldValue::Single(Value::Integer(10))),
        ]))
        .unwrap();
    engine
        .put(2, &make_doc(vec![
            ("reactionCount", FieldValue::Single(Value::Integer(20))),
        ]))
        .unwrap();
    wait_for_flush(&engine, 2, 500);
    // Sort by reactionCount desc — doc 2 (20) should come first
    let result = engine
        .query(
            &[],
            Some(&SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            2,
        )
        .unwrap();
    assert_eq!(result.ids, vec![2, 1]);
    // Update doc 1 to have higher reactionCount
    engine
        .put(1, &make_doc(vec![
            ("reactionCount", FieldValue::Single(Value::Integer(30))),
        ]))
        .unwrap();
    thread::sleep(Duration::from_millis(50));
    // Now doc 1 (30) should come first
    let result = engine
        .query(
            &[],
            Some(&SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            2,
        )
        .unwrap();
    assert_eq!(result.ids, vec![1, 2]);
    engine.shutdown();
}
// -----------------------------------------------------------------------
// DataSilo E2E integration tests
// -----------------------------------------------------------------------

/// E2E: put() writes doc through flush thread → docstore, then get reads it back.
#[test]
fn test_docstore_v3_put_and_read_back() {
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();

    engine.put(1, &make_doc(vec![
        ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
        ("reactionCount", FieldValue::Single(Value::Integer(42))),
    ])).unwrap();

    // Wait for flush thread to persist the doc
    wait_for_flush(&engine, 1, 500);

    // Read the doc back from DataSilo
    let doc = engine.docstore.lock().get(1).unwrap();
    assert!(doc.is_some(), "doc should be readable after put + flush");
    let doc = doc.unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(5))),
        "nsfwLevel should roundtrip through DataSilo"
    );
    assert_eq!(
        doc.fields.get("reactionCount"),
        Some(&FieldValue::Single(Value::Integer(42))),
        "reactionCount should roundtrip through DataSilo"
    );

    engine.shutdown();
}

/// E2E: upsert reads old doc from DataSilo for diff, clears stale bits.
#[test]
fn test_docstore_v3_upsert_reads_old_doc() {
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();

    // Insert doc with nsfwLevel=1
    engine.put(1, &make_doc(vec![
        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ("reactionCount", FieldValue::Single(Value::Integer(10))),
    ])).unwrap();
    wait_for_flush(&engine, 1, 500);

    // Verify nsfwLevel=1 matches
    let result = engine.query(
        &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
        None, 10,
    ).unwrap();
    assert_eq!(result.ids, vec![1], "nsfwLevel=1 should match before upsert");

    // Upsert with nsfwLevel=3 — this requires reading old doc from DataSilo
    engine.put(1, &make_doc(vec![
        ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
        ("reactionCount", FieldValue::Single(Value::Integer(10))),
    ])).unwrap();
    wait_for_flush(&engine, 1, 500);

    // Old nsfwLevel=1 bitmap bit should be cleared (clean delete via docstore diff)
    let result = engine.query(
        &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
        None, 10,
    ).unwrap();
    assert_eq!(result.total_matched, 0, "nsfwLevel=1 should be cleared after upsert to 3");

    // New nsfwLevel=3 should match
    let result = engine.query(
        &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(3))],
        None, 10,
    ).unwrap();
    assert_eq!(result.ids, vec![1], "nsfwLevel=3 should match after upsert");

    // Verify the stored doc has the new values
    let doc = engine.docstore.lock().get(1).unwrap().unwrap();
    assert_eq!(
        doc.fields.get("nsfwLevel"),
        Some(&FieldValue::Single(Value::Integer(3))),
    );

    engine.shutdown();
}

/// E2E: delete reads old doc from DataSilo to clear all bitmap bits.
#[test]
fn test_docstore_v3_delete_reads_old_doc() {
    let mut engine = ConcurrentEngine::new(test_config()).unwrap();

    engine.put(1, &make_doc(vec![
        ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
        ("reactionCount", FieldValue::Single(Value::Integer(99))),
    ])).unwrap();
    wait_for_flush(&engine, 1, 500);

    // Doc should exist
    assert!(engine.docstore.lock().get(1).unwrap().is_some());

    // Delete — this reads old doc from DataSilo to clear filter/sort bits
    engine.delete(1).unwrap();
    wait_for_flush(&engine, 0, 500);

    // Bitmap should be clean (no alive, no filter match)
    let result = engine.query(
        &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(2))],
        None, 10,
    ).unwrap();
    assert_eq!(result.total_matched, 0, "nsfwLevel=2 should be cleared after delete");

    engine.shutdown();
}

// DocWriter E2E test lives in ops_processor.rs (needs private method access)
