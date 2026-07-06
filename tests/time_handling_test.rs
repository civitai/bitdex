//! S3.7: Time handling integration tests.
//!
//! Verifies deferred alive lifecycle and TimeBucketManager integration.
use std::thread;
use std::time::Duration;
use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{BucketConfig, Config, DeferredAliveConfig, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{FilterClause, Value};
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
    // Don't assert — caller checks
}
#[test]
fn test_deferred_alive_far_future_invisible() {
    // A document with publishedAt far in the future should NOT be visible.
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "publishedAt".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
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
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        deferred_alive: Some(DeferredAliveConfig {
            source_field: "publishedAt".to_string(),
            ms_to_seconds: false,
        }),
        ..Default::default()
    };
    let engine = ConcurrentEngine::new(config).unwrap();
    // Insert doc with publishedAt far in the future
    engine
        .put(
            1,
            &make_doc(vec![
                (
                    "publishedAt",
                    FieldValue::Single(Value::Integer(i64::MAX)),
                ),
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(100)),
                ),
            ]),
        )
        .unwrap();
    // Wait for flush
    thread::sleep(Duration::from_millis(200));
    // Should NOT be visible (deferred alive not activated yet)
    assert_eq!(
        engine.alive_count(),
        0,
        "far-future doc should not be alive"
    );
    let result = engine.query(&[], None, 100).unwrap();
    assert!(
        result.ids.is_empty(),
        "far-future doc should not appear in queries"
    );
}
#[test]
fn test_deferred_alive_past_timestamp_visible() {
    // A document with publishedAt in the past should be immediately visible.
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "publishedAt".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
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
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        deferred_alive: Some(DeferredAliveConfig {
            source_field: "publishedAt".to_string(),
            ms_to_seconds: false,
        }),
        ..Default::default()
    };
    let engine = ConcurrentEngine::new(config).unwrap();
    // Insert doc with publishedAt in the past (timestamp 1 = long ago)
    engine
        .put(
            1,
            &make_doc(vec![
                ("publishedAt", FieldValue::Single(Value::Integer(1))),
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(100)),
                ),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 1, 2000);
    assert_eq!(engine.alive_count(), 1, "past-timestamp doc should be alive");
    let result = engine.query(&[], None, 100).unwrap();
    assert_eq!(result.ids, vec![1], "past-timestamp doc should be visible");
}
#[test]
fn test_mixed_deferred_and_immediate() {
    // Mix of deferred and immediate documents.
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "publishedAt".to_string(),
                field_type: FilterFieldType::SingleValue,
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
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        deferred_alive: Some(DeferredAliveConfig {
            source_field: "publishedAt".to_string(),
            ms_to_seconds: false,
        }),
        ..Default::default()
    };
    let engine = ConcurrentEngine::new(config).unwrap();
    // Doc 1: past timestamp (visible immediately)
    engine
        .put(
            1,
            &make_doc(vec![
                ("publishedAt", FieldValue::Single(Value::Integer(1))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(100)),
                ),
            ]),
        )
        .unwrap();
    // Doc 2: far future (invisible)
    engine
        .put(
            2,
            &make_doc(vec![
                (
                    "publishedAt",
                    FieldValue::Single(Value::Integer(i64::MAX)),
                ),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(200)),
                ),
            ]),
        )
        .unwrap();
    // Doc 3: past timestamp (visible immediately)
    engine
        .put(
            3,
            &make_doc(vec![
                ("publishedAt", FieldValue::Single(Value::Integer(100))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(300)),
                ),
            ]),
        )
        .unwrap();
    wait_for_flush(&engine, 2, 2000);
    // Only docs 1 and 3 should be visible
    assert_eq!(engine.alive_count(), 2);
    let result = engine.query(&[], None, 100).unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}
// ---------------------------------------------------------------------------
// Time bucket snapping integration tests
// ---------------------------------------------------------------------------
/// Helper to create an engine with time bucket config on a timestamp filter+sort field.
fn make_bucket_engine() -> (ConcurrentEngine, u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "sortAt".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            },
            FilterFieldConfig {
                name: "category".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                max_range_scan_values: None,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        }],
        time_buckets: Some(bitdex_v2::config::TimeBucketFieldConfig {
            filter_field: "sortAt".to_string(),
            sort_field: "sortAt".to_string(),
            range_buckets: vec![
                BucketConfig {
                    name: "24h".to_string(),
                    duration_secs: 86400,
                    refresh_interval_secs: 1, // 1s refresh for fast tests
                },
                BucketConfig {
                    name: "7d".to_string(),
                    duration_secs: 604800,
                    refresh_interval_secs: 1,
                },
                BucketConfig {
                    name: "30d".to_string(),
                    duration_secs: 2592000,
                    refresh_interval_secs: 1,
                },
            ],
            full_rebuild_interval_secs: 0,
        }),
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };
    let engine = ConcurrentEngine::new(config).unwrap();
    // Insert documents with various timestamps:
    // Slots 1-3: within last 24h
    // Slots 4-6: within last 7d but outside 24h
    // Slots 7-9: within last 30d but outside 7d
    // Slot 10: older than 30d
    let docs: Vec<(u32, i64, i64)> = vec![
        (1, (now - 3600) as i64, 1),       // 1h ago
        (2, (now - 7200) as i64, 1),       // 2h ago
        (3, (now - 43200) as i64, 2),      // 12h ago
        (4, (now - 172800) as i64, 1),     // 2d ago
        (5, (now - 345600) as i64, 2),     // 4d ago
        (6, (now - 518400) as i64, 1),     // 6d ago
        (7, (now - 864000) as i64, 1),     // 10d ago
        (8, (now - 1728000) as i64, 2),    // 20d ago
        (9, (now - 2500000) as i64, 1),    // ~29d ago
        (10, (now - 3000000) as i64, 1),   // ~35d ago
    ];
    for (id, ts, cat) in &docs {
        engine
            .put(
                *id,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer(*ts))),
                    ("category", FieldValue::Single(Value::Integer(*cat))),
                ]),
            )
            .unwrap();
    }
    wait_for_flush(&engine, 10, 2000);
    assert_eq!(engine.alive_count(), 10);
    // Wait for the flush thread to rebuild time buckets. Poll until the 24h bucket
    // is populated (verifies all buckets rebuilt, since they share a refresh cycle).
    let ts_24h = (now - 86400) as i64;
    let filters_24h = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts_24h))];
    for _ in 0..100 {
        let result = engine.query(&filters_24h, None, 100).unwrap();
        if result.ids.len() >= 3 {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    (engine, now)
}
#[test]
fn test_time_bucket_snap_24h() {
    let (engine, now) = make_bucket_engine();
    // Query: Gte("sortAt", now - 86400) → should snap to "24h" bucket
    // Expected: slots 1, 2, 3 (within 24h)
    let ts = (now - 86400) as i64;
    let filters = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts))];
    let result = engine.query(&filters, None, 100).unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3], "24h bucket should contain slots 1-3");
}
#[test]
fn test_time_bucket_snap_7d() {
    let (engine, now) = make_bucket_engine();
    // Query: Gte("sortAt", now - 604800) → should snap to "7d" bucket
    // Expected: slots 1-6 (within 7d)
    let ts = (now - 604800) as i64;
    let filters = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts))];
    let result = engine.query(&filters, None, 100).unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6], "7d bucket should contain slots 1-6");
}
#[test]
fn test_time_bucket_snap_30d() {
    let (engine, now) = make_bucket_engine();
    // Query: Gte("sortAt", now - 2592000) → should snap to "30d" bucket
    // Expected: slots 1-9 (within 30d)
    let ts = (now - 2592000) as i64;
    let filters = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts))];
    let result = engine.query(&filters, None, 100).unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 7, 8, 9], "30d bucket should contain slots 1-9");
}
#[test]
fn test_time_bucket_with_additional_filter() {
    let (engine, now) = make_bucket_engine();
    // Query: Gte("sortAt", now - 604800) AND category=1 → 7d bucket intersected with category=1
    // Expected: slots 1, 2, 4, 6 (within 7d AND category=1)
    let ts = (now - 604800) as i64;
    let filters = vec![
        FilterClause::Gte("sortAt".to_string(), Value::Integer(ts)),
        FilterClause::Eq("category".to_string(), Value::Integer(1)),
    ];
    let result = engine.query(&filters, None, 100).unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 4, 6], "7d + category=1 should match 4 docs");
}
#[test]
fn test_time_bucket_cache_stability() {
    let (engine, now) = make_bucket_engine();
    // Run the same 7d query twice — second should be a cache hit with stable key
    let ts = (now - 604800) as i64;
    let filters = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts))];
    let result1 = engine.query(&filters, None, 100).unwrap();
    let result2 = engine.query(&filters, None, 100).unwrap();
    assert_eq!(result1.ids, result2.ids, "repeated query should return same results");
    // Even with a slightly different timestamp (within 10% tolerance of 7d = ~16h),
    // it should still snap to the same "7d" bucket and produce identical results
    let ts_shifted = (now - 604800 + 50000) as i64; // ~14h off from 7d, within 10%
    let filters_shifted = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts_shifted))];
    let result3 = engine.query(&filters_shifted, None, 100).unwrap();
    assert_eq!(result1.ids, result3.ids, "shifted timestamp within tolerance should snap to same bucket");
}
#[test]
fn test_time_bucket_live_maintenance_on_insert() {
    let (engine, now) = make_bucket_engine();
    // Baseline: 7d bucket should have 6 docs (slots 1-6)
    let ts = (now - 604800) as i64;
    let filters = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts))];
    let result = engine.query(&filters, None, 100).unwrap();
    assert_eq!(result.ids.len(), 6, "baseline: 7d should have 6 docs");
    // Wait for the bucket refresh interval to elapse so the next flush triggers a rebuild
    thread::sleep(Duration::from_millis(1200));
    // Insert a new document within the 7d window — this triggers a flush cycle
    engine
        .put(
            11,
            &make_doc(vec![
                ("sortAt", FieldValue::Single(Value::Integer((now - 100000) as i64))), // ~1.2d ago
                ("category", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();
    // Wait for flush + bucket rebuild
    thread::sleep(Duration::from_millis(300));
    // After rebuild, 7d bucket should now include slot 11
    let result2 = engine.query(&filters, None, 100).unwrap();
    let mut ids = result2.ids.clone();
    ids.sort();
    assert!(ids.contains(&11), "new doc should appear in 7d bucket after rebuild");
    assert_eq!(ids.len(), 7, "7d should now have 7 docs");
}
#[test]
fn test_time_bucket_live_maintenance_on_delete() {
    let (engine, now) = make_bucket_engine();
    // Baseline: 24h bucket should have 3 docs (slots 1, 2, 3)
    let ts = (now - 86400) as i64;
    let filters = vec![FilterClause::Gte("sortAt".to_string(), Value::Integer(ts))];
    let result = engine.query(&filters, None, 100).unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3], "baseline: 24h should have 3 docs");
    // Wait for the bucket refresh interval to elapse so the next flush triggers a rebuild
    thread::sleep(Duration::from_millis(1200));
    // Delete slot 2 — this triggers a flush cycle which will also rebuild buckets
    engine.delete(2).unwrap();
    // Wait for flush + bucket rebuild
    thread::sleep(Duration::from_millis(300));
    // After rebuild, 24h bucket should no longer contain slot 2
    let result2 = engine.query(&filters, None, 100).unwrap();
    let mut ids2 = result2.ids.clone();
    ids2.sort();
    assert_eq!(ids2, vec![1, 3], "deleted doc should disappear from 24h bucket after rebuild");
}
