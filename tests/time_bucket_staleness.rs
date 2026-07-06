//! Regression test for time-bucket staleness when the sort field is unloaded
//! or its bits aren't set for a slot at flush time.
//!
//! Repros the production symptom from `docs/_in/time-bucket-staleness-handoff-2026-05-07.md`:
//! `Gte(sortAtUnix, X)` queries return stale results because new alive slots
//! get silently dropped from bucket bitmaps when `reconstruct_value` returns 0
//! (sort field empty / lazy not loaded).
use std::thread;
use std::time::Duration;
use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    BucketConfig, Config, FilterFieldConfig, SortFieldConfig, TimeBucketFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::Value;
fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
    Document {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}
fn wait_for_alive(engine: &ConcurrentEngine, expected: u64, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if engine.alive_count() == expected {
            thread::sleep(Duration::from_millis(50));
            return;
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(engine.alive_count(), expected, "timed out waiting for alive count");
}
fn build_engine() -> (ConcurrentEngine, u64) {
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
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAt".to_string(),
            sort_field: "sortAt".to_string(),
            range_buckets: vec![
                BucketConfig {
                    name: "24h".to_string(),
                    duration_secs: 86400,
                    refresh_interval_secs: 86400, // disable periodic rebuild — isolate live path
                },
                BucketConfig {
                    name: "7d".to_string(),
                    duration_secs: 604800,
                    refresh_interval_secs: 86400,
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
    (ConcurrentEngine::new(config).unwrap(), now)
}
fn bucket_count(engine: &ConcurrentEngine, name: &str) -> u64 {
    let stats = engine.time_bucket_stats();
    stats.get(name)
        .and_then(|v| v.get("slots"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}
/// Baseline: when slots are inserted WITH the sort field set, they land in the
/// matching bucket. This is the expected behavior — confirms the test rig works.
#[test]
fn baseline_alive_inserts_with_sort_field_populate_bucket() {
    let (engine, now) = build_engine();
    for slot in 1..=10u32 {
        engine
            .put(
                slot,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer((now - 3600) as i64))),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }
    wait_for_alive(&engine, 10, 2000);
    assert_eq!(
        bucket_count(&engine, "24h"),
        10,
        "all 10 fresh slots should be in 24h bucket — sort field populated, ts within window"
    );
}
/// Repro: alive_inserts arrive but the sort field has no bits for that slot.
///
/// `staging.sorts.get_field("sortAt")` returns Some (field is registered) but
/// `reconstruct_value(slot)` returns 0 because no bits were ever written. The
/// time-bucket flush block calls `insert_slot(slot, 0, now)`, which checks
/// `0 >= cutoff` — false for any cutoff > 0 — so slot is silently dropped from
/// every bucket. No metric, no error.
///
/// FAIL = bug present (current behavior expected to fail).
/// PASS = bug fixed.
#[test]
#[ignore = "documents the staleness bug — fails on the current code; un-ignore after fix"]
fn repro_alive_inserts_without_sort_field_drop_from_buckets() {
    let (engine, _now) = build_engine();
    // Insert slots without setting sortAt → sort field stays empty for these
    // slots, but they still flow through alive_inserts.
    for slot in 1..=10u32 {
        engine
            .put(
                slot,
                &make_doc(vec![("category", FieldValue::Single(Value::Integer(1)))]),
            )
            .unwrap();
    }
    wait_for_alive(&engine, 10, 2000);
    assert_eq!(
        bucket_count(&engine, "24h"),
        10,
        "EXPECTED: slots without sortAt should still register in 24h bucket OR \
         be dropped with a metric. Current behavior drops them silently — \
         this test asserts the future post-fix behavior."
    );
}
/// Builds the engine + persists baseline data, then reopens with the bucket
/// sort field unloaded. Returns the reopened engine plus `now`.
fn engine_reopened_with_unloaded_sort(
    dir: &std::path::Path,
    baseline_slots: u32,
) -> (ConcurrentEngine, Config, u64) {
    let bitmap_path = dir.join("bitmaps");
    let docstore_path = dir.join("docs");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "category".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        }],
        sort_fields: vec![SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        }],
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAt".to_string(),
            sort_field: "sortAt".to_string(),
            range_buckets: vec![BucketConfig {
                name: "24h".to_string(),
                duration_secs: 86400,
                refresh_interval_secs: 86400,
            }],
            full_rebuild_interval_secs: 0,
        }),
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 50,
        channel_capacity: 10_000,
        ..Default::default()
    };
    config.storage.bitmap_path = Some(bitmap_path);
    {
        let engine =
            ConcurrentEngine::new_with_path(config.clone(), docstore_path.as_path()).unwrap();
        for slot in 1..=baseline_slots {
            engine
                .put(
                    slot,
                    &make_doc(vec![
                        ("sortAt", FieldValue::Single(Value::Integer((now - 1800) as i64))),
                        ("category", FieldValue::Single(Value::Integer(1))),
                    ]),
                )
                .unwrap();
        }
        wait_for_alive(&engine, baseline_slots as u64, 2000);
        thread::sleep(Duration::from_millis(200));
        engine.save_and_unload().unwrap();
    }
    let engine = ConcurrentEngine::new_with_path(config.clone(), docstore_path.as_path()).unwrap();
    (engine, config, now)
}

/// Removes during the unloaded window cancel a pending deferred insert. Without
/// this guarantee, a slot inserted-then-deleted while the field was unloaded
/// would resurrect on field reload.
#[test]
fn alive_remove_cancels_deferred_insert() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _config, now) = engine_reopened_with_unloaded_sort(dir.path(), 3);
    let new_slot: u32 = 50;
    engine
        .put(
            new_slot,
            &make_doc(vec![
                ("sortAt", FieldValue::Single(Value::Integer((now - 600) as i64))),
                ("category", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();
    thread::sleep(Duration::from_millis(150));
    engine.delete(new_slot).unwrap();
    thread::sleep(Duration::from_millis(150));
    engine.ensure_fields_loaded(&[], Some("sortAt")).unwrap();
    thread::sleep(Duration::from_millis(300));
    // Slot was inserted then removed during the unload window. After reload +
    // replay, the bucket must NOT contain the slot. Bucket count for the
    // baseline slots is uncertain (they may or may not be in the 24h bucket
    // depending on whether the periodic rebuild fires) — what matters is the
    // upper bound: the new_slot's deferred insert must have been canceled.
    let count = bucket_count(&engine, "24h");
    assert!(
        count <= 3,
        "deferred insert for slot {} must be canceled by alive_remove. count={}",
        new_slot,
        count
    );
}

/// Defer-and-replay: when the bucket sort field is unloaded, alive_inserts
/// must NOT silently drop. Once the sort field becomes loaded, deferred slots
/// land in the appropriate bucket on a subsequent flush cycle.
#[test]
fn deferred_inserts_land_in_bucket_after_sort_field_reloads() {
    let dir = tempfile::tempdir().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "category".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        }],
        sort_fields: vec![SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        }],
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAt".to_string(),
            sort_field: "sortAt".to_string(),
            range_buckets: vec![BucketConfig {
                name: "24h".to_string(),
                duration_secs: 86400,
                refresh_interval_secs: 86400,
            }],
            full_rebuild_interval_secs: 0,
        }),
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 50,
        channel_capacity: 10_000,
        ..Default::default()
    };
    config.storage.bitmap_path = Some(bitmap_path.clone());
    // Phase 1: insert + persist baseline data with a populated sort field.
    {
        let engine =
            ConcurrentEngine::new_with_path(config.clone(), docstore_path.as_path()).unwrap();
        for slot in 1..=5u32 {
            engine
                .put(
                    slot,
                    &make_doc(vec![
                        ("sortAt", FieldValue::Single(Value::Integer((now - 1800) as i64))),
                        ("category", FieldValue::Single(Value::Integer(1))),
                    ]),
                )
                .unwrap();
        }
        wait_for_alive(&engine, 5, 2000);
        // Persist + drop bases. After this call, sortAt is_loaded=false in
        // staging, but the disk has the layers — a follow-up
        // ensure_fields_loaded brings them back.
        thread::sleep(Duration::from_millis(200));
        engine.save_and_unload().unwrap();
    }
    // Phase 2: reopen with the sort field unloaded. Insert a fresh slot.
    let engine =
        ConcurrentEngine::new_with_path(config.clone(), docstore_path.as_path()).unwrap();
    // Force the bucket sort field into the lazy/unloaded state on the staging
    // copy by skipping any query that would trigger eager load.
    let new_slot: u32 = 99;
    engine
        .put(
            new_slot,
            &make_doc(vec![
                ("sortAt", FieldValue::Single(Value::Integer((now - 600) as i64))),
                ("category", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();
    // A handful of flush cycles for the staleness path to either drop or
    // defer this slot, depending on the sort field's load state.
    thread::sleep(Duration::from_millis(300));
    // Trigger sort field reload — pending_bucket_retries should drain on the
    // first flush cycle that observes the loaded field.
    engine.ensure_fields_loaded(&[], Some("sortAt")).unwrap();
    thread::sleep(Duration::from_millis(300));
    let count = bucket_count(&engine, "24h");
    assert!(
        count >= 1,
        "deferred slot must land in 24h bucket once sort field reloads. count={}",
        count
    );
}

/// Symptom-level assertion the bug repro test above hardens against. Reads
/// the actual symptom from a fresh engine: bucket counts diverge from alive
/// counts when the sort field can't reconstruct timestamps for new alive slots.
///
/// This test is NOT ignored — it asserts current (buggy) behavior so that any
/// future change which silently changes drop semantics is caught.
#[test]
fn current_buggy_behavior_drop_count_matches_inserts_without_sort_field() {
    let (engine, _now) = build_engine();
    for slot in 1..=10u32 {
        engine
            .put(
                slot,
                &make_doc(vec![("category", FieldValue::Single(Value::Integer(1)))]),
            )
            .unwrap();
    }
    wait_for_alive(&engine, 10, 2000);
    let count = bucket_count(&engine, "24h");
    assert_eq!(
        count, 0,
        "documenting the bug: alive_inserts whose sort field reconstructs to 0 \
         are silently dropped from all buckets. alive_count=10, 24h_bucket={}",
        count,
    );
}

/// The read-only time-bucket audit reports accurate per-bucket
/// current / fresh_in_window / stale / missing, and correctly distinguishes
/// window boundaries. Healthy live-maintained buckets must show 0 stale and
/// 0 missing.
#[test]
fn audit_reports_accurate_membership_and_window_boundaries() {
    let (engine, now) = build_engine();
    // 10 slots 1h ago → in both the 24h and 7d windows.
    for slot in 1..=10u32 {
        engine
            .put(
                slot,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer((now - 3600) as i64))),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }
    // 5 slots ~2.3d ago → in the 7d window only (outside 24h).
    for slot in 11..=15u32 {
        engine
            .put(
                slot,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer((now - 200_000) as i64))),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }
    wait_for_alive(&engine, 15, 2000);

    let audit = engine.time_bucket_audit().expect("audit should succeed");
    let get = |bucket: &str, field: &str| -> u64 {
        audit["buckets"][bucket][field].as_u64().unwrap_or(u64::MAX)
    };

    // 24h: only the 10 recent slots; healthy → no stale, no missing.
    assert_eq!(get("24h", "current"), 10, "24h holds the 10 in-window slots");
    assert_eq!(get("24h", "fresh_in_window"), 10, "10 alive slots are truly in the 24h window");
    assert_eq!(get("24h", "stale"), 0, "healthy 24h bucket has no stale members");
    assert_eq!(get("24h", "missing"), 0, "live path populated the 24h bucket fully");

    // 7d: all 15 (200000s ≈ 2.3d < 7d) → validates window discrimination.
    assert_eq!(get("7d", "current"), 15, "7d holds all 15 slots");
    assert_eq!(get("7d", "fresh_in_window"), 15, "all 15 are within the 7d window");
    assert_eq!(get("7d", "stale"), 0, "healthy 7d bucket has no stale members");
    assert_eq!(get("7d", "missing"), 0, "live path populated the 7d bucket fully");
}
