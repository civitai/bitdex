//! Regression test for #274: a cache entry that FILTERS on a time bucket but
//! SORTS by a non-bucket field must gain a slot that enters the bucket window,
//! even when the slot's own sort field never changes.
//!
//! Scenario (Ivanna's repro): "top reactionCount, last 7d". An existing slot's
//! `sortAt` jumps into the 7d window (without `reactionCount` changing).
//! `collect_sort_work` keys on `reactionCount` (misses it) and
//! `collect_filter_work` keys on filter fields (`sortAtUnix` is only a bucket
//! range field, not a real filter field, so it misses too). Only the
//! bucket-membership maintenance pass surfaces the entry.
//!
//! The bucket's `filter_field` (`sortAtUnix`) is deliberately NOT in
//! `filter_fields` — that's the prod reality and what makes the filter path
//! blind to the membership change.

use std::thread;
use std::time::Duration;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    BucketConfig, CacheConfig, Config, FilterFieldConfig, SortFieldConfig, TimeBucketFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{BitdexQuery, FilterClause, SortClause, SortDirection, Value};

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
            thread::sleep(Duration::from_millis(80));
            return;
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(engine.alive_count(), expected, "timed out waiting for alive count");
}

fn build_engine(async_maintenance: bool) -> (ConcurrentEngine, u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let config = Config {
        // NOTE: `sortAtUnix` (the bucket range field) is intentionally NOT a
        // filter field — only `category` is. This is what makes collect_filter_work
        // blind to bucket-membership changes.
        filter_fields: vec![FilterFieldConfig {
            name: "category".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        }],
        sort_fields: vec![
            // `sortAt` drives bucket membership (the manager's sort_field).
            SortFieldConfig {
                name: "sortAt".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: true,
                computed: None,
            },
            // `reactionCount` is what the cached entry sorts by.
            SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: true,
                computed: None,
            },
        ],
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAtUnix".to_string(),
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
            reconcile_scan_threads: 0,
        }),
        cache: CacheConfig {
            async_maintenance,
            bucket_entry_ttl_secs: 0, // TTL band-aid OFF — prove the live path works
            ..Default::default()
        },
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };
    (ConcurrentEngine::new(config).unwrap(), now)
}

/// The cached "top reactionCount, last 7d" query.
fn top_reactions_7d(now: u64) -> BitdexQuery {
    BitdexQuery {
        // Gte(sortAtUnix, now-7d) snaps to the 7d bucket.
        filters: vec![FilterClause::Gte(
            "sortAtUnix".to_string(),
            Value::Integer((now - 604_800) as i64),
        )],
        sort: Some(SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        }),
        limit: 3,
        cursor: None,
        offset: None,
        skip_cache: false,
    }
}

fn run_case(async_maintenance: bool) {
    let (engine, now) = build_engine(async_maintenance);

    // Baseline: 5 in-window slots (1..=5), reactionCount 10..50.
    for slot in 1..=5u32 {
        engine
            .put(
                slot,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer((now - 3600) as i64))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer((slot * 10) as i64)),
                    ),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }
    // Slot 100: high reactionCount (1000) but OUTSIDE the 7d window (~11.5 days old).
    engine
        .put(
            100,
            &make_doc(vec![
                ("sortAt", FieldValue::Single(Value::Integer((now - 1_000_000) as i64))),
                ("reactionCount", FieldValue::Single(Value::Integer(1000))),
                ("category", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();
    wait_for_alive(&engine, 6, 3000);

    // Populate the cache. Slot 100 is out of window → excluded despite its high
    // reactionCount. Top-3 = slots 5,4,3 (reactionCount 50,40,30).
    let q = top_reactions_7d(now);
    let r0 = engine.execute_query(&q).unwrap();
    assert!(
        !r0.ids.contains(&100),
        "[async={async_maintenance}] slot 100 is out of the 7d window — must not be cached yet (got {:?})",
        r0.ids
    );

    // Move slot 100 INTO the 7d window by updating ONLY sortAt. reactionCount and
    // category are unchanged, so the update emits only a `sortAt` sort mutation —
    // which surfaces NO entry sorted by reactionCount and NO filter field. Only
    // the bucket-membership pass can add it.
    engine
        .put(
            100,
            &make_doc(vec![
                ("sortAt", FieldValue::Single(Value::Integer((now - 3600) as i64))),
                ("reactionCount", FieldValue::Single(Value::Integer(1000))),
                ("category", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();
    // Give the flush + cache worker a couple cycles to apply maintenance.
    thread::sleep(Duration::from_millis(400));

    // Cached read must now include slot 100 at the top (reactionCount 1000),
    // WITHOUT a rebuild — proving the #274 add path fired.
    let r1 = engine.execute_query(&q).unwrap();
    assert!(
        r1.ids.contains(&100),
        "[async={async_maintenance}] slot 100 entered the 7d window via a sortAt change; \
         the cached reactionCount-sorted + bucket-filtered entry must gain it live (got {:?})",
        r1.ids
    );
    assert_eq!(
        r1.ids.first().copied(),
        Some(100),
        "[async={async_maintenance}] slot 100 has the highest reactionCount — it must rank first (got {:?})",
        r1.ids
    );
}

#[test]
fn bucket_membership_add_inline_path() {
    run_case(false);
}

#[test]
fn bucket_membership_add_async_worker_path() {
    run_case(true);
}
