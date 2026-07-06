//! Regression test for the cross-bucket over-removal bug introduced by the
//! bucket_cutoff-seeding fix (see bucket_window_slide_natural_aging.rs).
//!
//! `PendingBucketDiffs` is a single GLOBAL diff log shared by every
//! configured bucket (24h/7d/30d/1y in prod — here a narrow "20s" and a wide
//! "100s" bucket on the same field). Each bucket's periodic refresh pushes
//! its own expired-slot diff into the SAME struct, so
//! `PendingBucketDiffs::merged_expired()` is a cross-bucket UNION.
//!
//! Once `apply_bucket_diff` is reachable (which the bucket_cutoff fix makes
//! true), a naive `entry.bitmap -= merged_expired()` would wrongly strip
//! slots from a WIDE-window cache entry (e.g. "100s") the instant the
//! NARROW bucket's (e.g. "20s") periodic refresh expires them — even though
//! those slots are still comfortably inside the wide entry's own window.
//!
//! `ConcurrentEngine::own_bucket_live_bitmap` + the updated
//! `UnifiedEntry::apply_bucket_diff` close this by validating each removal
//! candidate against the LIVE ground-truth bitmap of the entry's OWN
//! bucket before removing it.

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

/// Two windows on the same field: "20s" (narrow, refreshes every 1s) and
/// "100s" (wide, refreshes every 1s too — so both advance on the same
/// cadence and any scoping bug shows up fast).
fn build_engine() -> (ConcurrentEngine, u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let config = Config {
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
            SortFieldConfig {
                name: "sortAt".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: true,
                computed: None,
            },
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
                    name: "20s".to_string(),
                    duration_secs: 20,
                    refresh_interval_secs: 1,
                },
                BucketConfig {
                    name: "100s".to_string(),
                    duration_secs: 100,
                    refresh_interval_secs: 1,
                },
            ],
            full_rebuild_interval_secs: 0,
        }),
        cache: CacheConfig {
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

/// Builds a "last `window_secs`, top reactionCount" query using the CURRENT
/// wall clock — not a fixed point captured once at test start. Time-bucket
/// snapping (`snap_range_clauses`) picks the bucket name from
/// `now_secs_at_query_time - ts`, so a query built from a stale `now` drifts
/// wider (and can snap to a DIFFERENT, larger bucket) the longer the test
/// runs. Real clients issuing a rolling "last 20s" query re-derive the
/// timestamp on every call for the same reason — mirror that here.
fn top_reactions(window_secs: u64) -> BitdexQuery {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    BitdexQuery {
        filters: vec![FilterClause::Gte(
            "sortAtUnix".to_string(),
            Value::Integer((now - window_secs) as i64),
        )],
        sort: Some(SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        }),
        limit: 10,
        cursor: None,
        offset: None,
        skip_cache: false,
    }
}

/// A slot inside the 100s window but past the 20s window must NOT be
/// stripped from the cached "100s" entry just because the 20s bucket's
/// periodic refresh expires it.
#[test]
fn wide_bucket_entry_keeps_slot_narrow_bucket_expires() {
    let (engine, now) = build_engine();

    // Baseline: low-reactionCount slots well within both windows.
    for slot in 1..=3u32 {
        engine
            .put(
                slot,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer((now - 5) as i64))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer((slot * 10) as i64)),
                    ),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }
    // Target: slot 100, sortAt 19s old — one second from aging out of the
    // 20s window, but comfortably inside the 100s window (81s of slack).
    // Highest reactionCount so it ranks top in both cached feeds.
    engine
        .put(
            100,
            &make_doc(vec![
                ("sortAt", FieldValue::Single(Value::Integer((now - 19) as i64))),
                ("reactionCount", FieldValue::Single(Value::Integer(1000))),
                ("category", FieldValue::Single(Value::Integer(1))),
            ]),
        )
        .unwrap();
    wait_for_alive(&engine, 4, 3000);

    // Populate both caches — slot 100 is in-window for both at t~0.
    let r20_0 = engine.execute_query(&top_reactions(20)).unwrap();
    let r100_0 = engine.execute_query(&top_reactions(100)).unwrap();
    assert!(r20_0.ids.contains(&100), "20s cache should include slot 100 initially (got {:?})", r20_0.ids);
    assert!(r100_0.ids.contains(&100), "100s cache should include slot 100 initially (got {:?})", r100_0.ids);

    // Let time pass so the 20s window slides past slot 100's sortAt (several
    // 1s periodic refreshes fire for BOTH buckets), while the 100s window
    // still has ~77s of slack left.
    thread::sleep(Duration::from_millis(4000));

    // The 20s cached entry must have shed slot 100 (this is the leak fixed
    // by bucket_window_slide_natural_aging.rs — sanity-check it still holds).
    let r20_1 = engine.execute_query(&top_reactions(20)).unwrap();
    assert!(
        !r20_1.ids.contains(&100),
        "20s cache must shed slot 100 once it ages past the 20s window (got {:?})",
        r20_1.ids
    );

    // The 100s cached entry must KEEP slot 100 — it is nowhere near the
    // 100s window edge. This is the over-removal regression: the 20s
    // bucket's periodic-refresh diff must not leak into the 100s entry's
    // removal set.
    let r100_1 = engine.execute_query(&top_reactions(100)).unwrap();
    assert!(
        r100_1.ids.contains(&100),
        "100s cache wrongly dropped slot 100 — cross-bucket over-removal \
         (20s bucket's expiry diff leaked into the 100s entry's removal \
         set). got {:?}",
        r100_1.ids
    );
    assert_eq!(
        r100_1.ids.first().copied(),
        Some(100),
        "slot 100 has the highest reactionCount — it must still rank first \
         in the 100s feed (got {:?})",
        r100_1.ids
    );
}
