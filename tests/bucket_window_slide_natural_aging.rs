//! Regression test for the 2026-07-02 prod leak: a bucket-filtered,
//! non-bucket-sorted cache entry (`nsfwLevel==1 AND Gte(sortAtUnix, now-24h)`,
//! sort `reactionCount` Desc) kept returning slots whose `sortAt` had aged
//! *naturally* out of the window — no mutation involved, just time passing.
//! `skip_cache=true` on the same query returned 0 violators, proving the
//! ground truth (bucket bitmap) was correct and the leak was cache-only.
//!
//! Root cause: `UnifiedEntry::bucket_cutoff` is meant to hold the same
//! *snapped* cutoff scale as `PendingBucketDiffs` (`snap(now - duration,
//! refresh_interval)` — see `time_buckets.rs::TimeBucket::last_cutoff`). But
//! every place that seeds a freshly-created/-restored entry's `bucket_cutoff`
//! stamped raw wall-clock `now()` instead. Since `duration_secs` is always
//! positive (hours to a year in the real config), a freshly seeded
//! `bucket_cutoff` (~now) is *always* greater than `pending.current_cutoff()`
//! (~now - duration_secs). The read-path condition
//! `entry.bucket_cutoff() < pending.current_cutoff()` in
//! `ConcurrentEngine::execute_query{,_traced}` is therefore false for the
//! entry's entire life (until `duration_secs` of wall-clock time passes) —
//! `apply_bucket_diff` never fires, and every periodic-refresh window-slide
//! diff is silently ignored. Hot entries (read constantly, so never evicted)
//! leak forever; this is exactly the "actively-read entries never self-heal"
//! symptom seen in prod.
//!
//! Uses a short window (`duration_secs`) so the self-heal boundary (bug masks
//! itself once wall-clock time exceeds `duration_secs` past entry creation)
//! is far outside the test's timeframe.

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

/// 20s window, 1s refresh interval — several periodic refresh cycles happen
/// within a couple of wall-clock seconds, well before the 20s self-heal point.
fn build_engine() -> (ConcurrentEngine, u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let config = Config {
        // `sortAtUnix` (the bucket range field) is intentionally NOT a filter
        // field — matches prod, and rules out the filter-field path masking
        // the bug via an unrelated mechanism.
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
            range_buckets: vec![BucketConfig {
                name: "20s".to_string(),
                duration_secs: 20,
                refresh_interval_secs: 1,
            }],
            full_rebuild_interval_secs: 0,
            reconcile_scan_threads: 0,
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

fn top_reactions_20s(now: u64) -> BitdexQuery {
    BitdexQuery {
        filters: vec![FilterClause::Gte(
            "sortAtUnix".to_string(),
            Value::Integer((now - 20) as i64),
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

/// FAILS on current main: a slot whose `sortAt` naturally ages past the
/// window edge (no mutation, no aliveness change) leaks into a hot,
/// bucket-filtered + reactionCount-sorted cache entry forever. Ground truth
/// (skip_cache=true) is correct throughout — the leak is cache-only.
#[test]
fn bucket_entry_sheds_naturally_aged_out_slot() {
    let (engine, now) = build_engine();

    // Baseline: low-reactionCount slots comfortably inside the window.
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
    // Target: slot 100, sortAt 19s old — one second from aging out of the 20s
    // window — with the HIGHEST reactionCount so it ranks top-of-feed in the
    // cached, hot query. Never mutated again.
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

    let q = top_reactions_20s(now);

    // Populate + confirm the cache: slot 100 is still in-window at t~0.
    let r0 = engine.execute_query(&q).unwrap();
    assert!(
        r0.ids.contains(&100),
        "slot 100 is inside the 20s window at query time — must be cached (got {:?})",
        r0.ids
    );
    assert_eq!(r0.ids.first().copied(), Some(100));

    // Let wall-clock time pass so the window slides past slot 100's sortAt.
    // Several periodic refresh cycles (1s interval) run in this span, well
    // short of the 20s self-heal boundary.
    thread::sleep(Duration::from_millis(4000));

    // Ground truth: slot 100 has aged out. skip_cache=true must not return it.
    let mut ground_truth = top_reactions_20s(now);
    ground_truth.skip_cache = true;
    let rt = engine.execute_query(&ground_truth).unwrap();
    assert!(
        !rt.ids.contains(&100),
        "ground truth (skip_cache=true) must exclude slot 100 once its sortAt \
         ages past the 20s window (got {:?})",
        rt.ids
    );

    // Cache: repeated hot reads of the SAME query must self-heal via the
    // read-path window-slide diff — no rebuild required. This is what fails
    // on current main: bucket_cutoff was seeded with wall-clock now() instead
    // of the snapped cutoff, so the diff-apply condition never fires and the
    // stale slot leaks into every subsequent cache hit.
    for _ in 0..3 {
        let r = engine.execute_query(&q).unwrap();
        assert!(
            !r.ids.contains(&100),
            "cached (skip_cache=false) query still returns slot 100 after it \
             aged out of the 20s window — window-slide removal leak (got {:?})",
            r.ids
        );
    }
}
