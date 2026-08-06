//! Liveness proof for the page-1 divergence canary (#336 follow-up).
//!
//! The canary's divergence counter only moves when the cache is WRONG, so a
//! quiet counter is ambiguous: "sampled and clean" and "canary disabled" look
//! identical from Prometheus. (This bit us in prod: the sample rate was set by
//! runtime PATCH only, both pods restarted, and the canary silently died while
//! a 48h "zero divergences" reading was taken at face value.)
//!
//! `bitdex_cache_page1_canary_samples_total` disambiguates: it increments for
//! every sampled first-page check, diverged or not. This test proves both
//! directions:
//!   - rate > 0: cached first-page bucket queries increment samples while the
//!     divergence counter stays 0 (cache agrees with re-derivation),
//!   - rate = 0 (the serde default): samples stay 0 — the exact silent-death
//!     signature the counter exists to expose.
#![cfg(feature = "server")]

use std::thread;
use std::time::Duration;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    BucketConfig, CacheConfig, Config, FilterFieldConfig, SortFieldConfig, TimeBucketFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::metrics::Metrics;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{BitdexQuery, FilterClause, SortClause, SortDirection, Value};

fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
    Document {
        fields: fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
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

fn build_engine(sample_rate: f64) -> (ConcurrentEngine, Metrics, u64) {
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
            range_buckets: vec![BucketConfig {
                name: "7d".to_string(),
                duration_secs: 604800,
                refresh_interval_secs: 86400,
            }],
            full_rebuild_interval_secs: 0,
            reconcile_scan_threads: 0,
        }),
        cache: CacheConfig {
            page1_canary_sample_rate: sample_rate,
            ..Default::default()
        },
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };
    let engine = ConcurrentEngine::new(config).unwrap();
    let metrics = Metrics::new();
    engine.set_metrics_bridge(metrics.engine_bridge("test".to_string()));
    (engine, metrics, now)
}

fn top_reactions_7d(now: u64) -> BitdexQuery {
    BitdexQuery {
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

fn seed_and_query(engine: &ConcurrentEngine, now: u64, cached_reads: u64) {
    for slot in 1..=5u32 {
        engine
            .put(
                slot,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer((now - 3600) as i64))),
                    ("reactionCount", FieldValue::Single(Value::Integer((slot * 10) as i64))),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }
    wait_for_alive(engine, 5, 3000);

    let q = top_reactions_7d(now);
    // First execution populates the cache (miss — canary only samples hits).
    engine.execute_query(&q).unwrap();
    // Subsequent executions are first-page cache hits — canary territory.
    for _ in 0..cached_reads {
        engine.execute_query(&q).unwrap();
    }
}

fn counter_value(metrics: &Metrics, name: &str) -> u64 {
    metrics
        .gather()
        .lines()
        .filter(|l| l.starts_with(name) && !l.starts_with('#'))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum::<f64>() as u64
}

#[test]
fn canary_samples_counter_proves_liveness_at_rate_1() {
    let (engine, metrics, now) = build_engine(1.0);
    seed_and_query(&engine, now, 4);

    let samples = counter_value(&metrics, "bitdex_cache_page1_canary_samples_total");
    let divergence = counter_value(&metrics, "bitdex_cache_page1_divergence_total");
    assert!(
        samples >= 4,
        "rate=1.0 with 4 cached first-page reads must record >=4 samples, got {samples}"
    );
    assert_eq!(
        divergence, 0,
        "cache agrees with re-derivation — divergence must stay 0, got {divergence}"
    );
}

#[test]
fn canary_samples_counter_exposes_disabled_canary() {
    // Serde default: rate 0.0. This is what every pod restart reverts to unless
    // the rate is seeded in the index config — the silent-death signature.
    let (engine, metrics, now) = build_engine(0.0);
    seed_and_query(&engine, now, 4);

    let samples = counter_value(&metrics, "bitdex_cache_page1_canary_samples_total");
    assert_eq!(
        samples, 0,
        "rate=0.0 must record zero samples — a zero samples rate under first-page \
         traffic is the alert signature for a disabled canary, got {samples}"
    );
}
