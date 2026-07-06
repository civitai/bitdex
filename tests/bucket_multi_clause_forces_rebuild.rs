//! Regression test for a review gap in the per-bucket-name rework: a cache
//! entry that references MORE THAN ONE distinct time-bucket name (e.g. an
//! AND of two range clauses on the same bucket field —
//! `Gte(sortAtUnix, X) AND Gte(sortAtUnix, Y)`, syntactically reachable even
//! though no known Civitai query pattern constructs it) must never be served
//! stale. `ConcurrentEngine::resolve_bucket_diff_state`'s doc contract says
//! this case returns `BucketDiffState::Rebuild`, which every caller must
//! turn into `mark_for_rebuild()` — NOT a silent skip.
//!
//! Bug found in review: the read-path guard's `if let Some((candidates, ..))
//! = bucket_diff_state { .. }` had no `else`, so when the multi-bucket case
//! produced `None` (the pre-enum representation of "can't verify"), the
//! whole apply-or-rebuild block was skipped — no apply AND no
//! mark_for_rebuild. The entry was served as-is, stale until an unrelated
//! eviction, with no correctness backstop except an optional TTL.
//!
//! This proves the entry can never settle into a stable cache HIT: every
//! read forces a rebuild, which is externally observable as
//! `UnifiedCacheStats::inserts` incrementing on every single query call
//! instead of just the first (contrasted with a normal single-bucket query,
//! which inserts once and then hits on every subsequent call).

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

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn build_engine() -> ConcurrentEngine {
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
        // Two named windows on the same field — a query can independently
        // snap each of two Gte clauses to a different one of these.
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAtUnix".to_string(),
            sort_field: "sortAt".to_string(),
            range_buckets: vec![
                BucketConfig { name: "20s".to_string(), duration_secs: 20, refresh_interval_secs: 1 },
                BucketConfig { name: "100s".to_string(), duration_secs: 100, refresh_interval_secs: 1 },
            ],
            full_rebuild_interval_secs: 0,
        }),
        cache: CacheConfig {
            bucket_entry_ttl_secs: 0, // TTL backstop OFF — isolate the mark_for_rebuild path
            ..Default::default()
        },
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };
    ConcurrentEngine::new(config).unwrap()
}

/// Two Gte clauses on the SAME bucket field, ANDed (top-level flat list =
/// implicit AND — see `cache::canonicalize`). Each snaps independently via
/// `snap_range_clauses`, so this entry ends up referencing bucket names
/// "20s" AND "100s" simultaneously.
fn multi_bucket_query() -> BitdexQuery {
    let now = now_secs();
    BitdexQuery {
        filters: vec![
            FilterClause::Gte("sortAtUnix".to_string(), Value::Integer((now - 20) as i64)),
            FilterClause::Gte("sortAtUnix".to_string(), Value::Integer((now - 100) as i64)),
        ],
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

/// Control: a single-bucket-clause query on the same data/engine, to prove
/// the normal caching path DOES settle into a stable hit (inserts once,
/// then never again) — establishing that the multi-clause query's repeated
/// inserts are the anomaly under test, not a property of this engine/config
/// in general.
fn single_bucket_query() -> BitdexQuery {
    let now = now_secs();
    BitdexQuery {
        filters: vec![FilterClause::Gte("sortAtUnix".to_string(), Value::Integer((now - 20) as i64))],
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

#[test]
fn multi_bucket_clause_entry_never_settles_into_a_stable_hit() {
    let engine = build_engine();
    let now = now_secs();
    for slot in 1..=5u32 {
        engine
            .put(
                slot,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer((now - 5) as i64))),
                    ("reactionCount", FieldValue::Single(Value::Integer((slot * 10) as i64))),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
    }
    wait_for_alive(&engine, 5, 2000);

    // Control: single-bucket query settles into a stable hit — one insert
    // total across 3 calls.
    let inserts_before_control = engine.unified_cache_stats().inserts;
    for _ in 0..3 {
        let r = engine.execute_query(&single_bucket_query()).unwrap();
        assert!(!r.ids.is_empty(), "control query should return results");
    }
    let control_inserts = engine.unified_cache_stats().inserts - inserts_before_control;
    assert_eq!(
        control_inserts, 1,
        "single-bucket query must settle into a stable cache hit (exactly \
         1 insert across 3 calls) — got {control_inserts} inserts, \
         something else is forcing rebuilds in this test config",
    );

    // Under test: the multi-bucket-clause query must NOT settle into a
    // stable hit — every call should force a fresh insert (rebuild), never
    // silently serving a potentially-stale entry.
    let inserts_before = engine.unified_cache_stats().inserts;
    for i in 0..3 {
        let r = engine.execute_query(&multi_bucket_query()).unwrap();
        assert!(
            !r.ids.is_empty(),
            "multi-bucket-clause query should still return correct results \
             (call #{i}) — the fix is about staleness, not availability",
        );
    }
    let multi_inserts = engine.unified_cache_stats().inserts - inserts_before;
    assert_eq!(
        multi_inserts, 3,
        "a multi-bucket-clause entry must be rebuilt on every read (3 \
         inserts across 3 calls), never served as a stable cached hit — \
         got {multi_inserts} inserts. If this is < 3, the entry silently \
         settled into being served as-is despite BucketDiffState::Rebuild \
         (the bug: resolve_bucket_diff_state's contract not honored by a \
         caller).",
    );
}
