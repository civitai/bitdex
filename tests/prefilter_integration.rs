//! Integration tests for the prefilter registry.
//!
//! Verifies the end-to-end path:
//!   1. Register a prefilter with a clause subset.
//!   2. Run a query whose clauses are a superset.
//!   3. Confirm substitution fires (trace has `prefilter` set) and results
//!      are identical to the same query without the registered prefilter.

use std::thread;
use std::time::Duration;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{BitdexQuery, FilterClause, Value};

fn test_config(bitmap_path: &std::path::Path) -> Config {
    let mut config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: true,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "onSite".to_string(),
                field_type: FilterFieldType::Boolean,
                behaviors: None,
                eviction: None,
                eager_load: true,
                per_value_lazy: false,
            },
            FilterFieldConfig {
                name: "userId".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: true,
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
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };
    config.storage.bitmap_path = Some(bitmap_path.to_path_buf());
    config
}

fn doc(nsfw: i64, on_site: bool, user_id: i64, react: i64) -> Document {
    let mut fields: ahash::AHashMap<String, FieldValue> = ahash::AHashMap::new();
    fields.insert("nsfwLevel".into(), FieldValue::Single(Value::Integer(nsfw)));
    fields.insert("onSite".into(), FieldValue::Single(Value::Bool(on_site)));
    fields.insert("userId".into(), FieldValue::Single(Value::Integer(user_id)));
    fields.insert("reactionCount".into(), FieldValue::Single(Value::Integer(react)));
    Document { fields }
}

fn wait_for_alive(engine: &ConcurrentEngine, expected: u64, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if engine.alive_count() == expected {
            thread::sleep(Duration::from_millis(5));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for flush (alive={}, expected={})", engine.alive_count(), expected);
}

fn sorted_ids(q: BitdexQuery, engine: &ConcurrentEngine) -> Vec<i64> {
    let result = engine.execute_query_traced(&q, "test").unwrap().0;
    let mut ids = result.ids;
    ids.sort_unstable();
    ids
}

#[test]
fn prefilter_substitution_preserves_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = ConcurrentEngine::new_with_path(
        test_config(&dir.path().join("bitmaps")),
        &dir.path().join("docs"),
    ).unwrap();

    // 100 docs: nsfw in [1..5], onSite alternates, userId in [100..110], reactions 100..10000.
    for i in 1..=100u32 {
        let d = doc(
            (i as i64 % 5) + 1,
            i % 2 == 0,
            100 + (i as i64 % 11),
            i as i64 * 100,
        );
        engine.put(i, &d).unwrap();
    }
    wait_for_alive(&engine, 100, 2000);

    // Safety-prefix analog: nsfwLevel=1 AND onSite=true. Matches docs where
    // (i % 5)+1 == 1 AND i % 2 == 0  ->  (i % 5 == 0) AND (i % 2 == 0)
    let prefix = vec![
        FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
        FilterClause::Eq("onSite".into(), Value::Bool(true)),
    ];

    // Baseline: query without any registered prefilter.
    let baseline_query = BitdexQuery {
        filters: {
            let mut f = prefix.clone();
            f.push(FilterClause::Eq("userId".into(), Value::Integer(105)));
            f
        },
        sort: None,
        limit: 100,
        cursor: None,
        offset: None,
        skip_cache: true,
    };
    let baseline_ids = sorted_ids(baseline_query.clone(), &engine);

    // Register the prefilter.
    let entry = engine.register_prefilter(
        "safe".into(),
        prefix.clone(),
        300,
    ).unwrap();
    assert!(entry.cardinality() > 0, "safe prefilter should match at least one doc");

    // Run the same query — should substitute and return identical results.
    let (_result, trace) = engine.execute_query_traced(&baseline_query, "test").unwrap();
    assert_eq!(trace.prefilter.as_deref(), Some("safe"), "query should have substituted the safe prefilter");

    let substituted_ids = sorted_ids(baseline_query.clone(), &engine);
    assert_eq!(substituted_ids, baseline_ids, "substituted results must match baseline");

    // A query that does NOT match the full prefilter should not substitute.
    let partial_query = BitdexQuery {
        filters: vec![
            FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
            // onSite missing — so prefix isn't fully present.
            FilterClause::Eq("userId".into(), Value::Integer(105)),
        ],
        sort: None,
        limit: 100,
        cursor: None,
        offset: None,
        skip_cache: true,
    };
    let (_partial_result, partial_trace) = engine.execute_query_traced(&partial_query, "test").unwrap();
    assert!(partial_trace.prefilter.is_none(), "partial match should not substitute");

    // Unregister and re-run — substitution should no longer fire.
    assert!(engine.unregister_prefilter("safe"));
    let (_after_result, after_trace) = engine.execute_query_traced(&baseline_query, "test").unwrap();
    assert!(after_trace.prefilter.is_none(), "after unregister, no substitution");
    let after_ids = sorted_ids(baseline_query, &engine);
    assert_eq!(after_ids, baseline_ids, "results unchanged after unregister");
}

#[test]
fn deleted_docs_excluded_from_stale_prefilter() {
    // Engine uses clean-delete: filter bitmaps are kept clean and there is no
    // `alive AND filter` in the query hot path. Our cached prefilter bitmap is
    // NOT maintained by clean-delete — it's a frozen snapshot. Between
    // refreshes a deleted doc's bit stays set in the cached prefilter. Without
    // a guard, a query whose clause set matches the prefilter would resurface
    // deleted docs.
    //
    // substitute() intersects the prefilter with live alive at substitute
    // time. Verify end-to-end.
    let dir = tempfile::tempdir().unwrap();
    let engine = ConcurrentEngine::new_with_path(
        test_config(&dir.path().join("bitmaps")),
        &dir.path().join("docs"),
    ).unwrap();

    for i in 1..=10u32 {
        engine.put(i, &doc(1, true, 500, i as i64 * 10)).unwrap();
    }
    wait_for_alive(&engine, 10, 2000);

    let entry = engine.register_prefilter(
        "nsfw1".into(),
        vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
        300,
    ).unwrap();
    assert_eq!(entry.cardinality(), 10);

    // Delete 3/5/7 WITHOUT refreshing the prefilter.
    engine.delete(3).unwrap();
    engine.delete(5).unwrap();
    engine.delete(7).unwrap();
    wait_for_alive(&engine, 7, 2000);

    // Cached prefilter cardinality is still 10 — confirms the guard has to
    // handle stale bitmaps.
    assert_eq!(entry.cardinality(), 10, "prefilter bitmap NOT auto-refreshed");

    let q = BitdexQuery {
        filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
        sort: None,
        limit: 100,
        cursor: None,
        offset: None,
        skip_cache: true,
    };
    let (result, trace) = engine.execute_query_traced(&q, "test").unwrap();
    assert_eq!(trace.prefilter.as_deref(), Some("nsfw1"), "substitution should have fired");
    let mut ids = result.ids;
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 4, 6, 8, 9, 10], "deleted slots 3/5/7 must not leak through");
}

#[test]
fn refresh_updates_cardinality() {
    let dir = tempfile::tempdir().unwrap();
    let engine = ConcurrentEngine::new_with_path(
        test_config(&dir.path().join("bitmaps")),
        &dir.path().join("docs"),
    ).unwrap();

    // Insert 10 docs matching nsfw=1.
    for i in 1..=10u32 {
        engine.put(i, &doc(1, true, 100, i as i64)).unwrap();
    }
    wait_for_alive(&engine, 10, 2000);

    let entry = engine.register_prefilter(
        "nsfw1".into(),
        vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
        300,
    ).unwrap();
    assert_eq!(entry.cardinality(), 10);

    // Add 5 more matching docs.
    for i in 11..=15u32 {
        engine.put(i, &doc(1, true, 100, i as i64)).unwrap();
    }
    wait_for_alive(&engine, 15, 2000);

    // Cardinality on the existing entry is stale until refresh.
    assert_eq!(entry.cardinality(), 10, "before refresh, cardinality is stale");

    // Refresh should bring it current.
    let refreshed = engine.refresh_prefilter("nsfw1").unwrap().unwrap();
    assert_eq!(refreshed.cardinality(), 15, "after refresh, cardinality reflects new state");
}
