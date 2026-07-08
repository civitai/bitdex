//! Repro grid for the fan-out bitmap-apply miss (v1.1.30 live specimens
//! 136112167 / 136112697: fan-out doc writes land, bitmap mutations vanish,
//! isPublished stuck false + publishedAt sort layer 0, permanently).
//!
//! Prime suspect: a Post-publish fan-out whose publishedAt is (even 1s) in
//! the future of the pod clock at apply time takes the DEFERRED branch —
//! doc writes only, slots into the deferred map. activate_due later replays
//! `diff_document(None, doc)`, which is claimed (ops_processor deferred-
//! branch comment) to re-derive everything — but `diff_document` has NO
//! exists_boolean derivation, and the deferred branch deliberately skipped
//! writing `isPublished` into the doc. If the claim is false, activation
//! leaves the shadow false forever: the audit's Mode A signature, still
//! alive post-#291.

#![cfg(feature = "pg-sync")]

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    ComputedField, ComputedOp, Config, DataSchema, DeferredAliveConfig, FieldMapping,
    FieldValueType, FilterFieldConfig, SortFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::ingester::{BitmapSink, CoalescerSink};
use bitdex_v2::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
use bitdex_v2::pg_sync::ops::{EntityOps, Op};
use bitdex_v2::query::{BitdexQuery, FilterClause, SortClause, SortDirection, Value};
use serde_json::json;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IMAGE: u32 = 7;
const POST: i64 = 100;

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

/// Prod-shaped config: publishedAt sort field + deferred source, isPublished
/// exists_boolean shadow whose data_schema SOURCE is `publishedAtUnix`
/// (matching prod's config.yaml naming split), computed sortAt.
fn prod_shaped_config() -> Config {
    let mut config = Config::default();
    config.filter_fields = vec![
        FilterFieldConfig {
            name: "postId".into(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        },
        FilterFieldConfig {
            name: "isPublished".into(),
            field_type: FilterFieldType::Boolean,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        },
    ];
    config.sort_fields = vec![
        SortFieldConfig {
            name: "existedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        },
        SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        },
        SortFieldConfig {
            name: "sortAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: Some(ComputedField {
                op: ComputedOp::Greatest,
                source_fields: vec!["existedAt".into(), "publishedAt".into()],
            }),
        },
    ];
    config.deferred_alive = Some(DeferredAliveConfig {
        source_field: "publishedAt".into(),
        ms_to_seconds: false,
        sweep_interval_secs: 0,
        sweep_limit: 20_000,
    });
    config.data_schema = DataSchema {
        fields: vec![
            FieldMapping {
                source: "publishedAtUnix".into(),
                target: "publishedAt".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: true,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: false,
            },
            FieldMapping {
                source: "publishedAtUnix".into(),
                target: "isPublished".into(),
                value_type: FieldValueType::ExistsBoolean,
                fallback: None,
                string_map: None,
                doc_only: false,
                filter_only: false,
                ms_to_seconds: false,
                truncate_u32: false,
                case_sensitive: false,
                default_value: None,
                nullable: false,
            },
        ],
        ..Default::default()
    };
    config.flush_interval_us = 50;
    config.merge_interval_ms = 100;
    config
}

fn wait_alive(engine: &ConcurrentEngine, slot: u32, ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(ms);
    while Instant::now() < deadline {
        if engine.is_slot_alive(slot) {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("slot {slot} never alive");
}

fn query_ids(engine: &ConcurrentEngine, filters: Vec<FilterClause>) -> Vec<i64> {
    engine
        .execute_query(&BitdexQuery {
            filters,
            sort: None,
            limit: 100,
            cursor: None,
            offset: None,
            skip_cache: true,
        })
        .unwrap()
        .ids
}

fn published_sort_value(engine: &ConcurrentEngine, post: i64) -> u64 {
    let r = engine
        .execute_query(&BitdexQuery {
            filters: vec![FilterClause::Eq("postId".into(), Value::Integer(post))],
            sort: Some(SortClause {
                field: "publishedAt".into(),
                direction: SortDirection::Desc,
            }),
            limit: 1,
            cursor: None,
            offset: None,
            skip_cache: true,
        })
        .unwrap();
    r.cursor.map(|c| c.sort_value).unwrap_or(0)
}

/// The live-specimen shape: an ALIVE draft image; its post publishes with a
/// publishedAt a few seconds in the FUTURE of this process's clock (clock
/// skew / pipeline latency); the fan-out takes the deferred branch; after the
/// timestamp passes, activation must fully restore publish state — shadow
/// bitmap, publishedAt sort layer, and doc.
#[test]
fn fanout_future_publishedat_heals_after_activation() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine =
        ConcurrentEngine::new_with_path(prod_shaped_config(), &dir.path().join("docs")).unwrap();
    let meta = FieldMeta::from_config(engine.config());
    let now = now_secs();

    // 1. Draft image insert (post not yet published → publishedAt null).
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = vec![EntityOps {
        entity_id: IMAGE as i64,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(POST) },
            Op::Set { field: "existedAt".into(), value: json!(now - 300) },
            Op::Set { field: "publishedAt".into(), value: serde_json::Value::Null },
        ],
    }];
    let (a, _, e) = apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    assert_eq!((a, e), (1, 0));
    sink.flush().unwrap();
    dw.flush();
    wait_alive(&engine, IMAGE, 5_000);

    // 2. Post publishes; fan-out arrives with publishedAt 3s in OUR future.
    let t_pub = now_secs() + 3;
    let mut sink2 = CoalescerSink::new(engine.mutation_sender());
    let mut dw2 = DocWriter::new(engine.docstore_arc());
    let mut batch2 = vec![EntityOps {
        entity_id: POST,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST}")),
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(t_pub) }],
        }],
    }];
    let (a2, _, e2) =
        apply_ops_batch(&mut sink2, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
    assert_eq!((a2, e2), (1, 0), "fan-out must match and apply");
    sink2.flush().unwrap();
    dw2.flush();

    // Doc must carry publishedAt promptly (this half worked in prod).
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let doc = engine.get_document(IMAGE).unwrap().unwrap();
        if doc.fields.get("publishedAt").is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "doc write never landed");
        std::thread::sleep(Duration::from_millis(20));
    }

    // 3. Wait past t_pub + generous activation margin (flush cycle is ~50µs;
    //    give it 5s beyond the timestamp).
    let wait_until = t_pub + 5;
    while now_secs() < wait_until {
        std::thread::sleep(Duration::from_millis(100));
    }

    // 4. Activation must have restored FULL publish state.
    let pub_true = query_ids(
        &engine,
        vec![
            FilterClause::Eq("postId".into(), Value::Integer(POST)),
            FilterClause::Eq("isPublished".into(), Value::Bool(true)),
        ],
    );
    let layer = published_sort_value(&engine, POST);
    assert_eq!(
        layer, t_pub as u64,
        "publishedAt sort layer must be written by activation (got {layer})"
    );
    assert_eq!(
        pub_true,
        vec![IMAGE as i64],
        "isPublished shadow must flip true after activation — live-specimen bug if empty"
    );
}

/// Control: the same fan-out with a PAST publishedAt must apply immediately
/// through the normal path (this worked in prod for 29660803).
#[test]
fn fanout_past_publishedat_applies_immediately() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine =
        ConcurrentEngine::new_with_path(prod_shaped_config(), &dir.path().join("docs")).unwrap();
    let meta = FieldMeta::from_config(engine.config());
    let now = now_secs();

    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = vec![EntityOps {
        entity_id: IMAGE as i64,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(POST) },
            Op::Set { field: "existedAt".into(), value: json!(now - 300) },
        ],
    }];
    apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    sink.flush().unwrap();
    dw.flush();
    wait_alive(&engine, IMAGE, 5_000);

    let t_pub = now_secs() - 10;
    let mut sink2 = CoalescerSink::new(engine.mutation_sender());
    let mut dw2 = DocWriter::new(engine.docstore_arc());
    let mut batch2 = vec![EntityOps {
        entity_id: POST,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST}")),
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(t_pub) }],
        }],
    }];
    let (a2, _, e2) =
        apply_ops_batch(&mut sink2, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
    assert_eq!((a2, e2), (1, 0));
    sink2.flush().unwrap();
    dw2.flush();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let pub_true = query_ids(
            &engine,
            vec![
                FilterClause::Eq("postId".into(), Value::Integer(POST)),
                FilterClause::Eq("isPublished".into(), Value::Bool(true)),
            ],
        );
        if pub_true == vec![IMAGE as i64] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "past-publishedAt fan-out must flip isPublished promptly, got {pub_true:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(published_sort_value(&engine, POST), t_pub as u64);
}
