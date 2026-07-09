//! End-to-end repro for the schedule-fan-out collapse (2026-07-09): two Post
//! fan-outs for the same post in ONE batch (schedule action = Set publishedAt
//! then a second Post update carrying only availability) — the old queryOpSet
//! last-wins dedup discarded the publishedAt fan-out wholesale, so the post's
//! images never received publishedAt in doc OR bitmaps: the total per-post
//! publish no-op (~7% of scheduled posts; live specimens 29651562, 29651617,
//! 29651221, 29666515, 29666636, pre-go-live 29666669).

#![cfg(feature = "pg-sync")]

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    Config, DataSchema, DeferredAliveConfig, FieldMapping, FieldValueType, FilterFieldConfig,
    SortFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::ingester::{BitmapSink, CoalescerSink};
use bitdex_v2::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
use bitdex_v2::pg_sync::ops::{EntityOps, Op};
use serde_json::json;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IMAGE: u32 = 7;
const POST: i64 = 29_666_669;

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

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
    config.sort_fields = vec![SortFieldConfig {
        name: "publishedAt".into(),
        source_type: "uint32".into(),
        encoding: "linear".into(),
        bits: 32,
        eager_load: false,
        computed: None,
    }];
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
    config
}

fn wait_for_alive(engine: &ConcurrentEngine, slot: u32, max_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(max_ms);
    while Instant::now() < deadline {
        if engine.is_slot_alive(slot) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("slot {slot} never became alive");
}

fn insert_draft_image(engine: &ConcurrentEngine, meta: &FieldMeta) {
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = vec![EntityOps {
        entity_id: IMAGE as i64,
        creates_slot: true,
        ops: vec![Op::Set { field: "postId".into(), value: json!(POST) }],
    }];
    let (a, _, e) = apply_ops_batch(&mut sink, meta, &mut batch, Some(engine), Some(&mut dw));
    assert_eq!((a, e), (1, 0));
    BitmapSink::flush(&mut sink).unwrap();
    dw.flush();
    wait_for_alive(engine, IMAGE, 5_000);
}

fn schedule_shaped_batch(publish_value: i64) -> Vec<EntityOps> {
    // The schedule action's TWO Post-trigger rows, one WAL batch, same
    // entity + same query string — exactly what dedup_ops merges.
    vec![
        EntityOps {
            entity_id: POST,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {POST}")),
                ops: vec![Op::Set { field: "publishedAt".into(), value: json!(publish_value) }],
            }],
        },
        EntityOps {
            entity_id: POST,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {POST}")),
                ops: vec![Op::Set { field: "availability".into(), value: json!("Public") }],
            }],
        },
    ]
}

fn doc_published_at(engine: &ConcurrentEngine) -> Option<i64> {
    let doc = engine.get_document(IMAGE).unwrap().unwrap();
    match doc.fields.get("publishedAt") {
        Some(bitdex_v2::mutation::FieldValue::Single(bitdex_v2::types::Value::Integer(v))) => {
            Some(*v)
        }
        _ => None,
    }
}

/// Past-publishedAt variant (applies immediately): the image must receive
/// publishedAt despite the second fan-out in the same batch. Pre-fix:
/// dedup discarded the publishedAt fan-out wholesale.
#[test]
fn test_schedule_shaped_double_fanout_keeps_publish() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = prod_shaped_config();
    config.storage.bitmap_path = Some(dir.path().join("bitmaps"));
    let engine = ConcurrentEngine::new_with_path(config, &dir.path().join("docs")).unwrap();
    let meta = FieldMeta::from_config(engine.config());
    insert_draft_image(&engine, &meta);

    let t_pub = now_secs() - 100;
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = schedule_shaped_batch(t_pub);
    let (a, _, e) = apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    assert!(e == 0 && a >= 1, "fan-out batch must apply");
    BitmapSink::flush(&mut sink).unwrap();
    dw.flush();

    assert_eq!(
        doc_published_at(&engine),
        Some(t_pub),
        "publishedAt must survive the same-batch double fan-out (pre-fix: discarded by dedup)"
    );
}

/// Future-Tf variant (the real scheduled-post shape): the merged fan-out
/// must route the matched slot through the deferred branch — the doc gets
/// Tf (the exact pre-go-live predictor used in prod triage). Pre-fix:
/// nothing landed at all.
#[test]
fn test_schedule_shaped_double_fanout_defers_with_future_tf() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = prod_shaped_config();
    config.storage.bitmap_path = Some(dir.path().join("bitmaps"));
    let engine = ConcurrentEngine::new_with_path(config, &dir.path().join("docs")).unwrap();
    let meta = FieldMeta::from_config(engine.config());
    insert_draft_image(&engine, &meta);

    let t_f = now_secs() + 3_600;
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = schedule_shaped_batch(t_f);
    let (a, _, e) = apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    assert!(e == 0 && a >= 1, "fan-out batch must apply");
    BitmapSink::flush(&mut sink).unwrap();
    dw.flush();

    assert_eq!(
        doc_published_at(&engine),
        Some(t_f),
        "scheduled-post images must defer with doc.publishedAt=Tf (pre-fix: nothing landed)"
    );
}
