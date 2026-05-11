//! Repro for sortAt steady-state bug + cross-pod divergence.
//!
//! Source: docs/_in/sortAt-steady-state-bug-handoff-2026-05-08.md
//!
//! Three theories:
//!   T1 (Bug B + part of Bug A): apply_query_op_set runs `execute_query`
//!      against the ArcSwap-published snapshot. Same-batch CoalescerSink
//!      writes (e.g. Image's postId filter) are buffered, not yet visible
//!      to the fan-out's query → fan-out matches 0 slots → recompute
//!      never fires for the in-batch slot.
//!
//!   T2 (Bug A primary): recompute_computed_sorts_for_slot falls back to
//!      `engine.get_document(slot)` for source fields not present in the
//!      ops batch. If a prior batch wrote a stale derived field (e.g. via
//!      the deferred-alive doc-write at ops_processor.rs:1469-1496),
//!      recompute reads the stale value, GREATEST returns T_future, and
//!      bitmap stays at T_future even after the doc field is later
//!      overwritten with T_past.
//!
//!   T3 (dump-time): dump_processor writes sortAt = max(existedAt,
//!      publishedAt) bit-by-bit with no `< now` filter — scheduled-future
//!      posts seed the bitmap with T_future at dump time. Re-publish
//!      relies on the steady-state recompute hook to clear the stale bits.

#![cfg(feature = "pg-sync")]

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    ComputedField, ComputedOp, Config, FilterFieldConfig, SortFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::ingester::CoalescerSink;
use bitdex_v2::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
use bitdex_v2::pg_sync::ops::{EntityOps, Op};
use serde_json::json;
use std::thread;
use std::time::{Duration, Instant};

const POST_ID: i64 = 999;
const IMAGE_SLOT: i64 = 1;
const T_PAST: u32 = 1_700_000_000;       // existedAt
const T_FUTURE: u32 = 1_778_400_000;     // publishedAt scheduled 36h ahead
const T_REPUBLISH: u32 = 1_700_100_000;  // republished publishedAt

fn build_config() -> Config {
    Config {
        filter_fields: vec![FilterFieldConfig {
            name: "postId".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        }],
        sort_fields: vec![
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
        ],
        max_page_size: 100,
        flush_interval_us: 50,
        channel_capacity: 10_000,
        ..Default::default()
    }
}

fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(max_ms.max(5000));
    while Instant::now() < deadline {
        if engine.alive_count() == expected_alive {
            thread::sleep(Duration::from_millis(20));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!(
        "timed out waiting for flush; alive_count={} expected={}",
        engine.alive_count(),
        expected_alive
    );
}

fn drain_batch(engine: &ConcurrentEngine, batch: &mut Vec<EntityOps>) -> (usize, usize, usize) {
    let meta = FieldMeta::from_config(engine.config());
    let sender = engine.mutation_sender();
    let mut sink = CoalescerSink::new(sender);
    let mut doc_writer = DocWriter::new(engine.docstore_arc());
    let result = apply_ops_batch(&mut sink, &meta, batch, Some(engine), Some(&mut doc_writer));
    doc_writer.flush();
    result
}

fn read_sort_layer(engine: &ConcurrentEngine, field: &str, slot: u32) -> u32 {
    let snap = engine.snapshot_public();
    let f = snap
        .sorts
        .get_field(field)
        .unwrap_or_else(|| panic!("sort field {field} missing"));
    f.reconstruct_value(slot)
}

// ---------------------------------------------------------------------------
// T1 — Same-batch fan-out race (Bug B core, also seeds Bug A)
// ---------------------------------------------------------------------------

#[test]
fn t1_fanout_matches_image_in_same_batch() {
    let engine = ConcurrentEngine::new(build_config()).unwrap();

    let mut batch = vec![
        EntityOps {
            entity_id: IMAGE_SLOT,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(POST_ID) },
                Op::Set { field: "existedAt".into(), value: json!(T_PAST as i64) },
            ],
        },
        EntityOps {
            entity_id: POST_ID,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {POST_ID}")),
                ops: vec![Op::Set {
                    field: "publishedAt".into(),
                    value: json!(T_REPUBLISH as i64),
                }],
            }],
        },
    ];

    let (applied, _, errors) = drain_batch(&engine, &mut batch);
    assert!(applied >= 1, "at least one entity should apply");
    assert_eq!(errors, 0, "no errors expected");

    wait_for_flush(&engine, 1, 5000);

    let pub_at = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    let sort_at = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);

    eprintln!("T1: bitmap.publishedAt={pub_at} (expected {T_REPUBLISH})");
    eprintln!("T1: bitmap.sortAt={sort_at} (expected max({T_PAST},{T_REPUBLISH})={})", T_PAST.max(T_REPUBLISH));

    if pub_at == 0 {
        panic!(
            "BUG B REPRODUCED: fan-out missed in-batch Image. publishedAt bitmap=0 \
             (expected {T_REPUBLISH}). Theory T1 holds: apply_query_op_set's \
             execute_query is blind to same-batch CoalescerSink writes."
        );
    }
    assert_eq!(pub_at, T_REPUBLISH);
    assert_eq!(sort_at, T_PAST.max(T_REPUBLISH));
}

// ---------------------------------------------------------------------------
// T1b — Cross-batch fan-out (sanity baseline; should pass even with bug)
// ---------------------------------------------------------------------------

#[test]
fn t1b_fanout_matches_image_in_separate_batch() {
    let engine = ConcurrentEngine::new(build_config()).unwrap();

    let mut b1 = vec![EntityOps {
        entity_id: IMAGE_SLOT,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(POST_ID) },
            Op::Set { field: "existedAt".into(), value: json!(T_PAST as i64) },
        ],
    }];
    let _ = drain_batch(&engine, &mut b1);
    wait_for_flush(&engine, 1, 5000);

    let mut b2 = vec![EntityOps {
        entity_id: POST_ID,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST_ID}")),
            ops: vec![Op::Set {
                field: "publishedAt".into(),
                value: json!(T_REPUBLISH as i64),
            }],
        }],
    }];
    let (_, _, errors) = drain_batch(&engine, &mut b2);
    assert_eq!(errors, 0);

    thread::sleep(Duration::from_millis(150));

    let pub_at = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    let sort_at = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);

    eprintln!("T1b: bitmap.publishedAt={pub_at} (expected {T_REPUBLISH})");
    eprintln!("T1b: bitmap.sortAt={sort_at} (expected {})", T_PAST.max(T_REPUBLISH));

    assert_eq!(pub_at, T_REPUBLISH, "cross-batch fan-out must hit Image");
    assert_eq!(sort_at, T_PAST.max(T_REPUBLISH));
}

// ---------------------------------------------------------------------------
// T2 — Stuck T_future bitmap after republish (Bug A core)
// ---------------------------------------------------------------------------
//
// 1. Image inserted with postId=P, existedAt=T_PAST. (separate batch flush)
// 2. Post fan-out delivers publishedAt=T_FUTURE (scheduled). bitmap = T_FUTURE.
// 3. Post fan-out delivers publishedAt=T_REPUBLISH (re-published in past).
//    Expected: bitmap.sortAt = max(T_PAST, T_REPUBLISH) = T_REPUBLISH.
//    Bug: bitmap.sortAt stuck at T_FUTURE despite doc fields all in past.

#[test]
fn t2_stuck_t_future_after_republish() {
    let engine = ConcurrentEngine::new(build_config()).unwrap();

    // Batch 1: Image creates slot.
    let mut b1 = vec![EntityOps {
        entity_id: IMAGE_SLOT,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(POST_ID) },
            Op::Set { field: "existedAt".into(), value: json!(T_PAST as i64) },
        ],
    }];
    let _ = drain_batch(&engine, &mut b1);
    wait_for_flush(&engine, 1, 5000);

    // Batch 2: Post fan-out → schedules publishedAt=T_FUTURE.
    let mut b2 = vec![EntityOps {
        entity_id: POST_ID,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST_ID}")),
            ops: vec![Op::Set {
                field: "publishedAt".into(),
                value: json!(T_FUTURE as i64),
            }],
        }],
    }];
    let _ = drain_batch(&engine, &mut b2);
    thread::sleep(Duration::from_millis(150));

    let after_schedule_pub = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    let after_schedule_sort = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);
    eprintln!(
        "T2 after schedule: publishedAt={after_schedule_pub} sortAt={after_schedule_sort} (expected T_FUTURE={T_FUTURE})"
    );

    // Batch 3: Post re-publishes with publishedAt=T_REPUBLISH (in the past).
    let mut b3 = vec![EntityOps {
        entity_id: POST_ID,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST_ID}")),
            ops: vec![Op::Set {
                field: "publishedAt".into(),
                value: json!(T_REPUBLISH as i64),
            }],
        }],
    }];
    let _ = drain_batch(&engine, &mut b3);
    thread::sleep(Duration::from_millis(150));

    let pub_at = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    let sort_at = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);
    let expected_sort = T_PAST.max(T_REPUBLISH);

    eprintln!("T2 final: publishedAt={pub_at} (expected {T_REPUBLISH})");
    eprintln!("T2 final: sortAt={sort_at} (expected max({T_PAST},{T_REPUBLISH})={expected_sort})");

    assert_eq!(pub_at, T_REPUBLISH, "publishedAt bitmap must converge to republish value");

    if sort_at == T_FUTURE {
        panic!(
            "BUG A REPRODUCED: sortAt bitmap stuck at T_FUTURE={T_FUTURE} after \
             publishedAt updated to T_REPUBLISH={T_REPUBLISH}. Recompute did not \
             re-derive sortAt from current sources, or read a stale stored doc."
        );
    }
    if sort_at != expected_sort {
        panic!(
            "BUG A VARIANT: sortAt bitmap = {sort_at}, expected {expected_sort} \
             (max of existedAt={T_PAST} and publishedAt={T_REPUBLISH})"
        );
    }
}

// ---------------------------------------------------------------------------
// T2b — Same as T2 but all in one big batch (exercises fan-out → fan-out chain)
// ---------------------------------------------------------------------------

#[test]
fn t2b_stuck_t_future_in_single_batch() {
    let engine = ConcurrentEngine::new(build_config()).unwrap();

    let mut b = vec![
        EntityOps {
            entity_id: IMAGE_SLOT,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(POST_ID) },
                Op::Set { field: "existedAt".into(), value: json!(T_PAST as i64) },
            ],
        },
        EntityOps {
            entity_id: POST_ID,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {POST_ID}")),
                ops: vec![Op::Set {
                    field: "publishedAt".into(),
                    value: json!(T_FUTURE as i64),
                }],
            }],
        },
        EntityOps {
            entity_id: POST_ID,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {POST_ID}")),
                ops: vec![Op::Set {
                    field: "publishedAt".into(),
                    value: json!(T_REPUBLISH as i64),
                }],
            }],
        },
    ];
    let _ = drain_batch(&engine, &mut b);
    wait_for_flush(&engine, 1, 5000);
    thread::sleep(Duration::from_millis(150));

    let pub_at = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    let sort_at = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);
    let expected_sort = T_PAST.max(T_REPUBLISH);

    eprintln!("T2b final: publishedAt={pub_at} sortAt={sort_at} (expected sort {expected_sort})");

    // We only assert sortAt convergence; publishedAt may also reveal Bug B
    // if the dedup'd ops fail to write under in-batch fan-out blindness.
    if sort_at == T_FUTURE {
        panic!("BUG A T2b REPRODUCED: sortAt stuck at T_FUTURE in single-batch sequence");
    }
    if sort_at != expected_sort {
        panic!("BUG A T2b VARIANT: sortAt={sort_at} expected {expected_sort}");
    }
}
