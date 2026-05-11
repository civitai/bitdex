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
    ComputedField, ComputedOp, Config, DeferredAliveConfig, FilterFieldConfig, SortFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::ingester::CoalescerSink;
use bitdex_v2::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
use bitdex_v2::pg_sync::ops::{EntityOps, Op};
use serde_json::json;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const POST_ID: i64 = 999;
const IMAGE_SLOT: i64 = 1;
const T_PAST: u32 = 1_700_000_000;       // existedAt
const T_FUTURE: u32 = 1_778_400_000;     // publishedAt scheduled 36h ahead
const T_REPUBLISH: u32 = 1_700_100_000;  // republished publishedAt

fn now_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

fn build_config_with_deferred() -> Config {
    let mut c = build_config();
    c.deferred_alive = Some(DeferredAliveConfig {
        source_field: "publishedAt".into(),
        ms_to_seconds: false,
    });
    c
}

fn wait_for_bitmap_publishedAt(
    engine: &ConcurrentEngine,
    slot: u32,
    expected: u32,
    timeout: Duration,
) -> u32 {
    let deadline = Instant::now() + timeout;
    let mut last = 0u32;
    while Instant::now() < deadline {
        last = read_sort_layer(engine, "publishedAt", slot);
        if last == expected {
            return last;
        }
        thread::sleep(Duration::from_millis(50));
    }
    last
}

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

// ---------------------------------------------------------------------------
// D3 — activate_due bakes stored doc into bitmap via diff_document's
//      fresh-insert path. That path is SET-ONLY for sort layers (see
//      src/mutation.rs:222-256) — it does not emit SortClear for 0-bits.
//      So if the slot already has bitmap state from a prior write (e.g.
//      an earlier publishedAt fan-out), activate_due OR-accumulates on
//      top of it instead of overwriting cleanly.
// ---------------------------------------------------------------------------

#[test]
fn d3_pure_activate_due_writes_bitmap_from_deferred_doc() {
    // Pure case: slot was never written via direct fan-out. Just deferred,
    // then activate_due fires. Bitmap should end up matching doc.
    let engine = ConcurrentEngine::new(build_config_with_deferred()).unwrap();

    let t_past = now_secs() - 1000;
    let t_future = now_secs() + 4; // ~4s out so activate_due can fire during test

    // Step 1: Image insert (alive, postId, existedAt — no publishedAt)
    let mut b1 = vec![EntityOps {
        entity_id: IMAGE_SLOT,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(POST_ID) },
            Op::Set { field: "existedAt".into(), value: json!(t_past as i64) },
        ],
    }];
    let _ = drain_batch(&engine, &mut b1);
    wait_for_flush(&engine, 1, 5000);

    // Step 2: Post fan-out with publishedAt = T_FUTURE → deferred branch
    let mut b2 = vec![EntityOps {
        entity_id: POST_ID,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST_ID}")),
            ops: vec![Op::Set {
                field: "publishedAt".into(),
                value: json!(t_future as i64),
            }],
        }],
    }];
    let _ = drain_batch(&engine, &mut b2);
    thread::sleep(Duration::from_millis(200));

    // Verify deferred state: bitmap untouched, doc updated
    let pub_before = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    let sort_before = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);
    eprintln!(
        "D3-pure pre-activate: bitmap.publishedAt={pub_before} bitmap.sortAt={sort_before} (deferred should leave bitmap=0/existedAt)"
    );
    assert_eq!(pub_before, 0, "deferred path must NOT write bitmap.publishedAt");
    assert_eq!(sort_before, t_past, "bitmap.sortAt should still equal existedAt");

    // Step 3: Wait for activate_due to fire (now passes t_future)
    let pub_after = wait_for_bitmap_publishedAt(&engine, IMAGE_SLOT as u32, t_future, Duration::from_secs(10));
    let sort_after = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);

    eprintln!(
        "D3-pure post-activate: bitmap.publishedAt={pub_after} bitmap.sortAt={sort_after} (expect publishedAt={t_future}, sortAt={})",
        t_past.max(t_future)
    );

    assert_eq!(
        pub_after, t_future,
        "activate_due should write bitmap.publishedAt from stored doc"
    );
    assert_eq!(
        sort_after,
        t_past.max(t_future),
        "activate_due should write bitmap.sortAt = max(existedAt, publishedAt)"
    );
}

#[test]
fn d3_activate_due_or_accumulates_over_prior_bitmap() {
    // Bug case: slot was written with publishedAt = T_OLD via a direct
    // (non-deferred) fan-out. Bitmap has T_OLD bits set. Later, a NEW
    // fan-out with publishedAt > now takes the deferred branch — doc
    // updates to T_FUTURE, bitmap untouched (still T_OLD). Time passes;
    // activate_due fires. diff_document fresh-insert path emits
    // SortSet for each 1-bit of T_FUTURE but NO SortClear for 0-bits.
    // Bits set in T_OLD but not in T_FUTURE leak through.
    //
    // Pick values where T_OLD has bits that T_FUTURE doesn't, so the
    // OR-accumulation is observable as bitmap > doc.

    let engine = ConcurrentEngine::new(build_config_with_deferred()).unwrap();

    let t_past_existed = now_secs() - 2000;

    // Pick T_OLD with bits OUTSIDE T_FUTURE so OR-accumulation is visible.
    // T_OLD must be < now (so the first fan-out is NOT deferred).
    // T_FUTURE must be > now (so the second fan-out IS deferred).
    let t_old: u32 = now_secs() - 100;     // recent past
    let t_future: u32 = now_secs() + 4;     // 4s out

    // Step 1: Image insert
    let mut b1 = vec![EntityOps {
        entity_id: IMAGE_SLOT,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(POST_ID) },
            Op::Set { field: "existedAt".into(), value: json!(t_past_existed as i64) },
        ],
    }];
    let _ = drain_batch(&engine, &mut b1);
    wait_for_flush(&engine, 1, 5000);

    // Step 2: First fan-out (NORMAL branch, T_OLD < now): writes
    // bitmap.publishedAt=T_OLD via process_set_op (full overwrite).
    let mut b2 = vec![EntityOps {
        entity_id: POST_ID,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST_ID}")),
            ops: vec![Op::Set {
                field: "publishedAt".into(),
                value: json!(t_old as i64),
            }],
        }],
    }];
    let _ = drain_batch(&engine, &mut b2);
    thread::sleep(Duration::from_millis(200));

    let pub_a = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    assert_eq!(pub_a, t_old, "first fan-out should set bitmap.publishedAt = T_OLD");

    // Step 3: Second fan-out (DEFERRED branch, T_FUTURE > now): writes
    // doc.publishedAt=T_FUTURE, schedules deferred_alive. Bitmap UNCHANGED.
    let mut b3 = vec![EntityOps {
        entity_id: POST_ID,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {POST_ID}")),
            ops: vec![Op::Set {
                field: "publishedAt".into(),
                value: json!(t_future as i64),
            }],
        }],
    }];
    let _ = drain_batch(&engine, &mut b3);
    thread::sleep(Duration::from_millis(200));

    let pub_b = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
    eprintln!("D3-accum after deferred reschedule: bitmap.publishedAt={pub_b} (still expected T_OLD={t_old})");
    assert_eq!(
        pub_b, t_old,
        "deferred fan-out must NOT write bitmap.publishedAt — bitmap stays at T_OLD"
    );

    // Step 4: Wait for activate_due to fire (now passes t_future).
    // activate_due calls diff_document fresh-insert; we expect it to
    // write bitmap.publishedAt=T_FUTURE. With the SET-ONLY bug, bits
    // unique to T_OLD remain set → bitmap = T_OLD | T_FUTURE > T_FUTURE.

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut final_pub = pub_b;
    while Instant::now() < deadline {
        final_pub = read_sort_layer(&engine, "publishedAt", IMAGE_SLOT as u32);
        if final_pub != t_old {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let final_sort = read_sort_layer(&engine, "sortAt", IMAGE_SLOT as u32);
    let or_accum = t_old | t_future;
    let expected_correct = t_future;

    eprintln!(
        "D3-accum post-activate: bitmap.publishedAt={final_pub} bitmap.sortAt={final_sort}"
    );
    eprintln!(
        "D3-accum expected if CORRECT: publishedAt={expected_correct} (full overwrite)"
    );
    eprintln!(
        "D3-accum expected if BUGGY: publishedAt={or_accum} = T_OLD({t_old}) | T_FUTURE({t_future}) — OR-accumulation"
    );

    // Doc has publishedAt = T_FUTURE (from step 3 deferred-branch doc-write).
    // Bitmap should match doc per the fix; with the bug it exceeds doc.
    if final_pub == or_accum && or_accum != t_future {
        panic!(
            "BUG D3 REPRODUCED: bitmap.publishedAt = {final_pub} (= T_OLD | T_FUTURE), \
             but doc.publishedAt = T_FUTURE = {t_future}. activate_due's diff_document \
             fresh-insert path is set-only at src/mutation.rs:222-256 and OR-accumulates \
             over prior bitmap state. bitmap > doc divergence confirmed."
        );
    }
    if final_pub != t_future {
        panic!(
            "BUG D3 VARIANT: bitmap.publishedAt = {final_pub}, expected {t_future} \
             (full overwrite from current doc). doc=T_FUTURE, bitmap diverges."
        );
    }
}
