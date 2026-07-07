//! Repro: a per-row update to a COMPUTED-SORT SOURCE field (existedAt) that
//! shifts the computed sortAt into / within a time-bucket window updates the
//! sort LAYER but fails to re-bucket the slot via the live flush path.
//!
//! Prod evidence (2026-07-06, bitdex-0 v1.1.22 ?sample): the 24h bucket is
//! MISSING ~23% of in-window slots. All sampled missing slots' in-window event
//! was a plain per-row `set existedAt` (steady-state, CoalescerSink), not a
//! fan-out / insert / redump. Two variants seen:
//!   - out->in (outlier 115M): old existedAt (Dec 2025) → sortAt out of window;
//!     bump existedAt to ~now → sortAt in-window → should NEWLY enter bucket.
//!   - in->in (batch 135.8M): sortAt already in-window; existedAt bump shifts it
//!     to a DIFFERENT in-window value → should STAY bucketed.
//!
//! Both drive the ops path (apply_ops_batch + CoalescerSink → flush), NOT put()
//! (which uses mutation.rs resolve_computed_sort, a different computed path).

#![cfg(feature = "pg-sync")]

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    BucketConfig, ComputedField, ComputedOp, Config, FilterFieldConfig, SortFieldConfig,
    TimeBucketFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::ingester::CoalescerSink;
use bitdex_v2::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
use bitdex_v2::pg_sync::ops::{EntityOps, Op};
use serde_json::json;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn now_secs() -> u32 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as u32
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
                eager_load: true,
                computed: None,
            },
            SortFieldConfig {
                name: "publishedAt".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: true,
                computed: None,
            },
            SortFieldConfig {
                name: "sortAt".into(),
                source_type: "uint32".into(),
                encoding: "linear".into(),
                bits: 32,
                eager_load: true, // match prod: sortAt is eager → tb-block loaded branch
                computed: Some(ComputedField {
                    op: ComputedOp::Greatest,
                    source_fields: vec!["existedAt".into(), "publishedAt".into()],
                }),
            },
        ],
        // 24h bucket; large refresh + no full rebuild → isolate the LIVE path.
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAtUnix".into(),
            sort_field: "sortAt".into(),
            range_buckets: vec![BucketConfig {
                name: "24h".into(),
                duration_secs: 86400,
                refresh_interval_secs: 86400,
            }],
            full_rebuild_interval_secs: 0,
        }),
        max_page_size: 100,
        flush_interval_us: 50,
        channel_capacity: 10_000,
        ..Default::default()
    }
}

fn drain_batch(engine: &ConcurrentEngine, batch: &mut Vec<EntityOps>) {
    let meta = FieldMeta::from_config(engine.config());
    let sender = engine.mutation_sender();
    let mut sink = CoalescerSink::new(sender);
    let mut doc_writer = DocWriter::new(engine.docstore_arc());
    let _ = apply_ops_batch(&mut sink, &meta, batch, Some(engine), Some(&mut doc_writer));
    doc_writer.flush();
}

fn read_sort_layer(engine: &ConcurrentEngine, field: &str, slot: u32) -> u32 {
    let snap = engine.snapshot_public();
    let f = snap.sorts.get_field(field).unwrap_or_else(|| panic!("sort field {field} missing"));
    f.reconstruct_value(slot)
}

fn bucket_count(engine: &ConcurrentEngine, name: &str) -> u64 {
    engine
        .time_bucket_stats()
        .get(name)
        .and_then(|v| v.get("slots"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

fn wait_for_alive(engine: &ConcurrentEngine, expected: u64, max_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(max_ms.max(5000));
    while Instant::now() < deadline {
        if engine.alive_count() == expected {
            thread::sleep(Duration::from_millis(30));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out; alive_count={} expected={}", engine.alive_count(), expected);
}

/// Wait until the slot's reconstructed sortAt equals `expected` (i.e. the
/// update flush cycle has applied the sort-layer change), then a beat for the
/// bucket store in the same cycle.
fn wait_for_sortat(engine: &ConcurrentEngine, slot: u32, expected: u32, max_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(max_ms.max(5000));
    while Instant::now() < deadline {
        if read_sort_layer(engine, "sortAt", slot) == expected {
            thread::sleep(Duration::from_millis(50));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!(
        "timed out waiting for sortAt={expected}; got {}",
        read_sort_layer(engine, "sortAt", slot)
    );
}

/// Variant 1 — out->in: sortAt out of window, then an existedAt bump moves it
/// in-window. The slot should NEWLY enter the 24h bucket.
#[test]
fn source_update_out_to_in_window_buckets_slot() {
    let engine = ConcurrentEngine::new(build_config()).unwrap();
    let now = now_secs();
    let slot: i64 = 1;
    let existed_old = now - 2 * 86400; // 2 days ago → sortAt OUT of 24h
    let existed_new = now - 3600; // 1h ago → sortAt IN 24h

    let mut insert = vec![EntityOps {
        entity_id: slot,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(42) },
            Op::Set { field: "existedAt".into(), value: json!(existed_old as i64) },
        ],
    }];
    drain_batch(&engine, &mut insert);
    wait_for_alive(&engine, 1, 5000);
    assert_eq!(read_sort_layer(&engine, "sortAt", slot as u32), existed_old, "sortAt=existedAt at insert");
    assert_eq!(bucket_count(&engine, "24h"), 0, "out-of-window slot must NOT be in 24h bucket");

    // Plain per-row source-field update: bump existedAt into the window.
    let mut update = vec![EntityOps {
        entity_id: slot,
        creates_slot: false,
        ops: vec![
            Op::Remove { field: "existedAt".into(), value: json!(existed_old as i64) },
            Op::Set { field: "existedAt".into(), value: json!(existed_new as i64) },
        ],
    }];
    drain_batch(&engine, &mut update);
    wait_for_sortat(&engine, slot as u32, existed_new, 5000);

    let n = bucket_count(&engine, "24h");
    assert_eq!(
        n, 1,
        "BUG: source-field (existedAt) bump moved sortAt in-window (sort layer={}) but the slot never entered the 24h bucket (count={})",
        read_sort_layer(&engine, "sortAt", slot as u32),
        n
    );
}

/// Variant 2 — in->in: sortAt already in-window + bucketed, then an existedAt
/// bump shifts it to a DIFFERENT in-window value. The slot should STAY bucketed
/// (this is the "remove fires, re-insert doesn't" case).
#[test]
fn source_update_in_to_in_window_keeps_slot() {
    let engine = ConcurrentEngine::new(build_config()).unwrap();
    let now = now_secs();
    let slot: i64 = 1;
    let existed_old = now - 6 * 3600; // 6h ago → sortAt IN 24h
    let existed_new = now - 3 * 3600; // 3h ago → still IN 24h

    let mut insert = vec![EntityOps {
        entity_id: slot,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(42) },
            Op::Set { field: "existedAt".into(), value: json!(existed_old as i64) },
        ],
    }];
    drain_batch(&engine, &mut insert);
    wait_for_alive(&engine, 1, 5000);
    assert_eq!(bucket_count(&engine, "24h"), 1, "in-window slot must be in 24h bucket at insert");

    let mut update = vec![EntityOps {
        entity_id: slot,
        creates_slot: false,
        ops: vec![
            Op::Remove { field: "existedAt".into(), value: json!(existed_old as i64) },
            Op::Set { field: "existedAt".into(), value: json!(existed_new as i64) },
        ],
    }];
    drain_batch(&engine, &mut update);
    wait_for_sortat(&engine, slot as u32, existed_new, 5000);

    let n = bucket_count(&engine, "24h");
    assert_eq!(
        n, 1,
        "BUG: within-window sortAt shift dropped the slot from the 24h bucket (count={}) — remove fired, re-insert didn't",
        n
    );
}
