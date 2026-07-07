//! Variant #1 (Ivanna, deferred domain): mirror the FULL prod op sequence that
//! every missing slot shared, which Charlie's 3 passing variants lack:
//!   insert(existedAt) → PUBLISH FAN-OUT (QueryOpSet set publishedAt, past) →
//!   per-row `set existedAt` bump.
//! With `deferred_alive: publishedAt` configured (prod config) the existedAt-bump
//! recompute reads the STORED publishedAt via the deferred-gated source-read path
//! (ops_processor.rs:1207-1250). Charlie's variants never set publishedAt and had
//! no deferred_alive, so that branch never ran. This is the last untested gap.
//!
//! Prod: publishedAt < new existedAt, so sortAt = greatest(existedAt, publishedAt)
//! = existedAt both before and after — the bump is driven by existedAt, but
//! publishedAt is present in the stored doc and read during recompute.

#![cfg(feature = "pg-sync")]

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    BucketConfig, ComputedField, ComputedOp, Config, DeferredAliveConfig, FilterFieldConfig,
    SortFieldConfig, TimeBucketFieldConfig,
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

fn build_config_deferred() -> Config {
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
                eager_load: true,
                computed: Some(ComputedField {
                    op: ComputedOp::Greatest,
                    source_fields: vec!["existedAt".into(), "publishedAt".into()],
                }),
            },
        ],
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
        // prod config gate: the recompute's stored-doc source read is deferred_alive-gated.
        deferred_alive: Some(DeferredAliveConfig {
            source_field: "publishedAt".into(),
            ms_to_seconds: false,
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

fn wait_for_sortat(engine: &ConcurrentEngine, slot: u32, expected: u32, max_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(max_ms.max(5000));
    while Instant::now() < deadline {
        if read_sort_layer(engine, "sortAt", slot) == expected {
            thread::sleep(Duration::from_millis(50));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for sortAt={expected}; got {}", read_sort_layer(engine, "sortAt", slot));
}

/// Publish fan-out: set publishedAt on all images of a post, exactly like prod
/// (`queryOpSet "postId eq P" set publishedAt`).
fn publish_fanout(engine: &ConcurrentEngine, post_id: i64, published_at: u32) {
    let mut batch = vec![EntityOps {
        entity_id: post_id,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {post_id}")),
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(published_at as i64) }],
        }],
    }];
    drain_batch(engine, &mut batch);
}

/// out->in WITH publish fan-out + deferred_alive (full prod sequence, outlier case).
#[test]
fn deferred_source_update_out_to_in_buckets_slot() {
    let engine = ConcurrentEngine::new(build_config_deferred()).unwrap();
    let now = now_secs();
    let slot: i64 = 1;
    let post: i64 = 42;
    let existed_old = now - 2 * 86400; // 2d ago → out of 24h
    let published = now - 2 * 86400 - 100; // < existedAt, past → sortAt driven by existedAt
    let existed_new = now - 3600; // 1h ago → in 24h

    // 1) insert existedAt (alive immediately; publishedAt not yet set)
    let mut insert = vec![EntityOps {
        entity_id: slot,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(post) },
            Op::Set { field: "existedAt".into(), value: json!(existed_old as i64) },
        ],
    }];
    drain_batch(&engine, &mut insert);
    wait_for_alive(&engine, 1, 5000);

    // 2) publish fan-out sets publishedAt (past) — now stored for recompute to read
    publish_fanout(&engine, post, published);
    thread::sleep(Duration::from_millis(150));
    assert_eq!(read_sort_layer(&engine, "sortAt", slot as u32), existed_old, "sortAt=existedAt (>publishedAt)");
    assert_eq!(bucket_count(&engine, "24h"), 0, "out-of-window slot not bucketed");

    // 3) per-row existedAt bump into window — recompute reads STORED publishedAt (deferred-gated path)
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
        "BUG (deferred+fanout, out->in): existedAt bump moved sortAt in-window (layer={}) but slot never entered 24h bucket (count={})",
        read_sort_layer(&engine, "sortAt", slot as u32),
        n
    );
}

/// in->in WITH publish fan-out + deferred_alive (batch case).
#[test]
fn deferred_source_update_in_to_in_keeps_slot() {
    let engine = ConcurrentEngine::new(build_config_deferred()).unwrap();
    let now = now_secs();
    let slot: i64 = 1;
    let post: i64 = 42;
    let existed_old = now - 6 * 3600; // 6h ago → in 24h
    let published = now - 6 * 3600 - 50; // < existedAt, past
    let existed_new = now - 3 * 3600; // 3h ago → still in 24h

    let mut insert = vec![EntityOps {
        entity_id: slot,
        creates_slot: true,
        ops: vec![
            Op::Set { field: "postId".into(), value: json!(post) },
            Op::Set { field: "existedAt".into(), value: json!(existed_old as i64) },
        ],
    }];
    drain_batch(&engine, &mut insert);
    wait_for_alive(&engine, 1, 5000);
    publish_fanout(&engine, post, published);
    thread::sleep(Duration::from_millis(150));
    assert_eq!(bucket_count(&engine, "24h"), 1, "in-window slot bucketed at insert");

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
        "BUG (deferred+fanout, in->in): within-window sortAt shift dropped slot from 24h bucket (count={})",
        n
    );
}
