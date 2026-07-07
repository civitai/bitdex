//! Variant A (Ivanna) — CONCURRENT stress repro. The 5 deterministic variants
//! pass; the tb_arc store map is all-flush-thread (fallback race-safe). So the
//! source bug lives in WAL-reader-vs-flush-thread interleaving under continuous
//! load. This hammers per-row `set existedAt` bumps (within-window shifts) from
//! separate loader threads while the flush thread runs continuously, with a
//! short refresh_interval so the periodic subtract_expired interleaves the live
//! re-bucket. All seeded slots stay in-window the whole run → the 24h bucket
//! MUST retain every one (audit missing == 0). If missing > 0 → reproduced the
//! drain-split / interleave loss.

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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
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
            SortFieldConfig { name: "existedAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: true, computed: None },
            SortFieldConfig { name: "publishedAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: true, computed: None },
            SortFieldConfig {
                name: "sortAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32, eager_load: true,
                computed: Some(ComputedField { op: ComputedOp::Greatest, source_fields: vec!["existedAt".into(), "publishedAt".into()] }),
            },
        ],
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAtUnix".into(),
            sort_field: "sortAt".into(),
            range_buckets: vec![BucketConfig { name: "24h".into(), duration_secs: 86400, refresh_interval_secs: 1 }], // short → subtract_expired interleaves
            full_rebuild_interval_secs: 0,
        }),
        max_page_size: 100,
        flush_interval_us: 20,   // tiny → drains split mid-emit
        channel_capacity: 2_000, // modest → back-pressure/splits
        ..Default::default()
    }
}

fn meta_for(engine: &ConcurrentEngine) -> FieldMeta {
    FieldMeta::from_config(engine.config())
}

fn drain_batch(engine: &ConcurrentEngine, meta: &FieldMeta, batch: &mut Vec<EntityOps>) {
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let _ = apply_ops_batch(&mut sink, meta, batch, Some(engine), Some(&mut dw));
    dw.flush();
}

fn missing_24h(engine: &ConcurrentEngine) -> u64 {
    let a = engine.time_bucket_audit(0).expect("audit");
    a["buckets"]["24h"]["missing"].as_u64().unwrap_or(u64::MAX)
}
fn current_24h(engine: &ConcurrentEngine) -> u64 {
    let a = engine.time_bucket_audit(0).expect("audit");
    a["buckets"]["24h"]["current"].as_u64().unwrap_or(0)
}

#[test]
fn concurrent_existedat_bumps_keep_slots_bucketed() {
    let engine = Arc::new(ConcurrentEngine::new(build_config()).unwrap());
    let meta = meta_for(&engine);
    let now = now_secs();
    const K: i64 = 4000;

    // Seed K slots OUT of the 24h window (25h..48h ago) → NOT bucketed. Loaders
    // will bump them IN (out->in, the outlier case) under concurrency, and churn
    // some back OUT so aging + re-bucket race near the edge.
    for chunk_start in (1..=K).step_by(200) {
        let mut batch: Vec<EntityOps> = Vec::new();
        for slot in chunk_start..(chunk_start + 200).min(K + 1) {
            let existed = now - (25 * 3600 + (slot as u32 * 7) % (23 * 3600)); // 25h..48h ago → OUT
            batch.push(EntityOps {
                entity_id: slot,
                creates_slot: true,
                ops: vec![
                    Op::Set { field: "postId".into(), value: json!(slot % 50) },
                    Op::Set { field: "existedAt".into(), value: json!(existed as i64) },
                ],
            });
        }
        drain_batch(&engine, &meta, &mut batch);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && engine.alive_count() < K as u64 {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(engine.alive_count(), K as u64, "all seeded slots alive");
    thread::sleep(Duration::from_millis(300));

    // 8 loader threads: each bump moves the slot IN-window (1h..20h ago). Some
    // slots get repeatedly re-bumped; a fraction are pushed to just-inside the
    // edge (23h ago) so the interleaved subtract_expired can age them out and a
    // later bump must re-add them.
    let stop = Arc::new(AtomicBool::new(false));
    let ops_applied = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..8u32 {
        let eng = Arc::clone(&engine);
        let stopc = Arc::clone(&stop);
        let cnt = Arc::clone(&ops_applied);
        handles.push(thread::spawn(move || {
            let meta = meta_for(&eng);
            let mut seed = 0x9E3779B9u32 ^ (t.wrapping_mul(2654435761));
            while !stopc.load(Ordering::Relaxed) {
                seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
                let slot = 1 + (seed % K as u32) as i64;
                let n = now_secs();
                // mostly comfortably in-window; ~1/8 near the 24h edge (23h ago)
                let existed_new = if seed % 8 == 0 {
                    n - (23 * 3600 + (seed % 1800)) // just inside edge → age-out risk
                } else {
                    n - (3600 + (seed % (19 * 3600))) // 1h..20h ago → solidly in
                };
                let mut batch = vec![EntityOps {
                    entity_id: slot,
                    creates_slot: false,
                    ops: vec![Op::Set { field: "existedAt".into(), value: json!(existed_new as i64) }],
                }];
                drain_batch(&eng, &meta, &mut batch);
                cnt.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    thread::sleep(Duration::from_secs(10));
    stop.store(true, Ordering::Relaxed);
    for h in handles { let _ = h.join(); }
    // Drain the tail, then quiesce so the final sort layer settles.
    thread::sleep(Duration::from_millis(2000));

    let applied = ops_applied.load(Ordering::Relaxed);
    // Retry-read the audit a few times: any transient in-flight should settle;
    // a PERSISTENT missing>0 is the bug.
    let mut miss = missing_24h(&engine);
    let mut cur = current_24h(&engine);
    for _ in 0..5 {
        if miss == 0 { break; }
        thread::sleep(Duration::from_millis(400));
        miss = missing_24h(&engine);
        cur = current_24h(&engine);
    }
    eprintln!("[stress] applied {applied} existedAt bumps; 24h current={cur} missing={miss} (K={K})");
    assert_eq!(
        miss, 0,
        "REPRODUCED: {miss} slots in-window per sort layer but MISSING from 24h bucket after {applied} concurrent existedAt bumps (current={cur}, K={K})",
    );
}
