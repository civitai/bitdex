//! Repro for the fan-out silent no-op on freshly-dumped pods
//! (FOLLOWUP.md "Fan-out silent no-op on fresh pods", specimen 136063341).
//!
//! Hypothesis under test (lazy-shadow): a per_value_lazy filter value created
//! ONLY via steady-state sync after a restore (e.g. a new post's postId)
//! exists as an in-memory diff on an unloaded VersionedBitmap; when a query
//! triggers the per-value lazy load and the value has NO disk shard entry,
//! the load path may shadow or strand the diff → Eq(field, new_value)
//! matches zero → the Post-publish fan-out silently applies nothing.
//!
//! Same family as the collectionIds shadowing gotcha ("sync-created
//! VersionedBitmap entries shadow bulk-loaded disk data on lazy load").

#![cfg(feature = "pg-sync")]

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::ingester::{BitmapSink, CoalescerSink};
use bitdex_v2::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
use bitdex_v2::pg_sync::ops::{EntityOps, Op};
use bitdex_v2::query::{FilterClause, Value};
use serde_json::json;
use std::time::{Duration, Instant};

const BASELINE_POST: i64 = 100;
const NEW_POST: i64 = 29_651_893; // sync-created after the "dump"
const BASELINE_IMAGE: u32 = 1;
const NEW_IMAGE: u32 = 42;

fn build_config(bitmap_path: &std::path::Path) -> Config {
    let mut config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "postId".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: true, // prod postId shape (22M+ values)
            max_range_scan_values: None,
        }],
        sort_fields: vec![SortFieldConfig {
            name: "publishedAt".into(),
            source_type: "uint32".into(),
            encoding: "linear".into(),
            bits: 32,
            eager_load: false,
            computed: None,
        }],
        max_page_size: 100,
        flush_interval_us: 50,
        merge_interval_ms: 50,
        channel_capacity: 10_000,
        ..Default::default()
    };
    config.storage.bitmap_path = Some(bitmap_path.to_path_buf());
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
    panic!("slot {slot} never became alive within {max_ms}ms");
}

/// Build a persisted baseline (the "dump"), drop the engine, and reopen so
/// postId is in the per-value lazy state with its existence set built from
/// disk. Returns the reopened engine.
fn reopened_engine(dir: &std::path::Path) -> ConcurrentEngine {
    let bitmap_path = dir.join("bitmaps");
    let docstore_path = dir.join("docs");
    {
        let engine = ConcurrentEngine::new_with_path(
            build_config(&bitmap_path),
            docstore_path.as_path(),
        )
        .unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let mut sink = CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let mut batch = vec![EntityOps {
            entity_id: BASELINE_IMAGE as i64,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(BASELINE_POST) }],
        }];
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        sink.flush().unwrap();
        dw.flush();
        wait_for_alive(&engine, BASELINE_IMAGE, 5_000);
        // save_snapshot can transiently collide with the merge thread's own
        // persistence cycle (tmp-file TOCTOU → os error 2) — retry; rig
        // noise, not the behavior under test.
        let mut last_err = None;
        let mut saved = false;
        for _ in 0..5 {
            match engine.save_snapshot() {
                Ok(()) => {
                    saved = true;
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
        assert!(saved, "save_snapshot failed after retries: {last_err:?}");
    }
    ConcurrentEngine::new_with_path(build_config(&bitmap_path), docstore_path.as_path()).unwrap()
}

/// Insert NEW_IMAGE with a postId value that has never existed on disk,
/// through the steady-state ops path, and force-publish (the same barrier
/// apply_query_op_set uses before resolving a fan-out).
fn sync_insert_new_image(engine: &ConcurrentEngine) {
    let meta = FieldMeta::from_config(engine.config());
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = vec![EntityOps {
        entity_id: NEW_IMAGE as i64,
        creates_slot: true,
        ops: vec![Op::Set { field: "postId".into(), value: json!(NEW_POST) }],
    }];
    let (applied, skipped, errors) =
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    assert_eq!((applied, skipped, errors), (1, 0, 0), "sync insert must apply");
    sink.flush().unwrap();
    dw.flush();
    assert!(
        engine.force_publish_blocking(Duration::from_secs(5)),
        "force publish must succeed"
    );
    wait_for_alive(engine, NEW_IMAGE, 5_000);
}

/// The direct query shape the fan-out resolver executes: Eq(postId, NEW_POST).
/// This is the assertion at the heart of specimen 136063341 — if it returns
/// empty, the Post-publish fan-out silently applies nothing.
#[test]
fn sync_created_value_matches_after_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = reopened_engine(dir.path());
    sync_insert_new_image(&engine);

    // First query: triggers ensure_fields_loaded's per-value lazy path for a
    // value with NO disk shard entry.
    let result = engine
        .query(
            &[FilterClause::Eq("postId".into(), Value::Integer(NEW_POST))],
            None,
            100,
        )
        .unwrap();
    assert!(
        result.ids.contains(&(NEW_IMAGE as i64)),
        "FIRST query for a sync-created postId must match the new image; got {:?}",
        result.ids
    );

    // Second query: after the lazy-load machinery ran once (whatever state it
    // left the VB in), the match must still hold.
    let result2 = engine
        .query(
            &[FilterClause::Eq("postId".into(), Value::Integer(NEW_POST))],
            None,
            100,
        )
        .unwrap();
    assert!(
        result2.ids.contains(&(NEW_IMAGE as i64)),
        "SECOND query (post-lazy-load) must still match; got {:?}",
        result2.ids
    );

    // Baseline value must also still match after reopen (disk round-trip).
    let baseline = engine
        .query(
            &[FilterClause::Eq("postId".into(), Value::Integer(BASELINE_POST))],
            None,
            100,
        )
        .unwrap();
    assert!(
        baseline.ids.contains(&(BASELINE_IMAGE as i64)),
        "baseline postId must match after reopen; got {:?}",
        baseline.ids
    );
}

/// Variant: the image insert arrives while the engine is in LOADING MODE
/// (bitdex-0's exact post-nuke state: ops accumulated during the dump apply
/// after exit_loading_mode). The fan-out then resolves after exit.
#[test]
fn fanout_resolves_value_synced_through_loading_mode() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = reopened_engine(dir.path());

    engine.enter_loading_mode();
    // Insert lands in staging while publishing is suspended.
    let meta = FieldMeta::from_config(engine.config());
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = vec![EntityOps {
        entity_id: NEW_IMAGE as i64,
        creates_slot: true,
        ops: vec![Op::Set { field: "postId".into(), value: json!(NEW_POST) }],
    }];
    apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    sink.flush().unwrap();
    dw.flush();
    engine.exit_loading_mode();
    wait_for_alive(&engine, NEW_IMAGE, 5_000);

    let mut sink2 = CoalescerSink::new(engine.mutation_sender());
    let mut dw2 = DocWriter::new(engine.docstore_arc());
    let mut batch2 = vec![EntityOps {
        entity_id: NEW_POST,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {NEW_POST}")),
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(1_752_000_000i64) }],
        }],
    }];
    let (applied, _, errors) =
        apply_ops_batch(&mut sink2, &meta, &mut batch2, Some(&engine), Some(&mut dw2));
    assert_eq!(errors, 0);
    assert!(
        applied >= 1,
        "fan-out must match a value synced through a loading-mode window; applied=0 \
         is the specimen signature"
    );
}

/// Variant: image insert AND the Post-publish fan-out arrive in the SAME
/// WAL batch on the reopened (lazy) engine — the post-nuke forward-replay
/// shape, where the coalescing window packs the 10s-apart PG events together.
#[test]
fn fanout_resolves_same_batch_insert_on_reopened_engine() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = reopened_engine(dir.path());

    let meta = FieldMeta::from_config(engine.config());
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let mut batch = vec![
        EntityOps {
            entity_id: NEW_IMAGE as i64,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(NEW_POST) }],
        },
        EntityOps {
            entity_id: NEW_POST,
            creates_slot: false,
            ops: vec![Op::QueryOpSet {
                query: Some(format!("postId eq {NEW_POST}")),
                ops: vec![Op::Set { field: "publishedAt".into(), value: json!(1_752_000_000i64) }],
            }],
        },
    ];
    let (applied, _, errors) =
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    sink.flush().unwrap();
    dw.flush();
    assert_eq!(errors, 0);
    // applied counts the insert entry + fan-out slots; the fan-out contributes
    // at least one slot-apply beyond the insert itself.
    assert!(
        applied >= 2,
        "same-batch fan-out on a reopened lazy engine must match the in-batch \
         insert (barrier + fan-out-last ordering); applied={applied}"
    );
}

/// End-to-end: the actual fan-out mechanism. A Post-publish queryOpSet
/// (postId eq NEW_POST → Set publishedAt) must match and mutate the
/// sync-created image, exactly like specimen 136063341's publish should have.
#[test]
fn fanout_resolves_sync_created_value_after_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = reopened_engine(dir.path());
    sync_insert_new_image(&engine);

    let meta = FieldMeta::from_config(engine.config());
    let mut sink = CoalescerSink::new(engine.mutation_sender());
    let mut dw = DocWriter::new(engine.docstore_arc());
    let publish_ts: i64 = 1_752_000_000;
    let mut batch = vec![EntityOps {
        entity_id: NEW_POST,
        creates_slot: false,
        ops: vec![Op::QueryOpSet {
            query: Some(format!("postId eq {NEW_POST}")),
            ops: vec![Op::Set { field: "publishedAt".into(), value: json!(publish_ts) }],
        }],
    }];
    let (applied, _skipped, errors) =
        apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
    sink.flush().unwrap();
    dw.flush();
    assert_eq!(errors, 0, "fan-out must not error");
    assert!(
        applied >= 1,
        "Post-publish fan-out must match the sync-created image (specimen \
         136063341 signature: applied=0 means the publish silently no-ops)"
    );
}
