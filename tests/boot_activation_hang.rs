//! Deterministic repro for the v1.1.31 / v1.1.32 BOOT HANG.
//!
//! Prod symptom: "Boot phase: engine_create completed" then nothing — no
//! dictionary_load line, no HTTP listener, idle CPU, startup-probe kill loop.
//!
//! Mechanism under test: on the first flush cycle after reopen, past-due
//! deferred entries activate. The #297/#298 doc-coherence block iterates the
//! activated slots while HOLDING `docstore.read()` and calls
//! `DocWriter::write_set` for each derived shadow — and
//! `DocWriter::resolve_field`, on a field-dict-snapshot MISS, calls
//! `docstore.write()` (`ensure_field_index`). A shadow target that was never
//! stored in the docstore (a deferred draft never had `isPublished` written —
//! the deferred branch deliberately skips shadow doc writes) therefore makes
//! the flush thread take a WRITE lock while it holds a READ lock:
//! same-thread deadlock, no second thread required. The boot thread then
//! blocks forever on its own `docstore.write()`
//! (`set_docstore_defaults`, the first lock in the post-engine_create boot
//! window) and the server never binds.
//!
//! FAILS (times out → panics) on 841ad80/b7116db-era code; PASSES with the
//! fix (doc writes moved outside the read scope).

#![cfg(feature = "pg-sync")]

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    Config, DataSchema, DeferredAliveConfig, FieldMapping, FieldValueType, FilterFieldConfig,
    SortFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::ingester::BitmapSink;
use bitdex_v2::ops_processor::{apply_ops_batch, DocWriter, FieldMeta};
use bitdex_v2::pg_sync::ops::{EntityOps, Op};
use serde_json::json;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IMAGE: u32 = 7;

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn prod_shaped_config(bitmap_path: &std::path::Path) -> Config {
    let mut config = Config {
        filter_fields: vec![
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
        ],
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
    config.storage.bitmap_path = Some(bitmap_path.to_path_buf());
    config
}

/// Boot-hang repro: deferred entry goes past-due while the engine is down;
/// reopen triggers activation on the first flush cycles; the boot thread then
/// does what server.rs does right after engine_create (set_docstore_defaults,
/// which takes docstore.write()). Watchdog fails the test if the boot window
/// doesn't complete — which is exactly the prod hang.
#[test]
fn test_reopen_with_pastdue_deferred_does_not_hang_boot_window() {
    let dir = tempfile::TempDir::new().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    let config = prod_shaped_config(&bitmap_path);
    let schema = config.data_schema.clone();

    // Phase 1: engine with a slot DEFERRED via the ops path. The deferred
    // branch writes publishedAt to the doc but deliberately NOT the
    // isPublished shadow — so "isPublished" is never registered in the
    // docstore's field dictionary (the resolve_field-miss precondition).
    {
        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), docstore_path.as_path()).unwrap();
        let meta = FieldMeta::from_config(engine.config());
        let mut sink = bitdex_v2::ingester::CoalescerSink::new(engine.mutation_sender());
        let mut dw = DocWriter::new(engine.docstore_arc());
        let activate_at = now_secs() + 8; // well past phase-1 duration; past-due after reopen sleep
        // A plain ALIVE slot too: without a persisted alive bitmap, boot's
        // restore branch (concurrent_engine build, `if let Some(alive_bm)`)
        // skips entirely and never loads the deferred map into staging —
        // activation would silently never run and the repro would test
        // nothing (that skip is itself a bug, noted separately).
        let mut alive_batch = vec![EntityOps {
            entity_id: 5,
            creates_slot: true,
            ops: vec![Op::Set { field: "postId".into(), value: json!(50) }],
        }];
        let (a_applied, _, a_errors) =
            apply_ops_batch(&mut sink, &meta, &mut alive_batch, Some(&engine), Some(&mut dw));
        assert_eq!((a_applied, a_errors), (1, 0));
        let mut batch = vec![EntityOps {
            entity_id: IMAGE as i64,
            creates_slot: true,
            ops: vec![
                Op::Set { field: "postId".into(), value: json!(100) },
                Op::Set { field: "publishedAt".into(), value: json!(activate_at) },
            ],
        }];
        let (applied, _, errors) =
            apply_ops_batch(&mut sink, &meta, &mut batch, Some(&engine), Some(&mut dw));
        assert_eq!((applied, errors), (1, 0));
        sink.flush().unwrap();
        dw.flush();

        // Wait until the deferred entry is applied + persisted (flush thread
        // writes the deferred map on every applied deferral batch).
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if engine.is_slot_deferred(IMAGE) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(engine.is_slot_deferred(IMAGE), "rig: slot must be deferred");
        // Give the flush thread a beat to persist the map file.
        std::thread::sleep(Duration::from_millis(300));
        // Find EVERY deferred* file anywhere under the temp tree.
        fn walk(dir: &std::path::Path, hits: &mut Vec<(std::path::PathBuf, u64)>) {
            if let Ok(rd) = std::fs::read_dir(dir) {
                for e in rd.flatten() {
                    let p = e.path();
                    if p.is_dir() { walk(&p, hits); }
                    else if p.file_name().map(|n| n.to_string_lossy().contains("deferred")).unwrap_or(false) {
                        hits.push((p.clone(), e.metadata().map(|m| m.len()).unwrap_or(0)));
                    }
                }
            }
        }
        let mut hits = Vec::new();
        walk(dir.path(), &mut hits);
        eprintln!("RIG: deferred files pre-shutdown: {:?}", hits);
        let f = bitmap_path.join("shardstore").join("meta").join("deferred_alive.bin");
        eprintln!("RIG: pre-shutdown deferred file size={}",
            std::fs::metadata(&f).map(|m| m.len()).unwrap_or(0));
        engine.save_snapshot().expect("phase-1 snapshot");
        engine.shutdown();
        eprintln!("RIG: post-shutdown deferred file size={}",
            std::fs::metadata(&f).map(|m| m.len()).unwrap_or(0));
    }

    // Let the schedule go past-due while "the pod is down".
    std::thread::sleep(Duration::from_millis(9_000));

    // RIG DIAGNOSTIC: the deferred map file must have survived phase 1 —
    // otherwise phase 2 tests nothing.
    let deferred_file = bitmap_path.join("shardstore").join("meta").join("deferred_alive.bin");
    let deferred_size = std::fs::metadata(&deferred_file).map(|m| m.len()).unwrap_or(0);
    eprintln!("RIG: deferred_alive file at {:?} size={}", deferred_file, deferred_size);
    assert!(
        deferred_size > 8,
        "rig failure: deferred map not persisted in phase 1 (size={deferred_size}) — phase 2 would test nothing"
    );

    // Phase 2: reopen (= engine_create). The first flush cycles will run
    // activate_due → the doc-coherence block. Then do what server.rs does in
    // the post-engine_create boot window, under a watchdog.
    let (done_tx, done_rx) = std::sync::mpsc::channel::<&'static str>();
    let boot = std::thread::spawn(move || {
        let mut engine =
            ConcurrentEngine::new_with_path(config, docstore_path.as_path()).unwrap();
        done_tx.send("engine_create").unwrap();
        // server.rs boot window, in order:
        let _dicts = ConcurrentEngine::load_dictionaries(&schema, &bitmap_path).unwrap();
        engine.set_docstore_defaults(&schema); // ← docstore.write(): the prod block point
        done_tx.send("boot_window").unwrap();
        // Activation must actually RUN (slot leaves the deferred map, becomes
        // alive) and the shadow doc write must land — strict asserts, no
        // deadline escape: a rig that never activates must FAIL, not pass.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let activated = engine.is_slot_alive(IMAGE) && !engine.is_slot_deferred(IMAGE);
            let doc_ok = matches!(
                engine.get_document(IMAGE),
                Ok(Some(ref d)) if d.fields.get("isPublished").is_some()
            );
            if activated && doc_ok {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "activation never completed: alive={} deferred={} (rig failure or hang) ",
                engine.is_slot_alive(IMAGE),
                engine.is_slot_deferred(IMAGE),
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        done_tx.send("activation_settled").unwrap();
        engine.shutdown();
        done_tx.send("shutdown").unwrap();
    });

    // Watchdog: every boot-window stage must complete. 60s is generous — the
    // real workload here is milliseconds; the prod hang is infinite.
    for stage in ["engine_create", "boot_window", "activation_settled", "shutdown"] {
        match done_rx.recv_timeout(Duration::from_secs(60)) {
            Ok(s) => assert_eq!(s, stage, "boot stages out of order"),
            Err(_) => panic!(
                "BOOT HANG REPRODUCED: stage '{stage}' never completed — flush-thread \
                 activation is deadlocked (docstore write-under-read via \
                 DocWriter::resolve_field on an unregistered shadow field), and the \
                 boot thread is blocked in set_docstore_defaults. This is the \
                 v1.1.31/v1.1.32 prod boot hang."
            ),
        }
    }
    boot.join().unwrap();
}
