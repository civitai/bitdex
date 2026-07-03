//! Regression test: when a bucket's boot diff gap exceeds its own
//! `duration_secs` (long downtime with a stale persisted diff log), the
//! promised "full rebuild on first refresh" must actually happen.
//!
//! Bug (found in review of the per-bucket-name PendingBucketDiffs rework):
//! `concurrent_engine.rs`'s boot-init changed the old `continue` (which
//! skipped a bucket entirely on gap-too-large) into an `if/else`, so
//! `map.insert` still ran and a SECOND phase unconditionally advanced the
//! bucket's `last_cutoff` to `snap(now - duration)` using only the
//! (incomplete) retained diffs — silently marking the ENTIRE gap as
//! "already handled" with zero diff coverage for it. On the first flush
//! refresh, `new_cutoff <= old_cutoff` (both already at "now"), so nothing
//! runs — the promised rebuild never fires and stale slots leak in ground
//! truth forever (visible even with `skip_cache=true`; no backstop, since
//! `bucket_entry_ttl_secs` is cache-only).
//!
//! Fix: skip the second-phase `last_cutoff` advance entirely for a
//! gap-skipped bucket, leaving `last_cutoff` at its old (stale) persisted
//! value. The flush thread's first incremental refresh then naturally
//! covers the WHOLE gap in one pass, since `old_cutoff` is small enough
//! that every truly-expired slot's value satisfies
//! `val >= old_cutoff && val < new_cutoff` — a de facto full rebuild with
//! no separate code path.
//!
//! This needs a REAL elapsed-time gap (an actual downtime), not just a
//! fabricated log entry: the bucket's own `last_cutoff` is separately
//! persisted (`meta_store.load_time_bucket_cutoff`) and restored as-is on
//! boot, so if no real wall-clock time passes between save and reopen, it's
//! already fresh regardless of what the diff log claims. The fabricated log
//! entry only needs to make `PendingBucketDiffs::current_cutoff()` claim a
//! stale coverage point so the boot code's gap>duration branch fires.

use std::thread;
use std::time::Duration;

use bitdex_v2::bucket_diff_log::{BucketDiff, BucketDiffLog};
use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{
    BucketConfig, Config, FilterFieldConfig, SortFieldConfig, TimeBucketFieldConfig,
};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::Value;

fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
    Document {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

fn wait_for_alive(engine: &ConcurrentEngine, expected: u64, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if engine.alive_count() == expected {
            thread::sleep(Duration::from_millis(80));
            return;
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(engine.alive_count(), expected, "timed out waiting for alive count");
}

fn bucket_last_cutoff(engine: &ConcurrentEngine, name: &str) -> u64 {
    let stats = engine.time_bucket_stats();
    stats.get(name)
        .and_then(|v| v.get("last_cutoff"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

fn bucket_count(engine: &ConcurrentEngine, name: &str) -> u64 {
    let stats = engine.time_bucket_stats();
    stats.get(name)
        .and_then(|v| v.get("slots"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn make_config(bitmap_path: std::path::PathBuf) -> Config {
    let mut config = Config {
        filter_fields: vec![FilterFieldConfig {
            name: "category".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
            max_range_scan_values: None,
        }],
        sort_fields: vec![SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: true,
            computed: None,
        }],
        time_buckets: Some(TimeBucketFieldConfig {
            filter_field: "sortAt".to_string(),
            sort_field: "sortAt".to_string(),
            // Short window + fast refresh so both the real "downtime" gap
            // and the "de facto full rebuild" happen within the test's
            // timeframe.
            range_buckets: vec![BucketConfig {
                name: "5s".to_string(),
                duration_secs: 5,
                refresh_interval_secs: 1,
            }],
        }),
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };
    config.storage.bitmap_path = Some(bitmap_path);
    config
}

#[test]
fn boot_gap_exceeding_duration_does_not_defeat_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let bitmap_path = dir.path().join("bitmaps");
    let docstore_path = dir.path().join("docs");
    let config = make_config(bitmap_path.clone());

    // Phase 1: fresh boot. Insert slot G with sortAt = "now" — well inside
    // the 5s window. Let at least one real refresh cycle run (so the
    // bucket's own persisted `last_cutoff` becomes a real, fresh value —
    // not the TimeBucket::new() default of 0), then persist + unload so
    // both the bucket bitmap AND its cutoff survive to phase 2.
    let t0 = now_secs();
    {
        let engine = ConcurrentEngine::new_with_path(config.clone(), docstore_path.as_path()).unwrap();
        engine
            .put(
                100,
                &make_doc(vec![
                    ("sortAt", FieldValue::Single(Value::Integer(t0 as i64))),
                    ("category", FieldValue::Single(Value::Integer(1))),
                ]),
            )
            .unwrap();
        wait_for_alive(&engine, 1, 2000);
        thread::sleep(Duration::from_millis(1500)); // >= 1 refresh_interval_secs
        assert_eq!(bucket_count(&engine, "5s"), 1, "slot 100 must still be in the 5s bucket right after insert");
        engine.save_and_unload().unwrap();
    }

    // Simulate real downtime: sleep past the 5s window so slot 100 becomes
    // genuinely, physically stale by the time phase 2 boots — the bug can
    // only be observed with an actual elapsed-time gap (see module doc).
    thread::sleep(Duration::from_millis(7000));

    // Fabricate a stale diff log entry so PendingBucketDiffs::current_cutoff()
    // claims coverage far in the past — this is what drives the boot code's
    // `persisted_cutoff` (it takes precedence over the bucket's own
    // `last_cutoff` fallback whenever `pending.current_cutoff() > 0`) and
    // makes the gap (vs. the freshly computed `current_cutoff`) exceed the
    // 5s duration, taking the gap>duration branch under test.
    let log_path = bitmap_path.join("bucket_diffs__5s.log");
    let log = BucketDiffLog::new(log_path, 100, 0.3);
    log.append(&BucketDiff {
        cutoff_before: 0,
        cutoff_after: t0.saturating_sub(1000),
        expired: std::sync::Arc::new(roaring::RoaringBitmap::new()),
    })
    .unwrap();

    // Phase 2: reopen after the simulated downtime with the fabricated
    // stale log in place.
    let engine = ConcurrentEngine::new_with_path(config, docstore_path.as_path()).unwrap();

    // Immediately after boot (before the first flush refresh cycle runs),
    // last_cutoff must NOT have been advanced to "now - duration": that
    // would be the bug — silently claiming the whole real downtime gap is
    // already covered by zero diff data. It should still sit at its
    // restored (older, real) value from phase 1's own refresh.
    let last_cutoff_at_boot = bucket_last_cutoff(&engine, "5s");
    let now2 = now_secs();
    let would_be_new_cutoff = now2.saturating_sub(5); // snap_cutoff(now2-5, 1) ~= this
    assert!(
        last_cutoff_at_boot < would_be_new_cutoff,
        "boot must NOT advance last_cutoff past the unverified gap for a \
         gap>duration bucket (last_cutoff={}, would-be new_cutoff={})",
        last_cutoff_at_boot,
        would_be_new_cutoff,
    );
    // And slot 100 — genuinely stale after the real 7s downtime — must
    // still be present right after boot: the fix's job is to not silently
    // mark the gap handled, not to synchronously rebuild.
    assert_eq!(
        bucket_count(&engine, "5s"),
        1,
        "slot 100 must still be present immediately after boot (rebuild \
         happens on the next flush refresh, not synchronously at boot)",
    );

    // Give the flush thread a couple of refresh cycles (refresh_interval_secs=1)
    // to run. Because last_cutoff was left at its real, older, pre-boot
    // value, this incremental pass covers the WHOLE stale range in one shot
    // and must catch and remove slot 100 — the de facto full rebuild.
    thread::sleep(Duration::from_millis(2000));
    assert_eq!(
        bucket_count(&engine, "5s"),
        0,
        "the flush thread's first refresh after a gap>duration boot must \
         evict the stale slot in one incremental pass (de facto full \
         rebuild) — got {} slots still in the 5s bucket",
        bucket_count(&engine, "5s"),
    );
}
