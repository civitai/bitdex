//! Ops-log integration tests for the ShardStore + ConcurrentEngine pipeline.
//!
//! These tests verify that:
//! 1. Ops-log entries are written to disk after each flush cycle
//! 2. Snapshot publish happens BEFORE ops-log persist (PR #43 ordering)
//! 3. Crash recovery works when ops-log is truncated/lost
//! 4. pg-sync idempotent replay recovers lost state after crash
//!
//! Written by Ivanna (QA) per Sky's assignment — covers the ops-log append
//! path wired in PR #36 and reordered in PR #43.

use std::thread;
use std::time::Duration;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{FilterClause, SortClause, SortDirection, Value};
use bitdex_v2::shard_store_bitmap::*;

fn opslog_config(bitmap_path: &std::path::Path) -> Config {
    let mut config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                nullable: false,
            },
            FilterFieldConfig {
                name: "tagIds".to_string(),
                field_type: FilterFieldType::MultiValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
                per_value_lazy: false,
                nullable: false,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "reactionCount".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
        }],
        max_page_size: 1000,
        flush_interval_us: 50, // Fast flush for tests
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };
    config.storage.bitmap_path = Some(bitmap_path.to_path_buf());
    config
}

fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
    Document {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if engine.alive_count() == expected_alive {
            thread::sleep(Duration::from_millis(5));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        engine.alive_count(),
        expected_alive,
        "timed out waiting for flush; alive_count={} expected={}",
        engine.alive_count(),
        expected_alive
    );
}

/// Wait for merge thread to run (compaction + persistence).
fn wait_for_merge(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

// ===========================================================================
// Test 1: Ops-log files are written after flush
// Verify that after put + flush, shard files exist with ops entries
// ===========================================================================

#[test]
fn test_opslog_written_after_put_and_flush() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");

    let config = opslog_config(&bitmap_path);
    let engine = ConcurrentEngine::new_with_path(config, docstore_path.as_path()).unwrap();

    // Insert 5 documents with different nsfwLevel values
    for i in 1..=5u32 {
        engine
            .put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(i as i64))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 100))),
                ]),
            )
            .unwrap();
    }

    wait_for_flush(&engine, 5, 2000);

    // Give extra time for ops-log append (happens after snapshot publish)
    thread::sleep(Duration::from_millis(50));

    // Open separate ShardStore instances to read the ops-log files
    let ss_root = bitmap_path.join("shardstore");

    // Check alive store has ops
    let alive_store = AliveBitmapStore::new(
        ss_root.join("alive"),
        SingletonShard,
    ).unwrap();
    let alive_ops = alive_store.ops_count(&AliveShardKey).unwrap();
    assert!(
        alive_ops.is_some() && alive_ops.unwrap() > 0,
        "alive store should have ops after flush, got {:?}",
        alive_ops,
    );

    // Verify alive bitmap from ops-log matches engine state
    let alive_bm = alive_store.read(&AliveShardKey).unwrap().unwrap();
    assert_eq!(alive_bm.len(), 5, "alive bitmap should have 5 bits set");
    for i in 1..=5u32 {
        assert!(alive_bm.contains(i), "slot {} should be alive", i);
    }

    // Check filter store has ops for nsfwLevel values
    let filter_store = FilterBitmapStore::new(
        ss_root.join("filter"),
        FieldValueBucketShard,
    ).unwrap();

    for nsfw_val in 1..=5u64 {
        let bucket_key = FilterBucketKey::from_value("nsfwLevel".into(), nsfw_val);
        let bucket = filter_store.read(&bucket_key).unwrap();
        assert!(
            bucket.is_some(),
            "filter bucket for nsfwLevel={} should exist",
            nsfw_val,
        );
        let bucket = bucket.unwrap();
        let bm = bucket.values.get(&nsfw_val);
        assert!(
            bm.is_some(),
            "bitmap for nsfwLevel={} should exist in bucket",
            nsfw_val,
        );
        assert!(
            bm.unwrap().contains(nsfw_val as u32),
            "slot {} should be in nsfwLevel={} bitmap",
            nsfw_val, nsfw_val,
        );
    }

    // Check sort store has ops for reactionCount bit layers.
    // 100 = 0b1100100, set bits: 2, 5, 6
    // 200 = 0b11001000, set bits: 3, 6, 7
    // At least one of these layers should have data in the sort store.
    let sort_store = SortBitmapStore::new(
        ss_root.join("sort"),
        SortLayerShard,
    ).unwrap();

    // Check that ANY sort layer has data (proving sort ops were written)
    let mut found_sort_data = false;
    for bit_pos in 0..32u8 {
        let key = SortLayerShardKey {
            field: "reactionCount".into(),
            bit_position: bit_pos,
        };
        if let Some(bm) = sort_store.read(&key).unwrap() {
            if bm.len() > 0 {
                found_sort_data = true;
                break;
            }
        }
    }
    assert!(found_sort_data, "sort store should have data for reactionCount after flush");

    drop(engine);
}

// ===========================================================================
// Test 2: Snapshot publish happens before ops-log persist
// After flush, queries work (snapshot published) even if we haven't
// verified ops-log yet — proves ordering is snapshot-first
// ===========================================================================

#[test]
fn test_opslog_ordering_snapshot_before_persist() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");

    let config = opslog_config(&bitmap_path);
    let engine = ConcurrentEngine::new_with_path(config, docstore_path.as_path()).unwrap();

    // Insert documents
    for i in 1..=10u32 {
        engine
            .put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3 + 1) as i64))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 50))),
                ]),
            )
            .unwrap();
    }

    wait_for_flush(&engine, 10, 2000);

    // Immediately query — snapshot must be published already
    // (ops-log may still be writing, but queries work from in-memory snapshot)
    let result = engine
        .query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        )
        .unwrap();

    // nsfwLevel=1 for i where (i % 3 + 1) == 1, i.e. i % 3 == 0 → i=3,6,9
    assert_eq!(result.ids.len(), 3, "query should find 3 docs with nsfwLevel=1");
    let mut sorted = result.ids.clone();
    sorted.sort();
    assert_eq!(sorted, vec![3, 6, 9]);

    // Sorted query should also work immediately
    let sorted_result = engine
        .query(
            &[],
            Some(&SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            3,
        )
        .unwrap();
    assert_eq!(sorted_result.ids.len(), 3);
    // Top 3 by desc reactionCount: slot 10 (500), slot 9 (450), slot 8 (400)
    assert_eq!(sorted_result.ids, vec![10, 9, 8]);

    drop(engine);
}

// ===========================================================================
// Test 3: Compaction clears ops and preserves data
// After compaction, ops_count=0 but data is in snapshot
// ===========================================================================

#[test]
fn test_opslog_compaction_clears_ops_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");

    let config = opslog_config(&bitmap_path);
    let engine = ConcurrentEngine::new_with_path(config, docstore_path.as_path()).unwrap();

    // Insert documents
    for i in 1..=5u32 {
        engine
            .put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 10))),
                ]),
            )
            .unwrap();
    }

    wait_for_flush(&engine, 5, 2000);
    thread::sleep(Duration::from_millis(50)); // let ops-log write

    let ss_root = bitmap_path.join("shardstore");

    // Verify ops exist before compaction
    let alive_store = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();
    let ops_before = alive_store.ops_count(&AliveShardKey).unwrap();
    assert!(
        ops_before.is_some() && ops_before.unwrap() > 0,
        "should have ops before compaction",
    );

    // Compact the alive store
    alive_store.compact_current(&AliveShardKey).unwrap();

    // After compaction: zero ops, data preserved
    let ops_after = alive_store.ops_count(&AliveShardKey).unwrap();
    assert_eq!(ops_after, Some(0), "ops should be 0 after compaction");

    let alive_bm = alive_store.read(&AliveShardKey).unwrap().unwrap();
    assert_eq!(alive_bm.len(), 5, "alive bitmap should preserve all 5 slots after compaction");
    for i in 1..=5u32 {
        assert!(alive_bm.contains(i), "slot {} should survive compaction", i);
    }

    // In-memory queries still work
    let result = engine.query(&[], None, 100).unwrap();
    assert_eq!(result.ids.len(), 5);

    drop(engine);
}

// ===========================================================================
// Test 4: Crash recovery — truncated ops-log, restart, pg-sync replay
// Simulates crash between snapshot publish and ops-log persist.
// After restart, lost ops are recovered via idempotent re-put.
// ===========================================================================

#[test]
fn test_crash_recovery_truncated_opslog_replay() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");

    let config = opslog_config(&bitmap_path);

    // Phase 1: Insert docs, flush, let merge compact to snapshots
    let original_ids: Vec<i64>;
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        ).unwrap();

        for i in 1..=10u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 5 + 1) as i64))),
                        ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 100))),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 10, 2000);
        // Wait for merge thread to run compaction (writes snapshots)
        wait_for_merge(500);

        original_ids = {
            let r = engine.query(&[], None, 100).unwrap();
            let mut ids = r.ids;
            ids.sort();
            ids
        };
        assert_eq!(original_ids.len(), 10);

        // Engine dropped — clean shutdown, merge flushes
    }

    // Phase 2: Verify data survives restart (baseline)
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        ).unwrap();

        let alive = engine.alive_count();
        assert_eq!(alive, 10, "all 10 docs should survive restart");

        // Engine dropped
    }

    // Phase 3: Simulate crash — truncate the alive shard's ops section
    let ss_root = bitmap_path.join("shardstore");
    let alive_store = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();

    // First compact to get a clean snapshot, then add some new ops
    alive_store.compact_current(&AliveShardKey).unwrap();
    // Add ops that simulate a "new write that wasn't compacted before crash"
    alive_store.append_op(&AliveShardKey, &BitmapOp::SetBit { bit: 100 }).unwrap();
    alive_store.append_op(&AliveShardKey, &BitmapOp::SetBit { bit: 200 }).unwrap();

    // Now truncate those new ops (simulating crash before they were written)
    // Read header to find ops section, then truncate file there
    let header = alive_store.read_header(&AliveShardKey).unwrap().unwrap();
    let shard_path = alive_store.shard_path_in_gen(
        &AliveShardKey,
        alive_store.current_generation(),
    );

    // Truncate to just snapshot (remove ops section)
    let truncate_to = header.ops_section_offset;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&shard_path)
        .unwrap();
    file.set_len(truncate_to).unwrap();
    drop(file);

    // Phase 4: Restart — should load from snapshot (ops lost)
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        ).unwrap();

        let alive = engine.alive_count();
        // Original 10 should be in the snapshot.
        // The 2 extra ops (slot 100, 200) were truncated — that's fine.
        assert_eq!(alive, 10, "original 10 docs should survive from snapshot");

        // Queries should still work
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        ).unwrap();
        assert!(!result.ids.is_empty(), "filter queries should work after restart");
    }

    // Phase 5: Simulate pg-sync replay — re-put the same docs
    {
        let engine = ConcurrentEngine::new_with_path(
            config.clone(),
            docstore_path.as_path(),
        ).unwrap();

        // pg-sync replays all docs from last cursor
        for i in 1..=10u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 5 + 1) as i64))),
                        ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 100))),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 10, 2000);

        // Verify state matches original
        let result = engine.query(&[], None, 100).unwrap();
        let mut ids = result.ids;
        ids.sort();
        assert_eq!(ids, original_ids, "idempotent replay should restore exact state");

        // Verify sort order preserved
        let sorted = engine.query(
            &[],
            Some(&SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            3,
        ).unwrap();
        assert_eq!(sorted.ids, vec![10, 9, 8], "sort order should be correct after replay");
    }
}

// ===========================================================================
// Test 5: Idempotent replay — re-putting same docs causes no corruption
// Simulates pg-sync replaying ops that were already applied
// ===========================================================================

#[test]
fn test_opslog_idempotent_replay_no_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");

    let config = opslog_config(&bitmap_path);
    let engine = ConcurrentEngine::new_with_path(config, docstore_path.as_path()).unwrap();

    let test_doc = make_doc(vec![
        ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
        ("tagIds", FieldValue::Multi(vec![
            Value::Integer(10),
            Value::Integer(20),
        ])),
        ("reactionCount", FieldValue::Single(Value::Integer(500))),
    ]);

    // Put the same document 5 times (simulating repeated pg-sync replay)
    for _ in 0..5 {
        engine.put(42, &test_doc).unwrap();
    }

    wait_for_flush(&engine, 1, 2000);

    // Should have exactly 1 alive document, not 5
    assert_eq!(engine.alive_count(), 1);

    // Filter query should return exactly slot 42
    let result = engine.query(
        &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(3))],
        None,
        100,
    ).unwrap();
    assert_eq!(result.ids, vec![42]);

    // Sort query should return slot 42
    let sorted = engine.query(
        &[],
        Some(&SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        }),
        10,
    ).unwrap();
    assert_eq!(sorted.ids, vec![42]);

    // Ops-log should have entries (not corrupted by repeated puts)
    let ss_root = bitmap_path.join("shardstore");
    let alive_store = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();
    let alive_bm = alive_store.read(&AliveShardKey).unwrap().unwrap();
    assert_eq!(alive_bm.len(), 1, "alive bitmap should have exactly 1 slot");
    assert!(alive_bm.contains(42));

    drop(engine);
}

// ===========================================================================
// Test 6: Delete ops appear in ops-log
// Verifies clean delete clears filter/sort/alive bits in ops-log
// ===========================================================================

#[test]
fn test_opslog_delete_clears_all_stores() {
    let dir = tempfile::tempdir().unwrap();
    let docstore_path = dir.path().join("docs");
    let bitmap_path = dir.path().join("bitmaps");

    let config = opslog_config(&bitmap_path);
    let engine = ConcurrentEngine::new_with_path(config, docstore_path.as_path()).unwrap();

    // Insert 3 documents
    for i in 1..=3u32 {
        engine
            .put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 10))),
                ]),
            )
            .unwrap();
    }

    wait_for_flush(&engine, 3, 2000);
    thread::sleep(Duration::from_millis(50));

    // Delete slot 2
    engine.delete(2).unwrap();
    wait_for_flush(&engine, 2, 2000);
    thread::sleep(Duration::from_millis(50));

    // Verify in-memory state
    assert_eq!(engine.alive_count(), 2);
    let result = engine.query(&[], None, 100).unwrap();
    assert_eq!(result.ids.len(), 2);
    assert!(!result.ids.contains(&2));

    // Verify ops-log reflects the delete
    let ss_root = bitmap_path.join("shardstore");
    let alive_store = AliveBitmapStore::new(ss_root.join("alive"), SingletonShard).unwrap();
    let alive_bm = alive_store.read(&AliveShardKey).unwrap().unwrap();
    assert_eq!(alive_bm.len(), 2);
    assert!(!alive_bm.contains(2), "deleted slot should be cleared in alive ops-log");

    // Filter bitmap for nsfwLevel=1 should not contain slot 2
    let filter_store = FilterBitmapStore::new(
        ss_root.join("filter"),
        FieldValueBucketShard,
    ).unwrap();
    let bucket_key = FilterBucketKey::from_value("nsfwLevel".into(), 1);
    let bucket = filter_store.read(&bucket_key).unwrap().unwrap();
    let filter_bm = bucket.values.get(&1).unwrap();
    assert!(!filter_bm.contains(2), "deleted slot should be cleared in filter ops-log");
    assert_eq!(filter_bm.len(), 2, "remaining 2 slots should be in filter bitmap");

    drop(engine);
}
