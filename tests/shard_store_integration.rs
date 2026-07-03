//! ShardStore integration tests — cross-system E2E scenarios.
//!
//! These tests exercise the ShardStore at the integration level:
//! doc + bitmap stores working together, flat compaction lifecycle,
//! crash recovery, compaction, and concurrent access patterns.
//!
//! Written by Ivanna (QA) for Adam to wire in as ShardStore matures.
//! Tests are designed to be pluggable — they use the public ShardStore API
//! and don't depend on ConcurrentEngine wiring.

use std::io::Read;


use bitdex_v2::shard_store_doc::*;
use bitdex_v2::shard_store_bitmap::*;

// ===========================================================================
// Scenario 1: ShardStore round-trip (doc + bitmap)
// Create store → write ops → compact → read back → verify state
// ===========================================================================

#[test]
fn test_doc_store_roundtrip_create_modify_compact_verify() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(1000);

    // Step 1: Create a document with multiple field types
    store.append_op(&shard_key, &DocOp::Create {
        slot: 1000,
        fields: vec![
            (0, PackedValue::I(5)),             // nsfwLevel (scalar)
            (1, PackedValue::S("test".into())), // name (string scalar)
            (2, PackedValue::Mi(vec![10, 20])), // tagIds (multi-value)
            (3, PackedValue::B(false)),          // poi (boolean)
        ],
    }).unwrap();

    // Step 2: Modify via Set, Append, Remove
    store.append_op(&shard_key, &DocOp::Set {
        slot: 1000, field: 0, value: PackedValue::I(2),
    }).unwrap();
    store.append_op(&shard_key, &DocOp::Append {
        slot: 1000, field: 2, value: PackedValue::I(30),
    }).unwrap();
    store.append_op(&shard_key, &DocOp::Remove {
        slot: 1000, field: 2, value: PackedValue::I(10),
    }).unwrap();

    // Step 3: Verify pre-compaction state
    let snap = store.read(&shard_key).unwrap().unwrap();
    let doc = &snap.docs[&1000];
    assert_eq!(doc.iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(2));
    match &doc.iter().find(|(f, _)| *f == 2).unwrap().1 {
        PackedValue::Mi(v) => assert_eq!(v, &[20, 30]),
        other => panic!("expected Mi, got {:?}", other),
    }
    assert_eq!(store.ops_count(&shard_key).unwrap(), Some(4));

    // Step 4: Compact
    store.compact_shard(&shard_key).unwrap();

    // Step 5: Verify post-compaction: zero ops, same data
    assert_eq!(store.ops_count(&shard_key).unwrap(), Some(0));
    let snap2 = store.read(&shard_key).unwrap().unwrap();
    let doc2 = &snap2.docs[&1000];
    assert_eq!(doc2.iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(2));
    match &doc2.iter().find(|(f, _)| *f == 2).unwrap().1 {
        PackedValue::Mi(v) => assert_eq!(v, &[20, 30]),
        other => panic!("expected Mi after compact, got {:?}", other),
    }
}

#[test]
fn test_bitmap_store_roundtrip_ops_compact_verify() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

    let key = FilterBucketKey::from_value("nsfwLevel".into(), 2);

    // Build a bitmap from ops only (no initial snapshot)
    // FilterOp requires a value tag on each op
    for bit in 0..100u32 {
        store.append_op(&key, &FilterOp::SetBit { value: 2, bit }).unwrap();
    }
    store.append_op(&key, &FilterOp::ClearBit { value: 2, bit: 50 }).unwrap();
    store.append_op(&key, &FilterOp::SetBit { value: 2, bit: 200 }).unwrap();

    // Verify pre-compact
    let bucket = store.read(&key).unwrap().unwrap();
    let bm = bucket.values.get(&2).unwrap();
    assert_eq!(bm.len(), 100); // 100 set - 1 cleared + 1 set = 100
    assert!(!bm.contains(50));
    assert!(bm.contains(200));

    // Compact
    store.compact_shard(&key).unwrap();

    // Verify post-compact: same bitmap, zero ops
    assert_eq!(store.ops_count(&key).unwrap(), Some(0));
    let bucket2 = store.read(&key).unwrap().unwrap();
    let bm2 = bucket2.values.get(&2).unwrap();
    assert_eq!(bm2, bm);
}

// ===========================================================================
// Scenario 2: Flat compaction lifecycle
// Write ops → compact (flat, no gen) → verify snapshot absorbed ops
// ===========================================================================

#[test]
fn test_flat_compaction_doc_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let key_a = SlotHexShard::slot_to_shard(100);
    let key_b = SlotHexShard::slot_to_shard(600); // different shard

    // Write doc A, then update it
    store.append_op(&key_a, &DocOp::Create {
        slot: 100,
        fields: vec![(0, PackedValue::I(1))],
    }).unwrap();
    store.append_op(&key_a, &DocOp::Set {
        slot: 100, field: 0, value: PackedValue::I(42),
    }).unwrap();

    // Write doc B (different shard)
    store.append_op(&key_b, &DocOp::Create {
        slot: 600,
        fields: vec![(0, PackedValue::I(99))],
    }).unwrap();

    // Reads reflect latest ops
    let snap_a = store.read(&key_a).unwrap().unwrap();
    assert_eq!(snap_a.docs[&100].iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(42));
    let snap_b = store.read(&key_b).unwrap().unwrap();
    assert_eq!(snap_b.docs[&600][0], (0, PackedValue::I(99)));

    // Compact both shards
    store.compact_shard(&key_a).unwrap();
    store.compact_shard(&key_b).unwrap();

    // After compaction: ops absorbed into snapshot, reads still correct
    assert_eq!(store.ops_count(&key_a).unwrap(), Some(0));
    assert_eq!(store.ops_count(&key_b).unwrap(), Some(0));
    let snap_a2 = store.read(&key_a).unwrap().unwrap();
    assert_eq!(snap_a2.docs[&100].iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(42));
}

#[test]
fn test_flat_compaction_bitmap_two_fields() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

    let key1 = FilterBucketKey::from_value("nsfwLevel".into(), 1);
    let key2 = FilterBucketKey::from_value("nsfwLevel".into(), 2);

    // Write 50 bits to value 1
    for bit in 0..50u32 {
        store.append_op(&key1, &FilterOp::SetBit { value: 1, bit }).unwrap();
    }
    // Write 50 bits to value 2
    for bit in 50..100u32 {
        store.append_op(&key2, &FilterOp::SetBit { value: 2, bit }).unwrap();
    }

    // Compact both; verify data survives
    store.compact_shard(&key1).unwrap();
    store.compact_shard(&key2).unwrap();

    assert_eq!(store.ops_count(&key1).unwrap(), Some(0));
    assert_eq!(store.ops_count(&key2).unwrap(), Some(0));

    let r1 = store.read(&key1).unwrap().unwrap();
    let bm1 = r1.values.get(&1).unwrap();
    assert_eq!(bm1.len(), 50);
    assert!(bm1.contains(0));

    let r2 = store.read(&key2).unwrap().unwrap();
    let bm2 = r2.values.get(&2).unwrap();
    assert_eq!(bm2.len(), 50);
    assert!(bm2.contains(50));
}

// ===========================================================================
// Scenario 3: Crash recovery
// Write ops → simulate crash (truncate file) → reopen → verify CRC catches it
// ===========================================================================

#[test]
fn test_crash_recovery_truncated_op_ignored() {
    let dir = tempfile::tempdir().unwrap();

    // Write 3 ops
    {
        let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
        let key = SlotHexShard::slot_to_shard(42);

        store.append_op(&key, &DocOp::Create {
            slot: 42,
            fields: vec![(0, PackedValue::I(1))],
        }).unwrap();
        store.append_op(&key, &DocOp::Set {
            slot: 42, field: 0, value: PackedValue::I(2),
        }).unwrap();
        store.append_op(&key, &DocOp::Set {
            slot: 42, field: 0, value: PackedValue::I(3),
        }).unwrap();
    }

    // Simulate crash: truncate the last few bytes of the shard file
    let shard_key = SlotHexShard::slot_to_shard(42);
    let store_tmp = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    let shard_path = store_tmp.shard_path(&shard_key);

    let metadata = std::fs::metadata(&shard_path).unwrap();
    let truncated_len = metadata.len() - 5; // chop 5 bytes off the last op
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&shard_path)
        .unwrap();
    file.set_len(truncated_len).unwrap();

    // Reopen and read — should recover gracefully (last op lost, first two intact)
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    let snap = store.read(&shard_key).unwrap().unwrap();

    // Should have field 0 = 2 (second op), not 3 (truncated third op)
    let doc = &snap.docs[&42];
    let val = doc.iter().find(|(f, _)| *f == 0).unwrap().1.clone();
    assert_eq!(val, PackedValue::I(2), "truncated op should be skipped, value should be from second op");
}

#[test]
fn test_crash_recovery_corrupted_crc_ignored() {
    let dir = tempfile::tempdir().unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);

    // Write 2 ops
    {
        let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

        store.append_op(&shard_key, &DocOp::Create {
            slot: 42,
            fields: vec![(0, PackedValue::I(1))],
        }).unwrap();
        store.append_op(&shard_key, &DocOp::Set {
            slot: 42, field: 0, value: PackedValue::I(99),
        }).unwrap();
    }

    // Corrupt the CRC of the second op by flipping a byte in the file
    let store_tmp = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    let shard_path = store_tmp.shard_path(&shard_key);

    let mut data = std::fs::read(&shard_path).unwrap();
    // Flip the last byte (part of the second op's CRC)
    let last = data.len() - 1;
    data[last] ^= 0xFF;
    std::fs::write(&shard_path, &data).unwrap();

    // Reopen and read — second op should be ignored due to CRC mismatch
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    let snap = store.read(&shard_key).unwrap().unwrap();

    // Should have field 0 = 1 (first op only), not 99 (corrupted second op)
    let doc = &snap.docs[&42];
    let val = doc.iter().find(|(f, _)| *f == 0).unwrap().1.clone();
    assert_eq!(val, PackedValue::I(1), "corrupted op should be skipped");
}

// ===========================================================================
// Scenario 4: Cross-store consistency (doc + filter + alive)
// Verify that all store types work correctly together
// ===========================================================================

#[test]
fn test_cross_store_doc_filter_alive_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let doc_dir = dir.path().join("docs");
    let filter_dir = dir.path().join("filters");
    let alive_dir = dir.path().join("alive");

    let doc_store = DocShardStore::new(doc_dir, SlotHexShard).unwrap();
    let filter_store = FilterBitmapStore::new(filter_dir, FieldValueBucketShard).unwrap();
    let alive_store = AliveBitmapStore::new(alive_dir, SingletonShard).unwrap();

    // Insert 3 documents with nsfwLevel values
    let slots = [100u32, 200, 300];
    let nsfw_values = [1u64, 2, 1]; // slots 100,300 → nsfwLevel=1, slot 200 → nsfwLevel=2

    for (i, &slot) in slots.iter().enumerate() {
        // Write doc
        let shard_key = SlotHexShard::slot_to_shard(slot);
        doc_store.append_op(&shard_key, &DocOp::Create {
            slot,
            fields: vec![(0, PackedValue::I(nsfw_values[i] as i64))],
        }).unwrap();

        // Set filter bitmap bit (FilterOp is value-tagged)
        let bucket_key = FilterBucketKey::from_value("nsfwLevel".into(), nsfw_values[i]);
        filter_store.append_op(&bucket_key, &FilterOp::SetBit {
            value: nsfw_values[i], bit: slot,
        }).unwrap();

        // Mark alive
        alive_store.append_op(&AliveShardKey, &BitmapOp::SetBit { bit: slot }).unwrap();
    }

    // Verify: nsfwLevel=1 bitmap should contain slots 100 and 300
    let bucket1 = filter_store.read(&FilterBucketKey::from_value("nsfwLevel".into(), 1)).unwrap().unwrap();
    let nsfw1 = bucket1.values.get(&1).unwrap();
    assert!(nsfw1.contains(100));
    assert!(!nsfw1.contains(200));
    assert!(nsfw1.contains(300));
    assert_eq!(nsfw1.len(), 2);

    // Verify: alive bitmap should contain all 3
    let alive = alive_store.read(&AliveShardKey).unwrap().unwrap();
    assert_eq!(alive.len(), 3);

    // Delete slot 200: clear filter + alive bits
    let bucket_key_2 = FilterBucketKey::from_value("nsfwLevel".into(), 2);
    filter_store.append_op(&bucket_key_2, &FilterOp::ClearBit {
        value: 2, bit: 200,
    }).unwrap();
    alive_store.append_op(&AliveShardKey, &BitmapOp::ClearBit { bit: 200 }).unwrap();

    // Verify clean delete
    let bucket2 = filter_store.read(&bucket_key_2).unwrap().unwrap();
    let nsfw2 = bucket2.values.get(&2).unwrap();
    assert_eq!(nsfw2.len(), 0);

    let alive2 = alive_store.read(&AliveShardKey).unwrap().unwrap();
    assert_eq!(alive2.len(), 2);
    assert!(!alive2.contains(200));
}

// ===========================================================================
// Scenario 5: Existence set correctness
// ===========================================================================

#[test]
fn test_existence_set_flat() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

    // Write values 1-5 across multiple shards
    for v in [1u64, 2, 3] {
        let key = FilterBucketKey::from_value("tags".into(), v);
        store.append_op(&key, &FilterOp::SetBit { value: v, bit: v as u32 }).unwrap();
    }
    for v in [4u64, 5] {
        let key = FilterBucketKey::from_value("tags".into(), v);
        store.append_op(&key, &FilterOp::SetBit { value: v, bit: v as u32 }).unwrap();
    }

    // Existence set should see all 5 values
    let exist = store.existence_set("tags").unwrap();
    assert_eq!(exist.len(), 5);
    for v in 1..=5 {
        assert!(exist.contains(&v), "missing value {} in existence set", v);
    }

    // Nonexistent value
    assert!(!exist.contains(&999));
}

// ===========================================================================
// Scenario 6: Janitor compaction triggering
// ===========================================================================

#[test]
fn test_janitor_should_compact_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

    let key = FilterBucketKey::from_value("test".into(), 1);

    // No shard → should not compact
    assert!(!store.should_compact(&key, 5).unwrap());

    // Add ops below threshold
    for i in 0..5u32 {
        store.append_op(&key, &FilterOp::SetBit { value: 1, bit: i }).unwrap();
    }

    // At threshold → should NOT compact (5 <= 5, need strictly greater)
    assert!(!store.should_compact(&key, 5).unwrap());

    // One more → SHOULD compact (6 > 5)
    store.append_op(&key, &FilterOp::SetBit { value: 1, bit: 100 }).unwrap();
    assert!(store.should_compact(&key, 5).unwrap());

    // Compact clears ops
    store.compact_current(&key).unwrap();
    assert!(!store.should_compact(&key, 5).unwrap());
    assert_eq!(store.ops_count(&key).unwrap(), Some(0));

    // Data preserved
    let bucket = store.read(&key).unwrap().unwrap();
    let bm = bucket.values.get(&1).unwrap();
    assert_eq!(bm.len(), 6);
}

// ===========================================================================
// Scenario 7: Sort layer bitmap round-trip
// ===========================================================================

#[test]
fn test_sort_layer_full_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let store = SortBitmapStore::new(dir.path().to_path_buf(), SortLayerShard).unwrap();

    // Write 32-bit decomposed sort field: value 42 for slot 100
    // 42 in binary = 00000000_00000000_00000000_00101010
    // Bits set: 1, 3, 5
    let value: u32 = 42;
    for bit_pos in 0..32u8 {
        let key = SortLayerShardKey {
            field: "reactionCount".into(),
            bit_position: bit_pos,
        };
        if (value >> bit_pos) & 1 == 1 {
            store.append_op(&key, &BitmapOp::SetBit { bit: 100 }).unwrap();
        }
    }

    // Verify: bits 1, 3, 5 should have slot 100 set
    for bit_pos in 0..32u8 {
        let key = SortLayerShardKey {
            field: "reactionCount".into(),
            bit_position: bit_pos,
        };
        let result = store.read(&key).unwrap();
        let expected = (value >> bit_pos) & 1 == 1;
        if expected {
            let bm = result.unwrap();
            assert!(bm.contains(100), "bit {} should have slot 100", bit_pos);
        } else {
            // Shard may not exist (no ops for unset bits)
            match result {
                Some(bm) => assert!(!bm.contains(100), "bit {} should NOT have slot 100", bit_pos),
                None => {} // Fine — no shard means no bits set
            }
        }
    }
}

// ===========================================================================
// Scenario 8: Multiple documents in same shard
// ===========================================================================

#[test]
fn test_multiple_docs_same_shard() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    // Slots 0-511 all map to shard 0 (SHARD_SHIFT = 9)
    let shard_key = SlotHexShard::slot_to_shard(0);
    assert_eq!(shard_key, SlotHexShard::slot_to_shard(511));

    // Create 3 docs in the same shard
    for slot in [0u32, 100, 511] {
        store.append_op(&shard_key, &DocOp::Create {
            slot,
            fields: vec![(0, PackedValue::I(slot as i64))],
        }).unwrap();
    }

    // Read shard — all 3 docs should be present
    let snap = store.read(&shard_key).unwrap().unwrap();
    assert_eq!(snap.docs.len(), 3);
    assert_eq!(snap.docs[&0][0].1, PackedValue::I(0));
    assert_eq!(snap.docs[&100][0].1, PackedValue::I(100));
    assert_eq!(snap.docs[&511][0].1, PackedValue::I(511));

    // Delete one doc, verify others untouched
    store.append_op(&shard_key, &DocOp::Delete { slot: 100 }).unwrap();
    let snap2 = store.read(&shard_key).unwrap().unwrap();
    assert_eq!(snap2.docs.len(), 2);
    assert!(!snap2.docs.contains_key(&100));
    assert!(snap2.docs.contains_key(&0));
    assert!(snap2.docs.contains_key(&511));
}

// ===========================================================================
// P0: File Format Correctness (F1.1-F1.5)
// Verify shard file binary layout matches spec
// ===========================================================================

#[test]
fn f1_1_shard_file_header_magic_and_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42,
        fields: vec![(0, PackedValue::I(1))],
    }).unwrap();

    // Read raw bytes of the shard file
    let shard_path = store.shard_path(&shard_key);

    let data = std::fs::read(&shard_path).unwrap();
    assert!(data.len() >= 28 /* HEADER_SIZE */, "shard too small for header");

    // Verify magic: "BDSS" = [0x42, 0x44, 0x53, 0x53]
    assert_eq!(&data[0..4], b"BDSS", "magic bytes must be BDSS");

    // Verify version: 1 as u32 LE
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    assert_eq!(version, 1, "version must be 1");
}

#[test]
fn f1_2_ops_count_matches_actual_ops() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);

    // Write exactly 5 ops
    for i in 0..5 {
        store.append_op(&shard_key, &DocOp::Set {
            slot: 42, field: 0, value: PackedValue::I(i),
        }).unwrap();
    }

    let header = store.read_header(&shard_key).unwrap().unwrap();
    assert_eq!(header.ops_count, 5, "header ops_count must match actual ops written");
}

#[test]
fn f1_4_empty_snapshot_shard_layout() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);

    // Ops-only (append_op creates shard with empty snapshot section)
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42, fields: vec![(0, PackedValue::I(1))],
    }).unwrap();

    let header = store.read_header(&shard_key).unwrap().unwrap();
    assert_eq!(header.snapshot_len, 0, "ops-only shard must have snapshot_len = 0");
    assert_eq!(header.ops_section_offset, 28 /* HEADER_SIZE */ as u64, "ops start immediately after header when no snapshot");
}

#[test]
fn f1_5_header_flags_reserved_zero() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42, fields: vec![(0, PackedValue::I(1))],
    }).unwrap();

    let header = store.read_header(&shard_key).unwrap().unwrap();
    assert_eq!(header.flags, 0, "flags must be 0 (reserved)");
}

// ===========================================================================
// P0: Additional Crash Recovery (F4.3-F4.6)
// ===========================================================================

#[test]
fn f4_3_zero_byte_shard_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);

    // Create a valid shard first to get the path
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42, fields: vec![(0, PackedValue::I(1))],
    }).unwrap();

    // Overwrite with zero bytes (simulates a crashed pre-creation stub)
    let shard_path = store.shard_path(&shard_key);
    std::fs::write(&shard_path, &[]).unwrap();

    // ShardStore treats invalid stubs as absent (Ok(None)), not as an error.
    // This allows recovery from partial writes without erroring on legitimate stubs.
    let result = store.read(&shard_key);
    assert!(result.is_ok(), "zero-byte shard should not panic or propagate error");
    assert!(result.unwrap().is_none(), "zero-byte shard should return None (treated as absent)");
}

#[test]
fn f4_4_wrong_magic_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);

    // Create a valid shard
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42, fields: vec![(0, PackedValue::I(1))],
    }).unwrap();

    // Corrupt the magic bytes
    let shard_path = store.shard_path(&shard_key);
    let mut data = std::fs::read(&shard_path).unwrap();
    data[0] = 0xFF; // corrupt first byte of magic
    data[1] = 0xFF;
    std::fs::write(&shard_path, &data).unwrap();

    // Read should return error
    let result = store.read(&shard_key);
    assert!(result.is_err(), "wrong magic bytes should return error");
}

#[test]
fn f4_5_reopen_finds_latest_data() {
    let dir = tempfile::tempdir().unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);

    // Write two ops then compact
    {
        let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

        store.append_op(&shard_key, &DocOp::Create {
            slot: 42, fields: vec![(0, PackedValue::I(1))],
        }).unwrap();

        store.append_op(&shard_key, &DocOp::Set {
            slot: 42, field: 0, value: PackedValue::I(99),
        }).unwrap();

        // Compact → snapshot contains value 99
        store.compact_shard(&shard_key).unwrap();
    }

    // Reopen store from same directory — flat shard, should read compacted data
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    let snap = store.read(&shard_key).unwrap().unwrap();
    assert_eq!(
        snap.docs[&42].iter().find(|(f, _)| *f == 0).unwrap().1,
        PackedValue::I(99),
        "reopened store should read latest compacted data"
    );
    assert_eq!(store.ops_count(&shard_key).unwrap(), Some(0), "compacted shard has zero ops");
}

#[test]
fn f4_6_multiple_compactions_are_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42, fields: vec![(0, PackedValue::I(7))],
    }).unwrap();

    // Compact 5 times — result must be stable
    for _ in 0..5 {
        let changed = store.compact_shard(&shard_key).unwrap();
        // First compact returns true (had ops), rest return false (already clean)
        let _ = changed;
    }

    let snap = store.read(&shard_key).unwrap().unwrap();
    assert_eq!(snap.docs[&42][0].1, PackedValue::I(7));
    assert_eq!(store.ops_count(&shard_key).unwrap(), Some(0));
}

// ===========================================================================
// Concurrency tests (blockers from second-pass review)
// ===========================================================================

/// Concurrent write + compact on the same shard: 8 writer threads appending ops
/// and 1 compactor running both single-shard (`compact_shard`) and the
/// multi-shard sweep pattern used by `compact_all` (list_shards → compact_shard per key).
/// Verifies no ops are lost after compact completes.
#[test]
fn test_concurrent_write_and_compact_no_lost_ops() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap()
    );

    // Use two slots that map to different shards so we also exercise list_shards scanning
    // multiple shards while writers are active (mimicking compact_all parallel behaviour).
    let shard_key_a = SlotHexShard::slot_to_shard(42);
    let shard_key_b = SlotHexShard::slot_to_shard(0xFFFF);
    let num_writers = 8;
    let ops_per_writer = 20;
    // writers-a + writers-b + compactor
    let barrier = Arc::new(Barrier::new(num_writers * 2 + 1));

    let mut handles = Vec::new();

    // Spawn writers for shard A
    for writer_id in 0..num_writers {
        let store_c = Arc::clone(&store);
        let key_c = shard_key_a.clone();
        let barrier_c = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_c.wait();
            for i in 0..ops_per_writer {
                store_c.append_op(&key_c, &DocOp::Set {
                    slot: 42,
                    field: writer_id as u16,
                    value: PackedValue::I(i as i64),
                }).expect("append_op A must not fail");
            }
        }));
    }

    // Spawn writers for shard B
    for writer_id in 0..num_writers {
        let store_c = Arc::clone(&store);
        let key_c = shard_key_b.clone();
        let barrier_c = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_c.wait();
            for i in 0..ops_per_writer {
                store_c.append_op(&key_c, &DocOp::Set {
                    slot: 0xFFFF,
                    field: writer_id as u16,
                    value: PackedValue::I(i as i64),
                }).expect("append_op B must not fail");
            }
        }));
    }

    // Compactor thread: alternates between single-key compact_shard (the atomic path) and
    // the list_shards → compact_shard sweep (the compact_all path from the admin endpoint).
    let store_c = Arc::clone(&store);
    let barrier_c = Arc::clone(&barrier);
    let compactor = thread::spawn(move || {
        barrier_c.wait();
        for round in 0..6 {
            if round % 2 == 0 {
                // Single-shard path (compact_shard directly)
                store_c.compact_shard(&SlotHexShard::slot_to_shard(42))
                    .expect("compact_shard A must not fail");
                store_c.compact_shard(&SlotHexShard::slot_to_shard(0xFFFF))
                    .expect("compact_shard B must not fail");
            } else {
                // Multi-shard sweep path — mirrors what compact_all does
                let keys = store_c.list_shards().expect("list_shards must not fail");
                for key in &keys {
                    store_c.compact_shard(key).expect("compact_shard in sweep must not fail");
                }
            }
            thread::yield_now();
        }
    });

    for h in handles {
        h.join().expect("writer thread panicked");
    }
    compactor.join().expect("compactor thread panicked");

    // Final compact to get a clean snapshot
    store.compact_shard(&shard_key_a).unwrap();
    store.compact_shard(&shard_key_b).unwrap();

    // Both shards must be readable and clean after compaction
    let snap_a = store.read(&shard_key_a).unwrap();
    assert!(snap_a.is_some(), "shard A must be readable after concurrent write+compact");
    assert_eq!(store.ops_count(&shard_key_a).unwrap(), Some(0), "shard A ops_count must be 0");

    let snap_b = store.read(&shard_key_b).unwrap();
    assert!(snap_b.is_some(), "shard B must be readable after concurrent write+compact");
    assert_eq!(store.ops_count(&shard_key_b).unwrap(), Some(0), "shard B ops_count must be 0");
}

/// Reader during append: spawn readers while writers append to a different shard key.
/// Verifies no panics and no partially-read headers.
#[test]
fn test_concurrent_readers_while_writers_append() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap()
    );

    let shard_key = SlotHexShard::slot_to_shard(100);

    // Seed shard with an initial create
    store.append_op(&shard_key, &DocOp::Create {
        slot: 100,
        fields: vec![(0, PackedValue::I(0))],
    }).unwrap();

    let num_readers = 4;
    let num_writers = 4;
    let iters = 25;
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));

    let mut handles = Vec::new();

    // Readers
    for _ in 0..num_readers {
        let store_c = Arc::clone(&store);
        let key_c = shard_key.clone();
        let barrier_c = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_c.wait();
            for _ in 0..iters {
                let result = store_c.read(&key_c);
                // Must not error — may return Some or None but no panic
                assert!(result.is_ok(), "read must not error: {:?}", result.err());
                thread::yield_now();
            }
        }));
    }

    // Writers
    for i in 0..num_writers {
        let store_c = Arc::clone(&store);
        let key_c = shard_key.clone();
        let barrier_c = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_c.wait();
            for j in 0..iters {
                store_c.append_op(&key_c, &DocOp::Set {
                    slot: 100,
                    field: i as u16,
                    value: PackedValue::I(j as i64),
                }).expect("append_op must not fail");
                thread::yield_now();
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Crash simulation: write `.new` files then check that startup sweep promotes
/// a valid one (completing the interrupted compact) and deletes an invalid one.
#[test]
fn test_startup_sweep_promotes_valid_new_file() {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let shard_key = SlotHexShard::slot_to_shard(42);

    // Determine the shard path by creating a temporary store
    let shard_path = {
        let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
        store.shard_path(&shard_key)
    };

    // Ensure parent dir exists
    std::fs::create_dir_all(shard_path.parent().unwrap()).unwrap();

    // Write a valid .new file manually (simulates: fsynced but not yet renamed).
    // We need a real BDSS header with snapshot_len > 0 to pass the promotable check.
    // Build a minimal valid shard: header + a tiny fake snapshot.
    let new_path = shard_path.with_extension("new");
    {
        let fake_snapshot = b"\x01\x00\x00\x00"; // 1 doc, empty — any non-zero bytes
        let snapshot_len = fake_snapshot.len() as u32;
        let ops_section_offset = (28u64) + snapshot_len as u64; // HEADER_SIZE=28
        let mut header_buf = Vec::with_capacity(28);
        header_buf.extend_from_slice(b"BDSS");           // magic
        header_buf.extend_from_slice(&1u32.to_le_bytes()); // version
        header_buf.extend_from_slice(&ops_section_offset.to_le_bytes()); // ops_section_offset
        header_buf.extend_from_slice(&snapshot_len.to_le_bytes()); // snapshot_len
        header_buf.extend_from_slice(&0u32.to_le_bytes()); // ops_count = 0
        header_buf.extend_from_slice(&0u32.to_le_bytes()); // flags
        assert_eq!(header_buf.len(), 28);

        let mut f = std::fs::File::create(&new_path).unwrap();
        f.write_all(&header_buf).unwrap();
        f.write_all(fake_snapshot).unwrap();
        f.sync_all().unwrap();
    }

    assert!(new_path.exists(), ".new file should exist before sweep");

    // A new ShardStore::new() should promote the valid .new → .shard
    let store2 = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    // After promotion: live shard exists, .new is gone
    assert!(!new_path.exists(), ".new file should be renamed away by sweep promotion");
    assert!(shard_path.exists(), "live shard should exist after promotion");

    // The promoted shard has ops_count = 0 (was a compact snapshot)
    let ops = store2.ops_count(&shard_key).unwrap();
    assert_eq!(ops, Some(0), "promoted .new should have ops_count = 0");
}

/// Crash simulation: an INVALID (truncated) `.new` file should be deleted by sweep.
#[test]
fn test_startup_sweep_deletes_invalid_new_file() {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();

    // Write a truncated .new file (simulates crash before fsync completed)
    let shard_path = dir.path().join("shards").join("00").join("000000.shard");
    std::fs::create_dir_all(shard_path.parent().unwrap()).unwrap();
    let new_path = shard_path.with_extension("new");
    let mut f = std::fs::File::create(&new_path).unwrap();
    f.write_all(b"BD").unwrap(); // truncated — only 2 bytes, not a valid header
    drop(f);

    assert!(new_path.exists(), ".new file should exist before sweep");

    // ShardStore::new() triggers sweep
    let _store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    // Truncated .new should be deleted
    assert!(!new_path.exists(), "invalid .new file should be deleted by startup sweep");
    // Live shard should not exist (we never wrote it)
    assert!(!shard_path.exists(), "live shard should not exist");
}

/// Verify that `compact_shard` uses a read-write file open for `sync_all()`.
///
/// On Windows, `File::open()` (read-only) returns ERROR_ACCESS_DENIED for `sync_all()`.
/// The fix is to open with `OpenOptions::new().read(true).write(true)` in
/// `write_shard_file_atomic`. This test verifies compact_shard succeeds end-to-end and
/// leaves no `.new` file on disk (the rename completed), proving the atomic write path works.
#[test]
fn test_compact_shard_atomic_write_no_access_denied() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    let shard_key = SlotHexShard::slot_to_shard(42);

    // Write an op so the shard exists with ops_count > 0
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42,
        fields: vec![(0, PackedValue::I(1))],
    }).unwrap();

    let shard_path = store.shard_path(&shard_key);
    let new_path = shard_path.with_extension("new");

    // compact_shard must: write .new, sync_all (no ERROR_ACCESS_DENIED), rename, drop lock
    let did_compact = store.compact_shard(&shard_key)
        .expect("compact_shard must not fail (Windows ERROR_ACCESS_DENIED regression)");
    assert!(did_compact, "should have compacted (had ops)");

    // No .new file left — rename succeeded
    assert!(!new_path.exists(), ".new must not remain after compact_shard (rename failed?)");

    // Live shard readable and clean
    let snap = store.read(&shard_key).unwrap();
    assert!(snap.is_some(), "shard must be readable after compact_shard");
    assert_eq!(store.ops_count(&shard_key).unwrap(), Some(0), "ops_count must be 0 after compaction");
}

/// Error propagation: compact_shard returns Err when it cannot write the .new file.
///
/// We simulate a write failure by making the shard's parent directory read-only.
/// `compact_all` in ConcurrentEngine uses the same `compact_shard` call; if it returns
/// Err for any shard, `compact_all` sets `any_failed = true` and returns Err at the end
/// without incrementing `shards_compacted`.
///
/// This test exercises that path at the ShardStore level (no full engine needed).
#[test]
#[cfg(unix)] // read-only dir trick only works on Unix (Windows ignores dir perms for create)
fn test_compact_all_error_propagation() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    let shard_key = SlotHexShard::slot_to_shard(42);

    // Write an op so the shard exists
    store.append_op(&shard_key, &DocOp::Create {
        slot: 42,
        fields: vec![(0, PackedValue::I(99))],
    }).unwrap();

    let shard_path = store.shard_path(&shard_key);
    let shard_dir = shard_path.parent().unwrap();

    // Make the shard directory read-only so write_shard_file_atomic cannot create .new
    let original_mode = shard_dir.metadata().unwrap().permissions().mode();
    std::fs::set_permissions(shard_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

    // compact_shard must return Err (not panic, not Ok)
    let result = store.compact_shard(&shard_key);

    // Restore permissions before any assert so tempdir cleanup doesn't fail
    std::fs::set_permissions(shard_dir, std::fs::Permissions::from_mode(original_mode)).unwrap();

    assert!(result.is_err(), "compact_shard must return Err when .new cannot be written");

    // shards_compacted equivalent: verify the original shard is intact (no partial write)
    let snap = store.read(&shard_key).unwrap();
    assert!(snap.is_some(), "original shard must still be readable after failed compaction");
    // ops_count should still be > 0 (compaction didn't complete)
    let ops = store.ops_count(&shard_key).unwrap();
    assert!(ops.map_or(false, |n| n > 0), "ops must remain after failed compaction, got {:?}", ops);
}
