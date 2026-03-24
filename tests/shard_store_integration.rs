//! ShardStore integration tests — cross-system E2E scenarios.
//!
//! These tests exercise the ShardStore at the integration level:
//! doc + bitmap stores working together, generation lifecycle,
//! crash recovery, compaction, and concurrent access patterns.
//!
//! Written by Ivanna (QA) for Adam to wire in as ShardStore matures.
//! Tests are designed to be pluggable — they use the public ShardStore API
//! and don't depend on ConcurrentEngine wiring.

use std::collections::HashMap;
use std::io::Read;

use roaring::RoaringBitmap;

use bitdex_v2::shard_store::ShardHeader;
use bitdex_v2::shard_store_doc::*;
use bitdex_v2::shard_store_bitmap::*;
use bitdex_v2::docstore::PackedValue;

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
    store.compact_shard(&shard_key, 0).unwrap();

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
    store.compact_shard(&key, 0).unwrap();

    // Verify post-compact: same bitmap, zero ops
    assert_eq!(store.ops_count(&key).unwrap(), Some(0));
    let bucket2 = store.read(&key).unwrap().unwrap();
    let bm2 = bucket2.values.get(&2).unwrap();
    assert_eq!(bm2, bm);
}

// ===========================================================================
// Scenario 2: Generation lifecycle
// Create gen → write → pin gen → write to new gen → fall-through → compact
// ===========================================================================

#[test]
fn test_generation_lifecycle_pin_fallthrough_compact() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    let key_a = SlotHexShard::slot_to_shard(100);
    let key_b = SlotHexShard::slot_to_shard(600); // different shard

    // Gen 0: create doc A
    store.append_op(&key_a, &DocOp::Create {
        slot: 100,
        fields: vec![(0, PackedValue::I(1))],
    }).unwrap();

    // Pin → gen 0 frozen, gen 1 current
    let frozen = store.pin_generation().unwrap();
    assert_eq!(frozen, 0);
    assert_eq!(store.current_generation(), 1);

    // Gen 1: create doc B (different shard)
    store.append_op(&key_b, &DocOp::Create {
        slot: 600,
        fields: vec![(0, PackedValue::I(99))],
    }).unwrap();

    // Gen 1: modify doc A (overwrites in new gen)
    store.append_op(&key_a, &DocOp::Set {
        slot: 100, field: 0, value: PackedValue::I(42),
    }).unwrap();

    // Read doc A: should find gen 1 version (newest first)
    let snap_a = store.read(&key_a).unwrap().unwrap();
    // Gen 1 has Set{42} on top of empty snapshot → field 0 = 42
    let doc_a = &snap_a.docs[&100];
    assert_eq!(doc_a.iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(42));

    // Read doc B: should find gen 1
    let snap_b = store.read(&key_b).unwrap().unwrap();
    assert_eq!(snap_b.docs[&600][0], (0, PackedValue::I(99)));

    // Compact gen 1: merge everything into fresh snapshots
    store.compact_generation(1).unwrap();

    // After compaction: reads still correct
    let snap_a2 = store.read(&key_a).unwrap().unwrap();
    assert_eq!(snap_a2.docs[&100].iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(42));
}

#[test]
fn test_bitmap_generation_fallthrough() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

    let key1 = FilterBucketKey::from_value("nsfwLevel".into(), 1);
    let key2 = FilterBucketKey::from_value("nsfwLevel".into(), 2);

    // Gen 0: write bitmap for value 1 via ops
    for bit in 0..50u32 {
        store.append_op(&key1, &FilterOp::SetBit { value: 1, bit }).unwrap();
    }

    // Pin to gen 1
    store.pin_generation().unwrap();

    // Gen 1: write bitmap for value 2 via ops
    for bit in 50..100u32 {
        store.append_op(&key2, &FilterOp::SetBit { value: 2, bit }).unwrap();
    }

    // value 1 falls through to gen 0
    let r1 = store.read(&key1).unwrap().unwrap();
    let bm1 = r1.values.get(&1).unwrap();
    assert_eq!(bm1.len(), 50);
    assert!(bm1.contains(0));

    // value 2 found in gen 1
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
    let shard_path = store_tmp.shard_path_in_gen(&shard_key, 0);

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
    let shard_path = store_tmp.shard_path_in_gen(&shard_key, 0);

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
fn test_existence_set_across_generations() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

    // Gen 0: values 1, 2, 3
    for v in [1u64, 2, 3] {
        let key = FilterBucketKey::from_value("tags".into(), v);
        store.append_op(&key, &FilterOp::SetBit { value: v, bit: v as u32 }).unwrap();
    }

    // Pin → gen 1
    store.pin_generation().unwrap();

    // Gen 1: values 4, 5
    for v in [4u64, 5] {
        let key = FilterBucketKey::from_value("tags".into(), v);
        store.append_op(&key, &FilterOp::SetBit { value: v, bit: v as u32 }).unwrap();
    }

    // Existence set should span both generations
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
    let shard_path = store.shard_path_in_gen(&shard_key, 0);

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

    // Overwrite with zero bytes
    let shard_path = store.shard_path_in_gen(&shard_key, 0);
    std::fs::write(&shard_path, &[]).unwrap();

    // Read should return error, not panic
    let result = store.read(&shard_key);
    assert!(result.is_err(), "zero-byte shard should return error");
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
    let shard_path = store.shard_path_in_gen(&shard_key, 0);
    let mut data = std::fs::read(&shard_path).unwrap();
    data[0] = 0xFF; // corrupt first byte of magic
    data[1] = 0xFF;
    std::fs::write(&shard_path, &data).unwrap();

    // Read should return error
    let result = store.read(&shard_key);
    assert!(result.is_err(), "wrong magic bytes should return error");
}

#[test]
fn f4_5_generation_reopen_finds_latest() {
    let dir = tempfile::tempdir().unwrap();

    let shard_key = SlotHexShard::slot_to_shard(42);

    // Create store, write to gen 0, pin to gen 1, write to gen 1
    {
        let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

        store.append_op(&shard_key, &DocOp::Create {
            slot: 42, fields: vec![(0, PackedValue::I(1))],
        }).unwrap();

        store.pin_generation().unwrap();

        store.append_op(&shard_key, &DocOp::Set {
            slot: 42, field: 0, value: PackedValue::I(99),
        }).unwrap();

        assert_eq!(store.current_generation(), 1);
    }

    // Reopen store from same directory — should find gen 1
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();
    assert_eq!(store.current_generation(), 1, "reopened store must find latest generation");

    // Should read gen 1 data
    let snap = store.read(&shard_key).unwrap().unwrap();
    assert_eq!(
        snap.docs[&42].iter().find(|(f, _)| *f == 0).unwrap().1,
        PackedValue::I(99),
        "reopened store should read latest data"
    );
}

#[test]
fn f4_6_multiple_sequential_pins() {
    let dir = tempfile::tempdir().unwrap();
    let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

    // Pin 5 times
    for expected_frozen in 0..5u64 {
        let frozen = store.pin_generation().unwrap();
        assert_eq!(frozen, expected_frozen);
    }

    assert_eq!(store.current_generation(), 5);

    // Verify all gen directories exist
    for gen in 0..=5 {
        let gen_dir = dir.path().join(format!("gen_{:03}", gen));
        assert!(gen_dir.exists(), "gen_{:03} directory must exist", gen);
    }
}
