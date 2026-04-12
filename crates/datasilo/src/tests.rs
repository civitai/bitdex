//! Correctness tests for DataSilo — test plan: docs/design/docsilo-test-plan.md
//!
//! Uses a simple byte-blob codec (TestSnap / TestOp) so we can test the silo
//! engine in isolation without pulling in the full doc wire format.

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::Ordering;
    use crate::{DataSilo, SiloConfig, IndexEntry};
    use crate::traits::{SnapshotCodec, OpCodec};

    // ── Test codec: raw bytes with LWW put + append semantics ──────────

    /// Snapshot = raw bytes (Vec<u8>).
    #[derive(Clone, Debug, PartialEq)]
    struct TestSnap(Vec<u8>);

    struct TestSnapCodec;
    impl SnapshotCodec for TestSnapCodec {
        type Snapshot = TestSnap;
        fn encode(snap: &TestSnap, buf: &mut Vec<u8>) { buf.extend_from_slice(&snap.0); }
        fn decode(bytes: &[u8]) -> io::Result<TestSnap> { Ok(TestSnap(bytes.to_vec())) }
        fn empty() -> TestSnap { TestSnap(Vec::new()) }
    }

    /// Op = (key: u64, value: Vec<u8>). Apply = overwrite snapshot with op value.
    #[derive(Clone, Debug)]
    struct TestOp { key: u64, value: Vec<u8> }

    struct TestOpCodec;
    impl OpCodec for TestOpCodec {
        type Op = TestOp;
        type Snapshot = TestSnap;
        fn encode_op(op: &TestOp, buf: &mut Vec<u8>) {
            buf.extend_from_slice(&op.key.to_le_bytes());
            buf.extend_from_slice(&(op.value.len() as u32).to_le_bytes());
            buf.extend_from_slice(&op.value);
        }
        fn decode_op(bytes: &[u8]) -> io::Result<TestOp> {
            if bytes.len() < 12 { return Err(io::Error::new(io::ErrorKind::InvalidData, "short")); }
            let key = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
            let value = bytes[12..12+len].to_vec();
            Ok(TestOp { key, value })
        }
        fn op_key(op: &TestOp) -> u64 { op.key }
        fn apply(snap: &mut TestSnap, op: &TestOp) { snap.0 = op.value.clone(); }
    }

    type TestSilo = DataSilo<TestSnapCodec, TestOpCodec>;

    fn open_temp() -> (tempfile::TempDir, TestSilo) {
        let dir = tempfile::tempdir().unwrap();
        let silo = TestSilo::open(dir.path(), SiloConfig::default()).unwrap();
        (dir, silo)
    }

    // ── 1. Basic CRUD ──────────────────────────────────────────────────

    #[test]
    fn test_put_get_roundtrip() {
        let (dir, mut silo) = open_temp();
        let data = b"hello world";
        silo.bulk_load(&[(1, TestSnap(data.to_vec()))]).unwrap();
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(got.0, data);
    }

    #[test]
    fn test_put_overwrite_via_ops() {
        let (dir, mut silo) = open_temp();
        silo.bulk_load(&[(1, TestSnap(b"v1".to_vec()))]).unwrap();
        silo.append_op(&TestOp { key: 1, value: b"v2".to_vec() }).unwrap();
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(got.0, b"v2");
    }

    #[test]
    fn test_get_missing_key() {
        let (_dir, silo) = open_temp();
        assert!(silo.get(999).unwrap().is_none());
    }

    #[test]
    fn test_put_many_get_many() {
        let (_dir, mut silo) = open_temp();
        let entries: Vec<(u64, TestSnap)> = (1..=1000)
            .map(|i| (i, TestSnap(format!("doc-{i}").into_bytes())))
            .collect();
        silo.bulk_load(&entries).unwrap();
        let keys: Vec<u64> = (1..=1000).collect();
        let results = silo.get_many(&keys).unwrap();
        for (i, r) in results.iter().enumerate() {
            let expected = format!("doc-{}", i + 1);
            assert_eq!(r.as_ref().unwrap().0, expected.as_bytes());
        }
    }

    // ── 2. Bulk load ───────────────────────────────────────────────────

    #[test]
    fn test_bulk_load_parallel() {
        let (_dir, mut silo) = open_temp();
        let entries: Vec<(u64, TestSnap)> = (1..=10_000)
            .map(|i| (i, TestSnap(format!("entry-{i}").into_bytes())))
            .collect();
        let loaded = silo.bulk_load(&entries).unwrap();
        assert_eq!(loaded, 10_000);
        // Spot-check
        assert_eq!(silo.get(1).unwrap().unwrap().0, b"entry-1");
        assert_eq!(silo.get(10_000).unwrap().unwrap().0, b"entry-10000");
    }

    #[test]
    fn test_bulk_load_then_ops_update() {
        let (_dir, mut silo) = open_temp();
        silo.bulk_load(&[(1, TestSnap(b"original".to_vec()))]).unwrap();
        silo.append_op(&TestOp { key: 1, value: b"updated".to_vec() }).unwrap();
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(got.0, b"updated");
    }

    // ── 3. Ops log ─────────────────────────────────────────────────────

    #[test]
    fn test_ops_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut silo = TestSilo::open(dir.path(), SiloConfig::default()).unwrap();
            silo.bulk_load(&[(1, TestSnap(b"base".to_vec()))]).unwrap();
            silo.append_op(&TestOp { key: 1, value: b"op-value".to_vec() }).unwrap();
        }
        // Reopen
        let silo = TestSilo::open(dir.path(), SiloConfig::default()).unwrap();
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(got.0, b"op-value");
    }

    #[test]
    fn test_get_applies_pending_ops() {
        let (_dir, mut silo) = open_temp();
        silo.bulk_load(&[(1, TestSnap(b"base".to_vec()))]).unwrap();
        // Write op but don't compact
        silo.append_op(&TestOp { key: 1, value: b"pending".to_vec() }).unwrap();
        // get() must return the ops-applied value, not the stale snapshot
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(got.0, b"pending");
        // get_compacted() should still return the base
        let base = silo.get_compacted(1).unwrap().unwrap();
        assert_eq!(base.0, b"base");
    }

    // ── 4. Compaction ──────────────────────────────────────────────────

    #[test]
    fn test_compact_merges_ops() {
        let (_dir, mut silo) = open_temp();
        silo.bulk_load(&[(1, TestSnap(b"base".to_vec()))]).unwrap();
        silo.append_op(&TestOp { key: 1, value: b"updated".to_vec() }).unwrap();
        assert!(silo.has_ops());
        silo.compact().unwrap();
        // After compact, get_compacted should return the merged value
        let got = silo.get_compacted(1).unwrap().unwrap();
        assert_eq!(got.0, b"updated");
    }

    #[test]
    fn test_compact_preserves_all_data() {
        let (_dir, mut silo) = open_temp();
        let entries: Vec<(u64, TestSnap)> = (1..=100)
            .map(|i| (i, TestSnap(format!("doc-{i}").into_bytes())))
            .collect();
        silo.bulk_load(&entries).unwrap();
        // Add some ops
        for i in 1..=10u64 {
            silo.append_op(&TestOp { key: i, value: format!("op-{i}").into_bytes() }).unwrap();
        }
        silo.compact().unwrap();
        // All 100 entries still readable
        for i in 1..=100u64 {
            let got = silo.get(i).unwrap().unwrap();
            if i <= 10 {
                assert_eq!(got.0, format!("op-{i}").into_bytes());
            } else {
                assert_eq!(got.0, format!("doc-{i}").into_bytes());
            }
        }
    }

    // ── 5. HashIndex ───────────────────────────────────────────────────

    #[test]
    fn test_hash_index_build_and_lookup() {
        use crate::hash_index::HashIndex;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let entries: Vec<(u64, IndexEntry)> = (1..=100)
            .map(|i| (i, IndexEntry { offset: i * 100, length: 50, allocated: 100 }))
            .collect();
        HashIndex::build_bulk(&path, &entries).unwrap();
        let idx = HashIndex::open(&path).unwrap();
        for i in 1..=100u64 {
            let e = idx.get(i).unwrap();
            assert_eq!(e.offset, i * 100);
            assert_eq!(e.length, 50);
        }
    }

    #[test]
    fn test_hash_index_update_existing() {
        use crate::hash_index::HashIndex;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let entries = vec![(42, IndexEntry { offset: 1000, length: 50, allocated: 200 })];
        HashIndex::build_bulk(&path, &entries).unwrap();
        let idx = HashIndex::open(&path).unwrap();
        // Update length in-place
        unsafe {
            idx.update_existing_concurrent(42, IndexEntry { offset: 1000, length: 120, allocated: 200 });
        }
        let e = idx.get(42).unwrap();
        assert_eq!(e.length, 120);
        assert_eq!(e.offset, 1000); // unchanged
        assert_eq!(e.allocated, 200); // unchanged
    }

    #[test]
    fn test_hash_index_collision_handling() {
        use crate::hash_index::HashIndex;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        // Build with enough entries to force collisions at default load factor
        let entries: Vec<(u64, IndexEntry)> = (1..=500)
            .map(|i| (i, IndexEntry { offset: i * 10, length: 5, allocated: 10 }))
            .collect();
        HashIndex::build_bulk(&path, &entries).unwrap();
        let idx = HashIndex::open(&path).unwrap();
        // All must be retrievable despite collisions
        for i in 1..=500u64 {
            let e = idx.get(i).expect(&format!("key {i} not found"));
            assert_eq!(e.offset, i * 10);
        }
    }

    // ── 6. DumpMergeWriter ───────────────────────────────────────────

    fn open_with_buffer_ratio(ratio: f32) -> (tempfile::TempDir, TestSilo) {
        let dir = tempfile::tempdir().unwrap();
        let config = SiloConfig {
            buffer_ratio: ratio,
            ..SiloConfig::default()
        };
        let silo = TestSilo::open(dir.path(), config).unwrap();
        (dir, silo)
    }

    #[test]
    fn test_merge_put_combines_data() {
        let (_dir, mut silo) = open_with_buffer_ratio(4.0);
        silo.bulk_load(&[(1, TestSnap(b"hello".to_vec()))]).unwrap();
        let mut writer = silo.prepare_dump_merge().unwrap().unwrap();
        let ok = writer.merge_put(1, b" world", |existing, new| {
            let mut merged = existing.to_vec();
            merged.extend_from_slice(new);
            merged
        });
        assert!(ok);
        writer.flush().unwrap();
        drop(writer);
        silo.reload_data().unwrap();
        let got = silo.get_compacted(1).unwrap().unwrap();
        assert_eq!(got.0, b"hello world");
    }

    #[test]
    fn test_merge_put_fits_in_buffer() {
        let (_dir, mut silo) = open_with_buffer_ratio(4.0);
        silo.bulk_load(&[(1, TestSnap(b"base".to_vec()))]).unwrap();
        let writer = silo.prepare_dump_merge().unwrap().unwrap();
        let ok = writer.merge_put(1, b"extra", |existing, new| {
            let mut m = existing.to_vec();
            m.extend_from_slice(new);
            m
        });
        assert!(ok);
        assert_eq!(writer.in_place_count.load(Ordering::Relaxed), 1);
        assert_eq!(writer.overflow_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_merge_put_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let config = SiloConfig {
            buffer_ratio: 1.0,
            min_entry_size: 1, // prevent min_entry_size from adding headroom
            ..SiloConfig::default()
        };
        let mut silo = TestSilo::open(dir.path(), config).unwrap();
        // Use data larger than any min_entry_size default
        let big = vec![0xABu8; 2048];
        silo.bulk_load(&[(1, TestSnap(big))]).unwrap();
        let writer = silo.prepare_dump_merge().unwrap().unwrap();
        // Try to merge another 2048 bytes — should overflow since allocated == 2048
        let ok = writer.merge_put(1, &[0xCDu8; 2048], |existing, new| {
            let mut m = existing.to_vec();
            m.extend_from_slice(new);
            m
        });
        assert!(!ok, "should overflow when merged data exceeds allocated");
        assert_eq!(writer.overflow_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_merge_put_empty_slot() {
        let (_dir, mut silo) = open_with_buffer_ratio(4.0);
        // Bulk load with an empty snapshot
        silo.bulk_load(&[(1, TestSnap(Vec::new()))]).unwrap();
        let mut writer = silo.prepare_dump_merge().unwrap().unwrap();
        let ok = writer.merge_put(1, b"fresh", |_existing, _new| {
            panic!("merge_fn should NOT be called for empty slots");
        });
        assert!(ok);
        writer.flush().unwrap();
        drop(writer);
        silo.reload_data().unwrap();
        let got = silo.get_compacted(1).unwrap().unwrap();
        assert_eq!(got.0, b"fresh");
    }

    #[test]
    fn test_merge_put_concurrent() {
        let (_dir, mut silo) = open_with_buffer_ratio(4.0);
        let entries: Vec<(u64, TestSnap)> = (1..=1000)
            .map(|i| (i, TestSnap(format!("v{i}").into_bytes())))
            .collect();
        silo.bulk_load(&entries).unwrap();
        let writer = silo.prepare_dump_merge().unwrap().unwrap();
        use rayon::prelude::*;
        (1..=1000u64).into_par_iter().for_each(|i| {
            let ok = writer.merge_put(i, b"+merged", |existing, new| {
                let mut m = existing.to_vec();
                m.extend_from_slice(new);
                m
            });
            assert!(ok, "key {i} merge failed");
        });
        assert_eq!(writer.in_place_count.load(Ordering::Relaxed), 1000);
        assert_eq!(writer.overflow_count.load(Ordering::Relaxed), 0);
    }
}
