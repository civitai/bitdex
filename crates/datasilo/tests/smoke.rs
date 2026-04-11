//! End-to-end smoke test using a minimal counter codec.
//!
//! Exercises the full contract: typed ops → append → overlay read → compact.

use std::io;

use datasilo::{DataSilo, OpCodec, SiloConfig, SnapshotCodec};

/// Snapshot = a single i64 counter.
#[derive(Clone, Debug)]
struct Counter(i64);

#[derive(Clone, Debug)]
enum CounterOp {
    Set { key: u64, value: i64 },
    Add { key: u64, delta: i64 },
}

struct CounterSnapCodec;
struct CounterOpCodec;

impl SnapshotCodec for CounterSnapCodec {
    type Snapshot = Counter;

    fn encode(snapshot: &Counter, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&snapshot.0.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> io::Result<Counter> {
        if bytes.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short counter"));
        }
        Ok(Counter(i64::from_le_bytes(bytes[..8].try_into().unwrap())))
    }

    fn empty() -> Counter {
        Counter(0)
    }
}

impl OpCodec for CounterOpCodec {
    type Op = CounterOp;
    type Snapshot = Counter;

    fn encode_op(op: &CounterOp, buf: &mut Vec<u8>) {
        match op {
            CounterOp::Set { key, value } => {
                buf.push(0x01);
                buf.extend_from_slice(&key.to_le_bytes());
                buf.extend_from_slice(&value.to_le_bytes());
            }
            CounterOp::Add { key, delta } => {
                buf.push(0x02);
                buf.extend_from_slice(&key.to_le_bytes());
                buf.extend_from_slice(&delta.to_le_bytes());
            }
        }
    }

    fn decode_op(bytes: &[u8]) -> io::Result<CounterOp> {
        if bytes.len() < 17 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short op"));
        }
        let tag = bytes[0];
        let key = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let value = i64::from_le_bytes(bytes[9..17].try_into().unwrap());
        match tag {
            0x01 => Ok(CounterOp::Set { key, value }),
            0x02 => Ok(CounterOp::Add { key, delta: value }),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "bad tag")),
        }
    }

    fn op_key(op: &CounterOp) -> u64 {
        match op {
            CounterOp::Set { key, .. } | CounterOp::Add { key, .. } => *key,
        }
    }

    fn apply(snapshot: &mut Counter, op: &CounterOp) {
        match op {
            CounterOp::Set { value, .. } => snapshot.0 = *value,
            CounterOp::Add { delta, .. } => snapshot.0 += *delta,
        }
    }
}

type TestSilo = DataSilo<CounterSnapCodec, CounterOpCodec>;

#[test]
fn ops_overlay_read_returns_applied_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo: TestSilo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

    // Seed data.bin via bulk_load.
    silo.bulk_load(&[(1, Counter(10)), (2, Counter(20))]).unwrap();

    // Append live ops to key 1.
    silo.append_op(&CounterOp::Add { key: 1, delta: 5 }).unwrap();
    silo.append_op(&CounterOp::Add { key: 1, delta: 3 }).unwrap();

    // get() must overlay ops onto the seed (10 + 5 + 3 = 18).
    let c1 = silo.get(1).unwrap().unwrap();
    assert_eq!(c1.0, 18);

    // Untouched key returns its seed.
    let c2 = silo.get(2).unwrap().unwrap();
    assert_eq!(c2.0, 20);

    // Unknown key returns None.
    assert!(silo.get(99).unwrap().is_none());
}

#[test]
fn compact_folds_ops_into_data_file_and_subsequent_reads_need_no_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo: TestSilo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

    silo.bulk_load(&[(1, Counter(0))]).unwrap();
    silo.append_ops_batch(&[
        CounterOp::Add { key: 1, delta: 1 },
        CounterOp::Add { key: 1, delta: 2 },
        CounterOp::Add { key: 1, delta: 4 },
        CounterOp::Set { key: 1, value: 100 },
        CounterOp::Add { key: 1, delta: 1 },
    ])
    .unwrap();
    assert!(silo.has_ops());

    let folded = silo.compact().unwrap();
    assert_eq!(folded, 5);
    assert!(!silo.has_ops());

    // get_compacted (no overlay scan) should return the final value.
    let c1 = silo.get_compacted(1).unwrap().unwrap();
    assert_eq!(c1.0, 101);
}

#[test]
fn compact_creates_new_keys_not_previously_in_data_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo: TestSilo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

    // No bulk_load — data.bin doesn't exist yet.
    silo.append_op(&CounterOp::Set { key: 7, value: 42 }).unwrap();
    silo.append_op(&CounterOp::Add { key: 7, delta: 8 }).unwrap();
    silo.append_op(&CounterOp::Set { key: 11, value: 5 }).unwrap();

    silo.compact().unwrap();

    assert_eq!(silo.get_compacted(7).unwrap().unwrap().0, 50);
    assert_eq!(silo.get_compacted(11).unwrap().unwrap().0, 5);
}

#[test]
fn get_many_batches_the_overlay_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo: TestSilo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

    silo.bulk_load(&[(1, Counter(1)), (2, Counter(2)), (3, Counter(3))]).unwrap();
    silo.append_op(&CounterOp::Add { key: 2, delta: 10 }).unwrap();

    let got = silo.get_many(&[1, 2, 3, 99]).unwrap();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0].as_ref().unwrap().0, 1);
    assert_eq!(got[1].as_ref().unwrap().0, 12);
    assert_eq!(got[2].as_ref().unwrap().0, 3);
    assert!(got[3].is_none());
}

#[test]
fn ops_log_survives_reopen_and_applies_on_get() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut silo: TestSilo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
        silo.bulk_load(&[(1, Counter(0))]).unwrap();
        silo.append_op(&CounterOp::Add { key: 1, delta: 7 }).unwrap();
    }
    // Reopen — ops log should replay on next get.
    let silo: TestSilo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();
    let c1 = silo.get(1).unwrap().unwrap();
    assert_eq!(c1.0, 7);
}
