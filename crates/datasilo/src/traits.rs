//! Codec traits for DataSilo — typed snapshots + typed ops, matching the
//! ShardStore `SnapshotCodec` / `OpCodec` pattern so the silo can act as a
//! single-mmap replacement for hex-sharded doc storage.

use std::fmt;
use std::io;

/// Encodes and decodes the materialized per-key snapshot.
///
/// In DataSilo, each hash-index entry holds one encoded `Snapshot` — the
/// latest compacted state for that key. For docs, that snapshot is one
/// document's field list.
pub trait SnapshotCodec: Send + Sync + 'static {
    type Snapshot: Send + Sync + Clone + fmt::Debug;

    /// Serialize a snapshot into `buf` (append; do not clear).
    fn encode(snapshot: &Self::Snapshot, buf: &mut Vec<u8>);

    /// Deserialize a snapshot from exactly `bytes` (no length prefix).
    fn decode(bytes: &[u8]) -> io::Result<Self::Snapshot>;

    /// Return an empty / default snapshot (used when no data-file entry exists
    /// but ops for the key are present in the log).
    fn empty() -> Self::Snapshot;
}

/// Encodes, decodes, and applies a typed op to a snapshot.
///
/// `Snapshot` MUST match the paired `SnapshotCodec::Snapshot`; this is
/// enforced at the `DataSilo<S, O>` level via
/// `O: OpCodec<Snapshot = S::Snapshot>`.
pub trait OpCodec: Send + Sync + 'static {
    type Op: Send + Sync + Clone + fmt::Debug;
    type Snapshot: Send + Sync + Clone;

    /// Serialize an op into `buf` (append; do not clear).
    /// The encoded form must be self-delimiting — the ops log does NOT frame
    /// ops beyond a length prefix + CRC, so decode must consume exactly the
    /// bytes produced by encode.
    fn encode_op(op: &Self::Op, buf: &mut Vec<u8>);

    /// Deserialize an op from `bytes` (no length prefix).
    fn decode_op(bytes: &[u8]) -> io::Result<Self::Op>;

    /// Which silo key does this op target?
    ///
    /// The ops log is keyed per-op by this value so `DataSilo::get(key)` can
    /// filter the log scan down to ops for a single key. For doc silos this
    /// is `slot + 1` (avoiding the reserved key=0 sentinel).
    fn op_key(op: &Self::Op) -> u64;

    /// Apply a single op to a per-key snapshot in place.
    fn apply(snapshot: &mut Self::Snapshot, op: &Self::Op);
}
