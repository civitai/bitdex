//! Bitmap codecs and sharding strategies for ShardStore.
//!
//! Three codec pairs for three storage patterns:
//!
//! 1. **Filter bitmaps** (packed bucket): `BucketSnapshotCodec` + `FilterOpCodec`
//!    One shard file per hex bucket, containing multiple values with an index table.
//!    Ops are tagged with value_id to identify which bitmap within the bucket.
//!
//! 2. **Sort/Alive bitmaps** (single): `BitmapSnapshotCodec` + `BitmapOpCodec`
//!    One shard file per bitmap. Simple set/clear operations.
//!
//! Sharding strategies:
//! - `FieldValueBucketShard` — filter: (field, bucket) → `filter/{field}/{xx}.shard`
//! - `SortLayerShard` — sort: (field, bit_position) → `sort/{field}/bit{NN}.shard`
//! - `SingletonShard` — alive: single file → `system/alive.shard`

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use crate::shard_store::{SnapshotCodec, OpCodec, ShardingStrategy};

// ===========================================================================
// SECTION 1: Filter bitmap codecs (packed bucket — multiple values per shard)
// ===========================================================================

// ---------------------------------------------------------------------------
// BucketSnapshot — packed multi-value bitmap container
// ---------------------------------------------------------------------------

/// A bucket snapshot contains all bitmaps for values that hash to this bucket.
/// Maps value_id → RoaringBitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketSnapshot {
    pub values: HashMap<u64, RoaringBitmap>,
}

impl BucketSnapshot {
    pub fn new() -> Self {
        BucketSnapshot { values: HashMap::new() }
    }
}

// ---------------------------------------------------------------------------
// FilterOp — value-tagged bitmap operations
// ---------------------------------------------------------------------------

/// Operations on a specific value's bitmap within a bucket.
#[derive(Debug, Clone)]
pub enum FilterOp {
    /// Set a bit on a specific value's bitmap.
    SetBit { value: u64, bit: u32 },
    /// Clear a bit from a specific value's bitmap.
    ClearBit { value: u64, bit: u32 },
    /// Set multiple bits on a specific value's bitmap.
    BatchSet { value: u64, bits: Vec<u32> },
    /// Clear multiple bits from a specific value's bitmap.
    BatchClear { value: u64, bits: Vec<u32> },
}

// Filter op tags
const FILTER_OP_SET: u8 = 0x11;
const FILTER_OP_CLEAR: u8 = 0x12;
const FILTER_OP_BATCH_SET: u8 = 0x13;
const FILTER_OP_BATCH_CLEAR: u8 = 0x14;

// ---------------------------------------------------------------------------
// BucketSnapshotCodec
// ---------------------------------------------------------------------------

/// Encodes/decodes packed bucket snapshots.
///
/// Format:
/// ```text
/// [u32 num_values]
/// [index: N × (u64 value_id, u32 bitmap_offset, u32 bitmap_length)]
/// [packed serialized roaring bitmaps]
/// ```
pub struct BucketSnapshotCodec;

impl SnapshotCodec for BucketSnapshotCodec {
    type Snapshot = BucketSnapshot;

    fn encode(snapshot: &BucketSnapshot, buf: &mut Vec<u8>) {
        let count = snapshot.values.len() as u32;
        buf.extend_from_slice(&count.to_le_bytes());

        // Serialize all bitmaps first to know their sizes
        let mut bitmap_data: Vec<(u64, Vec<u8>)> = Vec::with_capacity(snapshot.values.len());
        for (&value_id, bm) in &snapshot.values {
            let mut bm_buf = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut bm_buf).expect("bitmap serialize");
            bitmap_data.push((value_id, bm_buf));
        }

        // Write index table: (value_id, offset, length) per entry
        // Index is relative to start of bitmap data section
        let mut offset: u32 = 0;
        for (value_id, bm_buf) in &bitmap_data {
            buf.extend_from_slice(&value_id.to_le_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&(bm_buf.len() as u32).to_le_bytes());
            offset += bm_buf.len() as u32;
        }

        // Write packed bitmap data
        for (_, bm_buf) in &bitmap_data {
            buf.extend_from_slice(bm_buf);
        }
    }

    fn decode(bytes: &[u8]) -> io::Result<BucketSnapshot> {
        if bytes.len() < 4 {
            return Ok(BucketSnapshot::new());
        }

        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        if count == 0 {
            return Ok(BucketSnapshot::new());
        }

        let index_size = count * 16; // 16 bytes per entry (u64 + u32 + u32)
        let index_start = 4;
        let data_start = index_start + index_size;

        let mut values = HashMap::with_capacity(count);

        for i in 0..count {
            let entry_offset = index_start + i * 16;
            let value_id = u64::from_le_bytes(
                bytes[entry_offset..entry_offset + 8].try_into().unwrap()
            );
            let bm_offset = u32::from_le_bytes(
                bytes[entry_offset + 8..entry_offset + 12].try_into().unwrap()
            ) as usize;
            let bm_length = u32::from_le_bytes(
                bytes[entry_offset + 12..entry_offset + 16].try_into().unwrap()
            ) as usize;

            let bm_start = data_start + bm_offset;
            let bm_end = bm_start + bm_length;

            if bm_end > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("bucket bitmap truncated for value {}", value_id),
                ));
            }

            let bm = RoaringBitmap::deserialize_from(&bytes[bm_start..bm_end])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bitmap: {e}")))?;
            values.insert(value_id, bm);
        }

        Ok(BucketSnapshot { values })
    }

    fn empty() -> BucketSnapshot {
        BucketSnapshot::new()
    }
}

// ---------------------------------------------------------------------------
// FilterOpCodec
// ---------------------------------------------------------------------------

/// Codec for value-tagged filter bitmap operations.
pub struct FilterOpCodec;

impl OpCodec for FilterOpCodec {
    type Op = FilterOp;
    type Snapshot = BucketSnapshot;

    fn encode_op(op: &FilterOp, buf: &mut Vec<u8>) {
        match op {
            FilterOp::SetBit { value, bit } => {
                buf.push(FILTER_OP_SET);
                buf.extend_from_slice(&value.to_le_bytes());
                buf.extend_from_slice(&bit.to_le_bytes());
            }
            FilterOp::ClearBit { value, bit } => {
                buf.push(FILTER_OP_CLEAR);
                buf.extend_from_slice(&value.to_le_bytes());
                buf.extend_from_slice(&bit.to_le_bytes());
            }
            FilterOp::BatchSet { value, bits } => {
                buf.push(FILTER_OP_BATCH_SET);
                buf.extend_from_slice(&value.to_le_bytes());
                buf.extend_from_slice(&(bits.len() as u32).to_le_bytes());
                for b in bits {
                    buf.extend_from_slice(&b.to_le_bytes());
                }
            }
            FilterOp::BatchClear { value, bits } => {
                buf.push(FILTER_OP_BATCH_CLEAR);
                buf.extend_from_slice(&value.to_le_bytes());
                buf.extend_from_slice(&(bits.len() as u32).to_le_bytes());
                for b in bits {
                    buf.extend_from_slice(&b.to_le_bytes());
                }
            }
        }
    }

    fn decode_op(bytes: &[u8]) -> io::Result<FilterOp> {
        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty filter op"));
        }

        let tag = bytes[0];
        let value = u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "truncated value_id")
        })?);

        match tag {
            FILTER_OP_SET => {
                let bit = u32::from_le_bytes(bytes[9..13].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated SetBit")
                })?);
                Ok(FilterOp::SetBit { value, bit })
            }
            FILTER_OP_CLEAR => {
                let bit = u32::from_le_bytes(bytes[9..13].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated ClearBit")
                })?);
                Ok(FilterOp::ClearBit { value, bit })
            }
            FILTER_OP_BATCH_SET => {
                let count = u32::from_le_bytes(bytes[9..13].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated count")
                })?) as usize;
                let mut bits = Vec::with_capacity(count);
                let mut pos = 13;
                for _ in 0..count {
                    let b = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "truncated bit")
                    })?);
                    pos += 4;
                    bits.push(b);
                }
                Ok(FilterOp::BatchSet { value, bits })
            }
            FILTER_OP_BATCH_CLEAR => {
                let count = u32::from_le_bytes(bytes[9..13].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated count")
                })?) as usize;
                let mut bits = Vec::with_capacity(count);
                let mut pos = 13;
                for _ in 0..count {
                    let b = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "truncated bit")
                    })?);
                    pos += 4;
                    bits.push(b);
                }
                Ok(FilterOp::BatchClear { value, bits })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown filter op tag: 0x{:02x}", other),
            )),
        }
    }

    fn apply(snapshot: &mut BucketSnapshot, op: &FilterOp) {
        match op {
            FilterOp::SetBit { value, bit } => {
                snapshot.values.entry(*value).or_insert_with(RoaringBitmap::new).insert(*bit);
            }
            FilterOp::ClearBit { value, bit } => {
                if let Some(bm) = snapshot.values.get_mut(value) {
                    bm.remove(*bit);
                }
            }
            FilterOp::BatchSet { value, bits } => {
                let bm = snapshot.values.entry(*value).or_insert_with(RoaringBitmap::new);
                for b in bits {
                    bm.insert(*b);
                }
            }
            FilterOp::BatchClear { value, bits } => {
                if let Some(bm) = snapshot.values.get_mut(value) {
                    for b in bits {
                        bm.remove(*b);
                    }
                }
            }
        }
    }
}

// ===========================================================================
// SECTION 2: Sort/Alive bitmap codecs (single bitmap per shard)
// ===========================================================================

/// A simple bitmap snapshot — just a RoaringBitmap.
pub type BitmapSnapshot = RoaringBitmap;

/// Simple bitmap operations (no value tag — one bitmap per shard).
#[derive(Debug, Clone)]
pub enum BitmapOp {
    SetBit { bit: u32 },
    ClearBit { bit: u32 },
    BatchSet { bits: Vec<u32> },
    BatchClear { bits: Vec<u32> },
}

const OP_TAG_SET_BIT: u8 = 0x01;
const OP_TAG_CLEAR_BIT: u8 = 0x02;
const OP_TAG_BATCH_SET: u8 = 0x03;
const OP_TAG_BATCH_CLEAR: u8 = 0x04;

pub struct BitmapSnapshotCodec;

impl SnapshotCodec for BitmapSnapshotCodec {
    type Snapshot = BitmapSnapshot;

    fn encode(snapshot: &BitmapSnapshot, buf: &mut Vec<u8>) {
        let start = buf.len();
        buf.resize(start + snapshot.serialized_size(), 0);
        snapshot.serialize_into(&mut buf[start..]).expect("bitmap serialize");
    }

    fn decode(bytes: &[u8]) -> io::Result<BitmapSnapshot> {
        RoaringBitmap::deserialize_from(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bitmap: {e}")))
    }

    fn empty() -> BitmapSnapshot {
        RoaringBitmap::new()
    }
}

pub struct BitmapOpCodec;

impl OpCodec for BitmapOpCodec {
    type Op = BitmapOp;
    type Snapshot = BitmapSnapshot;

    fn encode_op(op: &BitmapOp, buf: &mut Vec<u8>) {
        match op {
            BitmapOp::SetBit { bit } => {
                buf.push(OP_TAG_SET_BIT);
                buf.extend_from_slice(&bit.to_le_bytes());
            }
            BitmapOp::ClearBit { bit } => {
                buf.push(OP_TAG_CLEAR_BIT);
                buf.extend_from_slice(&bit.to_le_bytes());
            }
            BitmapOp::BatchSet { bits } => {
                buf.push(OP_TAG_BATCH_SET);
                buf.extend_from_slice(&(bits.len() as u32).to_le_bytes());
                for b in bits { buf.extend_from_slice(&b.to_le_bytes()); }
            }
            BitmapOp::BatchClear { bits } => {
                buf.push(OP_TAG_BATCH_CLEAR);
                buf.extend_from_slice(&(bits.len() as u32).to_le_bytes());
                for b in bits { buf.extend_from_slice(&b.to_le_bytes()); }
            }
        }
    }

    fn decode_op(bytes: &[u8]) -> io::Result<BitmapOp> {
        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty bitmap op"));
        }
        match bytes[0] {
            OP_TAG_SET_BIT => {
                let bit = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated")
                })?);
                Ok(BitmapOp::SetBit { bit })
            }
            OP_TAG_CLEAR_BIT => {
                let bit = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated")
                })?);
                Ok(BitmapOp::ClearBit { bit })
            }
            OP_TAG_BATCH_SET => {
                let count = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated")
                })?) as usize;
                let mut bits = Vec::with_capacity(count);
                let mut pos = 5;
                for _ in 0..count {
                    bits.push(u32::from_le_bytes(bytes[pos..pos+4].try_into().map_err(|_| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "truncated")
                    })?));
                    pos += 4;
                }
                Ok(BitmapOp::BatchSet { bits })
            }
            OP_TAG_BATCH_CLEAR => {
                let count = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated")
                })?) as usize;
                let mut bits = Vec::with_capacity(count);
                let mut pos = 5;
                for _ in 0..count {
                    bits.push(u32::from_le_bytes(bytes[pos..pos+4].try_into().map_err(|_| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "truncated")
                    })?));
                    pos += 4;
                }
                Ok(BitmapOp::BatchClear { bits })
            }
            tag => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unknown op: 0x{:02x}", tag))),
        }
    }

    fn apply(snapshot: &mut BitmapSnapshot, op: &BitmapOp) {
        match op {
            BitmapOp::SetBit { bit } => { snapshot.insert(*bit); }
            BitmapOp::ClearBit { bit } => { snapshot.remove(*bit); }
            BitmapOp::BatchSet { bits } => { for b in bits { snapshot.insert(*b); } }
            BitmapOp::BatchClear { bits } => { for b in bits { snapshot.remove(*b); } }
        }
    }
}

// ===========================================================================
// SECTION 3: Sharding strategies
// ===========================================================================

/// Shard key for filter bitmaps: (field_name, bucket).
/// The bucket is `(value >> 8) & 0xFF`. Multiple values share a bucket file.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterBucketKey {
    pub field: String,
    pub bucket: u8,
}

impl FilterBucketKey {
    /// Create a bucket key from a field name and value.
    pub fn from_value(field: String, value: u64) -> Self {
        FilterBucketKey {
            field,
            bucket: ((value >> 8) & 0xFF) as u8,
        }
    }
}

/// Maps (field, bucket) to hex-bucketed filter shard files.
///
/// Layout: `{gen_root}/filter/{field}/{xx}.shard`
/// where xx = bucket (0x00..0xFF).
///
/// Each shard contains a BucketSnapshot with all values in that bucket.
pub struct FieldValueBucketShard;

impl ShardingStrategy for FieldValueBucketShard {
    type Key = FilterBucketKey;

    fn shard_path(&self, key: &FilterBucketKey, gen_root: &Path) -> PathBuf {
        gen_root
            .join("filter")
            .join(&key.field)
            .join(format!("{:02x}.shard", key.bucket))
    }

    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<FilterBucketKey>> {
        let filter_dir = gen_root.join("filter");
        let mut keys = Vec::new();

        if !filter_dir.exists() {
            return Ok(keys);
        }

        for field_entry in std::fs::read_dir(&filter_dir)? {
            let field_entry = field_entry?;
            if !field_entry.file_type()?.is_dir() {
                continue;
            }
            let field_name = field_entry.file_name().to_string_lossy().into_owned();
            for shard_entry in std::fs::read_dir(field_entry.path())? {
                let shard_entry = shard_entry?;
                let name = shard_entry.file_name().to_string_lossy().into_owned();
                if let Some(hex_str) = name.strip_suffix(".shard") {
                    if let Ok(bucket) = u8::from_str_radix(hex_str, 16) {
                        keys.push(FilterBucketKey {
                            field: field_name.clone(),
                            bucket,
                        });
                    }
                }
            }
        }

        Ok(keys)
    }
}

/// Shard key for sort layer bitmaps.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortLayerShardKey {
    pub field: String,
    pub bit_position: u8,
}

/// Maps (field, bit_position) to sort layer files.
/// Layout: `{gen_root}/sort/{field}/bit{NN}.shard`
pub struct SortLayerShard;

impl ShardingStrategy for SortLayerShard {
    type Key = SortLayerShardKey;

    fn shard_path(&self, key: &SortLayerShardKey, gen_root: &Path) -> PathBuf {
        gen_root.join("sort").join(&key.field).join(format!("bit{:02}.shard", key.bit_position))
    }

    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<SortLayerShardKey>> {
        let sort_dir = gen_root.join("sort");
        let mut keys = Vec::new();
        if !sort_dir.exists() { return Ok(keys); }
        for field_entry in std::fs::read_dir(&sort_dir)? {
            let field_entry = field_entry?;
            if !field_entry.file_type()?.is_dir() { continue; }
            let field_name = field_entry.file_name().to_string_lossy().into_owned();
            for bit_entry in std::fs::read_dir(field_entry.path())? {
                let bit_entry = bit_entry?;
                let name = bit_entry.file_name().to_string_lossy().into_owned();
                if let Some(rest) = name.strip_prefix("bit") {
                    if let Some(num_str) = rest.strip_suffix(".shard") {
                        if let Ok(bit_pos) = num_str.parse::<u8>() {
                            keys.push(SortLayerShardKey { field: field_name.clone(), bit_position: bit_pos });
                        }
                    }
                }
            }
        }
        Ok(keys)
    }
}

/// Alive bitmap shard key (singleton).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AliveShardKey;

/// Single file for the alive bitmap.
/// Layout: `{gen_root}/system/alive.shard`
pub struct SingletonShard;

impl ShardingStrategy for SingletonShard {
    type Key = AliveShardKey;
    fn shard_path(&self, _key: &AliveShardKey, gen_root: &Path) -> PathBuf {
        gen_root.join("system").join("alive.shard")
    }
    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<AliveShardKey>> {
        if gen_root.join("system").join("alive.shard").exists() {
            Ok(vec![AliveShardKey])
        } else {
            Ok(vec![])
        }
    }
}

// ===========================================================================
// SECTION 4: Type aliases
// ===========================================================================

/// ShardStore for filter bitmaps (packed buckets — multiple values per shard).
pub type FilterBitmapStore = crate::shard_store::ShardStore<BucketSnapshotCodec, FilterOpCodec, FieldValueBucketShard>;

impl FilterBitmapStore {
    /// List all known values for a field by reading bucket snapshots.
    ///
    /// This is the existence set — used to eliminate disk I/O for queries
    /// on nonexistent values.
    pub fn existence_set(&self, field: &str) -> io::Result<std::collections::HashSet<u64>> {
        let mut values = std::collections::HashSet::new();
        let current_gen = self.current_generation();

        for gen in (0..=current_gen).rev() {
            let gen_dir = self.gen_dir(gen);
            let field_dir = gen_dir.join("filter").join(field);
            if !field_dir.exists() { continue; }

            for entry in std::fs::read_dir(&field_dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(hex_str) = name.strip_suffix(".shard") {
                    if let Ok(bucket) = u8::from_str_radix(hex_str, 16) {
                        let key = FilterBucketKey { field: field.to_string(), bucket };
                        // Read the bucket snapshot to get value IDs
                        if let Ok(Some(snap)) = self.read(&key) {
                            for &v in snap.values.keys() {
                                values.insert(v);
                            }
                        }
                    }
                }
            }
        }

        Ok(values)
    }
}

/// ShardStore for sort layer bitmaps.
pub type SortBitmapStore = crate::shard_store::ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SortLayerShard>;

/// ShardStore for the alive bitmap.
pub type AliveBitmapStore = crate::shard_store::ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SingletonShard>;

// ===========================================================================
// SECTION 5: Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Filter (packed bucket) tests ---

    #[test]
    fn test_bucket_snapshot_roundtrip() {
        let mut snap = BucketSnapshot::new();
        let mut bm1 = RoaringBitmap::new();
        bm1.insert_range(0..100);
        let mut bm2 = RoaringBitmap::new();
        bm2.insert_range(500..600);
        snap.values.insert(1, bm1);
        snap.values.insert(2, bm2);

        let mut buf = Vec::new();
        BucketSnapshotCodec::encode(&snap, &mut buf);
        let decoded = BucketSnapshotCodec::decode(&buf).unwrap();

        assert_eq!(decoded.values.len(), 2);
        assert_eq!(decoded.values[&1].len(), 100);
        assert_eq!(decoded.values[&2].len(), 100);
    }

    #[test]
    fn test_filter_op_set_roundtrip() {
        let op = FilterOp::SetBit { value: 42, bit: 999 };
        let mut buf = Vec::new();
        FilterOpCodec::encode_op(&op, &mut buf);
        let decoded = FilterOpCodec::decode_op(&buf).unwrap();
        match decoded {
            FilterOp::SetBit { value, bit } => { assert_eq!(value, 42); assert_eq!(bit, 999); }
            _ => panic!("expected SetBit"),
        }
    }

    #[test]
    fn test_filter_op_batch_roundtrip() {
        let op = FilterOp::BatchSet { value: 100, bits: vec![1, 2, 3] };
        let mut buf = Vec::new();
        FilterOpCodec::encode_op(&op, &mut buf);
        let decoded = FilterOpCodec::decode_op(&buf).unwrap();
        match decoded {
            FilterOp::BatchSet { value, bits } => {
                assert_eq!(value, 100);
                assert_eq!(bits, vec![1, 2, 3]);
            }
            _ => panic!("expected BatchSet"),
        }
    }

    #[test]
    fn test_filter_apply_ops() {
        let mut snap = BucketSnapshot::new();

        FilterOpCodec::apply(&mut snap, &FilterOp::SetBit { value: 1, bit: 42 });
        assert!(snap.values[&1].contains(42));

        FilterOpCodec::apply(&mut snap, &FilterOp::SetBit { value: 1, bit: 43 });
        assert_eq!(snap.values[&1].len(), 2);

        FilterOpCodec::apply(&mut snap, &FilterOp::ClearBit { value: 1, bit: 42 });
        assert!(!snap.values[&1].contains(42));
        assert!(snap.values[&1].contains(43));

        // Different value in same bucket
        FilterOpCodec::apply(&mut snap, &FilterOp::SetBit { value: 2, bit: 100 });
        assert_eq!(snap.values.len(), 2);
    }

    #[test]
    fn test_filter_bucket_key() {
        // Values 0x0100 and 0x0142 should be in the same bucket (0x01)
        let k1 = FilterBucketKey::from_value("tags".into(), 0x0100);
        let k2 = FilterBucketKey::from_value("tags".into(), 0x0142);
        assert_eq!(k1.bucket, k2.bucket);
        assert_eq!(k1.bucket, 0x01);
    }

    #[test]
    fn test_filter_shard_path() {
        let shard = FieldValueBucketShard;
        let key = FilterBucketKey { field: "tagIds".into(), bucket: 0x01 };
        let path = shard.shard_path(&key, Path::new("/data/gen_000"));
        assert_eq!(path, PathBuf::from("/data/gen_000/filter/tagIds/01.shard"));
    }

    #[test]
    fn test_filter_store_packed_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        // Two values in the same bucket (bucket 0x00 for small values)
        let bucket_key = FilterBucketKey::from_value("nsfwLevel".into(), 1);

        // Write ops for value=1 and value=2 (both in bucket 0x00)
        store.append_op(&bucket_key, &FilterOp::BatchSet { value: 1, bits: vec![10, 20, 30] }).unwrap();
        store.append_op(&bucket_key, &FilterOp::BatchSet { value: 2, bits: vec![40, 50] }).unwrap();
        store.append_op(&bucket_key, &FilterOp::ClearBit { value: 1, bit: 20 }).unwrap();

        // Read back — should have both values in the bucket
        let snap = store.read(&bucket_key).unwrap().unwrap();
        assert_eq!(snap.values[&1].len(), 2); // 10, 30 (20 cleared)
        assert!(snap.values[&1].contains(10));
        assert!(!snap.values[&1].contains(20));
        assert_eq!(snap.values[&2].len(), 2); // 40, 50
    }

    #[test]
    fn test_filter_store_compact() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        let key = FilterBucketKey::from_value("nsfwLevel".into(), 1);

        store.append_op(&key, &FilterOp::BatchSet { value: 1, bits: vec![1, 2, 3] }).unwrap();
        store.append_op(&key, &FilterOp::BatchSet { value: 2, bits: vec![4, 5] }).unwrap();
        store.append_op(&key, &FilterOp::ClearBit { value: 1, bit: 2 }).unwrap();

        assert_eq!(store.ops_count(&key).unwrap(), Some(3));

        store.compact_current(&key).unwrap();

        assert_eq!(store.ops_count(&key).unwrap(), Some(0));
        let snap = store.read(&key).unwrap().unwrap();
        assert_eq!(snap.values[&1].len(), 2); // 1, 3
        assert_eq!(snap.values[&2].len(), 2); // 4, 5
    }

    #[test]
    fn test_filter_no_collision_different_values_same_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        // Values 0x0100 and 0x0142 both in bucket 0x01
        let key = FilterBucketKey::from_value("tags".into(), 0x0100);

        store.append_op(&key, &FilterOp::SetBit { value: 0x0100, bit: 1 }).unwrap();
        store.append_op(&key, &FilterOp::SetBit { value: 0x0142, bit: 2 }).unwrap();

        let snap = store.read(&key).unwrap().unwrap();
        assert_eq!(snap.values.len(), 2);
        assert!(snap.values[&0x0100].contains(1));
        assert!(!snap.values[&0x0100].contains(2));
        assert!(snap.values[&0x0142].contains(2));
        assert!(!snap.values[&0x0142].contains(1));
    }

    #[test]
    fn test_existence_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        // Write bitmaps for 3 values of nsfwLevel (all in bucket 0x00)
        let key = FilterBucketKey::from_value("nsfwLevel".into(), 1);
        store.append_op(&key, &FilterOp::SetBit { value: 1, bit: 0 }).unwrap();
        store.append_op(&key, &FilterOp::SetBit { value: 2, bit: 0 }).unwrap();
        store.append_op(&key, &FilterOp::SetBit { value: 4, bit: 0 }).unwrap();

        let set = store.existence_set("nsfwLevel").unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains(&1));
        assert!(set.contains(&2));
        assert!(set.contains(&4));
        assert!(!set.contains(&3));

        // Nonexistent field
        assert!(store.existence_set("nonexistent").unwrap().is_empty());
    }

    // --- Sort/Alive (simple bitmap) tests ---

    #[test]
    fn test_bitmap_snapshot_roundtrip() {
        let mut bm = RoaringBitmap::new();
        bm.insert(1); bm.insert(100); bm.insert(10000);
        let mut buf = Vec::new();
        BitmapSnapshotCodec::encode(&bm, &mut buf);
        let decoded = BitmapSnapshotCodec::decode(&buf).unwrap();
        assert_eq!(decoded, bm);
    }

    #[test]
    fn test_bitmap_op_roundtrip() {
        let op = BitmapOp::SetBit { bit: 42 };
        let mut buf = Vec::new();
        BitmapOpCodec::encode_op(&op, &mut buf);
        match BitmapOpCodec::decode_op(&buf).unwrap() {
            BitmapOp::SetBit { bit } => assert_eq!(bit, 42),
            _ => panic!("expected SetBit"),
        }
    }

    #[test]
    fn test_bitmap_apply() {
        let mut bm = RoaringBitmap::new();
        BitmapOpCodec::apply(&mut bm, &BitmapOp::BatchSet { bits: vec![1, 2, 3, 4, 5] });
        assert_eq!(bm.len(), 5);
        BitmapOpCodec::apply(&mut bm, &BitmapOp::BatchClear { bits: vec![2, 4] });
        assert_eq!(bm.len(), 3);
    }

    #[test]
    fn test_sort_layer_shard_path() {
        let shard = SortLayerShard;
        let key = SortLayerShardKey { field: "reactionCount".into(), bit_position: 15 };
        let path = shard.shard_path(&key, Path::new("/data/gen_000"));
        assert_eq!(path, PathBuf::from("/data/gen_000/sort/reactionCount/bit15.shard"));
    }

    #[test]
    fn test_alive_shard_path() {
        let shard = SingletonShard;
        let path = shard.shard_path(&AliveShardKey, Path::new("/data/gen_000"));
        assert_eq!(path, PathBuf::from("/data/gen_000/system/alive.shard"));
    }

    #[test]
    fn test_sort_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = SortBitmapStore::new(dir.path().to_path_buf(), SortLayerShard).unwrap();
        let key = SortLayerShardKey { field: "reactionCount".into(), bit_position: 0 };
        let mut bm = RoaringBitmap::new();
        bm.insert(1); bm.insert(3); bm.insert(5);
        store.write_snapshot(&key, &bm).unwrap();
        store.append_op(&key, &BitmapOp::SetBit { bit: 7 }).unwrap();
        let result = store.read(&key).unwrap().unwrap();
        assert_eq!(result.len(), 4);
        assert!(result.contains(7));
    }

    #[test]
    fn test_alive_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = AliveBitmapStore::new(dir.path().to_path_buf(), SingletonShard).unwrap();
        let mut bm = RoaringBitmap::new();
        bm.insert_range(0..1000);
        store.write_snapshot(&AliveShardKey, &bm).unwrap();
        store.append_op(&AliveShardKey, &BitmapOp::ClearBit { bit: 42 }).unwrap();
        store.append_op(&AliveShardKey, &BitmapOp::ClearBit { bit: 999 }).unwrap();
        let result = store.read(&AliveShardKey).unwrap().unwrap();
        assert_eq!(result.len(), 998);
        assert!(!result.contains(42));
    }
}
