//! Bitmap codecs and sharding strategies for ShardStore.
//!
//! Implements:
//! - `BitmapSnapshotCodec` — serialize/deserialize roaring bitmaps
//! - `BitmapOpCodec` — SetBit/ClearBit/BatchSet/BatchClear operations
//! - `FieldValueBucketShard` — filter bitmaps: (field, value) → hex-bucketed .fpack
//! - `SortLayerShard` — sort bitmaps: (field, bit_position) → per-field .sort
//! - `SingletonShard` — alive bitmap: single file, no sharding

use std::io;
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use crate::shard_store::{SnapshotCodec, OpCodec, ShardingStrategy};

// ---------------------------------------------------------------------------
// BitmapSnapshot — a roaring bitmap
// ---------------------------------------------------------------------------

/// A bitmap snapshot is just a RoaringBitmap.
pub type BitmapSnapshot = RoaringBitmap;

// ---------------------------------------------------------------------------
// BitmapOp — bit-level operations
// ---------------------------------------------------------------------------

/// Operations on a roaring bitmap.
#[derive(Debug, Clone)]
pub enum BitmapOp {
    /// Set a single bit.
    SetBit { bit: u32 },
    /// Clear a single bit.
    ClearBit { bit: u32 },
    /// Set multiple bits.
    BatchSet { bits: Vec<u32> },
    /// Clear multiple bits.
    BatchClear { bits: Vec<u32> },
}

// Op tags
const OP_TAG_SET_BIT: u8 = 0x01;
const OP_TAG_CLEAR_BIT: u8 = 0x02;
const OP_TAG_BATCH_SET: u8 = 0x03;
const OP_TAG_BATCH_CLEAR: u8 = 0x04;

// ---------------------------------------------------------------------------
// BitmapSnapshotCodec
// ---------------------------------------------------------------------------

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
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bitmap deserialize: {e}")))
    }

    fn empty() -> BitmapSnapshot {
        RoaringBitmap::new()
    }
}

// ---------------------------------------------------------------------------
// BitmapOpCodec
// ---------------------------------------------------------------------------

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
                for b in bits {
                    buf.extend_from_slice(&b.to_le_bytes());
                }
            }
            BitmapOp::BatchClear { bits } => {
                buf.push(OP_TAG_BATCH_CLEAR);
                buf.extend_from_slice(&(bits.len() as u32).to_le_bytes());
                for b in bits {
                    buf.extend_from_slice(&b.to_le_bytes());
                }
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
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated SetBit")
                })?);
                Ok(BitmapOp::SetBit { bit })
            }
            OP_TAG_CLEAR_BIT => {
                let bit = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated ClearBit")
                })?);
                Ok(BitmapOp::ClearBit { bit })
            }
            OP_TAG_BATCH_SET => {
                let count = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated BatchSet count")
                })?) as usize;
                let mut bits = Vec::with_capacity(count);
                let mut pos = 5;
                for _ in 0..count {
                    let b = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "truncated BatchSet bit")
                    })?);
                    pos += 4;
                    bits.push(b);
                }
                Ok(BitmapOp::BatchSet { bits })
            }
            OP_TAG_BATCH_CLEAR => {
                let count = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated BatchClear count")
                })?) as usize;
                let mut bits = Vec::with_capacity(count);
                let mut pos = 5;
                for _ in 0..count {
                    let b = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "truncated BatchClear bit")
                    })?);
                    pos += 4;
                    bits.push(b);
                }
                Ok(BitmapOp::BatchClear { bits })
            }
            tag => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown bitmap op tag: 0x{:02x}", tag),
            )),
        }
    }

    fn apply(snapshot: &mut BitmapSnapshot, op: &BitmapOp) {
        match op {
            BitmapOp::SetBit { bit } => { snapshot.insert(*bit); }
            BitmapOp::ClearBit { bit } => { snapshot.remove(*bit); }
            BitmapOp::BatchSet { bits } => {
                for b in bits {
                    snapshot.insert(*b);
                }
            }
            BitmapOp::BatchClear { bits } => {
                for b in bits {
                    snapshot.remove(*b);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sharding strategies
// ---------------------------------------------------------------------------

/// Shard key for filter bitmaps: (field_name, value).
/// Each (field, value) pair maps to a hex-bucketed .fpack file within a field directory.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterShardKey {
    pub field: String,
    pub value: u64,
}

/// Maps (field, value) to per-value filter bitmap files within hex-bucketed directories.
///
/// Layout: `{gen_root}/filter/{field}/{xx}/{value}.shard`
/// where xx = (value >> 8) & 0xFF (bucket directory), value = decimal u64.
///
/// Each value gets its own shard file (one bitmap per file). The hex bucket
/// directory keeps each directory under ~1000 files for high-cardinality fields.
pub struct FieldValueBucketShard;

impl ShardingStrategy for FieldValueBucketShard {
    type Key = FilterShardKey;

    fn shard_path(&self, key: &FilterShardKey, gen_root: &Path) -> PathBuf {
        let bucket = ((key.value >> 8) & 0xFF) as u8;
        gen_root
            .join("filter")
            .join(&key.field)
            .join(format!("{:02x}", bucket))
            .join(format!("{}.shard", key.value))
    }

    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<FilterShardKey>> {
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

            // Iterate bucket directories
            for bucket_entry in std::fs::read_dir(field_entry.path())? {
                let bucket_entry = bucket_entry?;
                if !bucket_entry.file_type()?.is_dir() {
                    continue;
                }

                // Iterate value files within bucket
                for value_entry in std::fs::read_dir(bucket_entry.path())? {
                    let value_entry = value_entry?;
                    let name = value_entry.file_name().to_string_lossy().into_owned();
                    if let Some(val_str) = name.strip_suffix(".shard") {
                        if let Ok(value) = val_str.parse::<u64>() {
                            keys.push(FilterShardKey {
                                field: field_name.clone(),
                                value,
                            });
                        }
                    }
                }
            }
        }

        Ok(keys)
    }
}

/// Shard key for sort layer bitmaps: (field_name, bit_position).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortLayerShardKey {
    pub field: String,
    pub bit_position: u8,
}

/// Maps (field, bit_position) to sort layer files.
///
/// Layout: `{gen_root}/sort/{field}/bit{NN}.shard`
pub struct SortLayerShard;

impl ShardingStrategy for SortLayerShard {
    type Key = SortLayerShardKey;

    fn shard_path(&self, key: &SortLayerShardKey, gen_root: &Path) -> PathBuf {
        gen_root
            .join("sort")
            .join(&key.field)
            .join(format!("bit{:02}.shard", key.bit_position))
    }

    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<SortLayerShardKey>> {
        let sort_dir = gen_root.join("sort");
        let mut keys = Vec::new();

        if !sort_dir.exists() {
            return Ok(keys);
        }

        for field_entry in std::fs::read_dir(&sort_dir)? {
            let field_entry = field_entry?;
            if !field_entry.file_type()?.is_dir() {
                continue;
            }
            let field_name = field_entry.file_name().to_string_lossy().into_owned();
            for bit_entry in std::fs::read_dir(field_entry.path())? {
                let bit_entry = bit_entry?;
                let name = bit_entry.file_name().to_string_lossy().into_owned();
                if let Some(rest) = name.strip_prefix("bit") {
                    if let Some(num_str) = rest.strip_suffix(".shard") {
                        if let Ok(bit_pos) = num_str.parse::<u8>() {
                            keys.push(SortLayerShardKey {
                                field: field_name.clone(),
                                bit_position: bit_pos,
                            });
                        }
                    }
                }
            }
        }

        Ok(keys)
    }
}

/// Shard key for the alive bitmap: unit type (single file, no sharding).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AliveShardKey;

/// Maps the alive bitmap to a single file.
///
/// Layout: `{gen_root}/system/alive.shard`
pub struct SingletonShard;

impl ShardingStrategy for SingletonShard {
    type Key = AliveShardKey;

    fn shard_path(&self, _key: &AliveShardKey, gen_root: &Path) -> PathBuf {
        gen_root.join("system").join("alive.shard")
    }

    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<AliveShardKey>> {
        let path = gen_root.join("system").join("alive.shard");
        if path.exists() {
            Ok(vec![AliveShardKey])
        } else {
            Ok(vec![])
        }
    }
}

// ---------------------------------------------------------------------------
// Type aliases for bitmap ShardStores
// ---------------------------------------------------------------------------

/// ShardStore for filter bitmaps (one bitmap per field+value).
pub type FilterBitmapStore = crate::shard_store::ShardStore<BitmapSnapshotCodec, BitmapOpCodec, FieldValueBucketShard>;

impl FilterBitmapStore {
    /// List all known values for a field across all generations.
    ///
    /// This is the existence set — used to eliminate disk I/O for queries
    /// on nonexistent values (<22μs check vs 30-50ms disk read).
    ///
    /// Only reads directory listings, not bitmap data.
    pub fn existence_set(&self, field: &str) -> io::Result<std::collections::HashSet<u64>> {
        let all_shards = self.list_all_shards()?;
        Ok(all_shards
            .into_iter()
            .filter(|k| k.field == field)
            .map(|k| k.value)
            .collect())
    }
}

/// ShardStore for sort layer bitmaps (one bitmap per field+bit_position).
pub type SortBitmapStore = crate::shard_store::ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SortLayerShard>;

/// ShardStore for the alive bitmap (single file).
pub type AliveBitmapStore = crate::shard_store::ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SingletonShard>;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_op_set_roundtrip() {
        let op = BitmapOp::SetBit { bit: 42 };
        let mut buf = Vec::new();
        BitmapOpCodec::encode_op(&op, &mut buf);
        let decoded = BitmapOpCodec::decode_op(&buf).unwrap();
        match decoded {
            BitmapOp::SetBit { bit } => assert_eq!(bit, 42),
            _ => panic!("expected SetBit"),
        }
    }

    #[test]
    fn test_bitmap_op_clear_roundtrip() {
        let op = BitmapOp::ClearBit { bit: 999 };
        let mut buf = Vec::new();
        BitmapOpCodec::encode_op(&op, &mut buf);
        let decoded = BitmapOpCodec::decode_op(&buf).unwrap();
        match decoded {
            BitmapOp::ClearBit { bit } => assert_eq!(bit, 999),
            _ => panic!("expected ClearBit"),
        }
    }

    #[test]
    fn test_bitmap_op_batch_set_roundtrip() {
        let op = BitmapOp::BatchSet { bits: vec![1, 5, 10, 100] };
        let mut buf = Vec::new();
        BitmapOpCodec::encode_op(&op, &mut buf);
        let decoded = BitmapOpCodec::decode_op(&buf).unwrap();
        match decoded {
            BitmapOp::BatchSet { bits } => assert_eq!(bits, vec![1, 5, 10, 100]),
            _ => panic!("expected BatchSet"),
        }
    }

    #[test]
    fn test_bitmap_snapshot_roundtrip() {
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(100);
        bm.insert(10000);

        let mut buf = Vec::new();
        BitmapSnapshotCodec::encode(&bm, &mut buf);
        let decoded = BitmapSnapshotCodec::decode(&buf).unwrap();
        assert_eq!(decoded, bm);
    }

    #[test]
    fn test_bitmap_apply_set_clear() {
        let mut bm = RoaringBitmap::new();

        BitmapOpCodec::apply(&mut bm, &BitmapOp::SetBit { bit: 42 });
        assert!(bm.contains(42));

        BitmapOpCodec::apply(&mut bm, &BitmapOp::ClearBit { bit: 42 });
        assert!(!bm.contains(42));
    }

    #[test]
    fn test_bitmap_apply_batch() {
        let mut bm = RoaringBitmap::new();

        BitmapOpCodec::apply(&mut bm, &BitmapOp::BatchSet { bits: vec![1, 2, 3, 4, 5] });
        assert_eq!(bm.len(), 5);

        BitmapOpCodec::apply(&mut bm, &BitmapOp::BatchClear { bits: vec![2, 4] });
        assert_eq!(bm.len(), 3);
        assert!(bm.contains(1));
        assert!(!bm.contains(2));
        assert!(bm.contains(3));
    }

    #[test]
    fn test_filter_shard_path() {
        let shard = FieldValueBucketShard;
        let key = FilterShardKey { field: "tagIds".into(), value: 0x0142 };
        // bucket = (0x0142 >> 8) & 0xFF = 0x01, value = 322 decimal
        let path = shard.shard_path(&key, Path::new("/data/gen_000"));
        assert_eq!(path, PathBuf::from("/data/gen_000/filter/tagIds/01/322.shard"));
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
    fn test_filter_store_full_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        let key = FilterShardKey { field: "nsfwLevel".into(), value: 1 };

        // Write a bitmap snapshot
        let mut bm = RoaringBitmap::new();
        for i in 0..100 {
            bm.insert(i);
        }
        store.write_snapshot(&key, &bm).unwrap();

        // Append ops
        store.append_op(&key, &BitmapOp::SetBit { bit: 200 }).unwrap();
        store.append_op(&key, &BitmapOp::ClearBit { bit: 50 }).unwrap();

        // Read back
        let result = store.read(&key).unwrap().unwrap();
        assert!(result.contains(200));
        assert!(!result.contains(50));
        assert!(result.contains(0));
        assert!(result.contains(99));
        assert_eq!(result.len(), 100); // 100 original - 1 cleared + 1 set = 100
    }

    #[test]
    fn test_alive_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = AliveBitmapStore::new(dir.path().to_path_buf(), SingletonShard).unwrap();

        let mut bm = RoaringBitmap::new();
        bm.insert_range(0..1000);

        store.write_snapshot(&AliveShardKey, &bm).unwrap();

        // Append ops (delete some slots)
        store.append_op(&AliveShardKey, &BitmapOp::ClearBit { bit: 42 }).unwrap();
        store.append_op(&AliveShardKey, &BitmapOp::ClearBit { bit: 999 }).unwrap();

        let result = store.read(&AliveShardKey).unwrap().unwrap();
        assert_eq!(result.len(), 998);
        assert!(!result.contains(42));
        assert!(!result.contains(999));
        assert!(result.contains(0));
        assert!(result.contains(500));
    }

    #[test]
    fn test_sort_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = SortBitmapStore::new(dir.path().to_path_buf(), SortLayerShard).unwrap();

        // Write bit layer 0 for reactionCount
        let key = SortLayerShardKey { field: "reactionCount".into(), bit_position: 0 };
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(3);
        bm.insert(5);
        store.write_snapshot(&key, &bm).unwrap();

        // Append: slot 7 now has bit 0 set
        store.append_op(&key, &BitmapOp::SetBit { bit: 7 }).unwrap();

        let result = store.read(&key).unwrap().unwrap();
        assert_eq!(result.len(), 4);
        assert!(result.contains(7));
    }

    #[test]
    fn test_filter_store_compact() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        let key = FilterShardKey { field: "nsfwLevel".into(), value: 2 };

        // Build up through ops only (no initial snapshot)
        store.append_op(&key, &BitmapOp::BatchSet { bits: vec![1, 2, 3, 4, 5] }).unwrap();
        store.append_op(&key, &BitmapOp::ClearBit { bit: 3 }).unwrap();

        assert_eq!(store.ops_count(&key).unwrap(), Some(2));

        // Compact
        store.compact_shard(&key, 0).unwrap();

        assert_eq!(store.ops_count(&key).unwrap(), Some(0));
        let result = store.read(&key).unwrap().unwrap();
        assert_eq!(result.len(), 4);
        assert!(!result.contains(3));
    }

    #[test]
    fn test_existence_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        // Write bitmaps for 3 values of nsfwLevel
        for value in [1u64, 2, 4] {
            let key = FilterShardKey { field: "nsfwLevel".into(), value };
            let mut bm = RoaringBitmap::new();
            bm.insert(value as u32);
            store.write_snapshot(&key, &bm).unwrap();
        }

        // Write 2 values of userId
        for value in [100u64, 200] {
            let key = FilterShardKey { field: "userId".into(), value };
            let mut bm = RoaringBitmap::new();
            bm.insert(value as u32);
            store.write_snapshot(&key, &bm).unwrap();
        }

        // Existence set for nsfwLevel should have 3 values
        let nsfw_set = store.existence_set("nsfwLevel").unwrap();
        assert_eq!(nsfw_set.len(), 3);
        assert!(nsfw_set.contains(&1));
        assert!(nsfw_set.contains(&2));
        assert!(nsfw_set.contains(&4));
        assert!(!nsfw_set.contains(&3));

        // Existence set for userId should have 2 values
        let user_set = store.existence_set("userId").unwrap();
        assert_eq!(user_set.len(), 2);

        // Nonexistent field → empty set
        let empty_set = store.existence_set("nonexistent").unwrap();
        assert!(empty_set.is_empty());
    }

    #[test]
    fn test_per_value_sharding_no_collision() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        // Two values that would collide under bucket-only sharding:
        // value 0x0100 and 0x0142 both have bucket = 0x01
        let key1 = FilterShardKey { field: "tags".into(), value: 0x0100 };
        let key2 = FilterShardKey { field: "tags".into(), value: 0x0142 };

        let mut bm1 = RoaringBitmap::new();
        bm1.insert(1);
        let mut bm2 = RoaringBitmap::new();
        bm2.insert(2);

        store.write_snapshot(&key1, &bm1).unwrap();
        store.write_snapshot(&key2, &bm2).unwrap();

        // Both should be independently readable
        let r1 = store.read(&key1).unwrap().unwrap();
        let r2 = store.read(&key2).unwrap().unwrap();
        assert!(r1.contains(1));
        assert!(!r1.contains(2));
        assert!(r2.contains(2));
        assert!(!r2.contains(1));
    }
}
