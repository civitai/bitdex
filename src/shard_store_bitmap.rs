//! Bitmap codecs and sharding strategies for ShardStore.
//!
//! Codec pairs for storage patterns:
//!
//! 1. **Filter bitmaps** (packed bucket): `BucketSnapshotCodec` + `FilterOpCodec`
//!    One shard file per hex bucket, containing multiple values with an index table.
//!    Ops are tagged with value_id to identify which bitmap within the bucket.
//!
//! 2. **Alive bitmaps** (single): `BitmapSnapshotCodec` + `BitmapOpCodec`
//!    One shard file per bitmap. Simple set/clear operations.
//!
//! 3. **Sort bitmaps** (packed field): `SortFieldSnapshotCodec` + `SortLayerOpCodec`
//!    One shard file per sort field, containing all bit layers in a packed index.
//!    Ops are tagged with bit_position to target individual layers.
//!
//! Sharding strategies:
//! - `FieldValueBucketShard` — filter: (field, bucket) → `filter/{field}/{xx}.shard`
//! - `SortFieldShard` — sort: field → `sort/{field}.shard` (all layers packed)
//! - `SortLayerShard` — sort (legacy per-layer ops): (field, bit_position) → `sort/{field}/bit{NN}.shard`
//! - `SingletonShard` — alive: single file → `system/alive.shard`

use std::collections::BTreeMap;
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
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
// SECTION 3: Sort field packed codecs (all bit layers in one shard per field)
// ===========================================================================

// ---------------------------------------------------------------------------
// SortFieldSnapshot — packed multi-layer bitmap container
// ---------------------------------------------------------------------------

/// A sort field snapshot contains all bit-layer bitmaps for one sort field.
/// Maps bit_position → RoaringBitmap. Only non-empty layers are stored.
#[derive(Debug, Clone, PartialEq)]
pub struct SortFieldSnapshot {
    pub layers: BTreeMap<u8, RoaringBitmap>,
}

impl SortFieldSnapshot {
    pub fn new() -> Self {
        SortFieldSnapshot { layers: BTreeMap::new() }
    }
}

// ---------------------------------------------------------------------------
// SortLayerOp — bit-position-tagged sort layer operations
// ---------------------------------------------------------------------------

/// Operations on a specific bit layer's bitmap within a sort field shard.
#[derive(Debug, Clone)]
pub enum SortLayerOp {
    /// Set a slot bit on a specific layer's bitmap.
    SetBit { bit_position: u8, slot: u32 },
    /// Clear a slot bit from a specific layer's bitmap.
    ClearBit { bit_position: u8, slot: u32 },
}

const SORT_LAYER_OP_SET: u8 = 0x21;
const SORT_LAYER_OP_CLEAR: u8 = 0x22;

// ---------------------------------------------------------------------------
// SortFieldSnapshotCodec
// ---------------------------------------------------------------------------

/// Encodes/decodes packed sort field snapshots containing all bit layers.
///
/// Format:
/// ```text
/// [u8 num_layers]
/// [index: N × (u8 bit_position, u32 offset, u32 length)]  // 9 bytes per layer
/// [packed serialized roaring bitmaps]
/// ```
///
/// Only non-empty layers are stored. On decode, missing layers are treated
/// as empty bitmaps (not inserted into the BTreeMap).
pub struct SortFieldSnapshotCodec;

impl SnapshotCodec for SortFieldSnapshotCodec {
    type Snapshot = SortFieldSnapshot;

    fn encode(snapshot: &SortFieldSnapshot, buf: &mut Vec<u8>) {
        Self::encode_from_layers(snapshot.layers.iter().map(|(&pos, bm)| (pos, bm)), buf);
    }

    fn decode(bytes: &[u8]) -> io::Result<SortFieldSnapshot> {
        if bytes.is_empty() {
            return Ok(SortFieldSnapshot::new());
        }

        let num_layers = bytes[0] as usize;
        if num_layers == 0 {
            return Ok(SortFieldSnapshot::new());
        }

        let index_start = 1;
        let index_size = num_layers * 9; // 9 bytes per entry (u8 + u32 + u32)
        let data_start = index_start + index_size;

        if bytes.len() < data_start {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "sort field snapshot index truncated",
            ));
        }

        let mut layers = BTreeMap::new();

        for i in 0..num_layers {
            let entry_offset = index_start + i * 9;
            let bit_position = bytes[entry_offset];
            let bm_offset = u32::from_le_bytes(
                bytes[entry_offset + 1..entry_offset + 5].try_into().unwrap(),
            ) as usize;
            let bm_length = u32::from_le_bytes(
                bytes[entry_offset + 5..entry_offset + 9].try_into().unwrap(),
            ) as usize;

            let bm_start = data_start + bm_offset;
            let bm_end = bm_start + bm_length;

            if bm_end > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("sort layer bitmap truncated for bit_position {}", bit_position),
                ));
            }

            let bm = RoaringBitmap::deserialize_from(&bytes[bm_start..bm_end])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bitmap: {e}")))?;
            layers.insert(bit_position, bm);
        }

        Ok(SortFieldSnapshot { layers })
    }

    fn empty() -> SortFieldSnapshot {
        SortFieldSnapshot::new()
    }
}

impl SortFieldSnapshotCodec {
    /// Encode from an iterator of (bit_position, &bitmap) pairs.
    /// Used by write_sort_layers to avoid constructing a SortFieldSnapshot.
    pub fn encode_from_layers<'a>(
        layers: impl Iterator<Item = (u8, &'a RoaringBitmap)>,
        buf: &mut Vec<u8>,
    ) {
        // Serialize all non-empty bitmaps first to know their sizes
        let mut bitmap_data: Vec<(u8, Vec<u8>)> = Vec::new();
        for (pos, bm) in layers {
            if bm.is_empty() {
                continue;
            }
            let mut bm_buf = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut bm_buf).expect("bitmap serialize");
            bitmap_data.push((pos, bm_buf));
        }

        // Write number of non-empty layers
        buf.push(bitmap_data.len() as u8);

        // Write index: (bit_position, offset, length) per entry
        let mut offset: u32 = 0;
        for (pos, bm_buf) in &bitmap_data {
            buf.push(*pos);
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&(bm_buf.len() as u32).to_le_bytes());
            offset += bm_buf.len() as u32;
        }

        // Write packed bitmap data
        for (_, bm_buf) in &bitmap_data {
            buf.extend_from_slice(bm_buf);
        }
    }
}

// ---------------------------------------------------------------------------
// SortLayerOpCodec
// ---------------------------------------------------------------------------

/// Codec for bit-position-tagged sort layer operations.
///
/// Each op is 6 bytes: [u8 op_type][u8 bit_position][u32 slot]
pub struct SortLayerOpCodec;

impl OpCodec for SortLayerOpCodec {
    type Op = SortLayerOp;
    type Snapshot = SortFieldSnapshot;

    fn encode_op(op: &SortLayerOp, buf: &mut Vec<u8>) {
        match op {
            SortLayerOp::SetBit { bit_position, slot } => {
                buf.push(SORT_LAYER_OP_SET);
                buf.push(*bit_position);
                buf.extend_from_slice(&slot.to_le_bytes());
            }
            SortLayerOp::ClearBit { bit_position, slot } => {
                buf.push(SORT_LAYER_OP_CLEAR);
                buf.push(*bit_position);
                buf.extend_from_slice(&slot.to_le_bytes());
            }
        }
    }

    fn decode_op(bytes: &[u8]) -> io::Result<SortLayerOp> {
        if bytes.len() < 6 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "sort layer op too short"));
        }

        let tag = bytes[0];
        let bit_position = bytes[1];
        let slot = u32::from_le_bytes(
            bytes[2..6].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated slot")
            })?,
        );

        match tag {
            SORT_LAYER_OP_SET => Ok(SortLayerOp::SetBit { bit_position, slot }),
            SORT_LAYER_OP_CLEAR => Ok(SortLayerOp::ClearBit { bit_position, slot }),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown sort layer op tag: 0x{:02x}", other),
            )),
        }
    }

    fn apply(snapshot: &mut SortFieldSnapshot, op: &SortLayerOp) {
        match op {
            SortLayerOp::SetBit { bit_position, slot } => {
                snapshot.layers.entry(*bit_position)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(*slot);
            }
            SortLayerOp::ClearBit { bit_position, slot } => {
                if let Some(bm) = snapshot.layers.get_mut(bit_position) {
                    bm.remove(*slot);
                }
            }
        }
    }
}

// ===========================================================================
// SECTION 4: Sharding strategies
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
/// Layout: `{root}/filter/{field}/{xx}.shard`
/// where xx = bucket (0x00..0xFF).
///
/// Each shard contains a BucketSnapshot with all values in that bucket.
pub struct FieldValueBucketShard;

impl ShardingStrategy for FieldValueBucketShard {
    type Key = FilterBucketKey;

    fn shard_path(&self, key: &FilterBucketKey, root: &Path) -> PathBuf {
        root
            .join("filter")
            .join(&key.field)
            .join(format!("{:02x}.shard", key.bucket))
    }

    fn list_shards(&self, root: &Path) -> io::Result<Vec<FilterBucketKey>> {
        let filter_dir = root.join("filter");
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
/// Layout: `{root}/sort/{field}/bit{NN}.shard`
pub struct SortLayerShard;

impl ShardingStrategy for SortLayerShard {
    type Key = SortLayerShardKey;

    fn shard_path(&self, key: &SortLayerShardKey, root: &Path) -> PathBuf {
        root.join("sort").join(&key.field).join(format!("bit{:02}.shard", key.bit_position))
    }

    fn list_shards(&self, root: &Path) -> io::Result<Vec<SortLayerShardKey>> {
        let sort_dir = root.join("sort");
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

/// Shard key for packed sort field bitmaps (one file per sort field).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortFieldShardKey {
    pub field: String,
}

/// Maps field name to a single packed sort shard file.
/// Layout: `{root}/sort/{field}.shard`
///
/// All bit layers for the field are packed into one file using SortFieldSnapshotCodec.
pub struct SortFieldShard;

impl ShardingStrategy for SortFieldShard {
    type Key = SortFieldShardKey;

    fn shard_path(&self, key: &SortFieldShardKey, root: &Path) -> PathBuf {
        root.join("sort").join(format!("{}.shard", key.field))
    }

    fn list_shards(&self, root: &Path) -> io::Result<Vec<SortFieldShardKey>> {
        let sort_dir = root.join("sort");
        let mut keys = Vec::new();
        if !sort_dir.exists() { return Ok(keys); }
        for entry in std::fs::read_dir(&sort_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            // Only match files (not directories — those are legacy per-layer layout)
            if entry.file_type()?.is_file() {
                if let Some(field) = name.strip_suffix(".shard") {
                    keys.push(SortFieldShardKey { field: field.to_string() });
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
/// Layout: `{root}/system/alive.shard`
pub struct SingletonShard;

impl ShardingStrategy for SingletonShard {
    type Key = AliveShardKey;
    fn shard_path(&self, _key: &AliveShardKey, root: &Path) -> PathBuf {
        root.join("system").join("alive.shard")
    }
    fn list_shards(&self, root: &Path) -> io::Result<Vec<AliveShardKey>> {
        if root.join("system").join("alive.shard").exists() {
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
    pub fn existence_set(&self, field: &str) -> io::Result<HashSet<u64>> {
        let mut values = HashSet::new();
        let field_dir = self.root().join("filter").join(field);
        if !field_dir.exists() { return Ok(values); }

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

        Ok(values)
    }

    /// Load all bitmaps for a field, merging all buckets into a flat map.
    ///
    /// Replaces legacy BitmapFs::load_field(). Reads all bucket shards for the
    /// field and collects value→bitmap entries into a single HashMap.
    pub fn load_field(&self, field: &str) -> io::Result<HashMap<u64, RoaringBitmap>> {
        let mut result = HashMap::new();
        let field_dir = self.root().join("filter").join(field);
        if !field_dir.exists() { return Ok(result); }

        for entry in std::fs::read_dir(&field_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(hex_str) = name.strip_suffix(".shard") {
                if let Ok(bucket) = u8::from_str_radix(hex_str, 16) {
                    let key = FilterBucketKey { field: field.to_string(), bucket };
                    if let Some(snap) = self.read(&key)? {
                        for (value, bm) in snap.values {
                            result.entry(value).or_insert(bm);
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Load specific values for a field. Only reads the bucket shards that
    /// contain the requested values, then extracts just those entries.
    ///
    /// Uses a positioned-read fast path that exploits the in-shard index
    /// (`[u32 count][N × (u64 value, u32 offset, u32 length)][packed bitmaps]`)
    /// to read only the wanted values' bytes — instead of deserializing every
    /// bitmap in the bucket via `ShardStore::read()`. On any I/O or decode
    /// failure the call falls back to the legacy full-bucket read so callers
    /// never see a regression.
    ///
    /// Replaces legacy BitmapFs::load_field_values().
    pub fn load_field_values(&self, field: &str, values: &[u64]) -> io::Result<HashMap<u64, RoaringBitmap>> {
        // Group requested values by bucket
        let mut by_bucket: HashMap<u8, Vec<u64>> = HashMap::new();
        for &v in values {
            let bucket = ((v >> 8) & 0xFF) as u8;
            by_bucket.entry(bucket).or_default().push(v);
        }

        let mut result = HashMap::new();
        for (bucket, wanted) in by_bucket {
            let key = FilterBucketKey { field: field.to_string(), bucket };
            match self.read_bucket_values_indexed(&key, &wanted) {
                Ok(map) => {
                    for (v, bm) in map { result.insert(v, bm); }
                }
                Err(_) => {
                    // Fallback: legacy full-bucket read on any indexed-path error.
                    if let Some(snap) = self.read(&key)? {
                        for v in &wanted {
                            if let Some(bm) = snap.values.get(v) {
                                result.insert(*v, bm.clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Indexed-read fast path: reads the in-shard index, seeks to the requested
    /// values' bitmap byte ranges, and applies only the value-tagged ops that
    /// touch the wanted set. Avoids the O(bucket_size) full-snapshot decode.
    ///
    /// Reads on a missing or invalid shard return an empty map (matches the
    /// `self.read() == None` semantics). Any other error propagates so the
    /// caller can fall back to `self.read()`.
    fn read_bucket_values_indexed(
        &self,
        key: &FilterBucketKey,
        wanted: &[u64],
    ) -> io::Result<HashMap<u64, RoaringBitmap>> {
        if wanted.is_empty() {
            return Ok(HashMap::new());
        }

        let lock = self.shard_lock(key);
        let _guard = lock.read();

        let path = self.shard_path(key);
        if !path.exists() || !crate::shard_store::is_valid_shard_file(&path) {
            return Ok(HashMap::new());
        }

        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        // Header (28 bytes).
        let mut header_buf = [0u8; crate::shard_store::HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = crate::shard_store::ShardHeader::decode(&header_buf)?;

        // Bound declared section sizes against actual file size to keep a
        // corrupted header from triggering a multi-gigabyte allocation in
        // `vec![0u8; index_size]` before the read fails.
        let snapshot_end_declared =
            (crate::shard_store::HEADER_SIZE as u64) + (header.snapshot_len as u64);
        if snapshot_end_declared > file_len
            || header.ops_section_offset > file_len
            || header.ops_section_offset < snapshot_end_declared
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shard header section ranges exceed file length",
            ));
        }

        let mut result: HashMap<u64, RoaringBitmap> = HashMap::new();
        let wanted_set: HashSet<u64> = wanted.iter().copied().collect();

        // Snapshot section: [u32 count][N × 16-byte index][packed bitmaps].
        if header.snapshot_len >= 4 {
            let mut count_buf = [0u8; 4];
            file.read_exact(&mut count_buf)?;
            let count = u32::from_le_bytes(count_buf) as usize;

            if count > 0 {
                let index_size = count.checked_mul(16).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "shard index size overflow")
                })?;
                if 4 + index_size > header.snapshot_len as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "shard index size exceeds snapshot_len",
                    ));
                }

                let mut index_buf = vec![0u8; index_size];
                file.read_exact(&mut index_buf)?;

                let data_section_start =
                    crate::shard_store::HEADER_SIZE as u64 + 4 + index_size as u64;
                let snapshot_end =
                    crate::shard_store::HEADER_SIZE as u64 + header.snapshot_len as u64;

                // Walk the index, collect (value, abs_offset, len) for wanted entries.
                let mut found: Vec<(u64, u64, u64)> = Vec::with_capacity(wanted.len());
                for i in 0..count {
                    let off = i * 16;
                    let value_id = u64::from_le_bytes(
                        index_buf[off..off + 8].try_into().unwrap(),
                    );
                    if !wanted_set.contains(&value_id) {
                        continue;
                    }
                    let bm_offset = u32::from_le_bytes(
                        index_buf[off + 8..off + 12].try_into().unwrap(),
                    ) as u64;
                    let bm_length = u32::from_le_bytes(
                        index_buf[off + 12..off + 16].try_into().unwrap(),
                    ) as u64;
                    let abs = data_section_start
                        .checked_add(bm_offset)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bm offset overflow"))?;
                    let abs_end = abs
                        .checked_add(bm_length)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bm length overflow"))?;
                    if abs_end > snapshot_end {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "bucket bitmap extends past snapshot section",
                        ));
                    }
                    found.push((value_id, abs, bm_length));
                }

                // Sort by file offset for sequential I/O.
                found.sort_by_key(|&(_, abs, _)| abs);

                for (value, abs, len) in found {
                    file.seek(SeekFrom::Start(abs))?;
                    let mut bm_buf = vec![0u8; len as usize];
                    file.read_exact(&mut bm_buf)?;
                    let bm = RoaringBitmap::deserialize_from(&bm_buf[..]).map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("bitmap: {e}"))
                    })?;
                    result.insert(value, bm);
                }
            }
        }

        // Apply ops, but only the ones tagged with a wanted value.
        if header.ops_count > 0 {
            file.seek(SeekFrom::Start(header.ops_section_offset))?;
            let mut ops_buf = Vec::new();
            file.read_to_end(&mut ops_buf)?;
            for op in crate::shard_store::read_op_entries_pub::<FilterOpCodec>(&ops_buf) {
                let op_value = match &op {
                    FilterOp::SetBit { value, .. }
                    | FilterOp::ClearBit { value, .. }
                    | FilterOp::BatchSet { value, .. }
                    | FilterOp::BatchClear { value, .. } => *value,
                };
                if !wanted_set.contains(&op_value) {
                    continue;
                }
                match op {
                    FilterOp::SetBit { value, bit } => {
                        result.entry(value).or_insert_with(RoaringBitmap::new).insert(bit);
                    }
                    FilterOp::ClearBit { value, bit } => {
                        if let Some(bm) = result.get_mut(&value) {
                            bm.remove(bit);
                        }
                    }
                    FilterOp::BatchSet { value, bits } => {
                        let bm = result.entry(value).or_insert_with(RoaringBitmap::new);
                        bm.extend(bits.iter().copied());
                    }
                    FilterOp::BatchClear { value, bits } => {
                        if let Some(bm) = result.get_mut(&value) {
                            for b in bits {
                                bm.remove(b);
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Read a single filter bucket as a vec of (value, bitmap) pairs.
    ///
    /// Replaces legacy BitmapFs::read_filter_bucket().
    pub fn read_filter_bucket(&self, field: &str, bucket: u8) -> io::Result<Vec<(u64, RoaringBitmap)>> {
        let key = FilterBucketKey { field: field.to_string(), bucket };
        match self.read(&key)? {
            Some(snap) => Ok(snap.values.into_iter().collect()),
            None => Ok(Vec::new()),
        }
    }

    /// Write a filter bucket from (value, bitmap) pairs.
    ///
    /// Replaces legacy BitmapFs::write_filter_bucket().
    pub fn write_filter_bucket(&self, field: &str, bucket: u8, entries: &[(u64, &RoaringBitmap)]) -> io::Result<()> {
        let key = FilterBucketKey { field: field.to_string(), bucket };
        let mut snap = BucketSnapshot::new();
        for &(value, bm) in entries {
            snap.values.insert(value, bm.clone());
        }
        self.write_snapshot(&key, &snap)
    }

    /// Write a full snapshot of all filter bitmaps for all fields.
    ///
    /// Takes filter entries as (field, value, bitmap) triples and an alive bitmap
    /// with slot counter. Groups by (field, bucket) and writes each bucket shard.
    pub fn write_full_filter(&self, entries: &[(&str, u64, &RoaringBitmap)]) -> io::Result<()> {
        // Group by (field, bucket)
        let mut by_bucket: HashMap<(String, u8), Vec<(u64, &RoaringBitmap)>> = HashMap::new();
        for &(field, value, bm) in entries {
            let bucket = ((value >> 8) & 0xFF) as u8;
            by_bucket.entry((field.to_string(), bucket))
                .or_default()
                .push((value, bm));
        }
        for ((field, bucket), entries) in by_bucket {
            self.write_filter_bucket_raw(&field, bucket, &entries)?;
        }
        Ok(())
    }

    /// Write a filter bucket directly from (value, &bitmap) refs — zero clones.
    ///
    /// Encodes the bucket snapshot format inline without constructing a
    /// BucketSnapshot or cloning any bitmaps.
    pub fn write_filter_bucket_raw(&self, field: &str, bucket: u8, entries: &[(u64, &RoaringBitmap)]) -> io::Result<()> {
        let key = FilterBucketKey { field: field.to_string(), bucket };
        let shard_path = self.shard_path(&key);

        // Encode bucket snapshot format directly from references:
        // [u32 num_values]
        // [index: N × (u64 value_id, u32 bitmap_offset, u32 bitmap_length)]
        // [packed serialized roaring bitmaps]
        let count = entries.len() as u32;
        let mut snapshot_bytes = Vec::new();
        snapshot_bytes.extend_from_slice(&count.to_le_bytes());

        // Serialize bitmaps to get sizes for index table
        let mut bitmap_data: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for &(value, bm) in entries {
            let mut bm_buf = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut bm_buf).expect("bitmap serialize");
            bitmap_data.push((value, bm_buf));
        }

        // Write index table
        let mut offset: u32 = 0;
        for (value_id, bm_buf) in &bitmap_data {
            snapshot_bytes.extend_from_slice(&value_id.to_le_bytes());
            snapshot_bytes.extend_from_slice(&offset.to_le_bytes());
            snapshot_bytes.extend_from_slice(&(bm_buf.len() as u32).to_le_bytes());
            offset += bm_buf.len() as u32;
        }

        // Write packed bitmap data
        for (_, bm_buf) in &bitmap_data {
            snapshot_bytes.extend_from_slice(bm_buf);
        }

        // Write shard file
        let ops_offset = crate::shard_store::HEADER_SIZE as u64 + snapshot_bytes.len() as u64;
        let header = crate::shard_store::ShardHeader {
            version: crate::shard_store::SHARD_VERSION,
            ops_section_offset: ops_offset,
            snapshot_len: snapshot_bytes.len() as u32,
            ops_count: 0,
            flags: 0,
        };
        crate::shard_store::write_shard_file_atomic(&shard_path, &header, &snapshot_bytes, &[], crate::shard_store::ShardRewriteSource::Snapshot)
    }

    /// Pre-create shard directories for a field's filter buckets.
    /// Avoids per-write `create_dir_all` overhead during parallel writes.
    pub fn ensure_filter_dirs(&self, field: &str, buckets: &[u8]) -> io::Result<()> {
        for &bucket in buckets {
            let key = FilterBucketKey { field: field.to_string(), bucket };
            let shard_path = self.shard_path(&key);
            if let Some(parent) = shard_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }
        Ok(())
    }
}

/// ShardStore for sort layer bitmaps (legacy per-layer sharding).
///
/// This type alias is used by `concurrent_engine.rs` for per-layer ops via
/// `append_op(&SortLayerShardKey, &BitmapOp)`. The per-layer shard files are
/// a secondary ops path — `write_sort_layers` and `load_sort_layers` use the
/// packed format (one file per field) for snapshot I/O.
pub type SortBitmapStore = crate::shard_store::ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SortLayerShard>;

/// ShardStore for packed sort field bitmaps (all layers in one shard per field).
///
/// Used for snapshot reads/writes and sort-layer ops that embed bit_position.
pub type PackedSortBitmapStore = crate::shard_store::ShardStore<SortFieldSnapshotCodec, SortLayerOpCodec, SortFieldShard>;

impl SortBitmapStore {
    /// Load all sort layers for a field from the packed format.
    ///
    /// Reads a single `sort/{field}.shard` file containing all bit layers,
    /// and unpacks into a Vec<RoaringBitmap> ordered by bit position.
    /// Returns None if no packed shard exists on disk.
    pub fn load_sort_layers(&self, field: &str, bits: usize) -> io::Result<Option<Vec<RoaringBitmap>>> {
        // Try packed shard first
        let packed_path = self.root().join("sort").join(format!("{}.shard", field));
        if packed_path.exists() {
            let data = std::fs::read(&packed_path)?;
            let header = crate::shard_store::ShardHeader::decode(&data)?;
            let snap_start = crate::shard_store::HEADER_SIZE;
            let snap_end = snap_start + header.snapshot_len as usize;
            let mut snap = if header.snapshot_len > 0 {
                SortFieldSnapshotCodec::decode(&data[snap_start..snap_end])?
            } else {
                SortFieldSnapshot::new()
            };
            // Apply any ops
            if header.ops_count > 0 {
                let ops_start = header.ops_section_offset as usize;
                let ops_data = &data[ops_start..];
                let ops = crate::shard_store::read_op_entries_pub::<SortLayerOpCodec>(ops_data);
                for op in &ops {
                    SortLayerOpCodec::apply(&mut snap, op);
                }
            }
            let mut layers = Vec::with_capacity(bits);
            for bit in 0..bits {
                layers.push(snap.layers.get(&(bit as u8)).cloned().unwrap_or_default());
            }
            return Ok(Some(layers));
        }

        // Fall back to legacy per-layer format
        let mut layers = Vec::with_capacity(bits);
        let mut any_found = false;
        for bit in 0..bits {
            let key = SortLayerShardKey { field: field.to_string(), bit_position: bit as u8 };
            match self.read(&key)? {
                Some(bm) => {
                    any_found = true;
                    layers.push(bm);
                }
                None => layers.push(RoaringBitmap::new()),
            }
        }
        if any_found { Ok(Some(layers)) } else { Ok(None) }
    }

    /// Write sort layers for a field in the packed format.
    ///
    /// Encodes all layers into a single `sort/{field}.shard` file using
    /// the SortFieldSnapshotCodec packed format (index + packed bitmaps).
    pub fn write_sort_layers(&self, field: &str, layers: &[&RoaringBitmap]) -> io::Result<()> {
        let shard_path = self.root().join("sort").join(format!("{}.shard", field));

        // Encode packed snapshot directly from layer refs
        let mut snapshot_bytes = Vec::new();
        SortFieldSnapshotCodec::encode_from_layers(
            layers.iter().enumerate().map(|(i, bm)| (i as u8, *bm)),
            &mut snapshot_bytes,
        );

        let ops_offset = crate::shard_store::HEADER_SIZE as u64 + snapshot_bytes.len() as u64;
        let header = crate::shard_store::ShardHeader {
            version: crate::shard_store::SHARD_VERSION,
            ops_section_offset: ops_offset,
            snapshot_len: snapshot_bytes.len() as u32,
            ops_count: 0,
            flags: 0,
        };
        crate::shard_store::write_shard_file_atomic(&shard_path, &header, &snapshot_bytes, &[], crate::shard_store::ShardRewriteSource::Snapshot)
    }

    /// Pre-create the sort directory.
    /// Ensures `sort/` exists for packed shard writes.
    pub fn ensure_sort_dir(&self, _field: &str) -> io::Result<()> {
        let sort_dir = self.root().join("sort");
        std::fs::create_dir_all(&sort_dir)?;
        Ok(())
    }
}

impl PackedSortBitmapStore {
    /// Append a sort layer op to the packed shard for a field.
    ///
    /// This is the packed-format equivalent of `SortBitmapStore::append_op` —
    /// the op includes the bit_position, targeting a specific layer within
    /// the packed shard file.
    pub fn append_sort_op(&self, field: &str, bit_position: u8, slot: u32, set: bool) -> io::Result<()> {
        let key = SortFieldShardKey { field: field.to_string() };
        let op = if set {
            SortLayerOp::SetBit { bit_position, slot }
        } else {
            SortLayerOp::ClearBit { bit_position, slot }
        };
        self.append_op(&key, &op)
    }

    /// Load all sort layers for a field from the packed store.
    ///
    /// Reads the single packed shard (snapshot + ops) and unpacks into
    /// a Vec<RoaringBitmap> ordered by bit position.
    pub fn load_sort_layers(&self, field: &str, bits: usize) -> io::Result<Option<Vec<RoaringBitmap>>> {
        let key = SortFieldShardKey { field: field.to_string() };
        match self.read(&key)? {
            Some(snap) => {
                let mut layers = Vec::with_capacity(bits);
                for bit in 0..bits {
                    layers.push(
                        snap.layers.get(&(bit as u8)).cloned().unwrap_or_default()
                    );
                }
                Ok(Some(layers))
            }
            None => Ok(None),
        }
    }

    /// Write sort layers for a field as a packed snapshot.
    pub fn write_sort_layers(&self, field: &str, layers: &[&RoaringBitmap]) -> io::Result<()> {
        let key = SortFieldShardKey { field: field.to_string() };
        let mut snap = SortFieldSnapshot::new();
        for (i, bm) in layers.iter().enumerate() {
            if !bm.is_empty() {
                snap.layers.insert(i as u8, (*bm).clone());
            }
        }
        self.write_snapshot(&key, &snap)
    }

    /// Pre-create the sort directory for packed shard writes.
    pub fn ensure_sort_dir(&self, _field: &str) -> io::Result<()> {
        let sort_dir = self.root().join("sort");
        std::fs::create_dir_all(&sort_dir)?;
        Ok(())
    }
}

/// ShardStore for the alive bitmap.
pub type AliveBitmapStore = crate::shard_store::ShardStore<BitmapSnapshotCodec, BitmapOpCodec, SingletonShard>;

impl AliveBitmapStore {
    /// Load the alive bitmap.
    ///
    /// Replaces legacy BitmapFs::load_alive().
    pub fn load_alive(&self) -> io::Result<Option<RoaringBitmap>> {
        self.read(&AliveShardKey)
    }

    /// Write the alive bitmap.
    ///
    /// Replaces legacy BitmapFs::write_alive().
    pub fn write_alive(&self, bitmap: &RoaringBitmap) -> io::Result<()> {
        self.write_snapshot(&AliveShardKey, bitmap)
    }
}

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
        let path = shard.shard_path(&key, Path::new("/data"));
        assert_eq!(path, PathBuf::from("/data/filter/tagIds/01.shard"));
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
    fn test_load_field_values_after_compact_indexed_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();
        let key = FilterBucketKey { field: "postId".into(), bucket: 0x00 };

        for v in [1u64, 2, 4, 7, 100] {
            store
                .append_op(
                    &key,
                    &FilterOp::BatchSet { value: v, bits: vec![v as u32 * 10, v as u32 * 10 + 1] },
                )
                .unwrap();
        }
        store.compact_current(&key).unwrap();
        assert_eq!(store.ops_count(&key).unwrap(), Some(0));

        let res = store.load_field_values("postId", &[4]).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[&4].len(), 2);
        assert!(res[&4].contains(40));
        assert!(res[&4].contains(41));

        let res = store.load_field_values("postId", &[1, 100]).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res[&1].contains(10));
        assert!(res[&100].contains(1000));

        let res = store.load_field_values("postId", &[999]).unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn test_load_field_values_op_only_indexed_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();
        let key = FilterBucketKey { field: "postId".into(), bucket: 0x00 };

        store.append_op(&key, &FilterOp::SetBit { value: 1, bit: 100 }).unwrap();
        store.append_op(&key, &FilterOp::SetBit { value: 2, bit: 200 }).unwrap();
        store.append_op(&key, &FilterOp::SetBit { value: 3, bit: 300 }).unwrap();

        let res = store.load_field_values("postId", &[2]).unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[&2].contains(200));
        assert!(!res[&2].contains(100));
    }

    #[test]
    fn test_load_field_values_snapshot_plus_ops() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();
        let key = FilterBucketKey { field: "postId".into(), bucket: 0x00 };

        store.append_op(&key, &FilterOp::BatchSet { value: 1, bits: vec![10, 20] }).unwrap();
        store.append_op(&key, &FilterOp::BatchSet { value: 2, bits: vec![30] }).unwrap();
        store.compact_current(&key).unwrap();

        store.append_op(&key, &FilterOp::SetBit { value: 1, bit: 30 }).unwrap();
        store.append_op(&key, &FilterOp::ClearBit { value: 1, bit: 10 }).unwrap();
        store.append_op(&key, &FilterOp::SetBit { value: 3, bit: 40 }).unwrap();

        let res = store.load_field_values("postId", &[1, 3]).unwrap();
        assert_eq!(res.len(), 2);
        assert!(!res[&1].contains(10));
        assert!(res[&1].contains(20));
        assert!(res[&1].contains(30));
        assert!(res[&3].contains(40));

        let res = store.load_field_values("postId", &[2]).unwrap();
        assert!(res[&2].contains(30));
    }

    #[test]
    fn test_load_field_values_clears_to_empty_keep_entry() {
        // Snapshot has a value, ops clear all bits. The slow path keeps an empty
        // bitmap entry in the result (because FilterOpCodec::apply uses
        // get_mut + remove and never deletes). The fast path must do the same.
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();
        let key = FilterBucketKey { field: "postId".into(), bucket: 0x00 };

        store.append_op(&key, &FilterOp::BatchSet { value: 1, bits: vec![10, 11] }).unwrap();
        store.compact_current(&key).unwrap();

        store.append_op(&key, &FilterOp::BatchClear { value: 1, bits: vec![10, 11] }).unwrap();

        let res = store.load_field_values("postId", &[1]).unwrap();
        assert!(res.contains_key(&1), "value 1 should remain in result map even after all bits cleared");
        assert!(res[&1].is_empty(), "value 1 bitmap should be empty");
    }

    #[test]
    fn test_load_field_values_clear_only_no_insert() {
        // ClearBit on a value that's neither in snapshot nor introduced by an
        // earlier set op must not insert an entry into the result.
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();
        let key = FilterBucketKey { field: "postId".into(), bucket: 0x00 };

        store.append_op(&key, &FilterOp::SetBit { value: 1, bit: 100 }).unwrap();
        store.append_op(&key, &FilterOp::ClearBit { value: 99, bit: 9 }).unwrap();

        let res = store.load_field_values("postId", &[99]).unwrap();
        assert!(!res.contains_key(&99), "ClearBit on absent value must not insert entry");
    }

    #[test]
    fn test_load_field_values_missing_shard() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();
        let res = store.load_field_values("nonexistent", &[1, 2, 3]).unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn test_load_field_values_indexed_matches_full_read() {
        // Property: indexed result must equal a filtered full-bucket read.
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();
        let key = FilterBucketKey { field: "postId".into(), bucket: 0x00 };

        for v in 0u64..50 {
            store
                .append_op(
                    &key,
                    &FilterOp::BatchSet { value: v, bits: vec![v as u32, v as u32 + 1000] },
                )
                .unwrap();
        }
        store.compact_current(&key).unwrap();
        for v in 0u64..50 {
            if v % 3 == 0 {
                store.append_op(&key, &FilterOp::ClearBit { value: v, bit: v as u32 }).unwrap();
            }
            if v % 5 == 0 {
                store.append_op(&key, &FilterOp::SetBit { value: v, bit: 9999 }).unwrap();
            }
        }

        let wanted: Vec<u64> = vec![0, 3, 5, 7, 15, 30, 49];
        let indexed = store.load_field_values("postId", &wanted).unwrap();

        let full = store.read(&key).unwrap().unwrap();
        let mut expected: HashMap<u64, RoaringBitmap> = HashMap::new();
        for &v in &wanted {
            if let Some(bm) = full.values.get(&v) {
                expected.insert(v, bm.clone());
            }
        }

        assert_eq!(indexed.len(), expected.len());
        for (k, v) in &expected {
            assert_eq!(&indexed[k], v, "value {} mismatch", k);
        }
    }

    #[test]
    fn test_load_field_values_multiple_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilterBitmapStore::new(dir.path().to_path_buf(), FieldValueBucketShard).unwrap();

        let v_a = 0x0100u64;
        let v_b = 0x0200u64;
        let v_c = 0x0300u64;

        let key_a = FilterBucketKey::from_value("postId".into(), v_a);
        let key_b = FilterBucketKey::from_value("postId".into(), v_b);
        let key_c = FilterBucketKey::from_value("postId".into(), v_c);

        store.append_op(&key_a, &FilterOp::SetBit { value: v_a, bit: 1 }).unwrap();
        store.append_op(&key_b, &FilterOp::SetBit { value: v_b, bit: 2 }).unwrap();
        store.append_op(&key_c, &FilterOp::SetBit { value: v_c, bit: 3 }).unwrap();
        store.compact_current(&key_a).unwrap();
        store.compact_current(&key_b).unwrap();

        let res = store.load_field_values("postId", &[v_a, v_b, v_c]).unwrap();
        assert_eq!(res.len(), 3);
        assert!(res[&v_a].contains(1));
        assert!(res[&v_b].contains(2));
        assert!(res[&v_c].contains(3));
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
        let path = shard.shard_path(&key, Path::new("/data"));
        assert_eq!(path, PathBuf::from("/data/sort/reactionCount/bit15.shard"));
    }

    #[test]
    fn test_alive_shard_path() {
        let shard = SingletonShard;
        let path = shard.shard_path(&AliveShardKey, Path::new("/data"));
        assert_eq!(path, PathBuf::from("/data/system/alive.shard"));
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

    // --- Packed sort field tests ---

    #[test]
    fn test_sort_field_snapshot_roundtrip() {
        let mut snap = SortFieldSnapshot::new();
        let mut bm0 = RoaringBitmap::new();
        bm0.insert_range(0..100);
        let mut bm5 = RoaringBitmap::new();
        bm5.insert_range(500..600);
        let mut bm31 = RoaringBitmap::new();
        bm31.insert(42);
        bm31.insert(9999);
        snap.layers.insert(0, bm0.clone());
        snap.layers.insert(5, bm5.clone());
        snap.layers.insert(31, bm31.clone());

        let mut buf = Vec::new();
        SortFieldSnapshotCodec::encode(&snap, &mut buf);
        let decoded = SortFieldSnapshotCodec::decode(&buf).unwrap();

        assert_eq!(decoded.layers.len(), 3);
        assert_eq!(decoded.layers[&0], bm0);
        assert_eq!(decoded.layers[&5], bm5);
        assert_eq!(decoded.layers[&31], bm31);
    }

    #[test]
    fn test_sort_field_snapshot_empty_and_sparse() {
        // All empty layers should produce a snapshot with 0 stored layers
        let snap = SortFieldSnapshot::new();
        let mut buf = Vec::new();
        SortFieldSnapshotCodec::encode(&snap, &mut buf);
        let decoded = SortFieldSnapshotCodec::decode(&buf).unwrap();
        assert!(decoded.layers.is_empty());

        // Sparse: only layers 3 and 28 have data
        let mut snap2 = SortFieldSnapshot::new();
        let mut bm3 = RoaringBitmap::new();
        bm3.insert(1);
        snap2.layers.insert(3, bm3.clone());
        // Insert an empty bitmap for layer 10 — should NOT be stored
        snap2.layers.insert(10, RoaringBitmap::new());
        let mut bm28 = RoaringBitmap::new();
        bm28.insert(999);
        snap2.layers.insert(28, bm28.clone());

        let mut buf2 = Vec::new();
        SortFieldSnapshotCodec::encode(&snap2, &mut buf2);
        let decoded2 = SortFieldSnapshotCodec::decode(&buf2).unwrap();

        // Only 2 non-empty layers stored
        assert_eq!(decoded2.layers.len(), 2);
        assert_eq!(decoded2.layers[&3], bm3);
        assert_eq!(decoded2.layers[&28], bm28);
        assert!(!decoded2.layers.contains_key(&10));
    }

    #[test]
    fn test_sort_layer_op_roundtrip() {
        let op1 = SortLayerOp::SetBit { bit_position: 7, slot: 42 };
        let mut buf = Vec::new();
        SortLayerOpCodec::encode_op(&op1, &mut buf);
        let decoded = SortLayerOpCodec::decode_op(&buf).unwrap();
        match decoded {
            SortLayerOp::SetBit { bit_position, slot } => {
                assert_eq!(bit_position, 7);
                assert_eq!(slot, 42);
            }
            _ => panic!("expected SetBit"),
        }

        let op2 = SortLayerOp::ClearBit { bit_position: 31, slot: 999999 };
        let mut buf2 = Vec::new();
        SortLayerOpCodec::encode_op(&op2, &mut buf2);
        let decoded2 = SortLayerOpCodec::decode_op(&buf2).unwrap();
        match decoded2 {
            SortLayerOp::ClearBit { bit_position, slot } => {
                assert_eq!(bit_position, 31);
                assert_eq!(slot, 999999);
            }
            _ => panic!("expected ClearBit"),
        }
    }

    #[test]
    fn test_sort_layer_op_apply() {
        let mut snap = SortFieldSnapshot::new();

        // Set bit on layer 0
        SortLayerOpCodec::apply(&mut snap, &SortLayerOp::SetBit { bit_position: 0, slot: 42 });
        assert!(snap.layers[&0].contains(42));

        // Set another bit on layer 0
        SortLayerOpCodec::apply(&mut snap, &SortLayerOp::SetBit { bit_position: 0, slot: 43 });
        assert_eq!(snap.layers[&0].len(), 2);

        // Set bit on different layer
        SortLayerOpCodec::apply(&mut snap, &SortLayerOp::SetBit { bit_position: 5, slot: 100 });
        assert_eq!(snap.layers.len(), 2);
        assert!(snap.layers[&5].contains(100));

        // Clear bit from layer 0
        SortLayerOpCodec::apply(&mut snap, &SortLayerOp::ClearBit { bit_position: 0, slot: 42 });
        assert!(!snap.layers[&0].contains(42));
        assert!(snap.layers[&0].contains(43));

        // Clear bit from nonexistent layer — no panic
        SortLayerOpCodec::apply(&mut snap, &SortLayerOp::ClearBit { bit_position: 31, slot: 1 });
        assert!(!snap.layers.contains_key(&31));
    }

    #[test]
    fn test_sort_field_shard_path() {
        let shard = SortFieldShard;
        let key = SortFieldShardKey { field: "reactionCount".into() };
        let path = shard.shard_path(&key, Path::new("/data"));
        assert_eq!(path, PathBuf::from("/data/sort/reactionCount.shard"));
    }

    #[test]
    fn test_packed_sort_store_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = SortBitmapStore::new(dir.path().to_path_buf(), SortLayerShard).unwrap();

        // Create 32 layers, only some with data
        let mut layers: Vec<RoaringBitmap> = (0..32).map(|_| RoaringBitmap::new()).collect();
        layers[0].insert_range(0..100);
        layers[5].insert(42);
        layers[5].insert(999);
        layers[31].insert_range(1000..1100);

        let layer_refs: Vec<&RoaringBitmap> = layers.iter().collect();
        store.ensure_sort_dir("reactionCount").unwrap();
        store.write_sort_layers("reactionCount", &layer_refs).unwrap();

        // Read back
        let loaded = store.load_sort_layers("reactionCount", 32).unwrap().unwrap();
        assert_eq!(loaded.len(), 32);
        assert_eq!(loaded[0].len(), 100);
        assert_eq!(loaded[5].len(), 2);
        assert!(loaded[5].contains(42));
        assert!(loaded[5].contains(999));
        assert_eq!(loaded[31].len(), 100);

        // Empty layers should be empty
        assert!(loaded[1].is_empty());
        assert!(loaded[15].is_empty());
    }

    #[test]
    fn test_packed_sort_store_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let store = PackedSortBitmapStore::new(dir.path().to_path_buf(), SortFieldShard).unwrap();

        // Write initial snapshot
        let mut snap = SortFieldSnapshot::new();
        let mut bm0 = RoaringBitmap::new();
        bm0.insert_range(0..50);
        snap.layers.insert(0, bm0);

        let key = SortFieldShardKey { field: "reactionCount".into() };
        store.write_snapshot(&key, &snap).unwrap();

        // Append some ops
        store.append_sort_op("reactionCount", 0, 100, true).unwrap();
        store.append_sort_op("reactionCount", 5, 42, true).unwrap();
        store.append_sort_op("reactionCount", 0, 10, false).unwrap(); // clear

        assert_eq!(store.ops_count(&key).unwrap(), Some(3));

        // Compact
        store.compact_current(&key).unwrap();
        assert_eq!(store.ops_count(&key).unwrap(), Some(0));

        // Verify result
        let result = store.read(&key).unwrap().unwrap();
        assert_eq!(result.layers[&0].len(), 50); // 0..50 - 10 + 100 = 50
        assert!(result.layers[&0].contains(100));
        assert!(!result.layers[&0].contains(10));
        assert!(result.layers[&5].contains(42));
    }

    #[test]
    fn test_packed_sort_store_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = PackedSortBitmapStore::new(dir.path().to_path_buf(), SortFieldShard).unwrap();

        // Append ops without a snapshot first
        store.append_sort_op("sortAt", 0, 1, true).unwrap();
        store.append_sort_op("sortAt", 0, 2, true).unwrap();
        store.append_sort_op("sortAt", 15, 99, true).unwrap();
        store.append_sort_op("sortAt", 0, 1, false).unwrap(); // clear

        let key = SortFieldShardKey { field: "sortAt".into() };
        let result = store.read(&key).unwrap().unwrap();

        assert_eq!(result.layers[&0].len(), 1); // only slot 2 remains
        assert!(result.layers[&0].contains(2));
        assert!(!result.layers[&0].contains(1));
        assert!(result.layers[&15].contains(99));
    }

    #[test]
    fn test_packed_sort_load_via_packed_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = PackedSortBitmapStore::new(dir.path().to_path_buf(), SortFieldShard).unwrap();

        let mut bm0 = RoaringBitmap::new();
        bm0.insert_range(0..50);
        let mut bm7 = RoaringBitmap::new();
        bm7.insert(42);
        let layers = vec![&bm0, &bm7];

        // Use the PackedSortBitmapStore write path
        store.write_sort_layers("testField", &layers).unwrap();

        // Load via packed store
        let loaded = store.load_sort_layers("testField", 8).unwrap().unwrap();
        assert_eq!(loaded.len(), 8);
        assert_eq!(loaded[0].len(), 50);
        assert_eq!(loaded[1].len(), 1);
        assert!(loaded[1].contains(42));
        // Remaining should be empty
        for i in 2..8 {
            assert!(loaded[i].is_empty());
        }
    }

    #[test]
    fn test_sort_field_shard_list() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        // Create sort directory with packed shard files
        let sort_dir = root.join("sort");
        std::fs::create_dir_all(&sort_dir).unwrap();
        std::fs::write(sort_dir.join("reactionCount.shard"), b"dummy").unwrap();
        std::fs::write(sort_dir.join("sortAt.shard"), b"dummy").unwrap();
        // Legacy directory should NOT appear in packed list
        std::fs::create_dir_all(sort_dir.join("legacyField")).unwrap();

        let shard = SortFieldShard;
        let mut keys = shard.list_shards(root).unwrap();
        keys.sort_by(|a, b| a.field.cmp(&b.field));
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].field, "reactionCount");
        assert_eq!(keys[1].field, "sortAt");
    }
}
