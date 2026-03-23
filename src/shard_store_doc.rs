//! Document codecs and sharding strategy for ShardStore.
//!
//! Implements:
//! - `DocSnapshotCodec` — serialize/deserialize `DocSnapshot` (slot → field values)
//! - `DocOpCodec` — typed document operations (Set, Append, Remove, Delete, Create)
//! - `SlotHexShard` — maps slot IDs to hex-bucketed shard files (same layout as DocStore V2)

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::docstore::PackedValue;
use crate::shard_store::{SnapshotCodec, OpCodec, ShardingStrategy};

// ---------------------------------------------------------------------------
// Shard shift — same as DocStore V2: 512 docs per shard
// ---------------------------------------------------------------------------

const SHARD_SHIFT: u32 = 9;

// ---------------------------------------------------------------------------
// DocSnapshot — the materialized state of one shard
// ---------------------------------------------------------------------------

/// A snapshot of all documents in a shard.
///
/// Maps slot_id → list of (field_idx, value) pairs.
/// This matches the V2 tuple layout but in memory.
#[derive(Debug, Clone, PartialEq)]
pub struct DocSnapshot {
    /// slot_id → [(field_idx, value)]
    pub docs: HashMap<u32, Vec<(u16, PackedValue)>>,
}

impl DocSnapshot {
    pub fn new() -> Self {
        DocSnapshot { docs: HashMap::new() }
    }
}

// ---------------------------------------------------------------------------
// DocOp — typed document operations
// ---------------------------------------------------------------------------

/// A single document operation.
#[derive(Debug, Clone)]
pub enum DocOp {
    /// Set a scalar field to a value (replaces previous).
    Set { slot: u32, field: u16, value: PackedValue },

    /// Append a value to a multi-value field (e.g., add a tag).
    Append { slot: u32, field: u16, value: PackedValue },

    /// Remove a value from a multi-value field (e.g., remove a tag).
    Remove { slot: u32, field: u16, value: PackedValue },

    /// Delete an entire document.
    Delete { slot: u32 },

    /// Create a document with a full set of fields.
    Create { slot: u32, fields: Vec<(u16, PackedValue)> },
}

// ---------------------------------------------------------------------------
// Op tags for serialization
// ---------------------------------------------------------------------------

const OP_TAG_SET: u8 = 0x01;
const OP_TAG_APPEND: u8 = 0x02;
const OP_TAG_REMOVE: u8 = 0x03;
const OP_TAG_DELETE: u8 = 0x04;
const OP_TAG_CREATE: u8 = 0x05;

// ---------------------------------------------------------------------------
// PackedValue binary encoding (compact, no msgpack dependency)
// ---------------------------------------------------------------------------

const PV_TAG_I: u8 = 0x01;
const PV_TAG_F: u8 = 0x02;
const PV_TAG_B: u8 = 0x03;
const PV_TAG_S: u8 = 0x04;
const PV_TAG_MI: u8 = 0x05;
const PV_TAG_MM: u8 = 0x06;

fn encode_packed_value(pv: &PackedValue, buf: &mut Vec<u8>) {
    match pv {
        PackedValue::I(v) => {
            buf.push(PV_TAG_I);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        PackedValue::F(v) => {
            buf.push(PV_TAG_F);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        PackedValue::B(v) => {
            buf.push(PV_TAG_B);
            buf.push(if *v { 1 } else { 0 });
        }
        PackedValue::S(v) => {
            buf.push(PV_TAG_S);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        }
        PackedValue::Mi(v) => {
            buf.push(PV_TAG_MI);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for val in v {
                buf.extend_from_slice(&val.to_le_bytes());
            }
        }
        PackedValue::Mm(v) => {
            buf.push(PV_TAG_MM);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for val in v {
                encode_packed_value(val, buf);
            }
        }
    }
}

fn decode_packed_value(data: &[u8], pos: &mut usize) -> io::Result<PackedValue> {
    if *pos >= data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF in packed value"));
    }
    let tag = data[*pos];
    *pos += 1;

    match tag {
        PV_TAG_I => {
            let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated i64")
            })?);
            *pos += 8;
            Ok(PackedValue::I(v))
        }
        PV_TAG_F => {
            let v = f64::from_le_bytes(data[*pos..*pos + 8].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated f64")
            })?);
            *pos += 8;
            Ok(PackedValue::F(v))
        }
        PV_TAG_B => {
            let v = data[*pos] != 0;
            *pos += 1;
            Ok(PackedValue::B(v))
        }
        PV_TAG_S => {
            let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated string length")
            })?) as usize;
            *pos += 4;
            let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
            *pos += len;
            Ok(PackedValue::S(s))
        }
        PV_TAG_MI => {
            let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated mi length")
            })?) as usize;
            *pos += 4;
            let mut vals = Vec::with_capacity(len);
            for _ in 0..len {
                let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated mi element")
                })?);
                *pos += 8;
                vals.push(v);
            }
            Ok(PackedValue::Mi(vals))
        }
        PV_TAG_MM => {
            let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated mm length")
            })?) as usize;
            *pos += 4;
            let mut vals = Vec::with_capacity(len);
            for _ in 0..len {
                vals.push(decode_packed_value(data, pos)?);
            }
            Ok(PackedValue::Mm(vals))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown packed value tag: 0x{:02x}", other),
        )),
    }
}

/// Encode a field pair: [u16 field_idx][packed_value]
fn encode_field_pair(field: u16, value: &PackedValue, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field.to_le_bytes());
    encode_packed_value(value, buf);
}

/// Decode a field pair: returns (field_idx, value) and advances pos.
fn decode_field_pair(data: &[u8], pos: &mut usize) -> io::Result<(u16, PackedValue)> {
    if *pos + 2 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated field idx"));
    }
    let field = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    let value = decode_packed_value(data, pos)?;
    Ok((field, value))
}

// ---------------------------------------------------------------------------
// DocSnapshotCodec
// ---------------------------------------------------------------------------

pub struct DocSnapshotCodec;

impl SnapshotCodec for DocSnapshotCodec {
    type Snapshot = DocSnapshot;

    fn encode(snapshot: &DocSnapshot, buf: &mut Vec<u8>) {
        // [u32 num_docs]
        // per doc: [u32 slot_id][u16 num_fields][field_pairs...]
        buf.extend_from_slice(&(snapshot.docs.len() as u32).to_le_bytes());
        for (&slot, fields) in &snapshot.docs {
            buf.extend_from_slice(&slot.to_le_bytes());
            buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());
            for (field_idx, value) in fields {
                encode_field_pair(*field_idx, value, buf);
            }
        }
    }

    fn decode(bytes: &[u8]) -> io::Result<DocSnapshot> {
        let mut pos = 0;
        if bytes.len() < 4 {
            return Ok(DocSnapshot::new());
        }

        let num_docs = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut docs = HashMap::with_capacity(num_docs);
        for _ in 0..num_docs {
            if pos + 6 > bytes.len() {
                break;
            }
            let slot = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let num_fields = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            let mut fields = Vec::with_capacity(num_fields);
            for _ in 0..num_fields {
                let (field_idx, value) = decode_field_pair(bytes, &mut pos)?;
                fields.push((field_idx, value));
            }
            docs.insert(slot, fields);
        }

        Ok(DocSnapshot { docs })
    }

    fn empty() -> DocSnapshot {
        DocSnapshot::new()
    }
}

// ---------------------------------------------------------------------------
// DocOpCodec
// ---------------------------------------------------------------------------

pub struct DocOpCodec;

impl OpCodec for DocOpCodec {
    type Op = DocOp;
    type Snapshot = DocSnapshot;

    fn encode_op(op: &DocOp, buf: &mut Vec<u8>) {
        match op {
            DocOp::Set { slot, field, value } => {
                buf.push(OP_TAG_SET);
                buf.extend_from_slice(&slot.to_le_bytes());
                encode_field_pair(*field, value, buf);
            }
            DocOp::Append { slot, field, value } => {
                buf.push(OP_TAG_APPEND);
                buf.extend_from_slice(&slot.to_le_bytes());
                encode_field_pair(*field, value, buf);
            }
            DocOp::Remove { slot, field, value } => {
                buf.push(OP_TAG_REMOVE);
                buf.extend_from_slice(&slot.to_le_bytes());
                encode_field_pair(*field, value, buf);
            }
            DocOp::Delete { slot } => {
                buf.push(OP_TAG_DELETE);
                buf.extend_from_slice(&slot.to_le_bytes());
            }
            DocOp::Create { slot, fields } => {
                buf.push(OP_TAG_CREATE);
                buf.extend_from_slice(&slot.to_le_bytes());
                buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());
                for (field_idx, value) in fields {
                    encode_field_pair(*field_idx, value, buf);
                }
            }
        }
    }

    fn decode_op(bytes: &[u8]) -> io::Result<DocOp> {
        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty doc op"));
        }

        let tag = bytes[0];
        let mut pos = 1;

        match tag {
            OP_TAG_SET => {
                let slot = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated slot in Set")
                })?);
                pos += 4;
                let (field, value) = decode_field_pair(bytes, &mut pos)?;
                Ok(DocOp::Set { slot, field, value })
            }
            OP_TAG_APPEND => {
                let slot = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated slot in Append")
                })?);
                pos += 4;
                let (field, value) = decode_field_pair(bytes, &mut pos)?;
                Ok(DocOp::Append { slot, field, value })
            }
            OP_TAG_REMOVE => {
                let slot = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated slot in Remove")
                })?);
                pos += 4;
                let (field, value) = decode_field_pair(bytes, &mut pos)?;
                Ok(DocOp::Remove { slot, field, value })
            }
            OP_TAG_DELETE => {
                let slot = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated slot in Delete")
                })?);
                Ok(DocOp::Delete { slot })
            }
            OP_TAG_CREATE => {
                let slot = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated slot in Create")
                })?);
                pos += 4;
                let num_fields = u16::from_le_bytes(bytes[pos..pos + 2].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated field count in Create")
                })?) as usize;
                pos += 2;
                let mut fields = Vec::with_capacity(num_fields);
                for _ in 0..num_fields {
                    let (field_idx, value) = decode_field_pair(bytes, &mut pos)?;
                    fields.push((field_idx, value));
                }
                Ok(DocOp::Create { slot, fields })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown doc op tag: 0x{:02x}", other),
            )),
        }
    }

    fn apply(snapshot: &mut DocSnapshot, op: &DocOp) {
        match op {
            DocOp::Set { slot, field, value } => {
                let fields = snapshot.docs.entry(*slot).or_default();
                // Replace existing field or append
                if let Some(entry) = fields.iter_mut().find(|(f, _)| *f == *field) {
                    entry.1 = value.clone();
                } else {
                    fields.push((*field, value.clone()));
                }
            }
            DocOp::Append { slot, field, value } => {
                let fields = snapshot.docs.entry(*slot).or_default();
                if let Some(entry) = fields.iter_mut().find(|(f, _)| *f == *field) {
                    // Append to existing multi-value field
                    match &mut entry.1 {
                        PackedValue::Mi(v) => {
                            if let PackedValue::I(i) = value {
                                v.push(*i);
                            }
                        }
                        PackedValue::Mm(v) => {
                            v.push(value.clone());
                        }
                        _ => {
                            // Convert scalar to multi by wrapping
                            let old = std::mem::replace(&mut entry.1, PackedValue::Mm(vec![]));
                            if let PackedValue::Mm(ref mut v) = entry.1 {
                                v.push(old);
                                v.push(value.clone());
                            }
                        }
                    }
                } else {
                    // No existing field — create as single-element array
                    match value {
                        PackedValue::I(i) => fields.push((*field, PackedValue::Mi(vec![*i]))),
                        _ => fields.push((*field, PackedValue::Mm(vec![value.clone()]))),
                    }
                }
            }
            DocOp::Remove { slot, field, value } => {
                if let Some(fields) = snapshot.docs.get_mut(slot) {
                    if let Some(entry) = fields.iter_mut().find(|(f, _)| *f == *field) {
                        match &mut entry.1 {
                            PackedValue::Mi(v) => {
                                if let PackedValue::I(i) = value {
                                    v.retain(|x| x != i);
                                }
                            }
                            PackedValue::Mm(v) => {
                                // Remove by equality (best effort for mixed arrays)
                                v.retain(|x| !packed_value_eq(x, value));
                            }
                            _ => {} // Can't remove from a scalar
                        }
                    }
                }
            }
            DocOp::Delete { slot } => {
                snapshot.docs.remove(slot);
            }
            DocOp::Create { slot, fields } => {
                snapshot.docs.insert(*slot, fields.clone());
            }
        }
    }
}

/// Recursive equality check for PackedValue (used by Remove op).
fn packed_value_eq(a: &PackedValue, b: &PackedValue) -> bool {
    match (a, b) {
        (PackedValue::I(x), PackedValue::I(y)) => x == y,
        (PackedValue::F(x), PackedValue::F(y)) => x == y,
        (PackedValue::B(x), PackedValue::B(y)) => x == y,
        (PackedValue::S(x), PackedValue::S(y)) => x == y,
        (PackedValue::Mi(x), PackedValue::Mi(y)) => x == y,
        (PackedValue::Mm(x), PackedValue::Mm(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(a, b)| packed_value_eq(a, b))
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// SlotHexShard — maps slot_id to hex-bucketed shard file path
// ---------------------------------------------------------------------------

/// Shard key for document storage: the shard ID (slot_id >> SHARD_SHIFT).
pub type DocShardKey = u32;

/// Maps slot IDs to hex-bucketed shard files.
///
/// Layout: `{gen_root}/shards/{xx}/{NNNNNN}.shard`
/// where xx = (shard_id >> 8) & 0xFF, NNNNNN = shard_id.
///
/// This matches the existing DocStore V2 directory structure.
pub struct SlotHexShard;

impl SlotHexShard {
    /// Convert a slot ID to its shard ID.
    pub fn slot_to_shard(slot_id: u32) -> u32 {
        slot_id >> SHARD_SHIFT
    }
}

impl ShardingStrategy for SlotHexShard {
    type Key = DocShardKey;

    fn shard_path(&self, key: &DocShardKey, gen_root: &Path) -> PathBuf {
        let dir_byte = ((*key >> 8) & 0xFF) as u8;
        gen_root
            .join("shards")
            .join(format!("{:02x}", dir_byte))
            .join(format!("{:06}.shard", key))
    }

    fn list_shards(&self, gen_root: &Path) -> io::Result<Vec<DocShardKey>> {
        let shards_dir = gen_root.join("shards");
        let mut keys = Vec::new();

        if !shards_dir.exists() {
            return Ok(keys);
        }

        for hex_entry in std::fs::read_dir(&shards_dir)? {
            let hex_entry = hex_entry?;
            if !hex_entry.file_type()?.is_dir() {
                continue;
            }
            for shard_entry in std::fs::read_dir(hex_entry.path())? {
                let shard_entry = shard_entry?;
                let name = shard_entry.file_name().to_string_lossy().into_owned();
                if let Some(id_str) = name.strip_suffix(".shard") {
                    if let Ok(shard_id) = id_str.parse::<u32>() {
                        keys.push(shard_id);
                    }
                }
            }
        }

        Ok(keys)
    }
}

/// Type alias for a document ShardStore.
pub type DocShardStore = crate::shard_store::ShardStore<DocSnapshotCodec, DocOpCodec, SlotHexShard>;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packed_value_roundtrip_i64() {
        let pv = PackedValue::I(42);
        let mut buf = Vec::new();
        encode_packed_value(&pv, &mut buf);
        let mut pos = 0;
        let decoded = decode_packed_value(&buf, &mut pos).unwrap();
        assert_eq!(decoded, pv);
    }

    #[test]
    fn test_packed_value_roundtrip_string() {
        let pv = PackedValue::S("hello world".into());
        let mut buf = Vec::new();
        encode_packed_value(&pv, &mut buf);
        let mut pos = 0;
        let decoded = decode_packed_value(&buf, &mut pos).unwrap();
        assert_eq!(decoded, pv);
    }

    #[test]
    fn test_packed_value_roundtrip_mi() {
        let pv = PackedValue::Mi(vec![1, 2, 3, 100, -5]);
        let mut buf = Vec::new();
        encode_packed_value(&pv, &mut buf);
        let mut pos = 0;
        let decoded = decode_packed_value(&buf, &mut pos).unwrap();
        assert_eq!(decoded, pv);
    }

    #[test]
    fn test_packed_value_roundtrip_nested_mm() {
        let pv = PackedValue::Mm(vec![
            PackedValue::I(1),
            PackedValue::S("two".into()),
            PackedValue::B(true),
        ]);
        let mut buf = Vec::new();
        encode_packed_value(&pv, &mut buf);
        let mut pos = 0;
        let decoded = decode_packed_value(&buf, &mut pos).unwrap();
        assert_eq!(decoded, pv);
    }

    #[test]
    fn test_doc_op_set_roundtrip() {
        let op = DocOp::Set {
            slot: 12345,
            field: 3,
            value: PackedValue::I(99),
        };
        let mut buf = Vec::new();
        DocOpCodec::encode_op(&op, &mut buf);
        let decoded = DocOpCodec::decode_op(&buf).unwrap();
        match decoded {
            DocOp::Set { slot, field, value } => {
                assert_eq!(slot, 12345);
                assert_eq!(field, 3);
                assert_eq!(value, PackedValue::I(99));
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_doc_op_create_roundtrip() {
        let op = DocOp::Create {
            slot: 42,
            fields: vec![
                (0, PackedValue::I(1)),
                (1, PackedValue::S("test".into())),
                (2, PackedValue::Mi(vec![10, 20])),
            ],
        };
        let mut buf = Vec::new();
        DocOpCodec::encode_op(&op, &mut buf);
        let decoded = DocOpCodec::decode_op(&buf).unwrap();
        match decoded {
            DocOp::Create { slot, fields } => {
                assert_eq!(slot, 42);
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0], (0, PackedValue::I(1)));
                assert_eq!(fields[1], (1, PackedValue::S("test".into())));
            }
            _ => panic!("expected Create"),
        }
    }

    #[test]
    fn test_doc_op_delete_roundtrip() {
        let op = DocOp::Delete { slot: 999 };
        let mut buf = Vec::new();
        DocOpCodec::encode_op(&op, &mut buf);
        let decoded = DocOpCodec::decode_op(&buf).unwrap();
        match decoded {
            DocOp::Delete { slot } => assert_eq!(slot, 999),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_doc_snapshot_roundtrip() {
        let mut snap = DocSnapshot::new();
        snap.docs.insert(1, vec![
            (0, PackedValue::I(42)),
            (1, PackedValue::S("bitdex".into())),
        ]);
        snap.docs.insert(2, vec![
            (0, PackedValue::I(99)),
            (2, PackedValue::Mi(vec![1, 2, 3])),
        ]);

        let mut buf = Vec::new();
        DocSnapshotCodec::encode(&snap, &mut buf);
        let decoded = DocSnapshotCodec::decode(&buf).unwrap();

        assert_eq!(decoded.docs.len(), 2);
        assert_eq!(decoded.docs[&1].len(), 2);
        assert_eq!(decoded.docs[&2].len(), 2);
        assert_eq!(decoded.docs[&1][0], (0, PackedValue::I(42)));
    }

    #[test]
    fn test_apply_set_op() {
        let mut snap = DocSnapshot::new();
        snap.docs.insert(1, vec![(0, PackedValue::I(1))]);

        DocOpCodec::apply(&mut snap, &DocOp::Set {
            slot: 1, field: 0, value: PackedValue::I(99)
        });

        assert_eq!(snap.docs[&1][0], (0, PackedValue::I(99)));
    }

    #[test]
    fn test_apply_set_new_field() {
        let mut snap = DocSnapshot::new();
        snap.docs.insert(1, vec![(0, PackedValue::I(1))]);

        DocOpCodec::apply(&mut snap, &DocOp::Set {
            slot: 1, field: 5, value: PackedValue::S("new".into())
        });

        assert_eq!(snap.docs[&1].len(), 2);
    }

    #[test]
    fn test_apply_append_op() {
        let mut snap = DocSnapshot::new();
        snap.docs.insert(1, vec![(0, PackedValue::Mi(vec![10, 20]))]);

        DocOpCodec::apply(&mut snap, &DocOp::Append {
            slot: 1, field: 0, value: PackedValue::I(30)
        });

        match &snap.docs[&1][0].1 {
            PackedValue::Mi(v) => assert_eq!(v, &[10, 20, 30]),
            _ => panic!("expected Mi"),
        }
    }

    #[test]
    fn test_apply_remove_op() {
        let mut snap = DocSnapshot::new();
        snap.docs.insert(1, vec![(0, PackedValue::Mi(vec![10, 20, 30]))]);

        DocOpCodec::apply(&mut snap, &DocOp::Remove {
            slot: 1, field: 0, value: PackedValue::I(20)
        });

        match &snap.docs[&1][0].1 {
            PackedValue::Mi(v) => assert_eq!(v, &[10, 30]),
            _ => panic!("expected Mi"),
        }
    }

    #[test]
    fn test_apply_delete_op() {
        let mut snap = DocSnapshot::new();
        snap.docs.insert(1, vec![(0, PackedValue::I(1))]);
        snap.docs.insert(2, vec![(0, PackedValue::I(2))]);

        DocOpCodec::apply(&mut snap, &DocOp::Delete { slot: 1 });

        assert!(!snap.docs.contains_key(&1));
        assert!(snap.docs.contains_key(&2));
    }

    #[test]
    fn test_apply_create_op() {
        let mut snap = DocSnapshot::new();

        DocOpCodec::apply(&mut snap, &DocOp::Create {
            slot: 42,
            fields: vec![
                (0, PackedValue::I(1)),
                (1, PackedValue::S("hello".into())),
            ],
        });

        assert_eq!(snap.docs[&42].len(), 2);
        assert_eq!(snap.docs[&42][0], (0, PackedValue::I(1)));
    }

    #[test]
    fn test_slot_hex_shard_path() {
        let shard = SlotHexShard;
        let key: DocShardKey = 0x0123; // shard ID
        let path = shard.shard_path(&key, Path::new("/data/gen_000"));
        assert_eq!(path, PathBuf::from("/data/gen_000/shards/01/000291.shard"));
    }

    #[test]
    fn test_slot_to_shard() {
        // slot 0-511 → shard 0
        assert_eq!(SlotHexShard::slot_to_shard(0), 0);
        assert_eq!(SlotHexShard::slot_to_shard(511), 0);
        // slot 512+ → shard 1
        assert_eq!(SlotHexShard::slot_to_shard(512), 1);
        assert_eq!(SlotHexShard::slot_to_shard(1023), 1);
    }

    #[test]
    fn test_doc_shardstore_full_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

        let shard_key = SlotHexShard::slot_to_shard(42);

        // Create a doc via Create op
        store.append_op(&shard_key, &DocOp::Create {
            slot: 42,
            fields: vec![
                (0, PackedValue::I(1)),
                (1, PackedValue::S("hello".into())),
                (2, PackedValue::Mi(vec![10, 20])),
            ],
        }).unwrap();

        // Modify via Set
        store.append_op(&shard_key, &DocOp::Set {
            slot: 42, field: 0, value: PackedValue::I(99)
        }).unwrap();

        // Append to multi-value
        store.append_op(&shard_key, &DocOp::Append {
            slot: 42, field: 2, value: PackedValue::I(30)
        }).unwrap();

        // Read back
        let snap = store.read(&shard_key).unwrap().unwrap();
        let doc = &snap.docs[&42];

        // field 0 should be 99 (Set overrode 1)
        assert_eq!(doc.iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(99));
        // field 1 should be "hello"
        assert_eq!(doc.iter().find(|(f, _)| *f == 1).unwrap().1, PackedValue::S("hello".into()));
        // field 2 should be [10, 20, 30]
        match &doc.iter().find(|(f, _)| *f == 2).unwrap().1 {
            PackedValue::Mi(v) => assert_eq!(v, &[10, 20, 30]),
            other => panic!("expected Mi, got {:?}", other),
        }
    }

    #[test]
    fn test_doc_shardstore_compact() {
        let dir = tempfile::tempdir().unwrap();
        let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

        let shard_key = SlotHexShard::slot_to_shard(100);

        // Create + modify via ops
        store.append_op(&shard_key, &DocOp::Create {
            slot: 100,
            fields: vec![(0, PackedValue::I(1))],
        }).unwrap();
        store.append_op(&shard_key, &DocOp::Set {
            slot: 100, field: 0, value: PackedValue::I(42)
        }).unwrap();

        assert_eq!(store.ops_count(&shard_key).unwrap(), Some(2));

        // Compact
        store.compact_shard(&shard_key, 0).unwrap();

        // After compaction: zero ops, data preserved
        assert_eq!(store.ops_count(&shard_key).unwrap(), Some(0));
        let snap = store.read(&shard_key).unwrap().unwrap();
        assert_eq!(snap.docs[&100][0], (0, PackedValue::I(42)));
    }
}
