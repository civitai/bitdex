//! Document wire format — types, codecs, and serialization primitives.
//!
//! This module defines the on-disk encoding shared by DocSilo's underlying ShardStore:
//! - `StoredDoc` — the named-field document type used across the codebase
//! - `PackedValue` — compact enum for field values (integer, float, bool, string, multi)
//! - `DocStoreV3` — high-level document store backed by ShardStore
//! - `ShardStoreBulkWriter` — high-throughput parallel writer for dump processor
//! - `DocSnapshotCodec` / `DocOpCodec` — ShardStore codecs
//! - `SlotHexShard` — hex-bucketed shard file layout
//! - `json_to_packed_with_dict` — JSON → PackedValue conversion with dictionary support

use ahash::AHashMap as HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};

use crate::config::{FieldMapping, FieldValueType};
use crate::mutation::FieldValue;
use crate::shard_store::{SnapshotCodec, OpCodec, ShardingStrategy};

// ---------------------------------------------------------------------------
// Core types — StoredDoc + PackedValue
// ---------------------------------------------------------------------------

/// Number of bits to shift slot_id right to get shard index.
/// 9 → 512 docs per shard.
pub const SHARD_SHIFT: u32 = 9;

/// Public accessor for SHARD_SHIFT (used by slot_arena finalization).
pub const SHARD_SHIFT_PUB: u32 = SHARD_SHIFT;

/// A stored document containing all field values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredDoc {
    pub fields: HashMap<String, FieldValue>,
    /// Schema version this document was encoded with.
    /// 0 = legacy (pre-versioning), 1+ = versioned.
    #[serde(skip, default)]
    pub schema_version: u8,
}

/// Compact value encoding for document fields.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum PackedValue {
    I(i64),
    F(f64),
    B(bool),
    S(String),
    Mi(Vec<i64>),
    Mm(Vec<PackedValue>),
}

/// Convert a raw JSON value to PackedValue, with optional dictionary for LowCardinalityString.
pub fn json_to_packed_with_dict(
    raw: &serde_json::Value,
    mapping: &FieldMapping,
    ms_to_seconds: bool,
    dictionary: Option<&crate::dictionary::FieldDictionary>,
) -> Option<PackedValue> {
    match mapping.value_type {
        FieldValueType::Integer => {
            let n = raw
                .as_i64()
                .or_else(|| raw.as_u64().map(|u| u as i64))
                .or_else(|| raw.as_f64().map(|f| f as i64))?;
            let n = if ms_to_seconds {
                ((n / 1000) as u32) as i64
            } else {
                n
            };
            Some(PackedValue::I(n))
        }
        FieldValueType::Boolean => Some(PackedValue::B(raw.as_bool()?)),
        FieldValueType::String => Some(PackedValue::S(raw.as_str()?.to_string())),
        FieldValueType::MappedString => {
            let s = raw.as_str()?;
            let lookup = if mapping.case_sensitive {
                std::borrow::Cow::Borrowed(s)
            } else {
                std::borrow::Cow::Owned(s.to_lowercase())
            };
            let n = mapping
                .string_map
                .as_ref()
                .and_then(|m| m.get(lookup.as_ref()).copied())
                .unwrap_or(0);
            Some(PackedValue::I(n))
        }
        FieldValueType::LowCardinalityString => {
            let s = raw.as_str()?;
            if let Some(dict) = dictionary {
                let n = dict.get_or_insert(s);
                Some(PackedValue::I(n))
            } else {
                Some(PackedValue::I(0))
            }
        }
        FieldValueType::IntegerArray => {
            let arr = raw.as_array()?;
            if arr.is_empty() {
                return None;
            }
            let values: Vec<i64> = arr
                .iter()
                .filter_map(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)))
                .collect();
            if values.is_empty() { None } else { Some(PackedValue::Mi(values)) }
        }
        FieldValueType::ExistsBoolean => Some(PackedValue::B(true)),
    }
}

// ---------------------------------------------------------------------------
// Shard layout
// ---------------------------------------------------------------------------

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

    /// Merge fields into an existing document (or create if absent).
    /// Unlike Create which replaces the entire doc, Merge upserts each field.
    /// Used by multi-phase dump writes where phases add fields incrementally.
    Merge { slot: u32, fields: Vec<(u16, PackedValue)> },
}

// ---------------------------------------------------------------------------
// Op tags for serialization
// ---------------------------------------------------------------------------

const OP_TAG_SET: u8 = 0x01;
const OP_TAG_APPEND: u8 = 0x02;
const OP_TAG_REMOVE: u8 = 0x03;
const OP_TAG_DELETE: u8 = 0x04;
const OP_TAG_CREATE: u8 = 0x05;
const OP_TAG_MERGE: u8 = 0x06;

// ---------------------------------------------------------------------------
// PackedValue binary encoding (compact, no msgpack dependency)
// ---------------------------------------------------------------------------

const PV_TAG_I: u8 = 0x01;
const PV_TAG_F: u8 = 0x02;
const PV_TAG_B: u8 = 0x03;
const PV_TAG_S: u8 = 0x04;
const PV_TAG_MI: u8 = 0x05;
const PV_TAG_MM: u8 = 0x06;

// ---------------------------------------------------------------------------
// Zero-copy wire format helpers — write Merge ops directly from borrowed data.
// Used by the dump pipeline to avoid building PackedValue (which owns Strings).
// Same wire format as DocOpCodec::encode_op for DocOp::Merge.
// ---------------------------------------------------------------------------

/// Write a DocOp::Merge header: [tag][slot:u32][num_fields:u16].
/// Caller follows with one write_field_* call per field.
pub(crate) fn write_merge_header(slot: u32, num_fields: u16, buf: &mut Vec<u8>) {
    buf.push(OP_TAG_MERGE);
    buf.extend_from_slice(&slot.to_le_bytes());
    buf.extend_from_slice(&num_fields.to_le_bytes());
}

/// Write a (field_idx, i64) pair: [field:u16][PV_TAG_I][i64 LE].
pub(crate) fn write_field_int(field: u16, value: i64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field.to_le_bytes());
    buf.push(PV_TAG_I);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a (field_idx, bool) pair: [field:u16][PV_TAG_B][u8].
pub(crate) fn write_field_bool(field: u16, value: bool, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field.to_le_bytes());
    buf.push(PV_TAG_B);
    buf.push(if value { 1 } else { 0 });
}

/// Write a (field_idx, &str) pair: [field:u16][PV_TAG_S][len:u32 LE][bytes].
/// Zero-copy — borrows from caller's string slice.
pub(crate) fn write_field_str(field: u16, value: &str, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field.to_le_bytes());
    buf.push(PV_TAG_S);
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
}

/// Write a (field_idx, &[i64]) pair: [field:u16][PV_TAG_MI][len:u32 LE][i64 LE...].
pub(crate) fn write_field_multi_int(field: u16, values: &[i64], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field.to_le_bytes());
    buf.push(PV_TAG_MI);
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
}

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
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("truncated doc snapshot: expected {} docs, decoded {}", num_docs, docs.len()),
                ));
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
            DocOp::Create { slot, fields } | DocOp::Merge { slot, fields } => {
                let tag = if matches!(op, DocOp::Merge { .. }) { OP_TAG_MERGE } else { OP_TAG_CREATE };
                buf.push(tag);
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
            OP_TAG_CREATE | OP_TAG_MERGE => {
                let label = if tag == OP_TAG_MERGE { "Merge" } else { "Create" };
                let slot = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, format!("truncated slot in {}", label))
                })?);
                pos += 4;
                let num_fields = u16::from_le_bytes(bytes[pos..pos + 2].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, format!("truncated field count in {}", label))
                })?) as usize;
                pos += 2;
                let mut fields = Vec::with_capacity(num_fields);
                for _ in 0..num_fields {
                    let (field_idx, value) = decode_field_pair(bytes, &mut pos)?;
                    fields.push((field_idx, value));
                }
                if tag == OP_TAG_MERGE {
                    Ok(DocOp::Merge { slot, fields })
                } else {
                    Ok(DocOp::Create { slot, fields })
                }
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
                                if !v.contains(i) {
                                    v.push(*i);
                                }
                            }
                        }
                        PackedValue::Mm(v) => {
                            if !v.iter().any(|x| packed_value_eq(x, value)) {
                                v.push(value.clone());
                            }
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
            DocOp::Merge { slot, fields } => {
                let doc = snapshot.docs.entry(*slot).or_default();
                for (field_idx, value) in fields {
                    if let Some(entry) = doc.iter_mut().find(|(f, _)| *f == *field_idx) {
                        // Multi-value union semantics: when both old and new are
                        // multi-int (Mi), union the value lists with dedup. This
                        // is critical for bulk dumps where multiple parallel chunks
                        // emit Merge ops for the same slot with different subsets
                        // of values (e.g., tags phase where each chunk processes
                        // its own range of tag rows for a given imageId).
                        // Without this, the last Merge would overwrite all prior
                        // values, losing tags.
                        match (&mut entry.1, value) {
                            (PackedValue::Mi(existing), PackedValue::Mi(incoming)) => {
                                for v in incoming {
                                    if !existing.contains(v) {
                                        existing.push(*v);
                                    }
                                }
                            }
                            _ => entry.1 = value.clone(),
                        }
                    } else {
                        doc.push((*field_idx, value.clone()));
                    }
                }
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
