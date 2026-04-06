//! Document format types and codecs.
//!
//! This module is the single source of truth for document encoding:
//! - `StoredDoc` — the named-field document type used across the codebase
//! - `PackedValue` — compact enum for field values (integer, float, bool, string, multi)
//! - `DocOp` — typed document operations (Set, Append, Remove, Delete, Create, Merge)
//! - `DocSnapshot` — materialized state of a shard (slot_id → fields)
//! - Standalone encode/decode functions (DocOpCodec format, 71ns encode / 16ns decode)
//! - `json_to_packed_with_dict` — JSON → PackedValue conversion with dictionary support

use std::collections::HashMap;
use std::io;

use crate::config::{FieldMapping, FieldValueType};
use crate::mutation::FieldValue;

// ---------------------------------------------------------------------------
// Core types — StoredDoc + PackedValue
// ---------------------------------------------------------------------------

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
// DocSnapshot — materialized state of a document group
// ---------------------------------------------------------------------------

/// A snapshot of all documents in a group.
/// Maps slot_id → list of (field_idx, value) pairs.
#[derive(Debug, Clone, PartialEq)]
pub struct DocSnapshot {
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
// Shared wire format primitives — single source of truth for field encoding.
// Used by both PackedValue (general path) and DumpFieldValue (zero-copy dump path).
// ---------------------------------------------------------------------------

/// Write a Merge op header: tag + slot + field count.
#[inline]
pub fn write_merge_header(slot: u32, field_count: u16, buf: &mut Vec<u8>) {
    buf.push(OP_TAG_MERGE);
    buf.extend_from_slice(&slot.to_le_bytes());
    buf.extend_from_slice(&field_count.to_le_bytes());
}

/// Write an i64 field value.
#[inline]
pub fn write_field_int(field_idx: u16, value: i64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field_idx.to_le_bytes());
    buf.push(PV_TAG_I);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a bool field value.
#[inline]
pub fn write_field_bool(field_idx: u16, value: bool, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field_idx.to_le_bytes());
    buf.push(PV_TAG_B);
    buf.push(if value { 1 } else { 0 });
}

/// Write a string field value (takes &str — works for both owned and borrowed).
#[inline]
pub fn write_field_str(field_idx: u16, value: &str, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field_idx.to_le_bytes());
    buf.push(PV_TAG_S);
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
}

/// Write a multi-int field value.
#[inline]
pub fn write_field_multi_int(field_idx: u16, values: &[i64], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field_idx.to_le_bytes());
    buf.push(PV_TAG_MI);
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for val in values {
        buf.extend_from_slice(&val.to_le_bytes());
    }
}

pub fn encode_packed_value(pv: &PackedValue, buf: &mut Vec<u8>) {
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

pub fn decode_packed_value(data: &[u8], pos: &mut usize) -> io::Result<PackedValue> {
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
pub fn encode_field_pair(field: u16, value: &PackedValue, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field.to_le_bytes());
    encode_packed_value(value, buf);
}

/// Decode a field pair: returns (field_idx, value) and advances pos.
pub fn decode_field_pair(data: &[u8], pos: &mut usize) -> io::Result<(u16, PackedValue)> {
    if *pos + 2 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated field idx"));
    }
    let field = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    let value = decode_packed_value(data, pos)?;
    Ok((field, value))
}

// ---------------------------------------------------------------------------
// DocOp codec — standalone encode/decode/apply (DocOpCodec format, 71ns/16ns)
// ---------------------------------------------------------------------------

/// Encode a DocOp to bytes in DocOpCodec format.
pub fn encode_doc_op(op: &DocOp, buf: &mut Vec<u8>) {
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

/// Decode a DocOp from bytes in DocOpCodec format.
pub fn decode_doc_op(bytes: &[u8]) -> io::Result<DocOp> {
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

/// Apply a DocOp to a DocSnapshot (mutates in place).
pub fn apply_doc_op(snapshot: &mut DocSnapshot, op: &DocOp) {
    match op {
        DocOp::Set { slot, field, value } => {
            let fields = snapshot.docs.entry(*slot).or_default();
            if let Some(entry) = fields.iter_mut().find(|(f, _)| *f == *field) {
                entry.1 = value.clone();
            } else {
                fields.push((*field, value.clone()));
            }
        }
        DocOp::Append { slot, field, value } => {
            let fields = snapshot.docs.entry(*slot).or_default();
            if let Some(entry) = fields.iter_mut().find(|(f, _)| *f == *field) {
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
                        let old = std::mem::replace(&mut entry.1, PackedValue::Mm(vec![]));
                        if let PackedValue::Mm(ref mut v) = entry.1 {
                            v.push(old);
                            v.push(value.clone());
                        }
                    }
                }
            } else {
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
                            v.retain(|x| !packed_value_eq(x, value));
                        }
                        _ => {}
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
                    // Mi fields: concatenate instead of replace (enables streaming MV doc ops)
                    match (&mut entry.1, value) {
                        (PackedValue::Mi(existing), PackedValue::Mi(new_vals)) => {
                            existing.extend(new_vals.iter());
                        }
                        _ => { entry.1 = value.clone(); }
                    }
                } else {
                    doc.push((*field_idx, value.clone()));
                }
            }
        }
    }
}

/// Recursive equality check for PackedValue (used by Remove op).
pub fn packed_value_eq(a: &PackedValue, b: &PackedValue) -> bool {
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
// DocSnapshot codec — standalone encode/decode
// ---------------------------------------------------------------------------

/// Encode a DocSnapshot to bytes.
pub fn encode_doc_snapshot(snapshot: &DocSnapshot, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(snapshot.docs.len() as u32).to_le_bytes());
    for (&slot, fields) in &snapshot.docs {
        buf.extend_from_slice(&slot.to_le_bytes());
        buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());
        for (field_idx, value) in fields {
            encode_field_pair(*field_idx, value, buf);
        }
    }
}

/// Decode a DocSnapshot from bytes.
pub fn decode_doc_snapshot(bytes: &[u8]) -> io::Result<DocSnapshot> {
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

// ---------------------------------------------------------------------------
// Convenience: encode a Merge op directly (used by dump pipeline)
// ---------------------------------------------------------------------------

/// Encode a Merge op for a slot with given field tuples.
/// Returns the raw bytes suitable for DataSilo storage.
pub fn encode_merge_fields(slot: u32, fields: &[(u16, PackedValue)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(7 + fields.len() * 12);
    buf.push(OP_TAG_MERGE);
    buf.extend_from_slice(&slot.to_le_bytes());
    buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    for (field_idx, value) in fields {
        encode_field_pair(*field_idx, value, &mut buf);
    }
    buf
}

/// Encode a Merge op into a caller-provided buffer. Zero allocation.
pub fn encode_merge_fields_into(slot: u32, fields: &[(u16, PackedValue)], buf: &mut Vec<u8>) {
    buf.clear();
    buf.push(OP_TAG_MERGE);
    buf.extend_from_slice(&slot.to_le_bytes());
    buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    for (field_idx, value) in fields {
        encode_field_pair(*field_idx, value, buf);
    }
}

/// Encode a Create op for a slot with given field tuples.
pub fn encode_create_fields(slot: u32, fields: &[(u16, PackedValue)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(7 + fields.len() * 12);
    buf.push(OP_TAG_CREATE);
    buf.extend_from_slice(&slot.to_le_bytes());
    buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    for (field_idx, value) in fields {
        encode_field_pair(*field_idx, value, &mut buf);
    }
    buf
}

/// Decode fields from raw bytes stored in DataSilo.
/// Returns the list of (field_idx, value) pairs from a Create or Merge op.
pub fn decode_doc_fields(bytes: &[u8]) -> io::Result<Vec<(u16, PackedValue)>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let op = decode_doc_op(bytes)?;
    match op {
        DocOp::Create { fields, .. } | DocOp::Merge { fields, .. } => Ok(fields),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected Create or Merge op in doc silo entry",
        )),
    }
}

/// Merge two encoded doc records (Merge ops stored in DataSilo).
///
/// Decodes both records, merges field-by-field:
/// - `Mi` fields: concatenate arrays (multi-value accumulation)
/// - All other fields: new value replaces existing
///
/// Returns the re-encoded merged record.
/// Used by `DumpMergeWriter` during dump phases to fuse doc ops in-place.
pub fn merge_encoded_docs(existing: &[u8], new_data: &[u8]) -> io::Result<Vec<u8>> {
    let mut fields = decode_doc_fields(existing)?;
    let new_fields = decode_doc_fields(new_data)?;

    for (field_idx, value) in new_fields {
        if let Some(entry) = fields.iter_mut().find(|(f, _)| *f == field_idx) {
            // Mi fields: concatenate instead of replace
            match (&mut entry.1, &value) {
                (PackedValue::Mi(existing_vals), PackedValue::Mi(new_vals)) => {
                    existing_vals.extend_from_slice(new_vals);
                }
                _ => { entry.1 = value; }
            }
        } else {
            fields.push((field_idx, value));
        }
    }

    // Extract slot from existing record header (byte 1..5 after the op tag)
    let slot = if existing.len() >= 5 {
        u32::from_le_bytes(existing[1..5].try_into().unwrap())
    } else {
        0
    };
    Ok(encode_merge_fields(slot, &fields))
}

/// Merge two encoded doc records into a caller-provided buffer. Zero allocation
/// except for the field Vec decode. Used from DumpMergeWriter for maximum throughput.
pub fn merge_encoded_docs_into(existing: &[u8], new_data: &[u8], buf: &mut Vec<u8>) -> io::Result<()> {
    let mut fields = decode_doc_fields(existing)?;
    let new_fields = decode_doc_fields(new_data)?;

    for (field_idx, value) in new_fields {
        if let Some(entry) = fields.iter_mut().find(|(f, _)| *f == field_idx) {
            match (&mut entry.1, &value) {
                (PackedValue::Mi(existing_vals), PackedValue::Mi(new_vals)) => {
                    existing_vals.extend_from_slice(new_vals);
                }
                _ => { entry.1 = value; }
            }
        } else {
            fields.push((field_idx, value));
        }
    }

    let slot = if existing.len() >= 5 {
        u32::from_le_bytes(existing[1..5].try_into().unwrap())
    } else {
        0
    };
    encode_merge_fields_into(slot, &fields, buf);
    Ok(())
}

/// Decode a full StoredDoc from raw DataSilo bytes, using the field index→name mapping.
/// Optionally applies field defaults for missing fields.
pub fn decode_stored_doc(
    bytes: &[u8],
    idx_to_field: &[String],
    field_defaults: Option<&HashMap<u16, PackedValue>>,
) -> io::Result<StoredDoc> {
    let fields_packed = decode_doc_fields(bytes)?;
    let mut fields = HashMap::with_capacity(fields_packed.len());
    for (idx, pv) in &fields_packed {
        let name = idx_to_field.get(*idx as usize)
            .cloned()
            .unwrap_or_else(|| format!("field_{}", idx));
        let fv = packed_to_field_value(pv);
        fields.insert(name, fv);
    }
    // Apply defaults for missing fields
    if let Some(defaults) = field_defaults {
        for (&idx, default_pv) in defaults {
            if let Some(name) = idx_to_field.get(idx as usize) {
                if !fields.contains_key(name) {
                    fields.insert(name.clone(), packed_to_field_value(default_pv));
                }
            }
        }
    }
    Ok(StoredDoc { fields, schema_version: 0 })
}

/// Convert a PackedValue to a FieldValue.
pub fn packed_to_field_value(pv: &PackedValue) -> FieldValue {
    use crate::query::Value;
    match pv {
        PackedValue::I(i) => FieldValue::Single(Value::Integer(*i)),
        PackedValue::F(f) => FieldValue::Single(Value::Float(*f)),
        PackedValue::B(b) => FieldValue::Single(Value::Bool(*b)),
        PackedValue::S(s) => FieldValue::Single(Value::String(s.clone())),
        PackedValue::Mi(v) => FieldValue::Multi(v.iter().map(|i| Value::Integer(*i)).collect()),
        PackedValue::Mm(v) => FieldValue::Multi(v.iter().filter_map(|pv| match pv {
            PackedValue::I(i) => Some(Value::Integer(*i)),
            PackedValue::F(f) => Some(Value::Float(*f)),
            PackedValue::B(b) => Some(Value::Bool(*b)),
            PackedValue::S(s) => Some(Value::String(s.clone())),
            other => {
                eprintln!("packed_to_field_value: skipping nested multi-value {:?}", std::mem::discriminant(other));
                None
            }
        }).collect()),
    }
}

/// Convert a FieldValue to a PackedValue.
pub fn field_value_to_packed(fv: &FieldValue) -> PackedValue {
    use crate::query::Value;
    match fv {
        FieldValue::Single(v) => match v {
            Value::Integer(i) => PackedValue::I(*i),
            Value::Float(f) => PackedValue::F(*f),
            Value::Bool(b) => PackedValue::B(*b),
            Value::String(s) => PackedValue::S(s.clone()),
        },
        FieldValue::Multi(vs) => {
            if vs.iter().all(|v| matches!(v, Value::Integer(_))) {
                PackedValue::Mi(vs.iter().map(|v| match v {
                    Value::Integer(i) => *i,
                    _ => unreachable!(),
                }).collect())
            } else {
                PackedValue::Mm(vs.iter().map(|v| match v {
                    Value::Integer(i) => PackedValue::I(*i),
                    Value::Float(f) => PackedValue::F(*f),
                    Value::Bool(b) => PackedValue::B(*b),
                    Value::String(s) => PackedValue::S(s.clone()),
                }).collect())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packed_value_roundtrip() {
        let values = vec![
            PackedValue::I(42),
            PackedValue::F(3.14),
            PackedValue::B(true),
            PackedValue::S("hello".into()),
            PackedValue::Mi(vec![1, 2, 3]),
        ];
        for pv in &values {
            let mut buf = Vec::new();
            encode_packed_value(pv, &mut buf);
            let mut pos = 0;
            let decoded = decode_packed_value(&buf, &mut pos).unwrap();
            assert_eq!(&decoded, pv);
        }
    }

    #[test]
    fn test_doc_op_merge_roundtrip() {
        let fields = vec![
            (0, PackedValue::I(123)),
            (1, PackedValue::S("test".into())),
            (2, PackedValue::B(true)),
        ];
        let op = DocOp::Merge { slot: 42, fields };
        let mut buf = Vec::new();
        encode_doc_op(&op, &mut buf);
        let decoded = decode_doc_op(&buf).unwrap();
        match decoded {
            DocOp::Merge { slot, fields } => {
                assert_eq!(slot, 42);
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0], (0, PackedValue::I(123)));
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn test_encode_merge_fields_convenience() {
        let fields = vec![
            (0u16, PackedValue::I(100)),
            (5, PackedValue::S("hello".into())),
        ];
        let bytes = encode_merge_fields(42, &fields);
        let decoded = decode_doc_fields(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], (0, PackedValue::I(100)));
    }

    #[test]
    fn test_apply_merge_upserts() {
        let mut snap = DocSnapshot::new();
        let op1 = DocOp::Create { slot: 1, fields: vec![(0, PackedValue::I(10))] };
        apply_doc_op(&mut snap, &op1);
        let op2 = DocOp::Merge { slot: 1, fields: vec![(0, PackedValue::I(20)), (1, PackedValue::S("new".into()))] };
        apply_doc_op(&mut snap, &op2);
        let doc = &snap.docs[&1];
        assert_eq!(doc.len(), 2);
        assert_eq!(doc[0], (0, PackedValue::I(20)));
        assert_eq!(doc[1], (1, PackedValue::S("new".into())));
    }

    #[test]
    fn test_doc_snapshot_roundtrip() {
        let mut snap = DocSnapshot::new();
        snap.docs.insert(1, vec![(0, PackedValue::I(42))]);
        snap.docs.insert(2, vec![(1, PackedValue::S("hi".into()))]);
        let mut buf = Vec::new();
        encode_doc_snapshot(&snap, &mut buf);
        let decoded = decode_doc_snapshot(&buf).unwrap();
        assert_eq!(decoded.docs.len(), 2);
        assert_eq!(decoded.docs[&1], vec![(0, PackedValue::I(42))]);
    }

    #[test]
    fn test_merge_mi_concatenates() {
        let mut snap = DocSnapshot::new();
        // First merge: create slot with Mi field
        let op1 = DocOp::Merge { slot: 1, fields: vec![(0, PackedValue::Mi(vec![10, 20]))] };
        apply_doc_op(&mut snap, &op1);
        assert_eq!(snap.docs[&1], vec![(0, PackedValue::Mi(vec![10, 20]))]);

        // Second merge: Mi field should concatenate, not replace
        let op2 = DocOp::Merge { slot: 1, fields: vec![(0, PackedValue::Mi(vec![30, 40]))] };
        apply_doc_op(&mut snap, &op2);
        assert_eq!(snap.docs[&1], vec![(0, PackedValue::Mi(vec![10, 20, 30, 40]))]);

        // Non-Mi field still replaces on merge
        let op3 = DocOp::Merge { slot: 1, fields: vec![(1, PackedValue::I(100))] };
        apply_doc_op(&mut snap, &op3);
        let op4 = DocOp::Merge { slot: 1, fields: vec![(1, PackedValue::I(200))] };
        apply_doc_op(&mut snap, &op4);
        let doc = &snap.docs[&1];
        assert_eq!(doc.iter().find(|(f, _)| *f == 1).unwrap().1, PackedValue::I(200));
    }

    #[test]
    fn test_merge_encoded_docs_basic() {
        // Create first doc: slot=1, field 0 = I(42), field 1 = Mi([10, 20])
        let existing = encode_merge_fields(1, &[
            (0, PackedValue::I(42)),
            (1, PackedValue::Mi(vec![10, 20])),
        ]);

        // Create second doc: slot=1, field 1 = Mi([30, 40]), field 2 = I(99)
        let new_data = encode_merge_fields(1, &[
            (1, PackedValue::Mi(vec![30, 40])),
            (2, PackedValue::I(99)),
        ]);

        let merged = merge_encoded_docs(&existing, &new_data).unwrap();
        let fields = decode_doc_fields(&merged).unwrap();

        // field 0: unchanged (I(42))
        assert_eq!(fields.iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(42));
        // field 1: Mi concatenated ([10, 20, 30, 40])
        assert_eq!(fields.iter().find(|(f, _)| *f == 1).unwrap().1, PackedValue::Mi(vec![10, 20, 30, 40]));
        // field 2: new field (I(99))
        assert_eq!(fields.iter().find(|(f, _)| *f == 2).unwrap().1, PackedValue::I(99));
    }

    #[test]
    fn test_merge_encoded_docs_non_mi_replaces() {
        let existing = encode_merge_fields(5, &[
            (0, PackedValue::I(100)),
            (1, PackedValue::S("hello".to_string())),
        ]);

        let new_data = encode_merge_fields(5, &[
            (0, PackedValue::I(200)),
        ]);

        let merged = merge_encoded_docs(&existing, &new_data).unwrap();
        let fields = decode_doc_fields(&merged).unwrap();

        // field 0: replaced (I(200))
        assert_eq!(fields.iter().find(|(f, _)| *f == 0).unwrap().1, PackedValue::I(200));
        // field 1: unchanged (S("hello"))
        assert_eq!(fields.iter().find(|(f, _)| *f == 1).unwrap().1, PackedValue::S("hello".to_string()));
    }
}
