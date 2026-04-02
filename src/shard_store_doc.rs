//! Document storage engine — types, codecs, and ShardStore-backed persistence.
//!
//! This module is the single source of truth for document storage:
//! - `StoredDoc` — the named-field document type used across the codebase
//! - `PackedValue` — compact enum for field values (integer, float, bool, string, multi)
//! - `DocStoreV3` — high-level document store backed by ShardStore
//! - `ShardStoreBulkWriter` — high-throughput parallel writer for dump processor
//! - `DocSnapshotCodec` / `DocOpCodec` — ShardStore codecs
//! - `SlotHexShard` — hex-bucketed shard file layout
//! - `json_to_packed_with_dict` — JSON → PackedValue conversion with dictionary support

use std::collections::HashMap;
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
// DocStoreV3 — high-level wrapper over DocShardStore
// ---------------------------------------------------------------------------

use crate::config::DataSchema;

/// High-level document store backed by ShardStore.
///
/// Drop-in replacement for DocStore V2 that provides CRC32 integrity,
/// generation pinning, and native ShardStore compaction. Maintains the
/// same field dictionary and StoredDoc interface.
pub struct DocStoreV3 {
    store: Arc<DocShardStore>,
    root: PathBuf,
    field_to_idx: HashMap<String, u16>,
    idx_to_field: Vec<String>,
    /// Per-field default values keyed by field dict index.
    field_defaults: HashMap<u16, PackedValue>,
    /// Current schema version.
    schema_version: u8,
    /// Historical defaults keyed by schema version.
    historical_defaults: HashMap<u8, HashMap<u16, PackedValue>>,
    /// Compaction threshold: number of ops before auto-compaction.
    compact_threshold: u32,
    /// Shard IDs that received writes since last drain.
    /// Used by merge thread for targeted compaction (avoids scanning all 209K shards).
    dirty_shards: Arc<DashSet<u32>>,
}

impl DocStoreV3 {
    /// Open a DocStoreV3 at the given directory.
    pub fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path.join("meta"))?;

        let store = DocShardStore::new(path.to_path_buf(), SlotHexShard)?;
        let (field_to_idx, idx_to_field) = Self::load_field_dict(path)?;
        let historical_defaults = Self::load_schema_history(path, &field_to_idx);

        let (schema_version, field_defaults) = if let Some((&max_ver, defaults)) =
            historical_defaults.iter().max_by_key(|(&v, _)| v)
        {
            (max_ver, defaults.clone())
        } else {
            (1, HashMap::new())
        };

        Ok(Self {
            store: Arc::new(store),
            root: path.to_path_buf(),
            field_to_idx,
            idx_to_field,
            field_defaults,
            schema_version,
            historical_defaults,
            compact_threshold: 1000,
            dirty_shards: Arc::new(DashSet::new()),
        })
    }

    /// Open an in-memory DocStoreV3 (for testing).
    pub fn open_temp() -> io::Result<Self> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmp_dir = std::env::temp_dir()
            .join(format!("bitdex-docstore-v3-{}-{}", std::process::id(), ts));
        std::fs::create_dir_all(tmp_dir.join("meta"))?;
        let store = DocShardStore::new(tmp_dir.clone(), SlotHexShard)?;
        Ok(Self {
            store: Arc::new(store),
            root: tmp_dir,
            field_to_idx: HashMap::new(),
            idx_to_field: Vec::new(),
            field_defaults: HashMap::new(),
            schema_version: 1,
            historical_defaults: HashMap::new(),
            compact_threshold: 1000,
            dirty_shards: Arc::new(DashSet::new()),
        })
    }

    /// Get the root path.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Get the root path (alias for path()).
    pub fn root(&self) -> &Path {
        &self.root
    }

    // ---- Field dictionary ----

    fn dict_path(root: &Path) -> PathBuf {
        root.join("meta").join("field_dict.bin")
    }

    fn load_field_dict(root: &Path) -> io::Result<(HashMap<String, u16>, Vec<String>)> {
        let path = Self::dict_path(root);
        match std::fs::read(&path) {
            Ok(data) => {
                let names: Vec<String> = rmp_serde::from_slice(&data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("field dict decode: {e}")))?;
                let map: HashMap<String, u16> = names
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (n.clone(), i as u16))
                    .collect();
                Ok((map, names))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok((HashMap::new(), Vec::new())),
            Err(e) => Err(e),
        }
    }

    fn save_field_dict(&self) -> io::Result<()> {
        let bytes = rmp_serde::to_vec(&self.idx_to_field)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("field dict encode: {e}")))?;
        let path = Self::dict_path(&self.root);
        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::OpenOptions::new().write(true).open(&tmp)?
            .sync_all()?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn ensure_field_idx(&mut self, name: &str) -> io::Result<u16> {
        if let Some(&idx) = self.field_to_idx.get(name) {
            return Ok(idx);
        }
        if self.idx_to_field.len() >= u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("field dictionary overflow: cannot add '{}' (already {} fields)", name, self.idx_to_field.len()),
            ));
        }
        let idx = self.idx_to_field.len() as u16;
        self.idx_to_field.push(name.to_string());
        self.field_to_idx.insert(name.to_string(), idx);
        Ok(idx)
    }

    /// Get the field index for a name.
    pub fn field_index(&self, name: &str) -> Option<u16> {
        self.field_to_idx.get(name).copied()
    }

    /// Get or create a field index. Saves the dict if a new field was added.
    pub fn ensure_field_index(&mut self, name: &str) -> io::Result<u16> {
        let existed = self.field_to_idx.contains_key(name);
        let idx = self.ensure_field_idx(name)?;
        if !existed {
            self.save_field_dict()?;
        }
        Ok(idx)
    }

    /// Snapshot the current field name → index mapping.
    pub fn field_dict_snapshot(&self) -> HashMap<String, u16> {
        self.field_to_idx.clone()
    }

    /// Get the field name → index mapping.
    pub fn field_to_idx(&self) -> &HashMap<String, u16> {
        &self.field_to_idx
    }

    /// Get the index → field name mapping.
    pub fn idx_to_field(&self) -> &[String] {
        &self.idx_to_field
    }

    // ---- Schema ----

    /// Build the field_defaults map from a DataSchema.
    pub fn set_field_defaults(&mut self, schema: &DataSchema) {
        self.schema_version = schema.schema_version;
        self.field_defaults.clear();
        for mapping in &schema.fields {
            if let Some(ref default_val) = mapping.default_value {
                if let Some(&idx) = self.field_to_idx.get(&mapping.target) {
                    if let Some(pv) = json_to_packed_default(default_val) {
                        self.field_defaults.insert(idx, pv);
                    }
                }
            }
        }
        self.historical_defaults
            .insert(self.schema_version, self.field_defaults.clone());
        self.save_schema_history();
    }

    /// Get the current schema version.
    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    /// Build a schema registry mapping version → (field_name → default_json_value).
    pub fn build_schema_registry(&self) -> HashMap<u8, HashMap<String, serde_json::Value>> {
        let mut registry = HashMap::new();
        let current_defaults = if !self.field_defaults.is_empty() {
            self.idx_defaults_to_named(&self.field_defaults)
        } else if let Some(hist) = self.historical_defaults.get(&self.schema_version) {
            self.idx_defaults_to_named(hist)
        } else {
            HashMap::new()
        };
        registry.insert(self.schema_version, current_defaults);
        for (&version, defaults) in &self.historical_defaults {
            if version != self.schema_version {
                registry.insert(version, self.idx_defaults_to_named(defaults));
            }
        }
        registry
    }

    fn idx_defaults_to_named(
        &self,
        defaults: &HashMap<u16, PackedValue>,
    ) -> HashMap<String, serde_json::Value> {
        defaults
            .iter()
            .filter_map(|(&idx, pv)| {
                self.idx_to_field
                    .get(idx as usize)
                    .map(|name| (name.clone(), packed_value_to_json(pv)))
            })
            .collect()
    }

    // ---- Schema history persistence ----

    fn schema_dir(root: &Path) -> PathBuf {
        root.join("meta").join("schema")
    }

    fn save_schema_history(&self) {
        let dir = Self::schema_dir(&self.root);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("DocStoreV3: failed to create schema dir: {e}");
            return;
        }
        let defaults_map: HashMap<String, Option<serde_json::Value>> = self
            .field_defaults
            .iter()
            .filter_map(|(&idx, pv)| {
                self.idx_to_field
                    .get(idx as usize)
                    .map(|name| (name.clone(), Some(packed_value_to_json(pv))))
            })
            .collect();
        let payload = serde_json::json!({
            "schema_version": self.schema_version,
            "field_defaults": defaults_map,
        });
        let path = dir.join(format!("v{}.json", self.schema_version));
        let tmp = path.with_extension("json.tmp");
        if let Ok(json) = serde_json::to_string_pretty(&payload) {
            if let Err(e) = std::fs::write(&tmp, &json) {
                eprintln!("DocStoreV3: failed to write schema v{}: {e}", self.schema_version);
                return;
            }
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    fn load_schema_history(root: &Path, field_to_idx: &HashMap<String, u16>) -> HashMap<u8, HashMap<u16, PackedValue>> {
        let dir = Self::schema_dir(root);
        let mut history = HashMap::new();
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => return history,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if !name.starts_with('v') || path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let version: u8 = match name[1..].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let data = match std::fs::read_to_string(&path) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let json: serde_json::Value = match serde_json::from_str(&data) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(defaults_obj) = json.get("field_defaults").and_then(|v| v.as_object()) else {
                continue;
            };
            let mut defaults = HashMap::new();
            for (field_name, val) in defaults_obj {
                if let Some(&idx) = field_to_idx.get(field_name) {
                    if let Some(pv) = json_to_packed_default(val) {
                        defaults.insert(idx, pv);
                    }
                }
            }
            history.insert(version, defaults);
        }
        history
    }

    // ---- Document read/write ----

    /// Get a stored document by slot ID.
    pub fn get(&self, id: u32) -> io::Result<Option<StoredDoc>> {
        let shard_key = SlotHexShard::slot_to_shard(id);

        let snap = match self.store.read(&shard_key)? {
            Some(s) => s,
            None => return Ok(None),
        };

        Ok(snap.docs.get(&id).map(|fields| self.fields_to_stored_doc(fields)))
    }

    /// Read all documents from a single shard, decoded.
    pub fn get_shard(&self, shard_id: u32) -> io::Result<Vec<(u32, StoredDoc)>> {
        let snap = match self.store.read(&shard_id)? {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        Ok(snap.docs.iter().map(|(&slot, fields)| {
            (slot, self.fields_to_stored_doc(fields))
        }).collect())
    }

    /// Read a shard and return raw (slot_id, packed_pairs) without full StoredDoc decode.
    pub fn get_shard_packed(&self, shard_id: u32) -> io::Result<Vec<(u32, Vec<(u16, PackedValue)>)>> {
        let snap = match self.store.read(&shard_id)? {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        Ok(snap.docs.into_iter().collect())
    }

    /// Store a single document.
    pub fn put(&mut self, id: u32, doc: &StoredDoc) -> io::Result<()> {
        self.put_batch(&[(id, doc.clone())])
    }

    /// Store multiple documents. Converts to ShardStore Create ops.
    pub fn put_batch(&mut self, docs: &[(u32, StoredDoc)]) -> io::Result<()> {
        if docs.is_empty() {
            return Ok(());
        }

        // Ensure field dictionary is up to date
        let mut dict_changed = false;
        for (_, doc) in docs {
            for name in doc.fields.keys() {
                let old_len = self.idx_to_field.len();
                self.ensure_field_idx(name)?;
                if self.idx_to_field.len() > old_len {
                    dict_changed = true;
                }
            }
        }
        if dict_changed {
            self.save_field_dict()?;
        }

        // Group by shard and emit Create ops
        let mut by_shard: HashMap<u32, Vec<DocOp>> = HashMap::new();
        for (id, doc) in docs {
            let shard_key = SlotHexShard::slot_to_shard(*id);
            let fields = self.stored_doc_to_fields(doc);
            by_shard.entry(shard_key).or_default().push(DocOp::Create {
                slot: *id,
                fields,
            });
        }

        for (shard_key, ops) in by_shard {
            self.store.append_ops(&shard_key, &ops)?;
            self.dirty_shards.insert(shard_key);
        }

        Ok(())
    }

    /// Append tuples for a single slot (used by DocWriter in ops_processor).
    pub fn append_tuples_batch(&mut self, tuples: Vec<(u32, u16, Vec<u8>)>) -> io::Result<()> {
        // Group tuples by shard
        let mut by_shard: HashMap<u32, Vec<DocOp>> = HashMap::new();
        for (slot, field_idx, value_bytes) in tuples {
            let shard_key = SlotHexShard::slot_to_shard(slot);
            // Decode PackedValue from msgpack bytes
            let pv: PackedValue = rmp_serde::from_slice(&value_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("decode packed: {e}")))?;
            by_shard.entry(shard_key).or_default().push(DocOp::Set {
                slot,
                field: field_idx,
                value: pv,
            });
        }

        for (shard_key, ops) in by_shard {
            self.store.append_ops(&shard_key, &ops)?;
            self.dirty_shards.insert(shard_key);
        }
        Ok(())
    }

    /// Append a single tuple (used by ingester).
    pub fn append_tuple(&mut self, slot: u32, field_idx: u16, value_bytes: &[u8]) -> io::Result<()> {
        let shard_key = SlotHexShard::slot_to_shard(slot);
        let pv: PackedValue = rmp_serde::from_slice(value_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("decode packed: {e}")))?;
        self.store.append_op(&shard_key, &DocOp::Set {
            slot,
            field: field_idx,
            value: pv,
        })?;
        self.maybe_auto_compact(shard_key);
        Ok(())
    }

    /// Check ops count and auto-compact if threshold exceeded.
    fn maybe_auto_compact(&self, shard_key: u32) {
        if self.compact_threshold == 0 {
            return;
        }
        if let Ok(Some(count)) = self.store.ops_count(&shard_key) {
            if count > self.compact_threshold {
                if let Err(e) = self.store.compact_current(&shard_key) {
                    eprintln!("DocStoreV3: auto-compaction failed for shard {shard_key}: {e}");
                }
            }
        }
    }

    /// Compact all shards. Returns true if any compaction was done.
    pub fn compact(&self) -> io::Result<bool> {
        let shards = self.store.list_current_shards()?;
        let mut did_compact = false;
        for key in shards {
            if self.store.should_compact(&key, self.compact_threshold)? {
                self.store.compact_current(&key)?;
                did_compact = true;
            }
        }
        Ok(did_compact)
    }

    /// Set compaction threshold (ops count before triggering compaction).
    pub fn set_compact_threshold(&mut self, threshold: u32) {
        self.compact_threshold = threshold;
    }

    /// Prepare a ShardStoreBulkWriter for parallel docstore writes during bulk loading.
    pub fn prepare_bulk_load(&mut self, field_names: &[String]) -> io::Result<ShardStoreBulkWriter> {
        let mut changed = false;
        for name in field_names {
            let old_len = self.idx_to_field.len();
            self.ensure_field_idx(name)?;
            if self.idx_to_field.len() > old_len {
                changed = true;
            }
        }
        if changed {
            self.save_field_dict()?;
        }
        Ok(ShardStoreBulkWriter {
            field_to_idx: self.field_to_idx.clone(),
            root: self.root.clone(),
            field_defaults: self.field_defaults.clone(),
            shard_buffers: Arc::new(DashMap::new()),
        })
    }

    /// Prepare a StreamingDocWriter for write-through docstore writes during bulk loading.
    /// Unlike prepare_bulk_load which buffers in memory, this writer streams ops to disk.
    pub fn prepare_streaming_writer(&mut self, field_names: &[String]) -> io::Result<StreamingDocWriter> {
        let mut changed = false;
        for name in field_names {
            let old_len = self.idx_to_field.len();
            self.ensure_field_idx(name)?;
            if self.idx_to_field.len() > old_len {
                changed = true;
            }
        }
        if changed {
            self.save_field_dict()?;
        }
        Ok(StreamingDocWriter::new(
            self.root.clone(),
            self.field_to_idx.clone(),
            self.field_defaults.clone(),
        ))
    }

    /// Get a reference to the underlying ShardStore.
    pub fn shard_store(&self) -> &DocShardStore {
        &self.store
    }

    /// Get an Arc clone of the underlying ShardStore for concurrent access.
    /// Used by compact endpoint and merge thread to bypass the DocStoreV3 Mutex.
    pub fn shard_store_arc(&self) -> Arc<DocShardStore> {
        Arc::clone(&self.store)
    }

    /// Atomically drain the set of shard IDs that received writes since last drain.
    /// Uses retain(false) for atomic collect+remove — avoids TOCTOU race where a
    /// concurrent writer inserts between our collect and remove.
    pub fn drain_dirty_shards(&self) -> Vec<u32> {
        let mut keys = Vec::new();
        self.dirty_shards.retain(|k| {
            keys.push(*k);
            false
        });
        keys
    }

    /// Get an Arc clone of the dirty shards set (for passing to merge thread).
    pub fn dirty_shards_arc(&self) -> Arc<DashSet<u32>> {
        Arc::clone(&self.dirty_shards)
    }

    /// Pin the current generation for crash-consistent snapshots.
    pub fn pin_generation(&self) -> io::Result<u64> {
        self.store.pin_generation()
    }

    /// List all shard keys on disk.
    pub fn list_shards(&self) -> io::Result<Vec<u32>> {
        self.store.list_current_shards()
    }

    /// Get the shard ID for a slot.
    pub fn shard_id(slot_id: u32) -> u32 {
        SlotHexShard::slot_to_shard(slot_id)
    }

    /// Get the shard file path for a shard ID (compatibility with code that computes paths).
    pub fn shard_path(root: &Path, shard_id: u32) -> PathBuf {
        // Matches SlotHexShard layout in gen_000
        let dir_byte = ((shard_id >> 8) & 0xFF) as u8;
        root.join("gen_000")
            .join("shards")
            .join(format!("{:02x}", dir_byte))
            .join(format!("{:06}.shard", shard_id))
    }

    // ---- Conversion helpers ----

    fn fields_to_stored_doc(&self, fields: &[(u16, PackedValue)]) -> StoredDoc {
        let mut map = HashMap::with_capacity(fields.len());
        for (idx, pv) in fields {
            if let Some(name) = self.idx_to_field.get(*idx as usize) {
                map.insert(name.clone(), packed_to_field_value(pv));
            }
        }
        // Apply defaults for missing fields
        for (&idx, default_pv) in &self.field_defaults {
            if let Some(name) = self.idx_to_field.get(idx as usize) {
                if !map.contains_key(name) {
                    map.insert(name.clone(), packed_to_field_value(default_pv));
                }
            }
        }
        StoredDoc {
            fields: map,
            schema_version: self.schema_version,
        }
    }

    fn stored_doc_to_fields(&self, doc: &StoredDoc) -> Vec<(u16, PackedValue)> {
        let mut pairs = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            if let Some(&idx) = self.field_to_idx.get(name.as_str()) {
                let pv = field_value_to_packed(fv);
                // Elide fields matching their schema default
                if let Some(default_pv) = self.field_defaults.get(&idx) {
                    if &pv == default_pv {
                        continue;
                    }
                }
                pairs.push((idx, pv));
            }
        }
        pairs
    }
}

/// Convert a PackedValue to a FieldValue.
fn packed_to_field_value(pv: &PackedValue) -> FieldValue {
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
            // Nested multi-values (Mi/Mm inside Mm) cannot be represented in FieldValue.
            // Skip rather than silently corrupt to Integer(0).
            other => {
                eprintln!("packed_to_field_value: skipping nested multi-value {:?}", std::mem::discriminant(other));
                None
            }
        }).collect()),
    }
}

/// Convert a FieldValue to a PackedValue.
fn field_value_to_packed(fv: &FieldValue) -> PackedValue {
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
// ShardStoreBulkWriter — high-throughput parallel writes for dump processor
// ---------------------------------------------------------------------------

/// Lock-free bulk writer for DocStoreV3.
///
/// Buffers (slot, field_idx, value) tuples in memory, grouped by shard.
/// On flush, writes complete ShardStore snapshots — one per shard.
/// Thread-safe: multiple rayon threads can call append_tuple_raw concurrently.
pub struct ShardStoreBulkWriter {
    field_to_idx: HashMap<String, u16>,
    root: PathBuf,
    field_defaults: HashMap<u16, PackedValue>,
    /// Buffered tuples grouped by shard. Each shard holds a map of slot → fields.
    /// DashMap for concurrent access from rayon threads.
    /// Values are Arc<Mutex<...>> so we can clone them out and drop the DashMap lock
    /// before acquiring the inner Mutex (avoids holding DashMap shard lock during I/O).
    shard_buffers: Arc<DashMap<u32, Arc<parking_lot::Mutex<HashMap<u32, Vec<(u16, PackedValue)>>>>>>,
}

impl ShardStoreBulkWriter {
    /// Get the field name → index mapping.
    pub fn field_to_idx(&self) -> &HashMap<String, u16> {
        &self.field_to_idx
    }

    /// Append a single raw tuple. Thread-safe via DashMap + per-shard Mutex.
    pub fn append_tuple_raw(&self, slot: u32, field_idx: u16, value_bytes: &[u8]) {
        let pv: PackedValue = match rmp_serde::from_slice(value_bytes) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("ShardStoreBulkWriter: decode packed value: {e}");
                return;
            }
        };
        // Elide fields matching their schema default
        if let Some(default_pv) = self.field_defaults.get(&field_idx) {
            if &pv == default_pv {
                return;
            }
        }
        let shard_key = SlotHexShard::slot_to_shard(slot);
        // Clone Arc out of DashMap to drop the map shard lock before acquiring inner Mutex
        let mutex = self.shard_buffers.entry(shard_key)
            .or_insert_with(|| Arc::new(parking_lot::Mutex::new(HashMap::new())))
            .clone();
        let mut shard = mutex.lock();
        shard.entry(slot).or_default().push((field_idx, pv));
    }

    /// Append multiple tuples for the same slot in one call.
    /// The write_buf parameter is accepted for API compatibility but unused.
    pub fn append_tuples_raw(&self, slot: u32, tuples: &[(u16, &[u8])], _write_buf: &mut Vec<u8>) {
        if tuples.is_empty() {
            return;
        }
        let shard_key = SlotHexShard::slot_to_shard(slot);
        let mutex = self.shard_buffers.entry(shard_key)
            .or_insert_with(|| Arc::new(parking_lot::Mutex::new(HashMap::new())))
            .clone();
        let mut shard = mutex.lock();
        let fields = shard.entry(slot).or_default();
        for &(field_idx, value_bytes) in tuples {
            let pv: PackedValue = match rmp_serde::from_slice(value_bytes) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("ShardStoreBulkWriter: decode tuple: {e}");
                    continue;
                }
            };
            if let Some(default_pv) = self.field_defaults.get(&field_idx) {
                if &pv == default_pv {
                    continue;
                }
            }
            fields.push((field_idx, pv));
        }
    }

    /// Flush all buffered data as ShardStore snapshots.
    /// Merges buffered docs into existing shard data (read-merge-write).
    pub fn flush_to_shardstore(&self) -> io::Result<()> {
        let store = DocShardStore::new(self.root.clone(), SlotHexShard)?;

        let keys: Vec<u32> = self.shard_buffers.iter().map(|e| *e.key()).collect();

        for shard_key in keys {
            if let Some(entry) = self.shard_buffers.get(&shard_key) {
                let mutex = entry.value().clone();
                drop(entry); // Drop DashMap ref before locking inner Mutex
                let mut shard = mutex.lock();
                if shard.is_empty() {
                    continue;
                }
                // Take ownership of buffered data for this flush attempt.
                let shard_data = std::mem::take(&mut *shard);
                drop(shard); // Release lock before disk I/O

                // Read existing shard state and merge new docs into it.
                // Per-slot merge: existing fields are preserved, buffered fields
                // override by field_idx (last-write-wins), duplicates deduplicated.
                let flush_result = (|| -> io::Result<()> {
                    // Read existing shard; if file is corrupted/pre-created stub, start fresh.
                    let mut snapshot = match store.read(&shard_key) {
                        Ok(Some(s)) => s,
                        Ok(None) => DocSnapshot::new(),
                        Err(_) => DocSnapshot::new(),
                    };
                    for (&slot, buffered_fields) in &shard_data {
                        let doc = snapshot.docs.entry(slot).or_default();
                        for (field_idx, value) in buffered_fields {
                            if let Some(existing) = doc.iter_mut().find(|(f, _)| *f == *field_idx) {
                                existing.1 = value.clone();
                            } else {
                                doc.push((*field_idx, value.clone()));
                            }
                        }
                    }
                    store.write_snapshot(&shard_key, &snapshot)
                })();

                if let Err(e) = flush_result {
                    // Restore buffered data on failure so it's not lost
                    let mut shard = mutex.lock();
                    for (slot, fields) in shard_data {
                        shard.entry(slot).or_default().extend(fields);
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Flush all open writers. For ShardStoreBulkWriter this writes ShardStore snapshots.
    /// Named for API compatibility with the V2 BulkWriter.
    pub fn flush_v2_writers(&self) {
        if let Err(e) = self.flush_to_shardstore() {
            eprintln!("ShardStoreBulkWriter: flush failed: {e}");
        }
    }

    /// Write pre-encoded docs to shard files (ShardStore snapshot format).
    pub fn write_batch_encoded(&self, encoded: Vec<(u32, Vec<u8>)>) {
        for (slot, bytes) in encoded {
            let pairs: Vec<(u16, PackedValue)> = match rmp_serde::from_slice(&bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let shard_key = SlotHexShard::slot_to_shard(slot);
            let mutex = self.shard_buffers.entry(shard_key)
                .or_insert_with(|| Arc::new(parking_lot::Mutex::new(HashMap::new())))
                .clone();
            mutex.lock().insert(slot, pairs);
        }
    }

    /// Encode a StoredDoc to msgpack bytes using the snapshotted field dictionary.
    pub fn encode_doc(&self, doc: &StoredDoc) -> Vec<u8> {
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            if let Some(&idx) = self.field_to_idx.get(name.as_str()) {
                let pv = field_value_to_packed(fv);
                if let Some(default_pv) = self.field_defaults.get(&idx) {
                    if &pv == default_pv {
                        continue;
                    }
                }
                pairs.push((idx, pv));
            }
        }
        rmp_serde::to_vec(&pairs).unwrap_or_default()
    }

    /// Encode a JSON value directly using the DataSchema.
    pub fn encode_json(&self, json: &serde_json::Value, schema: &DataSchema) -> Vec<u8> {
        self.encode_json_with_dicts(json, schema, None)
    }

    /// Encode a JSON document with optional dictionaries.
    pub fn encode_json_with_dicts(
        &self,
        json: &serde_json::Value,
        schema: &DataSchema,
        dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
    ) -> Vec<u8> {
        use crate::config::FieldValueType;
        let mut pairs: Vec<(u16, PackedValue)> =
            Vec::with_capacity(schema.fields.len() + 1);

        // ID field
        if let Some(id_val) = json.get(&schema.id_field) {
            if let Some(&idx) = self.field_to_idx.get("id") {
                if let Some(n) = id_val
                    .as_i64()
                    .or_else(|| id_val.as_u64().map(|u| u as i64))
                {
                    pairs.push((idx, PackedValue::I(n)));
                }
            }
        }

        // Schema fields
        for mapping in &schema.fields {
            let Some(&idx) = self.field_to_idx.get(&mapping.target) else {
                continue;
            };

            let (raw, apply_ms) = match mapping.resolve_raw(json) {
                Some(pair) => pair,
                None => {
                    if matches!(mapping.value_type, FieldValueType::ExistsBoolean) {
                        let pv = PackedValue::B(false);
                        if let Some(default_pv) = self.field_defaults.get(&idx) {
                            if &pv == default_pv {
                                continue;
                            }
                        }
                        pairs.push((idx, pv));
                    }
                    continue;
                }
            };

            let dict = dictionaries.and_then(|d| d.get(&mapping.target));
            if let Some(pv) = json_to_packed_with_dict(raw, mapping, apply_ms, dict) {
                if let Some(default_pv) = self.field_defaults.get(&idx) {
                    if &pv == default_pv {
                        continue;
                    }
                }
                pairs.push((idx, pv));
            }
        }

        rmp_serde::to_vec(&pairs).unwrap_or_default()
    }
}

/// Convert a serde_json::Value to a PackedValue for default comparison.
fn json_to_packed_default(val: &serde_json::Value) -> Option<PackedValue> {
    match val {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(PackedValue::B(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(PackedValue::I(i))
            } else if let Some(f) = n.as_f64() {
                Some(PackedValue::F(f))
            } else {
                None
            }
        }
        serde_json::Value::String(s) => Some(PackedValue::S(s.clone())),
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                Some(PackedValue::Mi(Vec::new()))
            } else if arr.iter().all(|v| v.is_i64() || v.is_u64()) {
                let ints: Vec<i64> = arr
                    .iter()
                    .filter_map(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)))
                    .collect();
                Some(PackedValue::Mi(ints))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Convert a PackedValue to a serde_json::Value.
fn packed_value_to_json(pv: &PackedValue) -> serde_json::Value {
    match pv {
        PackedValue::I(i) => serde_json::json!(i),
        PackedValue::F(f) => serde_json::json!(f),
        PackedValue::B(b) => serde_json::json!(b),
        PackedValue::S(s) => serde_json::json!(s),
        PackedValue::Mi(arr) => serde_json::json!(arr),
        PackedValue::Mm(arr) => {
            serde_json::Value::Array(arr.iter().map(packed_value_to_json).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// StreamingDocWriter — write-through docstore writer for dump processing
// ---------------------------------------------------------------------------

/// Per-shard state for streaming writes.
struct ShardFileWriter {
    writer: std::io::BufWriter<std::fs::File>,
    ops_count: u32,
}

/// Write-through docstore writer that streams ops directly to ShardStore shard files.
///
/// Unlike ShardStoreBulkWriter which buffers all docs in memory, this writer
/// opens one BufWriter<File> per shard and writes ops immediately. Memory
/// footprint is just BufWriter buffers (~8KB × num_open_shards ≈ 1.6MB for 213K shards).
///
/// Thread-safe: multiple rayon threads can call write_doc concurrently via DashMap
/// + per-shard Mutex.
///
/// Shard file format: standard ShardStore with empty snapshot + ops log.
/// After dump completes, compaction merges ops into snapshots for fast reads.
pub struct StreamingDocWriter {
    field_to_idx: HashMap<String, u16>,
    field_defaults: HashMap<u16, PackedValue>,
    root: PathBuf,
    shards: DashMap<u32, Arc<parking_lot::Mutex<ShardFileWriter>>>,
}

impl StreamingDocWriter {
    /// Create a new streaming writer. `root` is the docstore directory (e.g. indexes/civitai/docs).
    pub fn new(
        root: PathBuf,
        field_to_idx: HashMap<String, u16>,
        field_defaults: HashMap<u16, PackedValue>,
    ) -> Self {
        Self {
            field_to_idx,
            field_defaults,
            root,
            shards: DashMap::new(),
        }
    }

    /// Get the field name → index mapping.
    pub fn field_to_idx(&self) -> &HashMap<String, u16> {
        &self.field_to_idx
    }

    /// Write a doc's fields as a DocOp::Create op to the shard file.
    /// Thread-safe via DashMap + per-shard Mutex. The BufWriter handles
    /// OS-level write batching — no in-memory doc accumulation.
    pub fn write_doc(&self, slot: u32, fields: &[(u16, PackedValue)]) {
        // Skip if all fields are defaults
        let non_default: Vec<(u16, PackedValue)> = fields.iter()
            .filter(|(idx, val)| {
                self.field_defaults.get(idx).map_or(true, |d| d != val)
            })
            .cloned()
            .collect();

        if non_default.is_empty() {
            return;
        }

        let shard_key = SlotHexShard::slot_to_shard(slot);
        let mutex = self.shards.entry(shard_key)
            .or_insert_with(|| {
                Arc::new(parking_lot::Mutex::new(self.open_shard(shard_key)))
            })
            .clone();

        // Encode the op: DocOp::Create { slot, fields }
        let op = DocOp::Create { slot, fields: non_default };
        let mut payload = Vec::new();
        DocOpCodec::encode_op(&op, &mut payload);

        // Write op entry: [u32 len][payload][u32 crc32]
        let len = payload.len() as u32;
        let crc = crate::shard_store::crc32_of(&payload);

        let mut shard = mutex.lock();
        use std::io::Write;
        let _ = shard.writer.write_all(&len.to_le_bytes());
        let _ = shard.writer.write_all(&payload);
        let _ = shard.writer.write_all(&crc.to_le_bytes());
        shard.ops_count += 1;
    }

    /// Write a single field value as a DocOp::Set op.
    /// Used for multi-value phases (tags, resources) that append to existing docs.
    pub fn write_field(&self, slot: u32, field_idx: u16, value: &PackedValue) {
        if self.field_defaults.get(&field_idx).map_or(false, |d| d == value) {
            return;
        }

        let shard_key = SlotHexShard::slot_to_shard(slot);
        let mutex = self.shards.entry(shard_key)
            .or_insert_with(|| {
                Arc::new(parking_lot::Mutex::new(self.open_shard(shard_key)))
            })
            .clone();

        let op = DocOp::Set { slot, field: field_idx, value: value.clone() };
        let mut payload = Vec::new();
        DocOpCodec::encode_op(&op, &mut payload);

        let len = payload.len() as u32;
        let crc = crate::shard_store::crc32_of(&payload);

        let mut shard = mutex.lock();
        use std::io::Write;
        let _ = shard.writer.write_all(&len.to_le_bytes());
        let _ = shard.writer.write_all(&payload);
        let _ = shard.writer.write_all(&crc.to_le_bytes());
        shard.ops_count += 1;
    }

    /// Write raw msgpack-encoded tuples as a DocOp::Create.
    /// API-compatible with ShardStoreBulkWriter::append_tuples_raw.
    pub fn append_tuples_raw(&self, slot: u32, tuples: &[(u16, &[u8])], _write_buf: &mut Vec<u8>) {
        if tuples.is_empty() {
            return;
        }

        let mut fields = Vec::with_capacity(tuples.len());
        for &(field_idx, value_bytes) in tuples {
            let pv: PackedValue = match rmp_serde::from_slice(value_bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if self.field_defaults.get(&field_idx).map_or(false, |d| d == &pv) {
                continue;
            }
            fields.push((field_idx, pv));
        }

        if fields.is_empty() {
            return;
        }

        let shard_key = SlotHexShard::slot_to_shard(slot);
        let mutex = self.shards.entry(shard_key)
            .or_insert_with(|| {
                Arc::new(parking_lot::Mutex::new(self.open_shard(shard_key)))
            })
            .clone();

        let op = DocOp::Create { slot, fields };
        let mut payload = Vec::new();
        DocOpCodec::encode_op(&op, &mut payload);

        let len = payload.len() as u32;
        let crc = crate::shard_store::crc32_of(&payload);

        let mut shard = mutex.lock();
        use std::io::Write;
        let _ = shard.writer.write_all(&len.to_le_bytes());
        let _ = shard.writer.write_all(&payload);
        let _ = shard.writer.write_all(&crc.to_le_bytes());
        shard.ops_count += 1;
    }

    /// Write a single raw msgpack tuple. API-compatible with ShardStoreBulkWriter.
    pub fn append_tuple_raw(&self, slot: u32, field_idx: u16, value_bytes: &[u8]) {
        let pv: PackedValue = match rmp_serde::from_slice(value_bytes) {
            Ok(v) => v,
            Err(_) => return,
        };
        if self.field_defaults.get(&field_idx).map_or(false, |d| d == &pv) {
            return;
        }
        self.write_field(slot, field_idx, &pv);
    }

    /// Finalize all shard files: flush BufWriters, update ops_count in headers, sync.
    ///
    /// Safe to call multiple times (e.g., after each dump phase). After updating
    /// the header, seeks back to end-of-file so the BufWriter can continue
    /// appending ops in subsequent phases.
    pub fn finalize(&self) -> io::Result<()> {
        use std::io::{Seek, Write};

        let keys: Vec<u32> = self.shards.iter().map(|e| *e.key()).collect();
        let mut errors = 0u32;

        for shard_key in keys {
            if let Some(entry) = self.shards.get(&shard_key) {
                let mutex = entry.value().clone();
                drop(entry);
                let mut shard = mutex.lock();

                // Flush buffered writes
                if let Err(e) = shard.writer.flush() {
                    eprintln!("StreamingDocWriter: flush shard {shard_key}: {e}");
                    errors += 1;
                    continue;
                }

                // Update ops_count in header
                let ops_count = shard.ops_count;
                let file = shard.writer.get_mut();
                if let Err(e) = file.seek(std::io::SeekFrom::Start(
                    crate::shard_store::HEADER_OPS_COUNT_OFFSET,
                )) {
                    eprintln!("StreamingDocWriter: seek shard {shard_key}: {e}");
                    errors += 1;
                    continue;
                }
                if let Err(e) = file.write_all(&ops_count.to_le_bytes()) {
                    eprintln!("StreamingDocWriter: write ops_count shard {shard_key}: {e}");
                    errors += 1;
                    continue;
                }

                // Seek back to end of file so subsequent writes (e.g., multi-value
                // phases) append correctly instead of overwriting ops data.
                if let Err(e) = file.seek(std::io::SeekFrom::End(0)) {
                    eprintln!("StreamingDocWriter: seek-to-end shard {shard_key}: {e}");
                    errors += 1;
                    continue;
                }

                if let Err(e) = file.sync_all() {
                    eprintln!("StreamingDocWriter: sync shard {shard_key}: {e}");
                    errors += 1;
                }
            }
        }

        if errors > 0 {
            eprintln!("StreamingDocWriter: finalize completed with {errors} errors");
        }
        Ok(())
    }

    /// No-op for API compatibility with ShardStoreBulkWriter.
    pub fn flush_v2_writers(&self) {
        // Streaming writer writes directly to disk — nothing to flush.
    }

    /// Open or create a shard file with a proper ShardStore header.
    fn open_shard(&self, shard_key: u32) -> ShardFileWriter {
        let path = DocStoreV3::shard_path(&self.root, shard_key);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        // Check if a valid shard file already exists (e.g., from a previous phase)
        let (file, existing_ops) = if path.exists() {
            match std::fs::metadata(&path) {
                Ok(meta) if meta.len() >= crate::shard_store::HEADER_SIZE as u64 => {
                    // Try to open and validate existing file
                    match std::fs::OpenOptions::new().read(true).write(true).open(&path) {
                        Ok(mut f) => {
                            use std::io::Read;
                            let mut header_buf = [0u8; crate::shard_store::HEADER_SIZE];
                            if f.read_exact(&mut header_buf).is_ok() {
                                if let Ok(header) = crate::shard_store::ShardHeader::decode(&header_buf) {
                                    // Valid shard — seek to end, append new ops
                                    use std::io::Seek;
                                    let _ = f.seek(std::io::SeekFrom::End(0));
                                    return ShardFileWriter {
                                        writer: std::io::BufWriter::with_capacity(256, f),
                                        ops_count: header.ops_count,
                                    };
                                }
                            }
                            // Invalid header — will overwrite below
                            drop(f);
                            (None::<std::fs::File>, 0u32)
                        }
                        Err(_) => (None, 0),
                    }
                }
                _ => (None, 0), // File too small or can't stat — overwrite
            }
        } else {
            (None, 0)
        };

        // Create new shard file with empty snapshot
        let header = crate::shard_store::ShardHeader {
            version: crate::shard_store::SHARD_VERSION,
            ops_section_offset: crate::shard_store::HEADER_SIZE as u64,
            snapshot_len: 0,
            ops_count: 0, // Updated in finalize()
            flags: 0,
        };
        let mut header_bytes = Vec::with_capacity(crate::shard_store::HEADER_SIZE);
        header.encode(&mut header_bytes);

        let f = std::fs::File::create(&path).expect("failed to create shard file");
        // Small buffer: 213K shards × 256 bytes = 54MB total, vs 1.7GB with default 8KB
        let mut writer = std::io::BufWriter::with_capacity(256, f);
        use std::io::Write;
        writer.write_all(&header_bytes).expect("failed to write shard header");

        ShardFileWriter {
            writer,
            ops_count: 0,
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
    fn test_streaming_writer_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut ds = DocStoreV3::open(&docs_dir).unwrap();

        let field_names = vec!["userId".to_string(), "nsfwLevel".to_string()];
        let writer = ds.prepare_streaming_writer(&field_names).unwrap();
        let fidx = writer.field_to_idx().clone();

        // Write a doc via streaming writer
        writer.write_doc(1000, &[
            (fidx["userId"], PackedValue::I(42)),
            (fidx["nsfwLevel"], PackedValue::I(3)),
        ]);
        writer.finalize().unwrap();

        // Read it back via DocStoreV3
        let doc = ds.get(1000).unwrap();
        assert!(doc.is_some(), "streaming writer doc should be readable");
        let doc = doc.unwrap();
        assert_eq!(doc.fields.len(), 2, "doc should have 2 fields, got {:?}", doc.fields);
    }

    #[test]
    fn test_streaming_writer_roundtrip_after_reopen() {
        // Simulates a server restart: write via streaming writer, drop DocStoreV3,
        // re-open, and verify docs are readable.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");

        // Phase 1: Write
        {
            let mut ds = DocStoreV3::open(&docs_dir).unwrap();
            let field_names = vec!["userId".to_string(), "nsfwLevel".to_string(), "sortAt".to_string()];
            let writer = ds.prepare_streaming_writer(&field_names).unwrap();
            let fidx = writer.field_to_idx().clone();

            writer.write_doc(1000, &[
                (fidx["userId"], PackedValue::I(42)),
                (fidx["nsfwLevel"], PackedValue::I(3)),
                (fidx["sortAt"], PackedValue::I(1700000000)),
            ]);
            writer.write_doc(2000, &[
                (fidx["userId"], PackedValue::I(99)),
                (fidx["nsfwLevel"], PackedValue::I(1)),
                (fidx["sortAt"], PackedValue::I(1700000001)),
            ]);
            writer.finalize().unwrap();
        }
        // DocStoreV3 dropped here

        // Phase 2: Re-open (simulates server restart) and read
        let ds2 = DocStoreV3::open(&docs_dir).unwrap();

        let doc1 = ds2.get(1000).unwrap();
        assert!(doc1.is_some(), "doc 1000 should exist after reopen");
        let doc1 = doc1.unwrap();
        eprintln!("doc1 fields: {:?}", doc1.fields);
        assert_eq!(doc1.fields.len(), 3, "doc1 should have 3 fields, got {:?}", doc1.fields);
        assert_eq!(
            doc1.fields.get("userId"),
            Some(&FieldValue::Single(crate::query::Value::Integer(42))),
        );

        let doc2 = ds2.get(2000).unwrap();
        assert!(doc2.is_some(), "doc 2000 should exist after reopen");
        let doc2 = doc2.unwrap();
        assert_eq!(doc2.fields.len(), 3);
        assert_eq!(
            doc2.fields.get("userId"),
            Some(&FieldValue::Single(crate::query::Value::Integer(99))),
        );
    }

    #[test]
    fn test_streaming_writer_append_tuples_raw_reopen() {
        // Simulates PRODUCTION path: append_tuples_raw (msgpack-encoded) with defaults,
        // then reopen and verify. This is exactly what the dump processor does.
        use crate::config::DataSchema;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");

        // Phase 1: Write via append_tuples_raw (production dump path)
        {
            let mut ds = DocStoreV3::open(&docs_dir).unwrap();

            // Set field defaults like production (reactionCount=0, hasMeta=false)
            let schema: DataSchema = serde_json::from_value(serde_json::json!({
                "id_field": "id",
                "schema_version": 1,
                "fields": [
                    { "source": "userId", "target": "userId", "value_type": "integer" },
                    { "source": "nsfwLevel", "target": "nsfwLevel", "value_type": "integer" },
                    { "source": "reactionCount", "target": "reactionCount", "value_type": "integer", "default": 0 },
                    { "source": "hasMeta", "target": "hasMeta", "value_type": "boolean", "default": false },
                    { "source": "sortAt", "target": "sortAt", "value_type": "integer" },
                ]
            })).unwrap();

            let field_names: Vec<String> = schema.fields.iter().map(|f| f.target.clone()).collect();
            let writer = ds.prepare_streaming_writer(&field_names).unwrap();

            // Set defaults AFTER preparing writer (matches production: set_docstore_defaults
            // is called after engine creation, and prepare_streaming_writer inherits defaults)
            ds.set_field_defaults(&schema);

            // Re-create writer with updated defaults
            let writer = ds.prepare_streaming_writer(&field_names).unwrap();
            let fidx = writer.field_to_idx().clone();

            // Write via append_tuples_raw (msgpack encoded, like dump processor)
            let mut write_buf = Vec::new();
            let tuples: Vec<(u16, Vec<u8>)> = vec![
                (fidx["userId"], rmp_serde::to_vec(&PackedValue::I(42)).unwrap()),
                (fidx["nsfwLevel"], rmp_serde::to_vec(&PackedValue::I(3)).unwrap()),
                (fidx["reactionCount"], rmp_serde::to_vec(&PackedValue::I(100)).unwrap()),
                (fidx["hasMeta"], rmp_serde::to_vec(&PackedValue::B(true)).unwrap()),
                (fidx["sortAt"], rmp_serde::to_vec(&PackedValue::I(1700000000)).unwrap()),
            ];
            let refs: Vec<(u16, &[u8])> = tuples.iter().map(|(idx, v)| (*idx, v.as_slice())).collect();
            writer.append_tuples_raw(1000000, &refs, &mut write_buf);

            // Also test with a doc where some fields match defaults (should be elided)
            let tuples2: Vec<(u16, Vec<u8>)> = vec![
                (fidx["userId"], rmp_serde::to_vec(&PackedValue::I(99)).unwrap()),
                (fidx["nsfwLevel"], rmp_serde::to_vec(&PackedValue::I(1)).unwrap()),
                (fidx["reactionCount"], rmp_serde::to_vec(&PackedValue::I(0)).unwrap()), // matches default
                (fidx["hasMeta"], rmp_serde::to_vec(&PackedValue::B(false)).unwrap()), // matches default
                (fidx["sortAt"], rmp_serde::to_vec(&PackedValue::I(1700000001)).unwrap()),
            ];
            let refs2: Vec<(u16, &[u8])> = tuples2.iter().map(|(idx, v)| (*idx, v.as_slice())).collect();
            writer.append_tuples_raw(2000000, &refs2, &mut write_buf);

            writer.finalize().unwrap();
        }
        // Everything dropped — simulates server restart

        // Phase 2: Reopen with schema defaults (simulates restore_index → set_docstore_defaults)
        {
            let mut ds2 = DocStoreV3::open(&docs_dir).unwrap();

            // Re-apply defaults like the server does on boot
            let schema: DataSchema = serde_json::from_value(serde_json::json!({
                "id_field": "id",
                "schema_version": 1,
                "fields": [
                    { "source": "userId", "target": "userId", "value_type": "integer" },
                    { "source": "nsfwLevel", "target": "nsfwLevel", "value_type": "integer" },
                    { "source": "reactionCount", "target": "reactionCount", "value_type": "integer", "default": 0 },
                    { "source": "hasMeta", "target": "hasMeta", "value_type": "boolean", "default": false },
                    { "source": "sortAt", "target": "sortAt", "value_type": "integer" },
                ]
            })).unwrap();
            ds2.set_field_defaults(&schema);

            // Read doc 1000000 (all non-default values)
            let doc1 = ds2.get(1000000).unwrap();
            assert!(doc1.is_some(), "doc 1000000 should exist after reopen");
            let doc1 = doc1.unwrap();
            eprintln!("doc1 fields: {:?}", doc1.fields);
            assert_eq!(
                doc1.fields.get("userId"),
                Some(&FieldValue::Single(crate::query::Value::Integer(42))),
                "userId should be 42, got {:?}", doc1.fields.get("userId")
            );
            assert_eq!(
                doc1.fields.get("nsfwLevel"),
                Some(&FieldValue::Single(crate::query::Value::Integer(3))),
            );
            assert_eq!(
                doc1.fields.get("reactionCount"),
                Some(&FieldValue::Single(crate::query::Value::Integer(100))),
            );
            assert_eq!(
                doc1.fields.get("hasMeta"),
                Some(&FieldValue::Single(crate::query::Value::Bool(true))),
            );

            // Read doc 2000000 (reactionCount=0 and hasMeta=false were elided as defaults)
            let doc2 = ds2.get(2000000).unwrap();
            assert!(doc2.is_some(), "doc 2000000 should exist after reopen");
            let doc2 = doc2.unwrap();
            eprintln!("doc2 fields: {:?}", doc2.fields);
            // reactionCount was elided (matched default 0), should be reconstructed
            assert_eq!(
                doc2.fields.get("reactionCount"),
                Some(&FieldValue::Single(crate::query::Value::Integer(0))),
                "reactionCount should be 0 (default), got {:?}", doc2.fields.get("reactionCount")
            );
            // hasMeta was elided (matched default false), should be reconstructed
            assert_eq!(
                doc2.fields.get("hasMeta"),
                Some(&FieldValue::Single(crate::query::Value::Bool(false))),
            );
            // userId should NOT be default
            assert_eq!(
                doc2.fields.get("userId"),
                Some(&FieldValue::Single(crate::query::Value::Integer(99))),
            );
        }
    }

    #[test]
    fn test_streaming_writer_finalize_between_phases() {
        // Reproduces production bug: finalize() after images phase leaves file
        // position at offset 24 (inside header). Multi-value phase writes
        // through the same BufWriter, corrupting ops data at the wrong offset.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut ds = DocStoreV3::open(&docs_dir).unwrap();

        let field_names = vec![
            "userId".to_string(),
            "nsfwLevel".to_string(),
            "tagIds".to_string(),
        ];
        let writer = ds.prepare_streaming_writer(&field_names).unwrap();
        let fidx = writer.field_to_idx().clone();

        // Phase 1: Images — write docs
        writer.write_doc(42, &[
            (fidx["userId"], PackedValue::I(123)),
            (fidx["nsfwLevel"], PackedValue::I(5)),
        ]);
        writer.write_doc(100, &[
            (fidx["userId"], PackedValue::I(456)),
            (fidx["nsfwLevel"], PackedValue::I(2)),
        ]);
        // Finalize after images phase (this is what production does)
        writer.finalize().unwrap();

        // Phase 2: Tags — write multi-value fields to the SAME shards
        writer.write_field(42, fidx["tagIds"], &PackedValue::Mi(vec![1, 2, 3]));
        writer.write_field(100, fidx["tagIds"], &PackedValue::Mi(vec![4, 5]));
        // Finalize after tags phase
        writer.finalize().unwrap();

        // Verify: read back docs — both images AND tags fields should be present
        let doc1 = ds.get(42).unwrap();
        assert!(doc1.is_some(), "doc 42 should exist after multi-phase write");
        let doc1 = doc1.unwrap();
        eprintln!("doc1 fields: {:?}", doc1.fields);
        assert_eq!(
            doc1.fields.get("userId"),
            Some(&FieldValue::Single(crate::query::Value::Integer(123))),
            "userId should be 123, got {:?}", doc1.fields.get("userId")
        );
        assert!(doc1.fields.contains_key("tagIds"), "tagIds should be present");

        let doc2 = ds.get(100).unwrap();
        assert!(doc2.is_some(), "doc 100 should exist");
        let doc2 = doc2.unwrap();
        eprintln!("doc2 fields: {:?}", doc2.fields);
        assert_eq!(
            doc2.fields.get("userId"),
            Some(&FieldValue::Single(crate::query::Value::Integer(456))),
        );

        // Also verify after reopen (simulates server restart)
        drop(ds);
        let ds2 = DocStoreV3::open(&docs_dir).unwrap();
        let doc1_reopened = ds2.get(42).unwrap();
        assert!(doc1_reopened.is_some(), "doc 42 should exist after reopen");
        let doc1_reopened = doc1_reopened.unwrap();
        assert_eq!(
            doc1_reopened.fields.get("userId"),
            Some(&FieldValue::Single(crate::query::Value::Integer(123))),
            "userId should survive reopen, got {:?}", doc1_reopened.fields.get("userId")
        );
    }

    #[test]
    fn test_streaming_writer_shard_file_format_diagnostic() {
        // Diagnostic test: write via StreamingDocWriter, then raw-read the shard file
        // to verify the binary format matches what ShardStore expects.
        use std::io::Read;

        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut ds = DocStoreV3::open(&docs_dir).unwrap();

        let field_names = vec!["userId".to_string(), "nsfwLevel".to_string()];
        let writer = ds.prepare_streaming_writer(&field_names).unwrap();
        let fidx = writer.field_to_idx().clone();

        writer.write_doc(42, &[
            (fidx["userId"], PackedValue::I(123)),
            (fidx["nsfwLevel"], PackedValue::I(5)),
        ]);
        writer.finalize().unwrap();

        // Find the shard file
        let shard_key = SlotHexShard::slot_to_shard(42);
        let shard_path = DocStoreV3::shard_path(&docs_dir, shard_key);
        eprintln!("Shard path: {}", shard_path.display());
        assert!(shard_path.exists(), "shard file should exist at {:?}", shard_path);

        // Read raw bytes
        let data = std::fs::read(&shard_path).unwrap();
        eprintln!("Shard file size: {} bytes", data.len());
        assert!(data.len() >= crate::shard_store::HEADER_SIZE, "file too small");

        // Parse header
        let header = crate::shard_store::ShardHeader::decode(&data[..crate::shard_store::HEADER_SIZE]).unwrap();
        eprintln!("Header: version={}, ops_section_offset={}, snapshot_len={}, ops_count={}, flags={}",
            header.version, header.ops_section_offset, header.snapshot_len, header.ops_count, header.flags);

        assert_eq!(header.ops_count, 1, "should have 1 op (Create)");
        assert_eq!(header.snapshot_len, 0, "snapshot should be empty (ops-only)");
        assert_eq!(header.ops_section_offset, crate::shard_store::HEADER_SIZE as u64);

        // Read via ShardStore
        let store = DocShardStore::new(docs_dir.clone(), SlotHexShard).unwrap();
        let snap = store.read(&shard_key).unwrap();
        assert!(snap.is_some(), "ShardStore should find the shard");
        let snap = snap.unwrap();
        eprintln!("DocSnapshot has {} docs", snap.docs.len());
        eprintln!("DocSnapshot docs: {:?}", snap.docs);
        assert!(snap.docs.contains_key(&42), "snapshot should contain slot 42");
        let fields = &snap.docs[&42];
        assert_eq!(fields.len(), 2, "doc should have 2 fields");

        // Read via DocStoreV3 (the higher-level API)
        let ds2 = DocStoreV3::open(&docs_dir).unwrap();
        let doc = ds2.get(42).unwrap();
        assert!(doc.is_some(), "DocStoreV3::get should find doc");
        let doc = doc.unwrap();
        eprintln!("DocStoreV3::get(42) fields: {:?}", doc.fields);
        assert_eq!(doc.fields.len(), 2);
        assert_eq!(
            doc.fields.get("userId"),
            Some(&FieldValue::Single(crate::query::Value::Integer(123))),
        );
    }

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

// ---------------------------------------------------------------------------
// Proptest round-trip tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy for generating arbitrary PackedValue instances.
    fn arb_packed_value() -> impl Strategy<Value = PackedValue> {
        prop_oneof![
            any::<i64>().prop_map(PackedValue::I),
            any::<f64>().prop_map(PackedValue::F),
            any::<bool>().prop_map(PackedValue::B),
            "[a-zA-Z0-9]{0,50}".prop_map(PackedValue::S),
            proptest::collection::vec(any::<i64>(), 0..10).prop_map(PackedValue::Mi),
        ]
    }

    /// Strategy for generating arbitrary DocOp instances.
    fn arb_doc_op(max_slot: u32) -> impl Strategy<Value = DocOp> {
        prop_oneof![
            (0..max_slot, 0..16u16, arb_packed_value()).prop_map(|(slot, field, value)| {
                DocOp::Set { slot, field, value }
            }),
            (0..max_slot, 0..16u16, any::<i64>()).prop_map(|(slot, field, v)| {
                DocOp::Append { slot, field, value: PackedValue::I(v) }
            }),
            (0..max_slot).prop_map(|slot| DocOp::Delete { slot }),
            (0..max_slot, proptest::collection::vec(
                (0..16u16, arb_packed_value()), 1..5
            )).prop_map(|(slot, fields)| {
                DocOp::Create { slot, fields }
            }),
        ]
    }

    proptest! {
        #[test]
        fn packed_value_roundtrip(pv in arb_packed_value()) {
            let mut buf = Vec::new();
            encode_packed_value(&pv, &mut buf);
            let mut pos = 0;
            let decoded = decode_packed_value(&buf, &mut pos).unwrap();
            // For floats, NaN != NaN, so skip NaN comparison
            match (&pv, &decoded) {
                (PackedValue::F(a), PackedValue::F(b)) => {
                    if a.is_nan() {
                        prop_assert!(b.is_nan());
                    } else {
                        prop_assert_eq!(a, b);
                    }
                }
                _ => prop_assert_eq!(&pv, &decoded),
            }
        }

        #[test]
        fn doc_op_roundtrip(op in arb_doc_op(1000)) {
            let mut buf = Vec::new();
            DocOpCodec::encode_op(&op, &mut buf);
            let decoded = DocOpCodec::decode_op(&buf).unwrap();
            // Verify the op tag matches
            match (&op, &decoded) {
                (DocOp::Set { slot: s1, field: f1, .. }, DocOp::Set { slot: s2, field: f2, .. }) => {
                    prop_assert_eq!(s1, s2);
                    prop_assert_eq!(f1, f2);
                }
                (DocOp::Append { slot: s1, field: f1, .. }, DocOp::Append { slot: s2, field: f2, .. }) => {
                    prop_assert_eq!(s1, s2);
                    prop_assert_eq!(f1, f2);
                }
                (DocOp::Delete { slot: s1 }, DocOp::Delete { slot: s2 }) => {
                    prop_assert_eq!(s1, s2);
                }
                (DocOp::Create { slot: s1, fields: f1 }, DocOp::Create { slot: s2, fields: f2 }) => {
                    prop_assert_eq!(s1, s2);
                    prop_assert_eq!(f1.len(), f2.len());
                }
                _ => prop_assert!(false, "op type mismatch"),
            }
        }

        #[test]
        fn doc_snapshot_roundtrip(
            entries in proptest::collection::vec(
                (0..10000u32, proptest::collection::vec(
                    (0..16u16, arb_packed_value()), 0..5
                )),
                0..20
            )
        ) {
            let mut snap = DocSnapshot::new();
            for (slot, fields) in entries {
                snap.docs.insert(slot, fields);
            }

            let mut buf = Vec::new();
            DocSnapshotCodec::encode(&snap, &mut buf);
            let decoded = DocSnapshotCodec::decode(&buf).unwrap();

            prop_assert_eq!(snap.docs.len(), decoded.docs.len());
            for (slot, fields) in &snap.docs {
                prop_assert!(decoded.docs.contains_key(slot));
                prop_assert_eq!(fields.len(), decoded.docs[slot].len());
            }
        }

        /// Random ops applied then compacted = same state as applying ops to fresh snapshot.
        #[test]
        fn ops_compact_equals_fresh_build(
            ops in proptest::collection::vec(arb_doc_op(100), 1..20)
        ) {
            // Build state by applying ops to empty snapshot
            let mut expected = DocSnapshot::new();
            for op in &ops {
                DocOpCodec::apply(&mut expected, op);
            }

            // Build via ShardStore: append ops then compact
            let dir = tempfile::tempdir().unwrap();
            let store = DocShardStore::new(dir.path().to_path_buf(), SlotHexShard).unwrap();

            let shard_key = 0u32; // all ops go to shard 0
            for op in &ops {
                store.append_op(&shard_key, op).unwrap();
            }
            store.compact_current(&shard_key).unwrap();

            let compacted = store.read(&shard_key).unwrap().unwrap();

            // Compare: same slots, same field counts
            prop_assert_eq!(expected.docs.len(), compacted.docs.len());
            for (slot, expected_fields) in &expected.docs {
                prop_assert!(compacted.docs.contains_key(slot),
                    "missing slot {} after compaction", slot);
                prop_assert_eq!(
                    expected_fields.len(),
                    compacted.docs[slot].len(),
                    "field count mismatch for slot {}", slot
                );
            }
        }
    }

    #[test]
    fn test_dirty_shard_tracking() {
        let ds = DocStoreV3::open_temp().unwrap();

        // Initially no dirty shards
        assert!(ds.drain_dirty_shards().is_empty());

        // Insert marks shard dirty
        let shard_key = SlotHexShard::slot_to_shard(100);
        ds.store.append_op(&shard_key, &DocOp::Create {
            slot: 100,
            fields: vec![(0, PackedValue::I(42))],
        }).unwrap();
        ds.dirty_shards.insert(shard_key);

        // Drain returns the dirty shard
        let dirty = ds.drain_dirty_shards();
        assert_eq!(dirty.len(), 1);
        assert!(dirty.contains(&shard_key));

        // After drain, set is empty
        assert!(ds.drain_dirty_shards().is_empty());
    }

    #[test]
    fn test_shard_store_arc_accessible() {
        let ds = DocStoreV3::open_temp().unwrap();
        let arc = ds.shard_store_arc();

        // Write through the Arc
        arc.write_snapshot(&0u32, &DocSnapshot::new()).unwrap();

        // Read through the Arc
        let snap = arc.read(&0u32).unwrap();
        assert!(snap.is_some());
    }

    #[test]
    fn test_dirty_shards_arc_shared() {
        let ds = DocStoreV3::open_temp().unwrap();
        let dirty_arc = ds.dirty_shards_arc();

        // Insert via the Arc
        dirty_arc.insert(42);

        // Visible through drain
        let drained = ds.drain_dirty_shards();
        assert_eq!(drained, vec![42]);
    }
}
