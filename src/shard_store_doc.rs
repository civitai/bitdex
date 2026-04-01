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
// DocStoreV3 — high-level wrapper over DocShardStore
// ---------------------------------------------------------------------------

use crate::config::DataSchema;
use crate::docstore::StoredDoc;
use crate::mutation::FieldValue;

/// High-level document store backed by ShardStore.
///
/// Drop-in replacement for DocStore V2 that provides CRC32 integrity,
/// generation pinning, and native ShardStore compaction. Maintains the
/// same field dictionary and StoredDoc interface.
pub struct DocStoreV3 {
    store: DocShardStore,
    root: PathBuf,
    field_to_idx: HashMap<String, u16>,
    idx_to_field: Vec<String>,
    in_memory: bool,
    /// In-memory backing store for testing (when in_memory=true).
    memory_shards: HashMap<u32, DocSnapshot>,
    /// Per-field default values keyed by field dict index.
    field_defaults: HashMap<u16, PackedValue>,
    /// Current schema version.
    schema_version: u8,
    /// Historical defaults keyed by schema version.
    historical_defaults: HashMap<u8, HashMap<u16, PackedValue>>,
    /// Compaction threshold: number of ops before auto-compaction.
    compact_threshold: u32,
}

impl DocStoreV3 {
    /// Open a DocStoreV3 at the given directory.
    pub fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path.join("meta"))?;

        let store = DocShardStore::new(path.to_path_buf(), SlotHexShard)?;
        let (field_to_idx, idx_to_field) = Self::load_field_dict(path)?;
        let historical_defaults = Self::load_schema_history(path, &field_to_idx);

        Ok(Self {
            store,
            root: path.to_path_buf(),
            field_to_idx,
            idx_to_field,
            in_memory: false,
            memory_shards: HashMap::new(),
            field_defaults: HashMap::new(),
            schema_version: 1,
            historical_defaults,
            compact_threshold: 1000,
        })
    }

    /// Open an in-memory DocStoreV3 (for testing).
    /// Creates a temp directory under std::env::temp_dir() for ShardStore files.
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
            store,
            root: tmp_dir,
            field_to_idx: HashMap::new(),
            idx_to_field: Vec::new(),
            in_memory: true,
            memory_shards: HashMap::new(),
            field_defaults: HashMap::new(),
            schema_version: 1,
            historical_defaults: HashMap::new(),
            compact_threshold: 1000,
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

    fn ensure_field_idx(&mut self, name: &str) -> u16 {
        if let Some(&idx) = self.field_to_idx.get(name) {
            return idx;
        }
        let idx = self.idx_to_field.len() as u16;
        self.idx_to_field.push(name.to_string());
        self.field_to_idx.insert(name.to_string(), idx);
        idx
    }

    /// Get the field index for a name.
    pub fn field_index(&self, name: &str) -> Option<u16> {
        self.field_to_idx.get(name).copied()
    }

    /// Get or create a field index. Saves the dict if a new field was added.
    pub fn ensure_field_index(&mut self, name: &str) -> io::Result<u16> {
        let existed = self.field_to_idx.contains_key(name);
        let idx = self.ensure_field_idx(name);
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
                self.ensure_field_idx(name);
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
            // Auto-compact if threshold exceeded
            if let Ok(Some(count)) = self.store.ops_count(&shard_key) {
                if count >= self.compact_threshold {
                    let _ = self.store.compact_current(&shard_key);
                }
            }
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
        })
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
            self.ensure_field_idx(name);
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
            schema_version: self.schema_version,
            shard_buffers: Arc::new(DashMap::new()),
        })
    }

    /// Get a reference to the underlying ShardStore.
    pub fn shard_store(&self) -> &DocShardStore {
        &self.store
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
        PackedValue::Mm(v) => FieldValue::Multi(v.iter().map(|pv| match pv {
            PackedValue::I(i) => Value::Integer(*i),
            PackedValue::F(f) => Value::Float(*f),
            PackedValue::B(b) => Value::Bool(*b),
            PackedValue::S(s) => Value::String(s.clone()),
            _ => Value::Integer(0), // nested multi not supported in FieldValue
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

use dashmap::DashMap;
use std::sync::Arc;

/// Lock-free bulk writer for DocStoreV3.
///
/// Buffers (slot, field_idx, value) tuples in memory, grouped by shard.
/// On flush, writes complete ShardStore snapshots — one per shard.
/// Thread-safe: multiple rayon threads can call append_tuple_raw concurrently.
pub struct ShardStoreBulkWriter {
    field_to_idx: HashMap<String, u16>,
    root: PathBuf,
    field_defaults: HashMap<u16, PackedValue>,
    schema_version: u8,
    /// Buffered tuples grouped by shard. Each shard holds a map of slot → fields.
    /// DashMap for concurrent access from rayon threads.
    shard_buffers: Arc<DashMap<u32, parking_lot::Mutex<HashMap<u32, Vec<(u16, PackedValue)>>>>>,
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
        let entry = self.shard_buffers.entry(shard_key)
            .or_insert_with(|| parking_lot::Mutex::new(HashMap::new()));
        let mut shard = entry.lock();
        shard.entry(slot).or_default().push((field_idx, pv));
    }

    /// Append multiple tuples for the same slot in one call.
    /// The write_buf parameter is accepted for API compatibility but unused.
    pub fn append_tuples_raw(&self, slot: u32, tuples: &[(u16, &[u8])], _write_buf: &mut Vec<u8>) {
        if tuples.is_empty() {
            return;
        }
        let shard_key = SlotHexShard::slot_to_shard(slot);
        let entry = self.shard_buffers.entry(shard_key)
            .or_insert_with(|| parking_lot::Mutex::new(HashMap::new()));
        let mut shard = entry.lock();
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
    /// Each shard buffer is written as a complete DocSnapshot.
    pub fn flush_to_shardstore(&self) -> io::Result<()> {
        let store = DocShardStore::new(self.root.clone(), SlotHexShard)?;

        // Collect all shard keys
        let keys: Vec<u32> = self.shard_buffers.iter().map(|e| *e.key()).collect();

        for shard_key in keys {
            if let Some(entry) = self.shard_buffers.get(&shard_key) {
                let shard = entry.lock();
                if shard.is_empty() {
                    continue;
                }
                let mut snapshot = DocSnapshot::new();
                for (&slot, fields) in shard.iter() {
                    snapshot.docs.insert(slot, fields.clone());
                }
                store.write_snapshot(&shard_key, &snapshot)?;
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
            let entry = self.shard_buffers.entry(shard_key)
                .or_insert_with(|| parking_lot::Mutex::new(HashMap::new()));
            entry.lock().insert(slot, pairs);
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
            if let Some(pv) = crate::docstore::json_to_packed_with_dict(raw, mapping, apply_ms, dict) {
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
}
