//! DocSilo — mmap-backed document storage layered on top of `datasilo::DataSilo`.
//!
//! Replaces `DocStoreV3`'s hex-sharded filesystem layout with a single triple of
//! mmap'd files (`index.bin` + `data.bin` + `ops_a.log` / `ops_b.log`) governed
//! by a typed op codec. Field-level mutations (`Set` / `Append` / `Remove`) land
//! in the ops log as ~20-byte typed frames — no whole-doc re-encoding on the
//! hot path.
//!
//! Design matches ShardStore's snapshot + ops log pattern. The per-silo
//! snapshot is one document (its field list); compaction folds every typed
//! op for a slot into its snapshot via `apply`, then rewrites the entry
//! in place if it fits the allocated slack, otherwise relocates.
//!
//! Slot-to-silo-key mapping: `slot + 1` to avoid the HashIndex `key = 0`
//! sentinel (matches v3's convention).

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use datasilo::{DataSilo, OpCodec, SiloConfig, SnapshotCodec};

use crate::mutation::FieldValue;
use crate::query::Value;
use crate::shard_store_doc::{PackedValue, StoredDoc};

// ---------------------------------------------------------------------------
// Slot-key bijection
// ---------------------------------------------------------------------------

/// Key 0 is reserved by `HashIndex` as an empty-slot sentinel; offset every
/// slot by 1 when mapping into the silo.
pub const SLOT_KEY_OFFSET: u64 = 1;

#[inline]
pub fn slot_to_key(slot: u32) -> u64 {
    slot as u64 + SLOT_KEY_OFFSET
}

// ---------------------------------------------------------------------------
// SlotSnapshot — one document's field list (plus tombstone flag)
// ---------------------------------------------------------------------------

/// A single document's compacted state.
///
/// `alive = false` means the slot has been Deleted and the compaction pass
/// will drop it from `data.bin`. On read, an `!alive` snapshot surfaces as
/// `Ok(None)` from `DocSilo::get`.
#[derive(Debug, Clone, Default)]
pub struct SlotSnapshot {
    pub fields: Vec<(u16, PackedValue)>,
    pub alive: bool,
}

impl SlotSnapshot {
    pub fn empty() -> Self {
        Self {
            fields: Vec::new(),
            alive: false,
        }
    }
}

// ---------------------------------------------------------------------------
// DocOp — typed document operations (silo-native variant)
// ---------------------------------------------------------------------------
//
// Mirrors `shard_store_doc::DocOp` exactly; kept in this module so the port
// doesn't depend on the soon-to-be-deleted `shard_store_doc`. Encoding is
// wire-compatible with `DocOpCodec`.

#[derive(Debug, Clone)]
pub enum DocOp {
    Set { slot: u32, field: u16, value: PackedValue },
    Append { slot: u32, field: u16, value: PackedValue },
    Remove { slot: u32, field: u16, value: PackedValue },
    Delete { slot: u32 },
    Create { slot: u32, fields: Vec<(u16, PackedValue)> },
    Merge { slot: u32, fields: Vec<(u16, PackedValue)> },
}

impl DocOp {
    pub fn slot(&self) -> u32 {
        match self {
            DocOp::Set { slot, .. }
            | DocOp::Append { slot, .. }
            | DocOp::Remove { slot, .. }
            | DocOp::Delete { slot }
            | DocOp::Create { slot, .. }
            | DocOp::Merge { slot, .. } => *slot,
        }
    }
}

// Op wire tags.
const OP_TAG_SET: u8 = 0x01;
const OP_TAG_APPEND: u8 = 0x02;
const OP_TAG_REMOVE: u8 = 0x03;
const OP_TAG_DELETE: u8 = 0x04;
const OP_TAG_CREATE: u8 = 0x05;
const OP_TAG_MERGE: u8 = 0x06;

// PackedValue wire tags.
const PV_TAG_I: u8 = 0x01;
const PV_TAG_F: u8 = 0x02;
const PV_TAG_B: u8 = 0x03;
const PV_TAG_S: u8 = 0x04;
const PV_TAG_MI: u8 = 0x05;
const PV_TAG_MM: u8 = 0x06;

// ---------------------------------------------------------------------------
// PackedValue encode / decode (same wire format as shard_store_doc)
// ---------------------------------------------------------------------------

fn encode_packed_value(pv: &PackedValue, buf: &mut Vec<u8>) {
    match pv {
        PackedValue::I(n) => {
            buf.push(PV_TAG_I);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        PackedValue::F(n) => {
            buf.push(PV_TAG_F);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        PackedValue::B(b) => {
            buf.push(PV_TAG_B);
            buf.push(if *b { 1 } else { 0 });
        }
        PackedValue::S(s) => {
            buf.push(PV_TAG_S);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        PackedValue::Mi(vs) => {
            buf.push(PV_TAG_MI);
            buf.extend_from_slice(&(vs.len() as u32).to_le_bytes());
            for v in vs {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        PackedValue::Mm(vs) => {
            buf.push(PV_TAG_MM);
            buf.extend_from_slice(&(vs.len() as u32).to_le_bytes());
            for v in vs {
                encode_packed_value(v, buf);
            }
        }
    }
}

fn decode_packed_value(data: &[u8], pos: &mut usize) -> io::Result<PackedValue> {
    if *pos >= data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated packed value tag",
        ));
    }
    let tag = data[*pos];
    *pos += 1;
    match tag {
        PV_TAG_I => {
            let n = i64::from_le_bytes(
                data[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated i64"))?,
            );
            *pos += 8;
            Ok(PackedValue::I(n))
        }
        PV_TAG_F => {
            let n = f64::from_le_bytes(
                data[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated f64"))?,
            );
            *pos += 8;
            Ok(PackedValue::F(n))
        }
        PV_TAG_B => {
            let b = data[*pos] != 0;
            *pos += 1;
            Ok(PackedValue::B(b))
        }
        PV_TAG_S => {
            let len = u32::from_le_bytes(
                data[*pos..*pos + 4]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated strlen"))?,
            ) as usize;
            *pos += 4;
            let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
            *pos += len;
            Ok(PackedValue::S(s))
        }
        PV_TAG_MI => {
            let len = u32::from_le_bytes(
                data[*pos..*pos + 4]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated mi len"))?,
            ) as usize;
            *pos += 4;
            let mut vs = Vec::with_capacity(len);
            for _ in 0..len {
                let v = i64::from_le_bytes(
                    data[*pos..*pos + 8]
                        .try_into()
                        .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated mi elem"))?,
                );
                *pos += 8;
                vs.push(v);
            }
            Ok(PackedValue::Mi(vs))
        }
        PV_TAG_MM => {
            let len = u32::from_le_bytes(
                data[*pos..*pos + 4]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated mm len"))?,
            ) as usize;
            *pos += 4;
            let mut vs = Vec::with_capacity(len);
            for _ in 0..len {
                vs.push(decode_packed_value(data, pos)?);
            }
            Ok(PackedValue::Mm(vs))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown packed value tag: 0x{:02x}", other),
        )),
    }
}

#[inline]
fn encode_field_pair(field: u16, value: &PackedValue, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&field.to_le_bytes());
    encode_packed_value(value, buf);
}

fn decode_field_pair(data: &[u8], pos: &mut usize) -> io::Result<(u16, PackedValue)> {
    if *pos + 2 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated field idx",
        ));
    }
    let field = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    let value = decode_packed_value(data, pos)?;
    Ok((field, value))
}

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
// SlotSnapshotCodec
// ---------------------------------------------------------------------------
//
// Wire format (per slot):
//   [u8 alive_flag]
//   [u16 num_fields]
//   [field_pair...]   -- each: [u16 field_idx][packed_value]

pub struct SlotSnapshotCodec;

impl SnapshotCodec for SlotSnapshotCodec {
    type Snapshot = SlotSnapshot;

    fn encode(snapshot: &SlotSnapshot, buf: &mut Vec<u8>) {
        buf.push(if snapshot.alive { 1 } else { 0 });
        buf.extend_from_slice(&(snapshot.fields.len() as u16).to_le_bytes());
        for (field, value) in &snapshot.fields {
            encode_field_pair(*field, value, buf);
        }
    }

    fn decode(bytes: &[u8]) -> io::Result<SlotSnapshot> {
        if bytes.is_empty() {
            return Ok(SlotSnapshot::empty());
        }
        let mut pos = 0;
        let alive = bytes[pos] != 0;
        pos += 1;
        if pos + 2 > bytes.len() {
            return Ok(SlotSnapshot {
                fields: Vec::new(),
                alive,
            });
        }
        let num_fields = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let mut fields = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            fields.push(decode_field_pair(bytes, &mut pos)?);
        }
        Ok(SlotSnapshot { fields, alive })
    }

    fn empty() -> SlotSnapshot {
        SlotSnapshot::empty()
    }
}

// ---------------------------------------------------------------------------
// SlotDocOpCodec
// ---------------------------------------------------------------------------

pub struct SlotDocOpCodec;

impl OpCodec for SlotDocOpCodec {
    type Op = DocOp;
    type Snapshot = SlotSnapshot;

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
                let tag = if matches!(op, DocOp::Merge { .. }) {
                    OP_TAG_MERGE
                } else {
                    OP_TAG_CREATE
                };
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

        fn read_slot(data: &[u8], pos: &mut usize) -> io::Result<u32> {
            if *pos + 4 > data.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated slot"));
            }
            let s = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
            Ok(s)
        }

        match tag {
            OP_TAG_SET => {
                let slot = read_slot(bytes, &mut pos)?;
                let (field, value) = decode_field_pair(bytes, &mut pos)?;
                Ok(DocOp::Set { slot, field, value })
            }
            OP_TAG_APPEND => {
                let slot = read_slot(bytes, &mut pos)?;
                let (field, value) = decode_field_pair(bytes, &mut pos)?;
                Ok(DocOp::Append { slot, field, value })
            }
            OP_TAG_REMOVE => {
                let slot = read_slot(bytes, &mut pos)?;
                let (field, value) = decode_field_pair(bytes, &mut pos)?;
                Ok(DocOp::Remove { slot, field, value })
            }
            OP_TAG_DELETE => {
                let slot = read_slot(bytes, &mut pos)?;
                Ok(DocOp::Delete { slot })
            }
            OP_TAG_CREATE | OP_TAG_MERGE => {
                let slot = read_slot(bytes, &mut pos)?;
                if pos + 2 > bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated field count",
                    ));
                }
                let num_fields =
                    u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                let mut fields = Vec::with_capacity(num_fields);
                for _ in 0..num_fields {
                    fields.push(decode_field_pair(bytes, &mut pos)?);
                }
                Ok(if tag == OP_TAG_MERGE {
                    DocOp::Merge { slot, fields }
                } else {
                    DocOp::Create { slot, fields }
                })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown doc op tag: 0x{:02x}", other),
            )),
        }
    }

    fn op_key(op: &DocOp) -> u64 {
        slot_to_key(op.slot())
    }

    fn apply(snapshot: &mut SlotSnapshot, op: &DocOp) {
        match op {
            DocOp::Set { field, value, .. } => {
                snapshot.alive = true;
                if let Some(entry) = snapshot.fields.iter_mut().find(|(f, _)| *f == *field) {
                    entry.1 = value.clone();
                } else {
                    snapshot.fields.push((*field, value.clone()));
                }
            }
            DocOp::Append { field, value, .. } => {
                snapshot.alive = true;
                if let Some(entry) = snapshot.fields.iter_mut().find(|(f, _)| *f == *field) {
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
                            // Convert scalar to multi by wrapping.
                            let old = std::mem::replace(&mut entry.1, PackedValue::Mm(Vec::new()));
                            if let PackedValue::Mm(ref mut v) = entry.1 {
                                v.push(old);
                                v.push(value.clone());
                            }
                        }
                    }
                } else {
                    match value {
                        PackedValue::I(i) => snapshot
                            .fields
                            .push((*field, PackedValue::Mi(vec![*i]))),
                        _ => snapshot
                            .fields
                            .push((*field, PackedValue::Mm(vec![value.clone()]))),
                    }
                }
            }
            DocOp::Remove { field, value, .. } => {
                if let Some(entry) = snapshot.fields.iter_mut().find(|(f, _)| *f == *field) {
                    match &mut entry.1 {
                        PackedValue::Mi(v) => {
                            if let PackedValue::I(i) = value {
                                v.retain(|x| x != i);
                            }
                        }
                        PackedValue::Mm(v) => {
                            v.retain(|x| !packed_value_eq(x, value));
                        }
                        _ => {
                            // Can't remove from a scalar.
                        }
                    }
                }
            }
            DocOp::Delete { .. } => {
                snapshot.fields.clear();
                snapshot.alive = false;
            }
            DocOp::Create { fields, .. } => {
                snapshot.fields = fields.clone();
                snapshot.alive = true;
            }
            DocOp::Merge { fields, .. } => {
                snapshot.alive = true;
                for (field_idx, value) in fields {
                    if let Some(entry) =
                        snapshot.fields.iter_mut().find(|(f, _)| *f == *field_idx)
                    {
                        match (&mut entry.1, value) {
                            // Multi-int union-dedup semantics: multi-phase dump
                            // merges produce Merge ops with partial arrays that
                            // must union, not overwrite. Matches DocSnapshotCodec.
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
                        snapshot.fields.push((*field_idx, value.clone()));
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FieldValue ↔ PackedValue conversions
// ---------------------------------------------------------------------------

pub fn field_value_to_packed(fv: &FieldValue) -> PackedValue {
    match fv {
        FieldValue::Single(v) => value_to_packed(v),
        FieldValue::Multi(vs) => {
            // Try the fast integer path first.
            let mut ints: Vec<i64> = Vec::with_capacity(vs.len());
            let mut all_int = true;
            for v in vs {
                if let Value::Integer(i) = v {
                    ints.push(*i);
                } else {
                    all_int = false;
                    break;
                }
            }
            if all_int {
                PackedValue::Mi(ints)
            } else {
                PackedValue::Mm(vs.iter().map(value_to_packed).collect())
            }
        }
    }
}

pub fn packed_to_field_value(pv: &PackedValue) -> FieldValue {
    match pv {
        PackedValue::I(i) => FieldValue::Single(Value::Integer(*i)),
        PackedValue::F(f) => FieldValue::Single(Value::Float(*f)),
        PackedValue::B(b) => FieldValue::Single(Value::Bool(*b)),
        PackedValue::S(s) => FieldValue::Single(Value::String(s.clone())),
        PackedValue::Mi(v) => {
            FieldValue::Multi(v.iter().map(|i| Value::Integer(*i)).collect())
        }
        PackedValue::Mm(v) => {
            FieldValue::Multi(v.iter().map(packed_value_to_value).collect())
        }
    }
}

fn value_to_packed(v: &Value) -> PackedValue {
    match v {
        Value::Integer(i) => PackedValue::I(*i),
        Value::Float(f) => PackedValue::F(*f),
        Value::Bool(b) => PackedValue::B(*b),
        Value::String(s) => PackedValue::S(s.clone()),
    }
}

fn packed_value_to_value(pv: &PackedValue) -> Value {
    match pv {
        PackedValue::I(i) => Value::Integer(*i),
        PackedValue::F(f) => Value::Float(*f),
        PackedValue::B(b) => Value::Bool(*b),
        PackedValue::S(s) => Value::String(s.clone()),
        PackedValue::Mi(_) | PackedValue::Mm(_) => Value::Integer(0), // unreachable in practice
    }
}

// ---------------------------------------------------------------------------
// DocSilo — the public-facing type that bitdex-v2 holds in ConcurrentEngine
// ---------------------------------------------------------------------------

type Inner = DataSilo<SlotSnapshotCodec, SlotDocOpCodec>;

pub struct DocSilo {
    silo: Inner,
    root: PathBuf,
    field_to_idx: HashMap<String, u16>,
    idx_to_field: Vec<String>,
}

impl DocSilo {
    pub fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;
        let silo_path = path.join("silo");
        // buffer_ratio=4.0 + min_entry_size=1024 mirrors v3 doc silo config:
        // multi-phase dumps grow each doc ~3-5x between phase 1 and phase N,
        // so we overallocate up front to keep rewrites in-place.
        let config = SiloConfig {
            buffer_ratio: 4.0,
            min_entry_size: 1024,
            ..SiloConfig::default()
        };
        let silo: Inner = DataSilo::open(&silo_path, config)?;

        // Load field dictionary if present.
        let dict_path = path.join("field_dict.json");
        let (field_to_idx, idx_to_field) = if dict_path.exists() {
            let data = std::fs::read_to_string(&dict_path)?;
            let dict: Vec<String> = serde_json::from_str(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let f2i: HashMap<String, u16> = dict
                .iter()
                .enumerate()
                .map(|(i, name)| (name.clone(), i as u16))
                .collect();
            (f2i, dict)
        } else {
            (HashMap::new(), Vec::new())
        };

        Ok(Self {
            silo,
            root: path.to_path_buf(),
            field_to_idx,
            idx_to_field,
        })
    }

    // ── Field dictionary ────────────────────────────────────────────────

    pub fn field_to_idx(&self) -> &HashMap<String, u16> {
        &self.field_to_idx
    }

    pub fn idx_to_field(&self) -> &[String] {
        &self.idx_to_field
    }

    pub fn ensure_field_index(&mut self, name: &str) -> u16 {
        if let Some(&idx) = self.field_to_idx.get(name) {
            return idx;
        }
        let idx = self.idx_to_field.len() as u16;
        self.field_to_idx.insert(name.to_string(), idx);
        self.idx_to_field.push(name.to_string());
        idx
    }

    pub fn save_field_dict(&self) -> io::Result<()> {
        let dict_path = self.root.join("field_dict.json");
        let json = serde_json::to_string(&self.idx_to_field)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(&dict_path, json)
    }

    // ── Read path ──────────────────────────────────────────────────────

    pub fn get(&self, slot: u32) -> io::Result<Option<StoredDoc>> {
        match self.silo.get(slot_to_key(slot))? {
            Some(snap) if snap.alive => Ok(Some(self.snapshot_to_stored_doc(&snap))),
            _ => Ok(None),
        }
    }

    pub fn get_many(&self, slots: &[u32]) -> io::Result<Vec<Option<StoredDoc>>> {
        let keys: Vec<u64> = slots.iter().map(|&s| slot_to_key(s)).collect();
        let snapshots = self.silo.get_many(&keys)?;
        Ok(snapshots
            .into_iter()
            .map(|snap| match snap {
                Some(s) if s.alive => Some(self.snapshot_to_stored_doc(&s)),
                _ => None,
            })
            .collect())
    }

    pub fn contains(&self, slot: u32) -> io::Result<bool> {
        Ok(self.get(slot)?.is_some())
    }

    fn snapshot_to_stored_doc(&self, snap: &SlotSnapshot) -> StoredDoc {
        let mut map: ahash::AHashMap<String, FieldValue> =
            ahash::AHashMap::with_capacity(snap.fields.len());
        for (field_idx, value) in &snap.fields {
            if let Some(name) = self.idx_to_field.get(*field_idx as usize) {
                map.insert(name.clone(), packed_to_field_value(value));
            }
        }
        StoredDoc {
            fields: map,
            schema_version: 0u8,
        }
    }

    // ── Write path ─────────────────────────────────────────────────────

    /// Append one typed op, auto-registering any field name referenced via
    /// its string form. For raw typed ops with pre-assigned field indices,
    /// call `apply_op` directly.
    pub fn apply_op(&self, op: &DocOp) -> io::Result<()> {
        self.silo.append_op(op)
    }

    pub fn apply_ops_batch(&self, ops: &[DocOp]) -> io::Result<()> {
        self.silo.append_ops_batch(ops)
    }

    /// Encode a `StoredDoc` into a `DocOp::Create` and append it.
    /// Auto-registers any new field names.
    pub fn put(&mut self, slot: u32, doc: &StoredDoc) -> io::Result<()> {
        let fields = self.encode_stored_doc_auto(doc);
        let op = DocOp::Create { slot, fields };
        self.silo.append_op(&op)
    }

    pub fn put_batch(&mut self, docs: &[(u32, StoredDoc)]) -> io::Result<()> {
        let ops: Vec<DocOp> = docs
            .iter()
            .map(|(slot, doc)| {
                let fields = self.encode_stored_doc_auto(doc);
                DocOp::Create { slot: *slot, fields }
            })
            .collect();
        self.silo.append_ops_batch(&ops)
    }

    fn encode_stored_doc_auto(&mut self, doc: &StoredDoc) -> Vec<(u16, PackedValue)> {
        let mut out = Vec::with_capacity(doc.fields.len());
        for (name, value) in &doc.fields {
            let idx = self.ensure_field_index(name);
            out.push((idx, field_value_to_packed(value)));
        }
        out
    }

    /// Bulk load: replace the data file + index with a fresh snapshot.
    /// Destructive — truncates both ops logs. Used by dump phase 1.
    pub fn bulk_load(&mut self, docs: &[(u32, StoredDoc)]) -> io::Result<u64> {
        // Auto-register fields first so we don't mutate the dict mid-encode.
        for (_, doc) in docs {
            for name in doc.fields.keys() {
                let _ = self.ensure_field_index(name);
            }
        }
        let mut entries: Vec<(u64, SlotSnapshot)> = Vec::with_capacity(docs.len());
        for (slot, doc) in docs {
            let fields = self.encode_stored_doc_auto(doc);
            entries.push((
                slot_to_key(*slot),
                SlotSnapshot { fields, alive: true },
            ));
        }
        self.silo.bulk_load(&entries)
    }

    // ── Maintenance ────────────────────────────────────────────────────

    pub fn compact(&mut self) -> io::Result<u64> {
        self.silo.compact()
    }

    /// Flush the silo's data + index mmaps to disk. Streaming populate paths
    /// that call `compact()` many times should call this ONCE at the end
    /// rather than paying per-compact msync cost.
    pub fn sync(&self) -> io::Result<()> {
        self.silo.sync()
    }

    pub fn has_ops(&self) -> bool {
        self.silo.has_ops()
    }

    pub fn ops_size(&self) -> u64 {
        self.silo.ops_size()
    }

    pub fn needs_compaction(&self) -> bool {
        self.silo.needs_compaction()
    }

    pub fn data_bytes(&self) -> u64 {
        self.silo.data_bytes()
    }

    pub fn dead_bytes(&self) -> u64 {
        self.silo.dead_bytes()
    }

    pub fn index_count(&self) -> u64 {
        self.silo.index_count()
    }

    pub fn path(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(fields: &[(&str, FieldValue)]) -> StoredDoc {
        let mut map = HashMap::new();
        for (k, v) in fields {
            map.insert(k.to_string(), v.clone());
        }
        StoredDoc {
            fields: map,
            schema_version: 0u8,
        }
    }

    #[test]
    fn put_then_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DocSilo::open(dir.path()).unwrap();
        let doc = make_doc(&[
            ("nsfwLevel", FieldValue::Single(Value::Integer(8))),
            ("isPublished", FieldValue::Single(Value::Bool(true))),
            (
                "tagIds",
                FieldValue::Multi(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                ]),
            ),
        ]);
        silo.put(42, &doc).unwrap();
        let got = silo.get(42).unwrap().unwrap();
        assert_eq!(got.fields.len(), 3);
        assert_eq!(
            got.fields.get("nsfwLevel"),
            Some(&FieldValue::Single(Value::Integer(8)))
        );
        assert!(matches!(
            got.fields.get("tagIds"),
            Some(FieldValue::Multi(_))
        ));
    }

    #[test]
    fn typed_set_op_updates_field_without_reencoding_doc() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DocSilo::open(dir.path()).unwrap();
        silo.put(
            1,
            &make_doc(&[("nsfwLevel", FieldValue::Single(Value::Integer(8)))]),
        )
        .unwrap();
        let idx = silo.ensure_field_index("nsfwLevel");
        silo.apply_op(&DocOp::Set {
            slot: 1,
            field: idx,
            value: PackedValue::I(16),
        })
        .unwrap();
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(
            got.fields.get("nsfwLevel"),
            Some(&FieldValue::Single(Value::Integer(16)))
        );
    }

    #[test]
    fn typed_append_op_unions_multi_int() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DocSilo::open(dir.path()).unwrap();
        silo.put(
            1,
            &make_doc(&[(
                "tagIds",
                FieldValue::Multi(vec![Value::Integer(1), Value::Integer(2)]),
            )]),
        )
        .unwrap();
        let idx = silo.ensure_field_index("tagIds");
        silo.apply_ops_batch(&[
            DocOp::Append {
                slot: 1,
                field: idx,
                value: PackedValue::I(3),
            },
            DocOp::Append {
                slot: 1,
                field: idx,
                value: PackedValue::I(2), // dedup
            },
        ])
        .unwrap();
        let got = silo.get(1).unwrap().unwrap();
        match got.fields.get("tagIds") {
            Some(FieldValue::Multi(vs)) => {
                let ints: Vec<i64> = vs
                    .iter()
                    .filter_map(|v| if let Value::Integer(i) = v { Some(*i) } else { None })
                    .collect();
                assert_eq!(ints, vec![1, 2, 3]);
            }
            other => panic!("expected Multi, got {other:?}"),
        }
    }

    #[test]
    fn delete_op_hides_doc_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DocSilo::open(dir.path()).unwrap();
        silo.put(
            1,
            &make_doc(&[("x", FieldValue::Single(Value::Integer(1)))]),
        )
        .unwrap();
        assert!(silo.get(1).unwrap().is_some());
        silo.apply_op(&DocOp::Delete { slot: 1 }).unwrap();
        assert!(silo.get(1).unwrap().is_none());
    }

    #[test]
    fn compact_folds_ops_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DocSilo::open(dir.path()).unwrap();
        silo.put(
            1,
            &make_doc(&[("nsfwLevel", FieldValue::Single(Value::Integer(0)))]),
        )
        .unwrap();
        let idx = silo.ensure_field_index("nsfwLevel");
        silo.apply_ops_batch(&[
            DocOp::Set {
                slot: 1,
                field: idx,
                value: PackedValue::I(8),
            },
            DocOp::Set {
                slot: 1,
                field: idx,
                value: PackedValue::I(16),
            },
        ])
        .unwrap();
        assert!(silo.has_ops());
        silo.compact().unwrap();
        assert!(!silo.has_ops());
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(
            got.fields.get("nsfwLevel"),
            Some(&FieldValue::Single(Value::Integer(16)))
        );
    }

    #[test]
    fn get_many_batched() {
        let dir = tempfile::tempdir().unwrap();
        let mut silo = DocSilo::open(dir.path()).unwrap();
        for i in 0..10u32 {
            silo.put(
                i,
                &make_doc(&[("x", FieldValue::Single(Value::Integer(i as i64)))]),
            )
            .unwrap();
        }
        let got = silo.get_many(&[0, 1, 2, 99]).unwrap();
        assert_eq!(got.len(), 4);
        assert!(got[0].is_some());
        assert!(got[1].is_some());
        assert!(got[2].is_some());
        assert!(got[3].is_none());
    }
}
