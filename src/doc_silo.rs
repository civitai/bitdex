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
use crate::doc_wire_format::{PackedValue, StoredDoc};

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
// Mirrors `doc_wire_format::DocOp` exactly; kept in this module so the port
// doesn't depend on the soon-to-be-deleted `doc_wire_format`. Encoding is
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
// PackedValue encode / decode (same wire format as doc_wire_format)
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

/// Encode a slot's packed field list directly to `SlotSnapshotCodec`'s
/// wire format without constructing a `SlotSnapshot` intermediate. Used
/// by populate paths that read `Vec<(u16, PackedValue)>` straight from
/// DocStoreV3 shards and want to avoid allocating StoredDoc HashMaps.
/// Merge two encoded snapshot byte slices: decode both, combine fields
/// (new overwrites existing by field_idx), re-encode. Used by DumpMergeWriter
/// during multi-phase dumps and by the dump processor's DocWriteTarget::Merge path.
pub fn merge_encoded_snapshots(existing: &[u8], new: &[u8]) -> Vec<u8> {
    let existing_snap = SlotSnapshotCodec::decode(existing).unwrap_or_else(|_| SlotSnapshot::empty());
    let new_snap = SlotSnapshotCodec::decode(new).unwrap_or_else(|_| SlotSnapshot::empty());
    let mut merged: std::collections::HashMap<u16, PackedValue> = std::collections::HashMap::new();
    for (idx, val) in &existing_snap.fields {
        merged.insert(*idx, val.clone());
    }
    for (idx, val) in &new_snap.fields {
        merged.insert(*idx, val.clone());
    }
    let fields: Vec<(u16, PackedValue)> = merged.into_iter().collect();
    let mut buf = Vec::new();
    encode_slot_bytes(true, &fields, &mut buf);
    buf
}

pub fn encode_slot_bytes(alive: bool, fields: &[(u16, PackedValue)], out: &mut Vec<u8>) {
    out.clear();
    out.push(if alive { 1 } else { 0 });
    out.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    for (field, value) in fields {
        encode_field_pair(*field, value, out);
    }
}

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
    pub fn open_temp() -> io::Result<Self> {
        let dir = tempfile::tempdir()?;
        Self::open(dir.path())
    }

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

    /// Bulk load pre-encoded entries. Each `Vec<u8>` must be a `SlotSnapshotCodec`-
    /// compatible byte sequence `[alive:u8][num_fields:u16][field_pair...]`.
    /// Use `encode_slot_bytes()` to produce these. Skips the `StoredDoc` +
    /// `SlotSnapshot` intermediates — the path populate uses at 107M scale
    /// to keep per-doc memory to ~200 bytes instead of ~1.4 KB.
    pub fn bulk_load_encoded(&mut self, encoded: Vec<(u64, Vec<u8>)>) -> io::Result<u64> {
        self.silo.bulk_load_encoded(encoded)
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

    /// Create a DumpMergeWriter for in-place read-modify-write during dump
    /// phases 2+. Returns None if no data/index exist (phase 1 hasn't run).
    pub fn prepare_dump_merge(&self) -> std::io::Result<Option<datasilo::DumpMergeWriter>> {
        self.silo.prepare_dump_merge()
    }

    /// Reload the data mmap after DumpMergeWriter writes complete.
    pub fn reload_data(&mut self) -> std::io::Result<()> {
        self.silo.reload_data()
    }
}

// ---------------------------------------------------------------------------
// DocSiloBulkWriter — parallel-safe dump-time writer
// ---------------------------------------------------------------------------
//
// Replaces `StreamingDocWriter` in the dump_processor hot path. Matches the
// method signatures so dump_processor only needs to swap the type.
//
// Architecture:
// - One append-only `data.bin` file, tightly packed (no per-entry slack).
// - Rayon-parallel writes go through a single parking_lot::Mutex. At dump
//   rate (tens of thousands of docs/sec) lock contention is negligible.
// - Each `append_merge_payload` call decodes the merge wire format, re-
//   encodes as SlotSnapshot bytes, seeks to end-of-file, appends, records
//   layout.
// - `finalize` builds the hash index via `HashIndex::build_bulk` from the
//   accumulated layout Vec (~109M × 24 bytes = 2.6 GB for 107M dataset,
//   manageable), flushes data.bin, persists the field dictionary.

use std::io::Write;

/// Lock-free mmap-backed bulk writer state. Pre-allocates a data.bin file
/// large enough to fit the expected output plus slack, then hands rayon
/// threads atomic-cursor-bumped disjoint regions to write into. No shared
/// lock in the hot path — writes are racy-by-design against DIFFERENT
/// regions of the mmap, which is safe because regions don't overlap.
///
/// Key insight: the dump knows the CSV row count upfront. We use that to
/// pre-allocate `rows × INITIAL_BYTES_PER_DOC × 1.5` of file space, which
/// is truncated to the actual used bytes at finalize. Growth-on-overflow
/// is a fallback if the estimate is too low.
const INITIAL_BYTES_PER_DOC: usize = 256;
/// Ratio of data.bin capacity to estimated data size. 1.1 = 10% slack.
/// Previously 1.5, reduced to fit within 32 GiB pod limit at 107M records.
/// At 107M × ~200 bytes/doc, 1.5 = ~31 GB sparse vs 1.1 = ~23 GB.
const SLACK_RATIO: f64 = 1.1;

struct BulkState {
    /// Keeps the writable mmap alive. Only touched during `new`, `grow`,
    /// and `finalize` — not the hot write path.
    mmap: memmap2::MmapMut,
    /// Backing file handle. Needed to `set_len` on grow + finalize truncate.
    data_file: std::fs::File,
    /// Layouts for hash-index build. Appended via a per-thread DashMap slab
    /// and flattened at finalize.
    layouts: Vec<(u64, u64, u32)>,
}

/// Per-rayon-thread scratch state. Each rayon worker owns exactly one of
/// these — indexed by `rayon::current_thread_index()` — so access is
/// single-threaded and needs no synchronization. The cursor-lease pattern
/// batches global `fetch_add` calls, trading a few KB of trailing slack
/// per thread for ~8000x fewer atomic ops.
/// Max layouts to hold in memory per thread before spilling to disk.
/// 64K entries × 24 bytes = 1.5 MB per thread, ~48 MB total at 32 threads.
/// Previously unlimited, which meant 220M entries × 24 bytes = 5.3 GB at 107M.
const LAYOUT_SPILL_THRESHOLD: usize = 64 * 1024;

#[repr(align(64))] // cache-line pad to avoid false sharing between workers
struct ThreadSlot {
    /// Next free byte within the current lease region.
    lease_cur: u64,
    /// End of the current lease region (exclusive).
    lease_end: u64,
    /// In-memory layout buffer; spilled to `layout_file` when full.
    layouts: Vec<(u64, u64, u32)>,
    /// Spill file for layouts that exceed the in-memory threshold.
    layout_file: Option<std::io::BufWriter<std::fs::File>>,
    /// Path to the spill file (for reading back at finalize).
    layout_path: Option<PathBuf>,
    /// Total entries spilled to disk.
    spilled_count: u64,
}

impl ThreadSlot {
    fn new() -> Self {
        Self {
            lease_cur: 0,
            lease_end: 0,
            layouts: Vec::with_capacity(LAYOUT_SPILL_THRESHOLD),
            layout_file: None,
            layout_path: None,
            spilled_count: 0,
        }
    }

    /// Spill in-memory layouts to a temp file when the buffer is full.
    fn maybe_spill(&mut self, silo_root: &Path) {
        if self.layouts.len() < LAYOUT_SPILL_THRESHOLD {
            return;
        }
        use std::io::Write;
        // Lazy-init the spill file
        if self.layout_file.is_none() {
            let spill_dir = silo_root.join("silo");
            let _ = std::fs::create_dir_all(&spill_dir);
            let tid = std::thread::current().id();
            let path = spill_dir.join(format!("layouts_spill_{:?}.tmp", tid));
            match std::fs::File::create(&path) {
                Ok(f) => {
                    self.layout_file = Some(std::io::BufWriter::with_capacity(256 * 1024, f));
                    self.layout_path = Some(path);
                }
                Err(e) => {
                    eprintln!("WARNING: layout spill file create failed: {e} — keeping in memory");
                    return;
                }
            }
        }
        // Write all buffered layouts to disk
        if let Some(ref mut writer) = self.layout_file {
            let mut ok = true;
            for &(key, offset, length) in &self.layouts {
                if writer.write_all(&key.to_le_bytes()).is_err()
                    || writer.write_all(&offset.to_le_bytes()).is_err()
                    || writer.write_all(&length.to_le_bytes()).is_err()
                {
                    eprintln!("WARNING: layout spill write failed — keeping remaining in memory");
                    ok = false;
                    break;
                }
            }
            if ok {
                self.spilled_count += self.layouts.len() as u64;
                self.layouts.clear();
            }
        }
    }
}

/// Bytes each thread reserves from the global cursor in one atomic bump.
/// At ~85 bytes/doc average, 1 MB holds ~12K docs before the next fetch_add.
const CURSOR_LEASE_BYTES: u64 = 1024 * 1024;

/// Wrapper for a `Vec<UnsafeCell<ThreadSlot>>` that is safe to share
/// across rayon workers: each thread only ever touches its own slot
/// (indexed by `current_thread_index`), so there is no aliasing.
struct ThreadSlots {
    slots: Vec<std::cell::UnsafeCell<ThreadSlot>>,
    /// Fallback slot for non-rayon threads (tests, ad-hoc callers).
    fallback: parking_lot::Mutex<ThreadSlot>,
}

unsafe impl Sync for ThreadSlots {}

pub struct DocSiloBulkWriter {
    silo_root: PathBuf,
    field_to_idx: ahash::AHashMap<String, u16>,
    idx_to_field: Vec<String>,
    field_defaults: HashMap<u16, PackedValue>,
    /// State that needs serialized access during new/grow/finalize. Held
    /// briefly ONLY on those paths, never on the hot `append_merge_payload`.
    state: parking_lot::Mutex<BulkState>,
    /// Raw pointer to the mmap's start. Reads of this pointer are valid for
    /// the entire lifetime of the writer because `BulkState::mmap` is not
    /// remapped during writes (growth is a follow-up — currently we
    /// over-allocate upfront).
    mmap_ptr: std::sync::atomic::AtomicPtr<u8>,
    /// Total bytes available in the pre-allocated mmap region.
    mmap_capacity: std::sync::atomic::AtomicU64,
    /// Atomic byte cursor into the mmap. Each thread leases
    /// `CURSOR_LEASE_BYTES` at a time and carves per-doc offsets locally.
    write_cursor: std::sync::atomic::AtomicU64,
    /// Count of successful appends (atomic so we can report without locks).
    appended_count: std::sync::atomic::AtomicU64,
    /// Count of appends that were dropped because the mmap overflowed.
    overflow_count: std::sync::atomic::AtomicU64,
    /// Per-rayon-worker scratch state. Hot path does NO synchronization.
    thread_slots: ThreadSlots,
}

impl DocSiloBulkWriter {
    /// Create a fresh bulk writer. Opens `silo_root/silo/data.bin` for
    /// append, truncating any existing file. The field dictionary is
    /// initialized from `field_names` in order (silo index N == field_names[N]).
    pub fn new(
        silo_root: PathBuf,
        field_names: &[String],
    ) -> io::Result<Self> {
        // Default to pre-allocating for 14M rows. The dump pipeline can
        // override with `new_with_capacity` when it knows the actual row
        // count. If the estimate is too small we bail at the first
        // overflow with an error — no auto-grow (v3 does auto-grow but
        // that requires remap + pointer stability tricks; we skip it).
        Self::new_with_capacity(silo_root, field_names, 14_000_000)
    }

    /// Pre-allocate the data.bin mmap for `estimated_rows` documents at
    /// `INITIAL_BYTES_PER_DOC * SLACK_RATIO` bytes per doc. This is the
    /// v3 `write_batch_parallel` pattern adapted for streaming writes.
    pub fn new_with_capacity(
        silo_root: PathBuf,
        field_names: &[String],
        estimated_rows: usize,
    ) -> io::Result<Self> {
        let silo_subdir = silo_root.join("silo");
        std::fs::create_dir_all(&silo_subdir)?;
        let data_path = silo_subdir.join("data.bin");
        let data_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&data_path)?;
        // Wipe stale siblings so the silo opens clean later.
        let _ = std::fs::remove_file(silo_subdir.join("index.bin"));
        let _ = std::fs::remove_file(silo_subdir.join("ops_a.log"));
        let _ = std::fs::remove_file(silo_subdir.join("ops_b.log"));

        // Pre-allocate with generous slack. For 14M docs × 256 × 1.5 = ~5 GB.
        // The file is truncated to actual used bytes at finalize.
        let capacity = (estimated_rows as f64 * INITIAL_BYTES_PER_DOC as f64 * SLACK_RATIO)
            .ceil() as u64;
        let capacity = capacity.max(64 * 1024 * 1024); // at least 64 MB
        data_file.set_len(capacity)?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&data_file)? };
        #[cfg(unix)]
        let _ = mmap.advise(memmap2::Advice::Sequential);
        let mmap_ptr = mmap.as_mut_ptr();

        let mut field_to_idx: ahash::AHashMap<String, u16> =
            ahash::AHashMap::with_capacity(field_names.len());
        let mut idx_to_field = Vec::with_capacity(field_names.len());
        for (i, name) in field_names.iter().enumerate() {
            field_to_idx.insert(name.clone(), i as u16);
            idx_to_field.push(name.clone());
        }

        eprintln!(
            "DocSiloBulkWriter: pre-allocated data.bin {:.1} MB for ~{} rows",
            capacity as f64 / 1e6,
            estimated_rows
        );

        Ok(Self {
            silo_root,
            field_to_idx,
            idx_to_field,
            field_defaults: HashMap::new(),
            state: parking_lot::Mutex::new(BulkState {
                mmap,
                data_file,
                layouts: Vec::new(),
            }),
            mmap_ptr: std::sync::atomic::AtomicPtr::new(mmap_ptr),
            mmap_capacity: std::sync::atomic::AtomicU64::new(capacity),
            write_cursor: std::sync::atomic::AtomicU64::new(0),
            appended_count: std::sync::atomic::AtomicU64::new(0),
            overflow_count: std::sync::atomic::AtomicU64::new(0),
            thread_slots: ThreadSlots {
                slots: (0..rayon::current_num_threads())
                    .map(|_| std::cell::UnsafeCell::new(ThreadSlot::new()))
                    .collect(),
                fallback: parking_lot::Mutex::new(ThreadSlot::new()),
            },
        })
    }

    pub fn field_to_idx(&self) -> &ahash::AHashMap<String, u16> {
        &self.field_to_idx
    }

    /// Set field defaults. Dump pipeline calls this after construction.
    pub fn set_field_defaults(&mut self, defaults: HashMap<u16, PackedValue>) {
        self.field_defaults = defaults;
    }

    /// Append a pre-encoded DocOp::Merge payload for a slot.
    ///
    /// Payload wire format (produced by `doc_wire_format::write_merge_header`
    /// + `write_field_*`):
    ///
    /// ```text
    /// [OP_TAG_MERGE = 0x06][slot:u32][num_fields:u16][field_pair ...]
    /// ```
    ///
    /// We translate to SlotSnapshot wire format:
    ///
    /// ```text
    /// [alive:u8 = 1][num_fields:u16][field_pair ...]
    /// ```
    ///
    /// by stripping the first 5 bytes and prepending `[1u8]`. The field
    /// pair bytes are identical between the two formats.
    pub fn append_merge_payload(&self, slot: u32, payload: &[u8]) {
        if payload.len() < 7 {
            return;
        }
        // Skip OP_TAG_MERGE + slot prefix. The remaining
        // [num_fields:u16][field_pair...] is exactly the SlotSnapshot tail.
        let fields_bytes = &payload[5..];
        let key = slot_to_key(slot);
        let length = (1 + fields_bytes.len()) as u32;
        let length_u64 = length as u64;

        let silo_root = &self.silo_root;
        self.with_thread_slot(|ts| {
            let Some(offset) = self.reserve(ts, length_u64) else { return };
            unsafe {
                let base = self.mmap_ptr.load(std::sync::atomic::Ordering::Relaxed);
                let dst = base.add(offset as usize);
                *dst = 1u8;
                std::ptr::copy_nonoverlapping(
                    fields_bytes.as_ptr(),
                    dst.add(1),
                    fields_bytes.len(),
                );
            }
            ts.layouts.push((key, offset, length));
            ts.maybe_spill(silo_root);
        });
        self.appended_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Encode a slot's packed tuples as a DocOp::Merge and append.
    /// API-compatible with `StreamingDocWriter::append_tuples_merge`, but
    /// here the tuples are raw msgpack-encoded bytes that need decoding first.
    pub fn append_tuples_merge(
        &self,
        slot: u32,
        tuples: &[(u16, &[u8])],
        write_buf: &mut Vec<u8>,
    ) {
        if tuples.is_empty() {
            return;
        }
        // Decode msgpack → PackedValue → rebuild as SlotSnapshot bytes directly.
        write_buf.clear();
        write_buf.push(1u8); // alive
        write_buf.extend_from_slice(&(tuples.len() as u16).to_le_bytes());
        let mut kept = 0u16;
        let reserve_pos = write_buf.len() - 2;
        for &(field_idx, value_bytes) in tuples {
            let pv: PackedValue = match rmp_serde::from_slice(value_bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if self.field_defaults.get(&field_idx).map_or(false, |d| d == &pv) {
                continue;
            }
            encode_field_pair(field_idx, &pv, write_buf);
            kept += 1;
        }
        if kept == 0 {
            return;
        }
        // Update num_fields header (may differ from tuples.len() after filtering).
        write_buf[reserve_pos..reserve_pos + 2].copy_from_slice(&kept.to_le_bytes());
        let _ = reserve_pos;

        let key = slot_to_key(slot);
        let length = write_buf.len() as u32;
        let length_u64 = length as u64;

        let silo_root = &self.silo_root;
        self.with_thread_slot(|ts| {
            let Some(offset) = self.reserve(ts, length_u64) else { return };
            unsafe {
                let base = self.mmap_ptr.load(std::sync::atomic::Ordering::Relaxed);
                let dst = base.add(offset as usize);
                std::ptr::copy_nonoverlapping(write_buf.as_ptr(), dst, write_buf.len());
            }
            ts.layouts.push((key, offset, length));
            ts.maybe_spill(silo_root);
        });
        self.appended_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Dispatch to the current thread's slot. Rayon workers hit a wait-free
    /// path (plain `UnsafeCell` access indexed by `current_thread_index`);
    /// non-rayon threads fall back to a shared mutex slot.
    #[inline]
    fn with_thread_slot<R>(&self, f: impl FnOnce(&mut ThreadSlot) -> R) -> R {
        match rayon::current_thread_index() {
            Some(i) if i < self.thread_slots.slots.len() => {
                let cell = &self.thread_slots.slots[i];
                // SAFETY: `rayon::current_thread_index()` returns a unique
                // stable index per worker, so this &mut is the only live
                // reference to this slot for the duration of the closure.
                unsafe { f(&mut *cell.get()) }
            }
            _ => {
                let mut guard = self.thread_slots.fallback.lock();
                f(&mut *guard)
            }
        }
    }

    /// Reserve `length` contiguous bytes in the mmap for the calling
    /// thread. Amortizes global cursor bumps via per-thread leases of
    /// `CURSOR_LEASE_BYTES`. Returns `None` and bumps the overflow counter
    /// if the mmap is exhausted.
    #[inline]
    fn reserve(&self, ts: &mut ThreadSlot, length: u64) -> Option<u64> {
        use std::sync::atomic::Ordering;
        if ts.lease_cur + length > ts.lease_end {
            // Need a fresh lease. Grab at least enough for this doc.
            let grab = CURSOR_LEASE_BYTES.max(length);
            let base = self.write_cursor.fetch_add(grab, Ordering::Relaxed);
            let capacity = self.mmap_capacity.load(Ordering::Relaxed);
            if base + grab > capacity {
                self.overflow_count.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            ts.lease_cur = base;
            ts.lease_end = base + grab;
        }
        let offset = ts.lease_cur;
        ts.lease_cur += length;
        Some(offset)
    }

    /// Flush data.bin, build the hash index, and persist the field dict.
    pub fn finalize(&self) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        let final_len = self.write_cursor.load(Ordering::Acquire);
        let appended = self.appended_count.load(Ordering::Relaxed);
        let overflow = self.overflow_count.load(Ordering::Relaxed);
        eprintln!(
            "DocSiloBulkWriter::finalize: {} appended, {} overflow, cursor={}",
            appended, overflow, final_len
        );
        if overflow > 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "DocSiloBulkWriter overflowed pre-allocated mmap ({} entries dropped). \
                     Increase `estimated_rows` in `new_with_capacity`.",
                    overflow
                ),
            ));
        }

        // Drain every per-rayon-worker layouts: flush remaining in-memory
        // entries to spill files, then read all spill files back into one Vec.
        // This keeps peak memory at ~48 MB (32 threads × 1.5 MB) instead of
        // 5.3 GB (220M entries × 24 bytes).
        use std::io::{Read as IoRead, Write as IoWrite2};
        let mut spill_paths: Vec<PathBuf> = Vec::new();
        for cell in &self.thread_slots.slots {
            let slab = unsafe { &mut *cell.get() };
            // Flush remaining in-memory layouts to spill file
            if !slab.layouts.is_empty() {
                slab.maybe_spill(&self.silo_root);
                // If there are still entries (below threshold), force-spill them
                if !slab.layouts.is_empty() && slab.layout_file.is_some() {
                    if let Some(ref mut writer) = slab.layout_file {
                        let mut ok = true;
                        for &(key, offset, length) in &slab.layouts {
                            if writer.write_all(&key.to_le_bytes()).is_err()
                                || writer.write_all(&offset.to_le_bytes()).is_err()
                                || writer.write_all(&length.to_le_bytes()).is_err()
                            {
                                ok = false;
                                break;
                            }
                        }
                        if ok {
                            slab.spilled_count += slab.layouts.len() as u64;
                            slab.layouts.clear();
                        }
                    }
                }
            }
            // Flush the BufWriter
            if let Some(ref mut writer) = slab.layout_file {
                let _ = writer.flush();
            }
            slab.layout_file = None; // close file handle
            if let Some(path) = slab.layout_path.take() {
                spill_paths.push(path);
            }
        }
        // Collect fallback slot layouts that never spilled (kept in-memory).
        // These must be captured here so they're not lost.
        let mut fallback_unsorted: Vec<(u64, u64, u32)> = Vec::new();
        {
            let mut fallback = self.thread_slots.fallback.lock();
            if !fallback.layouts.is_empty() {
                fallback.maybe_spill(&self.silo_root);
            }
            let remaining: Vec<(u64, u64, u32)> = std::mem::take(&mut fallback.layouts);
            if !remaining.is_empty() {
                if let Some(ref mut writer) = fallback.layout_file {
                    let mut ok = true;
                    for &(key, offset, length) in &remaining {
                        if writer.write_all(&key.to_le_bytes()).is_err()
                            || writer.write_all(&offset.to_le_bytes()).is_err()
                            || writer.write_all(&length.to_le_bytes()).is_err()
                        {
                            ok = false;
                            break;
                        }
                    }
                    if !ok {
                        // Write failed — keep entries in fallback_unsorted
                        fallback_unsorted = remaining;
                    }
                    let _ = writer.flush();
                } else {
                    // No spill file — keep entries directly
                    fallback_unsorted = remaining;
                }
            }
            fallback.layout_file = None;
            if let Some(path) = fallback.layout_path.take() {
                spill_paths.push(path);
            }
        }

        // Read all spill files back into one combined Vec.
        // Don't pre-allocate for `appended` entries — that would be 5.3 GB at 220M.
        // Instead grow incrementally; Vec doubling strategy keeps total alloc < 2× final.
        const SPILL_ENTRY_SIZE: usize = 20; // u64(8) + u64(8) + u32(4)
        let mut layouts: Vec<(u64, u64, u32)> = Vec::new();
        for path in &spill_paths {
            match std::fs::read(path) {
                Ok(data) => {
                    let n_entries = data.len() / SPILL_ENTRY_SIZE;
                    layouts.reserve(n_entries);
                    let mut pos = 0;
                    while pos + SPILL_ENTRY_SIZE <= data.len() {
                        let key = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
                        let offset = u64::from_le_bytes(data[pos+8..pos+16].try_into().unwrap());
                        let length = u32::from_le_bytes(data[pos+16..pos+20].try_into().unwrap());
                        layouts.push((key, offset, length));
                        pos += SPILL_ENTRY_SIZE;
                    }
                }
                Err(e) => {
                    eprintln!("WARNING: failed to read spill file {}: {e}", path.display());
                }
            }
            let _ = std::fs::remove_file(path);
        }
        // Collect any remaining in-memory layouts from threads that
        // never hit the spill threshold (small dumps / tests)
        for cell in &self.thread_slots.slots {
            let slab = unsafe { &mut *cell.get() };
            layouts.append(&mut slab.layouts);
        }
        // Collect fallback entries that never spilled
        layouts.append(&mut fallback_unsorted);
        eprintln!(
            "DocSiloBulkWriter::finalize: read {} layouts from {} spill files",
            layouts.len(), spill_paths.len()
        );

        // Flush the mmap, then release it before touching the file.
        let state = self.state.lock();
        state.mmap.flush()?;
        drop(state); // drops mmap + File — both now closed.

        // Reopen the file by path so we can optionally truncate to the
        // actual used bytes. On Windows the truncate can fail if anything
        // else is still holding a mapping (e.g. DataSilo::open has
        // already mmap'd it for reads). That's harmless — data.bin is
        // still valid at any size ≥ final_len because the trailing bytes
        // are zeros that no index entry points at.
        let silo_subdir = self.silo_root.join("silo");
        let data_path = silo_subdir.join("data.bin");
        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&data_path) {
            match file.set_len(final_len) {
                Ok(()) => {
                    let _ = file.sync_data();
                }
                Err(e) => {
                    eprintln!(
                        "DocSiloBulkWriter::finalize: truncate to {} skipped ({}); \
                         data.bin stays at pre-allocated size. Trailing zeros are safe.",
                        final_len, e
                    );
                }
            }
        }

        // Write layouts to a `layouts.bin` sidecar and defer the hash index
        // build to `DocSilo::open` time. Building the index inline here runs
        // into a Windows memory-manager pathology where the 5.4 GB sparse
        // `data.bin` mmap we just released interacts with the subsequent
        // 670 MB `build_bulk` heap buffer and the probe loop slows from
        // nanoseconds to hundreds of microseconds per entry. By the time
        // the server cold-starts and calls `DocSilo::open`, the dump
        // mmap is long gone and `build_bulk` runs in normal time.
        //
        // layouts.bin wire format (matches `DocSilo::load_index` loader):
        //     [count:u64]
        //     [ (key:u64, offset:u64, length:u32, allocated:u32) × count ]
        // All little-endian, 24 bytes per entry. Sorted by key so that the
        // subsequent `HashIndex::build_bulk` writes hit its heap buffer
        // pages sequentially.
        let silo_subdir = self.silo_root.join("silo");
        let index_path = silo_subdir.join("index.bin");
        let layouts_path = silo_subdir.join("layouts.bin");
        let _ = std::fs::remove_file(&index_path);
        let _ = std::fs::remove_file(&layouts_path);

        // Sort by key, then dedup keeping the LAST write per slot. Duplicates
        // indicate the dump source wrote the same PG ID twice (e.g. the
        // legacy `images-small.csv` test fixture is a byte-doubled export).
        // Real Civitai exports never produce duplicates, so this path must
        // be a no-op in production; we warn loudly if it isn't so regression
        // is caught early.
        let mut sorted = layouts;
        sorted.sort_unstable_by_key(|(k, _, _)| *k);
        let before_dedup = sorted.len();
        // dedup_by_key keeps the FIRST of each run; reverse → dedup → reverse
        // keeps the LAST write per slot (the more recent payload wins).
        sorted.reverse();
        sorted.dedup_by_key(|(k, _, _)| *k);
        sorted.reverse();
        let dedup_removed = before_dedup - sorted.len();
        if dedup_removed > 0 {
            eprintln!(
                "WARN: DocSiloBulkWriter::finalize deduped {} duplicate slot writes \
                 ({} → {} layouts). This should be zero for a clean dump — non-zero \
                 means the upstream CSV contains duplicate PG IDs.",
                dedup_removed, before_dedup, sorted.len()
            );
        }

        use std::io::Write as _IoWrite;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&layouts_path)?;
        let count_u64 = sorted.len() as u64;
        file.write_all(&count_u64.to_le_bytes())?;
        // Write in ~1 MB chunks to avoid a single 300+ MB allocation.
        const CHUNK_ENTRIES: usize = 1 << 15; // 32768 × 24 B ≈ 768 KB
        let mut buf: Vec<u8> = Vec::with_capacity(CHUNK_ENTRIES * 24);
        for chunk in sorted.chunks(CHUNK_ENTRIES) {
            buf.clear();
            for &(key, offset, length) in chunk {
                buf.extend_from_slice(&key.to_le_bytes());
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&length.to_le_bytes());
                buf.extend_from_slice(&length.to_le_bytes()); // allocated == length, tight packing
            }
            file.write_all(&buf)?;
        }
        file.sync_data()?;
        drop(file);

        // Persist the field dictionary.
        let dict_path = self.silo_root.join("field_dict.json");
        let json = serde_json::to_string(&self.idx_to_field)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(&dict_path, json)?;

        eprintln!(
            "DocSiloBulkWriter::finalize: {} entries, data.bin {} bytes (index deferred to open via layouts.bin)",
            sorted.len(),
            final_len
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(fields: &[(&str, FieldValue)]) -> StoredDoc {
        let mut map = ahash::AHashMap::new();
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

    // ── Cold restart ───────────────────────────────────────────────

    #[test]
    fn cold_restart_preserves_docs() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut silo = DocSilo::open(dir.path()).unwrap();
            silo.put(1, &make_doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(8))),
            ])).unwrap();
            silo.put(2, &make_doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(4))),
            ])).unwrap();
            silo.save_field_dict().unwrap();
        }
        // Reopen
        let silo = DocSilo::open(dir.path()).unwrap();
        let d1 = silo.get(1).unwrap().unwrap();
        assert_eq!(d1.fields.get("nsfwLevel"), Some(&FieldValue::Single(Value::Integer(8))));
        let d2 = silo.get(2).unwrap().unwrap();
        assert_eq!(d2.fields.get("nsfwLevel"), Some(&FieldValue::Single(Value::Integer(4))));
    }

    #[test]
    fn cold_restart_replays_ops() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut silo = DocSilo::open(dir.path()).unwrap();
            silo.put(1, &make_doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(8))),
            ])).unwrap();
            let idx = silo.ensure_field_index("nsfwLevel");
            silo.apply_op(&DocOp::Set { slot: 1, field: idx, value: PackedValue::I(16) }).unwrap();
            silo.save_field_dict().unwrap();
        }
        let silo = DocSilo::open(dir.path()).unwrap();
        let got = silo.get(1).unwrap().unwrap();
        assert_eq!(got.fields.get("nsfwLevel"), Some(&FieldValue::Single(Value::Integer(16))));
    }

    #[test]
    fn cold_restart_field_dict_intact() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut silo = DocSilo::open(dir.path()).unwrap();
            let idx_a = silo.ensure_field_index("nsfwLevel");
            let idx_b = silo.ensure_field_index("tagIds");
            silo.save_field_dict().unwrap();
            assert_eq!(idx_a, 0);
            assert_eq!(idx_b, 1);
        }
        let silo = DocSilo::open(dir.path()).unwrap();
        assert_eq!(silo.field_to_idx().get("nsfwLevel"), Some(&0));
        assert_eq!(silo.field_to_idx().get("tagIds"), Some(&1));
    }

    // ── Multi-phase dump (DumpMergeWriter) ─────────────────────────

    /// Merge two encoded snapshots: decode both, combine fields (new overwrites
    /// existing by field_idx), re-encode. This is the merge function for
    /// DumpMergeWriter during multi-phase dumps.
    fn merge_encoded_snapshots(existing: &[u8], new: &[u8]) -> Vec<u8> {
        use std::collections::HashMap;
        let existing_snap = SlotSnapshotCodec::decode(existing).unwrap_or_else(|_| SlotSnapshot::empty());
        let new_snap = SlotSnapshotCodec::decode(new).unwrap_or_else(|_| SlotSnapshot::empty());
        // Build merged field map: existing fields + new fields (new wins on conflict)
        let mut merged: HashMap<u16, PackedValue> = HashMap::new();
        for (idx, val) in &existing_snap.fields {
            merged.insert(*idx, val.clone());
        }
        for (idx, val) in &new_snap.fields {
            merged.insert(*idx, val.clone());
        }
        let fields: Vec<(u16, PackedValue)> = merged.into_iter().collect();
        let mut buf = Vec::new();
        encode_slot_bytes(true, &fields, &mut buf);
        buf
    }

    #[test]
    fn multi_phase_images_then_tags() {
        use crate::doc_silo::slot_to_key;

        let dir = tempfile::tempdir().unwrap();
        let silo_path = dir.path();

        // Phase 1: write image docs via put (simulating bulk load)
        {
            let mut silo = DocSilo::open(silo_path).unwrap();
            silo.put(1, &make_doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(8))),
                ("sortAt", FieldValue::Single(Value::Integer(1000))),
            ])).unwrap();
            silo.put(2, &make_doc(&[
                ("nsfwLevel", FieldValue::Single(Value::Integer(4))),
                ("sortAt", FieldValue::Single(Value::Integer(2000))),
            ])).unwrap();
            silo.save_field_dict().unwrap();
            // Compact so data is in data.bin (not just ops log)
            silo.compact().unwrap();
        }

        // Phase 2: merge tags into existing docs via DumpMergeWriter
        {
            let mut silo = DocSilo::open(silo_path).unwrap();
            let tag_idx = silo.ensure_field_index("tagIds");
            silo.save_field_dict().unwrap();

            let mut writer = silo.prepare_dump_merge().unwrap()
                .expect("should have data after phase 1");

            // Encode tag fields as snapshot bytes
            let mut buf1 = Vec::new();
            encode_slot_bytes(true, &[
                (tag_idx, PackedValue::Mi(vec![10, 20, 30])),
            ], &mut buf1);

            let mut buf2 = Vec::new();
            encode_slot_bytes(true, &[
                (tag_idx, PackedValue::Mi(vec![40, 50])),
            ], &mut buf2);

            // merge_put: read existing doc, combine fields, write back
            assert!(writer.merge_put(slot_to_key(1), &buf1, merge_encoded_snapshots));
            assert!(writer.merge_put(slot_to_key(2), &buf2, merge_encoded_snapshots));

            writer.flush().unwrap();
            drop(writer);
            silo.reload_data().unwrap();

            // Verify: get returns doc with BOTH image fields AND tagIds
            let d1 = silo.get(1).unwrap().unwrap();
            assert_eq!(d1.fields.get("nsfwLevel"), Some(&FieldValue::Single(Value::Integer(8))));
            assert_eq!(d1.fields.get("sortAt"), Some(&FieldValue::Single(Value::Integer(1000))));
            match d1.fields.get("tagIds") {
                Some(FieldValue::Multi(vs)) => {
                    let ints: Vec<i64> = vs.iter().filter_map(|v| {
                        if let Value::Integer(i) = v { Some(*i) } else { None }
                    }).collect();
                    assert!(ints.contains(&10) && ints.contains(&20) && ints.contains(&30),
                        "expected tagIds [10,20,30], got {:?}", ints);
                }
                other => panic!("expected Multi tagIds, got {:?}", other),
            }

            let d2 = silo.get(2).unwrap().unwrap();
            assert_eq!(d2.fields.get("nsfwLevel"), Some(&FieldValue::Single(Value::Integer(4))));
            match d2.fields.get("tagIds") {
                Some(FieldValue::Multi(vs)) => {
                    let ints: Vec<i64> = vs.iter().filter_map(|v| {
                        if let Value::Integer(i) = v { Some(*i) } else { None }
                    }).collect();
                    assert!(ints.contains(&40) && ints.contains(&50),
                        "expected tagIds [40,50], got {:?}", ints);
                }
                other => panic!("expected Multi tagIds, got {:?}", other),
            }
        }
    }

    #[test]
    fn multi_phase_no_data_loss() {
        use crate::doc_silo::slot_to_key;

        let dir = tempfile::tempdir().unwrap();
        let silo_path = dir.path();

        // Phase 1: write 100 docs
        {
            let mut silo = DocSilo::open(silo_path).unwrap();
            for i in 0..100u32 {
                silo.put(i, &make_doc(&[
                    ("nsfwLevel", FieldValue::Single(Value::Integer(i as i64))),
                ])).unwrap();
            }
            silo.save_field_dict().unwrap();
            silo.compact().unwrap();
        }

        // Phase 2: merge extra field into all 100
        {
            let mut silo = DocSilo::open(silo_path).unwrap();
            let extra_idx = silo.ensure_field_index("extra");
            silo.save_field_dict().unwrap();

            let mut writer = silo.prepare_dump_merge().unwrap().unwrap();
            for i in 0..100u32 {
                let mut buf = Vec::new();
                encode_slot_bytes(true, &[
                    (extra_idx, PackedValue::I(i as i64 * 100)),
                ], &mut buf);
                assert!(writer.merge_put(slot_to_key(i), &buf, merge_encoded_snapshots),
                    "merge_put failed for slot {i}");
            }
            writer.flush().unwrap();
            drop(writer);
            silo.reload_data().unwrap();

            // Verify: all 100 docs have BOTH nsfwLevel AND extra
            let keys: Vec<u32> = (0..100).collect();
            let results = silo.get_many(&keys).unwrap();
            for (i, opt) in results.iter().enumerate() {
                let doc = opt.as_ref().unwrap_or_else(|| panic!("doc {i} missing"));
                assert_eq!(doc.fields.get("nsfwLevel"),
                    Some(&FieldValue::Single(Value::Integer(i as i64))),
                    "doc {i} nsfwLevel wrong");
                assert_eq!(doc.fields.get("extra"),
                    Some(&FieldValue::Single(Value::Integer(i as i64 * 100))),
                    "doc {i} extra wrong");
            }
        }
    }
}
