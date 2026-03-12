//! Filesystem-based document store with packed shard files.
//!
//! Documents are grouped into shard files by slot ID range:
//! ```text
//! docs/meta/field_dict.bin     # field name ↔ u16 dictionary
//! docs/shards/000000.bin       # slot_ids 0..16383
//! docs/shards/000001.bin       # slot_ids 16384..32767
//! ```
//!
//! Each shard file contains a sorted index table + concatenated compressed docs:
//! ```text
//! [u32 version=1][u32 num_entries]
//! [index: N × (u32 slot_id, u32 data_offset, u32 data_length)]
//! [data: compressed doc bytes...]
//! ```
//!
//! Read: binary search index for slot_id, decompress at offset.
//! Batch write: group by shard, write each shard file.
//! At 105M records with 16K/shard = 6400 files (vs 105M individual files).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::config::{DataSchema, FieldMapping, FieldValueType};
use crate::error::{BitdexError, Result};
use crate::mutation::FieldValue;
use crate::query::Value;

/// Number of bits to shift slot_id right to get shard index.
/// 9 → 512 docs per shard → ~75KB per shard compressed, ~205K shards at 105M records.
const SHARD_SHIFT: u32 = 9;

/// Shard file version. Bump if format changes.
const SHARD_VERSION: u32 = 1;

/// A stored document containing all field values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredDoc {
    pub fields: HashMap<String, FieldValue>,
}

/// Filesystem-based document store with packed shard files.
pub struct DocStore {
    root: PathBuf,
    field_to_idx: HashMap<String, u16>,
    idx_to_field: Vec<String>,
    in_memory: bool,
    memory_store: HashMap<u32, Vec<u8>>,
}

impl DocStore {
    /// Open a docstore at the given directory.
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path.join("meta"))
            .map_err(|e| BitdexError::DocStore(format!("create docs dir: {e}")))?;
        std::fs::create_dir_all(path.join("shards"))
            .map_err(|e| BitdexError::DocStore(format!("create shards dir: {e}")))?;

        let (field_to_idx, idx_to_field) = Self::load_field_dict_from_dir(path)?;

        Ok(Self {
            root: path.to_path_buf(),
            field_to_idx,
            idx_to_field,
            in_memory: false,
            memory_store: HashMap::new(),
        })
    }

    /// Open a docstore using an in-memory backend (for testing).
    pub fn open_temp() -> Result<Self> {
        Ok(Self {
            root: PathBuf::new(),
            field_to_idx: HashMap::new(),
            idx_to_field: Vec::new(),
            in_memory: true,
            memory_store: HashMap::new(),
        })
    }

    /// Get the root path of this docstore.
    pub fn path(&self) -> &Path {
        &self.root
    }

    // ---- Shard path helpers ----

    pub(crate) fn shard_id(slot_id: u32) -> u32 {
        slot_id >> SHARD_SHIFT
    }

    pub(crate) fn shard_path(root: &Path, shard_id: u32) -> PathBuf {
        // Nest into hex subdirectories: shards/AB/000123.bin
        // Top byte of shard_id → 256 dirs, keeps each dir under ~1000 files at any scale.
        let dir_byte = ((shard_id >> 8) & 0xFF) as u8;
        root.join("shards")
            .join(format!("{:02x}", dir_byte))
            .join(format!("{:06}.bin", shard_id))
    }

    // ---- Field dictionary persistence ----

    fn dict_path(root: &Path) -> PathBuf {
        root.join("meta").join("field_dict.bin")
    }

    fn load_field_dict_from_dir(root: &Path) -> Result<(HashMap<String, u16>, Vec<String>)> {
        let path = Self::dict_path(root);
        match std::fs::read(&path) {
            Ok(data) => {
                let names: Vec<String> = rmp_serde::from_slice(&data)
                    .map_err(|e| BitdexError::DocStore(format!("field dict decode: {e}")))?;
                let map: HashMap<String, u16> = names
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (n.clone(), i as u16))
                    .collect();
                Ok((map, names))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok((HashMap::new(), Vec::new()))
            }
            Err(e) => Err(BitdexError::DocStore(format!("read field dict: {e}"))),
        }
    }

    fn save_field_dict(&self) -> Result<()> {
        if self.in_memory {
            return Ok(());
        }
        let bytes = rmp_serde::to_vec(&self.idx_to_field)
            .map_err(|e| BitdexError::DocStore(format!("field dict encode: {e}")))?;
        let path = Self::dict_path(&self.root);
        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, &bytes)
            .map_err(|e| BitdexError::DocStore(format!("write field dict: {e}")))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| BitdexError::DocStore(format!("rename field dict: {e}")))?;
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

    // ---- Shard file I/O ----

    /// Read the index table from a shard file. Returns sorted (slot_id, offset, length) entries.
    fn read_shard_index(data: &[u8]) -> Result<Vec<(u32, u32, u32)>> {
        if data.len() < 8 {
            return Err(BitdexError::DocStore("shard too short".into()));
        }
        let version = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if version != SHARD_VERSION {
            return Err(BitdexError::DocStore(format!("unknown shard version {version}")));
        }
        let num = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let index_start = 8;
        let index_bytes = num * 12;
        if data.len() < index_start + index_bytes {
            return Err(BitdexError::DocStore("shard index truncated".into()));
        }
        let mut entries = Vec::with_capacity(num);
        for i in 0..num {
            let base = index_start + i * 12;
            let slot_id = u32::from_le_bytes([data[base], data[base + 1], data[base + 2], data[base + 3]]);
            let offset = u32::from_le_bytes([data[base + 4], data[base + 5], data[base + 6], data[base + 7]]);
            let length = u32::from_le_bytes([data[base + 8], data[base + 9], data[base + 10], data[base + 11]]);
            entries.push((slot_id, offset, length));
        }
        Ok(entries)
    }

    /// Data section starts after the header + index table.
    fn data_section_offset(num_entries: usize) -> usize {
        8 + num_entries * 12
    }

    /// Look up a single doc's raw bytes from a shard file buffer.
    /// Decompresses the entire shard data section, then extracts the doc.
    fn find_in_shard(file_data: &[u8], slot_id: u32) -> Result<Option<Vec<u8>>> {
        let (entries, decompressed) = Self::read_shard_file(file_data)?;
        match entries.binary_search_by_key(&slot_id, |e| e.0) {
            Ok(idx) => {
                let (_, offset, length) = entries[idx];
                let start = offset as usize;
                let end = start + length as usize;
                if end > decompressed.len() {
                    return Err(BitdexError::DocStore("shard data truncated".into()));
                }
                Ok(Some(decompressed[start..end].to_vec()))
            }
            Err(_) => Ok(None),
        }
    }

    /// Write a complete shard file from a sorted list of (slot_id, raw_bytes).
    /// The data section is zstd-compressed as a single block for efficiency.
    pub(crate) fn write_shard_file(path: &Path, entries: &[(u32, Vec<u8>)]) -> Result<()> {
        // Concatenate all raw doc bytes
        let total_raw: usize = entries.iter().map(|(_, d)| d.len()).sum();
        let mut raw_data = Vec::with_capacity(total_raw);
        let mut offsets: Vec<(u32, u32, u32)> = Vec::with_capacity(entries.len()); // (slot_id, offset, length)
        let mut data_offset: u32 = 0;
        for (slot_id, doc_bytes) in entries {
            offsets.push((*slot_id, data_offset, doc_bytes.len() as u32));
            raw_data.extend_from_slice(doc_bytes);
            data_offset += doc_bytes.len() as u32;
        }

        // Compress entire data section as one block
        let compressed_data = zstd::encode_all(raw_data.as_slice(), 1)
            .map_err(|e| BitdexError::DocStore(format!("zstd compress shard: {e}")))?;

        // Build file: header + index + compressed_data
        let header_size = 8 + entries.len() * 12 + 4; // +4 for uncompressed size
        let mut buf = Vec::with_capacity(header_size + compressed_data.len());

        // Header
        buf.extend_from_slice(&SHARD_VERSION.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        // Index table (offsets into the UNCOMPRESSED data)
        for (slot_id, offset, length) in &offsets {
            buf.extend_from_slice(&slot_id.to_le_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&length.to_le_bytes());
        }

        // Uncompressed data size (needed for decompression)
        buf.extend_from_slice(&(raw_data.len() as u32).to_le_bytes());

        // Compressed data section
        buf.extend_from_slice(&compressed_data);

        // Ensure parent directory exists (hex-nested shard dirs)
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BitdexError::DocStore(format!("create shard dir: {e}")))?;
        }

        // Atomic write
        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, &buf)
            .map_err(|e| BitdexError::DocStore(format!("write shard: {e}")))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| BitdexError::DocStore(format!("rename shard: {e}")))?;
        Ok(())
    }

    /// Read and decompress a shard file, returning (index_entries, decompressed_data).
    pub(crate) fn read_shard_file(data: &[u8]) -> Result<(Vec<(u32, u32, u32)>, Vec<u8>)> {
        let entries = Self::read_shard_index(data)?;
        let index_end = Self::data_section_offset(entries.len());

        // Read uncompressed size
        if data.len() < index_end + 4 {
            return Err(BitdexError::DocStore("shard missing uncompressed size".into()));
        }
        let _uncompressed_size = u32::from_le_bytes([
            data[index_end], data[index_end + 1], data[index_end + 2], data[index_end + 3],
        ]);

        // Decompress data section
        let compressed = &data[index_end + 4..];
        let decompressed = zstd::decode_all(compressed)
            .map_err(|e| BitdexError::DocStore(format!("zstd decompress shard: {e}")))?;

        Ok((entries, decompressed))
    }

    // ---- Encoding ----

    fn encode_doc(&mut self, doc: &StoredDoc) -> Result<Vec<u8>> {
        let mut dict_changed = false;
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            let old_len = self.idx_to_field.len();
            let idx = self.ensure_field_idx(name);
            if self.idx_to_field.len() > old_len {
                dict_changed = true;
            }
            pairs.push((idx, pack_field_value(fv)));
        }
        if dict_changed {
            self.save_field_dict()?;
        }
        rmp_serde::to_vec(&pairs)
            .map_err(|e| BitdexError::DocStore(format!("msgpack encode: {e}")))
    }

    fn encode_doc_readonly(&self, doc: &StoredDoc) -> Result<Vec<u8>> {
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            if let Some(&idx) = self.field_to_idx.get(name.as_str()) {
                pairs.push((idx, pack_field_value(fv)));
            }
        }
        rmp_serde::to_vec(&pairs)
            .map_err(|e| BitdexError::DocStore(format!("msgpack encode: {e}")))
    }

    fn decode_doc(&self, raw: &[u8]) -> Result<StoredDoc> {
        let pairs: Vec<(u16, PackedValue)> = rmp_serde::from_slice(raw)
            .map_err(|e| BitdexError::DocStore(format!("msgpack decode: {e}")))?;
        let mut fields = HashMap::with_capacity(pairs.len());
        for (idx, pv) in pairs {
            if let Some(name) = self.idx_to_field.get(idx as usize) {
                fields.insert(name.clone(), unpack_field_value(pv));
            }
        }
        Ok(StoredDoc { fields })
    }

    // ---- Public API ----

    /// Get a stored document by slot ID.
    pub fn get(&self, id: u32) -> Result<Option<StoredDoc>> {
        if self.in_memory {
            return match self.memory_store.get(&id) {
                Some(data) => Ok(Some(self.decode_doc(data)?)),
                None => Ok(None),
            };
        }
        let path = Self::shard_path(&self.root, Self::shard_id(id));
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
        };
        match Self::find_in_shard(&data, id)? {
            Some(compressed) => Ok(Some(self.decode_doc(&compressed)?)),
            None => Ok(None),
        }
    }

    /// Read all documents from a single shard, decoded.
    ///
    /// Decompresses the shard once and returns all (slot_id, StoredDoc) pairs.
    /// Much faster than calling `get()` for each slot when you need all docs in a shard.
    pub fn get_shard(&self, shard_id: u32) -> Result<Vec<(u32, StoredDoc)>> {
        if self.in_memory {
            let start = shard_id << SHARD_SHIFT;
            let end = start + (1 << SHARD_SHIFT);
            let mut out = Vec::new();
            for slot in start..end {
                if let Some(data) = self.memory_store.get(&slot) {
                    out.push((slot, self.decode_doc(data)?));
                }
            }
            return Ok(out);
        }
        let path = Self::shard_path(&self.root, shard_id);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
        };
        let (entries, decompressed) = Self::read_shard_file(&data)?;
        let mut out = Vec::with_capacity(entries.len());
        for (slot_id, offset, length) in &entries {
            let start = *offset as usize;
            let end = start + *length as usize;
            if end > decompressed.len() {
                continue;
            }
            out.push((*slot_id, self.decode_doc(&decompressed[start..end])?));
        }
        Ok(out)
    }

    /// Store a single document. Reads the existing shard, merges, and rewrites.
    pub fn put(&mut self, id: u32, doc: &StoredDoc) -> Result<()> {
        let raw_bytes = self.encode_doc(doc)?;
        if self.in_memory {
            self.memory_store.insert(id, raw_bytes);
            return Ok(());
        }

        let sid = Self::shard_id(id);
        let path = Self::shard_path(&self.root, sid);

        // Load existing entries for this shard
        let mut entries: Vec<(u32, Vec<u8>)> = match std::fs::read(&path) {
            Ok(file_data) => {
                let (index, decompressed) = Self::read_shard_file(&file_data)?;
                index.iter().filter(|(s, _, _)| *s != id).map(|(s, off, len)| {
                    let start = *off as usize;
                    (*s, decompressed[start..start + *len as usize].to_vec())
                }).collect()
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
        };

        // Insert new entry in sorted position
        let pos = entries.binary_search_by_key(&id, |e| e.0).unwrap_or_else(|p| p);
        entries.insert(pos, (id, raw_bytes));

        Self::write_shard_file(&path, &entries)
    }

    /// Store multiple documents. Groups by shard and writes each shard file once.
    pub fn put_batch(&mut self, docs: &[(u32, StoredDoc)]) -> Result<()> {
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

        if self.in_memory {
            for (id, doc) in docs {
                let compressed = self.encode_doc_readonly(doc)?;
                self.memory_store.insert(*id, compressed);
            }
            return Ok(());
        }

        // Encode all docs and group by shard
        let mut by_shard: HashMap<u32, Vec<(u32, Vec<u8>)>> = HashMap::new();
        for (id, doc) in docs {
            let compressed = self.encode_doc_readonly(doc)?;
            by_shard.entry(Self::shard_id(*id)).or_default().push((*id, compressed));
        }

        // Write each shard
        for (sid, mut new_entries) in by_shard {
            let path = Self::shard_path(&self.root, sid);

            // Load existing entries
            let mut entries: Vec<(u32, Vec<u8>)> = match std::fs::read(&path) {
                Ok(file_data) => {
                    let (index, decompressed) = Self::read_shard_file(&file_data)?;
                    let new_ids: std::collections::HashSet<u32> = new_entries.iter().map(|(id, _)| *id).collect();
                    index.iter().filter(|(s, _, _)| !new_ids.contains(s)).map(|(s, off, len)| {
                        let start = *off as usize;
                        (*s, decompressed[start..start + *len as usize].to_vec())
                    }).collect()
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
            };

            // Merge and sort
            entries.append(&mut new_entries);
            entries.sort_by_key(|e| e.0);

            Self::write_shard_file(&path, &entries)?;
        }

        Ok(())
    }

    /// Delete a document by slot ID. Rewrites the shard without the entry.
    pub fn delete(&self, id: u32) -> Result<()> {
        if self.in_memory {
            return Ok(());
        }
        let sid = Self::shard_id(id);
        let path = Self::shard_path(&self.root, sid);
        let file_data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
        };
        let (index, decompressed) = Self::read_shard_file(&file_data)?;
        let entries: Vec<(u32, Vec<u8>)> = index.iter()
            .filter(|(s, _, _)| *s != id)
            .map(|(s, off, len)| {
                let start = *off as usize;
                (*s, decompressed[start..start + *len as usize].to_vec())
            })
            .collect();
        if entries.is_empty() {
            // Remove empty shard file
            let _ = std::fs::remove_file(&path);
        } else {
            Self::write_shard_file(&path, &entries)?;
        }
        Ok(())
    }

    /// No-op for filesystem store.
    pub fn compact(&mut self) -> Result<bool> {
        Ok(false)
    }

    /// Prepare for bulk loading: ensure field dictionary contains all field names,
    /// then return a BulkWriter that can encode and write docs without the DocStore lock.
    pub fn prepare_bulk_load(&mut self, field_names: &[String]) -> Result<BulkWriter> {
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
        Ok(BulkWriter {
            field_to_idx: self.field_to_idx.clone(),
            root: self.root.clone(),
            shard_locks: Arc::new(DashMap::new()),
        })
    }
}

// ---------------------------------------------------------------------------
// BulkWriter — lock-free parallel docstore writes for bulk loading
// ---------------------------------------------------------------------------

/// Lock-free docstore writer for bulk loading.
///
/// Created by `DocStore::prepare_bulk_load()`. Holds a snapshot of the field
/// dictionary and the docstore root path. Multiple threads can call
/// `write_batch()` concurrently — per-shard locking ensures boundary shards
/// (where consecutive blocks share a shard) are handled correctly.
pub struct BulkWriter {
    field_to_idx: HashMap<String, u16>,
    root: PathBuf,
    /// Per-shard locks: only contended at block boundaries (~1 shard per block).
    /// Most shards are written by exactly one thread → zero contention.
    shard_locks: Arc<DashMap<u32, parking_lot::Mutex<()>>>,
}

impl BulkWriter {
    /// Write pre-encoded docs to shard files. Pure I/O — no CPU-bound encoding.
    /// Docs are already msgpack bytes from the parse stage.
    pub fn write_batch_encoded(&self, encoded: Vec<(u32, Vec<u8>)>) {
        if encoded.is_empty() {
            return;
        }

        // Group by shard
        let mut by_shard: HashMap<u32, Vec<(u32, Vec<u8>)>> = HashMap::new();
        for (slot, bytes) in encoded {
            by_shard
                .entry(DocStore::shard_id(slot))
                .or_default()
                .push((slot, bytes));
        }

        // Parallel write shard files with per-shard locking
        let shards: Vec<(u32, Vec<(u32, Vec<u8>)>)> = by_shard.into_iter().collect();
        shards.into_par_iter().for_each(|(sid, mut new_entries)| {
            new_entries.sort_by_key(|e| e.0);

            // Per-shard lock: prevents concurrent writers from clobbering
            // the same shard file (only happens at block boundaries).
            self.shard_locks
                .entry(sid)
                .or_insert_with(|| parking_lot::Mutex::new(()));
            let shard_lock = self.shard_locks.get(&sid).unwrap();
            let _guard = shard_lock.lock();

            let path = DocStore::shard_path(&self.root, sid);

            // Read existing entries (if any) and merge
            let mut entries: Vec<(u32, Vec<u8>)> = match std::fs::read(&path) {
                Ok(file_data) => {
                    match DocStore::read_shard_file(&file_data) {
                        Ok((index, decompressed)) => {
                            let new_ids: std::collections::HashSet<u32> =
                                new_entries.iter().map(|(id, _)| *id).collect();
                            index
                                .iter()
                                .filter(|(s, _, _)| !new_ids.contains(s))
                                .map(|(s, off, len)| {
                                    let start = *off as usize;
                                    (*s, decompressed[start..start + *len as usize].to_vec())
                                })
                                .collect()
                        }
                        Err(_) => Vec::new(), // corrupted shard, overwrite
                    }
                }
                Err(_) => Vec::new(), // new shard
            };

            entries.append(&mut new_entries);
            entries.sort_by_key(|e| e.0);

            if let Err(e) = DocStore::write_shard_file(&path, &entries) {
                eprintln!("BulkWriter: shard {} write failed: {e}", sid);
            }
        });
    }

    /// Encode a StoredDoc to msgpack bytes using the snapshotted field dictionary.
    pub fn encode_doc(&self, doc: &StoredDoc) -> Vec<u8> {
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            if let Some(&idx) = self.field_to_idx.get(name.as_str()) {
                pairs.push((idx, pack_field_value(fv)));
            }
        }
        rmp_serde::to_vec(&pairs).unwrap_or_default()
    }

    /// Encode a JSON value directly to msgpack bytes using the DataSchema.
    /// Skips the intermediate StoredDoc/HashMap allocation entirely —
    /// walks schema fields once, converts JSON → PackedValue → msgpack.
    pub fn encode_json(&self, json: &serde_json::Value, schema: &DataSchema) -> Vec<u8> {
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
                        pairs.push((idx, PackedValue::B(false)));
                    }
                    continue;
                }
            };

            if let Some(pv) = json_to_packed(raw, mapping, apply_ms) {
                pairs.push((idx, pv));
            }
        }

        rmp_serde::to_vec(&pairs).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Compact value encoding
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize)]
enum PackedValue {
    I(i64),
    F(f64),
    B(bool),
    S(String),
    Mi(Vec<i64>),
    Mm(Vec<PackedValue>),
}

fn pack_field_value(fv: &FieldValue) -> PackedValue {
    match fv {
        FieldValue::Single(v) => pack_value(v),
        FieldValue::Multi(vs) => {
            if vs.iter().all(|v| matches!(v, Value::Integer(_))) {
                PackedValue::Mi(
                    vs.iter()
                        .map(|v| match v {
                            Value::Integer(i) => *i,
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            } else {
                PackedValue::Mm(vs.iter().map(pack_value).collect())
            }
        }
    }
}

fn pack_value(v: &Value) -> PackedValue {
    match v {
        Value::Integer(i) => PackedValue::I(*i),
        Value::Float(f) => PackedValue::F(*f),
        Value::Bool(b) => PackedValue::B(*b),
        Value::String(s) => PackedValue::S(s.clone()),
    }
}

/// Convert a raw JSON value directly to PackedValue based on field mapping type.
/// Skips FieldValue intermediate — JSON → PackedValue in one step.
fn json_to_packed(raw: &serde_json::Value, mapping: &FieldMapping, ms_to_seconds: bool) -> Option<PackedValue> {
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
        FieldValueType::IntegerArray => {
            let arr = raw.as_array()?;
            if arr.is_empty() {
                return None;
            }
            let values: Vec<i64> = arr
                .iter()
                .filter_map(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)))
                .collect();
            if values.is_empty() {
                None
            } else {
                Some(PackedValue::Mi(values))
            }
        }
        FieldValueType::ExistsBoolean => Some(PackedValue::B(true)),
    }
}

fn unpack_field_value(pv: PackedValue) -> FieldValue {
    match pv {
        PackedValue::I(i) => FieldValue::Single(Value::Integer(i)),
        PackedValue::F(f) => FieldValue::Single(Value::Float(f)),
        PackedValue::B(b) => FieldValue::Single(Value::Bool(b)),
        PackedValue::S(s) => FieldValue::Single(Value::String(s)),
        PackedValue::Mi(is) => FieldValue::Multi(is.into_iter().map(Value::Integer).collect()),
        PackedValue::Mm(pvs) => FieldValue::Multi(
            pvs.into_iter()
                .map(|pv| match unpack_field_value(pv) {
                    FieldValue::Single(v) => v,
                    _ => Value::Integer(0),
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut store = DocStore::open_temp().unwrap();
        let doc = StoredDoc {
            fields: vec![
                ("nsfwLevel".to_string(), FieldValue::Single(Value::Integer(1))),
                (
                    "tagIds".to_string(),
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                ),
            ]
            .into_iter()
            .collect(),
        };
        store.put(42, &doc).unwrap();
        let got = store.get(42).unwrap().unwrap();
        assert_eq!(got.fields.len(), 2);
        match &got.fields["nsfwLevel"] {
            FieldValue::Single(Value::Integer(1)) => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_get_missing() {
        let store = DocStore::open_temp().unwrap();
        assert!(store.get(999).unwrap().is_none());
    }

    #[test]
    fn test_batch_put() {
        let mut store = DocStore::open_temp().unwrap();
        let docs: Vec<(u32, StoredDoc)> = (0..100)
            .map(|i| {
                let doc = StoredDoc {
                    fields: vec![
                        ("id".to_string(), FieldValue::Single(Value::Integer(i as i64))),
                        ("url".to_string(), FieldValue::Single(Value::String(format!("guid-{}", i)))),
                    ]
                    .into_iter()
                    .collect(),
                };
                (i, doc)
            })
            .collect();
        store.put_batch(&docs).unwrap();
        let got = store.get(50).unwrap().unwrap();
        assert_eq!(got.fields.len(), 2);
    }

    #[test]
    fn test_filesystem_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        {
            let mut store = DocStore::open(&docs_dir).unwrap();
            let doc = StoredDoc {
                fields: vec![
                    ("x".to_string(), FieldValue::Single(Value::Integer(42))),
                    ("name".to_string(), FieldValue::Single(Value::String("test".into()))),
                ]
                .into_iter()
                .collect(),
            };
            store.put(100, &doc).unwrap();
        }
        // Reopen and verify
        let store2 = DocStore::open(&docs_dir).unwrap();
        let got = store2.get(100).unwrap().unwrap();
        match &got.fields["x"] {
            FieldValue::Single(Value::Integer(42)) => {}
            other => panic!("unexpected: {:?}", other),
        }
        match &got.fields["name"] {
            FieldValue::Single(Value::String(s)) => assert_eq!(s, "test"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_filesystem_batch_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        {
            let mut store = DocStore::open(&docs_dir).unwrap();
            let docs: Vec<(u32, StoredDoc)> = (0..50)
                .map(|i| {
                    let doc = StoredDoc {
                        fields: vec![("val".to_string(), FieldValue::Single(Value::Integer(i as i64)))]
                            .into_iter()
                            .collect(),
                    };
                    (i, doc)
                })
                .collect();
            store.put_batch(&docs).unwrap();
        }
        // Reopen and verify
        let store2 = DocStore::open(&docs_dir).unwrap();
        for i in 0..50u32 {
            let got = store2.get(i).unwrap().unwrap();
            match &got.fields["val"] {
                FieldValue::Single(Value::Integer(v)) => assert_eq!(*v, i as i64),
                other => panic!("unexpected for {}: {:?}", i, other),
            }
        }
    }

    #[test]
    fn test_delete() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        let doc = StoredDoc {
            fields: vec![("x".to_string(), FieldValue::Single(Value::Integer(1)))].into_iter().collect(),
        };
        store.put(5, &doc).unwrap();
        assert!(store.get(5).unwrap().is_some());
        store.delete(5).unwrap();
        assert!(store.get(5).unwrap().is_none());
    }

    #[test]
    fn test_shard_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        // Write docs spanning two shards
        let boundary: u32 = 1 << SHARD_SHIFT;
        let docs: Vec<(u32, StoredDoc)> = vec![boundary - 1, boundary, boundary + 1]
            .into_iter()
            .map(|i| {
                let doc = StoredDoc {
                    fields: vec![("id".to_string(), FieldValue::Single(Value::Integer(i as i64)))]
                        .into_iter()
                        .collect(),
                };
                (i, doc)
            })
            .collect();
        store.put_batch(&docs).unwrap();
        // Verify each is in correct shard and readable
        for id in [boundary - 1, boundary, boundary + 1] {
            let got = store.get(id).unwrap().unwrap();
            match &got.fields["id"] {
                FieldValue::Single(Value::Integer(v)) => assert_eq!(*v, id as i64),
                other => panic!("unexpected for {}: {:?}", id, other),
            }
        }
    }
}
