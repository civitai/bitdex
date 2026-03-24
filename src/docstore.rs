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
use std::io::{BufWriter, Write};
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

/// Public accessor for SHARD_SHIFT (used by slot_arena finalization).
pub const SHARD_SHIFT_PUB: u32 = SHARD_SHIFT;

/// Shard file version. Bump if format changes.
const SHARD_VERSION: u32 = 1;

/// V2 shard magic number: "BDX2" in little-endian.
const V2_MAGIC: u32 = 0x42445832;

/// V2 shard header size in bytes: magic(4) + version(4) + flags(4) + num_tuples(4).
const V2_HEADER_SIZE: usize = 16;

/// Marker byte indicating the document has a version prefix.
/// Chosen as 0x00 because msgpack arrays (our doc format) always start at 0x90+.
const DOC_VERSION_MARKER: u8 = 0x00;

/// Stale tuple percentage threshold for triggering reader-driven compaction.
/// When stale_count * 100 / total_count exceeds this, a compaction is enqueued.
/// Default compaction threshold (percentage). Overridden by Config.compact_threshold_pct.
const DEFAULT_COMPACT_THRESHOLD_PCT: u64 = 30;

/// A stored document containing all field values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredDoc {
    pub fields: HashMap<String, FieldValue>,
    /// Schema version this document was encoded with.
    /// Used to select correct defaults when reading elided fields.
    /// 0 = legacy (pre-versioning), 1+ = versioned.
    #[serde(skip, default)]
    pub schema_version: u8,
}

/// Filesystem-based document store with packed shard files.
pub struct DocStore {
    root: PathBuf,
    field_to_idx: HashMap<String, u16>,
    idx_to_field: Vec<String>,
    in_memory: bool,
    memory_store: HashMap<u32, Vec<u8>>,
    /// Per-field default values keyed by field dict index.
    /// Fields matching their default are elided on write to save space.
    field_defaults: HashMap<u16, PackedValue>,
    /// Current schema version. Prepended to every encoded document.
    schema_version: u8,
    /// Historical defaults keyed by schema version.
    /// Used to reconstruct elided fields from documents encoded with older schemas.
    historical_defaults: HashMap<u8, HashMap<u16, PackedValue>>,
    /// Per-shard buffered writers for V2 append-only tuple format.
    /// Lazily opened on first append to each shard.
    v2_writers: Arc<DashMap<u32, parking_lot::Mutex<BufWriter<std::fs::File>>>>,
    /// Channel for sending (shard_id, raw_data) to a background compaction worker.
    /// Set via `set_compact_channel()`. When a reader detects stale tuples
    /// exceeding compact_threshold_pct, it fire-and-forgets the buffer here.
    compact_tx: Option<crossbeam_channel::Sender<(u32, Vec<u8>)>>,
    /// Compaction threshold: percentage of stale tuples that triggers compaction.
    /// 0 = disabled (no staleness tracking, no compaction worker).
    /// Default: 30.
    compact_threshold_pct: u64,
    /// Counter incremented when a compaction is skipped due to channel backpressure.
    compact_skipped: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl DocStore {
    /// Open a docstore at the given directory.
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path.join("meta"))
            .map_err(|e| BitdexError::DocStore(format!("create docs dir: {e}")))?;
        std::fs::create_dir_all(path.join("shards"))
            .map_err(|e| BitdexError::DocStore(format!("create shards dir: {e}")))?;

        let (field_to_idx, idx_to_field) = Self::load_field_dict_from_dir(path)?;

        let historical_defaults = Self::load_schema_history(path);

        // Clean up orphaned .bin.tmp files from interrupted compactions
        let shards_dir = path.join("shards");
        if shards_dir.exists() {
            Self::cleanup_tmp_files(&shards_dir);
        }

        Ok(Self {
            root: path.to_path_buf(),
            field_to_idx,
            idx_to_field,
            in_memory: false,
            memory_store: HashMap::new(),
            field_defaults: HashMap::new(),
            schema_version: 1,
            historical_defaults,
            v2_writers: Arc::new(DashMap::new()),
            compact_tx: None,
            compact_threshold_pct: DEFAULT_COMPACT_THRESHOLD_PCT,
            compact_skipped: None,
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
            field_defaults: HashMap::new(),
            schema_version: 1,
            historical_defaults: HashMap::new(),
            v2_writers: Arc::new(DashMap::new()),
            compact_tx: None,
            compact_threshold_pct: DEFAULT_COMPACT_THRESHOLD_PCT,
            compact_skipped: None,
        })
    }

    /// Get the root path of this docstore.
    pub fn path(&self) -> &Path {
        &self.root
    }

    // ---- Accessors for lock-free compaction ----

    /// Root path of the docstore directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Shared handle to per-shard V2 writers (DashMap, safe without full DocStore lock).
    pub fn v2_writers_handle(&self) -> Arc<DashMap<u32, parking_lot::Mutex<BufWriter<std::fs::File>>>> {
        Arc::clone(&self.v2_writers)
    }

    /// Recursively remove orphaned `.bin.tmp` files from a directory tree.
    /// These are left behind by interrupted compactions (atomic rename failed).
    fn cleanup_tmp_files(dir: &Path) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    Self::cleanup_tmp_files(&p);
                } else if p.extension().and_then(|e| e.to_str()) == Some("tmp") {
                    let _ = std::fs::remove_file(&p);
                }
            }
        }
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
        std::fs::OpenOptions::new().write(true).open(&tmp)
            .map_err(|e| BitdexError::DocStore(format!("open field dict for fsync: {e}")))?
            .sync_all()
            .map_err(|e| BitdexError::DocStore(format!("fsync field dict: {e}")))?;
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

    /// Build the field_defaults map from a DataSchema.
    /// Must be called after field dictionary is populated (i.e., after prepare_bulk_load
    /// or after fields have been ensured). For null defaults, we treat them as
    /// "elide when the field is absent from the source" — no PackedValue stored.
    ///
    /// Also sets the schema version and saves the current schema history to disk.
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
        // Store current version's defaults for future historical lookups
        self.historical_defaults
            .insert(self.schema_version, self.field_defaults.clone());
        // Persist schema history to disk
        self.save_schema_history();
    }

    /// Get the current schema version.
    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    /// Build a schema registry mapping version → (field_name → default_json_value).
    /// Used by the server layer for version-aware default reconstruction in format_document.
    pub fn build_schema_registry(&self) -> HashMap<u8, HashMap<String, serde_json::Value>> {
        let mut registry = HashMap::new();
        // Current version — prefer live field_defaults, fall back to historical
        let current_defaults = if !self.field_defaults.is_empty() {
            self.idx_defaults_to_named(&self.field_defaults)
        } else if let Some(hist) = self.historical_defaults.get(&self.schema_version) {
            self.idx_defaults_to_named(hist)
        } else {
            HashMap::new()
        };
        registry.insert(self.schema_version, current_defaults);
        // Historical versions
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
        if self.in_memory {
            return;
        }
        let dir = Self::schema_dir(&self.root);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("DocStore: failed to create schema dir: {e}");
            return;
        }
        // Save current version's defaults as field_name → JSON default pairs
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
                eprintln!("DocStore: failed to write schema v{}: {e}", self.schema_version);
                return;
            }
            if let Err(e) = std::fs::rename(&tmp, &path) {
                eprintln!("DocStore: failed to rename schema v{}: {e}", self.schema_version);
            }
        }
    }

    fn load_schema_history(root: &Path) -> HashMap<u8, HashMap<u16, PackedValue>> {
        let dir = Self::schema_dir(root);
        let mut history = HashMap::new();
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => return history,
        };
        // Load field dictionary for name→idx mapping
        let (field_to_idx, _) = match Self::load_field_dict_from_dir(root) {
            Ok(pair) => pair,
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
        std::fs::OpenOptions::new().write(true).open(&tmp)
            .map_err(|e| BitdexError::DocStore(format!("open shard for fsync: {e}")))?
            .sync_all()
            .map_err(|e| BitdexError::DocStore(format!("fsync shard: {e}")))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| BitdexError::DocStore(format!("rename shard: {e}")))?;
        Ok(())
    }

    /// Read and decompress a shard file, returning (index_entries, decompressed_data).
    pub(crate) fn read_shard_file(data: &[u8]) -> Result<(Vec<(u32, u32, u32)>, Vec<u8>)> {
        if Self::is_v2_shard(data) {
            return Err(BitdexError::DocStore("cannot read V2 shard with V1 reader — use get_v2 instead".into()));
        }
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

    /// Prepend the version marker + version byte to encoded msgpack bytes.
    fn prepend_version(version: u8, msgpack: Vec<u8>) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + msgpack.len());
        out.push(DOC_VERSION_MARKER);
        out.push(version);
        out.extend_from_slice(&msgpack);
        out
    }

    /// Strip the version prefix from raw bytes, returning (version, msgpack_data).
    /// Legacy docs (pre-versioning) have no prefix and return version 0.
    fn strip_version(raw: &[u8]) -> (u8, &[u8]) {
        if raw.len() >= 2 && raw[0] == DOC_VERSION_MARKER {
            (raw[1], &raw[2..])
        } else {
            // Legacy unversioned doc
            (0, raw)
        }
    }

    fn encode_doc(&mut self, doc: &StoredDoc) -> Result<Vec<u8>> {
        let mut dict_changed = false;
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            let old_len = self.idx_to_field.len();
            let idx = self.ensure_field_idx(name);
            if self.idx_to_field.len() > old_len {
                dict_changed = true;
            }
            let pv = pack_field_value(fv);
            // Elide fields matching their schema default
            if let Some(default_pv) = self.field_defaults.get(&idx) {
                if &pv == default_pv {
                    continue;
                }
            }
            pairs.push((idx, pv));
        }
        if dict_changed {
            self.save_field_dict()?;
        }
        let msgpack = rmp_serde::to_vec(&pairs)
            .map_err(|e| BitdexError::DocStore(format!("msgpack encode: {e}")))?;
        Ok(Self::prepend_version(self.schema_version, msgpack))
    }

    fn encode_doc_readonly(&self, doc: &StoredDoc) -> Result<Vec<u8>> {
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            if let Some(&idx) = self.field_to_idx.get(name.as_str()) {
                let pv = pack_field_value(fv);
                // Elide fields matching their schema default
                if let Some(default_pv) = self.field_defaults.get(&idx) {
                    if &pv == default_pv {
                        continue;
                    }
                }
                pairs.push((idx, pv));
            }
        }
        let msgpack = rmp_serde::to_vec(&pairs)
            .map_err(|e| BitdexError::DocStore(format!("msgpack encode: {e}")))?;
        Ok(Self::prepend_version(self.schema_version, msgpack))
    }

    fn decode_doc(&self, raw: &[u8]) -> Result<StoredDoc> {
        let (version, msgpack) = Self::strip_version(raw);
        let pairs: Vec<(u16, PackedValue)> = rmp_serde::from_slice(msgpack)
            .map_err(|e| BitdexError::DocStore(format!("msgpack decode: {e}")))?;
        let mut fields = HashMap::with_capacity(pairs.len());
        for (idx, pv) in pairs {
            if let Some(name) = self.idx_to_field.get(idx as usize) {
                fields.insert(name.clone(), unpack_field_value(pv));
            }
        }
        Ok(StoredDoc {
            fields,
            schema_version: version,
        })
    }

    // ---- Public API ----

    /// Get a stored document by slot ID.
    /// Auto-detects V1 vs V2 shard format by checking the first 4 bytes.
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
        if Self::is_v2_shard(&data) {
            let shard_id = Self::shard_id(id);
            let (doc, total, unique) = self.get_v2_from_data(&data, id)?;
            self.maybe_enqueue_compact(shard_id, data, total, unique);
            Ok(doc)
        } else {
            match Self::find_in_shard(&data, id)? {
                Some(compressed) => Ok(Some(self.decode_doc(&compressed)?)),
                None => Ok(None),
            }
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

    /// Read a shard and return raw (slot_id, packed_pairs) without full StoredDoc decode.
    /// Each entry is the raw `Vec<(u16, PackedValue)>` — caller uses field dictionary
    /// indices directly, avoiding HashMap + String allocations.
    pub fn get_shard_packed(&self, shard_id: u32) -> Result<Vec<(u32, Vec<(u16, PackedValue)>)>> {
        if self.in_memory {
            let start = shard_id << SHARD_SHIFT;
            let end = start + (1 << SHARD_SHIFT);
            let mut out = Vec::new();
            for slot in start..end {
                if let Some(data) = self.memory_store.get(&slot) {
                    let pairs: Vec<(u16, PackedValue)> = rmp_serde::from_slice(data)
                        .map_err(|e| BitdexError::DocStore(format!("msgpack decode: {e}")))?;
                    out.push((slot, pairs));
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
            let pairs: Vec<(u16, PackedValue)> = rmp_serde::from_slice(&decompressed[start..end])
                .map_err(|e| BitdexError::DocStore(format!("msgpack decode: {e}")))?;
            out.push((*slot_id, pairs));
        }
        Ok(out)
    }

    /// Get the field name → u16 dictionary index mapping.
    pub fn field_to_idx(&self) -> &HashMap<String, u16> {
        &self.field_to_idx
    }

    /// Get the u16 → field name mapping.
    pub fn idx_to_field(&self) -> &[String] {
        &self.idx_to_field
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
                if Self::is_v2_shard(&file_data) {
                    // V2 shard: append each field as a tuple instead of V1 read-modify-write
                    for (field_name, fv) in &doc.fields {
                        if let Some(&fidx) = self.field_to_idx.get(field_name.as_str()) {
                            let packed = pack_field_value(fv);
                            let bytes = rmp_serde::to_vec(&packed)
                                .map_err(|e| BitdexError::DocStore(format!("serialize packed: {e}")))?;
                            self.append_tuple(id, fidx, &bytes)?;
                        }
                    }
                    return Ok(());
                }
                let (index, decompressed) = Self::read_shard_file(&file_data)?;
                index.iter().filter(|(s, _, _)| *s != id).map(|(s, off, len)| {
                    let start = *off as usize;
                    (*s, decompressed[start..start + *len as usize].to_vec())
                }).collect()
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
        };

        // Insert new entry in sorted position (V1 path)
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
            match std::fs::read(&path) {
                Ok(file_data) if Self::is_v2_shard(&file_data) => {
                    // V2 shard: append each doc's fields as tuples
                    // Find the original StoredDocs for these entries
                    for (id, _compressed) in &new_entries {
                        if let Some((_, doc)) = docs.iter().find(|(did, _)| did == id) {
                            for (field_name, fv) in &doc.fields {
                                if let Some(&fidx) = self.field_to_idx.get(field_name.as_str()) {
                                    let packed = pack_field_value(fv);
                                    let bytes = rmp_serde::to_vec(&packed)
                                        .map_err(|e| BitdexError::DocStore(format!("serialize packed: {e}")))?;
                                    self.append_tuple(*id, fidx, &bytes)?;
                                }
                            }
                        }
                    }
                }
                Ok(file_data) => {
                    // V1 shard: read-modify-write
                    let (index, decompressed) = Self::read_shard_file(&file_data)?;
                    let new_ids: std::collections::HashSet<u32> = new_entries.iter().map(|(id, _)| *id).collect();
                    let mut entries: Vec<(u32, Vec<u8>)> = index.iter().filter(|(s, _, _)| !new_ids.contains(s)).map(|(s, off, len)| {
                        let start = *off as usize;
                        (*s, decompressed[start..start + *len as usize].to_vec())
                    }).collect();
                    entries.append(&mut new_entries);
                    entries.sort_by_key(|e| e.0);
                    Self::write_shard_file(&path, &entries)?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // New shard — write V2 tuples
                    for (id, _compressed) in &new_entries {
                        if let Some((_, doc)) = docs.iter().find(|(did, _)| did == id) {
                            for (field_name, fv) in &doc.fields {
                                if let Some(&fidx) = self.field_to_idx.get(field_name.as_str()) {
                                    let packed = pack_field_value(fv);
                                    let bytes = rmp_serde::to_vec(&packed)
                                        .map_err(|e| BitdexError::DocStore(format!("serialize packed: {e}")))?;
                                    self.append_tuple(*id, fidx, &bytes)?;
                                }
                            }
                        }
                    }
                }
                Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
            };
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

    /// Public wrapper for `write_shard_file` (for benchmarks).
    pub fn write_shard_file_pub(path: &Path, entries: &[(u32, Vec<u8>)]) -> Result<()> {
        Self::write_shard_file(path, entries)
    }

    /// Public wrapper for `read_shard_file` (for benchmarks).
    pub fn read_shard_file_pub(data: &[u8]) -> Result<(Vec<(u32, u32, u32)>, Vec<u8>)> {
        Self::read_shard_file(data)
    }

    /// Encode a doc using the field dictionary (public for benchmarks).
    pub fn encode_doc_pub(&mut self, doc: &StoredDoc) -> Result<Vec<u8>> {
        self.encode_doc(doc)
    }

    /// Decode raw bytes into a StoredDoc (public for benchmarks).
    pub fn decode_doc_pub(&self, raw: &[u8]) -> Result<StoredDoc> {
        self.decode_doc(raw)
    }

    /// No-op for filesystem store.
    pub fn compact(&mut self) -> Result<bool> {
        Ok(false)
    }

    // ---- V2 shard format (append-only BitTuple log) ----

    /// Check if raw shard data starts with the V2 magic number.
    fn is_v2_shard(data: &[u8]) -> bool {
        data.len() >= 4
            && u32::from_le_bytes([data[0], data[1], data[2], data[3]]) == V2_MAGIC
    }

    /// Write a V2 shard header to a new file.
    fn write_v2_header(writer: &mut impl Write) -> Result<()> {
        writer
            .write_all(&V2_MAGIC.to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 magic: {e}")))?;
        writer
            .write_all(&2u32.to_le_bytes()) // version = 2
            .map_err(|e| BitdexError::DocStore(format!("write v2 version: {e}")))?;
        writer
            .write_all(&0u32.to_le_bytes()) // flags = 0
            .map_err(|e| BitdexError::DocStore(format!("write v2 flags: {e}")))?;
        writer
            .write_all(&0u32.to_le_bytes()) // num_tuples = 0
            .map_err(|e| BitdexError::DocStore(format!("write v2 num_tuples: {e}")))?;
        Ok(())
    }

    /// Write a single V2 tuple to a writer.
    /// Format: [u32 slot_id LE] [u16 field_idx LE] [u16 value_len LE] [value_len bytes]
    fn write_v2_tuple(writer: &mut impl Write, slot: u32, field_idx: u16, value: &[u8]) -> Result<()> {
        writer
            .write_all(&slot.to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 slot: {e}")))?;
        writer
            .write_all(&field_idx.to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 field_idx: {e}")))?;
        writer
            .write_all(&(value.len() as u16).to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 value_len: {e}")))?;
        writer
            .write_all(value)
            .map_err(|e| BitdexError::DocStore(format!("write v2 value: {e}")))?;
        Ok(())
    }

    /// Parse all V2 tuples from raw shard data.
    /// Returns tuples in file order (append order).
    #[allow(dead_code)]
    fn parse_v2_tuples(data: &[u8]) -> Result<Vec<(u32, u16, Vec<u8>)>> {
        if data.len() < V2_HEADER_SIZE {
            return Err(BitdexError::DocStore("v2 shard too short".into()));
        }
        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != V2_MAGIC {
            return Err(BitdexError::DocStore(format!("not a v2 shard: magic={magic:#x}")));
        }
        let mut pos = V2_HEADER_SIZE;
        let mut tuples = Vec::new();
        while pos + 8 <= data.len() {
            let slot = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            let field_idx = u16::from_le_bytes([data[pos + 4], data[pos + 5]]);
            let value_len = u16::from_le_bytes([data[pos + 6], data[pos + 7]]) as usize;
            pos += 8;
            if pos + value_len > data.len() {
                break; // truncated tuple at end of file — skip
            }
            tuples.push((slot, field_idx, data[pos..pos + value_len].to_vec()));
            pos += value_len;
        }
        Ok(tuples)
    }

    /// Read a single document from V2 shard data — zero-copy scan.
    ///
    /// Forward pass builds a lightweight offset index (no value allocations).
    /// Reverse pass does LIFO dedup and only deserializes values for the target
    /// slot's fields (~3-20 values instead of all 5-15K tuples in the shard).
    ///
    /// Returns (doc, total_tuples, unique_tuples) for staleness calculation.
    /// When compact_tx is None, skips staleness counting entirely.
    fn get_v2_from_data(&self, data: &[u8], slot_id: u32) -> Result<(Option<StoredDoc>, u64, u64)> {
        if data.len() < V2_HEADER_SIZE {
            return Ok((None, 0, 0));
        }

        // Forward scan: build offset index — (slot, field_idx, value_offset, value_len).
        // Zero allocations for tuple values — just byte positions.
        let mut offsets: Vec<(u32, u16, usize, usize)> = Vec::new();
        let mut pos = V2_HEADER_SIZE;
        while pos + 8 <= data.len() {
            let slot = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            let field_idx = u16::from_le_bytes([data[pos + 4], data[pos + 5]]);
            let value_len = u16::from_le_bytes([data[pos + 6], data[pos + 7]]) as usize;
            pos += 8;
            if pos + value_len > data.len() {
                break; // truncated tuple
            }
            offsets.push((slot, field_idx, pos, value_len));
            pos += value_len;
        }

        let total_tuples = offsets.len() as u64;

        // Only count shard-wide staleness when a compaction worker exists.
        let track_staleness = self.compact_tx.is_some() && self.compact_threshold_pct > 0;
        let mut seen_all: Option<std::collections::HashSet<(u32, u16)>> = if track_staleness {
            Some(std::collections::HashSet::new())
        } else {
            None
        };

        // Reverse scan: LIFO dedup. Only deserialize values for the target slot.
        let mut fields: HashMap<u16, PackedValue> = HashMap::new();
        for &(slot, field_idx, value_offset, value_len) in offsets.iter().rev() {
            if let Some(ref mut sa) = seen_all {
                sa.insert((slot, field_idx));
            }
            if slot != slot_id {
                continue;
            }
            if fields.contains_key(&field_idx) {
                continue; // already have a newer value
            }
            // Only deserialize the target slot's fields — ~3-20 values, not 5-15K
            let value_bytes = &data[value_offset..value_offset + value_len];
            let pv: PackedValue = rmp_serde::from_slice(value_bytes)
                .map_err(|e| BitdexError::DocStore(format!("v2 decode field {field_idx}: {e}")))?;
            fields.insert(field_idx, pv);
        }
        let unique_tuples = seen_all.map_or(total_tuples, |sa| sa.len() as u64);

        if fields.is_empty() {
            return Ok((None, total_tuples, unique_tuples));
        }
        let mut doc_fields = HashMap::with_capacity(fields.len());
        for (idx, pv) in fields {
            if let Some(name) = self.idx_to_field.get(idx as usize) {
                doc_fields.insert(name.clone(), unpack_field_value(pv));
            }
        }
        Ok((Some(StoredDoc {
            fields: doc_fields,
            schema_version: 0,
        }), total_tuples, unique_tuples))
    }

    /// Try to enqueue a shard for background compaction if staleness exceeds threshold.
    fn maybe_enqueue_compact(&self, shard_id: u32, data: Vec<u8>, total: u64, unique: u64) {
        if self.compact_threshold_pct == 0 || total == 0 || unique == total {
            return;
        }
        let stale = total - unique;
        if stale * 100 / total > self.compact_threshold_pct {
            if let Some(ref tx) = self.compact_tx {
                // Fire-and-forget: if the channel is full, skip this compaction.
                if tx.try_send((shard_id, data)).is_err() {
                    if let Some(ref counter) = self.compact_skipped {
                        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Get a stored document from a V2 shard by slot ID.
    pub fn get_v2(&self, slot_id: u32) -> Result<Option<StoredDoc>> {
        if self.in_memory {
            return self.get(slot_id);
        }
        let shard_id = Self::shard_id(slot_id);
        let path = Self::shard_path(&self.root, shard_id);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
        };
        if !Self::is_v2_shard(&data) {
            return Err(BitdexError::DocStore("shard is not v2 format".into()));
        }
        let (doc, total, unique) = self.get_v2_from_data(&data, slot_id)?;
        self.maybe_enqueue_compact(shard_id, data, total, unique);
        Ok(doc)
    }

    /// Get or create a buffered writer for a V2 shard.
    /// Creates the file with a V2 header if it doesn't exist.
    fn get_v2_writer(&self, shard_id: u32) -> Result<()> {
        // Already have a writer for this shard
        if self.v2_writers.contains_key(&shard_id) {
            return Ok(());
        }
        let path = Self::shard_path(&self.root, shard_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BitdexError::DocStore(format!("create v2 shard dir: {e}")))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| BitdexError::DocStore(format!("open v2 shard: {e}")))?;
        let file_len = file
            .metadata()
            .map_err(|e| BitdexError::DocStore(format!("stat v2 shard: {e}")))?
            .len();
        let mut buf_writer = BufWriter::new(file);
        if file_len == 0 {
            Self::write_v2_header(&mut buf_writer)?;
        }
        self.v2_writers.insert(shard_id, parking_lot::Mutex::new(buf_writer));
        Ok(())
    }

    /// Append a single tuple to the V2 shard for the given slot.
    pub fn append_tuple(&self, slot: u32, field_idx: u16, value: &[u8]) -> Result<()> {
        let sid = Self::shard_id(slot);
        self.get_v2_writer(sid)?;
        let entry = self.v2_writers.get(&sid)
            .ok_or_else(|| BitdexError::DocStore(format!("v2 writer missing for shard {sid} after init")))?;
        let mut w = entry.lock();
        Self::write_v2_tuple(&mut *w, slot, field_idx, value)?;
        w.flush()
            .map_err(|e| BitdexError::DocStore(format!("flush v2 shard: {e}")))?;
        Ok(())
    }

    /// Batch append tuples, grouped internally by shard.
    pub fn append_tuples_batch(&self, tuples: Vec<(u32, u16, Vec<u8>)>) -> Result<()> {
        // Group by shard
        let mut by_shard: HashMap<u32, Vec<(u32, u16, Vec<u8>)>> = HashMap::new();
        for (slot, field_idx, value) in tuples {
            by_shard
                .entry(Self::shard_id(slot))
                .or_default()
                .push((slot, field_idx, value));
        }
        for (sid, entries) in by_shard {
            self.get_v2_writer(sid)?;
            let entry = self.v2_writers.get(&sid).unwrap();
            let mut w = entry.lock();
            for (slot, field_idx, value) in &entries {
                Self::write_v2_tuple(&mut *w, *slot, *field_idx, value)?;
            }
            w.flush()
                .map_err(|e| BitdexError::DocStore(format!("flush v2 shard: {e}")))?;
        }
        Ok(())
    }

    /// Compact a V2 shard: read file, delegate to zero-copy `compact_shard_from_buffer`.
    pub fn compact_shard(&self, shard_id: u32) -> Result<()> {
        let path = Self::shard_path(&self.root, shard_id);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(BitdexError::DocStore(format!("read shard: {e}"))),
        };
        if data.len() < V2_HEADER_SIZE || !Self::is_v2_shard(&data) {
            return Err(BitdexError::DocStore("compact_shard: not a v2 shard".into()));
        }
        compact_shard_from_buffer(shard_id, &data, &self.root, &self.v2_writers)
    }

    /// Set the compaction channel. Called by ConcurrentEngine after spawning the
    /// compaction worker thread. Readers that detect stale shards will fire-and-forget
    /// the shard buffer over this channel.
    pub fn set_compact_channel(&mut self, tx: crossbeam_channel::Sender<(u32, Vec<u8>)>) {
        self.compact_tx = Some(tx);
    }

    /// Clear the compact channel sender. Called during engine shutdown so the
    /// compact worker's recv() returns Err and the thread can exit.
    pub fn clear_compact_channel(&mut self) {
        self.compact_tx = None;
    }

    /// Set the compaction threshold percentage. 0 = disabled.
    pub fn set_compact_threshold(&mut self, pct: u64) {
        self.compact_threshold_pct = pct;
    }

    /// Set the atomic counter for tracking skipped compactions (channel full).
    pub fn set_compact_skipped(&mut self, counter: Arc<std::sync::atomic::AtomicU64>) {
        self.compact_skipped = Some(counter);
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
            field_defaults: self.field_defaults.clone(),
            schema_version: self.schema_version,
            v2_writers: Arc::new(DashMap::new()),
        })
    }
}

// ---------------------------------------------------------------------------
// Standalone zero-copy shard compaction — no DocStore lock required
// ---------------------------------------------------------------------------

/// Zero-copy V2 shard compaction. Scans `data` to build an offset index,
/// reverse-iterates for LIFO dedup, writes winning tuples directly from the
/// source buffer. No per-tuple allocations. No fsync.
///
/// This is a standalone function so the compaction worker can call it without
/// holding the DocStore mutex — only a brief DashMap remove is needed.
pub fn compact_shard_from_buffer(
    shard_id: u32,
    data: &[u8],
    root: &Path,
    v2_writers: &DashMap<u32, parking_lot::Mutex<BufWriter<std::fs::File>>>,
) -> Result<()> {
    if data.len() < V2_HEADER_SIZE {
        return Err(BitdexError::DocStore("compact_shard_from_buffer: shard too short".into()));
    }
    let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if magic != V2_MAGIC {
        return Err(BitdexError::DocStore(format!(
            "compact_shard_from_buffer: not a v2 shard (magic={magic:#x})"
        )));
    }

    // Forward scan: build offset index — (slot, field_idx, tuple_start, tuple_len).
    // No data copying — just byte positions into the source buffer.
    let mut offsets: Vec<(u32, u16, usize, usize)> = Vec::new();
    let mut pos = V2_HEADER_SIZE;
    while pos + 8 <= data.len() {
        let slot = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        let field_idx = u16::from_le_bytes([data[pos + 4], data[pos + 5]]);
        let value_len = u16::from_le_bytes([data[pos + 6], data[pos + 7]]) as usize;
        let tuple_len = 8 + value_len;
        if pos + tuple_len > data.len() {
            break; // truncated tuple
        }
        offsets.push((slot, field_idx, pos, tuple_len));
        pos += tuple_len;
    }

    // Reverse-iterate to find winners (LIFO dedup), collect their indices
    let mut seen = std::collections::HashSet::new();
    let mut winner_indices: Vec<usize> = Vec::new();
    for (i, (slot, field_idx, _, _)) in offsets.iter().enumerate().rev() {
        if seen.insert((*slot, *field_idx)) {
            winner_indices.push(i);
        }
    }
    winner_indices.reverse(); // restore file order

    // Early exit: nothing to compact
    if winner_indices.len() == offsets.len() {
        return Ok(());
    }

    // Remove any existing buffered writer for this shard
    v2_writers.remove(&shard_id);

    let path = DocStore::shard_path(root, shard_id);

    // Write clean shard: header with correct num_tuples + winning tuples
    // directly from source buffer. No fsync — background janitor work.
    let tmp = path.with_extension("bin.tmp");
    {
        let file = std::fs::File::create(&tmp)
            .map_err(|e| BitdexError::DocStore(format!("create v2 tmp: {e}")))?;
        let mut w = BufWriter::new(file);
        // Header with correct count upfront
        w.write_all(&V2_MAGIC.to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 magic: {e}")))?;
        w.write_all(&2u32.to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 version: {e}")))?;
        w.write_all(&0u32.to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 flags: {e}")))?;
        w.write_all(&(winner_indices.len() as u32).to_le_bytes())
            .map_err(|e| BitdexError::DocStore(format!("write v2 num_tuples: {e}")))?;

        // Write winning tuples — zero-copy slices from source buffer
        for &idx in &winner_indices {
            let (_, _, start, len) = offsets[idx];
            w.write_all(&data[start..start + len])
                .map_err(|e| BitdexError::DocStore(format!("write v2 tuple: {e}")))?;
        }
        w.flush()
            .map_err(|e| BitdexError::DocStore(format!("flush v2 tmp: {e}")))?;
    }
    // Atomic rename — on Windows, remove destination first
    let _ = std::fs::remove_file(&path);
    std::fs::rename(&tmp, &path)
        .map_err(|e| BitdexError::DocStore(format!("rename v2 shard: {e}")))?;

    Ok(())
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
    /// Per-field default values keyed by field dict index.
    /// Fields matching their default are elided on write.
    field_defaults: HashMap<u16, PackedValue>,
    /// Schema version to prepend to each encoded document.
    schema_version: u8,
    /// Per-shard buffered writers for V2 append-only tuple format.
    v2_writers: Arc<DashMap<u32, parking_lot::Mutex<BufWriter<std::fs::File>>>>,
}

impl BulkWriter {
    /// Get the field name → index mapping.
    pub fn field_to_idx(&self) -> &HashMap<String, u16> {
        &self.field_to_idx
    }

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

    /// Write pre-encoded docs to fresh shard files. No read-back, no merge.
    /// For bulk load where every slot is new — skips the read-modify-write cycle.
    /// No nested rayon — caller handles parallelism.
    pub fn write_batch_fresh(&self, encoded: Vec<(u32, Vec<u8>)>) {
        if encoded.is_empty() {
            return;
        }

        // Group by docstore shard
        let mut by_shard: HashMap<u32, Vec<(u32, Vec<u8>)>> = HashMap::new();
        for (slot, bytes) in encoded {
            by_shard
                .entry(DocStore::shard_id(slot))
                .or_default()
                .push((slot, bytes));
        }

        // Write each shard file directly — no read-back, no locks needed
        // (each scratch shard maps to non-overlapping docstore shards)
        for (sid, mut entries) in by_shard {
            entries.sort_by_key(|e| e.0);
            let path = DocStore::shard_path(&self.root, sid);
            if let Err(e) = DocStore::write_shard_file(&path, &entries) {
                eprintln!("BulkWriter: shard {} write failed: {e}", sid);
            }
        }
    }

    /// Encode a StoredDoc to msgpack bytes using the snapshotted field dictionary.
    pub fn encode_doc(&self, doc: &StoredDoc) -> Vec<u8> {
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            if let Some(&idx) = self.field_to_idx.get(name.as_str()) {
                let pv = pack_field_value(fv);
                // Elide fields matching their schema default
                if let Some(default_pv) = self.field_defaults.get(&idx) {
                    if &pv == default_pv {
                        continue;
                    }
                }
                pairs.push((idx, pv));
            }
        }
        let msgpack = rmp_serde::to_vec(&pairs).unwrap_or_default();
        DocStore::prepend_version(self.schema_version, msgpack)
    }

    /// Encode a JSON value directly to msgpack bytes using the DataSchema.
    /// Skips the intermediate StoredDoc/HashMap allocation entirely —
    /// walks schema fields once, converts JSON → PackedValue → msgpack.
    pub fn encode_json(&self, json: &serde_json::Value, schema: &DataSchema) -> Vec<u8> {
        self.encode_json_with_dicts(json, schema, None)
    }

    /// Encode a JSON document to msgpack bytes, with optional dictionaries for LowCardinalityString.
    pub fn encode_json_with_dicts(
        &self,
        json: &serde_json::Value,
        schema: &DataSchema,
        dictionaries: Option<&std::collections::HashMap<String, crate::dictionary::FieldDictionary>>,
    ) -> Vec<u8> {
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
                        // ExistsBoolean false — check if it matches the default before storing
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
                // Elide fields matching their schema default
                if let Some(default_pv) = self.field_defaults.get(&idx) {
                    if &pv == default_pv {
                        continue;
                    }
                }
                pairs.push((idx, pv));
            }
        }

        let msgpack = rmp_serde::to_vec(&pairs).unwrap_or_default();
        DocStore::prepend_version(self.schema_version, msgpack)
    }

    // ---- V2 append-only tuple methods ----

    /// Append a single raw tuple to the V2 shard for the given slot.
    /// No compression, no read-modify-write. Files opened lazily on first write.
    pub fn append_tuple_raw(&self, slot: u32, field_idx: u16, value_bytes: &[u8]) {
        let sid = DocStore::shard_id(slot);

        // Ensure writer exists for this shard
        if !self.v2_writers.contains_key(&sid) {
            let path = DocStore::shard_path(&self.root, sid);
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let file = match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("BulkWriter: v2 open shard {sid}: {e}");
                    return;
                }
            };
            let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
            let mut bw = BufWriter::new(file);
            if file_len == 0 {
                if let Err(e) = DocStore::write_v2_header(&mut bw) {
                    eprintln!("BulkWriter: v2 write header shard {sid}: {e}");
                    return;
                }
            }
            self.v2_writers.insert(sid, parking_lot::Mutex::new(bw));
        }

        let entry = self.v2_writers.get(&sid).unwrap();
        let mut w = entry.lock();
        if let Err(e) = DocStore::write_v2_tuple(&mut *w, slot, field_idx, value_bytes) {
            eprintln!("BulkWriter: v2 write tuple shard {sid}: {e}");
        }
    }

    /// Flush all open V2 writers. Call after bulk loading is complete.
    pub fn flush_v2_writers(&self) {
        for entry in self.v2_writers.iter() {
            let mut w = entry.value().lock();
            let _ = w.flush();
        }
    }
}

// ---------------------------------------------------------------------------
// Compact value encoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum PackedValue {
    I(i64),
    F(f64),
    B(bool),
    S(String),
    Mi(Vec<i64>),
    Mm(Vec<PackedValue>),
}

/// Convert a serde_json::Value to a PackedValue for default comparison.
/// Returns None for types that can't be represented (objects, etc.) or
/// for null (which means "elide if field is missing").
fn json_to_packed_default(val: &serde_json::Value) -> Option<PackedValue> {
    match val {
        serde_json::Value::Null => None, // null default = elide when field is absent
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

/// Convert a PackedValue back to a serde_json::Value (for schema history persistence).
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

/// Convert a raw JSON value to PackedValue, with optional dictionary for LowCardinalityString.
fn json_to_packed_with_dict(
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
                // Without dictionary, store 0 (will be resolved later if needed)
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
            schema_version: 0,
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
                    schema_version: 0,
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
                schema_version: 0,
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
                        schema_version: 0,
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
            schema_version: 0,
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
                    schema_version: 0,
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

    // ---- Default value elision tests ----

    fn make_schema_with_defaults() -> DataSchema {
        DataSchema {
            id_field: "id".to_string(),
            schema_version: 1,
            fields: vec![
                FieldMapping {
                    source: "commentCount".into(),
                    target: "commentCount".into(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: Some(serde_json::json!(0)),
                },
                FieldMapping {
                    source: "poi".into(),
                    target: "poi".into(),
                    value_type: FieldValueType::Boolean,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: Some(serde_json::json!(false)),
                },
                FieldMapping {
                    source: "toolIds".into(),
                    target: "toolIds".into(),
                    value_type: FieldValueType::IntegerArray,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: Some(serde_json::json!([])),
                },
                FieldMapping {
                    source: "blockedFor".into(),
                    target: "blockedFor".into(),
                    value_type: FieldValueType::MappedString,
                    fallback: None,
                    string_map: Some([("tos".to_string(), 1i64)].into_iter().collect()),
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: Some(serde_json::Value::Null),
                },
                FieldMapping {
                    source: "userId".into(),
                    target: "userId".into(),
                    value_type: FieldValueType::Integer,
                    fallback: None,
                    string_map: None,
                    doc_only: false,
                    filter_only: false,
                    ms_to_seconds: false,
                    truncate_u32: false,
                    case_sensitive: false,
                    default_value: None, // no default — always stored
                },
            ],
        }
    }

    #[test]
    fn test_elision_default_fields_not_stored() {
        // Fields matching their default should be elided from encoded bytes
        let mut store = DocStore::open_temp().unwrap();
        let schema = make_schema_with_defaults();

        // Ensure field dictionary is populated
        for f in &schema.fields {
            store.ensure_field_idx(&f.target);
        }
        store.ensure_field_idx("id");
        store.set_field_defaults(&schema);

        // Doc with all-default values: commentCount=0, poi=false, toolIds=[], userId=42
        let doc = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(1))),
                ("commentCount".to_string(), FieldValue::Single(Value::Integer(0))),
                ("poi".to_string(), FieldValue::Single(Value::Bool(false))),
                ("toolIds".to_string(), FieldValue::Multi(vec![])),
                ("userId".to_string(), FieldValue::Single(Value::Integer(42))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };

        store.put(1, &doc).unwrap();
        let got = store.get(1).unwrap().unwrap();

        // Default fields should NOT be in the decoded doc (they were elided)
        assert!(got.fields.get("commentCount").is_none(), "commentCount=0 should be elided");
        assert!(got.fields.get("poi").is_none(), "poi=false should be elided");
        assert!(got.fields.get("toolIds").is_none(), "toolIds=[] should be elided");

        // Non-default field should be preserved
        match &got.fields["userId"] {
            FieldValue::Single(Value::Integer(42)) => {}
            other => panic!("userId should be 42, got: {:?}", other),
        }
        // ID should be preserved
        match &got.fields["id"] {
            FieldValue::Single(Value::Integer(1)) => {}
            other => panic!("id should be 1, got: {:?}", other),
        }
    }

    #[test]
    fn test_elision_non_default_preserved() {
        let mut store = DocStore::open_temp().unwrap();
        let schema = make_schema_with_defaults();
        for f in &schema.fields {
            store.ensure_field_idx(&f.target);
        }
        store.ensure_field_idx("id");
        store.set_field_defaults(&schema);

        // Doc with non-default values
        let doc = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(2))),
                ("commentCount".to_string(), FieldValue::Single(Value::Integer(5))),
                ("poi".to_string(), FieldValue::Single(Value::Bool(true))),
                ("toolIds".to_string(), FieldValue::Multi(vec![Value::Integer(10), Value::Integer(20)])),
                ("userId".to_string(), FieldValue::Single(Value::Integer(99))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };

        store.put(2, &doc).unwrap();
        let got = store.get(2).unwrap().unwrap();

        match &got.fields["commentCount"] {
            FieldValue::Single(Value::Integer(5)) => {}
            other => panic!("expected commentCount=5, got: {:?}", other),
        }
        match &got.fields["poi"] {
            FieldValue::Single(Value::Bool(true)) => {}
            other => panic!("expected poi=true, got: {:?}", other),
        }
        match &got.fields["toolIds"] {
            FieldValue::Multi(vs) => {
                assert_eq!(vs.len(), 2);
            }
            other => panic!("expected toolIds with 2 elements, got: {:?}", other),
        }
    }

    #[test]
    fn test_elision_round_trip_mixed() {
        let mut store = DocStore::open_temp().unwrap();
        let schema = make_schema_with_defaults();
        for f in &schema.fields {
            store.ensure_field_idx(&f.target);
        }
        store.ensure_field_idx("id");
        store.set_field_defaults(&schema);

        // Mix of default and non-default
        let doc1 = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(10))),
                ("commentCount".to_string(), FieldValue::Single(Value::Integer(0))), // default
                ("poi".to_string(), FieldValue::Single(Value::Bool(true))),          // non-default
                ("userId".to_string(), FieldValue::Single(Value::Integer(7))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };
        let doc2 = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(11))),
                ("commentCount".to_string(), FieldValue::Single(Value::Integer(3))), // non-default
                ("poi".to_string(), FieldValue::Single(Value::Bool(false))),         // default
                ("userId".to_string(), FieldValue::Single(Value::Integer(8))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };

        store.put(10, &doc1).unwrap();
        store.put(11, &doc2).unwrap();

        let got1 = store.get(10).unwrap().unwrap();
        assert!(got1.fields.get("commentCount").is_none()); // elided
        match &got1.fields["poi"] {
            FieldValue::Single(Value::Bool(true)) => {}
            other => panic!("doc1 poi: {:?}", other),
        }

        let got2 = store.get(11).unwrap().unwrap();
        match &got2.fields["commentCount"] {
            FieldValue::Single(Value::Integer(3)) => {}
            other => panic!("doc2 commentCount: {:?}", other),
        }
        assert!(got2.fields.get("poi").is_none()); // elided
    }

    #[test]
    fn test_elision_backward_compatibility() {
        // Docs stored WITHOUT elision (no field_defaults) should still decode correctly
        let mut store = DocStore::open_temp().unwrap();
        // Don't set field_defaults — simulates old data

        let doc = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(1))),
                ("commentCount".to_string(), FieldValue::Single(Value::Integer(0))),
                ("poi".to_string(), FieldValue::Single(Value::Bool(false))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };
        store.put(1, &doc).unwrap();

        // Without defaults, all fields are stored
        let got = store.get(1).unwrap().unwrap();
        assert_eq!(got.fields.len(), 3);
        match &got.fields["commentCount"] {
            FieldValue::Single(Value::Integer(0)) => {}
            other => panic!("expected commentCount=0, got: {:?}", other),
        }
    }

    #[test]
    fn test_elision_null_default() {
        // blockedFor has null default — when field is absent from source,
        // it's simply not stored (natural behavior). When field IS present
        // as a mapped string value, it should be stored.
        let mut store = DocStore::open_temp().unwrap();
        let schema = make_schema_with_defaults();
        for f in &schema.fields {
            store.ensure_field_idx(&f.target);
        }
        store.ensure_field_idx("id");
        store.set_field_defaults(&schema);

        // blockedFor = "tos" → mapped to 1 → should be stored
        let doc = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(1))),
                ("blockedFor".to_string(), FieldValue::Single(Value::Integer(1))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };
        store.put(1, &doc).unwrap();
        let got = store.get(1).unwrap().unwrap();
        match &got.fields["blockedFor"] {
            FieldValue::Single(Value::Integer(1)) => {}
            other => panic!("expected blockedFor=1, got: {:?}", other),
        }

        // Doc without blockedFor field — nothing stored (null default = natural absence)
        let doc2 = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(2))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };
        store.put(2, &doc2).unwrap();
        let got2 = store.get(2).unwrap().unwrap();
        assert!(got2.fields.get("blockedFor").is_none());
    }

    #[test]
    fn test_elision_bulk_writer_encode_doc() {
        let mut store = DocStore::open_temp().unwrap();
        let schema = make_schema_with_defaults();
        let field_names: Vec<String> = schema.fields.iter().map(|f| f.target.clone())
            .chain(std::iter::once("id".to_string()))
            .collect();
        for name in &field_names {
            store.ensure_field_idx(name);
        }
        store.set_field_defaults(&schema);
        let writer = store.prepare_bulk_load(&field_names).unwrap();

        // Encode a doc with default values via BulkWriter
        let doc = StoredDoc {
            fields: vec![
                ("id".to_string(), FieldValue::Single(Value::Integer(1))),
                ("commentCount".to_string(), FieldValue::Single(Value::Integer(0))),
                ("poi".to_string(), FieldValue::Single(Value::Bool(false))),
                ("userId".to_string(), FieldValue::Single(Value::Integer(42))),
            ]
            .into_iter()
            .collect(),
            schema_version: 0,
        };

        let bytes = writer.encode_doc(&doc);
        // Decode and verify defaults were elided
        let decoded = store.decode_doc(&bytes).unwrap();
        assert!(decoded.fields.get("commentCount").is_none(), "commentCount=0 should be elided in BulkWriter");
        assert!(decoded.fields.get("poi").is_none(), "poi=false should be elided in BulkWriter");
        assert!(decoded.fields.get("userId").is_some(), "userId should be preserved");
    }

    #[test]
    fn test_elision_bulk_writer_encode_json() {
        let mut store = DocStore::open_temp().unwrap();
        let schema = make_schema_with_defaults();
        let field_names: Vec<String> = schema.fields.iter().map(|f| f.target.clone())
            .chain(std::iter::once("id".to_string()))
            .collect();
        for name in &field_names {
            store.ensure_field_idx(name);
        }
        store.set_field_defaults(&schema);
        let writer = store.prepare_bulk_load(&field_names).unwrap();

        // Encode JSON directly with default values
        let json = serde_json::json!({
            "id": 1,
            "commentCount": 0,
            "poi": false,
            "toolIds": [],
            "userId": 42
        });
        let bytes = writer.encode_json(&json, &schema);
        let decoded = store.decode_doc(&bytes).unwrap();
        assert!(decoded.fields.get("commentCount").is_none(), "commentCount=0 should be elided in encode_json");
        assert!(decoded.fields.get("poi").is_none(), "poi=false should be elided in encode_json");
        // toolIds=[] returns None from json_to_packed (empty array), so it's always absent
        assert!(decoded.fields.get("toolIds").is_none());
        assert!(decoded.fields.get("userId").is_some());
    }

    // ---- Schema versioning tests ----

    #[test]
    fn test_version_byte_roundtrip() {
        let mut store = DocStore::open_temp().unwrap();
        store.schema_version = 3;

        let doc = StoredDoc {
            fields: vec![("x".to_string(), FieldValue::Single(Value::Integer(42)))]
                .into_iter()
                .collect(),
            schema_version: 0,
        };
        store.put(1, &doc).unwrap();

        let got = store.get(1).unwrap().unwrap();
        assert_eq!(got.schema_version, 3, "decoded doc should carry the schema version it was encoded with");
        assert_eq!(
            got.fields["x"],
            FieldValue::Single(Value::Integer(42))
        );
    }

    #[test]
    fn test_legacy_doc_decodes_as_version_0() {
        // Simulate a legacy doc encoded without version prefix (raw msgpack)
        let store = DocStore::open_temp().unwrap();
        let pairs: Vec<(u16, PackedValue)> = vec![(0, PackedValue::I(99))];
        let raw = rmp_serde::to_vec(&pairs).unwrap();

        // Legacy: first byte should be >= 0x90 (msgpack fixarray)
        assert!(raw[0] >= 0x90, "msgpack array should start >= 0x90");

        let decoded = store.decode_doc(&raw).unwrap();
        assert_eq!(decoded.schema_version, 0, "legacy doc should decode as version 0");
    }

    #[test]
    fn test_versioned_doc_first_bytes() {
        let mut store = DocStore::open_temp().unwrap();
        store.schema_version = 5;

        let doc = StoredDoc {
            fields: vec![("x".to_string(), FieldValue::Single(Value::Integer(1)))]
                .into_iter()
                .collect(),
            schema_version: 0,
        };
        let encoded = store.encode_doc(&doc).unwrap();

        // First two bytes should be [0x00, 5]
        assert_eq!(encoded[0], 0x00, "version marker byte");
        assert_eq!(encoded[1], 5, "schema version byte");
    }

    #[test]
    fn test_schema_history_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");

        // Create store, set defaults for version 1
        {
            let mut store = DocStore::open(&docs_dir).unwrap();
            let schema = make_schema_with_defaults();
            store.ensure_field_idx("commentCount");
            store.ensure_field_idx("poi");
            store.ensure_field_idx("toolIds");
            store.ensure_field_idx("userId");
            store.ensure_field_idx("blockedFor");
            store.save_field_dict().unwrap(); // persist so reload can map names→indices
            store.set_field_defaults(&schema);
        }

        // Verify schema file was created
        let schema_path = docs_dir.join("meta").join("schema").join("v1.json");
        assert!(schema_path.exists(), "v1.json schema history should exist");

        // Reopen and verify historical defaults loaded
        let store2 = DocStore::open(&docs_dir).unwrap();
        let registry = store2.build_schema_registry();
        assert!(registry.contains_key(&1), "registry should contain version 1");
        let v1_defaults = &registry[&1];
        assert_eq!(v1_defaults["commentCount"], serde_json::json!(0));
        assert_eq!(v1_defaults["poi"], serde_json::json!(false));
    }

    #[test]
    fn test_bulk_writer_encodes_with_version() {
        let mut store = DocStore::open_temp().unwrap();
        let schema = make_schema_with_defaults();
        let field_names: Vec<String> = schema.fields.iter().map(|f| f.target.clone())
            .chain(std::iter::once("id".to_string()))
            .collect();
        for name in &field_names {
            store.ensure_field_idx(name);
        }
        store.set_field_defaults(&schema);
        let writer = store.prepare_bulk_load(&field_names).unwrap();

        let doc = StoredDoc {
            fields: vec![("userId".to_string(), FieldValue::Single(Value::Integer(42)))]
                .into_iter()
                .collect(),
            schema_version: 0,
        };
        let encoded = writer.encode_doc(&doc);

        // Should have version prefix
        assert_eq!(encoded[0], 0x00, "BulkWriter should prepend version marker");
        assert_eq!(encoded[1], 1, "BulkWriter should use schema_version 1");

        // Should decode correctly
        let decoded = store.decode_doc(&encoded).unwrap();
        assert_eq!(decoded.schema_version, 1);
        assert!(decoded.fields.get("userId").is_some());
    }

    // ---- V2 shard format tests ----

    #[test]
    fn test_v2_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("name");
        store.ensure_field_idx("score");
        store.save_field_dict().unwrap();

        let name_idx = store.field_to_idx["name"];
        let score_idx = store.field_to_idx["score"];

        // Append tuples for slot 42
        let name_val = rmp_serde::to_vec(&PackedValue::S("hello".into())).unwrap();
        let score_val = rmp_serde::to_vec(&PackedValue::I(99)).unwrap();

        store.append_tuple(42, name_idx, &name_val).unwrap();
        store.append_tuple(42, score_idx, &score_val).unwrap();

        // Read back
        let doc = store.get(42).unwrap().unwrap();
        match &doc.fields["name"] {
            FieldValue::Single(Value::String(s)) => assert_eq!(s, "hello"),
            other => panic!("expected name=hello, got: {:?}", other),
        }
        match &doc.fields["score"] {
            FieldValue::Single(Value::Integer(99)) => {}
            other => panic!("expected score=99, got: {:?}", other),
        }
    }

    #[test]
    fn test_v2_newest_wins() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.save_field_dict().unwrap();

        let val_idx = store.field_to_idx["val"];

        // Append val=1, then val=2
        let v1 = rmp_serde::to_vec(&PackedValue::I(1)).unwrap();
        let v2 = rmp_serde::to_vec(&PackedValue::I(2)).unwrap();
        store.append_tuple(10, val_idx, &v1).unwrap();
        store.append_tuple(10, val_idx, &v2).unwrap();

        let doc = store.get(10).unwrap().unwrap();
        match &doc.fields["val"] {
            FieldValue::Single(Value::Integer(2)) => {}
            other => panic!("expected val=2, got: {:?}", other),
        }
    }

    #[test]
    fn test_v2_multiple_slots() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("x");
        store.save_field_dict().unwrap();
        let x_idx = store.field_to_idx["x"];

        // Slots 0, 1, 2 all in shard 0 (SHARD_SHIFT=9 → first 512 slots in shard 0)
        for slot in 0..3u32 {
            let val = rmp_serde::to_vec(&PackedValue::I(slot as i64 * 10)).unwrap();
            store.append_tuple(slot, x_idx, &val).unwrap();
        }

        for slot in 0..3u32 {
            let doc = store.get(slot).unwrap().unwrap();
            match &doc.fields["x"] {
                FieldValue::Single(Value::Integer(v)) => assert_eq!(*v, slot as i64 * 10),
                other => panic!("slot {slot}: expected x={}, got: {:?}", slot * 10, other),
            }
        }
        // Non-existent slot returns None
        assert!(store.get(100).unwrap().is_none());
    }

    #[test]
    fn test_v2_compact() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.save_field_dict().unwrap();
        let val_idx = store.field_to_idx["val"];

        // Append stale entries: val=1, val=2, val=3 for slot 5
        for i in 1..=3i64 {
            let v = rmp_serde::to_vec(&PackedValue::I(i)).unwrap();
            store.append_tuple(5, val_idx, &v).unwrap();
        }

        let sid = DocStore::shard_id(5);
        let path = DocStore::shard_path(&docs_dir, sid);
        let size_before = std::fs::metadata(&path).unwrap().len();

        // Compact
        store.compact_shard(sid).unwrap();

        let size_after = std::fs::metadata(&path).unwrap().len();
        assert!(
            size_after < size_before,
            "compacted file ({size_after}) should be smaller than original ({size_before})"
        );

        // Verify newest value survived
        let doc = store.get(5).unwrap().unwrap();
        match &doc.fields["val"] {
            FieldValue::Single(Value::Integer(3)) => {}
            other => panic!("expected val=3 after compaction, got: {:?}", other),
        }

        // Verify header num_tuples was updated
        let data = std::fs::read(&path).unwrap();
        let num_tuples = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        assert_eq!(num_tuples, 1, "compacted shard should have 1 tuple");
    }

    #[test]
    fn test_v2_format_detection() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");

        // Write a V1 shard
        {
            let mut store = DocStore::open(&docs_dir).unwrap();
            let doc = StoredDoc {
                fields: vec![("x".to_string(), FieldValue::Single(Value::Integer(1)))]
                    .into_iter()
                    .collect(),
                schema_version: 0,
            };
            store.put(0, &doc).unwrap();
        }

        // Write a V2 shard (different shard — use slot 512+ for shard 1)
        {
            let mut store = DocStore::open(&docs_dir).unwrap();
            store.ensure_field_idx("x"); // already exists but need the idx
            store.save_field_dict().unwrap();
            let x_idx = store.field_to_idx["x"];
            let val = rmp_serde::to_vec(&PackedValue::I(2)).unwrap();
            store.append_tuple(512, x_idx, &val).unwrap();
        }

        // Read both — auto-detection should work
        let store = DocStore::open(&docs_dir).unwrap();

        // V1 shard read
        let doc_v1 = store.get(0).unwrap().unwrap();
        match &doc_v1.fields["x"] {
            FieldValue::Single(Value::Integer(1)) => {}
            other => panic!("v1 read: expected x=1, got: {:?}", other),
        }

        // V2 shard read
        let doc_v2 = store.get(512).unwrap().unwrap();
        match &doc_v2.fields["x"] {
            FieldValue::Single(Value::Integer(2)) => {}
            other => panic!("v2 read: expected x=2, got: {:?}", other),
        }
    }

    #[test]
    fn test_v2_batch_append() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("name");
        store.ensure_field_idx("age");
        store.ensure_field_idx("tags");
        store.save_field_dict().unwrap();

        let name_idx = store.field_to_idx["name"];
        let age_idx = store.field_to_idx["age"];
        let tags_idx = store.field_to_idx["tags"];

        // Simulate batch from multiple "CSVs"
        let mut tuples = Vec::new();

        // CSV 1: names
        for slot in 0..5u32 {
            let val = rmp_serde::to_vec(&PackedValue::S(format!("user_{slot}"))).unwrap();
            tuples.push((slot, name_idx, val));
        }
        // CSV 2: ages
        for slot in 0..5u32 {
            let val = rmp_serde::to_vec(&PackedValue::I(20 + slot as i64)).unwrap();
            tuples.push((slot, age_idx, val));
        }
        // CSV 3: tags (multi-value)
        for slot in 0..5u32 {
            let val = rmp_serde::to_vec(&PackedValue::Mi(vec![slot as i64, slot as i64 + 100]))
                .unwrap();
            tuples.push((slot, tags_idx, val));
        }

        store.append_tuples_batch(tuples).unwrap();

        // Verify all fields assembled per slot
        for slot in 0..5u32 {
            let doc = store.get(slot).unwrap().unwrap();
            assert_eq!(doc.fields.len(), 3, "slot {slot} should have 3 fields");
            match &doc.fields["name"] {
                FieldValue::Single(Value::String(s)) => {
                    assert_eq!(s, &format!("user_{slot}"));
                }
                other => panic!("slot {slot} name: {:?}", other),
            }
            match &doc.fields["age"] {
                FieldValue::Single(Value::Integer(v)) => {
                    assert_eq!(*v, 20 + slot as i64);
                }
                other => panic!("slot {slot} age: {:?}", other),
            }
            match &doc.fields["tags"] {
                FieldValue::Multi(vs) => {
                    assert_eq!(vs.len(), 2);
                }
                other => panic!("slot {slot} tags: {:?}", other),
            }
        }
    }

    // ---- Janitor / reader-triggered compaction tests ----

    #[test]
    fn test_janitor_staleness_detection() {
        // Write tuples with >30% staleness, verify get_v2_from_data returns correct counts.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.save_field_dict().unwrap();
        let val_idx = store.field_to_idx["val"];
        // Set compact channel so staleness tracking is enabled in get_v2_from_data
        let (compact_tx, _compact_rx) = crossbeam_channel::bounded::<(u32, Vec<u8>)>(32);
        store.set_compact_channel(compact_tx);

        // Write 3 versions of the same (slot=0, field=val) — 2 are stale
        for v in 0..3i64 {
            let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
            store.append_tuple(0, val_idx, &packed).unwrap();
        }
        // Flush writers so data is on disk
        store.v2_writers.clear();

        let path = DocStore::shard_path(&store.root, DocStore::shard_id(0));
        let data = std::fs::read(&path).unwrap();
        let (doc, total, unique) = store.get_v2_from_data(&data, 0).unwrap();

        // Should find the doc with newest value
        // doc already unwrapped — existence proven by .unwrap().unwrap() above
        match &doc.unwrap().fields["val"] {
            FieldValue::Single(Value::Integer(2)) => {}
            other => panic!("expected val=2, got: {:?}", other),
        }

        // 3 total tuples, 1 unique (slot=0, field=val)
        assert_eq!(total, 3);
        assert_eq!(unique, 1);

        // stale = 3 - 1 = 2, pct = 2*100/3 = 66 > 30 → would trigger compaction
        let stale = total - unique;
        assert!(stale * 100 / total > DEFAULT_COMPACT_THRESHOLD_PCT);
    }

    #[test]
    fn test_janitor_no_trigger_when_clean() {
        // Write unique tuples only — no staleness, should not trigger.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("a");
        store.ensure_field_idx("b");
        store.save_field_dict().unwrap();
        let a_idx = store.field_to_idx["a"];
        let b_idx = store.field_to_idx["b"];

        let va = rmp_serde::to_vec(&PackedValue::I(1)).unwrap();
        let vb = rmp_serde::to_vec(&PackedValue::I(2)).unwrap();
        store.append_tuple(0, a_idx, &va).unwrap();
        store.append_tuple(0, b_idx, &vb).unwrap();
        store.v2_writers.clear();

        let path = DocStore::shard_path(&store.root, DocStore::shard_id(0));
        let data = std::fs::read(&path).unwrap();
        let (_doc, total, unique) = store.get_v2_from_data(&data, 0).unwrap();

        assert_eq!(total, 2);
        assert_eq!(unique, 2);
        // stale = 0, would NOT trigger
        assert_eq!(total - unique, 0);
    }

    #[test]
    fn test_janitor_compact_shard_from_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.save_field_dict().unwrap();
        let val_idx = store.field_to_idx["val"];

        // Write 5 versions — only the last should survive compaction
        for v in 0..5i64 {
            let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
            store.append_tuple(0, val_idx, &packed).unwrap();
        }
        store.v2_writers.clear();

        let shard_id = DocStore::shard_id(0);
        let path = DocStore::shard_path(&store.root, shard_id);
        let size_before = std::fs::metadata(&path).unwrap().len();
        let data = std::fs::read(&path).unwrap();

        // Compact from buffer (standalone function — no DocStore lock needed)
        compact_shard_from_buffer(shard_id, &data, &store.root, &store.v2_writers).unwrap();

        let size_after = std::fs::metadata(&path).unwrap().len();
        assert!(
            size_after < size_before,
            "compacted ({size_after}) should be smaller than original ({size_before})"
        );

        // Verify newest value survived
        let doc = store.get_v2(0).unwrap().unwrap();
        match &doc.fields["val"] {
            FieldValue::Single(Value::Integer(4)) => {}
            other => panic!("expected val=4 after compaction, got: {:?}", other),
        }
    }

    #[test]
    fn test_janitor_reader_triggered_compaction() {
        // Integration test: set up compact channel, write stale tuples,
        // read via get_v2 (triggers compaction), wait, verify shard compacted.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.save_field_dict().unwrap();
        let val_idx = store.field_to_idx["val"];

        // Set up compact channel with a worker thread
        let (compact_tx, compact_rx) = crossbeam_channel::bounded::<(u32, Vec<u8>)>(32);
        store.set_compact_channel(compact_tx);

        // Write 10 versions of the same tuple — 90% stale
        for v in 0..10i64 {
            let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
            store.append_tuple(0, val_idx, &packed).unwrap();
        }
        store.v2_writers.clear();

        let shard_id = DocStore::shard_id(0);
        let path = DocStore::shard_path(&store.root, shard_id);
        let size_before = std::fs::metadata(&path).unwrap().len();

        // Reading triggers compaction enqueue
        let doc = store.get_v2(0).unwrap().unwrap();
        match &doc.fields["val"] {
            FieldValue::Single(Value::Integer(9)) => {}
            other => panic!("expected val=9, got: {:?}", other),
        }

        // Drain the compact channel and process manually
        // (simulating what the background worker would do)
        match compact_rx.try_recv() {
            Ok((sid, data)) => {
                assert_eq!(sid, shard_id);
                compact_shard_from_buffer(sid, &data, &store.root, &store.v2_writers).unwrap();
            }
            Err(_) => panic!("expected compaction to be enqueued"),
        }

        let size_after = std::fs::metadata(&path).unwrap().len();
        assert!(
            size_after < size_before,
            "compacted ({size_after}) should be smaller than original ({size_before})"
        );

        // Verify data is still correct after compaction
        let doc = store.get_v2(0).unwrap().unwrap();
        match &doc.fields["val"] {
            FieldValue::Single(Value::Integer(9)) => {}
            other => panic!("expected val=9 after compaction, got: {:?}", other),
        }
    }

    #[test]
    fn test_janitor_channel_full_drops_silently() {
        // A bounded(1) compact channel that is already full should not block or panic
        // when a reader triggers another compaction enqueue.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.save_field_dict().unwrap();
        let val_idx = store.field_to_idx["val"];

        // Bounded(1) — only one slot in the channel
        let (compact_tx, compact_rx) = crossbeam_channel::bounded::<(u32, Vec<u8>)>(1);
        store.set_compact_channel(compact_tx);

        // Write enough stale tuples to trigger compaction (>30% stale)
        for v in 0..10i64 {
            let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
            store.append_tuple(0, val_idx, &packed).unwrap();
        }
        store.v2_writers.clear();

        // First read fills the channel (1 slot)
        let doc = store.get_v2(0).unwrap().unwrap();
        // doc already unwrapped — existence proven by .unwrap().unwrap() above

        // Write more stale data to a different slot in the same shard
        for v in 0..10i64 {
            let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
            store.append_tuple(1, val_idx, &packed).unwrap();
        }
        store.v2_writers.clear();

        // Second read should try_send but the channel is full — must not block or panic
        let _doc2 = store.get_v2(1).unwrap().unwrap();

        // Verify exactly one message in the channel (capacity was 1)
        assert!(compact_rx.try_recv().is_ok());
        // Channel should now be empty (second enqueue was silently dropped)
        assert!(compact_rx.try_recv().is_err());
    }

    #[test]
    fn test_janitor_no_overhead_without_compact_channel() {
        // Without a compact channel, get_v2 should skip staleness tracking entirely.
        // Verify reads work correctly and no unnecessary overhead occurs.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.save_field_dict().unwrap();
        let val_idx = store.field_to_idx["val"];

        // No compact channel set — compact_tx is None

        // Write stale tuples (multiple versions of same field)
        for v in 0..5i64 {
            let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
            store.append_tuple(0, val_idx, &packed).unwrap();
        }
        store.v2_writers.clear();

        // Read should still return the newest value
        let doc = store.get_v2(0).unwrap().unwrap();
        match &doc.fields["val"] {
            FieldValue::Single(Value::Integer(4)) => {}
            other => panic!("expected val=4, got: {:?}", other),
        }

        // Verify compact_tx is indeed None (no channel set)
        assert!(store.compact_tx.is_none());
    }

    #[test]
    fn test_janitor_early_exit_when_clean() {
        // When all tuples are unique (no stale data), compact_shard_from_buffer
        // should exit early without rewriting the file.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("a");
        store.ensure_field_idx("b");
        store.save_field_dict().unwrap();
        let a_idx = store.field_to_idx["a"];
        let b_idx = store.field_to_idx["b"];

        // Write exactly one tuple per (slot, field) — no stale data
        let va = rmp_serde::to_vec(&PackedValue::I(10)).unwrap();
        let vb = rmp_serde::to_vec(&PackedValue::I(20)).unwrap();
        store.append_tuple(0, a_idx, &va).unwrap();
        store.append_tuple(0, b_idx, &vb).unwrap();
        store.append_tuple(1, a_idx, &va).unwrap();
        store.v2_writers.clear();

        let shard_id = DocStore::shard_id(0);
        let path = DocStore::shard_path(&store.root, shard_id);
        let size_before = std::fs::metadata(&path).unwrap().len();
        let data = std::fs::read(&path).unwrap();

        // Compact — should be a no-op (early exit)
        compact_shard_from_buffer(shard_id, &data, &store.root, &store.v2_writers).unwrap();

        let size_after = std::fs::metadata(&path).unwrap().len();
        assert_eq!(
            size_before, size_after,
            "file should be unchanged when there are no stale tuples"
        );
    }

    #[test]
    fn test_janitor_num_tuples_header_correct() {
        // After compaction, bytes 12-15 of the shard file should contain the
        // correct num_tuples count as a little-endian u32.
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        store.ensure_field_idx("val");
        store.ensure_field_idx("extra");
        store.save_field_dict().unwrap();
        let val_idx = store.field_to_idx["val"];
        let extra_idx = store.field_to_idx["extra"];

        // Write 5 versions of val for slot 0 (4 stale + 1 winner)
        // plus 1 tuple for extra field (1 winner)
        // Total = 6 tuples, after compaction = 2 winners
        for v in 0..5i64 {
            let packed = rmp_serde::to_vec(&PackedValue::I(v)).unwrap();
            store.append_tuple(0, val_idx, &packed).unwrap();
        }
        let extra_packed = rmp_serde::to_vec(&PackedValue::I(99)).unwrap();
        store.append_tuple(0, extra_idx, &extra_packed).unwrap();
        store.v2_writers.clear();

        let shard_id = DocStore::shard_id(0);
        let path = DocStore::shard_path(&store.root, shard_id);
        let data = std::fs::read(&path).unwrap();

        // Compact
        compact_shard_from_buffer(shard_id, &data, &store.root, &store.v2_writers).unwrap();

        // Read raw shard and check header bytes 12-15
        let compacted = std::fs::read(&path).unwrap();
        assert!(compacted.len() >= 16, "compacted shard too short");
        let num_tuples = u32::from_le_bytes([
            compacted[12], compacted[13], compacted[14], compacted[15],
        ]);
        assert_eq!(num_tuples, 2, "num_tuples header should be 2 (val + extra)");

        // Verify data is still correct
        let doc = store.get_v2(0).unwrap().unwrap();
        match &doc.fields["val"] {
            FieldValue::Single(Value::Integer(4)) => {}
            other => panic!("expected val=4, got: {:?}", other),
        }
        match &doc.fields["extra"] {
            FieldValue::Single(Value::Integer(99)) => {}
            other => panic!("expected extra=99, got: {:?}", other),
        }
    }

    /// Concurrent append_tuple stress test.
    /// Reproduces the race condition where multiple threads call append_tuple
    /// on different slots (hitting different shards) simultaneously. Before the
    /// fix, get_v2_writer + get(&sid) could panic with unwrap on None under
    /// concurrent DashMap access.
    #[test]
    fn test_concurrent_append_tuple_no_panic() {
        let dir = tempfile::tempdir().unwrap();
        let docs_dir = dir.path().join("docs");
        let mut store = DocStore::open(&docs_dir).unwrap();
        let _bw = store.prepare_bulk_load(&["val".to_string()]).unwrap();
        let val_idx: u16 = 0;
        let store = std::sync::Arc::new(parking_lot::Mutex::new(store));

        let threads: Vec<_> = (0..8)
            .map(|t| {
                let store = std::sync::Arc::clone(&store);
                std::thread::spawn(move || {
                    // Each thread writes to different slots across many shards
                    for i in 0..100 {
                        let slot = (t * 10000 + i * 512) as u32; // spread across shards
                        let packed = rmp_serde::to_vec(&PackedValue::I(t as i64 * 1000 + i)).unwrap();
                        store.lock().append_tuple(slot, val_idx, &packed).unwrap();
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // Verify writes succeeded — just check that no panic occurred
        // and the writers exist for the shards we wrote to
        {
            let s = store.lock();
            assert!(s.v2_writers.len() > 0, "should have created v2 writers");
        }
    }
}
