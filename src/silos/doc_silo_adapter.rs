//! DocSiloAdapter — DataSilo-backed document store with field dictionary encoding.
//!
//! Provides the get/put interface used by ConcurrentEngine, mutation, and ops_processor,
//! backed by DataSilo's mmap'd storage.
//!
//! The adapter manages:
//! - Field name ↔ index mappings (field dictionary)
//! - Encoding/decoding via DocOpCodec format (71ns encode, 16ns decode)
//! - Schema versioning and field defaults
//! - ParallelWriter creation for dump pipeline

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use crate::config::DataSchema;
use crate::silos::doc_format::{self, PackedValue, StoredDoc};

/// Offset applied to slot IDs to avoid HashIndex key=0 sentinel collision.
/// Slot 0 maps to key 1, slot 1 to key 2, etc.
const SLOT_KEY_OFFSET: u64 = 1;

/// Convert a slot ID to a DataSilo key (offset by 1 to avoid key=0 sentinel).
/// Public so dump_processor can use it for direct parallel writes.
#[inline]
pub fn slot_to_key(slot: u32) -> u64 {
    slot as u64 + SLOT_KEY_OFFSET
}

/// DataSilo-backed document store adapter.
pub struct DocSiloAdapter {
    silo: datasilo::DataSilo,
    root: PathBuf,
    field_to_idx: HashMap<String, u16>,
    idx_to_field: Vec<String>,
    field_defaults: HashMap<u16, PackedValue>,
    schema_version: u8,
}

impl DocSiloAdapter {
    /// Open or create a DocSiloAdapter at the given directory.
    pub fn open(path: &Path) -> io::Result<Self> {
        let silo_path = path.join("doc_silo");
        // Higher buffer_ratio (4x) because dump pipeline writes image fields first,
        // then subsequent phases merge in tags, resources, tools, techniques, metrics.
        // The final doc can be 3-5x the initial image-only size.
        // Higher min_entry_size (1024) ensures even small docs have room for merges.
        let config = datasilo::SiloConfig {
            buffer_ratio: 4.0,
            min_entry_size: 1024,
            ..datasilo::SiloConfig::default()
        };
        let mut silo = datasilo::DataSilo::open(&silo_path, config)?;

        // Set merge function so compaction merges Mi arrays instead of LWW.
        silo.set_merge_fn(|existing, new_data| {
            doc_format::merge_encoded_docs(existing, new_data)
                .unwrap_or_else(|_| new_data.to_vec())
        });

        // Load field dictionary from disk if it exists
        let dict_path = path.join("field_dict.json");
        let (field_to_idx, idx_to_field) = if dict_path.exists() {
            let data = std::fs::read_to_string(&dict_path)?;
            let dict: Vec<String> = serde_json::from_str(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let f2i: HashMap<String, u16> = dict.iter().enumerate()
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
            field_defaults: HashMap::new(),
            schema_version: 0,
        })
    }

    /// Open a temporary adapter (for testing). Uses a unique temp directory.
    pub fn open_temp() -> io::Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bitdex_doc_silo_{}_{}", std::process::id(), id
        ));
        let _ = std::fs::remove_dir_all(&path); // clean up previous
        Self::open(&path)
    }

    /// Get a document by slot ID.
    pub fn get(&self, slot: u32) -> io::Result<Option<StoredDoc>> {
        let bytes = match self.silo.get_with_ops(slot_to_key(slot)) {
            Some(b) => b,
            None => return Ok(None),
        };
        if bytes.is_empty() {
            return Ok(None);
        }
        doc_format::decode_stored_doc(&bytes, &self.idx_to_field, Some(&self.field_defaults))
            .map(Some)
    }

    /// Write a document to the silo (via ops log for online mutations).
    /// Auto-registers any new field names encountered.
    pub fn put(&mut self, slot: u32, doc: &StoredDoc) -> io::Result<()> {
        let fields = self.encode_stored_doc_auto(doc);
        let bytes = doc_format::encode_merge_fields(slot, &fields);
        self.silo.append_op(slot_to_key(slot), &bytes)
    }

    /// Write a batch of documents. Auto-registers any new field names.
    pub fn put_batch(&mut self, docs: &[(u32, StoredDoc)]) -> io::Result<()> {
        let ops: Vec<(u64, Vec<u8>)> = docs.iter().map(|(slot, doc)| {
            let fields = self.encode_stored_doc_auto(doc);
            (slot_to_key(*slot), doc_format::encode_merge_fields(*slot, &fields))
        }).collect();
        self.silo.append_ops_batch(&ops)
    }

    /// Encode a StoredDoc to (field_idx, PackedValue) pairs.
    /// Auto-registers any new field names not yet in the dictionary.
    fn encode_stored_doc_auto(&mut self, doc: &StoredDoc) -> Vec<(u16, PackedValue)> {
        let mut fields = Vec::with_capacity(doc.fields.len());
        for (name, value) in &doc.fields {
            let idx = if let Some(&idx) = self.field_to_idx.get(name) {
                idx
            } else {
                let idx = self.idx_to_field.len() as u16;
                self.field_to_idx.insert(name.clone(), idx);
                self.idx_to_field.push(name.clone());
                idx
            };
            fields.push((idx, doc_format::field_value_to_packed(value)));
        }
        fields
    }

    /// Get the field name → index mapping.
    pub fn field_to_idx(&self) -> &HashMap<String, u16> {
        &self.field_to_idx
    }

    /// Get the field index → name mapping.
    pub fn idx_to_field(&self) -> &[String] {
        &self.idx_to_field
    }

    /// Ensure a field name has an index, creating one if needed.
    pub fn ensure_field_index(&mut self, name: &str) -> io::Result<u16> {
        if let Some(&idx) = self.field_to_idx.get(name) {
            return Ok(idx);
        }
        let idx = self.idx_to_field.len() as u16;
        self.field_to_idx.insert(name.to_string(), idx);
        self.idx_to_field.push(name.to_string());
        Ok(idx)
    }

    /// Get a snapshot of the field dictionary.
    pub fn field_dict_snapshot(&self) -> HashMap<String, u16> {
        self.field_to_idx.clone()
    }

    /// Persist the field dictionary to disk.
    pub fn save_field_dict(&self) -> io::Result<()> {
        let dict_path = self.root.join("field_dict.json");
        let json = serde_json::to_string_pretty(&self.idx_to_field)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(&dict_path, json)
    }

    /// Set field defaults from a DataSchema.
    pub fn set_field_defaults(&mut self, schema: &DataSchema) {
        for mapping in &schema.fields {
            if let Some(ref default_val) = mapping.default_value {
                if let Some(&idx) = self.field_to_idx.get(&mapping.target) {
                    if let Some(pv) = crate::silos::doc_format::json_to_packed_with_dict(
                        default_val, mapping, false, None,
                    ) {
                        self.field_defaults.insert(idx, pv);
                    }
                }
            }
        }
    }

    /// Get the current schema version.
    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    /// Build schema registry (compatibility stub — returns empty).
    pub fn build_schema_registry(&self) -> HashMap<u8, HashMap<String, serde_json::Value>> {
        HashMap::new()
    }

    /// Get root path.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Get the underlying DataSilo (for ParallelWriter creation during dump).
    pub fn silo_mut(&mut self) -> &mut datasilo::DataSilo {
        &mut self.silo
    }

    /// Get the underlying DataSilo (shared reference).
    pub fn silo(&self) -> &datasilo::DataSilo {
        &self.silo
    }

    /// Create a DumpMergeWriter for direct read-modify-write during dump phases.
    /// Returns None if the data file doesn't exist yet (images phase hasn't run).
    pub fn prepare_dump_merge(&self) -> io::Result<Option<datasilo::DumpMergeWriter>> {
        self.silo.prepare_dump_merge()
    }

    /// Reload the data mmap after dump merge writes complete.
    pub fn reload_data(&mut self) -> io::Result<()> {
        self.silo.reload_data()
    }

    /// Compact the silo (apply pending ops).
    pub fn compact(&mut self) -> io::Result<bool> {
        let count = self.silo.compact()?;
        Ok(count > 0)
    }

    /// Pin generation (compatibility stub — DataSilo doesn't use generations).
    pub fn pin_generation(&self) -> io::Result<u64> {
        Ok(0)
    }

    /// Prepare field names for writing (ensures all field names have indexes).
    pub fn prepare_field_names(&mut self, field_names: &[String]) -> io::Result<()> {
        for name in field_names {
            self.ensure_field_index(name)?;
        }
        self.save_field_dict()
    }

    /// Get all documents in a shard (treating shard_id as a slot range).
    ///
    /// With DataSilo, documents are stored per-slot rather than per-file-shard.
    /// This method returns a single-element vec for the slot at `shard_id`, or an
    /// empty vec if the slot has no document.  Callers that iterate over a range of
    /// shard IDs therefore get one slot per call — consistent with the DataSilo model.
    pub fn get_shard(&self, shard_id: u32) -> io::Result<Vec<(u32, StoredDoc)>> {
        match self.get(shard_id)? {
            Some(doc) => Ok(vec![(shard_id, doc)]),
            None => Ok(Vec::new()),
        }
    }

    /// Get all documents in a shard in packed (index-keyed) form.
    ///
    /// Returns `Vec<(slot_id, Vec<(field_idx, PackedValue)>)>` without converting
    /// field indices to names.  Used by the packed-rebuild benchmark path that avoids
    /// the `StoredDoc` HashMap allocation entirely.
    pub fn get_shard_packed(&self, shard_id: u32) -> io::Result<Vec<(u32, Vec<(u16, PackedValue)>)>> {
        let bytes = match self.silo.get_with_ops(shard_id as u64) {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let fields = doc_format::decode_doc_fields(&bytes)?;
        Ok(vec![(shard_id, fields)])
    }

    /// Get the data root path.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::FieldValue;
    use crate::query::Value;

    #[test]
    fn test_roundtrip() {
        let mut adapter = DocSiloAdapter::open_temp().unwrap();
        adapter.ensure_field_index("name").unwrap();
        adapter.ensure_field_index("score").unwrap();

        let mut fields = HashMap::new();
        fields.insert("name".to_string(), FieldValue::Single(Value::String("test".into())));
        fields.insert("score".to_string(), FieldValue::Single(Value::Integer(42)));
        let doc = StoredDoc { fields, schema_version: 0 };

        adapter.put(1, &doc).unwrap();
        let loaded = adapter.get(1).unwrap().unwrap();
        assert_eq!(loaded.fields.len(), 2);
        assert_eq!(
            loaded.fields.get("name"),
            Some(&FieldValue::Single(Value::String("test".into())))
        );
    }

    #[test]
    fn test_put_batch() {
        let mut adapter = DocSiloAdapter::open_temp().unwrap();
        adapter.ensure_field_index("x").unwrap();

        let docs: Vec<(u32, StoredDoc)> = (0..10).map(|i| {
            let mut fields = HashMap::new();
            fields.insert("x".to_string(), FieldValue::Single(Value::Integer(i as i64)));
            (i, StoredDoc { fields, schema_version: 0 })
        }).collect();

        adapter.put_batch(&docs).unwrap();
        for i in 0..10 {
            let doc = adapter.get(i).unwrap().unwrap();
            assert_eq!(doc.fields.get("x"), Some(&FieldValue::Single(Value::Integer(i as i64))));
        }
    }
}
