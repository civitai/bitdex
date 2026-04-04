//! BitmapSilo — persistent bitmap storage backed by DataSilo.
//!
//! Stores filter bitmaps, sort bit-layers, alive bitmap, and metadata
//! in a DataSilo with a manifest that maps logical names to silo keys.
//!
//! Key assignment:
//!   0 = alive bitmap
//!   1 = metadata (slot_counter, cursors, deferred_alive as JSON)
//!   2..N = filter bitmaps (field:value pairs)
//!   N+1..M = sort bit-layers (field:bit_index pairs)
//!
//! The manifest (`manifest.json`) maps logical names to u32 keys and is
//! loaded on startup to reconstruct the key mapping.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use roaring::{FrozenRoaringBitmap, RoaringBitmap};

use crate::filter::FilterIndex;
use crate::sort::SortIndex;
use crate::slot::SlotAllocator;

/// Reserved key for the alive bitmap.
const KEY_ALIVE: u32 = 0;
/// Reserved key for metadata (slot_counter, cursors, deferred alive).
const KEY_META: u32 = 1;
/// First key available for filter/sort bitmaps.
const KEY_BITMAP_START: u32 = 2;

/// Persistent bitmap storage.
pub struct BitmapSilo {
    silo: datasilo::DataSilo,
    path: PathBuf,
    /// Maps logical bitmap name → silo key.
    /// Format: "filter:{field}:{value}" or "sort:{field}:{bit}" → u32
    name_to_key: HashMap<String, u32>,
    /// Reverse mapping for loading.
    key_to_name: HashMap<u32, String>,
    /// Next available key for new bitmaps.
    next_key: u32,
}

impl BitmapSilo {
    /// Open or create a BitmapSilo at the given directory.
    pub fn open(path: &Path) -> io::Result<Self> {
        let silo_path = path.join("bitmap_silo");
        let silo = datasilo::DataSilo::open(
            &silo_path,
            datasilo::SiloConfig {
                buffer_ratio: 1.2,    // bitmaps don't change size much
                min_entry_size: 64,   // small bitmaps are common
                alignment: 32,        // FrozenRoaringBitmap requires 32-byte aligned data
                compact_threshold: 0.20, // compact when 20% dead space
            },
        )?;

        // Load manifest if it exists
        let manifest_path = path.join("bitmap_manifest.json");
        let (name_to_key, key_to_name, next_key) = if manifest_path.exists() {
            let data = std::fs::read_to_string(&manifest_path)?;
            let map: HashMap<String, u32> = serde_json::from_str(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let reverse: HashMap<u32, String> = map.iter().map(|(k, v)| (*v, k.clone())).collect();
            let max_key = map.values().copied().max().unwrap_or(KEY_BITMAP_START);
            (map, reverse, max_key + 1)
        } else {
            (HashMap::new(), HashMap::new(), KEY_BITMAP_START)
        };

        Ok(Self { silo, path: path.to_path_buf(), name_to_key, key_to_name, next_key })
    }

    /// Save the current manifest to disk.
    fn save_manifest(&self) -> io::Result<()> {
        let json = serde_json::to_string_pretty(&self.name_to_key)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(self.path.join("bitmap_manifest.json"), json)
    }

    /// Get or assign a silo key for a logical bitmap name.
    fn ensure_key(&mut self, name: &str) -> u32 {
        if let Some(&key) = self.name_to_key.get(name) {
            return key;
        }
        let key = self.next_key;
        self.next_key += 1;
        self.name_to_key.insert(name.to_string(), key);
        self.key_to_name.insert(key, name.to_string());
        key
    }

    // ── Save ────────────────────────────────────────────────────────────

    /// Save all bitmaps from the engine's in-memory state to the silo.
    pub fn save_all(
        &mut self,
        filters: &FilterIndex,
        sorts: &SortIndex,
        slots: &SlotAllocator,
        cursors: &HashMap<String, String>,
    ) -> io::Result<u64> {
        let mut count = 0u64;

        // Save alive bitmap in frozen format
        let alive = slots.alive_bitmap();
        let size = alive.frozen_serialized_size();
        let mut buf = vec![0u8; size];
        alive.serialize_frozen_into(&mut buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("frozen serialize alive: {e:?}")))?;
        self.silo.append_op(KEY_ALIVE, &buf)?;
        count += 1;

        // Save metadata
        let meta = serde_json::json!({
            "slot_counter": slots.slot_counter(),
            "cursors": cursors,
        });
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.silo.append_op(KEY_META, &meta_bytes)?;
        count += 1;

        // Save filter bitmaps in CRoaring frozen format (zero-copy mmap reads)
        for (field_name, field) in filters.fields() {
            for (value, bitmap) in field.bitmaps_fused() {
                let name = format!("filter:{}:{}", field_name, value);
                let key = self.ensure_key(&name);
                let size = bitmap.frozen_serialized_size();
                let mut buf = vec![0u8; size];
                bitmap.serialize_frozen_into(&mut buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("frozen serialize: {e:?}")))?;
                self.silo.append_op(key, &buf)?;
                count += 1;
            }
        }

        // Save sort bit-layers
        for (field_name, field) in sorts.fields() {
            for (bit_idx, bitmap) in field.layers_fused().iter().enumerate() {
                if bitmap.is_empty() { continue; }
                let name = format!("sort:{}:{}", field_name, bit_idx);
                let key = self.ensure_key(&name);
                let size = bitmap.frozen_serialized_size();
                let mut buf = vec![0u8; size];
                bitmap.serialize_frozen_into(&mut buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("frozen serialize: {e:?}")))?;
                self.silo.append_op(key, &buf)?;
                count += 1;
            }
        }

        // Compact to write everything to the data file
        self.silo.compact()?;

        // Save manifest
        self.save_manifest()?;

        Ok(count)
    }

    // ── Load ────────────────────────────────────────────────────────────

    /// Load alive bitmap from the silo via FrozenRoaringBitmap::view() → to_owned().
    pub fn load_alive(&self) -> io::Result<Option<RoaringBitmap>> {
        match self.silo.get(KEY_ALIVE) {
            Some(bytes) => {
                let frozen = roaring::FrozenRoaringBitmap::view(bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("frozen alive: {e:?}")))?;
                Ok(Some(frozen.to_owned()))
            }
            None => Ok(None),
        }
    }

    /// Load metadata from the silo.
    pub fn load_meta(&self) -> io::Result<Option<serde_json::Value>> {
        match self.silo.get(KEY_META) {
            Some(bytes) => {
                let meta: serde_json::Value = serde_json::from_slice(bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Load all filter bitmaps into a FilterIndex.
    pub fn load_filters(&self, filters: &mut FilterIndex) -> io::Result<u64> {
        let mut count = 0u64;
        for (name, &key) in &self.name_to_key {
            if !name.starts_with("filter:") { continue; }
            let bytes = match self.silo.get(key) {
                Some(b) => b,
                None => continue,
            };
            // Parse "filter:{field}:{value}"
            let parts: Vec<&str> = name.splitn(3, ':').collect();
            if parts.len() != 3 { continue; }
            let field_name = parts[1];
            let value: u64 = match parts[2].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let frozen = roaring::FrozenRoaringBitmap::view(bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{name}: {e:?}")))?;
            let bitmap = frozen.to_owned();
            if let Some(field) = filters.get_field_mut(field_name) {
                field.or_bitmap(value, &bitmap);
                count += 1;
            }
        }
        Ok(count)
    }

    /// Load all sort bit-layers into a SortIndex.
    pub fn load_sorts(&self, sorts: &mut SortIndex) -> io::Result<u64> {
        let mut count = 0u64;
        // Collect all sort layers per field
        let mut field_layers: HashMap<String, Vec<(usize, RoaringBitmap)>> = HashMap::new();

        for (name, &key) in &self.name_to_key {
            if !name.starts_with("sort:") { continue; }
            let bytes = match self.silo.get(key) {
                Some(b) => b,
                None => continue,
            };
            // Parse "sort:{field}:{bit_index}"
            let parts: Vec<&str> = name.splitn(3, ':').collect();
            if parts.len() != 3 { continue; }
            let field_name = parts[1];
            let bit_idx: usize = match parts[2].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let frozen = roaring::FrozenRoaringBitmap::view(bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("sort {name}: {e:?}")))?;
            let bitmap = frozen.to_owned();
            field_layers.entry(field_name.to_string()).or_default().push((bit_idx, bitmap));
            count += 1;
        }

        // Apply layers to sort fields
        for (field_name, layers) in field_layers {
            if let Some(field) = sorts.get_field_mut(&field_name) {
                // Sort by bit index
                let mut sorted_layers: Vec<RoaringBitmap> = Vec::new();
                let max_bit = layers.iter().map(|(i, _)| *i).max().unwrap_or(0);
                sorted_layers.resize_with(max_bit + 1, RoaringBitmap::new);
                for (bit_idx, bitmap) in layers {
                    sorted_layers[bit_idx] = bitmap;
                }
                field.load_layers(sorted_layers);
            }
        }

        Ok(count)
    }

    /// Load all bitmaps and metadata, populating the engine state.
    /// Returns (slot_counter, cursors, filter_count, sort_count).
    pub fn load_all(
        &self,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) -> io::Result<(Option<u32>, HashMap<String, String>, u64, u64)> {
        let meta = self.load_meta()?;
        let slot_counter = meta.as_ref()
            .and_then(|m| m.get("slot_counter"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        let cursors: HashMap<String, String> = meta.as_ref()
            .and_then(|m| m.get("cursors"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let filter_count = self.load_filters(filters)?;
        let sort_count = self.load_sorts(sorts)?;

        Ok((slot_counter, cursors, filter_count, sort_count))
    }

    /// Check if the silo has data (non-empty data file or ops).
    pub fn has_data(&self) -> bool {
        self.silo.data_bytes() > 0 || self.silo.has_ops()
    }

    /// Whether the silo needs compaction (dead space exceeds threshold).
    pub fn needs_compaction(&self) -> bool {
        self.silo.needs_compaction()
    }

    /// Compact the silo — merge ops into the data file, reclaim dead space.
    pub fn compact(&mut self) -> io::Result<u64> {
        self.silo.compact()
    }

    // ── Frozen accessors (zero-copy from mmap) ────────────────────────

    /// Get a frozen bitmap view for a filter field+value directly from the mmap.
    /// Returns None if the field+value isn't in the silo.
    pub fn get_frozen_filter(&self, field: &str, value: u64) -> Option<FrozenRoaringBitmap<'_>> {
        let name = format!("filter:{}:{}", field, value);
        let key = self.name_to_key.get(&name)?;
        let bytes = self.silo.get(*key)?;
        FrozenRoaringBitmap::view(bytes).ok()
    }

    /// Get a frozen bitmap view for a sort bit-layer directly from the mmap.
    /// Returns None if the field+bit isn't in the silo.
    pub fn get_frozen_sort_layer(&self, field: &str, bit: usize) -> Option<FrozenRoaringBitmap<'_>> {
        let name = format!("sort:{}:{}", field, bit);
        let key = self.name_to_key.get(&name)?;
        let bytes = self.silo.get(*key)?;
        FrozenRoaringBitmap::view(bytes).ok()
    }

    /// Iterate all filter (field_name, value) pairs stored in the silo.
    pub fn filter_entries(&self) -> impl Iterator<Item = (&str, u64)> {
        self.name_to_key.keys().filter_map(|name| {
            let stripped = name.strip_prefix("filter:")?;
            let (field, val_str) = stripped.rsplit_once(':')?;
            let value: u64 = val_str.parse().ok()?;
            Some((field, value))
        })
    }

    /// Check if a sort field has any layers stored.
    pub fn has_sort_field(&self, field: &str) -> bool {
        let prefix = format!("sort:{}:", field);
        self.name_to_key.keys().any(|k| k.starts_with(&prefix))
    }

    // ── Backed loading (mark as unloaded, read frozen at query time) ──

    /// Mark all filter values in the silo as backed (unloaded) in the FilterIndex.
    /// Creates VersionedBitmap::new_unloaded() placeholders so the executor knows
    /// to fall back to frozen reads from the silo.
    pub fn mark_filters_backed(&self, filters: &mut FilterIndex) -> u64 {
        let mut count = 0u64;
        for (name, &_key) in &self.name_to_key {
            if !name.starts_with("filter:") { continue; }
            let parts: Vec<&str> = name.splitn(3, ':').collect();
            if parts.len() != 3 { continue; }
            let field_name = parts[1];
            let value: u64 = match parts[2].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Some(field) = filters.get_field_mut(field_name) {
                field.mark_value_backed(value);
                count += 1;
            }
        }
        count
    }

    /// Mark all sort layers in the silo as backed (unloaded) in the SortIndex.
    pub fn mark_sorts_backed(&self, sorts: &mut SortIndex) -> u64 {
        let mut count = 0u64;
        // Collect field names that have sort data
        let mut fields: HashMap<String, usize> = HashMap::new();
        for name in self.name_to_key.keys() {
            if !name.starts_with("sort:") { continue; }
            let parts: Vec<&str> = name.splitn(3, ':').collect();
            if parts.len() != 3 { continue; }
            let field_name = parts[1];
            let bit_idx: usize = match parts[2].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let max = fields.entry(field_name.to_string()).or_insert(0);
            if bit_idx > *max { *max = bit_idx; }
            count += 1;
        }
        for (field_name, _max_bit) in &fields {
            if let Some(field) = sorts.get_field_mut(field_name) {
                field.mark_layers_backed();
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        // Build in-memory state
        let mut filters = FilterIndex::new();
        filters.add_field(FilterFieldConfig {
            name: "nsfwLevel".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        // Insert some bitmaps
        let field = filters.get_field_mut("nsfwLevel").unwrap();
        let mut bm1 = RoaringBitmap::new();
        bm1.insert_range(0..100);
        field.or_bitmap(1, &bm1);
        let mut bm5 = RoaringBitmap::new();
        bm5.insert_range(100..200);
        field.or_bitmap(5, &bm5);

        let mut sorts = SortIndex::new();
        sorts.add_field(SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        // Insert some sort layers
        let sort_field = sorts.get_field_mut("sortAt").unwrap();
        let mut layer0 = RoaringBitmap::new();
        layer0.insert_range(0..50);
        sort_field.or_layer(0, &layer0);

        let mut slots = SlotAllocator::new();
        // Simulate alive state
        let alive = {
            let mut bm = RoaringBitmap::new();
            bm.insert_range(0..200);
            bm
        };
        slots = SlotAllocator::from_state(200, alive, RoaringBitmap::new());

        let cursors = HashMap::from([("wal".to_string(), "100".to_string())]);

        // Save
        let mut silo = BitmapSilo::open(dir.path()).unwrap();
        let saved = silo.save_all(&filters, &sorts, &slots, &cursors).unwrap();
        assert!(saved > 0);
        drop(silo);

        // Load into fresh state
        let silo = BitmapSilo::open(dir.path()).unwrap();
        assert!(silo.has_data());

        // Load alive
        let loaded_alive = silo.load_alive().unwrap().unwrap();
        assert_eq!(loaded_alive.len(), 200);

        // Load meta
        let meta = silo.load_meta().unwrap().unwrap();
        assert_eq!(meta["slot_counter"], 200);
        assert_eq!(meta["cursors"]["wal"], "100");

        // Load filters
        let mut new_filters = FilterIndex::new();
        new_filters.add_field(FilterFieldConfig {
            name: "nsfwLevel".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        let filter_count = silo.load_filters(&mut new_filters).unwrap();
        assert_eq!(filter_count, 2); // two values: 1 and 5
        let nf = new_filters.get_field("nsfwLevel").unwrap();
        assert_eq!(nf.get(1).unwrap().len(), 100);
        assert_eq!(nf.get(5).unwrap().len(), 100);

        // Load sorts
        let mut new_sorts = SortIndex::new();
        new_sorts.add_field(SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        let sort_count = silo.load_sorts(&mut new_sorts).unwrap();
        assert!(sort_count > 0);
    }

    #[test]
    fn test_frozen_accessors() {
        let dir = tempfile::tempdir().unwrap();

        // Build and save
        let mut filters = FilterIndex::new();
        filters.add_field(FilterFieldConfig {
            name: "nsfwLevel".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        let field = filters.get_field_mut("nsfwLevel").unwrap();
        let mut bm1 = RoaringBitmap::new();
        bm1.insert_range(0..100);
        field.or_bitmap(1, &bm1);

        let mut sorts = SortIndex::new();
        sorts.add_field(SortFieldConfig {
            name: "sortAt".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        let sort_field = sorts.get_field_mut("sortAt").unwrap();
        let mut layer0 = RoaringBitmap::new();
        layer0.insert_range(0..50);
        sort_field.or_layer(0, &layer0);

        let slots = crate::slot::SlotAllocator::from_state(100, {
            let mut bm = RoaringBitmap::new();
            bm.insert_range(0..100);
            bm
        }, RoaringBitmap::new());
        let cursors = std::collections::HashMap::new();

        let mut silo = BitmapSilo::open(dir.path()).unwrap();
        silo.save_all(&filters, &sorts, &slots, &cursors).unwrap();
        drop(silo);

        // Reopen and test frozen accessors
        let silo = BitmapSilo::open(dir.path()).unwrap();

        // Frozen filter read
        let frozen = silo.get_frozen_filter("nsfwLevel", 1).expect("should find frozen filter");
        assert_eq!(frozen.len(), 100);
        assert!(frozen.contains(50));
        assert!(!frozen.contains(100));

        // Frozen sort layer read
        let frozen_layer = silo.get_frozen_sort_layer("sortAt", 0).expect("should find frozen sort layer");
        assert_eq!(frozen_layer.len(), 50);

        // Mark backed and verify
        let mut new_filters = FilterIndex::new();
        new_filters.add_field(FilterFieldConfig {
            name: "nsfwLevel".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        let count = silo.mark_filters_backed(&mut new_filters);
        assert_eq!(count, 1);
        let field = new_filters.get_field("nsfwLevel").unwrap();
        let vb = field.get_versioned(1).expect("should have unloaded placeholder");
        assert!(!vb.is_loaded(), "should be marked as unloaded");
    }
}
