//! BitmapSilo — persistent bitmap storage backed by DataSilo.
//!
//! Stores filter bitmaps, sort bit-layers, alive bitmap, and metadata
//! in a DataSilo. Keys are computed deterministically from (field_id, value)
//! using the bitmap_keys encoding — no string manifest needed.
//!
//! ## Key encoding
//!
//! All silo keys are u64 values produced by `crate::silos::bitmap_keys`:
//! - `KEY_ALIVE` (1) — alive bitmap
//! - `KEY_META` (2) — metadata JSON (slot_counter, cursors)
//! - `encode_filter_key(field_id, value)` — filter bitmaps
//! - `encode_sort_key(field_id, bit_layer)` — sort bit-layers
//! - `encode_bucket_key(field_id, bucket_id)` — time bucket bitmaps
//!
//! Field names are mapped to stable u16 IDs via `FieldRegistry` (persisted to
//! `field_registry.bin` in the silo directory). Bucket names are treated as
//! additional fields and registered in the same registry.
//!
//! ## DataSilo index compatibility note
//!
//! The current `DataSilo` uses a flat array index (`key as usize` offset).
//! This is incompatible with large u64 keys produced by `encode_filter_key`
//! (minimum key = `1 << 48` ≈ 281 trillion). All bitmap ops therefore remain
//! in the ops log and are read back via `scan_ops_for_key` / ops-on-read.
//!
//! When DataSilo is upgraded to use `HashIndex` for the data file (planned),
//! `compact()` and `get_frozen_*` will work correctly with large keys.
//! Until then: do NOT call `compact()` on a BitmapSilo with encoded keys.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use roaring::{FrozenRoaringBitmap, RoaringBitmap};

use crate::engine::filter::FilterIndex;
use crate::engine::sort::SortIndex;
use crate::engine::slot::SlotAllocator;
use crate::silos::bitmap_keys::{
    decode_key, encode_bucket_key, encode_filter_key, encode_sort_key,
    DecodedKey, KEY_ALIVE, KEY_META,
};
use crate::silos::field_registry::FieldRegistry;

// Ops value type tags for bitmap mutations
const OP_FULL_BITMAP: u8 = 0x00;  // Full frozen bitmap (from save_all/compaction)
const OP_SET_BIT: u8 = 0x01;      // Set a single bit: [0x01][u32 slot]
const OP_CLEAR_BIT: u8 = 0x02;    // Clear a single bit: [0x02][u32 slot]

/// Persistent bitmap storage.
pub struct BitmapSilo {
    silo: datasilo::DataSilo,
    /// Maps field names to stable u16 IDs. Persisted to `field_registry.bin`.
    /// Mutex for interior mutability — `ensure()` on first encounter of a new field.
    field_registry: parking_lot::Mutex<FieldRegistry>,
}

impl BitmapSilo {
    /// Open or create a BitmapSilo at the given directory.
    pub fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;
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

        // Open field registry at the silo directory (same level as bitmap_silo/)
        let field_registry = FieldRegistry::open(path)?;

        Ok(Self {
            silo,
            field_registry: parking_lot::Mutex::new(field_registry),
        })
    }

    // ── Field ID helpers ─────────────────────────────────────────────────

    /// Get the field_id for a field, returning an error if not registered.
    /// Used for read paths — reading a field that was never written returns None.
    #[inline]
    fn field_id(&self, name: &str) -> Option<u16> {
        self.field_registry.lock().get(name)
    }

    /// Get or assign a field_id for a field.
    /// Used for write paths — registers new fields on first write.
    #[inline]
    fn ensure_field_id(&self, name: &str) -> io::Result<u16> {
        self.field_registry.lock().ensure(name)
    }

    // ── Save ────────────────────────────────────────────────────────────

    /// Save all bitmaps from the engine's in-memory state to the silo.
    ///
    /// Writes all bitmaps as ops to the ops log. Does NOT compact — compaction
    /// requires DataSilo to support HashIndex for large u64 keys (planned upgrade).
    pub fn save_all(
        &mut self,
        filters: &FilterIndex,
        sorts: &SortIndex,
        slots: &SlotAllocator,
        cursors: &HashMap<String, String>,
    ) -> io::Result<u64> {
        let mut count = 0u64;

        // Save alive bitmap — prefix with OP_FULL_BITMAP so get_bitmap_with_ops
        // recognises it as a full snapshot (not a set/clear op).
        // Uses standard (portable) serialization — alignment-independent in ops log.
        let alive = slots.alive_bitmap();
        let std_size = alive.serialized_size();
        let mut buf = vec![OP_FULL_BITMAP; 1 + std_size];
        alive.serialize_into(&mut buf[1..])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize alive: {e}")))?;
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

        // Pre-register all field names (single save to field_registry.bin)
        {
            let filter_names: Vec<&str> = filters.fields().map(|(n, _)| n.as_str()).collect();
            let sort_names: Vec<&str> = sorts.fields().map(|(n, _)| n.as_str()).collect();
            let all_names: Vec<&str> = filter_names.into_iter().chain(sort_names).collect();
            self.field_registry.lock().ensure_all(&all_names)?;
        }

        // Save filter bitmaps — prefix with OP_FULL_BITMAP for ops log disambiguation.
        // Uses standard (portable) serialization — frozen format requires alignment
        // not guaranteed in the ops log.
        for (field_name, field) in filters.fields() {
            let field_id = self.ensure_field_id(field_name)?;
            for (value, bitmap) in field.bitmaps_fused() {
                let key = encode_filter_key(field_id, value);
                let std_size = bitmap.serialized_size();
                let mut buf = vec![OP_FULL_BITMAP; 1 + std_size];
                bitmap.serialize_into(&mut buf[1..])
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize filter: {e}")))?;
                self.silo.append_op(key, &buf)?;
                count += 1;
            }
        }

        // Save sort bit-layers — prefix with OP_FULL_BITMAP, standard serialization
        for (field_name, field) in sorts.fields() {
            let field_id = self.ensure_field_id(field_name)?;
            for (bit_idx, bitmap) in field.layers_fused().iter().enumerate() {
                if bitmap.is_empty() { continue; }
                let key = encode_sort_key(field_id, bit_idx as u32);
                let std_size = bitmap.serialized_size();
                let mut buf = vec![OP_FULL_BITMAP; 1 + std_size];
                bitmap.serialize_into(&mut buf[1..])
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize sort: {e}")))?;
                self.silo.append_op(key, &buf)?;
                count += 1;
            }
        }

        // NOTE: Compaction is intentionally skipped. DataSilo's flat array index
        // does not support large u64 keys (e.g., encode_filter_key(1, 0) ≈ 2^48).
        // Compact will be re-enabled when DataSilo upgrades to HashIndex.

        Ok(count)
    }

    /// Save all bitmaps using parallel writes for maximum throughput.
    /// Serializes bitmaps in parallel via rayon, writes ops to the log.
    ///
    /// NOTE: Does not compact — see `save_all` for the reason.
    pub fn save_all_parallel(
        &mut self,
        filters: &FilterIndex,
        sorts: &SortIndex,
        slots: &SlotAllocator,
        cursors: &HashMap<String, String>,
    ) -> io::Result<u64> {
        use rayon::prelude::*;

        // Step 1: Alive + metadata (small, sequential)
        // Prefix alive bitmap with OP_FULL_BITMAP for ops log disambiguation.
        // Standard serialization — frozen format requires alignment not guaranteed in ops log.
        let alive = slots.alive_bitmap();
        let alive_size = alive.serialized_size();
        let mut alive_buf = vec![OP_FULL_BITMAP; 1 + alive_size];
        alive.serialize_into(&mut alive_buf[1..])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize alive: {e}")))?;

        let meta = serde_json::json!({
            "slot_counter": slots.slot_counter(),
            "cursors": cursors,
        });
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Step 2: Pre-register all field names
        {
            let filter_names: Vec<&str> = filters.fields().map(|(n, _)| n.as_str()).collect();
            let sort_names: Vec<&str> = sorts.fields().map(|(n, _)| n.as_str()).collect();
            let all_names: Vec<&str> = filter_names.into_iter().chain(sort_names).collect();
            self.field_registry.lock().ensure_all(&all_names)?;
        }

        // Step 3: Collect (key, RoaringBitmap) pairs
        let filter_items: Vec<(u64, RoaringBitmap)> = filters.fields()
            .flat_map(|(field_name, field)| {
                let field_id = self.field_registry.lock().get(field_name)
                    .expect("field was just registered");
                field.bitmaps_fused().map(move |(value, bitmap)| {
                    let key = encode_filter_key(field_id, value);
                    (key, bitmap)
                })
            })
            .collect();

        let sort_items: Vec<(u64, RoaringBitmap)> = sorts.fields()
            .flat_map(|(field_name, field)| {
                let field_id = self.field_registry.lock().get(field_name)
                    .expect("field was just registered");
                field.layers_fused().into_iter().enumerate()
                    .filter(|(_, bm)| !bm.is_empty())
                    .map(move |(bit_idx, bitmap)| {
                        let key = encode_sort_key(field_id, bit_idx as u32);
                        (key, bitmap)
                    })
            })
            .collect();

        // Step 4: Parallel serialize to standard bytes (with OP_FULL_BITMAP prefix).
        // Standard serialization used — frozen format requires alignment not
        // guaranteed in the ops log.
        let filter_bufs: Vec<(u64, Vec<u8>)> = filter_items.par_iter()
            .map(|(key, bitmap)| {
                let size = bitmap.serialized_size();
                let mut buf = vec![OP_FULL_BITMAP; 1 + size];
                bitmap.serialize_into(&mut buf[1..]).ok();
                (*key, buf)
            })
            .collect();

        let sort_bufs: Vec<(u64, Vec<u8>)> = sort_items.par_iter()
            .map(|(key, bitmap)| {
                let size = bitmap.serialized_size();
                let mut buf = vec![OP_FULL_BITMAP; 1 + size];
                bitmap.serialize_into(&mut buf[1..]).ok();
                (*key, buf)
            })
            .collect();

        // Step 5: Write to ops log (sequential — ops log is not parallel-writable here)
        self.silo.append_op(KEY_ALIVE, &alive_buf)?;
        self.silo.append_op(KEY_META, &meta_bytes)?;
        let mut count = 2u64;

        for (key, buf) in filter_bufs.iter().chain(sort_bufs.iter()) {
            self.silo.append_op(*key, buf)?;
            count += 1;
        }

        // NOTE: Compaction skipped — DataSilo needs HashIndex upgrade for large keys.

        Ok(count)
    }

    /// Write dump-produced bitmap maps directly to the silo (no staging roundtrip).
    ///
    /// Takes the raw HashMaps from the dump merge phase, serializes each bitmap
    /// in frozen format via rayon, and appends ops to the log.
    ///
    /// NOTE: Compaction is not performed here — DataSilo's flat array index cannot
    /// handle the large u64 keys produced by encode_filter_key/encode_sort_key.
    pub fn write_dump_maps(
        &mut self,
        filter_maps: std::collections::HashMap<String, std::collections::HashMap<u64, RoaringBitmap>>,
        sort_maps: std::collections::HashMap<String, Vec<RoaringBitmap>>,
        alive: &RoaringBitmap,
        slot_counter: u32,
        cursors: &std::collections::HashMap<String, String>,
    ) -> io::Result<u64> {
        use rayon::prelude::*;

        // Alive bitmap — prefix with OP_FULL_BITMAP for ops log disambiguation.
        // Standard serialization — frozen format requires alignment not guaranteed in ops log.
        let alive_size = alive.serialized_size();
        let mut alive_buf = vec![OP_FULL_BITMAP; 1 + alive_size];
        alive.serialize_into(&mut alive_buf[1..])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize alive: {e}")))?;

        // Metadata
        let meta = serde_json::json!({
            "slot_counter": slot_counter,
            "cursors": cursors,
        });
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Pre-register all field names
        {
            let filter_names: Vec<&str> = filter_maps.keys().map(|s| s.as_str()).collect();
            let sort_names: Vec<&str> = sort_maps.keys().map(|s| s.as_str()).collect();
            let all_names: Vec<&str> = filter_names.into_iter().chain(sort_names).collect();
            self.field_registry.lock().ensure_all(&all_names)?;
        }

        // Collect filter bitmap (key, bitmap) pairs
        let filter_items: Vec<(u64, RoaringBitmap)> = filter_maps.into_iter()
            .flat_map(|(field_name, value_map)| {
                let field_id = self.field_registry.lock().get(&field_name)
                    .expect("field was just registered");
                value_map.into_iter().map(move |(value, bitmap)| {
                    let key = encode_filter_key(field_id, value);
                    (key, bitmap)
                })
            })
            .collect();

        // Collect sort bitmap (key, bitmap) pairs
        let sort_items: Vec<(u64, RoaringBitmap)> = sort_maps.into_iter()
            .flat_map(|(field_name, layers)| {
                let field_id = self.field_registry.lock().get(&field_name)
                    .expect("field was just registered");
                layers.into_iter().enumerate()
                    .filter(|(_, bm)| !bm.is_empty())
                    .map(move |(bit_idx, bitmap)| {
                        let key = encode_sort_key(field_id, bit_idx as u32);
                        (key, bitmap)
                    })
            })
            .collect();

        // Parallel serialize to standard bytes (with OP_FULL_BITMAP prefix)
        let filter_bufs: Vec<(u64, Vec<u8>)> = filter_items.par_iter()
            .map(|(key, bitmap)| {
                let size = bitmap.serialized_size();
                let mut buf = vec![OP_FULL_BITMAP; 1 + size];
                bitmap.serialize_into(&mut buf[1..]).ok();
                (*key, buf)
            })
            .collect();

        let sort_bufs: Vec<(u64, Vec<u8>)> = sort_items.par_iter()
            .map(|(key, bitmap)| {
                let size = bitmap.serialized_size();
                let mut buf = vec![OP_FULL_BITMAP; 1 + size];
                bitmap.serialize_into(&mut buf[1..]).ok();
                (*key, buf)
            })
            .collect();

        // Write to ops log
        self.silo.append_op(KEY_ALIVE, &alive_buf)?;
        self.silo.append_op(KEY_META, &meta_bytes)?;
        let mut count = 2u64;

        for (key, buf) in filter_bufs.iter().chain(sort_bufs.iter()) {
            self.silo.append_op(*key, buf)?;
            count += 1;
        }

        // NOTE: Compaction skipped — DataSilo needs HashIndex upgrade for large keys.

        Ok(count)
    }

    // ── Load ────────────────────────────────────────────────────────────

    /// Load metadata from the silo.
    pub fn load_meta(&self) -> io::Result<Option<serde_json::Value>> {
        // Scan ops log for the latest metadata op
        let data = self.silo.get_with_ops(KEY_META);
        match data {
            Some(bytes) => {
                let meta: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Load all filter bitmaps into a FilterIndex.
    ///
    /// Scans the active ops log for filter-namespace keys, decodes them via
    /// `decode_key()`, reverse-looks up field names from the FieldRegistry,
    /// and populates the FilterIndex.
    ///
    /// NOTE: Only reads the active ops log (not the frozen log during compaction).
    pub fn load_filters(&self, filters: &mut FilterIndex) -> io::Result<u64> {
        let mut count = 0u64;

        // Build reverse map: field_id → field_name from the registry
        let id_to_name: HashMap<u16, String> = {
            let reg = self.field_registry.lock();
            reg.active_fields()
                .map(|(name, id)| (id, name.to_string()))
                .collect()
        };

        // Collect all unique filter keys from the active ops log
        let mut filter_keys: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let log = self.silo.ops_log().lock();
        log.for_each(|op_key, _| {
            if let DecodedKey::Filter { .. } = decode_key(op_key) {
                filter_keys.insert(op_key);
            }
        })?;
        drop(log);

        // For each key, compute the final merged bitmap (snapshot + incremental ops)
        for op_key in filter_keys {
            let (field_id, filter_value) = match decode_key(op_key) {
                DecodedKey::Filter { field_id, value } => (field_id, value),
                _ => continue,
            };
            let field_name = match id_to_name.get(&field_id) {
                Some(n) => n.as_str(),
                None => continue,
            };
            // get_bitmap_with_ops computes frozen_base + all SET/CLEAR ops on top
            if let Some(bitmap) = self.get_bitmap_with_ops(op_key) {
                if let Some(field) = filters.get_field_mut(field_name) {
                    field.or_bitmap(filter_value, &bitmap);
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Load all sort bit-layers into a SortIndex.
    ///
    /// Scans the active ops log for sort-namespace keys, decodes them, and
    /// populates the SortIndex.
    pub fn load_sorts(&self, sorts: &mut SortIndex) -> io::Result<u64> {
        let mut count = 0u64;

        // Build reverse map: field_id → field_name
        let id_to_name: HashMap<u16, String> = {
            let reg = self.field_registry.lock();
            reg.active_fields()
                .map(|(name, id)| (id, name.to_string()))
                .collect()
        };

        // Collect all unique sort keys from the active ops log
        let mut sort_keys: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let log = self.silo.ops_log().lock();
        log.for_each(|op_key, _| {
            if let DecodedKey::Sort { .. } = decode_key(op_key) {
                sort_keys.insert(op_key);
            }
        })?;
        drop(log);

        // Group by field
        let mut field_layers: HashMap<String, Vec<(usize, RoaringBitmap)>> = HashMap::new();

        for op_key in sort_keys {
            let (field_id, bit_layer) = match decode_key(op_key) {
                DecodedKey::Sort { field_id, bit_layer } => (field_id, bit_layer),
                _ => continue,
            };
            let field_name = match id_to_name.get(&field_id) {
                Some(n) => n.clone(),
                None => continue,
            };
            // get_bitmap_with_ops computes frozen_base + all SET/CLEAR ops on top
            if let Some(bitmap) = self.get_bitmap_with_ops(op_key) {
                field_layers.entry(field_name).or_default().push((bit_layer as usize, bitmap));
                count += 1;
            }
        }

        // Apply layers to sort fields
        for (field_name, layers) in field_layers {
            if let Some(field) = sorts.get_field_mut(&field_name) {
                let max_bit = layers.iter().map(|(i, _)| *i).max().unwrap_or(0);
                let mut sorted_layers: Vec<RoaringBitmap> = Vec::new();
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

    // ── Mutation ops (individual bit set/clear) ────────────────────────

    /// Set a single bit in a filter bitmap. Appends a SetBit op to the ops log.
    /// Auto-registers the field in the FieldRegistry on first write.
    pub fn filter_set(&self, field: &str, value: u64, slot: u32) -> io::Result<()> {
        let field_id = self.ensure_field_id(field)?;
        let key = encode_filter_key(field_id, value);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Clear a single bit in a filter bitmap. Appends a ClearBit op to the ops log.
    pub fn filter_clear(&self, field: &str, value: u64, slot: u32) -> io::Result<()> {
        let field_id = self.ensure_field_id(field)?;
        let key = encode_filter_key(field_id, value);
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Set a single bit in a sort layer bitmap.
    pub fn sort_set(&self, field: &str, bit_idx: usize, slot: u32) -> io::Result<()> {
        let field_id = self.ensure_field_id(field)?;
        let key = encode_sort_key(field_id, bit_idx as u32);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Clear a single bit in a sort layer bitmap.
    pub fn sort_clear(&self, field: &str, bit_idx: usize, slot: u32) -> io::Result<()> {
        let field_id = self.ensure_field_id(field)?;
        let key = encode_sort_key(field_id, bit_idx as u32);
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Set a bit in the alive bitmap.
    pub fn alive_set(&self, slot: u32) -> io::Result<()> {
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(KEY_ALIVE, &buf)
    }

    /// Clear a bit in the alive bitmap.
    pub fn alive_clear(&self, slot: u32) -> io::Result<()> {
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(KEY_ALIVE, &buf)
    }

    // ── Parallel bulk writer (for dump pipeline) ──────────────────────

    /// Prepare a lock-free parallel writer for bulk bitmap mutations.
    /// Used by the dump pipeline — rayon threads write ops without mutex contention.
    /// Call `flush_parallel_writer()` after all writes are done.
    pub fn prepare_parallel_writer(&self, estimated_ops: u64) -> io::Result<ParallelBitmapWriter<'_>> {
        // Each op is ~25 bytes framed (4 header + 4 key + 5 value + CRC + padding)
        let estimated_bytes = estimated_ops * 25;
        let writer = self.silo.prepare_parallel_ops(estimated_bytes)?;
        Ok(ParallelBitmapWriter { writer, silo: self })
    }

    /// Flush ops after parallel writes complete.
    pub fn flush_parallel_writer(&self) -> io::Result<()> {
        self.silo.flush_ops()
    }

    // ── Ops-on-read (frozen base + pending mutations) ─────────────────

    /// Read a filter bitmap with pending ops applied.
    /// Returns the frozen base | pending_sets - pending_clears.
    pub fn get_filter_with_ops(&self, field: &str, value: u64) -> Option<RoaringBitmap> {
        let field_id = self.field_id(field)?;
        let key = encode_filter_key(field_id, value);
        self.get_bitmap_with_ops(key)
    }

    /// Read a sort layer bitmap with pending ops applied.
    pub fn get_sort_layer_with_ops(&self, field: &str, bit: usize) -> Option<RoaringBitmap> {
        let field_id = self.field_id(field)?;
        let key = encode_sort_key(field_id, bit as u32);
        self.get_bitmap_with_ops(key)
    }

    /// Read the alive bitmap with pending ops applied.
    pub fn get_alive_with_ops(&self) -> Option<RoaringBitmap> {
        self.get_bitmap_with_ops(KEY_ALIVE)
    }

    /// Internal: read frozen base from data file + scan ops log for pending mutations.
    fn get_bitmap_with_ops(&self, key: u64) -> Option<RoaringBitmap> {
        // Get frozen base from data file (returns None for large keys until DataSilo
        // is upgraded to HashIndex — all data is in the ops log for now).
        let frozen_base = self.silo.get(key)
            .and_then(|bytes| if bytes.is_empty() { None } else { FrozenRoaringBitmap::view(bytes).ok() });

        // Collect pending set/clear ops from both ops logs
        let mut sets: Vec<u32> = Vec::new();
        let mut clears: Vec<u32> = Vec::new();
        let mut full_replace: Option<RoaringBitmap> = None;

        let _ = self.silo.scan_ops_for_key(key, |value| {
            if value.is_empty() { return; }
            match value[0] {
                OP_FULL_BITMAP if value.len() > 1 => {
                    // Full bitmap snapshot written by save_all / save_bucket.
                    // Bytes after tag: standard-serialized roaring bitmap (not frozen —
                    // frozen format requires 32-byte alignment not guaranteed in ops log).
                    if let Ok(bm) = RoaringBitmap::deserialize_from(&value[1..]) {
                        full_replace = Some(bm);
                        sets.clear();
                        clears.clear();
                    }
                }
                OP_SET_BIT if value.len() >= 5 => {
                    let slot = u32::from_le_bytes(value[1..5].try_into().unwrap());
                    sets.push(slot);
                }
                OP_CLEAR_BIT if value.len() >= 5 => {
                    let slot = u32::from_le_bytes(value[1..5].try_into().unwrap());
                    clears.push(slot);
                }
                _ => {} // unknown op tag, skip
            }
        });

        // If we got a full replacement, apply remaining ops to it
        if let Some(mut bitmap) = full_replace {
            for &slot in &sets { bitmap.insert(slot); }
            for &slot in &clears { bitmap.remove(slot); }
            return Some(bitmap);
        }

        if sets.is_empty() && clears.is_empty() {
            return frozen_base.map(|f| f.to_owned());
        }

        sets.sort_unstable();
        clears.sort_unstable();
        match frozen_base {
            Some(frozen) => Some(frozen.apply_ops(&sets, &clears)),
            None => {
                let mut bitmap = RoaringBitmap::new();
                for &slot in &sets { bitmap.insert(slot); }
                for &slot in &clears { bitmap.remove(slot); }
                Some(bitmap)
            }
        }
    }

    /// Whether the silo needs compaction (dead space exceeds threshold).
    ///
    /// NOTE: Compaction is not safe for BitmapSilo with the current DataSilo flat
    /// array index — large u64 keys would cause an enormous index allocation.
    /// This returns false until DataSilo is upgraded to HashIndex.
    pub fn needs_compaction(&self) -> bool {
        false
    }

    /// Compact the silo — intentionally disabled until DataSilo supports HashIndex.
    ///
    /// The flat array index (`key as usize`) cannot handle the large u64 keys
    /// produced by encode_filter_key (minimum ≈ 2^48). Once DataSilo is upgraded
    /// to use HashIndex for the data file, this restriction can be removed.
    pub fn compact(&mut self) -> io::Result<u64> {
        // TODO: Re-enable when DataSilo upgrades to HashIndex for large key support.
        Ok(0)
    }

    // ── Frozen accessors (zero-copy from mmap) ────────────────────────

    /// Get a frozen bitmap view for a filter field+value directly from the mmap.
    ///
    /// Returns None if the field+value isn't in the silo data file. This will
    /// always be None until DataSilo is upgraded to support large u64 keys in
    /// its index. Use `get_filter_with_ops()` to read from the ops log instead.
    pub fn get_frozen_filter(&self, field: &str, value: u64) -> Option<FrozenRoaringBitmap<'_>> {
        let field_id = self.field_id(field)?;
        let key = encode_filter_key(field_id, value);
        let bytes = self.silo.get(key)?;
        FrozenRoaringBitmap::view(bytes).ok()
    }

    /// Get a frozen bitmap view for a sort bit-layer directly from the mmap.
    ///
    /// Returns None until DataSilo is upgraded to support large u64 keys.
    /// Use `get_sort_layer_with_ops()` to read from the ops log instead.
    pub fn get_frozen_sort_layer(&self, field: &str, bit: usize) -> Option<FrozenRoaringBitmap<'_>> {
        let field_id = self.field_id(field)?;
        let key = encode_sort_key(field_id, bit as u32);
        let bytes = self.silo.get(key)?;
        FrozenRoaringBitmap::view(bytes).ok()
    }

    /// Iterate all filter (field_name, value) pairs stored in the silo.
    ///
    /// TODO: Enumerate filter values from the ops log once DataSilo exposes
    /// an all-keys iteration API. Currently returns empty. (#13 range scan
    /// migration tracks this.)
    pub fn filter_entries(&self) -> impl Iterator<Item = (String, u64)> {
        std::iter::empty()
    }

    /// Iterate all values stored for a specific filter field.
    ///
    /// TODO: Enumerate from ops log once DataSilo exposes all-keys iteration.
    /// Currently returns empty. Used by range_scan in the executor; migration
    /// tracked in #13.
    pub fn filter_values_for_field(&self, _field: &str) -> Vec<u64> {
        vec![]
    }

    /// Check if a sort field has any layers stored (i.e., is registered).
    pub fn has_sort_field(&self, field: &str) -> bool {
        self.field_id(field).is_some()
    }

    // ── Backed loading (mark as unloaded, read frozen at query time) ──

    /// Load filter bitmaps from the silo into the FilterIndex.
    ///
    /// Scans the active ops log for filter-namespace keys and eagerly populates
    /// the FilterIndex. This replaces the old "mark as backed for lazy frozen reads"
    /// approach — since frozen filter reads require the data file (not available until
    /// DataSilo upgrades to HashIndex), we always eager-load from the ops log.
    ///
    /// Returns the number of filter (field, value) entries loaded.
    pub fn mark_filters_backed(&self, filters: &mut FilterIndex) -> u64 {
        // Delegate to load_filters — eagerly load all filter data from ops log.
        self.load_filters(filters).unwrap_or(0)
    }

    /// Load sort layers from the silo into the SortIndex.
    ///
    /// Scans the active ops log for sort-namespace keys and eagerly populates
    /// the SortIndex. This replaces the old "mark as backed for lazy frozen reads"
    /// approach — since frozen sort reads require the data file (not available until
    /// DataSilo upgrades to HashIndex), we always eager-load from the ops log.
    ///
    /// Returns the number of (field, layer) entries loaded.
    pub fn mark_sorts_backed(&self, sorts: &mut SortIndex) -> u64 {
        // Delegate to load_sorts — eagerly load all sort data from ops log.
        // The "backed" concept only applies to the frozen data file path which is
        // not yet available with the current DataSilo flat array index.
        self.load_sorts(sorts).unwrap_or(0)
    }

    // ── Time bucket storage ───────────────────────────────────────────

    /// Store a time bucket bitmap as a snapshot op in the ops log.
    ///
    /// Bucket names are registered as fields in the FieldRegistry. The sort
    /// field's field_id and the bucket's registered ID are combined via
    /// `encode_bucket_key`.
    ///
    /// Uses standard (non-frozen) serialization prefixed with `OP_FULL_BITMAP (0x00)`
    /// so the payload doesn't need 32-byte alignment.
    pub fn save_bucket(&self, field: &str, bucket_name: &str, bitmap: &RoaringBitmap) -> io::Result<()> {
        let field_id = self.ensure_field_id(field)?;
        // Register the bucket name as a field to get a stable u16 bucket_id
        let bucket_id = self.ensure_field_id(bucket_name)? as u16;
        let key = encode_bucket_key(field_id, bucket_id);
        // Standard (portable) serialization — alignment-independent, safe in ops log
        let bitmap_size = bitmap.serialized_size();
        let mut buf = vec![0u8; 1 + bitmap_size];
        buf[0] = OP_FULL_BITMAP;
        bitmap.serialize_into(&mut buf[1..])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize bucket: {e}")))?;
        self.silo.append_op(key, &buf)
    }

    /// Read a bucket bitmap with pending SET/CLEAR ops applied.
    ///
    /// Returns None if this bucket has never been saved.
    pub fn get_bucket_with_ops(&self, field: &str, bucket_name: &str) -> Option<RoaringBitmap> {
        let field_id = self.field_id(field)?;
        let bucket_id = self.field_id(bucket_name)? as u16;
        let key = encode_bucket_key(field_id, bucket_id);

        let mut base: Option<RoaringBitmap> = None;
        let mut sets: Vec<u32> = Vec::new();
        let mut clears: Vec<u32> = Vec::new();

        let _ = self.silo.scan_ops_for_key(key, |value| {
            if value.is_empty() { return; }
            match value[0] {
                OP_FULL_BITMAP => {
                    if let Ok(bm) = RoaringBitmap::deserialize_from(&value[1..]) {
                        base = Some(bm);
                        sets.clear();
                        clears.clear();
                    }
                }
                OP_SET_BIT if value.len() >= 5 => {
                    let slot = u32::from_le_bytes(value[1..5].try_into().unwrap());
                    sets.push(slot);
                }
                OP_CLEAR_BIT if value.len() >= 5 => {
                    let slot = u32::from_le_bytes(value[1..5].try_into().unwrap());
                    clears.push(slot);
                }
                _ => {}
            }
        });

        let mut bitmap = base?;
        for &slot in &sets { bitmap.insert(slot); }
        for &slot in &clears { bitmap.remove(slot); }
        Some(bitmap)
    }

    /// Append a SET op to a bucket bitmap (slot entered the time window).
    pub fn bucket_set(&self, field: &str, bucket_name: &str, slot: u32) -> io::Result<()> {
        let field_id = self.ensure_field_id(field)?;
        let bucket_id = self.ensure_field_id(bucket_name)? as u16;
        let key = encode_bucket_key(field_id, bucket_id);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Append a CLEAR op to a bucket bitmap (slot aged out or was deleted).
    pub fn bucket_clear(&self, field: &str, bucket_name: &str, slot: u32) -> io::Result<()> {
        let field_id = self.ensure_field_id(field)?;
        let bucket_id = self.ensure_field_id(bucket_name)? as u16;
        let key = encode_bucket_key(field_id, bucket_id);
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }
}

// ---------------------------------------------------------------------------
// ParallelBitmapWriter — lock-free bulk bitmap writes for the dump pipeline
// ---------------------------------------------------------------------------

/// Lock-free parallel writer for bulk bitmap mutations.
/// Created by `BitmapSilo::prepare_parallel_writer()`.
/// Each rayon thread gets its own cursor/end pair for zero-contention writes.
pub struct ParallelBitmapWriter<'a> {
    writer: datasilo::ParallelOpsWriter,
    silo: &'a BitmapSilo,
}

// Safety: writer is Send+Sync (atomic cursor + disjoint mmap regions).
// silo ref is shared read-only (field_registry uses internal Mutex).
unsafe impl Send for ParallelBitmapWriter<'_> {}
unsafe impl Sync for ParallelBitmapWriter<'_> {}

impl<'a> ParallelBitmapWriter<'a> {
    /// Set a single bit in a filter bitmap. Lock-free, safe from rayon threads.
    /// `cursor` and `end` are thread-local state — initialize both to 0.
    #[inline]
    pub fn filter_set(&self, field: &str, value: u64, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let field_id = match self.silo.ensure_field_id(field) {
            Ok(id) => id,
            Err(_) => return false,
        };
        let key = encode_filter_key(field_id, value);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(key, &buf, cursor, end)
    }

    /// Clear a single bit in a filter bitmap. Lock-free.
    #[inline]
    pub fn filter_clear(&self, field: &str, value: u64, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let field_id = match self.silo.ensure_field_id(field) {
            Ok(id) => id,
            Err(_) => return false,
        };
        let key = encode_filter_key(field_id, value);
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(key, &buf, cursor, end)
    }

    /// Set a single bit in a sort layer bitmap. Lock-free.
    #[inline]
    pub fn sort_set(&self, field: &str, bit_idx: usize, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let field_id = match self.silo.ensure_field_id(field) {
            Ok(id) => id,
            Err(_) => return false,
        };
        let key = encode_sort_key(field_id, bit_idx as u32);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(key, &buf, cursor, end)
    }

    /// Clear a single bit in a sort layer bitmap. Lock-free.
    #[inline]
    pub fn sort_clear(&self, field: &str, bit_idx: usize, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let field_id = match self.silo.ensure_field_id(field) {
            Ok(id) => id,
            Err(_) => return false,
        };
        let key = encode_sort_key(field_id, bit_idx as u32);
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(key, &buf, cursor, end)
    }

    /// Set a bit in the alive bitmap. Lock-free.
    #[inline]
    pub fn alive_set(&self, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(KEY_ALIVE, &buf, cursor, end)
    }

    /// Clear a bit in the alive bitmap. Lock-free.
    #[inline]
    pub fn alive_clear(&self, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(KEY_ALIVE, &buf, cursor, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::engine::filter::FilterFieldType;

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

        // Load alive via ops-on-read
        let loaded_alive = silo.get_alive_with_ops().unwrap();
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

        let slots = crate::engine::slot::SlotAllocator::from_state(100, {
            let mut bm = RoaringBitmap::new();
            bm.insert_range(0..100);
            bm
        }, RoaringBitmap::new());
        let cursors = std::collections::HashMap::new();

        let mut silo = BitmapSilo::open(dir.path()).unwrap();
        silo.save_all(&filters, &sorts, &slots, &cursors).unwrap();
        // No compact — frozen accessors are not available until DataSilo
        // supports HashIndex. Use ops-on-read path instead.

        // Verify ops-on-read path (replaces frozen accessor test)
        let filter_bm = silo.get_filter_with_ops("nsfwLevel", 1)
            .expect("should find filter via ops-on-read");
        assert_eq!(filter_bm.len(), 100);
        assert!(filter_bm.contains(50));
        assert!(!filter_bm.contains(100));

        let sort_bm = silo.get_sort_layer_with_ops("sortAt", 0)
            .expect("should find sort layer via ops-on-read");
        assert_eq!(sort_bm.len(), 50);

        drop(silo);

        // Reopen and test backed marking via ops log scan
        let silo = BitmapSilo::open(dir.path()).unwrap();

        let mut new_filters = FilterIndex::new();
        new_filters.add_field(FilterFieldConfig {
            name: "nsfwLevel".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        // mark_filters_backed now eagerly loads from ops log (frozen data file
        // is not available until DataSilo upgrades to HashIndex).
        let count = silo.mark_filters_backed(&mut new_filters);
        assert_eq!(count, 1);
        let field = new_filters.get_field("nsfwLevel").unwrap();
        // Eagerly loaded — bitmap is present in memory
        let bm = field.get(1).expect("filter should be loaded by mark_filters_backed");
        assert_eq!(bm.len(), 100, "should have 100 bits set");
    }

    /// Test save_bucket / get_bucket_with_ops round-trip.
    #[test]
    fn test_bucket_save_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let silo = BitmapSilo::open(dir.path()).unwrap();

        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(2);
        bm.insert(3);

        // Save the initial snapshot
        silo.save_bucket("sortAt", "24h", &bm).unwrap();

        // Read it back with no pending ops — should match exactly
        let result = silo.get_bucket_with_ops("sortAt", "24h")
            .expect("bucket should exist after save");
        assert_eq!(result.len(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    /// Test that bucket_set / bucket_clear ops are applied on read.
    #[test]
    fn test_bucket_set_clear_ops_applied() {
        let dir = tempfile::tempdir().unwrap();
        let silo = BitmapSilo::open(dir.path()).unwrap();

        // Save initial snapshot: slots 1, 2, 3
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(2);
        bm.insert(3);
        silo.save_bucket("sortAt", "7d", &bm).unwrap();

        // Append SET op for slot 10 (new slot entered window)
        silo.bucket_set("sortAt", "7d", 10).unwrap();
        // Append CLEAR op for slot 2 (slot aged out)
        silo.bucket_clear("sortAt", "7d", 2).unwrap();

        // Read back: should include 10, exclude 2, keep 1 and 3
        let result = silo.get_bucket_with_ops("sortAt", "7d")
            .expect("bucket should exist");
        assert!(result.contains(1), "slot 1 should still be present");
        assert!(!result.contains(2), "slot 2 should be cleared");
        assert!(result.contains(3), "slot 3 should still be present");
        assert!(result.contains(10), "slot 10 should be set");
        assert_eq!(result.len(), 3);
    }

    /// Test that get_bucket_with_ops returns None for an unknown bucket.
    #[test]
    fn test_bucket_not_found_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let silo = BitmapSilo::open(dir.path()).unwrap();
        assert!(silo.get_bucket_with_ops("sortAt", "24h").is_none());
    }

    /// Test multiple buckets on the same field are stored independently.
    #[test]
    fn test_multiple_buckets_independent() {
        let dir = tempfile::tempdir().unwrap();
        let silo = BitmapSilo::open(dir.path()).unwrap();

        // 24h bucket: slots 1..=3
        let mut bm24 = RoaringBitmap::new();
        bm24.extend([1u32, 2, 3]);
        silo.save_bucket("sortAt", "24h", &bm24).unwrap();

        // 7d bucket: slots 1..=10
        let mut bm7d = RoaringBitmap::new();
        bm7d.extend(1u32..=10);
        silo.save_bucket("sortAt", "7d", &bm7d).unwrap();

        // Mutate only 24h
        silo.bucket_clear("sortAt", "24h", 1).unwrap();

        let r24 = silo.get_bucket_with_ops("sortAt", "24h").unwrap();
        let r7d = silo.get_bucket_with_ops("sortAt", "7d").unwrap();

        // 24h: slot 1 cleared
        assert!(!r24.contains(1));
        assert!(r24.contains(2));
        assert_eq!(r24.len(), 2);

        // 7d: untouched
        assert!(r7d.contains(1), "7d should not be affected by 24h clear");
        assert_eq!(r7d.len(), 10);
    }

    /// Test that field IDs are stable across re-opens.
    #[test]
    fn test_field_registry_persists() {
        let dir = tempfile::tempdir().unwrap();

        {
            let silo = BitmapSilo::open(dir.path()).unwrap();
            silo.filter_set("nsfwLevel", 1, 42).unwrap();
            silo.sort_set("sortAt", 0, 42).unwrap();
        }

        // Re-open — field IDs must be stable
        let silo = BitmapSilo::open(dir.path()).unwrap();
        let bm = silo.get_filter_with_ops("nsfwLevel", 1).unwrap();
        assert!(bm.contains(42));
        let sort_bm = silo.get_sort_layer_with_ops("sortAt", 0).unwrap();
        assert!(sort_bm.contains(42));
    }

    /// Test individual mutation ops round-trip via ops-on-read.
    #[test]
    fn test_filter_set_clear_ops() {
        let dir = tempfile::tempdir().unwrap();
        let silo = BitmapSilo::open(dir.path()).unwrap();

        silo.filter_set("tagIds", 100, 1).unwrap();
        silo.filter_set("tagIds", 100, 2).unwrap();
        silo.filter_set("tagIds", 100, 3).unwrap();
        silo.filter_clear("tagIds", 100, 2).unwrap();

        let bm = silo.get_filter_with_ops("tagIds", 100).unwrap();
        assert!(bm.contains(1));
        assert!(!bm.contains(2));
        assert!(bm.contains(3));
        assert_eq!(bm.len(), 2);
    }

    /// Test alive set/clear round-trip.
    #[test]
    fn test_alive_ops() {
        let dir = tempfile::tempdir().unwrap();
        let silo = BitmapSilo::open(dir.path()).unwrap();

        silo.alive_set(10).unwrap();
        silo.alive_set(20).unwrap();
        silo.alive_clear(10).unwrap();

        let alive = silo.get_alive_with_ops().unwrap();
        assert!(!alive.contains(10));
        assert!(alive.contains(20));
    }

    /// Test has_sort_field after registering a field.
    #[test]
    fn test_has_sort_field() {
        let dir = tempfile::tempdir().unwrap();
        let silo = BitmapSilo::open(dir.path()).unwrap();

        assert!(!silo.has_sort_field("sortAt"));
        silo.sort_set("sortAt", 0, 1).unwrap();
        assert!(silo.has_sort_field("sortAt"));
        assert!(!silo.has_sort_field("unknownField"));
    }
}
