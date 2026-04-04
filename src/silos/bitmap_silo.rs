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

use crate::engine::filter::FilterIndex;
use crate::engine::sort::SortIndex;
use crate::engine::slot::SlotAllocator;

/// Reserved key for the alive bitmap.
const KEY_ALIVE: u32 = 0;
/// Reserved key for metadata (slot_counter, cursors, deferred alive).
const KEY_META: u32 = 1;
/// First key available for filter/sort bitmaps.
const KEY_BITMAP_START: u32 = 2;

// Ops value type tags for bitmap mutations
const OP_FULL_BITMAP: u8 = 0x00;  // Full frozen bitmap (from save_all/compaction)
const OP_SET_BIT: u8 = 0x01;      // Set a single bit: [0x01][u32 slot]
const OP_CLEAR_BIT: u8 = 0x02;    // Clear a single bit: [0x02][u32 slot]

/// Persistent bitmap storage.
pub struct BitmapSilo {
    silo: datasilo::DataSilo,
    path: PathBuf,
    /// Maps logical bitmap name → silo key.
    /// Format: "filter:{field}:{value}" or "sort:{field}:{bit}" → u32
    /// Protected by RwLock for concurrent mutation method access.
    name_to_key: parking_lot::RwLock<HashMap<String, u32>>,
    /// Reverse mapping for loading.
    key_to_name: parking_lot::RwLock<HashMap<u32, String>>,
    /// Next available key for new bitmaps.
    next_key: std::sync::atomic::AtomicU32,
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

        Ok(Self {
            silo,
            path: path.to_path_buf(),
            name_to_key: parking_lot::RwLock::new(name_to_key),
            key_to_name: parking_lot::RwLock::new(key_to_name),
            next_key: std::sync::atomic::AtomicU32::new(next_key),
        })
    }

    /// Save the current manifest to disk.
    fn save_manifest(&self) -> io::Result<()> {
        let json = serde_json::to_string_pretty(&*self.name_to_key.read())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(self.path.join("bitmap_manifest.json"), json)
    }

    /// Get or assign a silo key for a logical bitmap name.
    fn ensure_key(&self, name: &str) -> u32 {
        // Fast path: read lock
        if let Some(&key) = self.name_to_key.read().get(name) {
            return key;
        }
        // Slow path: write lock to insert
        let mut map = self.name_to_key.write();
        // Double-check after acquiring write lock
        if let Some(&key) = map.get(name) {
            return key;
        }
        let key = self.next_key.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        map.insert(name.to_string(), key);
        self.key_to_name.write().insert(key, name.to_string());
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

    /// Save all bitmaps using parallel writes for maximum throughput.
    /// Serializes bitmaps in parallel via rayon, writes directly to data.bin + index.bin
    /// using DataSilo::write_batch_parallel() — bypasses the ops log entirely.
    pub fn save_all_parallel(
        &mut self,
        filters: &FilterIndex,
        sorts: &SortIndex,
        slots: &SlotAllocator,
        cursors: &HashMap<String, String>,
    ) -> io::Result<u64> {
        use rayon::prelude::*;

        // Step 1: Alive + metadata (small, sequential)
        let alive = slots.alive_bitmap();
        let alive_size = alive.frozen_serialized_size();
        let mut alive_buf = vec![0u8; alive_size];
        alive.serialize_frozen_into(&mut alive_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("frozen serialize alive: {e:?}")))?;

        let meta = serde_json::json!({
            "slot_counter": slots.slot_counter(),
            "cursors": cursors,
        });
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Step 2: Collect all bitmap (key, RoaringBitmap) pairs with key assignment
        // Use name_to_key + next_key refs to avoid borrowing &mut self in closures
        let name_to_key = &self.name_to_key;
        let key_to_name = &self.key_to_name;
        let next_key = &self.next_key;
        let ensure = |name: &str| -> u32 {
            if let Some(&key) = name_to_key.read().get(name) { return key; }
            let mut map = name_to_key.write();
            if let Some(&key) = map.get(name) { return key; }
            let key = next_key.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map.insert(name.to_string(), key);
            key_to_name.write().insert(key, name.to_string());
            key
        };

        let filter_items: Vec<(u32, RoaringBitmap)> = filters.fields()
            .flat_map(|(field_name, field)| {
                field.bitmaps_fused().map(move |(value, bitmap)| {
                    let name = format!("filter:{}:{}", field_name, value);
                    let key = ensure(&name);
                    (key, bitmap)
                })
            })
            .collect();

        let sort_items: Vec<(u32, RoaringBitmap)> = sorts.fields()
            .flat_map(|(field_name, field)| {
                field.layers_fused().into_iter().enumerate()
                    .filter(|(_, bm)| !bm.is_empty())
                    .map(move |(bit_idx, bitmap)| {
                        let name = format!("sort:{}:{}", field_name, bit_idx);
                        let key = ensure(&name);
                        (key, bitmap)
                    })
            })
            .collect();

        // Step 3: Parallel serialize all bitmaps to frozen bytes
        let filter_bufs: Vec<(u32, Vec<u8>)> = filter_items.par_iter()
            .map(|(key, bitmap)| {
                let size = bitmap.frozen_serialized_size();
                let mut buf = vec![0u8; size];
                bitmap.serialize_frozen_into(&mut buf).ok();
                (*key, buf)
            })
            .collect();

        let sort_bufs: Vec<(u32, Vec<u8>)> = sort_items.par_iter()
            .map(|(key, bitmap)| {
                let size = bitmap.frozen_serialized_size();
                let mut buf = vec![0u8; size];
                bitmap.serialize_frozen_into(&mut buf).ok();
                (*key, buf)
            })
            .collect();

        // Step 4: Combine all entries and write directly to data.bin + index.bin
        let mut all_entries: Vec<(u32, Vec<u8>)> = Vec::with_capacity(
            2 + filter_bufs.len() + sort_bufs.len()
        );
        all_entries.push((KEY_ALIVE, alive_buf));
        all_entries.push((KEY_META, meta_bytes));
        all_entries.extend(filter_bufs);
        all_entries.extend(sort_bufs);

        let count = self.silo.write_batch_parallel(&all_entries)?;
        self.save_manifest()?;

        Ok(count)
    }

    // ── Load ────────────────────────────────────────────────────────────

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
        let entries: Vec<(String, u32)> = self.name_to_key.read()
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect();
        for (name, key) in entries {
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

        let entries: Vec<(String, u32)> = self.name_to_key.read()
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect();
        for (name, key) in entries {
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

    // ── Mutation ops (individual bit set/clear) ────────────────────────

    /// Set a single bit in a filter bitmap. Appends a SetBit op to the ops log.
    /// Auto-creates the key if this is the first write for this field+value.
    pub fn filter_set(&self, field: &str, value: u64, slot: u32) -> io::Result<()> {
        let name = format!("filter:{}:{}", field, value);
        let key = self.ensure_key(&name);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Clear a single bit in a filter bitmap. Appends a ClearBit op to the ops log.
    /// Auto-creates the key if this is the first write for this field+value.
    pub fn filter_clear(&self, field: &str, value: u64, slot: u32) -> io::Result<()> {
        let name = format!("filter:{}:{}", field, value);
        let key = self.ensure_key(&name);
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Set a single bit in a sort layer bitmap.
    /// Auto-creates the key if this is the first write for this field+bit.
    pub fn sort_set(&self, field: &str, bit_idx: usize, slot: u32) -> io::Result<()> {
        let name = format!("sort:{}:{}", field, bit_idx);
        let key = self.ensure_key(&name);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.silo.append_op(key, &buf)
    }

    /// Clear a single bit in a sort layer bitmap.
    /// Auto-creates the key if this is the first write for this field+bit.
    pub fn sort_clear(&self, field: &str, bit_idx: usize, slot: u32) -> io::Result<()> {
        let name = format!("sort:{}:{}", field, bit_idx);
        let key = self.ensure_key(&name);
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
    pub fn prepare_parallel_writer(&self, estimated_ops: u64) -> io::Result<ParallelBitmapWriter> {
        // Each op is ~25 bytes framed (4 header + 4 key + 5 value + CRC + padding)
        let estimated_bytes = estimated_ops * 25;
        let writer = self.silo.prepare_parallel_ops(estimated_bytes)?;
        Ok(ParallelBitmapWriter { writer, silo: self })
    }

    /// Flush ops and save manifest after parallel writes complete.
    pub fn flush_parallel_writer(&self) -> io::Result<()> {
        self.silo.flush_ops()?;
        self.save_manifest()
    }

    // ── Ops-on-read (frozen base + pending mutations) ─────────────────

    /// Read a filter bitmap with pending ops applied.
    /// Returns the frozen base | pending_sets - pending_clears.
    pub fn get_filter_with_ops(&self, field: &str, value: u64) -> Option<RoaringBitmap> {
        let name = format!("filter:{}:{}", field, value);
        let key = *self.name_to_key.read().get(&name)?;
        self.get_bitmap_with_ops(key)
    }

    /// Read a sort layer bitmap with pending ops applied.
    pub fn get_sort_layer_with_ops(&self, field: &str, bit: usize) -> Option<RoaringBitmap> {
        let name = format!("sort:{}:{}", field, bit);
        let key = *self.name_to_key.read().get(&name)?;
        self.get_bitmap_with_ops(key)
    }

    /// Read the alive bitmap with pending ops applied.
    pub fn get_alive_with_ops(&self) -> Option<RoaringBitmap> {
        self.get_bitmap_with_ops(KEY_ALIVE)
    }

    /// Internal: read frozen base from data file + scan ops log for pending mutations.
    fn get_bitmap_with_ops(&self, key: u32) -> Option<RoaringBitmap> {
        // Get frozen base from data file
        let frozen_base = self.silo.get(key)
            .and_then(|bytes| if bytes.is_empty() { None } else { FrozenRoaringBitmap::view(bytes).ok() });

        // Collect pending set/clear ops from both ops logs
        let mut sets: Vec<u32> = Vec::new();
        let mut clears: Vec<u32> = Vec::new();
        let mut full_replace: Option<RoaringBitmap> = None;

        let _ = self.silo.scan_ops_for_key(key, |value| {
            if value.is_empty() { return; }
            match value[0] {
                OP_SET_BIT if value.len() >= 5 => {
                    let slot = u32::from_le_bytes(value[1..5].try_into().unwrap());
                    sets.push(slot);
                }
                OP_CLEAR_BIT if value.len() >= 5 => {
                    let slot = u32::from_le_bytes(value[1..5].try_into().unwrap());
                    clears.push(slot);
                }
                _ => {
                    // Legacy or full bitmap value — replace base entirely
                    if let Ok(frozen) = FrozenRoaringBitmap::view(value) {
                        full_replace = Some(frozen.to_owned());
                        sets.clear();
                        clears.clear();
                    }
                }
            }
        });

        // If we got a full replacement, apply remaining ops to it
        if let Some(mut bitmap) = full_replace {
            for &slot in &sets { bitmap.insert(slot); }
            for &slot in &clears { bitmap.remove(slot); }
            return Some(bitmap);
        }

        if sets.is_empty() && clears.is_empty() {
            // No ops — return frozen base as owned (or None if no base)
            return frozen_base.map(|f| f.to_owned());
        }

        // Container-level CoW: only copies containers touched by ops
        sets.sort_unstable();
        clears.sort_unstable();
        match frozen_base {
            Some(frozen) => Some(frozen.apply_ops(&sets, &clears)),
            None => {
                // No base — build from ops alone
                let mut bitmap = RoaringBitmap::new();
                for &slot in &sets { bitmap.insert(slot); }
                Some(bitmap)
            }
        }
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
        let key = *self.name_to_key.read().get(&name)?;
        let bytes = self.silo.get(key)?;
        FrozenRoaringBitmap::view(bytes).ok()
    }

    /// Get a frozen bitmap view for a sort bit-layer directly from the mmap.
    /// Returns None if the field+bit isn't in the silo.
    pub fn get_frozen_sort_layer(&self, field: &str, bit: usize) -> Option<FrozenRoaringBitmap<'_>> {
        let name = format!("sort:{}:{}", field, bit);
        let key = *self.name_to_key.read().get(&name)?;
        let bytes = self.silo.get(key)?;
        FrozenRoaringBitmap::view(bytes).ok()
    }

    /// Iterate all filter (field_name, value) pairs stored in the silo.
    pub fn filter_entries(&self) -> impl Iterator<Item = (String, u64)> {
        let entries: Vec<(String, u64)> = self.name_to_key.read().keys()
            .filter_map(|name| {
                let stripped = name.strip_prefix("filter:")?;
                let (field, val_str) = stripped.rsplit_once(':')?;
                let value: u64 = val_str.parse().ok()?;
                Some((field.to_string(), value))
            })
            .collect();
        entries.into_iter()
    }

    /// Check if a sort field has any layers stored.
    pub fn has_sort_field(&self, field: &str) -> bool {
        let prefix = format!("sort:{}:", field);
        self.name_to_key.read().keys().any(|k| k.starts_with(&prefix))
    }

    // ── Backed loading (mark as unloaded, read frozen at query time) ──

    /// Mark all filter values in the silo as backed (unloaded) in the FilterIndex.
    /// Creates VersionedBitmap::new_unloaded() placeholders so the executor knows
    /// to fall back to frozen reads from the silo.
    pub fn mark_filters_backed(&self, filters: &mut FilterIndex) -> u64 {
        let mut count = 0u64;
        let names: Vec<String> = self.name_to_key.read().keys()
            .filter(|n| n.starts_with("filter:"))
            .cloned()
            .collect();
        for name in names {
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
        let names: Vec<String> = self.name_to_key.read().keys()
            .filter(|n| n.starts_with("sort:"))
            .cloned()
            .collect();
        for name in names {
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
// silo ref is shared read-only (ensure_key uses internal RwLock).
unsafe impl Send for ParallelBitmapWriter<'_> {}
unsafe impl Sync for ParallelBitmapWriter<'_> {}

impl<'a> ParallelBitmapWriter<'a> {
    /// Set a single bit in a filter bitmap. Lock-free, safe from rayon threads.
    /// `cursor` and `end` are thread-local state — initialize both to 0.
    #[inline]
    pub fn filter_set(&self, field: &str, value: u64, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let name = format!("filter:{}:{}", field, value);
        let key = self.silo.ensure_key(&name);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(key, &buf, cursor, end)
    }

    /// Clear a single bit in a filter bitmap. Lock-free.
    #[inline]
    pub fn filter_clear(&self, field: &str, value: u64, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let name = format!("filter:{}:{}", field, value);
        let key = self.silo.ensure_key(&name);
        let mut buf = [0u8; 5];
        buf[0] = OP_CLEAR_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(key, &buf, cursor, end)
    }

    /// Set a single bit in a sort layer bitmap. Lock-free.
    #[inline]
    pub fn sort_set(&self, field: &str, bit_idx: usize, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let name = format!("sort:{}:{}", field, bit_idx);
        let key = self.silo.ensure_key(&name);
        let mut buf = [0u8; 5];
        buf[0] = OP_SET_BIT;
        buf[1..5].copy_from_slice(&slot.to_le_bytes());
        self.writer.write_put(key, &buf, cursor, end)
    }

    /// Clear a single bit in a sort layer bitmap. Lock-free.
    #[inline]
    pub fn sort_clear(&self, field: &str, bit_idx: usize, slot: u32, cursor: &mut usize, end: &mut usize) -> bool {
        let name = format!("sort:{}:{}", field, bit_idx);
        let key = self.silo.ensure_key(&name);
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
