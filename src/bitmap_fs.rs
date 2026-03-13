//! Filesystem-based bitmap persistence.
//!
//! Each bitmap is stored as an individual `.roar` file containing the serialized
//! roaring bitmap data. This replaces the redb-backed `BitmapStore`.
//!
//! **Write path**: Atomic tmp→fsync→rename pattern:
//!   1. Write to `{name}.roar.tmp`
//!   2. Fsync the file
//!   3. Rename over `{name}.roar` (atomic on POSIX, close-enough on NTFS)
//!
//! **Read path**: Read file into memory and deserialize. OS page cache handles
//! hot/cold bitmap caching transparently.
//!
//! Directory layout:
//! ```text
//! bitmaps/
//!   filter/{field_name}/{value}.roar
//!   sort/{field_name}/bit{00..31}.roar
//!   system/alive.roar
//!   meta/slot_counter.bin
//! ```

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use crate::error::{BitdexError, Result};

/// Filesystem-based bitmap store.
pub struct BitmapFs {
    root: PathBuf,
}

impl BitmapFs {
    /// Create a new bitmap store rooted at the given directory.
    /// Creates the directory structure if it doesn't exist.
    pub fn new(root: &Path) -> Result<Self> {
        let root = root.to_path_buf();
        std::fs::create_dir_all(root.join("filter"))
            .map_err(|e| BitdexError::DocStore(format!("create filter dir: {e}")))?;
        std::fs::create_dir_all(root.join("sort"))
            .map_err(|e| BitdexError::DocStore(format!("create sort dir: {e}")))?;
        std::fs::create_dir_all(root.join("system"))
            .map_err(|e| BitdexError::DocStore(format!("create system dir: {e}")))?;
        std::fs::create_dir_all(root.join("meta"))
            .map_err(|e| BitdexError::DocStore(format!("create meta dir: {e}")))?;
        Ok(Self { root })
    }

    /// Create a temporary in-memory bitmap store for testing.
    /// Uses a tempdir that is cleaned up when the BitmapFs is dropped
    /// (caller should hold the tempdir handle).
    pub fn new_temp(dir: &Path) -> Result<Self> {
        Self::new(dir)
    }

    // ---- Atomic write helpers ----

    fn write_bitmap_atomic(path: &Path, bitmap: &RoaringBitmap) -> Result<()> {
        let tmp_path = path.with_extension("roar.tmp");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BitdexError::DocStore(format!("create dir: {e}")))?;
        }
        let mut buf = Vec::with_capacity(bitmap.serialized_size());
        bitmap
            .serialize_into(&mut buf)
            .map_err(|e| BitdexError::DocStore(format!("bitmap serialize: {e}")))?;
        std::fs::write(&tmp_path, &buf)
            .map_err(|e| BitdexError::DocStore(format!("write tmp: {e}")))?;
        std::fs::rename(&tmp_path, path)
            .map_err(|e| BitdexError::DocStore(format!("rename: {e}")))?;
        Ok(())
    }

    fn read_bitmap(path: &Path) -> Result<Option<RoaringBitmap>> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let bm = RoaringBitmap::deserialize_from(bytes.as_slice())
                    .map_err(|e| BitdexError::DocStore(format!("bitmap deserialize: {e}")))?;
                Ok(Some(bm))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BitdexError::DocStore(format!("read bitmap: {e}"))),
        }
    }

    fn write_bytes_atomic(path: &Path, data: &[u8]) -> Result<()> {
        let tmp_path = path.with_extension("bin.tmp");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BitdexError::DocStore(format!("create dir: {e}")))?;
        }
        std::fs::write(&tmp_path, data)
            .map_err(|e| BitdexError::DocStore(format!("write tmp: {e}")))?;
        std::fs::rename(&tmp_path, path)
            .map_err(|e| BitdexError::DocStore(format!("rename: {e}")))?;
        Ok(())
    }

    // ---- Filter bitmaps (hex-bucket packed files) ----
    //
    // Layout: filter/{field}/{xx}.fpack
    // where xx = (value >> 8) & 0xFF (hex bucket byte)
    //
    // Each .fpack file format:
    // [u32 num_entries]
    // [index: N × (u64 value, u32 offset, u32 length)]
    // [packed serialized roaring bitmaps]
    //
    // High-cardinality fields get ~256 pack files, each ~300 entries.
    // Low-cardinality fields (nsfwLevel=7 values) get 1-2 tiny pack files.

    fn filter_bucket(value: u64) -> u8 {
        ((value >> 8) & 0xFF) as u8
    }

    fn filter_pack_path(&self, field: &str, bucket: u8) -> PathBuf {
        self.root
            .join("filter")
            .join(field)
            .join(format!("{:02x}.fpack", bucket))
    }

    /// Write a single bucket pack file.
    fn write_pack_file(path: &Path, entries: &[(u64, &RoaringBitmap)]) -> Result<()> {
        // Serialize all bitmaps
        let mut serialized: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for &(value, bm) in entries {
            let mut buf = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut buf)
                .map_err(|e| BitdexError::DocStore(format!("filter bitmap serialize: {e}")))?;
            serialized.push((value, buf));
        }

        let num_entries = serialized.len() as u32;
        let header_size = 4 + serialized.len() * 16;
        let data_size: usize = serialized.iter().map(|(_, d)| d.len()).sum();
        let mut buf = Vec::with_capacity(header_size + data_size);

        buf.extend_from_slice(&num_entries.to_le_bytes());

        let mut offset: u32 = 0;
        for (value, data) in &serialized {
            buf.extend_from_slice(&value.to_le_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            offset += data.len() as u32;
        }

        for (_, data) in &serialized {
            buf.extend_from_slice(data);
        }

        Self::write_bytes_atomic(path, &buf)
    }

    /// Read entries from a single pack file.
    fn read_pack_file(data: &[u8]) -> Result<Vec<(u64, RoaringBitmap)>> {
        if data.len() < 4 {
            return Err(BitdexError::DocStore("filter pack header truncated".into()));
        }

        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header_size = 4 + num_entries * 16;
        if data.len() < header_size {
            return Err(BitdexError::DocStore("filter pack index truncated".into()));
        }

        let data_start = header_size;
        let mut result = Vec::with_capacity(num_entries);

        for i in 0..num_entries {
            let idx = 4 + i * 16;
            let value = u64::from_le_bytes([
                data[idx], data[idx+1], data[idx+2], data[idx+3],
                data[idx+4], data[idx+5], data[idx+6], data[idx+7],
            ]);
            let offset = u32::from_le_bytes([
                data[idx+8], data[idx+9], data[idx+10], data[idx+11],
            ]) as usize;
            let length = u32::from_le_bytes([
                data[idx+12], data[idx+13], data[idx+14], data[idx+15],
            ]) as usize;

            let start = data_start + offset;
            let end = start + length;
            if end > data.len() {
                return Err(BitdexError::DocStore("filter bitmap data truncated".into()));
            }

            let bm = RoaringBitmap::deserialize_from(&data[start..end])
                .map_err(|e| BitdexError::DocStore(format!("filter bitmap deserialize: {e}")))?;
            result.push((value, bm));
        }

        Ok(result)
    }

    /// Load specific values from a field's bucket pack files.
    /// Groups requested values by bucket, reads only the needed pack files,
    /// and deserializes only the matching entries. Values not present on disk
    /// are simply absent from the result.
    pub fn load_field_values(
        &self,
        field_name: &str,
        values: &[u64],
    ) -> Result<HashMap<u64, RoaringBitmap>> {
        if values.is_empty() {
            return Ok(HashMap::new());
        }

        // Group requested values by bucket
        let mut by_bucket: HashMap<u8, Vec<u64>> = HashMap::new();
        for &v in values {
            by_bucket.entry(Self::filter_bucket(v)).or_default().push(v);
        }

        let mut result = HashMap::with_capacity(values.len());

        for (bucket, wanted) in &by_bucket {
            let path = self.filter_pack_path(field_name, *bucket);
            let data = match std::fs::read(&path) {
                Ok(d) => d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(BitdexError::DocStore(format!("read pack file: {e}"))),
            };

            if data.len() < 4 {
                return Err(BitdexError::DocStore("filter pack header truncated".into()));
            }

            let num_entries =
                u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            let header_size = 4 + num_entries * 16;
            if data.len() < header_size {
                return Err(BitdexError::DocStore("filter pack index truncated".into()));
            }

            let data_start = header_size;

            // Scan the index for matching values only
            for i in 0..num_entries {
                let idx = 4 + i * 16;
                let value = u64::from_le_bytes([
                    data[idx],
                    data[idx + 1],
                    data[idx + 2],
                    data[idx + 3],
                    data[idx + 4],
                    data[idx + 5],
                    data[idx + 6],
                    data[idx + 7],
                ]);

                if !wanted.contains(&value) {
                    continue;
                }

                let offset = u32::from_le_bytes([
                    data[idx + 8],
                    data[idx + 9],
                    data[idx + 10],
                    data[idx + 11],
                ]) as usize;
                let length = u32::from_le_bytes([
                    data[idx + 12],
                    data[idx + 13],
                    data[idx + 14],
                    data[idx + 15],
                ]) as usize;

                let start = data_start + offset;
                let end = start + length;
                if end > data.len() {
                    return Err(BitdexError::DocStore(
                        "filter bitmap data truncated".into(),
                    ));
                }

                let bm = RoaringBitmap::deserialize_from(&data[start..end]).map_err(|e| {
                    BitdexError::DocStore(format!("filter bitmap deserialize: {e}"))
                })?;
                result.insert(value, bm);
            }
        }

        Ok(result)
    }

    /// Load all bitmaps for a single field by reading all bucket pack files.
    pub fn load_field(&self, field_name: &str) -> Result<HashMap<u64, RoaringBitmap>> {
        let dir = self.root.join("filter").join(field_name);
        let mut result = HashMap::new();
        if !dir.exists() {
            return Ok(result);
        }
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| BitdexError::DocStore(format!("read filter dir: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let path = entry.path();
            if path.extension().map_or(true, |ext| ext != "fpack") {
                continue;
            }
            let data = std::fs::read(&path)
                .map_err(|e| BitdexError::DocStore(format!("read pack file: {e}")))?;
            for (value, bm) in Self::read_pack_file(&data)? {
                result.insert(value, bm);
            }
        }
        Ok(result)
    }

    /// List all existing value keys for a field without loading bitmap payloads.
    /// Reads only the `.fpack` header index (value IDs) from each bucket file.
    /// Used to build positive existence sets for zero-result query elimination.
    pub fn list_field_keys(&self, field_name: &str) -> Result<HashSet<u64>> {
        let dir = self.root.join("filter").join(field_name);
        let mut keys = HashSet::new();
        if !dir.exists() {
            return Ok(keys);
        }
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| BitdexError::DocStore(format!("read filter dir: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let path = entry.path();
            if path.extension().map_or(true, |ext| ext != "fpack") {
                continue;
            }
            let data = std::fs::read(&path)
                .map_err(|e| BitdexError::DocStore(format!("read pack file: {e}")))?;
            if data.len() < 4 {
                continue;
            }
            let num_entries =
                u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            let header_size = 4 + num_entries * 16;
            if data.len() < header_size {
                continue;
            }
            for i in 0..num_entries {
                let idx = 4 + i * 16;
                let value = u64::from_le_bytes([
                    data[idx], data[idx + 1], data[idx + 2], data[idx + 3],
                    data[idx + 4], data[idx + 5], data[idx + 6], data[idx + 7],
                ]);
                keys.insert(value);
            }
        }
        Ok(keys)
    }

    /// Load multiple fields at once.
    pub fn load_all_fields(
        &self,
        field_names: &[&str],
    ) -> Result<HashMap<String, HashMap<u64, RoaringBitmap>>> {
        let mut result = HashMap::new();
        for name in field_names {
            result.insert(name.to_string(), self.load_field(name)?);
        }
        Ok(result)
    }

    /// Write multiple filter bitmap entries, grouped by field + hex bucket.
    /// Write a single filter bucket directly (used by streaming save).
    pub fn write_filter_bucket(&self, field: &str, bucket: u8, entries: &[(u64, &RoaringBitmap)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let path = self.filter_pack_path(field, bucket);
        Self::write_pack_file(&path, entries)
    }

    pub fn write_batch(&self, entries: &[(&str, u64, &RoaringBitmap)]) -> Result<()> {
        // Group by (field, bucket)
        let mut by_bucket: HashMap<(&str, u8), Vec<(u64, &RoaringBitmap)>> = HashMap::new();
        for &(field, value, bitmap) in entries {
            let bucket = Self::filter_bucket(value);
            by_bucket.entry((field, bucket)).or_default().push((value, bitmap));
        }
        // Write one pack file per (field, bucket)
        for ((field, bucket), bitmaps) in &by_bucket {
            let path = self.filter_pack_path(field, *bucket);
            Self::write_pack_file(&path, bitmaps)?;
        }
        Ok(())
    }

    // ---- Alive bitmap ----

    /// Write the alive bitmap.
    pub fn write_alive(&self, bitmap: &RoaringBitmap) -> Result<()> {
        Self::write_bitmap_atomic(&self.root.join("system").join("alive.roar"), bitmap)
    }

    /// Load the alive bitmap.
    pub fn load_alive(&self) -> Result<Option<RoaringBitmap>> {
        Self::read_bitmap(&self.root.join("system").join("alive.roar"))
    }

    // ---- Sort layers (packed single-file per sort field) ----
    //
    // Format: sort/{field}.sort
    // [u8 num_layers][layer_index: N × (u8 bit_position, u32 offset, u32 length)][packed roaring bitmaps]
    //
    // All 32 layers for a sort field in one file. One open, one read, one atomic write.

    fn sort_field_path(&self, field: &str) -> PathBuf {
        self.root.join("sort").join(format!("{field}.sort"))
    }

    /// Write all sort layers for a field as a single packed file.
    pub fn write_sort_layers(&self, field: &str, layers: &[&RoaringBitmap]) -> Result<()> {
        let path = self.sort_field_path(field);

        // Serialize all layers
        let mut layer_data: Vec<Vec<u8>> = Vec::with_capacity(layers.len());
        for bm in layers {
            let mut buf = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut buf)
                .map_err(|e| BitdexError::DocStore(format!("sort layer serialize: {e}")))?;
            layer_data.push(buf);
        }

        // Build packed file
        let header_size = 1 + layers.len() * 9; // 1 byte num_layers + N × (1 + 4 + 4)
        let data_size: usize = layer_data.iter().map(|d| d.len()).sum();
        let mut buf = Vec::with_capacity(header_size + data_size);

        // Header: num_layers
        buf.push(layers.len() as u8);

        // Index table: (bit_position, offset, length) for each layer
        let mut offset: u32 = 0;
        for (i, data) in layer_data.iter().enumerate() {
            buf.push(i as u8);
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            offset += data.len() as u32;
        }

        // Packed bitmap data
        for data in &layer_data {
            buf.extend_from_slice(data);
        }

        Self::write_bytes_atomic(&path, &buf)
    }

    /// Load sort layers for a field from the packed file. Returns None if not found.
    pub fn load_sort_layers(
        &self,
        field: &str,
        num_layers: usize,
    ) -> Result<Option<Vec<RoaringBitmap>>> {
        let path = self.sort_field_path(field);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(BitdexError::DocStore(format!("read sort file: {e}"))),
        };

        if data.is_empty() {
            return Ok(None);
        }

        let stored_layers = data[0] as usize;
        let header_size = 1 + stored_layers * 9;
        if data.len() < header_size {
            return Err(BitdexError::DocStore("sort file header truncated".into()));
        }

        let data_start = header_size;
        let mut layers = vec![RoaringBitmap::new(); num_layers];

        for i in 0..stored_layers {
            let idx_offset = 1 + i * 9;
            let bit_pos = data[idx_offset] as usize;
            let offset = u32::from_le_bytes([
                data[idx_offset + 1], data[idx_offset + 2],
                data[idx_offset + 3], data[idx_offset + 4],
            ]) as usize;
            let length = u32::from_le_bytes([
                data[idx_offset + 5], data[idx_offset + 6],
                data[idx_offset + 7], data[idx_offset + 8],
            ]) as usize;

            if bit_pos < num_layers {
                let start = data_start + offset;
                let end = start + length;
                if end > data.len() {
                    return Err(BitdexError::DocStore("sort layer data truncated".into()));
                }
                layers[bit_pos] = RoaringBitmap::deserialize_from(&data[start..end])
                    .map_err(|e| BitdexError::DocStore(format!("sort layer deserialize: {e}")))?;
            }
        }

        Ok(Some(layers))
    }

    // ---- Slot counter ----

    /// Write the slot counter.
    pub fn write_slot_counter(&self, counter: u32) -> Result<()> {
        Self::write_bytes_atomic(
            &self.root.join("meta").join("slot_counter.bin"),
            &counter.to_le_bytes(),
        )
    }

    /// Load the slot counter.
    pub fn load_slot_counter(&self) -> Result<Option<u32>> {
        let path = self.root.join("meta").join("slot_counter.bin");
        match std::fs::read(&path) {
            Ok(bytes) if bytes.len() >= 4 => {
                let counter = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Ok(Some(counter))
            }
            Ok(_) => Err(BitdexError::DocStore("slot counter too short".into())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BitdexError::DocStore(format!("read slot counter: {e}"))),
        }
    }

    // ---- Time bucket bitmaps ----
    //
    // Layout: time_buckets/{name}.roar
    // One roaring bitmap per time bucket (24h, 7d, 30d, 1y).

    fn time_bucket_path(&self, bucket_name: &str) -> PathBuf {
        self.root.join("time_buckets").join(format!("{bucket_name}.roar"))
    }

    /// Write a single time bucket bitmap.
    pub fn write_time_bucket(&self, bucket_name: &str, bitmap: &RoaringBitmap) -> Result<()> {
        let path = self.time_bucket_path(bucket_name);
        let mut buf = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut buf)
            .map_err(|e| BitdexError::DocStore(format!("time bucket serialize: {e}")))?;
        Self::write_bytes_atomic(&path, &buf)
    }

    /// Load all time bucket bitmaps. Returns (name, bitmap) pairs for each found bucket.
    pub fn load_time_buckets(&self) -> Result<Vec<(String, RoaringBitmap)>> {
        let dir = self.root.join("time_buckets");
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(BitdexError::DocStore(format!("read time_buckets dir: {e}"))),
        };
        let mut result = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| BitdexError::DocStore(format!("dir entry: {e}")))?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("roar") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(bm) = Self::read_bitmap(&path)? {
                        result.push((name.to_string(), bm));
                    }
                }
            }
        }
        Ok(result)
    }

    // ---- Full snapshot ----

    /// Write all engine state: filter bitmaps, alive, sort layers, slot counter.
    pub fn write_full_snapshot(
        &self,
        filter_entries: &[(&str, u64, &RoaringBitmap)],
        alive: &RoaringBitmap,
        sort_layers: &[(&str, &[&RoaringBitmap])],
        slot_counter: u32,
    ) -> Result<()> {
        // Write critical metadata first (alive + slot counter) so partial saves
        // still produce a usable restart. Filter/sort writes are the slow part.
        self.write_alive(alive)?;
        self.write_slot_counter(slot_counter)?;
        for &(field, layers) in sort_layers {
            self.write_sort_layers(field, layers)?;
        }
        self.write_batch(filter_entries)?;
        Ok(())
    }

    /// Count total stored filter bitmap files (for metrics).
    pub fn bitmap_count(&self) -> Result<usize> {
        let filter_dir = self.root.join("filter");
        if !filter_dir.exists() {
            return Ok(0);
        }
        let mut count = 0;
        // Scan field directories, count entries across all .fpack files
        for field_entry in std::fs::read_dir(&filter_dir)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?
        {
            let field_entry = field_entry.map_err(|e| BitdexError::DocStore(e.to_string()))?;
            if !field_entry.path().is_dir() { continue; }
            let field_name = field_entry.path().file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            count += self.load_field(&field_name)?.len();
        }
        Ok(count)
    }

    // ---- Named cursors (opaque string key-value pairs) ----
    //
    // Layout: cursors/{name}
    // Each file contains the cursor value as UTF-8 text.

    /// Write a named cursor value atomically.
    pub fn write_cursor(&self, name: &str, value: &str) -> Result<()> {
        let dir = self.root.join("cursors");
        std::fs::create_dir_all(&dir)
            .map_err(|e| BitdexError::DocStore(format!("create cursors dir: {e}")))?;
        Self::write_bytes_atomic(&dir.join(name), value.as_bytes())
    }

    /// Load a single named cursor. Returns None if it doesn't exist.
    pub fn load_cursor(&self, name: &str) -> Result<Option<String>> {
        let path = self.root.join("cursors").join(name);
        match std::fs::read_to_string(&path) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BitdexError::DocStore(format!("read cursor {name}: {e}"))),
        }
    }

    /// Load all named cursors from disk.
    pub fn load_all_cursors(&self) -> Result<HashMap<String, String>> {
        let dir = self.root.join("cursors");
        let mut result = HashMap::new();
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
            Err(e) => return Err(BitdexError::DocStore(format!("read cursors dir: {e}"))),
        };
        for entry in entries {
            let entry = entry.map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // Skip tmp files from atomic writes
                    if name.ends_with(".tmp") {
                        continue;
                    }
                    let value = std::fs::read_to_string(&path)
                        .map_err(|e| BitdexError::DocStore(format!("read cursor {name}: {e}")))?;
                    result.insert(name.to_string(), value);
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bitmap(values: &[u32]) -> RoaringBitmap {
        values.iter().copied().collect()
    }

    #[test]
    fn test_write_and_load_field() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        let bm1 = make_bitmap(&[1, 2, 3]);
        let bm2 = make_bitmap(&[10, 20, 30]);

        store.write_batch(&[("tagIds", 42, &bm1), ("tagIds", 99, &bm2)]).unwrap();

        let loaded = store.load_field("tagIds").unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&42], bm1);
        assert_eq!(loaded[&99], bm2);
    }

    #[test]
    fn test_load_nonexistent_field() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();
        let loaded = store.load_field("doesNotExist").unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_overwrite_filter_field() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        let bm1 = make_bitmap(&[1, 2, 3]);
        let bm2 = make_bitmap(&[10, 20]);
        store.write_batch(&[("tagIds", 42, &bm1), ("tagIds", 99, &bm2)]).unwrap();

        // Overwrite with fewer entries — old values should be gone
        let bm3 = make_bitmap(&[50]);
        store.write_batch(&[("tagIds", 42, &bm3)]).unwrap();

        let loaded = store.load_field("tagIds").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[&42], bm3);
        assert!(!loaded.contains_key(&99), "old value 99 should be removed");
    }

    #[test]
    fn test_alive_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        assert!(store.load_alive().unwrap().is_none());

        let alive = make_bitmap(&[1, 2, 5, 100, 9999]);
        store.write_alive(&alive).unwrap();

        let loaded = store.load_alive().unwrap().unwrap();
        assert_eq!(alive, loaded);
    }

    #[test]
    fn test_sort_layers_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        assert!(store.load_sort_layers("score", 32).unwrap().is_none());

        let l0 = make_bitmap(&[1, 3, 5]);
        let l1 = make_bitmap(&[2, 4]);
        let l2 = RoaringBitmap::new();
        store.write_sort_layers("score", &[&l0, &l1, &l2]).unwrap();

        let loaded = store.load_sort_layers("score", 3).unwrap().unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0], l0);
        assert_eq!(loaded[1], l1);
        assert_eq!(loaded[2], l2);
    }

    #[test]
    fn test_slot_counter_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        assert!(store.load_slot_counter().unwrap().is_none());

        store.write_slot_counter(12345).unwrap();
        assert_eq!(store.load_slot_counter().unwrap().unwrap(), 12345);
    }

    #[test]
    fn test_full_snapshot_persists() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        let bm = make_bitmap(&[1, 2, 3]);
        let alive = make_bitmap(&[1, 2, 3]);
        let sl = make_bitmap(&[1, 3]);

        store
            .write_full_snapshot(
                &[("field", 10, &bm)],
                &alive,
                &[("sort", &[&sl])],
                100,
            )
            .unwrap();

        // Reopen from same dir
        let store2 = BitmapFs::new(dir.path()).unwrap();
        assert_eq!(store2.load_alive().unwrap().unwrap(), alive);
        assert_eq!(store2.load_slot_counter().unwrap().unwrap(), 100);
        assert_eq!(store2.load_field("field").unwrap()[&10], bm);
        assert_eq!(store2.load_sort_layers("sort", 1).unwrap().unwrap()[0], sl);
    }

    #[test]
    fn test_bitmap_count() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();
        assert_eq!(store.bitmap_count().unwrap(), 0);

        let bm = make_bitmap(&[1]);
        store.write_batch(&[("a", 1, &bm), ("b", 2, &bm), ("a", 3, &bm)]).unwrap();
        assert_eq!(store.bitmap_count().unwrap(), 3);
    }

    #[test]
    fn test_load_field_values_selective() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        let bm1 = make_bitmap(&[1, 2, 3]);
        let bm2 = make_bitmap(&[10, 20, 30]);
        let bm3 = make_bitmap(&[100, 200]);

        store
            .write_batch(&[
                ("tagIds", 42, &bm1),
                ("tagIds", 99, &bm2),
                ("tagIds", 7, &bm3),
            ])
            .unwrap();

        // Load only value 42 — should get just that one
        let loaded = store.load_field_values("tagIds", &[42]).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[&42], bm1);

        // Load values 99 and 7 — should get both
        let loaded = store.load_field_values("tagIds", &[99, 7]).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&99], bm2);
        assert_eq!(loaded[&7], bm3);

        // Load a value that doesn't exist — empty result
        let loaded = store.load_field_values("tagIds", &[999]).unwrap();
        assert!(loaded.is_empty());

        // Load from nonexistent field — empty result
        let loaded = store.load_field_values("nope", &[1]).unwrap();
        assert!(loaded.is_empty());

        // Empty values slice — empty result
        let loaded = store.load_field_values("tagIds", &[]).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_field_values_cross_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        // Values in different buckets (bucket = (value >> 8) & 0xFF)
        // value 1 → bucket 0, value 256 → bucket 1, value 512 → bucket 2
        let bm1 = make_bitmap(&[1]);
        let bm2 = make_bitmap(&[2]);
        let bm3 = make_bitmap(&[3]);

        store
            .write_batch(&[
                ("field", 1, &bm1),
                ("field", 256, &bm2),
                ("field", 512, &bm3),
            ])
            .unwrap();

        // Load values from different buckets in one call
        let loaded = store.load_field_values("field", &[1, 512]).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&1], bm1);
        assert_eq!(loaded[&512], bm3);
    }

    #[test]
    fn test_cursor_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        // No cursors initially
        assert!(store.load_all_cursors().unwrap().is_empty());
        assert!(store.load_cursor("pg-sync-0").unwrap().is_none());

        // Write a cursor
        store.write_cursor("pg-sync-0", "48291537").unwrap();
        assert_eq!(store.load_cursor("pg-sync-0").unwrap().unwrap(), "48291537");

        // Write another cursor
        store.write_cursor("pg-sync-1", "48291200").unwrap();

        // Load all
        let all = store.load_all_cursors().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all["pg-sync-0"], "48291537");
        assert_eq!(all["pg-sync-1"], "48291200");

        // Overwrite
        store.write_cursor("pg-sync-0", "48291600").unwrap();
        assert_eq!(store.load_cursor("pg-sync-0").unwrap().unwrap(), "48291600");
    }

    #[test]
    fn test_cursor_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = BitmapFs::new(dir.path()).unwrap();

        store.write_cursor("my-cursor", "12345").unwrap();

        // Reopen from same dir
        let store2 = BitmapFs::new(dir.path()).unwrap();
        assert_eq!(store2.load_cursor("my-cursor").unwrap().unwrap(), "12345");

        let all = store2.load_all_cursors().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all["my-cursor"], "12345");
    }
}
