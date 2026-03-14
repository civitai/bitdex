//! BoundStore — Unified Cache Persistence
//!
//! Persists unified cache entries (bounded bitmaps) to disk so the server
//! starts warm. Uses two file types:
//!
//! - `meta.bin`: Entry registrations + tombstone bitmap (eager load on startup)
//! - `{field}_{direction}.ucpack`: Packed cache entry bitmaps (lazy load per shard)
//!
//! Directory layout:
//! ```text
//! bitmaps/bounds/
//!   meta.bin
//!   reactionCount_Desc.ucpack
//!   sortAt_Desc.ucpack
//!   ...
//! ```
//!
//! Reuses proven patterns from BitmapFs: atomic tmp→fsync→rename writes,
//! pack file format with index+data sections, lazy loading.

use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use crate::cache::CanonicalClause;
use crate::error::{BitdexError, Result};
use crate::meta_index::CacheEntryId;
use crate::query::SortDirection;

// ── Meta File Format ────────────────────────────────────────────────────────
//
// [u32 version = 1]
// [u32 num_entries]
// [entries: N × {
//     u32 entry_id
//     u16 sort_field_len
//     [u8] sort_field (UTF-8)
//     u8  direction (0=Desc, 1=Asc)
//     u32 key_len
//     [u8] key_bytes (msgpack-serialized Vec<CanonicalClause>)
//     u32 capacity
//     u32 max_capacity
//     u32 min_tracked_value
//     u64 total_matched
//     u8  has_more
// }]
// [u32 tombstone_bitmap_len]
// [u8] tombstone_bitmap_bytes
// [u32 next_entry_id]

const META_VERSION: u32 = 1;
const SHARD_VERSION: u32 = 1;

/// Registration data for a single cache entry (persisted in meta.bin).
#[derive(Debug, Clone)]
pub struct MetaEntry {
    pub entry_id: CacheEntryId,
    pub sort_field: String,
    pub direction: SortDirection,
    pub filter_clauses: Vec<CanonicalClause>,
    pub capacity: u32,
    pub max_capacity: u32,
    pub min_tracked_value: u32,
    pub total_matched: u64,
    pub has_more: bool,
}

/// Contents of a deserialized meta.bin file.
#[derive(Debug)]
pub struct MetaFile {
    pub entries: Vec<MetaEntry>,
    pub tombstones: RoaringBitmap,
    pub next_entry_id: CacheEntryId,
}

/// A single entry within a shard file (bitmap + key for HashMap insertion).
#[derive(Debug, Clone)]
pub struct ShardEntry {
    pub entry_id: CacheEntryId,
    pub filter_clauses: Vec<CanonicalClause>,
    pub bitmap: RoaringBitmap,
}

/// Key identifying a shard file: (sort_field, direction).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub sort_field: String,
    pub direction: SortDirection,
}

impl ShardKey {
    pub fn new(sort_field: String, direction: SortDirection) -> Self {
        Self { sort_field, direction }
    }

    /// Generate the filename for this shard.
    pub fn filename(&self) -> String {
        let dir_str = match self.direction {
            SortDirection::Desc => "Desc",
            SortDirection::Asc => "Asc",
        };
        format!("{}_{}.ucpack", self.sort_field, dir_str)
    }
}

/// Filesystem-based persistence for unified cache entries.
pub struct BoundStore {
    root: PathBuf,
}

impl BoundStore {
    /// Create a new BoundStore rooted at the given directory.
    /// Creates the directory if it doesn't exist.
    pub fn new(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)
            .map_err(|e| BitdexError::DocStore(format!("create bounds dir: {e}")))?;
        Ok(Self { root: root.to_path_buf() })
    }

    /// Get the root directory path.
    pub fn root_path(&self) -> &Path {
        &self.root
    }

    // ── Atomic Write Helpers ────────────────────────────────────────────

    fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
        let tmp_path = path.with_extension("tmp");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BitdexError::DocStore(format!("create dir: {e}")))?;
        }
        std::fs::write(&tmp_path, data)
            .map_err(|e| BitdexError::DocStore(format!("write tmp: {e}")))?;
        // fsync
        std::fs::OpenOptions::new()
            .write(true)
            .open(&tmp_path)
            .map_err(|e| BitdexError::DocStore(format!("open tmp for fsync: {e}")))?
            .sync_all()
            .map_err(|e| BitdexError::DocStore(format!("fsync tmp: {e}")))?;
        std::fs::rename(&tmp_path, path)
            .map_err(|e| BitdexError::DocStore(format!("rename: {e}")))?;
        Ok(())
    }

    // ── Meta File I/O ───────────────────────────────────────────────────

    fn meta_path(&self) -> PathBuf {
        self.root.join("meta.bin")
    }

    /// Write the meta file atomically.
    pub fn write_meta(&self, meta: &MetaFile) -> Result<()> {
        let buf = serialize_meta(meta)?;
        Self::write_atomic(&self.meta_path(), &buf)
    }

    /// Load the meta file. Returns None if it doesn't exist.
    pub fn load_meta(&self) -> Result<Option<MetaFile>> {
        let path = self.meta_path();
        match std::fs::read(&path) {
            Ok(data) => {
                match deserialize_meta(&data) {
                    Ok(meta) => Ok(Some(meta)),
                    Err(e) => {
                        eprintln!("BoundStore: meta.bin corrupt, purging cache: {e}");
                        self.purge()?;
                        Ok(None)
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BitdexError::DocStore(format!("read meta.bin: {e}"))),
        }
    }

    // ── Shard File I/O ──────────────────────────────────────────────────

    fn shard_path(&self, key: &ShardKey) -> PathBuf {
        self.root.join(key.filename())
    }

    /// Write a shard file atomically.
    pub fn write_shard(&self, key: &ShardKey, entries: &[ShardEntry]) -> Result<()> {
        let buf = serialize_shard(entries)?;
        Self::write_atomic(&self.shard_path(key), &buf)
    }

    /// Load a shard file. Returns None if it doesn't exist.
    pub fn load_shard(&self, key: &ShardKey) -> Result<Option<Vec<ShardEntry>>> {
        let path = self.shard_path(key);
        match std::fs::read(&path) {
            Ok(data) => {
                match deserialize_shard(&data) {
                    Ok(entries) => Ok(Some(entries)),
                    Err(e) => {
                        eprintln!("BoundStore: shard {} corrupt: {e}", key.filename());
                        // Delete corrupt shard
                        let _ = std::fs::remove_file(&path);
                        Ok(None)
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BitdexError::DocStore(format!("read shard {}: {e}", key.filename()))),
        }
    }

    /// List all shard keys that exist on disk.
    pub fn list_shards(&self) -> Result<Vec<ShardKey>> {
        let mut shards = Vec::new();
        let entries = match std::fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(shards),
            Err(e) => return Err(BitdexError::DocStore(format!("read bounds dir: {e}"))),
        };

        for entry in entries {
            let entry = entry.map_err(|e| BitdexError::DocStore(format!("read dir entry: {e}")))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(key) = parse_shard_filename(&name) {
                shards.push(key);
            }
        }
        Ok(shards)
    }

    /// Delete meta.bin and all .ucpack files (cache purge).
    pub fn purge(&self) -> Result<()> {
        let meta = self.meta_path();
        if meta.exists() {
            std::fs::remove_file(&meta)
                .map_err(|e| BitdexError::DocStore(format!("delete meta.bin: {e}")))?;
        }

        if let Ok(entries) = std::fs::read_dir(&self.root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("ucpack") {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        Ok(())
    }

    /// Delete a single shard file.
    pub fn delete_shard(&self, key: &ShardKey) -> Result<()> {
        let path = self.shard_path(key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(BitdexError::DocStore(format!("delete shard {}: {e}", key.filename()))),
        }
    }
}

// ── Serialization ───────────────────────────────────────────────────────────

fn serialize_meta(meta: &MetaFile) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(4096);

    // Header
    buf.extend_from_slice(&META_VERSION.to_le_bytes());
    buf.extend_from_slice(&(meta.entries.len() as u32).to_le_bytes());

    // Entries
    for entry in &meta.entries {
        buf.extend_from_slice(&entry.entry_id.to_le_bytes());

        // Sort field
        let sf_bytes = entry.sort_field.as_bytes();
        buf.extend_from_slice(&(sf_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(sf_bytes);

        // Direction
        let dir_byte: u8 = match entry.direction {
            SortDirection::Desc => 0,
            SortDirection::Asc => 1,
        };
        buf.push(dir_byte);

        // Key (msgpack-serialized filter clauses)
        let key_bytes = rmp_serde::to_vec(&entry.filter_clauses)
            .map_err(|e| BitdexError::DocStore(format!("serialize filter clauses: {e}")))?;
        buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&key_bytes);

        // Metadata
        buf.extend_from_slice(&entry.capacity.to_le_bytes());
        buf.extend_from_slice(&entry.max_capacity.to_le_bytes());
        buf.extend_from_slice(&entry.min_tracked_value.to_le_bytes());
        buf.extend_from_slice(&entry.total_matched.to_le_bytes());
        buf.push(if entry.has_more { 1 } else { 0 });
    }

    // Tombstone bitmap
    let mut tombstone_buf = Vec::with_capacity(meta.tombstones.serialized_size());
    meta.tombstones
        .serialize_into(&mut tombstone_buf)
        .map_err(|e| BitdexError::DocStore(format!("serialize tombstones: {e}")))?;
    buf.extend_from_slice(&(tombstone_buf.len() as u32).to_le_bytes());
    buf.extend_from_slice(&tombstone_buf);

    // Next entry ID
    buf.extend_from_slice(&meta.next_entry_id.to_le_bytes());

    Ok(buf)
}

fn deserialize_meta(data: &[u8]) -> Result<MetaFile> {
    let mut pos = 0;

    let version = read_u32(data, &mut pos)?;
    if version != META_VERSION {
        return Err(BitdexError::DocStore(format!("unsupported meta version: {version}")));
    }

    let num_entries = read_u32(data, &mut pos)? as usize;
    let mut entries = Vec::with_capacity(num_entries);

    for _ in 0..num_entries {
        let entry_id = read_u32(data, &mut pos)?;

        // Sort field
        let sf_len = read_u16(data, &mut pos)? as usize;
        if pos + sf_len > data.len() {
            return Err(BitdexError::DocStore("meta entry truncated (sort_field)".into()));
        }
        let sort_field = std::str::from_utf8(&data[pos..pos + sf_len])
            .map_err(|e| BitdexError::DocStore(format!("invalid sort_field UTF-8: {e}")))?
            .to_string();
        pos += sf_len;

        // Direction
        if pos >= data.len() {
            return Err(BitdexError::DocStore("meta entry truncated (direction)".into()));
        }
        let direction = match data[pos] {
            0 => SortDirection::Desc,
            1 => SortDirection::Asc,
            d => return Err(BitdexError::DocStore(format!("invalid direction byte: {d}"))),
        };
        pos += 1;

        // Key
        let key_len = read_u32(data, &mut pos)? as usize;
        if pos + key_len > data.len() {
            return Err(BitdexError::DocStore("meta entry truncated (key)".into()));
        }
        let filter_clauses: Vec<CanonicalClause> = rmp_serde::from_slice(&data[pos..pos + key_len])
            .map_err(|e| BitdexError::DocStore(format!("deserialize filter clauses: {e}")))?;
        pos += key_len;

        // Metadata
        let capacity = read_u32(data, &mut pos)?;
        let max_capacity = read_u32(data, &mut pos)?;
        let min_tracked_value = read_u32(data, &mut pos)?;
        let total_matched = read_u64(data, &mut pos)?;

        if pos >= data.len() {
            return Err(BitdexError::DocStore("meta entry truncated (has_more)".into()));
        }
        let has_more = data[pos] != 0;
        pos += 1;

        entries.push(MetaEntry {
            entry_id,
            sort_field,
            direction,
            filter_clauses,
            capacity,
            max_capacity,
            min_tracked_value,
            total_matched,
            has_more,
        });
    }

    // Tombstone bitmap
    let tombstone_len = read_u32(data, &mut pos)? as usize;
    if pos + tombstone_len > data.len() {
        return Err(BitdexError::DocStore("meta truncated (tombstone bitmap)".into()));
    }
    let tombstones = RoaringBitmap::deserialize_from(&data[pos..pos + tombstone_len])
        .map_err(|e| BitdexError::DocStore(format!("deserialize tombstones: {e}")))?;
    pos += tombstone_len;

    // Next entry ID
    let next_entry_id = read_u32(data, &mut pos)?;

    Ok(MetaFile {
        entries,
        tombstones,
        next_entry_id,
    })
}

// ── Shard Serialization ─────────────────────────────────────────────────────
//
// [u32 version = 1]
// [u32 num_entries]
// [index: N × {
//     u32 entry_id
//     u32 key_offset     (into key section)
//     u32 key_length
//     u32 bitmap_offset  (into bitmap section)
//     u32 bitmap_length
// }]
// [key section: concatenated msgpack keys]
// [bitmap section: concatenated serialized roaring bitmaps]

const SHARD_INDEX_ENTRY_SIZE: usize = 20; // 5 × u32

fn serialize_shard(entries: &[ShardEntry]) -> Result<Vec<u8>> {
    // Pre-serialize all keys and bitmaps
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
    let mut bitmaps: Vec<Vec<u8>> = Vec::with_capacity(entries.len());

    for entry in entries {
        let key_bytes = rmp_serde::to_vec(&entry.filter_clauses)
            .map_err(|e| BitdexError::DocStore(format!("serialize shard key: {e}")))?;
        keys.push(key_bytes);

        let mut bm_buf = Vec::with_capacity(entry.bitmap.serialized_size());
        entry.bitmap
            .serialize_into(&mut bm_buf)
            .map_err(|e| BitdexError::DocStore(format!("serialize shard bitmap: {e}")))?;
        bitmaps.push(bm_buf);
    }

    let header_size = 8 + entries.len() * SHARD_INDEX_ENTRY_SIZE;
    let key_section_size: usize = keys.iter().map(|k| k.len()).sum();
    let bitmap_section_size: usize = bitmaps.iter().map(|b| b.len()).sum();
    let total_size = header_size + key_section_size + bitmap_section_size;

    let mut buf = Vec::with_capacity(total_size);

    // Header
    buf.extend_from_slice(&SHARD_VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    // Index
    let mut key_offset: u32 = 0;
    let mut bitmap_offset: u32 = 0;
    for i in 0..entries.len() {
        buf.extend_from_slice(&entries[i].entry_id.to_le_bytes());
        buf.extend_from_slice(&key_offset.to_le_bytes());
        buf.extend_from_slice(&(keys[i].len() as u32).to_le_bytes());
        buf.extend_from_slice(&bitmap_offset.to_le_bytes());
        buf.extend_from_slice(&(bitmaps[i].len() as u32).to_le_bytes());
        key_offset += keys[i].len() as u32;
        bitmap_offset += bitmaps[i].len() as u32;
    }

    // Key section
    for key in &keys {
        buf.extend_from_slice(key);
    }

    // Bitmap section
    for bm in &bitmaps {
        buf.extend_from_slice(bm);
    }

    Ok(buf)
}

fn deserialize_shard(data: &[u8]) -> Result<Vec<ShardEntry>> {
    let mut pos = 0;

    let version = read_u32(data, &mut pos)?;
    if version != SHARD_VERSION {
        return Err(BitdexError::DocStore(format!("unsupported shard version: {version}")));
    }

    let num_entries = read_u32(data, &mut pos)? as usize;
    let index_size = num_entries * SHARD_INDEX_ENTRY_SIZE;
    if pos + index_size > data.len() {
        return Err(BitdexError::DocStore("shard index truncated".into()));
    }

    // Parse index
    struct IndexEntry {
        entry_id: u32,
        key_offset: u32,
        key_length: u32,
        bitmap_offset: u32,
        bitmap_length: u32,
    }

    let mut index = Vec::with_capacity(num_entries);
    for _ in 0..num_entries {
        index.push(IndexEntry {
            entry_id: read_u32(data, &mut pos)?,
            key_offset: read_u32(data, &mut pos)?,
            key_length: read_u32(data, &mut pos)?,
            bitmap_offset: read_u32(data, &mut pos)?,
            bitmap_length: read_u32(data, &mut pos)?,
        });
    }

    // Section offsets
    let key_section_start = pos;
    let key_section_size: usize = index.iter().map(|e| e.key_length as usize).sum();
    let bitmap_section_start = key_section_start + key_section_size;

    let mut entries = Vec::with_capacity(num_entries);
    for ie in &index {
        // Deserialize key
        let ks = key_section_start + ie.key_offset as usize;
        let ke = ks + ie.key_length as usize;
        if ke > data.len() {
            return Err(BitdexError::DocStore("shard key data truncated".into()));
        }
        let filter_clauses: Vec<CanonicalClause> = rmp_serde::from_slice(&data[ks..ke])
            .map_err(|e| BitdexError::DocStore(format!("deserialize shard key: {e}")))?;

        // Deserialize bitmap
        let bs = bitmap_section_start + ie.bitmap_offset as usize;
        let be = bs + ie.bitmap_length as usize;
        if be > data.len() {
            return Err(BitdexError::DocStore("shard bitmap data truncated".into()));
        }
        let bitmap = RoaringBitmap::deserialize_from(&data[bs..be])
            .map_err(|e| BitdexError::DocStore(format!("deserialize shard bitmap: {e}")))?;

        entries.push(ShardEntry {
            entry_id: ie.entry_id,
            filter_clauses,
            bitmap,
        });
    }

    Ok(entries)
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn parse_shard_filename(name: &str) -> Option<ShardKey> {
    let stem = name.strip_suffix(".ucpack")?;
    let (field, dir_str) = stem.rsplit_once('_')?;
    let direction = match dir_str {
        "Desc" => SortDirection::Desc,
        "Asc" => SortDirection::Asc,
        _ => return None,
    };
    Some(ShardKey {
        sort_field: field.to_string(),
        direction,
    })
}

fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16> {
    if *pos + 2 > data.len() {
        return Err(BitdexError::DocStore("unexpected EOF reading u16".into()));
    }
    let val = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;
    Ok(val)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > data.len() {
        return Err(BitdexError::DocStore("unexpected EOF reading u32".into()));
    }
    let val = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(val)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > data.len() {
        return Err(BitdexError::DocStore("unexpected EOF reading u64".into()));
    }
    let val = u64::from_le_bytes([
        data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
        data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
    ]);
    *pos += 8;
    Ok(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_clause(field: &str, value: &str) -> CanonicalClause {
        CanonicalClause {
            field: field.to_string(),
            op: "eq".to_string(),
            value_repr: value.to_string(),
        }
    }

    fn make_meta_entry(id: u32, sort_field: &str, direction: SortDirection) -> MetaEntry {
        MetaEntry {
            entry_id: id,
            sort_field: sort_field.to_string(),
            direction,
            filter_clauses: vec![make_clause("nsfwLevel", "1")],
            capacity: 4000,
            max_capacity: 64000,
            min_tracked_value: 500,
            total_matched: 12345,
            has_more: true,
        }
    }

    // ── Meta round-trip tests ───────────────────────────────────────────

    #[test]
    fn test_meta_round_trip_empty() {
        let meta = MetaFile {
            entries: vec![],
            tombstones: RoaringBitmap::new(),
            next_entry_id: 0,
        };
        let buf = serialize_meta(&meta).unwrap();
        let restored = deserialize_meta(&buf).unwrap();
        assert!(restored.entries.is_empty());
        assert!(restored.tombstones.is_empty());
        assert_eq!(restored.next_entry_id, 0);
    }

    #[test]
    fn test_meta_round_trip_with_entries() {
        let mut tombstones = RoaringBitmap::new();
        tombstones.insert(5);
        tombstones.insert(10);

        let meta = MetaFile {
            entries: vec![
                make_meta_entry(0, "reactionCount", SortDirection::Desc),
                make_meta_entry(1, "sortAt", SortDirection::Asc),
            ],
            tombstones,
            next_entry_id: 42,
        };

        let buf = serialize_meta(&meta).unwrap();
        let restored = deserialize_meta(&buf).unwrap();

        assert_eq!(restored.entries.len(), 2);
        assert_eq!(restored.entries[0].entry_id, 0);
        assert_eq!(restored.entries[0].sort_field, "reactionCount");
        assert_eq!(restored.entries[0].direction, SortDirection::Desc);
        assert_eq!(restored.entries[0].capacity, 4000);
        assert_eq!(restored.entries[0].max_capacity, 64000);
        assert_eq!(restored.entries[0].min_tracked_value, 500);
        assert_eq!(restored.entries[0].total_matched, 12345);
        assert!(restored.entries[0].has_more);
        assert_eq!(restored.entries[0].filter_clauses.len(), 1);
        assert_eq!(restored.entries[0].filter_clauses[0].field, "nsfwLevel");

        assert_eq!(restored.entries[1].entry_id, 1);
        assert_eq!(restored.entries[1].sort_field, "sortAt");
        assert_eq!(restored.entries[1].direction, SortDirection::Asc);

        assert!(restored.tombstones.contains(5));
        assert!(restored.tombstones.contains(10));
        assert_eq!(restored.tombstones.len(), 2);

        assert_eq!(restored.next_entry_id, 42);
    }

    #[test]
    fn test_meta_round_trip_complex_clauses() {
        let meta = MetaFile {
            entries: vec![MetaEntry {
                entry_id: 7,
                sort_field: "commentCount".to_string(),
                direction: SortDirection::Desc,
                filter_clauses: vec![
                    make_clause("nsfwLevel", "1"),
                    CanonicalClause {
                        field: "tagIds".to_string(),
                        op: "in".to_string(),
                        value_repr: "100,200,300".to_string(),
                    },
                    CanonicalClause {
                        field: "sortAt".to_string(),
                        op: "bucket".to_string(),
                        value_repr: "7d".to_string(),
                    },
                ],
                capacity: 64000,
                max_capacity: 64000,
                min_tracked_value: 0,
                total_matched: 999999,
                has_more: false,
            }],
            tombstones: RoaringBitmap::new(),
            next_entry_id: 8,
        };

        let buf = serialize_meta(&meta).unwrap();
        let restored = deserialize_meta(&buf).unwrap();

        assert_eq!(restored.entries[0].filter_clauses.len(), 3);
        assert_eq!(restored.entries[0].filter_clauses[1].op, "in");
        assert_eq!(restored.entries[0].filter_clauses[1].value_repr, "100,200,300");
        assert_eq!(restored.entries[0].filter_clauses[2].op, "bucket");
        assert!(!restored.entries[0].has_more);
    }

    // ── Shard round-trip tests ──────────────────────────────────────────

    #[test]
    fn test_shard_round_trip_empty() {
        let buf = serialize_shard(&[]).unwrap();
        let restored = deserialize_shard(&buf).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn test_shard_round_trip_with_entries() {
        let mut bm1 = RoaringBitmap::new();
        for i in 0..100 {
            bm1.insert(i * 10);
        }
        let mut bm2 = RoaringBitmap::new();
        for i in 500..600 {
            bm2.insert(i);
        }

        let entries = vec![
            ShardEntry {
                entry_id: 0,
                filter_clauses: vec![make_clause("nsfwLevel", "1")],
                bitmap: bm1.clone(),
            },
            ShardEntry {
                entry_id: 3,
                filter_clauses: vec![
                    make_clause("nsfwLevel", "1"),
                    make_clause("onSite", "true"),
                ],
                bitmap: bm2.clone(),
            },
        ];

        let buf = serialize_shard(&entries).unwrap();
        let restored = deserialize_shard(&buf).unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0].entry_id, 0);
        assert_eq!(restored[0].bitmap, bm1);
        assert_eq!(restored[0].filter_clauses.len(), 1);

        assert_eq!(restored[1].entry_id, 3);
        assert_eq!(restored[1].bitmap, bm2);
        assert_eq!(restored[1].filter_clauses.len(), 2);
    }

    // ── Filesystem tests ────────────────────────────────────────────────

    #[test]
    fn test_store_write_load_meta() {
        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();

        // No meta initially
        assert!(store.load_meta().unwrap().is_none());

        let meta = MetaFile {
            entries: vec![make_meta_entry(0, "reactionCount", SortDirection::Desc)],
            tombstones: RoaringBitmap::new(),
            next_entry_id: 1,
        };
        store.write_meta(&meta).unwrap();

        let loaded = store.load_meta().unwrap().unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].sort_field, "reactionCount");
    }

    #[test]
    fn test_store_write_load_shard() {
        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();

        let key = ShardKey::new("reactionCount".into(), SortDirection::Desc);

        // No shard initially
        assert!(store.load_shard(&key).unwrap().is_none());

        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        bm.insert(100);
        let entries = vec![ShardEntry {
            entry_id: 0,
            filter_clauses: vec![make_clause("nsfwLevel", "1")],
            bitmap: bm.clone(),
        }];
        store.write_shard(&key, &entries).unwrap();

        let loaded = store.load_shard(&key).unwrap().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].bitmap, bm);
    }

    #[test]
    fn test_store_list_shards() {
        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();

        store.write_shard(
            &ShardKey::new("reactionCount".into(), SortDirection::Desc),
            &[],
        ).unwrap();
        store.write_shard(
            &ShardKey::new("sortAt".into(), SortDirection::Asc),
            &[],
        ).unwrap();

        let shards = store.list_shards().unwrap();
        assert_eq!(shards.len(), 2);
        let names: Vec<String> = shards.iter().map(|s| s.filename()).collect();
        assert!(names.contains(&"reactionCount_Desc.ucpack".to_string()));
        assert!(names.contains(&"sortAt_Asc.ucpack".to_string()));
    }

    #[test]
    fn test_store_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();

        let meta = MetaFile {
            entries: vec![],
            tombstones: RoaringBitmap::new(),
            next_entry_id: 0,
        };
        store.write_meta(&meta).unwrap();
        store.write_shard(
            &ShardKey::new("x".into(), SortDirection::Desc),
            &[],
        ).unwrap();

        store.purge().unwrap();

        assert!(store.load_meta().unwrap().is_none());
        assert!(store.list_shards().unwrap().is_empty());
    }

    #[test]
    fn test_shard_filename_parsing() {
        assert_eq!(
            parse_shard_filename("reactionCount_Desc.ucpack"),
            Some(ShardKey::new("reactionCount".into(), SortDirection::Desc))
        );
        assert_eq!(
            parse_shard_filename("sortAt_Asc.ucpack"),
            Some(ShardKey::new("sortAt".into(), SortDirection::Asc))
        );
        assert_eq!(parse_shard_filename("meta.bin"), None);
        assert_eq!(parse_shard_filename("bad.ucpack"), None);
        assert_eq!(parse_shard_filename("field_Bad.ucpack"), None);
    }

    #[test]
    fn test_store_delete_shard() {
        let dir = tempfile::tempdir().unwrap();
        let store = BoundStore::new(&dir.path().join("bounds")).unwrap();

        let key = ShardKey::new("x".into(), SortDirection::Desc);
        store.write_shard(&key, &[]).unwrap();
        assert!(store.load_shard(&key).unwrap().is_some());

        store.delete_shard(&key).unwrap();
        assert!(store.load_shard(&key).unwrap().is_none());

        // Deleting non-existent shard is fine
        store.delete_shard(&key).unwrap();
    }

    #[test]
    fn test_corrupt_meta_triggers_purge() {
        let dir = tempfile::tempdir().unwrap();
        let bounds_dir = dir.path().join("bounds");
        let store = BoundStore::new(&bounds_dir).unwrap();

        // Write a shard
        store.write_shard(
            &ShardKey::new("x".into(), SortDirection::Desc),
            &[],
        ).unwrap();

        // Write corrupt meta
        std::fs::write(bounds_dir.join("meta.bin"), b"garbage data").unwrap();

        // Loading corrupt meta should purge and return None
        let result = store.load_meta().unwrap();
        assert!(result.is_none());

        // Shards should also be purged
        assert!(store.list_shards().unwrap().is_empty());
    }
}
