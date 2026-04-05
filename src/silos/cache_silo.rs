//! CacheSilo — persistent query cache backed by DataSilo.
//!
//! Persists cache entries across restarts. The key is a u32 hash
//! derived from the cache key (filter_clauses + sort_field + direction).
//! The value is a binary-encoded CacheEntryData.
//!
//! # Binary format (version 1)
//! ```text
//! [u8  version=1]
//! [u8  direction: 0=Asc, 1=Desc]
//! [u32 min_tracked_value]
//! [u32 capacity]
//! [u32 max_capacity]
//! [u8  has_more: 0/1]
//! [u64 total_matched]
//! [u32 bitmap_len][bitmap_bytes...]
//! [u32 sorted_keys_count][u64 sorted_keys...]   // 0 count means None
//! ```
//!
//! # Threading
//! CacheSilo is NOT on the hot query path. Only the flush thread writes
//! (save_entry / delete_entry) and startup reads (load_all). The merge
//! thread may call compact(). Wrapped in `Arc<parking_lot::RwLock<CacheSilo>>`
//! on ConcurrentEngine so threads share safely with minimal contention.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use super::cache::CanonicalClause;
use crate::query::SortDirection;

// ---------------------------------------------------------------------------
// UnifiedKey — moved here from unified_cache.rs (Phase 3)
// ---------------------------------------------------------------------------

/// Cache lookup key: canonical filter clauses + sort field + direction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct UnifiedKey {
    pub filter_clauses: Vec<CanonicalClause>,
    pub sort_field: String,
    pub direction: SortDirection,
}

// ---------------------------------------------------------------------------
// CacheEntryData — the serializable subset of UnifiedEntry
// ---------------------------------------------------------------------------

/// Serializable subset of UnifiedEntry for cross-restart persistence.
///
/// Does NOT include: `last_used`, `needs_rebuild`, `rebuilding`, `prefetching`,
/// `meta_id`, `persist_dirty`, `radix`, `bucket_cutoff`, `uses_bucket`.
/// These are either transient or rebuilt on demand.
#[derive(Debug, Clone)]
pub struct CacheEntryData {
    /// The cache key (filter clauses + sort field + direction).
    /// Stored alongside the entry so restore can reconstruct the UnifiedKey.
    pub key: UnifiedKey,
    /// Bounded top-K bitmap within the filter result.
    pub bitmap: RoaringBitmap,
    /// Sort floor (Desc) or ceiling (Asc) of the current bound.
    pub min_tracked_value: u32,
    /// Current capacity tier (initial or expanded).
    pub capacity: usize,
    /// Maximum capacity ceiling from config.
    pub max_capacity: usize,
    /// Whether more results exist beyond the current bound.
    pub has_more: bool,
    /// Total documents matching the filter predicate.
    pub total_matched: u64,
    /// Sort direction for this entry.
    pub direction: SortDirection,
    /// Pre-sorted packed keys `(sort_value << 32 | slot_id)` for initial-capacity entries.
    /// None when the entry has been expanded (radix takes over).
    pub sorted_keys: Option<Vec<u64>>,
    /// Global mutation epoch at the time this entry was formed (in-process only, not persisted).
    /// Disk-restored entries get epoch=0, which `is_stale()` treats as always-stale.
    pub epoch: u64,
    /// Per-field mutation epochs at the time this entry was formed (in-process only, not persisted).
    /// Maps field name → epoch. Stale if any field's current epoch exceeds the recorded value.
    pub field_epochs: Vec<(String, u64)>,
}

const FORMAT_VERSION: u8 = 2;

fn encode_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn decode_string(cur: &mut Cursor<&[u8]>) -> io::Result<String> {
    let mut len_buf = [0u8; 4];
    cur.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut str_buf = vec![0u8; len];
    cur.read_exact(&mut str_buf)?;
    String::from_utf8(str_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

impl CacheEntryData {
    /// Encode to bytes using the documented binary format.
    pub fn encode(&self) -> Vec<u8> {
        // Estimate capacity to avoid re-allocations.
        let bitmap_serialized_size = self.bitmap.serialized_size();
        let keys_len = self.sorted_keys.as_ref().map(|k| k.len()).unwrap_or(0);
        let estimated = 1 + 1 + 4 + 4 + 4 + 1 + 8 + 4 + bitmap_serialized_size + 4 + keys_len * 8;
        let mut buf = Vec::with_capacity(estimated);

        // Header
        buf.push(FORMAT_VERSION);
        buf.push(match self.direction {
            SortDirection::Asc => 0u8,
            SortDirection::Desc => 1u8,
        });
        buf.extend_from_slice(&(self.min_tracked_value).to_le_bytes());
        buf.extend_from_slice(&(self.capacity as u32).to_le_bytes());
        buf.extend_from_slice(&(self.max_capacity as u32).to_le_bytes());
        buf.push(if self.has_more { 1 } else { 0 });
        buf.extend_from_slice(&self.total_matched.to_le_bytes());

        // Bitmap: roaring serialization prefixed with u32 length
        let mut bitmap_bytes = Vec::with_capacity(bitmap_serialized_size);
        self.bitmap.serialize_into(&mut bitmap_bytes)
            .expect("RoaringBitmap serialization is infallible");
        buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bitmap_bytes);

        // Sorted keys: u32 count followed by u64 values
        match &self.sorted_keys {
            None => {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
            Some(keys) => {
                buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for &k in keys {
                    buf.extend_from_slice(&k.to_le_bytes());
                }
            }
        }

        // UnifiedKey: sort_field + direction + filter_clauses
        encode_string(&mut buf, &self.key.sort_field);
        // direction already encoded in header (byte 1)
        buf.extend_from_slice(&(self.key.filter_clauses.len() as u32).to_le_bytes());
        for cc in &self.key.filter_clauses {
            encode_string(&mut buf, &cc.field);
            encode_string(&mut buf, &cc.op);
            encode_string(&mut buf, &cc.value_repr);
        }

        buf
    }

    /// Decode from bytes. Returns an error if the bytes are malformed or the
    /// version is unrecognised.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut cur = Cursor::new(bytes);

        let mut version_buf = [0u8; 1];
        cur.read_exact(&mut version_buf)?;
        if version_buf[0] != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported CacheEntryData version {}", version_buf[0]),
            ));
        }

        let mut dir_buf = [0u8; 1];
        cur.read_exact(&mut dir_buf)?;
        let direction = match dir_buf[0] {
            0 => SortDirection::Asc,
            1 => SortDirection::Desc,
            b => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid direction byte {b}"),
                ))
            }
        };

        let min_tracked_value = read_u32_le(&mut cur)?;
        let capacity = read_u32_le(&mut cur)? as usize;
        let max_capacity = read_u32_le(&mut cur)? as usize;

        let mut has_more_buf = [0u8; 1];
        cur.read_exact(&mut has_more_buf)?;
        let has_more = has_more_buf[0] != 0;

        let total_matched = read_u64_le(&mut cur)?;

        // Bitmap
        let bitmap_len = read_u32_le(&mut cur)? as usize;
        let mut bitmap_bytes = vec![0u8; bitmap_len];
        cur.read_exact(&mut bitmap_bytes)?;
        let bitmap = RoaringBitmap::deserialize_from(Cursor::new(&bitmap_bytes))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bitmap decode: {e}")))?;

        // Sorted keys
        let keys_count = read_u32_le(&mut cur)? as usize;
        let sorted_keys = if keys_count == 0 {
            None
        } else {
            let mut keys = Vec::with_capacity(keys_count);
            for _ in 0..keys_count {
                keys.push(read_u64_le(&mut cur)?);
            }
            Some(keys)
        };

        // UnifiedKey
        let sort_field = decode_string(&mut cur)?;
        // direction already decoded from header
        let clause_count = read_u32_le(&mut cur)? as usize;
        let mut filter_clauses = Vec::with_capacity(clause_count);
        for _ in 0..clause_count {
            let field = decode_string(&mut cur)?;
            let op = decode_string(&mut cur)?;
            let value_repr = decode_string(&mut cur)?;
            filter_clauses.push(CanonicalClause { field, op, value_repr });
        }
        let key = UnifiedKey {
            filter_clauses,
            sort_field,
            direction,
        };

        Ok(Self {
            key,
            bitmap,
            min_tracked_value,
            capacity,
            max_capacity,
            has_more,
            total_matched,
            direction,
            sorted_keys,
            // Disk-restored entries have no epoch — treated as stale until re-seeded
            // in the current process lifetime.
            epoch: 0,
            field_epochs: Vec::new(),
        })
    }

    /// Check whether this entry is stale given a function that returns the
    /// current epoch for a named field.
    ///
    /// An entry is stale if:
    /// - It was formed with epoch=0 and no field_epochs (disk-restored or pre-epoch entries).
    /// - Any recorded field epoch is less than the current epoch for that field.
    pub fn is_stale<F>(&self, current_field_epoch: F) -> bool
    where
        F: Fn(&str) -> u64,
    {
        if self.epoch == 0 && self.field_epochs.is_empty() {
            // Disk-restored entry or pre-epoch entry — treat as stale so it gets
            // re-seeded with proper epoch tracking on the next query.
            return true;
        }
        for (field, recorded_epoch) in &self.field_epochs {
            if current_field_epoch(field) > *recorded_epoch {
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Key hashing
// ---------------------------------------------------------------------------

/// Derive a stable u64 key from a UnifiedKey.
///
/// Uses DefaultHasher (std deterministic within a single process run). This is
/// adequate for a persistent cache — collisions cause silent eviction (the key
/// stored under the same hash slot is overwritten), not correctness errors.
/// At typical cache sizes (<100K entries) the collision probability is negligible.
///
/// The key must not be 0 or u64::MAX (reserved by HashIndex as sentinel values).
/// We map those collisions to a safe nearby value.
pub fn hash_unified_key(key: &UnifiedKey) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let h = hasher.finish();
    // Avoid the two reserved sentinel values used by HashIndex.
    match h {
        0 => 1,
        u64::MAX => u64::MAX - 1,
        v => v,
    }
}

// ---------------------------------------------------------------------------
// CacheSilo
// ---------------------------------------------------------------------------

/// Persistent cache store: wraps a DataSilo whose keys are u64 hashes of
/// UnifiedKey and whose values are binary-encoded CacheEntryData.
pub struct CacheSilo {
    silo: datasilo::DataSilo,
    path: PathBuf,
}

impl CacheSilo {
    /// Open or create a CacheSilo at `path`. The directory is created if absent.
    pub fn open(path: &Path) -> io::Result<Self> {
        let config = datasilo::SiloConfig {
            buffer_ratio: 1.3,
            min_entry_size: 256,
            alignment: 1,
            compact_threshold: 0.20,
        };
        let silo = datasilo::DataSilo::open(path, config)?;
        Ok(Self { silo, path: path.to_path_buf() })
    }

    /// Persist a cache entry. Called by the flush thread after cache update.
    pub fn save_entry(&self, key_hash: u64, entry: &CacheEntryData) -> io::Result<()> {
        let bytes = entry.encode();
        self.silo.append_op(key_hash, &bytes)
    }

    /// Remove a persisted cache entry. Called on eviction.
    pub fn delete_entry(&self, key_hash: u64) -> io::Result<()> {
        self.silo.delete(key_hash)
    }

    /// Read a single entry by key hash. Checks both ops logs (last-write-wins) and
    /// falls back to the data file for compacted entries. Returns `None` if the key
    /// is absent or tombstoned.
    ///
    /// Used by the query fast path to check the persistent cache.
    pub fn get_entry(&self, key_hash: u64) -> Option<CacheEntryData> {
        let bytes = self.silo.get_with_ops(key_hash)?;
        match CacheEntryData::decode(&bytes) {
            Ok(entry) => Some(entry),
            Err(e) => {
                eprintln!("CacheSilo: decode error for key {key_hash}: {e} (skipping)");
                None
            }
        }
    }

    /// Load all persisted entries. Called on startup before the engine accepts queries.
    ///
    /// Iterates the ops log (LIFO — last write wins) and falls back to the data
    /// file for entries that were compacted. Skips tombstoned (deleted) keys.
    pub fn load_all(&self) -> io::Result<Vec<(u64, CacheEntryData)>> {
        use datasilo::SiloOp;
        use std::collections::HashMap;

        // Collect last op per key from the ops log (last-write-wins, like DataSilo compaction).
        let mut latest: HashMap<u64, Option<Vec<u8>>> = HashMap::new();
        let log = self.silo.ops_log().lock();
        let _ = log.for_each_ops(|op| {
            match op {
                SiloOp::Put { key, value } => {
                    latest.insert(key, Some(value));
                }
                SiloOp::Delete { key } => {
                    latest.insert(key, None); // tombstone
                }
            }
        });
        drop(log);

        let mut results = Vec::new();

        // Entries with ops overlay
        for (key, maybe_val) in &latest {
            if let Some(bytes) = maybe_val {
                match CacheEntryData::decode(bytes) {
                    Ok(entry) => results.push((*key, entry)),
                    Err(e) => {
                        eprintln!("CacheSilo: decode error for key {key}: {e} (skipping)");
                    }
                }
            }
            // None = tombstoned; skip.
        }

        // Entries only in the data file (compacted, no ops overlay).
        // Iterate the hash index directly instead of probing 0..N.
        for key in self.silo.iter_index_keys() {
            if latest.contains_key(&key) {
                continue; // ops overlay already processed this key
            }
            if let Some(bytes) = self.silo.get(key) {
                match CacheEntryData::decode(bytes) {
                    Ok(entry) => results.push((key, entry)),
                    Err(e) => {
                        eprintln!("CacheSilo: decode error for key {key} (data file): {e} (skipping)");
                    }
                }
            }
        }

        Ok(results)
    }

    /// Compact the silo: merge the ops log into the data file.
    /// Returns the number of entries written.
    pub fn compact(&mut self) -> io::Result<u64> {
        self.silo.compact()
    }

    /// The directory path for this silo.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Ops log size in bytes (uncompacted writes).
    pub fn ops_size(&self) -> u64 {
        self.silo.ops_size()
    }

    /// Data file size in bytes.
    pub fn data_bytes(&self) -> u64 {
        self.silo.data_bytes()
    }

    /// Whether compaction is recommended based on dead space.
    pub fn needs_compaction(&self) -> bool {
        self.silo.needs_compaction()
    }

    /// Whether the silo has any pending ops.
    pub fn has_ops(&self) -> bool {
        self.silo.has_ops()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_u32_le(cur: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_le(cur: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    cur.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;
    use crate::silos::cache::CanonicalClause;
    use crate::query::SortDirection;
    use tempfile::TempDir;

    fn make_entry(direction: SortDirection, with_keys: bool) -> CacheEntryData {
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(42);
        bm.insert(1000);

        let sorted_keys = if with_keys {
            Some(vec![
                (99u64 << 32) | 42,
                (50u64 << 32) | 1000,
                (10u64 << 32) | 1,
            ])
        } else {
            None
        };

        CacheEntryData {
            key: UnifiedKey {
                filter_clauses: vec![CanonicalClause {
                    field: "nsfwLevel".to_string(),
                    op: "eq".to_string(),
                    value_repr: "1".to_string(),
                }],
                sort_field: "sortAt".to_string(),
                direction,
            },
            bitmap: bm,
            min_tracked_value: 10,
            capacity: 4000,
            max_capacity: 64000,
            has_more: true,
            total_matched: 123_456,
            direction,
            sorted_keys,
            epoch: 0,
            field_epochs: Vec::new(),
        }
    }

    fn make_key(field: &str, direction: SortDirection) -> UnifiedKey {
        UnifiedKey {
            filter_clauses: vec![
                CanonicalClause {
                    field: "nsfw".to_string(),
                    op: "eq".to_string(),
                    value_repr: "false".to_string(),
                },
            ],
            sort_field: field.to_string(),
            direction,
        }
    }

    // ── roundtrip encode / decode ─────────────────────────────────────────

    #[test]
    fn encode_decode_roundtrip_with_sorted_keys() {
        let entry = make_entry(SortDirection::Desc, true);
        let bytes = entry.encode();
        let restored = CacheEntryData::decode(&bytes).expect("decode should succeed");

        assert_eq!(restored.direction, SortDirection::Desc);
        assert_eq!(restored.min_tracked_value, 10);
        assert_eq!(restored.capacity, 4000);
        assert_eq!(restored.max_capacity, 64000);
        assert!(restored.has_more);
        assert_eq!(restored.total_matched, 123_456);
        assert_eq!(restored.bitmap, entry.bitmap);
        assert_eq!(restored.sorted_keys, entry.sorted_keys);
    }

    #[test]
    fn encode_decode_roundtrip_no_sorted_keys() {
        let entry = make_entry(SortDirection::Asc, false);
        let bytes = entry.encode();
        let restored = CacheEntryData::decode(&bytes).expect("decode should succeed");

        assert_eq!(restored.direction, SortDirection::Asc);
        assert_eq!(restored.sorted_keys, None);
        assert_eq!(restored.bitmap, entry.bitmap);
    }

    #[test]
    fn decode_rejects_bad_version() {
        let entry = make_entry(SortDirection::Asc, false);
        let mut bytes = entry.encode();
        bytes[0] = 99; // corrupt version byte
        assert!(CacheEntryData::decode(&bytes).is_err());
    }

    // ── save + load roundtrip through CacheSilo ───────────────────────────

    #[test]
    fn save_and_load_roundtrip() {
        let dir = TempDir::new().expect("tempdir");
        let silo_path = dir.path().join("cache_silo");

        let entry = make_entry(SortDirection::Desc, true);
        let key = make_key("sortAt", SortDirection::Desc);
        let key_hash = hash_unified_key(&key);

        {
            let silo = CacheSilo::open(&silo_path).expect("open silo");
            silo.save_entry(key_hash, &entry).expect("save_entry");
        }

        // Reopen to simulate restart
        let silo = CacheSilo::open(&silo_path).expect("reopen silo");
        let loaded = silo.load_all().expect("load_all");

        assert_eq!(loaded.len(), 1, "should have exactly one entry");
        let (restored_key_hash, restored_entry) = &loaded[0];
        assert_eq!(*restored_key_hash, key_hash);
        assert_eq!(restored_entry.bitmap, entry.bitmap);
        assert_eq!(restored_entry.min_tracked_value, entry.min_tracked_value);
        assert_eq!(restored_entry.total_matched, entry.total_matched);
        assert_eq!(restored_entry.direction, entry.direction);
        assert_eq!(restored_entry.sorted_keys, entry.sorted_keys);
    }

    // ── delete_entry removes from persisted store ─────────────────────────

    #[test]
    fn delete_entry_removes_from_load() {
        let dir = TempDir::new().expect("tempdir");
        let silo_path = dir.path().join("cache_silo");

        let entry = make_entry(SortDirection::Asc, false);
        let key = make_key("likeCount", SortDirection::Asc);
        let key_hash = hash_unified_key(&key);

        {
            let silo = CacheSilo::open(&silo_path).expect("open silo");
            silo.save_entry(key_hash, &entry).expect("save_entry");
            silo.delete_entry(key_hash).expect("delete_entry");
        }

        // Reopen — tombstone should suppress the entry
        let silo = CacheSilo::open(&silo_path).expect("reopen silo");
        let loaded = silo.load_all().expect("load_all");
        assert!(loaded.is_empty(), "deleted entry must not appear in load_all");
    }

    // ── compact removes dead space ─────────────────────────────────────────

    #[test]
    fn compact_reduces_ops_size() {
        let dir = TempDir::new().expect("tempdir");
        let silo_path = dir.path().join("cache_silo");

        let entry = make_entry(SortDirection::Desc, false);
        let key = make_key("sortAt", SortDirection::Desc);
        let key_hash = hash_unified_key(&key);

        let mut silo = CacheSilo::open(&silo_path).expect("open silo");
        silo.save_entry(key_hash, &entry).expect("save_entry");
        let ops_before = silo.ops_size();
        assert!(ops_before > 0, "ops log should be non-empty before compaction");

        silo.compact().expect("compact");
        let ops_after = silo.ops_size();
        assert_eq!(ops_after, 0, "ops log should be empty after compaction");
    }

    // ── get_entry — single-key read path ─────────────────────────────────

    #[test]
    fn get_entry_returns_saved_entry() {
        let dir = TempDir::new().expect("tempdir");
        let silo_path = dir.path().join("cache_silo");

        let entry = make_entry(SortDirection::Desc, true);
        let key = make_key("sortAt", SortDirection::Desc);
        let key_hash = hash_unified_key(&key);

        let silo = CacheSilo::open(&silo_path).expect("open silo");
        silo.save_entry(key_hash, &entry).expect("save_entry");

        let got = silo.get_entry(key_hash).expect("get_entry should find saved entry");
        assert_eq!(got.bitmap, entry.bitmap);
        assert_eq!(got.min_tracked_value, entry.min_tracked_value);
        assert_eq!(got.total_matched, entry.total_matched);
        assert_eq!(got.direction, entry.direction);
        assert_eq!(got.sorted_keys, entry.sorted_keys);
    }

    #[test]
    fn get_entry_returns_none_for_unknown_key() {
        let dir = TempDir::new().expect("tempdir");
        let silo_path = dir.path().join("cache_silo");

        let silo = CacheSilo::open(&silo_path).expect("open silo");
        assert!(silo.get_entry(99999).is_none(), "unknown key should return None");
    }

    #[test]
    fn get_entry_returns_none_after_delete() {
        let dir = TempDir::new().expect("tempdir");
        let silo_path = dir.path().join("cache_silo");

        let entry = make_entry(SortDirection::Asc, false);
        let key = make_key("likeCount", SortDirection::Asc);
        let key_hash = hash_unified_key(&key);

        let silo = CacheSilo::open(&silo_path).expect("open silo");
        silo.save_entry(key_hash, &entry).expect("save_entry");
        silo.delete_entry(key_hash).expect("delete_entry");

        assert!(silo.get_entry(key_hash).is_none(), "deleted entry should return None");
    }

    #[test]
    fn get_entry_sees_update_after_save() {
        let dir = TempDir::new().expect("tempdir");
        let silo_path = dir.path().join("cache_silo");

        let mut entry_v1 = make_entry(SortDirection::Desc, false);
        entry_v1.total_matched = 111;
        let mut entry_v2 = make_entry(SortDirection::Desc, false);
        entry_v2.total_matched = 222;

        let key = make_key("sortAt", SortDirection::Desc);
        let key_hash = hash_unified_key(&key);

        let silo = CacheSilo::open(&silo_path).expect("open silo");
        silo.save_entry(key_hash, &entry_v1).expect("save v1");
        silo.save_entry(key_hash, &entry_v2).expect("save v2 (overwrite)");

        // get_entry uses get_with_ops which returns the last write
        let got = silo.get_entry(key_hash).expect("get_entry should return v2");
        assert_eq!(got.total_matched, 222, "should see the latest value");
    }

    // ── hash_unified_key is stable ─────────────────────────────────────────

    #[test]
    fn hash_is_deterministic_within_run() {
        let key = make_key("sortAt", SortDirection::Desc);
        let h1 = hash_unified_key(&key);
        let h2 = hash_unified_key(&key);
        assert_eq!(h1, h2, "hash must be deterministic");
    }

    #[test]
    fn different_keys_produce_different_hashes() {
        let k1 = make_key("sortAt", SortDirection::Desc);
        let k2 = make_key("likeCount", SortDirection::Asc);
        // Not guaranteed by hash theory, but holds for these distinct keys.
        assert_ne!(hash_unified_key(&k1), hash_unified_key(&k2));
    }
}
