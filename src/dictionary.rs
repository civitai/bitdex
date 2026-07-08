//! Per-field string dictionaries for `LowCardinalityString` fields.
//!
//! Auto-assigns integer keys as new string values are encountered.
//! Thread-safe via `DashMap` for concurrent ingest, with a snapshot
//! mechanism for lock-free reads at query time.

use ahash::AHashMap as HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use dashmap::DashMap;

/// Thread-safe auto-assigning dictionary: string → integer key.
///
/// Case-insensitive lookup: the key in `map` is always lowercase.
/// The `originals` map stores the first-seen casing (lowercase → original).
pub struct FieldDictionary {
    /// lowercase_string → integer key
    map: DashMap<String, i64>,
    /// lowercase_string → original casing (first occurrence)
    originals: DashMap<String, String>,
    /// Next key to assign (monotonically increasing, starts at 1; 0 is reserved for unknown)
    next_key: AtomicI64,
    /// Set when a new entry is inserted; cleared after persist.
    dirty: AtomicBool,
}

impl FieldDictionary {
    /// Create an empty dictionary. Keys start at 1 (0 = unknown/empty).
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            originals: DashMap::new(),
            next_key: AtomicI64::new(1),
            dirty: AtomicBool::new(false),
        }
    }

    /// Create a dictionary pre-loaded with existing mappings.
    /// Used when restoring from disk.
    pub fn from_snapshot(snapshot: &DictionarySnapshot) -> Self {
        let map = DashMap::new();
        let originals = DashMap::new();
        let mut max_key: i64 = 0;
        for (lower, &key) in &snapshot.forward {
            map.insert(lower.clone(), key);
            if key > max_key {
                max_key = key;
            }
        }
        for (lower, original) in &snapshot.originals {
            originals.insert(lower.clone(), original.clone());
        }
        Self {
            map,
            originals,
            next_key: AtomicI64::new(max_key + 1),
            dirty: AtomicBool::new(false),
        }
    }

    /// Look up or auto-assign a key for a string value.
    /// Case-insensitive: "SD 1.5" and "sd 1.5" map to the same key.
    /// Stores the original casing of the first occurrence.
    pub fn get_or_insert(&self, value: &str) -> i64 {
        let lower = value.to_lowercase();

        // Fast path: already in the map
        if let Some(entry) = self.map.get(&lower) {
            return *entry;
        }

        // Slow path: insert new entry. Use entry API for atomicity.
        let mut inserted = false;
        let key = *self.map.entry(lower.clone()).or_insert_with(|| {
            inserted = true;
            self.next_key.fetch_add(1, Ordering::Relaxed)
        });
        if inserted {
            self.dirty.store(true, Ordering::Release);
        }

        // Store original casing (first writer wins via entry API)
        self.originals.entry(lower).or_insert_with(|| value.to_string());

        key
    }

    /// Look up a key without auto-assigning. Returns None if not found.
    /// Case-insensitive.
    pub fn get(&self, value: &str) -> Option<i64> {
        let lower = value.to_lowercase();
        self.map.get(&lower).map(|v| *v)
    }

    /// Returns true if new entries were added since last `clear_dirty()`.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Clear the dirty flag (call after persisting).
    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    /// Take a snapshot for serialization or for building string maps.
    pub fn snapshot(&self) -> DictionarySnapshot {
        let mut forward = HashMap::new();
        let mut originals = HashMap::new();
        for entry in self.map.iter() {
            forward.insert(entry.key().clone(), *entry.value());
        }
        for entry in self.originals.iter() {
            originals.insert(entry.key().clone(), entry.value().clone());
        }
        DictionarySnapshot { forward, originals }
    }
}

/// Serializable snapshot of a dictionary.
/// `forward`: lowercase_string → integer key
/// `originals`: lowercase_string → original casing
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DictionarySnapshot {
    pub forward: HashMap<String, i64>,
    pub originals: HashMap<String, String>,
}

impl DictionarySnapshot {
    /// Build a forward string map (lowercase → int) for query resolution.
    /// This is compatible with the existing `StringMaps` type.
    pub fn to_string_map(&self) -> HashMap<String, i64> {
        self.forward.clone()
    }

    /// Build a reverse map (int → original string) for document serving.
    pub fn to_reverse_map(&self) -> HashMap<i64, String> {
        let mut rev = HashMap::new();
        for (lower, &key) in &self.forward {
            if let Some(original) = self.originals.get(lower) {
                rev.insert(key, original.clone());
            } else {
                rev.insert(key, lower.clone());
            }
        }
        rev
    }
}

/// Save a dictionary snapshot to a JSON file — atomically and durably.
///
/// Write-tmp → fsync → rename-over. The previous bare `fs::write` could be
/// torn by a crash, and an un-fsynced write could vanish entirely — either
/// way boot would reload a stale dictionary and `from_snapshot`'s
/// `next_key = max + 1` would RE-ISSUE keys already referenced by docs and
/// filter bitmaps on disk, silently aliasing two distinct strings to one key
/// forever (FOLLOWUP.md "LCS dictionary durability hole", 2026-07-08).
pub fn save_dictionary(snapshot: &DictionarySnapshot, path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create dict dir: {e}"))?;
    }
    let json = serde_json::to_string_pretty(snapshot)
        .map_err(|e| format!("serialize dict: {e}"))?;
    let tmp = path.with_extension("dict.tmp");
    {
        let mut f = std::fs::File::create(&tmp).map_err(|e| format!("create dict tmp: {e}"))?;
        use std::io::Write as _;
        f.write_all(json.as_bytes())
            .map_err(|e| format!("write dict tmp: {e}"))?;
        f.sync_data().map_err(|e| format!("fsync dict tmp: {e}"))?;
    }
    // Windows can't rename over an existing file; prod (Linux) rename is
    // atomic either way. remove+rename leaves a torn window only on Windows
    // dev boxes, never in prod.
    #[cfg(windows)]
    {
        let _ = std::fs::remove_file(path);
    }
    std::fs::rename(&tmp, path).map_err(|e| format!("rename dict into place: {e}"))?;
    Ok(())
}

/// Load a dictionary snapshot from a JSON file. Returns None if file doesn't exist.
pub fn load_dictionary(path: &Path) -> Result<Option<DictionarySnapshot>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let json = std::fs::read_to_string(path).map_err(|e| format!("read dict: {e}"))?;
    let snapshot: DictionarySnapshot =
        serde_json::from_str(&json).map_err(|e| format!("parse dict: {e}"))?;
    Ok(Some(snapshot))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_assignment() {
        let dict = FieldDictionary::new();
        let k1 = dict.get_or_insert("SD 1.5");
        let k2 = dict.get_or_insert("SDXL 1.0");
        let k3 = dict.get_or_insert("SD 1.5"); // same as k1

        assert_eq!(k1, k3, "same string should return same key");
        assert_ne!(k1, k2, "different strings should return different keys");
        assert!(k1 >= 1, "keys should start at 1");
        assert!(k2 >= 1);
    }

    #[test]
    fn test_case_insensitive_lookup() {
        let dict = FieldDictionary::new();
        let k1 = dict.get_or_insert("SD 1.5");
        let k2 = dict.get_or_insert("sd 1.5");
        let k3 = dict.get_or_insert("Sd 1.5");

        assert_eq!(k1, k2);
        assert_eq!(k2, k3);
    }

    #[test]
    fn test_original_casing_preserved() {
        let dict = FieldDictionary::new();
        dict.get_or_insert("SD 1.5"); // first occurrence
        dict.get_or_insert("sd 1.5"); // should not overwrite original

        let snap = dict.snapshot();
        assert_eq!(snap.originals.get("sd 1.5"), Some(&"SD 1.5".to_string()));
    }

    #[test]
    fn test_get_without_insert() {
        let dict = FieldDictionary::new();
        assert_eq!(dict.get("SD 1.5"), None);

        dict.get_or_insert("SD 1.5");
        assert!(dict.get("sd 1.5").is_some());
        assert!(dict.get("SD 1.5").is_some());
        assert_eq!(dict.get("SDXL"), None);
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let dict = FieldDictionary::new();
        dict.get_or_insert("Alpha");
        dict.get_or_insert("Beta");
        dict.get_or_insert("Gamma");

        let snap = dict.snapshot();
        let dict2 = FieldDictionary::from_snapshot(&snap);

        assert_eq!(dict2.get("alpha"), dict.get("alpha"));
        assert_eq!(dict2.get("beta"), dict.get("beta"));
        assert_eq!(dict2.get("gamma"), dict.get("gamma"));

        // New inserts should continue from where we left off
        let k_new = dict2.get_or_insert("Delta");
        assert!(k_new > dict.get("gamma").unwrap());
    }

    #[test]
    fn test_reverse_map() {
        let dict = FieldDictionary::new();
        let k1 = dict.get_or_insert("SD 1.5");
        let k2 = dict.get_or_insert("SDXL 1.0");

        let snap = dict.snapshot();
        let rev = snap.to_reverse_map();

        assert_eq!(rev.get(&k1), Some(&"SD 1.5".to_string()));
        assert_eq!(rev.get(&k2), Some(&"SDXL 1.0".to_string()));
    }

    #[test]
    fn test_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dict");

        let dict = FieldDictionary::new();
        dict.get_or_insert("Hello");
        dict.get_or_insert("World");

        let snap = dict.snapshot();
        save_dictionary(&snap, &path).unwrap();

        let loaded = load_dictionary(&path).unwrap().unwrap();
        let dict2 = FieldDictionary::from_snapshot(&loaded);

        assert_eq!(dict2.get("hello"), dict.get("hello"));
        assert_eq!(dict2.get("world"), dict.get("world"));
        assert_eq!(loaded.originals.get("hello"), Some(&"Hello".to_string()));
    }

    /// save_dictionary must atomically replace an existing file (tmp + fsync +
    /// rename) and leave no tmp litter — the old bare fs::write could tear or
    /// vanish on crash, and boot's next_key = max+1 then re-issues on-disk-
    /// referenced keys to different strings (FOLLOWUP.md dictionary hole).
    #[test]
    fn test_save_dictionary_atomic_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("field.dict");

        let dict = FieldDictionary::new();
        dict.get_or_insert("First");
        save_dictionary(&dict.snapshot(), &path).unwrap();

        // Overwrite with a grown snapshot (Windows rename-over path included).
        dict.get_or_insert("Second");
        dict.get_or_insert("Third");
        save_dictionary(&dict.snapshot(), &path).unwrap();

        let loaded = load_dictionary(&path).unwrap().unwrap();
        assert_eq!(loaded.forward.len(), 3);
        assert!(
            !path.with_extension("dict.tmp").exists(),
            "tmp file must not survive a successful save"
        );

        // Reopen: next key must continue past everything persisted.
        let reopened = FieldDictionary::from_snapshot(&loaded);
        let k_new = reopened.get_or_insert("Fourth");
        let max_persisted = loaded.forward.values().copied().max().unwrap();
        assert!(k_new > max_persisted, "no key re-issue after reload");
    }

    #[test]
    fn test_load_nonexistent_returns_none() {
        let path = std::path::PathBuf::from("/nonexistent/dict.json");
        let result = load_dictionary(&path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_dirty_flag() {
        let dict = FieldDictionary::new();
        assert!(!dict.is_dirty(), "new dict should not be dirty");

        dict.get_or_insert("Alpha");
        assert!(dict.is_dirty(), "should be dirty after new insert");

        dict.clear_dirty();
        assert!(!dict.is_dirty(), "should be clean after clear");

        // Re-inserting existing value should NOT set dirty
        dict.get_or_insert("alpha"); // same key (case-insensitive)
        assert!(!dict.is_dirty(), "duplicate insert should not set dirty");

        // New value should set dirty again
        dict.get_or_insert("Beta");
        assert!(dict.is_dirty(), "new value should set dirty");
    }

    #[test]
    fn test_dirty_flag_from_snapshot() {
        let dict = FieldDictionary::new();
        dict.get_or_insert("Hello");
        let snap = dict.snapshot();

        let dict2 = FieldDictionary::from_snapshot(&snap);
        assert!(!dict2.is_dirty(), "restored dict should not be dirty");

        dict2.get_or_insert("hello"); // existing
        assert!(!dict2.is_dirty(), "existing value should not dirty restored dict");

        dict2.get_or_insert("World"); // new
        assert!(dict2.is_dirty(), "new value should dirty restored dict");
    }
}
