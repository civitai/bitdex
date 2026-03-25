//! Metadata I/O for ShardStore — simple files alongside generation directories.
//!
//! These are small values that don't benefit from the generation/ops model:
//! - slot_counter: u32 (4 bytes)
//! - deferred_alive: BTreeMap<u64, Vec<u32>> (msgpack)
//! - time_buckets: named roaring bitmaps
//! - cursors: named UTF-8 strings
//!
//! All use atomic write (tmp → fsync → rename) for crash safety.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

/// Manages metadata files alongside ShardStore generation directories.
pub struct MetaStore {
    root: PathBuf,
}

impl MetaStore {
    pub fn new(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        fs::create_dir_all(root.join("meta"))?;
        fs::create_dir_all(root.join("cursors"))?;
        fs::create_dir_all(root.join("time_buckets"))?;
        Ok(MetaStore { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    // -----------------------------------------------------------------------
    // Slot counter
    // -----------------------------------------------------------------------

    pub fn write_slot_counter(&self, counter: u32) -> io::Result<()> {
        let path = self.root.join("meta").join("slot_counter.bin");
        write_atomic(&path, &counter.to_le_bytes())
    }

    pub fn load_slot_counter(&self) -> io::Result<Option<u32>> {
        let path = self.root.join("meta").join("slot_counter.bin");
        match fs::read(&path) {
            Ok(data) if data.len() >= 4 => {
                Ok(Some(u32::from_le_bytes(data[..4].try_into().unwrap())))
            }
            Ok(_) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    // -----------------------------------------------------------------------
    // Deferred alive
    // -----------------------------------------------------------------------

    pub fn write_deferred_alive(&self, deferred: &BTreeMap<u64, Vec<u32>>) -> io::Result<()> {
        let path = self.root.join("meta").join("deferred_alive.bin");
        if deferred.is_empty() {
            // Remove file if no deferred entries
            let _ = fs::remove_file(&path);
            return Ok(());
        }
        // Simple binary format: [u32 num_timestamps][per timestamp: u64 ts, u32 num_slots, u32* slots]
        let mut buf = Vec::new();
        buf.extend_from_slice(&(deferred.len() as u32).to_le_bytes());
        for (&ts, slots) in deferred {
            buf.extend_from_slice(&ts.to_le_bytes());
            buf.extend_from_slice(&(slots.len() as u32).to_le_bytes());
            for &slot in slots {
                buf.extend_from_slice(&slot.to_le_bytes());
            }
        }
        write_atomic(&path, &buf)
    }

    pub fn load_deferred_alive(&self) -> io::Result<Option<BTreeMap<u64, Vec<u32>>>> {
        let path = self.root.join("meta").join("deferred_alive.bin");
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if data.len() < 4 {
            return Ok(None);
        }
        let num_ts = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        let mut result = BTreeMap::new();
        for _ in 0..num_ts {
            if pos + 12 > data.len() { break; }
            let ts = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
            pos += 8;
            let num_slots = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
            pos += 4;
            let mut slots = Vec::with_capacity(num_slots);
            for _ in 0..num_slots {
                if pos + 4 > data.len() { break; }
                slots.push(u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()));
                pos += 4;
            }
            result.insert(ts, slots);
        }
        Ok(Some(result))
    }

    // -----------------------------------------------------------------------
    // Time buckets
    // -----------------------------------------------------------------------

    pub fn write_time_bucket(&self, name: &str, bitmap: &RoaringBitmap) -> io::Result<()> {
        let path = self.root.join("time_buckets").join(format!("{}.roar", name));
        let mut buf = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize: {e}")))?;
        write_atomic(&path, &buf)
    }

    pub fn load_time_buckets(&self) -> io::Result<Vec<(String, RoaringBitmap)>> {
        let dir = self.root.join("time_buckets");
        let mut result = Vec::new();
        if !dir.exists() {
            return Ok(result);
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(bucket_name) = name.strip_suffix(".roar") {
                let data = fs::read(entry.path())?;
                let bm = RoaringBitmap::deserialize_from(&data[..])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bitmap: {e}")))?;
                result.push((bucket_name.to_string(), bm));
            }
        }
        Ok(result)
    }

    /// Write a time bucket's last_cutoff to disk for incremental diff recovery on restart.
    pub fn write_time_bucket_cutoff(&self, name: &str, cutoff: u64) -> io::Result<()> {
        let path = self.root.join("time_buckets").join(format!("{}.cutoff", name));
        write_atomic(&path, &cutoff.to_le_bytes())
    }

    /// Load a time bucket's persisted last_cutoff. Returns 0 if not found.
    pub fn load_time_bucket_cutoff(&self, name: &str) -> io::Result<u64> {
        let path = self.root.join("time_buckets").join(format!("{}.cutoff", name));
        match fs::read(&path) {
            Ok(data) if data.len() == 8 => {
                Ok(u64::from_le_bytes(data[..8].try_into().unwrap()))
            }
            Ok(_) => Ok(0),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e),
        }
    }

    // -----------------------------------------------------------------------
    // Cursors
    // -----------------------------------------------------------------------

    pub fn write_cursor(&self, name: &str, value: &str) -> io::Result<()> {
        let path = self.root.join("cursors").join(name);
        write_atomic(&path, value.as_bytes())
    }

    pub fn load_cursor(&self, name: &str) -> io::Result<Option<String>> {
        let path = self.root.join("cursors").join(name);
        match fs::read_to_string(&path) {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn load_all_cursors(&self) -> io::Result<HashMap<String, String>> {
        let dir = self.root.join("cursors");
        let mut result = HashMap::new();
        if !dir.exists() {
            return Ok(result);
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") { continue; }
            if let Ok(value) = fs::read_to_string(entry.path()) {
                result.insert(name, value);
            }
        }
        Ok(result)
    }
}

/// Atomic write: tmp → fsync → rename.
fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(&tmp)?;
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_counter_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::new(dir.path().to_path_buf()).unwrap();

        assert_eq!(store.load_slot_counter().unwrap(), None);
        store.write_slot_counter(42).unwrap();
        assert_eq!(store.load_slot_counter().unwrap(), Some(42));
        store.write_slot_counter(100_000).unwrap();
        assert_eq!(store.load_slot_counter().unwrap(), Some(100_000));
    }

    #[test]
    fn test_deferred_alive_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::new(dir.path().to_path_buf()).unwrap();

        assert_eq!(store.load_deferred_alive().unwrap(), None);

        let mut deferred = BTreeMap::new();
        deferred.insert(1000u64, vec![1, 2, 3]);
        deferred.insert(2000u64, vec![10, 20]);
        store.write_deferred_alive(&deferred).unwrap();

        let loaded = store.load_deferred_alive().unwrap().unwrap();
        assert_eq!(loaded, deferred);
    }

    #[test]
    fn test_deferred_alive_empty_removes_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::new(dir.path().to_path_buf()).unwrap();

        let mut deferred = BTreeMap::new();
        deferred.insert(1000u64, vec![1]);
        store.write_deferred_alive(&deferred).unwrap();
        assert!(store.load_deferred_alive().unwrap().is_some());

        store.write_deferred_alive(&BTreeMap::new()).unwrap();
        assert_eq!(store.load_deferred_alive().unwrap(), None);
    }

    #[test]
    fn test_time_bucket_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::new(dir.path().to_path_buf()).unwrap();

        let mut bm = RoaringBitmap::new();
        bm.insert_range(0..100);
        store.write_time_bucket("24h", &bm).unwrap();

        let mut bm2 = RoaringBitmap::new();
        bm2.insert_range(0..1000);
        store.write_time_bucket("7d", &bm2).unwrap();

        let loaded = store.load_time_buckets().unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn test_cursor_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::new(dir.path().to_path_buf()).unwrap();

        assert_eq!(store.load_cursor("pg-sync-0").unwrap(), None);
        store.write_cursor("pg-sync-0", "12345").unwrap();
        assert_eq!(store.load_cursor("pg-sync-0").unwrap(), Some("12345".into()));

        store.write_cursor("pg-sync-1", "67890").unwrap();
        let all = store.load_all_cursors().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all["pg-sync-0"], "12345");
    }
}
