//! FieldRegistry — persistent mapping of field names to stable u16 IDs.
//!
//! Small binary file (~40 entries) loaded once at startup. Field IDs start at 1
//! (ID 0 is reserved for system keys in the BitmapSilo key encoding). Deleted
//! fields are tombstoned — their ID is never reused.
//!
//! ## Binary format
//!
//! ```text
//! [4 bytes] magic: b"FREG"
//! [2 bytes] version: u16 LE (currently 1)
//! [2 bytes] entry_count: u16 LE
//! N entries:
//!   [2 bytes] field_id: u16 LE
//!   [1 byte]  is_tombstoned: u8 (0 or 1)
//!   [2 bytes] name_len: u16 LE
//!   [N bytes] name: UTF-8 bytes
//! ```

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"FREG";
const VERSION: u16 = 1;

/// A single field registry entry.
#[derive(Debug, Clone)]
struct FieldEntry {
    field_id: u16,
    name: String,
    is_tombstoned: bool,
}

/// Persistent field name → u16 ID registry.
///
/// Loaded from disk at startup. New fields are assigned the next available ID.
/// Tombstoned fields retain their ID forever (never reused).
#[derive(Debug)]
pub struct FieldRegistry {
    path: PathBuf,
    entries: Vec<FieldEntry>,
    /// Fast lookup: field_name → field_id (excludes tombstoned entries).
    name_to_id: HashMap<String, u16>,
    /// Next ID to assign.
    next_id: u16,
}

impl FieldRegistry {
    /// Open or create a field registry at the given path.
    pub fn open(path: &Path) -> io::Result<Self> {
        let registry_path = path.join("field_registry.bin");
        if registry_path.exists() {
            Self::load(&registry_path)
        } else {
            Ok(Self {
                path: registry_path,
                entries: Vec::new(),
                name_to_id: HashMap::new(),
                next_id: 1, // 0 is reserved for system keys
            })
        }
    }

    /// Load the registry from a binary file.
    fn load(path: &Path) -> io::Result<Self> {
        let data = std::fs::read(path)?;
        let mut pos = 0;

        // Magic
        if data.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file too short"));
        }
        if &data[pos..pos + 4] != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
        }
        pos += 4;

        // Version
        let version = u16::from_le_bytes([data[pos], data[pos + 1]]);
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported version {version}"),
            ));
        }
        pos += 2;

        // Entry count
        let count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Entries
        let mut entries = Vec::with_capacity(count);
        let mut name_to_id = HashMap::with_capacity(count);
        let mut max_id: u16 = 0;

        for _ in 0..count {
            if pos + 5 > data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated entry"));
            }
            let field_id = u16::from_le_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            let is_tombstoned = data[pos] != 0;
            pos += 1;
            let name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if pos + name_len > data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated name"));
            }
            let name = String::from_utf8(data[pos..pos + name_len].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            pos += name_len;

            if !is_tombstoned {
                name_to_id.insert(name.clone(), field_id);
            }
            if field_id > max_id {
                max_id = field_id;
            }
            entries.push(FieldEntry { field_id, name, is_tombstoned });
        }

        Ok(Self {
            path: path.to_path_buf(),
            entries,
            name_to_id,
            next_id: max_id + 1,
        })
    }

    /// Save the registry to disk.
    pub fn save(&self) -> io::Result<()> {
        let mut buf = Vec::with_capacity(8 + self.entries.len() * 32);

        // Header
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.entries.len() as u16).to_le_bytes());

        // Entries
        for entry in &self.entries {
            buf.extend_from_slice(&entry.field_id.to_le_bytes());
            buf.push(entry.is_tombstoned as u8);
            buf.extend_from_slice(&(entry.name.len() as u16).to_le_bytes());
            buf.extend_from_slice(entry.name.as_bytes());
        }

        // Atomic write: write to temp, rename
        let tmp = self.path.with_extension("bin.tmp");
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Look up a field ID by name. Returns None for unknown or tombstoned fields.
    #[inline]
    pub fn get(&self, name: &str) -> Option<u16> {
        self.name_to_id.get(name).copied()
    }

    /// Get or assign a field ID. Assigns the next available ID if the field is new.
    /// Saves to disk after assignment. Field IDs are capped at MAX_FIELD_ID (16383)
    /// to fit within the 14-bit namespace constraint of the key encoding.
    pub fn ensure(&mut self, name: &str) -> io::Result<u16> {
        if let Some(id) = self.name_to_id.get(name) {
            return Ok(*id);
        }
        let id = self.next_id;
        if id > crate::silos::bitmap_keys::MAX_FIELD_ID {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("field ID overflow: {} exceeds MAX_FIELD_ID {}", id, crate::silos::bitmap_keys::MAX_FIELD_ID),
            ));
        }
        self.next_id = self.next_id.checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "field ID overflow"))?;
        self.entries.push(FieldEntry {
            field_id: id,
            name: name.to_string(),
            is_tombstoned: false,
        });
        self.name_to_id.insert(name.to_string(), id);
        self.save()?;
        Ok(id)
    }

    /// Ensure multiple fields exist, returning their IDs. Saves once after all assignments.
    pub fn ensure_all(&mut self, names: &[&str]) -> io::Result<Vec<u16>> {
        let mut ids = Vec::with_capacity(names.len());
        let mut changed = false;
        for &name in names {
            if let Some(id) = self.name_to_id.get(name) {
                ids.push(*id);
            } else {
                let id = self.next_id;
                self.next_id = self.next_id.checked_add(1)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "field ID overflow"))?;
                self.entries.push(FieldEntry {
                    field_id: id,
                    name: name.to_string(),
                    is_tombstoned: false,
                });
                self.name_to_id.insert(name.to_string(), id);
                ids.push(id);
                changed = true;
            }
        }
        if changed {
            self.save()?;
        }
        Ok(ids)
    }

    /// Tombstone a field. The ID is never reused.
    pub fn tombstone(&mut self, name: &str) -> io::Result<bool> {
        if let Some(&id) = self.name_to_id.get(name) {
            self.name_to_id.remove(name);
            if let Some(entry) = self.entries.iter_mut().find(|e| e.field_id == id) {
                entry.is_tombstoned = true;
            }
            self.save()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Number of active (non-tombstoned) fields.
    pub fn active_count(&self) -> usize {
        self.name_to_id.len()
    }

    /// Total entries including tombstoned.
    pub fn total_count(&self) -> usize {
        self.entries.len()
    }

    /// Iterate over all active (non-tombstoned) field entries as (name, id).
    pub fn active_fields(&self) -> impl Iterator<Item = (&str, u16)> {
        self.name_to_id.iter().map(|(name, &id)| (name.as_str(), id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn fresh_registry_starts_at_id_1() {
        let dir = TempDir::new().unwrap();
        let reg = FieldRegistry::open(dir.path()).unwrap();
        assert_eq!(reg.next_id, 1);
        assert_eq!(reg.active_count(), 0);
    }

    #[test]
    fn ensure_assigns_sequential_ids() {
        let dir = TempDir::new().unwrap();
        let mut reg = FieldRegistry::open(dir.path()).unwrap();
        assert_eq!(reg.ensure("nsfwLevel").unwrap(), 1);
        assert_eq!(reg.ensure("userId").unwrap(), 2);
        assert_eq!(reg.ensure("tagIds").unwrap(), 3);
        // Duplicate returns same ID
        assert_eq!(reg.ensure("nsfwLevel").unwrap(), 1);
        assert_eq!(reg.active_count(), 3);
    }

    #[test]
    fn save_and_reload() {
        let dir = TempDir::new().unwrap();
        {
            let mut reg = FieldRegistry::open(dir.path()).unwrap();
            reg.ensure("field_a").unwrap();
            reg.ensure("field_b").unwrap();
            reg.ensure("field_c").unwrap();
        }
        // Reload from disk
        let reg = FieldRegistry::open(dir.path()).unwrap();
        assert_eq!(reg.get("field_a"), Some(1));
        assert_eq!(reg.get("field_b"), Some(2));
        assert_eq!(reg.get("field_c"), Some(3));
        assert_eq!(reg.next_id, 4);
        assert_eq!(reg.active_count(), 3);
    }

    #[test]
    fn tombstone_hides_field() {
        let dir = TempDir::new().unwrap();
        let mut reg = FieldRegistry::open(dir.path()).unwrap();
        reg.ensure("alive_field").unwrap();
        reg.ensure("dead_field").unwrap();
        reg.ensure("another_field").unwrap();

        assert!(reg.tombstone("dead_field").unwrap());
        assert_eq!(reg.get("dead_field"), None);
        assert_eq!(reg.active_count(), 2);
        assert_eq!(reg.total_count(), 3);

        // New field gets next ID, not reusing tombstoned ID
        assert_eq!(reg.ensure("new_field").unwrap(), 4);
    }

    #[test]
    fn tombstone_survives_reload() {
        let dir = TempDir::new().unwrap();
        {
            let mut reg = FieldRegistry::open(dir.path()).unwrap();
            reg.ensure("keep").unwrap();
            reg.ensure("remove").unwrap();
            reg.tombstone("remove").unwrap();
        }
        let reg = FieldRegistry::open(dir.path()).unwrap();
        assert_eq!(reg.get("keep"), Some(1));
        assert_eq!(reg.get("remove"), None);
        assert_eq!(reg.next_id, 3); // next after tombstoned ID 2
    }

    #[test]
    fn ensure_all_batch() {
        let dir = TempDir::new().unwrap();
        let mut reg = FieldRegistry::open(dir.path()).unwrap();
        let ids = reg.ensure_all(&["a", "b", "c"]).unwrap();
        assert_eq!(ids, vec![1, 2, 3]);

        // Mix of existing and new
        let ids2 = reg.ensure_all(&["b", "d", "a"]).unwrap();
        assert_eq!(ids2, vec![2, 4, 1]);
    }

    #[test]
    fn tombstone_nonexistent_returns_false() {
        let dir = TempDir::new().unwrap();
        let mut reg = FieldRegistry::open(dir.path()).unwrap();
        assert!(!reg.tombstone("nonexistent").unwrap());
    }
}
