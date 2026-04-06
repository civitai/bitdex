//! Dump pipeline — manages table dump lifecycle for initial loading.
//!
//! Server side: dump registry (track which tables have been loaded).
//! Client side: pg-sync checks dump history, runs flat COPYs, writes WAL files.
//!
//! Dump lifecycle:
//! 1. PUT /dumps — register a new dump (returns task ID, WAL reader starts polling)
//! 2. pg-sync writes ops to WAL file on shared filesystem
//! 3. POST /dumps/{name}/loaded — signal file is complete
//! 4. WAL reader finishes processing, marks dump as complete
//! 5. GET /dumps — check status per table

use ahash::AHashMap as HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// State of a single dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpEntry {
    /// Dump name (e.g., "Image-a1b2c3d4")
    pub name: String,
    /// WAL file path (relative to data_dir)
    pub wal_path: Option<String>,
    /// Current status
    pub status: DumpStatus,
    /// Number of ops written (reported by pg-sync)
    pub ops_written: u64,
    /// Number of ops processed by WAL reader
    pub ops_processed: u64,
    /// Task ID for live progress tracking via GET /tasks/{id}.
    /// V2 dumps set this so the sidecar can poll records_processed during loading.
    #[serde(default)]
    pub task_id: Option<u64>,
    /// When the dump was registered
    pub created_at: u64,
    /// When the dump completed processing
    pub completed_at: Option<u64>,
}

/// Dump status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DumpStatus {
    /// pg-sync is writing to the WAL file
    Writing,
    /// pg-sync signaled the file is complete, WAL reader is processing
    Loading,
    /// WAL reader finished processing
    Complete,
    /// Dump failed
    Failed(String),
}

/// Registry of dump state. Persisted to dumps.json in the data directory.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DumpRegistry {
    pub dumps: HashMap<String, DumpEntry>,
}

impl DumpRegistry {
    /// Load from a JSON file. Returns empty registry if file doesn't exist.
    pub fn load(path: &Path) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    /// Save to a JSON file.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Atomic write via temp file
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Register a new dump. Returns the entry.
    pub fn register(&mut self, name: String, wal_path: Option<String>) -> &DumpEntry {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.dumps.insert(
            name.clone(),
            DumpEntry {
                name: name.clone(),
                wal_path,
                status: DumpStatus::Writing,
                ops_written: 0,
                ops_processed: 0,
                task_id: None,
                created_at: now,
                completed_at: None,
            },
        );
        &self.dumps[&name]
    }

    /// Mark a dump as loaded (pg-sync finished writing the WAL file).
    pub fn mark_loaded(&mut self, name: &str, ops_written: u64) -> Option<&DumpEntry> {
        if let Some(entry) = self.dumps.get_mut(name) {
            entry.status = DumpStatus::Loading;
            entry.ops_written = ops_written;
            Some(entry)
        } else {
            None
        }
    }

    /// Mark a dump as complete (WAL reader finished processing).
    pub fn mark_complete(&mut self, name: &str, ops_processed: u64) -> Option<&DumpEntry> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(entry) = self.dumps.get_mut(name) {
            entry.status = DumpStatus::Complete;
            entry.ops_processed = ops_processed;
            entry.completed_at = Some(now);
            Some(entry)
        } else {
            None
        }
    }

    /// Mark a dump as failed.
    pub fn mark_failed(&mut self, name: &str, error: String) {
        if let Some(entry) = self.dumps.get_mut(name) {
            entry.status = DumpStatus::Failed(error);
        }
    }

    /// Remove a dump from the registry.
    pub fn remove(&mut self, name: &str) -> Option<DumpEntry> {
        self.dumps.remove(name)
    }

    /// Clear all dumps.
    pub fn clear(&mut self) {
        self.dumps.clear();
    }

    /// Check if a dump with the given name exists and is complete.
    pub fn is_complete(&self, name: &str) -> bool {
        self.dumps
            .get(name)
            .map(|e| e.status == DumpStatus::Complete)
            .unwrap_or(false)
    }

    /// Get all dump names that are complete.
    pub fn completed_names(&self) -> Vec<&str> {
        self.dumps
            .values()
            .filter(|e| e.status == DumpStatus::Complete)
            .map(|e| e.name.as_str())
            .collect()
    }

    /// Check if all dumps are complete (no pending/writing/loading).
    pub fn all_complete(&self) -> bool {
        !self.dumps.is_empty()
            && self.dumps.values().all(|e| e.status == DumpStatus::Complete)
    }
}

/// Build the dump name from a table name and config hash.
/// Format: "{Table}-{hash8}"
pub fn dump_name(table: &str, config_hash: &str) -> String {
    format!("{}-{}", table, &config_hash[..8.min(config_hash.len())])
}

/// Compute a config hash for a sync source entry.
pub fn config_hash(yaml_fragment: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    yaml_fragment.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dump_lifecycle() {
        let mut reg = DumpRegistry::default();
        assert!(reg.dumps.is_empty());

        // Register
        reg.register("Image-a1b2c3d4".into(), Some("dumps/image.wal".into()));
        assert_eq!(reg.dumps.len(), 1);
        assert_eq!(reg.dumps["Image-a1b2c3d4"].status, DumpStatus::Writing);

        // Mark loaded
        reg.mark_loaded("Image-a1b2c3d4", 107_000_000);
        assert_eq!(reg.dumps["Image-a1b2c3d4"].status, DumpStatus::Loading);
        assert_eq!(reg.dumps["Image-a1b2c3d4"].ops_written, 107_000_000);

        // Mark complete
        reg.mark_complete("Image-a1b2c3d4", 107_000_000);
        assert_eq!(reg.dumps["Image-a1b2c3d4"].status, DumpStatus::Complete);
        assert!(reg.dumps["Image-a1b2c3d4"].completed_at.is_some());

        assert!(reg.is_complete("Image-a1b2c3d4"));
        assert!(reg.all_complete());
    }

    #[test]
    fn test_dump_persistence() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("dumps.json");

        let mut reg = DumpRegistry::default();
        reg.register("Image-abc".into(), None);
        reg.mark_complete("Image-abc", 100);
        reg.save(&path).unwrap();

        let loaded = DumpRegistry::load(&path);
        assert_eq!(loaded.dumps.len(), 1);
        assert!(loaded.is_complete("Image-abc"));
    }

    #[test]
    fn test_dump_removal() {
        let mut reg = DumpRegistry::default();
        reg.register("Image-abc".into(), None);
        reg.register("Tags-def".into(), None);
        assert_eq!(reg.dumps.len(), 2);

        reg.remove("Image-abc");
        assert_eq!(reg.dumps.len(), 1);
        assert!(!reg.dumps.contains_key("Image-abc"));
    }

    #[test]
    fn test_dump_clear() {
        let mut reg = DumpRegistry::default();
        reg.register("Image-abc".into(), None);
        reg.register("Tags-def".into(), None);
        reg.clear();
        assert!(reg.dumps.is_empty());
    }

    #[test]
    fn test_all_complete() {
        let mut reg = DumpRegistry::default();
        assert!(!reg.all_complete()); // Empty = not all complete

        reg.register("Image-abc".into(), None);
        reg.register("Tags-def".into(), None);
        assert!(!reg.all_complete());

        reg.mark_complete("Image-abc", 100);
        assert!(!reg.all_complete()); // Tags still pending

        reg.mark_loaded("Tags-def", 50);
        reg.mark_complete("Tags-def", 50);
        assert!(reg.all_complete());
    }

    #[test]
    fn test_dump_name() {
        assert_eq!(dump_name("Image", "a1b2c3d4e5f6"), "Image-a1b2c3d4");
    }

    #[test]
    fn test_config_hash_deterministic() {
        let h1 = config_hash("table: Image\nslot_field: id\ntrack_fields: [nsfwLevel]");
        let h2 = config_hash("table: Image\nslot_field: id\ntrack_fields: [nsfwLevel]");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_config_hash_changes() {
        let h1 = config_hash("table: Image\ntrack_fields: [nsfwLevel]");
        let h2 = config_hash("table: Image\ntrack_fields: [nsfwLevel, type]");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_load_missing_file() {
        let reg = DumpRegistry::load(Path::new("/nonexistent/dumps.json"));
        assert!(reg.dumps.is_empty());
    }

    #[test]
    fn test_failed_dump() {
        let mut reg = DumpRegistry::default();
        reg.register("Image-abc".into(), None);
        reg.mark_failed("Image-abc", "connection reset".into());
        assert!(matches!(reg.dumps["Image-abc"].status, DumpStatus::Failed(_)));
        assert!(!reg.is_complete("Image-abc"));
    }
}
