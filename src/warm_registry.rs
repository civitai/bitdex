//! Warm Registry — tracks popular query shapes and persists them for
//! automatic cache warming on boot.
//!
//! During normal operation, each query's filter+sort shape is recorded
//! with a frequency counter. Periodically, the merge thread persists
//! the top-N shapes to a JSON file in the data directory. On startup,
//! the server reads this file and executes warm queries for each shape,
//! pre-populating the unified cache before real traffic arrives.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cache::CanonicalClause;
use crate::query::{FilterClause, SortClause, SortDirection};

/// Maximum number of shapes to track in memory.
const MAX_TRACKED_SHAPES: usize = 10_000;
/// Maximum number of shapes to persist and warm on boot.
const MAX_WARM_SHAPES: usize = 500;

/// A query shape: the canonical filter clauses + sort field + direction.
/// Two queries with the same shape will produce the same cache key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryShape {
    pub filter_clauses: Vec<CanonicalClause>,
    pub sort_field: String,
    pub direction: SortDirection,
}

/// Persisted warm entry: a query shape with its original filter clauses
/// (needed to re-execute the query on boot) and frequency count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmEntry {
    pub filters: Vec<FilterClause>,
    pub sort_field: String,
    pub direction: SortDirection,
    pub frequency: u64,
}

/// Thread-safe shape frequency tracker.
pub struct WarmRegistry {
    /// Shape → (frequency counter, original filter clauses).
    /// Original clauses are stored so we can re-execute the query on boot.
    shapes: parking_lot::Mutex<HashMap<QueryShape, ShapeRecord>>,
    /// Path to the warm file (e.g. data/indexes/civitai/warm.json).
    persist_path: Option<PathBuf>,
    /// Total queries recorded since boot.
    total_recorded: AtomicU64,
}

struct ShapeRecord {
    frequency: u64,
    /// Original filter clauses (pre-canonicalization) for query replay.
    filters: Vec<FilterClause>,
}

impl WarmRegistry {
    pub fn new(persist_path: Option<PathBuf>) -> Self {
        Self {
            shapes: parking_lot::Mutex::new(HashMap::new()),
            persist_path,
            total_recorded: AtomicU64::new(0),
        }
    }

    /// Record a query shape. Called from the query handler on every query.
    /// Fast path: single lock acquisition, bounded map.
    pub fn record(
        &self,
        filter_clauses: &[FilterClause],
        canonical: &[CanonicalClause],
        sort_field: &str,
        direction: SortDirection,
    ) {
        self.total_recorded.fetch_add(1, Ordering::Relaxed);

        let shape = QueryShape {
            filter_clauses: canonical.to_vec(),
            sort_field: sort_field.to_string(),
            direction,
        };

        let mut map = self.shapes.lock();

        if let Some(record) = map.get_mut(&shape) {
            record.frequency += 1;
            return;
        }

        // New shape — check capacity
        if map.len() >= MAX_TRACKED_SHAPES {
            // Evict the least frequent shape
            if let Some(min_key) = map
                .iter()
                .min_by_key(|(_, r)| r.frequency)
                .map(|(k, _)| k.clone())
            {
                map.remove(&min_key);
            }
        }

        map.insert(shape, ShapeRecord {
            frequency: 1,
            filters: filter_clauses.to_vec(),
        });
    }

    /// Get the top-N shapes by frequency for warming.
    pub fn top_shapes(&self, n: usize) -> Vec<WarmEntry> {
        let map = self.shapes.lock();
        let mut entries: Vec<_> = map
            .iter()
            .map(|(shape, record)| WarmEntry {
                filters: record.filters.clone(),
                sort_field: shape.sort_field.clone(),
                direction: shape.direction,
                frequency: record.frequency,
            })
            .collect();

        entries.sort_by(|a, b| b.frequency.cmp(&a.frequency));
        entries.truncate(n);
        entries
    }

    /// Total queries recorded since boot.
    pub fn total_recorded(&self) -> u64 {
        self.total_recorded.load(Ordering::Relaxed)
    }

    /// Number of unique shapes tracked.
    pub fn shape_count(&self) -> usize {
        self.shapes.lock().len()
    }

    /// Persist top-N shapes to disk. Called periodically by the merge thread.
    pub fn persist(&self) -> std::io::Result<usize> {
        let path = match &self.persist_path {
            Some(p) => p,
            None => return Ok(0),
        };

        let entries = self.top_shapes(MAX_WARM_SHAPES);
        let count = entries.len();
        if count == 0 {
            return Ok(0);
        }

        let json = serde_json::to_string_pretty(&entries)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Write to temp file then rename for atomic persistence.
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)?;

        Ok(count)
    }

    /// Load persisted warm entries from disk. Called on boot.
    pub fn load(path: &Path) -> Vec<WarmEntry> {
        match std::fs::read_to_string(path) {
            Ok(json) => {
                match serde_json::from_str::<Vec<WarmEntry>>(&json) {
                    Ok(entries) => {
                        eprintln!(
                            "Loaded {} warm entries from {}",
                            entries.len(),
                            path.display()
                        );
                        entries
                    }
                    Err(e) => {
                        eprintln!("Failed to parse warm file {}: {e}", path.display());
                        Vec::new()
                    }
                }
            }
            Err(_) => Vec::new(), // No warm file yet — first boot
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{FilterClause, Value};

    fn eq(field: &str, val: i64) -> FilterClause {
        FilterClause::Eq(field.into(), Value::Integer(val))
    }

    fn canonical(clauses: &[FilterClause]) -> Vec<CanonicalClause> {
        clauses.iter().filter_map(CanonicalClause::from_filter).collect()
    }

    #[test]
    fn record_increments_frequency() {
        let reg = WarmRegistry::new(None);
        let filters = vec![eq("nsfwLevel", 1)];
        let canon = canonical(&filters);

        reg.record(&filters, &canon, "reactionCount", SortDirection::Desc);
        reg.record(&filters, &canon, "reactionCount", SortDirection::Desc);
        reg.record(&filters, &canon, "reactionCount", SortDirection::Desc);

        let top = reg.top_shapes(10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].frequency, 3);
    }

    #[test]
    fn different_shapes_tracked_separately() {
        let reg = WarmRegistry::new(None);
        let f1 = vec![eq("nsfwLevel", 1)];
        let f2 = vec![eq("nsfwLevel", 2)];
        let c1 = canonical(&f1);
        let c2 = canonical(&f2);

        reg.record(&f1, &c1, "reactionCount", SortDirection::Desc);
        reg.record(&f2, &c2, "reactionCount", SortDirection::Desc);

        assert_eq!(reg.shape_count(), 2);
    }

    #[test]
    fn top_shapes_sorted_by_frequency() {
        let reg = WarmRegistry::new(None);
        let f1 = vec![eq("nsfwLevel", 1)];
        let f2 = vec![eq("nsfwLevel", 2)];
        let c1 = canonical(&f1);
        let c2 = canonical(&f2);

        // f1 gets 5 hits, f2 gets 2
        for _ in 0..5 { reg.record(&f1, &c1, "sortAt", SortDirection::Desc); }
        for _ in 0..2 { reg.record(&f2, &c2, "sortAt", SortDirection::Desc); }

        let top = reg.top_shapes(10);
        assert_eq!(top[0].frequency, 5);
        assert_eq!(top[1].frequency, 2);
    }

    #[test]
    fn persist_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("warm.json");
        let reg = WarmRegistry::new(Some(path.clone()));

        let filters = vec![eq("nsfwLevel", 1)];
        let canon = canonical(&filters);
        reg.record(&filters, &canon, "reactionCount", SortDirection::Desc);

        let persisted = reg.persist().unwrap();
        assert_eq!(persisted, 1);

        let loaded = WarmRegistry::load(&path);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].sort_field, "reactionCount");
    }
}
