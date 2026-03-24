//! In-memory document cache for DocStore.
//!
//! DashMap-based read cache with write-through semantics. Sits alongside
//! the DocStore — ConcurrentEngine checks the cache before disk reads
//! and populates it on cache misses.
//!
//! ## Design
//!
//! - **Cache-on-read**: First `get()` populates cache, subsequent reads hit memory
//! - **Write-through**: `put()` updates cache directly (avoids stale reads)
//! - **LRU eviction**: Approximate LRU via last-accessed timestamps, evicted by
//!   a sweep when total bytes exceeds `max_bytes`
//! - **No locking**: DashMap provides lock-free concurrent reads/writes per shard

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::docstore::StoredDoc;

/// Configuration for the document cache.
#[derive(Debug, Clone)]
pub struct DocCacheConfig {
    /// Maximum cache size in bytes. Eviction sweeps when exceeded.
    pub max_bytes: u64,
}

impl Default for DocCacheConfig {
    fn default() -> Self {
        DocCacheConfig {
            max_bytes: 1_073_741_824, // 1 GB
        }
    }
}

/// A cached document entry with approximate size and access tracking.
struct CachedEntry {
    doc: StoredDoc,
    /// Approximate size in bytes (fields + overhead).
    size_bytes: u64,
    /// Wall-clock millis when last accessed (for LRU eviction).
    last_accessed_ms: AtomicU64,
}

/// In-memory document cache backed by DashMap.
pub struct DocCache {
    entries: DashMap<u32, CachedEntry>,
    /// Current total size in bytes (approximate).
    total_bytes: AtomicU64,
    config: DocCacheConfig,
    /// Cumulative cache hits.
    hits: AtomicU64,
    /// Cumulative cache misses.
    misses: AtomicU64,
    /// Cumulative evictions.
    evictions: AtomicU64,
}

impl DocCache {
    /// Create a new document cache.
    pub fn new(config: DocCacheConfig) -> Self {
        DocCache {
            entries: DashMap::new(),
            total_bytes: AtomicU64::new(0),
            config,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Look up a document in the cache. Returns None on miss.
    pub fn get(&self, slot_id: u32) -> Option<StoredDoc> {
        match self.entries.get(&slot_id) {
            Some(entry) => {
                entry.last_accessed_ms.store(now_ms(), Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.doc.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert a document into the cache. If the cache exceeds max_bytes,
    /// a background eviction sweep should be triggered (caller's responsibility).
    pub fn insert(&self, slot_id: u32, doc: StoredDoc) {
        let size = estimate_doc_size(&doc);

        // Update or insert
        if let Some(mut existing) = self.entries.get_mut(&slot_id) {
            // Update in place — subtract old size, add new
            let old_size = existing.size_bytes;
            existing.doc = doc;
            existing.size_bytes = size;
            existing.last_accessed_ms.store(now_ms(), Ordering::Relaxed);
            // Adjust total (may underflow briefly, but AtomicU64 wraps safely)
            if size > old_size {
                self.total_bytes.fetch_add(size - old_size, Ordering::Relaxed);
            } else {
                self.total_bytes.fetch_sub(old_size - size, Ordering::Relaxed);
            }
        } else {
            self.entries.insert(slot_id, CachedEntry {
                doc,
                size_bytes: size,
                last_accessed_ms: AtomicU64::new(now_ms()),
            });
            self.total_bytes.fetch_add(size, Ordering::Relaxed);
        }
    }

    /// Insert a batch of documents into the cache.
    pub fn insert_batch(&self, docs: &[(u32, StoredDoc)]) {
        for (slot_id, doc) in docs {
            self.insert(*slot_id, doc.clone());
        }
    }

    /// Update documents that are already in the cache; skip new ones.
    ///
    /// Used by the flush thread for write-through: only update docs that
    /// queries have already loaded (cache-on-read). New docs from pg-sync
    /// mutations go straight to disk without filling the cache with cold
    /// entries that may never be queried.
    pub fn update_batch_if_cached(&self, docs: &[(u32, StoredDoc)]) {
        for (slot_id, doc) in docs {
            if let Some(mut existing) = self.entries.get_mut(slot_id) {
                let new_size = estimate_doc_size(doc);
                let old_size = existing.size_bytes;
                existing.doc = doc.clone();
                existing.size_bytes = new_size;
                existing.last_accessed_ms.store(now_ms(), Ordering::Relaxed);
                if new_size > old_size {
                    self.total_bytes.fetch_add(new_size - old_size, Ordering::Relaxed);
                } else {
                    self.total_bytes.fetch_sub(old_size - new_size, Ordering::Relaxed);
                }
            }
            // Not in cache — skip. Doc goes to disk only.
        }
    }

    /// Remove a document from the cache (on delete).
    pub fn remove(&self, slot_id: u32) {
        if let Some((_, entry)) = self.entries.remove(&slot_id) {
            self.total_bytes.fetch_sub(entry.size_bytes, Ordering::Relaxed);
        }
    }

    /// Check if eviction is needed.
    pub fn needs_eviction(&self) -> bool {
        self.total_bytes.load(Ordering::Relaxed) > self.config.max_bytes
    }

    /// Run an LRU eviction sweep. Removes the oldest entries until total_bytes
    /// is below 80% of max_bytes (hysteresis to avoid thrashing).
    pub fn evict(&self) -> u64 {
        let target = (self.config.max_bytes as f64 * 0.8) as u64;
        let mut current = self.total_bytes.load(Ordering::Relaxed);
        if current <= self.config.max_bytes {
            return 0;
        }

        // Collect (slot_id, last_accessed_ms) for sorting
        let mut candidates: Vec<(u32, u64, u64)> = self.entries.iter()
            .map(|entry| (
                *entry.key(),
                entry.last_accessed_ms.load(Ordering::Relaxed),
                entry.size_bytes,
            ))
            .collect();

        // Sort by last_accessed ascending (oldest first)
        candidates.sort_by_key(|&(_, ts, _)| ts);

        let mut evicted = 0u64;
        for (slot_id, _, size) in candidates {
            if current <= target {
                break;
            }
            if self.entries.remove(&slot_id).is_some() {
                current = self.total_bytes.fetch_sub(size, Ordering::Relaxed) - size;
                evicted += 1;
            }
        }

        self.evictions.fetch_add(evicted, Ordering::Relaxed);
        evicted
    }

    /// Current cache size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Number of entries in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Cache hit count.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cache miss count.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Cache eviction count.
    pub fn eviction_count(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        self.entries.clear();
        self.total_bytes.store(0, Ordering::Relaxed);
    }
}

/// Estimate the in-memory size of a StoredDoc.
fn estimate_doc_size(doc: &StoredDoc) -> u64 {
    // Base overhead: HashMap + schema_version
    let mut size: u64 = 128; // HashMap overhead estimate

    for (key, value) in &doc.fields {
        // Key: String (24 bytes + data)
        size += 24 + key.len() as u64;
        // Value: FieldValue (varies)
        size += estimate_field_value_size(value);
    }

    size
}

/// Estimate the in-memory size of a FieldValue.
fn estimate_field_value_size(value: &crate::mutation::FieldValue) -> u64 {
    use crate::mutation::FieldValue;
    match value {
        FieldValue::Single(v) => 8 + estimate_value_size(v),
        FieldValue::Multi(values) => {
            24 + values.iter().map(|v| estimate_value_size(v)).sum::<u64>()
        }
    }
}

/// Estimate the in-memory size of a Value.
fn estimate_value_size(value: &crate::query::Value) -> u64 {
    use crate::query::Value;
    match value {
        Value::Integer(_) => 8,
        Value::Float(_) => 8,
        Value::Bool(_) => 1,
        Value::String(s) => 24 + s.len() as u64,
    }
}

/// Current wall-clock time in milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::FieldValue;
    use crate::query::Value;
    use std::collections::HashMap;

    fn make_doc(fields: Vec<(&str, FieldValue)>) -> StoredDoc {
        StoredDoc {
            fields: fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            schema_version: 0,
        }
    }

    #[test]
    fn test_cache_hit_miss() {
        let cache = DocCache::new(DocCacheConfig::default());

        // Miss
        assert!(cache.get(1).is_none());
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 0);

        // Insert
        let doc = make_doc(vec![("name", FieldValue::Single(Value::String("test".into())))]);
        cache.insert(1, doc.clone());

        // Hit
        let result = cache.get(1).unwrap();
        assert_eq!(result.fields["name"], doc.fields["name"]);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_cache_update() {
        let cache = DocCache::new(DocCacheConfig::default());

        let doc1 = make_doc(vec![("x", FieldValue::Single(Value::Integer(1)))]);
        cache.insert(1, doc1);
        let size1 = cache.size_bytes();

        let doc2 = make_doc(vec![
            ("x", FieldValue::Single(Value::Integer(2))),
            ("y", FieldValue::Single(Value::String("bigger".into()))),
        ]);
        cache.insert(1, doc2.clone());
        let size2 = cache.size_bytes();

        assert!(size2 > size1, "larger doc should increase cache size");
        assert_eq!(cache.len(), 1, "update should not create duplicate");

        let result = cache.get(1).unwrap();
        assert_eq!(result.fields["x"], FieldValue::Single(Value::Integer(2)));
    }

    #[test]
    fn test_cache_remove() {
        let cache = DocCache::new(DocCacheConfig::default());

        let doc = make_doc(vec![("x", FieldValue::Single(Value::Integer(1)))]);
        cache.insert(1, doc);
        assert_eq!(cache.len(), 1);
        assert!(cache.size_bytes() > 0);

        cache.remove(1);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size_bytes(), 0);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_cache_eviction() {
        // Tiny cache: 500 bytes
        let config = DocCacheConfig { max_bytes: 500 };
        let cache = DocCache::new(config);

        // Insert enough docs to exceed limit
        for i in 0..20u32 {
            let doc = make_doc(vec![
                ("id", FieldValue::Single(Value::Integer(i as i64))),
                ("data", FieldValue::Single(Value::String("x".repeat(50)))),
            ]);
            cache.insert(i, doc);
        }

        assert!(cache.needs_eviction(), "should need eviction after many inserts");

        let evicted = cache.evict();
        assert!(evicted > 0, "should evict some entries");
        assert!(!cache.needs_eviction(), "should be under limit after eviction");
    }

    #[test]
    fn test_cache_clear() {
        let cache = DocCache::new(DocCacheConfig::default());

        for i in 0..10u32 {
            cache.insert(i, make_doc(vec![("x", FieldValue::Single(Value::Integer(i as i64)))]));
        }
        assert_eq!(cache.len(), 10);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size_bytes(), 0);
    }
}
