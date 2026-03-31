//! Amortized bitmap memory scanner.
//!
//! Maintains cached per-field bitmap memory totals via a background scanner
//! thread, replacing the expensive on-scrape `bitmap_memory_report()` call
//! that takes 52s at 107M records.
//!
//! The scanner processes stale fields in small batches, dropping the ArcSwap
//! guard between each field to avoid pinning old snapshot memory.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

/// Cached bitmap memory sizes, updated incrementally by a background scanner.
pub struct BitmapMemoryCache {
    /// Per-field cached sizes: field_name -> (bytes, count).
    filter_cache: DashMap<String, (AtomicU64, AtomicU64)>,
    /// Per-sort-field cached sizes: field_name -> bytes.
    sort_cache: DashMap<String, AtomicU64>,
    /// Slot (alive) bitmap bytes.
    slot_bytes: AtomicU64,

    /// Fields that have been mutated since last scan.
    stale_fields: Mutex<HashSet<String>>,
    /// When true, ALL fields need scanning (post-restore or bulk load).
    all_stale: AtomicBool,

    /// Scanner enabled flag (runtime toggle).
    enabled: AtomicBool,
    /// Scan interval in milliseconds.
    interval_ms: AtomicU64,
    /// Max fields to scan per tick.
    batch_size: AtomicU64,
}

impl BitmapMemoryCache {
    /// Create a new cache with the given scanner configuration.
    pub fn new(enabled: bool, interval_ms: u64, batch_size: u64) -> Self {
        Self {
            filter_cache: DashMap::new(),
            sort_cache: DashMap::new(),
            slot_bytes: AtomicU64::new(0),
            stale_fields: Mutex::new(HashSet::new()),
            all_stale: AtomicBool::new(false),
            enabled: AtomicBool::new(enabled),
            interval_ms: AtomicU64::new(interval_ms),
            batch_size: AtomicU64::new(batch_size),
        }
    }

    /// Mark a specific field as needing a memory rescan.
    /// Called by the flush thread after applying mutations.
    pub fn mark_stale(&self, field_name: &str) {
        self.stale_fields.lock().insert(field_name.to_string());
    }

    /// Mark all fields as stale. Called after bulk load or initial restore.
    pub fn mark_all_stale(&self) {
        self.all_stale.store(true, Ordering::Release);
    }

    /// Return cached filter memory: Vec of (field_name, bytes, count).
    pub fn cached_filter_memory(&self) -> Vec<(String, u64, u64)> {
        self.filter_cache
            .iter()
            .map(|entry| {
                let (bytes_atom, count_atom) = entry.value();
                (
                    entry.key().clone(),
                    bytes_atom.load(Ordering::Relaxed),
                    count_atom.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// Return cached sort memory: Vec of (field_name, bytes).
    pub fn cached_sort_memory(&self) -> Vec<(String, u64)> {
        self.sort_cache
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Return cached slot bitmap bytes.
    pub fn cached_slot_bytes(&self) -> u64 {
        self.slot_bytes.load(Ordering::Relaxed)
    }

    /// Runtime toggle: enable/disable the scanner.
    pub fn set_enabled(&self, v: bool) {
        self.enabled.store(v, Ordering::Relaxed);
    }

    /// Runtime config: set scan interval.
    pub fn set_interval_ms(&self, v: u64) {
        self.interval_ms.store(v, Ordering::Relaxed);
    }

    /// Runtime config: set batch size.
    pub fn set_batch_size(&self, v: u64) {
        self.batch_size.store(v, Ordering::Relaxed);
    }

    /// Check if scanner is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Get current interval in milliseconds.
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms.load(Ordering::Relaxed)
    }

    /// Get current batch size.
    pub fn batch_size(&self) -> u64 {
        self.batch_size.load(Ordering::Relaxed)
    }

    /// Drain up to `batch_size` stale fields. Returns them for scanning.
    /// If `all_stale` is set, populates from the provided field name lists instead.
    fn drain_stale(
        &self,
        all_filter_names: &[String],
        all_sort_names: &[String],
    ) -> Vec<String> {
        let batch = self.batch_size.load(Ordering::Relaxed) as usize;

        // If all_stale is set, swap it to false and enqueue everything.
        if self.all_stale.compare_exchange(
            true, false, Ordering::AcqRel, Ordering::Relaxed,
        ).is_ok() {
            let mut set = self.stale_fields.lock();
            for name in all_filter_names {
                set.insert(name.clone());
            }
            for name in all_sort_names {
                set.insert(name.clone());
            }
            // Also mark a sentinel so slot bytes get updated.
            set.insert("__slots__".to_string());
        }

        let mut set = self.stale_fields.lock();
        let mut result = Vec::with_capacity(batch.min(set.len()));
        let drain: Vec<String> = set.iter().take(batch).cloned().collect();
        for f in &drain {
            set.remove(f);
        }
        result.extend(drain);
        result
    }

    /// Run one scan tick. Called by the scanner thread.
    ///
    /// Takes the ArcSwap handle to load snapshots per-field (dropping guard
    /// between each field to avoid pinning old snapshot memory).
    pub fn scan_tick(
        &self,
        inner: &arc_swap::ArcSwap<crate::concurrent_engine::InnerEngine>,
        loading_mode: &AtomicBool,
        all_filter_names: &[String],
        all_sort_names: &[String],
    ) {
        if !self.is_enabled() {
            return;
        }
        // Skip scanning during loading mode — bitmaps are changing rapidly
        // and snapshots aren't being published.
        if loading_mode.load(Ordering::Relaxed) {
            return;
        }

        let stale = self.drain_stale(all_filter_names, all_sort_names);
        if stale.is_empty() {
            return;
        }

        for field_name in &stale {
            if field_name == "__slots__" {
                // Update slot bytes (always cheap).
                let snap = inner.load();
                self.slot_bytes.store(snap.slots.bitmap_bytes() as u64, Ordering::Relaxed);
                // Guard dropped here.
                continue;
            }

            // Try as filter field first. Load snapshot, read one field, drop guard.
            {
                let snap = inner.load();
                if let Some(filter_field) = snap.filters.get_field(field_name) {
                    let bytes = filter_field.bitmap_bytes() as u64;
                    let count = filter_field.bitmap_count() as u64;
                    // Update or insert cache entry.
                    self.filter_cache
                        .entry(field_name.clone())
                        .and_modify(|(b, c)| {
                            b.store(bytes, Ordering::Relaxed);
                            c.store(count, Ordering::Relaxed);
                        })
                        .or_insert_with(|| (AtomicU64::new(bytes), AtomicU64::new(count)));
                }
                // Guard drops here at end of block.
            }

            // Try as sort field. Separate snapshot load.
            {
                let snap = inner.load();
                if let Some(sort_field) = snap.sorts.get_field(field_name) {
                    let bytes = sort_field.bitmap_bytes() as u64;
                    self.sort_cache
                        .entry(field_name.clone())
                        .and_modify(|b| {
                            b.store(bytes, Ordering::Relaxed);
                        })
                        .or_insert_with(|| AtomicU64::new(bytes));
                }
                // Guard drops here at end of block.
            }
        }

        // Always update slot bytes (cheap — single bitmap).
        {
            let snap = inner.load();
            self.slot_bytes.store(snap.slots.bitmap_bytes() as u64, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod bitmap_memory_cache_tests {
    use super::*;

    #[test]
    fn test_mark_stale_and_drain() {
        let cache = BitmapMemoryCache::new(true, 100, 2);
        cache.mark_stale("field_a");
        cache.mark_stale("field_b");
        cache.mark_stale("field_c");

        let filter_names = vec![];
        let sort_names = vec![];
        let batch = cache.drain_stale(&filter_names, &sort_names);
        assert_eq!(batch.len(), 2, "should drain up to batch_size");

        let batch2 = cache.drain_stale(&filter_names, &sort_names);
        assert_eq!(batch2.len(), 1, "should drain remaining");

        let batch3 = cache.drain_stale(&filter_names, &sort_names);
        assert!(batch3.is_empty(), "should be empty after draining all");
    }

    #[test]
    fn test_mark_all_stale() {
        let cache = BitmapMemoryCache::new(true, 100, 100);
        cache.mark_all_stale();

        let filter_names = vec!["f1".to_string(), "f2".to_string()];
        let sort_names = vec!["s1".to_string()];
        let batch = cache.drain_stale(&filter_names, &sort_names);
        // Should contain f1, f2, s1, and __slots__
        assert_eq!(batch.len(), 4);
        assert!(batch.contains(&"f1".to_string()));
        assert!(batch.contains(&"s1".to_string()));
        assert!(batch.contains(&"__slots__".to_string()));
    }

    #[test]
    fn test_cached_values_default_zero() {
        let cache = BitmapMemoryCache::new(true, 100, 10);
        assert_eq!(cache.cached_slot_bytes(), 0);
        assert!(cache.cached_filter_memory().is_empty());
        assert!(cache.cached_sort_memory().is_empty());
    }

    #[test]
    fn test_runtime_config() {
        let cache = BitmapMemoryCache::new(true, 100, 3);
        assert!(cache.is_enabled());
        assert_eq!(cache.interval_ms(), 100);
        assert_eq!(cache.batch_size(), 3);

        cache.set_enabled(false);
        cache.set_interval_ms(500);
        cache.set_batch_size(10);

        assert!(!cache.is_enabled());
        assert_eq!(cache.interval_ms(), 500);
        assert_eq!(cache.batch_size(), 10);
    }
}
