//! Unified Cache — Flat HashMap replacing trie cache + bound cache
//!
//! Each entry is keyed by (canonical filter clauses, sort field, sort direction) and stores
//! a dynamically-sized bounded bitmap: the approximate top-K documents within the filter
//! result, sorted by the specified field. Entries start at initial_capacity (default 4K)
//! and jump straight to max_capacity (default 64K) on first expansion.
//!
//! Live maintenance is performed by the flush thread: when documents are inserted, updated,
//! or deleted, the meta-index identifies affected entries, and each entry's bitmap is updated
//! via per-slot contains() checks against the engine's field bitmaps.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use roaring::RoaringBitmap;

use crate::bound_store::ShardKey;
use crate::cache::CanonicalClause;
use crate::filter::FilterIndex;
use crate::meta_index::{CacheEntryId, MetaIndex};
use crate::query::SortDirection;
use crate::radix_sort::RadixSortIndex;
use crate::sort::SortIndex;
use crate::write_coalescer::FilterGroupKey;

/// Configuration for the unified cache.
#[derive(Debug, Clone)]
pub struct UnifiedCacheConfig {
    /// Maximum number of cache entries (safety cap, default 100_000).
    pub max_entries: usize,
    /// Maximum total cache memory in bytes (default 512 MB). Primary eviction trigger.
    pub max_bytes: usize,
    /// Initial bound capacity per entry (default 4000).
    pub initial_capacity: usize,
    /// Maximum bound capacity per entry after expansion (default 64000).
    pub max_capacity: usize,
    /// Skip caching if filter result has fewer docs than this (default 0 = cache everything).
    pub min_filter_size: usize,
    /// Maximum maintenance work per flush (affected_entries × changed_slots).
    /// When exceeded, affected entries are marked for rebuild instead of
    /// per-slot evaluation. Prevents positive feedback loops under burst writes.
    /// Default 50_000 (~5ms of maintenance work).
    pub max_maintenance_work: usize,
}

impl Default for UnifiedCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_bytes: 512 * 1024 * 1024, // 512 MB
            initial_capacity: 4_000,
            max_capacity: 64_000,
            min_filter_size: 0,
            max_maintenance_work: 50_000,
        }
    }
}

/// Cache key: canonical filters + sort field + direction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnifiedKey {
    pub filter_clauses: Vec<CanonicalClause>,
    pub sort_field: String,
    pub direction: SortDirection,
}

/// Cache entry: dynamically-sized bounded bitmap.
///
/// At initial capacity (≤4K), pagination uses bitmap sort traversal.
/// After expansion (>4K → 64K), a `RadixSortIndex` is built for O(1) bucket-based
/// pagination and O(1) maintenance (vs O(n) memmove for sorted vecs).
pub struct UnifiedEntry {
    /// Bounded top-K bitmap within the filter result.
    bitmap: Arc<RoaringBitmap>,
    /// Sort floor (Desc) or ceiling (Asc) of the current bound.
    min_tracked_value: u32,
    /// Current capacity: starts at initial_capacity (4K), jumps to max_capacity (64K) on expansion.
    capacity: usize,
    /// Ceiling from config.
    max_capacity: usize,
    /// Whether more results exist beyond the current bound.
    has_more: bool,
    /// Total documents matching the filter (for returning total_matched without recomputing filters).
    total_matched: u64,
    /// Bloat control: flagged when cardinality exceeds 2 * capacity.
    needs_rebuild: bool,
    /// Guard to prevent concurrent rebuilds.
    rebuilding: AtomicBool,
    /// LRU timestamp.
    last_used: Instant,
    /// Meta-index entry ID for this cache entry.
    meta_id: CacheEntryId,
    /// Dirty flag for persistence: set when bitmap modified by live maintenance,
    /// cleared when merge thread writes the shard. LRU eviction skips dirty entries.
    persist_dirty: bool,
    /// Pre-sorted packed keys for O(1) pagination via binary search at initial capacity.
    /// Each key is `(sort_value as u64) << 32 | slot_id`. Sorted in traversal order.
    /// Cleared on expand() when radix takes over.
    sorted_keys: Option<Arc<Vec<u64>>>,
    /// Radix sort index for expanded entries (>4K items).
    /// Built during expand(), enables O(1) bucket-based pagination and maintenance.
    /// None at initial capacity — sorted vec binary search is faster for ≤4K items.
    radix: Option<Arc<RadixSortIndex>>,
    /// Sort direction for this entry (needed for radix iteration order).
    direction: SortDirection,
}

impl UnifiedEntry {
    /// Create a new entry from a sort traversal result.
    ///
    /// `sorted_slots` should be the top-N slots from the sort traversal, in sort order.
    /// `value_fn` returns the sort value for a given slot.
    /// At formation, capacity is initial_capacity (4K) — no radix needed.
    pub fn new(
        sorted_slots: &[u32],
        capacity: usize,
        max_capacity: usize,
        has_more: bool,
        total_matched: u64,
        meta_id: CacheEntryId,
        direction: SortDirection,
        value_fn: impl Fn(u32) -> u32,
    ) -> Self {
        let mut bitmap = RoaringBitmap::new();
        let take_count = sorted_slots.len().min(capacity);
        for &slot in &sorted_slots[..take_count] {
            bitmap.insert(slot);
        }

        let min_tracked_value = if take_count > 0 {
            value_fn(sorted_slots[take_count - 1])
        } else {
            0
        };

        let bitmap = Arc::new(bitmap);

        // Build sorted keys for fast binary search pagination at initial capacity.
        // Each key is (sort_value << 32) | slot_id, sorted in traversal order.
        let sorted_keys = if take_count > 0 {
            Some(Arc::new(Self::build_sorted_keys(&sorted_slots[..take_count], direction, &value_fn)))
        } else {
            None
        };

        Self {
            bitmap,
            min_tracked_value,
            capacity,
            max_capacity,
            has_more,
            total_matched,
            needs_rebuild: false,
            rebuilding: AtomicBool::new(false),
            last_used: Instant::now(),
            meta_id,
            persist_dirty: true, // New entries need persisting
            sorted_keys,
            radix: None, // No radix at initial capacity — sorted vec is faster
            direction,
        }
    }

    /// Create an entry restored from disk (shard load).
    /// Has bitmap but no sorted_keys/radix — those are reconstructed lazily.
    pub fn from_restored(
        bitmap: RoaringBitmap,
        meta_id: CacheEntryId,
        initial_capacity: usize,
        max_capacity: usize,
        direction: SortDirection,
        value_fn: impl Fn(u32) -> u32,
    ) -> Self {
        let card = bitmap.len() as usize;
        let capacity = if card > initial_capacity {
            max_capacity
        } else {
            initial_capacity
        };

        // Build sorted_keys for fast binary search pagination
        let slots: Vec<u32> = bitmap.iter().collect();
        let sorted_keys = if !slots.is_empty() && card <= max_capacity {
            Some(Arc::new(Self::build_sorted_keys(&slots, direction, &value_fn)))
        } else {
            None
        };

        // Compute min_tracked_value from the sorted keys
        let min_tracked_value = sorted_keys.as_ref().and_then(|keys| {
            keys.last().map(|&k| (k >> 32) as u32)
        }).unwrap_or(0);

        // Compute total_matched from bitmap cardinality (approximation —
        // the true total may be larger if has_more was true)
        let total_matched = card as u64;

        Self {
            bitmap: Arc::new(bitmap),
            min_tracked_value,
            capacity,
            max_capacity,
            has_more: true,
            total_matched,
            needs_rebuild: false,
            rebuilding: AtomicBool::new(false),
            last_used: Instant::now(),
            meta_id,
            persist_dirty: false, // Just loaded from disk — clean
            sorted_keys,
            radix: None,
            direction,
        }
    }

    pub fn bitmap(&self) -> &Arc<RoaringBitmap> {
        &self.bitmap
    }

    pub fn bitmap_mut(&mut self) -> &mut RoaringBitmap {
        Arc::make_mut(&mut self.bitmap)
    }

    pub fn min_tracked_value(&self) -> u32 {
        self.min_tracked_value
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    pub fn has_more(&self) -> bool {
        self.has_more
    }

    pub fn total_matched(&self) -> u64 {
        self.total_matched
    }

    pub fn needs_rebuild(&self) -> bool {
        self.needs_rebuild
    }

    pub fn mark_for_rebuild(&mut self) {
        self.needs_rebuild = true;
    }

    pub fn meta_id(&self) -> CacheEntryId {
        self.meta_id
    }

    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    pub fn last_used(&self) -> Instant {
        self.last_used
    }

    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// Add a slot to the bounded bitmap. Returns true if bloat threshold was exceeded.
    /// `sort_value` is needed to maintain the radix index when present.
    pub fn add_slot(&mut self, slot: u32, sort_value: u32) -> bool {
        Arc::make_mut(&mut self.bitmap).insert(slot);
        self.persist_dirty = true;

        // Invalidate sorted_keys — maintaining sorted order in a Vec is O(n)
        // per operation. The bitmap path is only slightly slower and correct.
        // sorted_keys will be rebuilt on next rebuild() call.
        self.sorted_keys = None;

        // Maintain radix if present (expanded entry)
        if let Some(ref mut radix) = self.radix {
            Arc::make_mut(radix).insert(slot, sort_value);
        }

        let bloat_threshold = self.capacity * 2;
        if self.bitmap.len() as usize > bloat_threshold {
            self.needs_rebuild = true;
            true
        } else {
            false
        }
    }

    /// Remove a slot from the bounded bitmap.
    /// `sort_value` is needed to maintain the radix index when present.
    pub fn remove_slot(&mut self, slot: u32, sort_value: u32) {
        Arc::make_mut(&mut self.bitmap).remove(slot);
        self.persist_dirty = true;

        // Invalidate sorted_keys — stale keys would return removed slots.
        self.sorted_keys = None;

        // Maintain radix if present (expanded entry)
        if let Some(ref mut radix) = self.radix {
            Arc::make_mut(radix).remove(slot, sort_value);
        }
    }

    /// Remove a slot without knowing its sort value. Uses blind scan for radix.
    pub fn remove_slot_blind(&mut self, slot: u32) {
        Arc::make_mut(&mut self.bitmap).remove(slot);
        self.persist_dirty = true;

        // Invalidate sorted_keys — stale keys would return removed slots.
        self.sorted_keys = None;

        if let Some(ref mut radix) = self.radix {
            Arc::make_mut(radix).remove_blind(slot);
        }
    }

    /// Check if a sort value qualifies for this bound.
    pub fn sort_qualifies(&self, value: u32, direction: SortDirection) -> bool {
        match direction {
            SortDirection::Desc => value > self.min_tracked_value,
            SortDirection::Asc => value < self.min_tracked_value,
        }
    }

    /// Expand the entry by appending new slots from a deeper sort traversal.
    /// Returns the new capacity after expansion.
    ///
    /// Builds a RadixSortIndex from the full bitmap for O(1) bucket-based pagination
    /// and O(1) maintenance at the expanded capacity.
    pub fn expand(
        &mut self,
        new_slots: &[u32],
        value_fn: impl Fn(u32) -> u32,
    ) -> usize {
        let bm = Arc::make_mut(&mut self.bitmap);
        for &slot in new_slots {
            bm.insert(slot);
        }

        // Update min_tracked_value from the last new slot
        if let Some(&last) = new_slots.last() {
            self.min_tracked_value = value_fn(last);
        }

        // Jump straight to max capacity on expansion — memory is cheap (~8-16KB per
        // entry at 64K) and this eliminates repeated expansion events at boundaries.
        let old_capacity = self.capacity;
        self.capacity = self.max_capacity;

        // Clear sorted keys — radix takes over for expanded entries
        self.sorted_keys = None;

        // Build radix index from the full bitmap (old + new slots).
        // ~1ms at 64K items (benchmarked). Enables O(1) pagination and maintenance.
        self.radix = Some(Arc::new(RadixSortIndex::from_bitmap(&self.bitmap, &value_fn)));

        // If expansion returned fewer than expected, no more results
        let expected_chunk = self.max_capacity - old_capacity;
        if new_slots.len() < expected_chunk {
            self.has_more = false;
        }

        self.max_capacity
    }

    /// Rebuild the entry from a fresh sort traversal.
    pub fn rebuild(
        &mut self,
        sorted_slots: &[u32],
        value_fn: impl Fn(u32) -> u32,
    ) {
        let take_count = sorted_slots.len().min(self.capacity);
        let mut bitmap = RoaringBitmap::new();
        for &slot in &sorted_slots[..take_count] {
            bitmap.insert(slot);
        }

        self.min_tracked_value = if take_count > 0 {
            value_fn(sorted_slots[take_count - 1])
        } else {
            0
        };

        self.bitmap = Arc::new(bitmap);

        // Rebuild radix if at expanded capacity, sorted keys if at initial capacity
        if self.capacity >= self.max_capacity {
            self.sorted_keys = None;
            self.radix = Some(Arc::new(RadixSortIndex::from_bitmap(&self.bitmap, &value_fn)));
        } else {
            self.sorted_keys = if take_count > 0 {
                Some(Arc::new(Self::build_sorted_keys(&sorted_slots[..take_count], self.direction, &value_fn)))
            } else {
                None
            };
            self.radix = None;
        }

        self.needs_rebuild = false;
        self.rebuilding.store(false, Ordering::Release);
    }

    /// Try to acquire the rebuild guard. Returns true if this caller should do the rebuild.
    pub fn try_start_rebuild(&self) -> bool {
        self.rebuilding
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Get the radix sort index (present for expanded entries).
    pub fn radix(&self) -> Option<&Arc<RadixSortIndex>> {
        self.radix.as_ref()
    }

    /// Get the sort direction for this entry.
    pub fn direction(&self) -> SortDirection {
        self.direction
    }

    /// Whether this entry has unsaved bitmap modifications.
    pub fn is_persist_dirty(&self) -> bool {
        self.persist_dirty
    }

    /// Mark this entry as having unsaved modifications.
    pub fn mark_persist_dirty(&mut self) {
        self.persist_dirty = true;
    }

    /// Clear the persist dirty flag (after successful shard write).
    pub fn clear_persist_dirty(&mut self) {
        self.persist_dirty = false;
    }

    /// Get the pre-sorted keys for binary search pagination (initial capacity only).
    /// Returns None after expand() when radix takes over.
    pub fn sorted_keys(&self) -> Option<&Arc<Vec<u64>>> {
        self.sorted_keys.as_ref()
    }

    /// Memory usage of this entry's bitmap + sorted keys + radix index.
    pub fn memory_bytes(&self) -> usize {
        let bitmap_bytes = self.bitmap.serialized_size();
        let keys_bytes = self.sorted_keys.as_ref()
            .map(|k| k.capacity() * 8)
            .unwrap_or(0);
        let radix_bytes = self.radix.as_ref().map(|r| r.memory_bytes()).unwrap_or(0);
        bitmap_bytes + keys_bytes + radix_bytes
    }

    /// Build packed sorted keys from slots + values.
    fn build_sorted_keys(slots: &[u32], direction: SortDirection, value_fn: &impl Fn(u32) -> u32) -> Vec<u64> {
        let mut keys: Vec<u64> = slots.iter().map(|&slot| {
            let val = value_fn(slot) as u64;
            (val << 32) | (slot as u64)
        }).collect();
        match direction {
            SortDirection::Desc => keys.sort_unstable_by(|a, b| b.cmp(a)),
            SortDirection::Asc => keys.sort_unstable(),
        }
        keys
    }
}

/// Stats snapshot for the unified cache.
pub struct UnifiedCacheStats {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub memory_bytes: usize,
    pub meta_index_entries: usize,
    pub meta_index_bytes: usize,
    // Persistence stats
    pub persistence_enabled: bool,
    pub tombstone_count: u64,
    pub pending_shard_count: usize,
    pub dirty_shard_count: usize,
    pub meta_dirty: bool,
}

/// Per-entry diagnostic detail.
pub struct UnifiedEntryDetail {
    pub sort_field: String,
    pub direction: String,
    pub filter_count: usize,
    pub cardinality: u64,
    pub capacity: usize,
    pub max_capacity: usize,
    pub has_more: bool,
    pub min_tracked_value: u32,
}

/// The unified cache: flat HashMap keyed by (filters, sort, direction).
pub struct UnifiedCache {
    entries: HashMap<UnifiedKey, UnifiedEntry>,
    /// Reverse index: meta_id → key, for O(1) lookup from MetaIndex results.
    meta_id_to_key: HashMap<CacheEntryId, UnifiedKey>,
    meta: MetaIndex,
    config: UnifiedCacheConfig,
    hits: u64,
    misses: u64,
    /// Running total of entry memory (bitmap + sorted_keys + radix bytes).
    total_bytes: usize,

    // ── Persistence State ──────────────────────────────────────────────
    /// Shards that exist on disk but haven't been loaded into RAM yet.
    pending_shards: HashSet<ShardKey>,
    /// Shards currently being loaded by another thread (loading sentinel).
    loading_shards: HashSet<ShardKey>,
    /// Whether meta.bin needs rewriting (new entry, expansion, tombstone).
    meta_dirty: bool,
    /// Which shards need rewriting (bitmap modified by maintenance).
    shard_dirty: HashSet<ShardKey>,
    /// Whether persistence is enabled (BoundStore exists).
    persistence_enabled: bool,
}

impl UnifiedCache {
    pub fn new(config: UnifiedCacheConfig) -> Self {
        Self {
            entries: HashMap::new(),
            meta_id_to_key: HashMap::new(),
            meta: MetaIndex::new(),
            config,
            hits: 0,
            misses: 0,
            total_bytes: 0,
            pending_shards: HashSet::new(),
            loading_shards: HashSet::new(),
            meta_dirty: false,
            shard_dirty: HashSet::new(),
            persistence_enabled: false,
        }
    }

    /// Look up a cache entry by key. Returns None on miss.
    /// Increments hit/miss counters.
    pub fn lookup(&mut self, key: &UnifiedKey) -> Option<&mut UnifiedEntry> {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.needs_rebuild {
                // Entry is stale (alive/filter change) — treat as miss.
                // The caller will do a full traversal and re-form the entry.
                self.misses += 1;
                return None;
            }
            self.hits += 1;
            entry.touch();
            Some(entry)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Look up immutably (no touch).
    pub fn get(&self, key: &UnifiedKey) -> Option<&UnifiedEntry> {
        self.entries.get(key)
    }

    /// Store a new entry, evicting LRU if over budget. Returns the meta_id assigned.
    pub fn store(&mut self, key: UnifiedKey, entry: UnifiedEntry) -> CacheEntryId {
        let meta_id = entry.meta_id;
        let new_bytes = entry.memory_bytes();

        // If replacing an existing entry, deregister the old one and subtract its bytes
        if let Some(old) = self.entries.remove(&key) {
            self.total_bytes = self.total_bytes.saturating_sub(old.memory_bytes());
            self.meta_id_to_key.remove(&old.meta_id);
            self.meta.deregister(old.meta_id);
        }

        // Evict LRU entries while over byte budget or entry count cap
        while (self.total_bytes + new_bytes > self.config.max_bytes
            || self.entries.len() >= self.config.max_entries)
            && !self.entries.is_empty()
        {
            self.evict_lru();
        }

        // Mark dirty for persistence
        if self.persistence_enabled {
            self.meta_dirty = true;
            let shard_key = ShardKey::new(key.sort_field.clone(), key.direction);
            self.shard_dirty.insert(shard_key);
        }

        self.total_bytes += new_bytes;
        self.meta_id_to_key.insert(meta_id, key.clone());
        self.entries.insert(key, entry);
        meta_id
    }

    /// Register a new entry with the meta-index and create the entry.
    /// This is the primary way to create and store entries.
    pub fn form_and_store(
        &mut self,
        key: UnifiedKey,
        sorted_slots: &[u32],
        has_more: bool,
        total_matched: u64,
        value_fn: impl Fn(u32) -> u32,
    ) -> CacheEntryId {
        // Register with meta-index
        let meta_id = self.meta.register(
            &key.filter_clauses,
            Some(&key.sort_field),
            Some(key.direction),
        );

        let direction = key.direction;
        let entry = UnifiedEntry::new(
            sorted_slots,
            self.config.initial_capacity,
            self.config.max_capacity,
            has_more,
            total_matched,
            meta_id,
            direction,
            value_fn,
        );

        self.store(key, entry)
    }

    /// Evict the least-recently-used entry. Returns the evicted key, if any.
    ///
    /// When persistence is enabled:
    /// - Skips dirty entries (unsaved bitmap modifications)
    /// - Does NOT deregister from meta-index (entry stays on disk as orphan)
    pub fn evict_lru(&mut self) -> Option<UnifiedKey> {
        let lru_key = if self.persistence_enabled {
            // Skip dirty entries — they have unsaved bitmap modifications
            self.entries
                .iter()
                .filter(|(_, entry)| !entry.persist_dirty)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
                .or_else(|| {
                    // All entries dirty — fall back to oldest regardless
                    self.entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(key, _)| key.clone())
                })
        } else {
            self.entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
        }?;

        if let Some(evicted) = self.entries.remove(&lru_key) {
            tracing::info!(
                "Cache evicted entry: sort={} {:?} | filters={} | card={} | bytes={}",
                lru_key.sort_field, lru_key.direction, lru_key.filter_clauses.len(),
                evicted.cardinality(), evicted.memory_bytes()
            );
            self.total_bytes = self.total_bytes.saturating_sub(evicted.memory_bytes());
            self.meta_id_to_key.remove(&evicted.meta_id);
            if !self.persistence_enabled {
                // Without persistence, deregister fully (original behavior)
                self.meta.deregister(evicted.meta_id);
            }
            // With persistence: meta-index keeps the registration.
            // Entry stays on disk as orphan — can be reloaded from shard.
        }

        Some(lru_key)
    }

    /// Get a mutable reference to an entry by key (no touch).
    pub fn get_mut(&mut self, key: &UnifiedKey) -> Option<&mut UnifiedEntry> {
        self.entries.get_mut(key)
    }

    /// Access the meta-index.
    pub fn meta(&self) -> &MetaIndex {
        &self.meta
    }

    /// Access the meta-index mutably.
    pub fn meta_mut(&mut self) -> &mut MetaIndex {
        &mut self.meta
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Total memory of all bounded bitmaps.
    pub fn total_memory_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Reconcile the tracked total_bytes with actual entry sizes.
    /// Call after bulk maintenance operations (expand/rebuild/add_slot/remove_slot)
    /// which mutate entries in-place without updating the running total.
    pub fn reconcile_bytes(&mut self) {
        self.total_bytes = self.entries.values().map(|e| e.memory_bytes()).sum();
    }

    /// Clear all entries, reset the meta-index, and reset counters.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.meta_id_to_key.clear();
        self.meta = MetaIndex::new();
        self.hits = 0;
        self.misses = 0;
        self.total_bytes = 0;
        self.pending_shards.clear();
        self.loading_shards.clear();
        self.meta_dirty = false;
        self.shard_dirty.clear();
    }

    /// Return a stats snapshot.
    pub fn stats(&self) -> UnifiedCacheStats {
        UnifiedCacheStats {
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            memory_bytes: self.total_memory_bytes(),
            meta_index_entries: self.meta.entry_count(),
            meta_index_bytes: self.meta.memory_bytes(),
            persistence_enabled: self.persistence_enabled,
            tombstone_count: self.meta.tombstone_count(),
            pending_shard_count: self.pending_shards.len(),
            dirty_shard_count: self.shard_dirty.len(),
            meta_dirty: self.meta_dirty,
        }
    }

    /// Return per-entry detail for diagnostics/testing.
    pub fn entry_details(&self) -> Vec<UnifiedEntryDetail> {
        self.entries.iter().map(|(key, entry)| {
            UnifiedEntryDetail {
                sort_field: key.sort_field.to_string(),
                direction: format!("{:?}", key.direction),
                filter_count: key.filter_clauses.len(),
                cardinality: entry.bitmap.len(),
                capacity: entry.capacity,
                max_capacity: entry.max_capacity,
                has_more: entry.has_more,
                min_tracked_value: entry.min_tracked_value,
            }
        }).collect()
    }

    /// Reset hit/miss counters without clearing entries.
    pub fn reset_counters(&mut self) {
        self.hits = 0;
        self.misses = 0;
    }

    /// Get the cache config.
    pub fn config(&self) -> &UnifiedCacheConfig {
        &self.config
    }

    /// Iterate all entries mutably (for flush thread maintenance).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&UnifiedKey, &mut UnifiedEntry)> {
        self.entries.iter_mut()
    }

    /// Get entry by meta_id. O(1) via reverse index.
    pub fn entry_by_meta_id(&mut self, meta_id: CacheEntryId) -> Option<&mut UnifiedEntry> {
        let key = self.meta_id_to_key.get(&meta_id)?;
        self.entries.get_mut(key)
    }

    /// Get the key for a meta_id. O(1) via reverse index.
    pub fn key_for_meta_id(&self, meta_id: CacheEntryId) -> Option<&UnifiedKey> {
        self.meta_id_to_key.get(&meta_id)
    }

    /// Iterate over all meta_id → key mappings (for persistence snapshot).
    pub fn iter_meta_id_to_key(&self) -> impl Iterator<Item = (&CacheEntryId, &UnifiedKey)> {
        self.meta_id_to_key.iter()
    }

    // ── Persistence Support ──────────────────────────────────────────────────

    /// Enable persistence mode. Called when a BoundStore is available.
    pub fn enable_persistence(&mut self) {
        self.persistence_enabled = true;
    }

    /// Whether persistence is enabled.
    pub fn persistence_enabled(&self) -> bool {
        self.persistence_enabled
    }

    /// Check if a shard is pending (exists on disk, not loaded).
    pub fn is_shard_pending(&self, sort_field: &str, direction: SortDirection) -> bool {
        self.pending_shards.contains(&ShardKey::new(sort_field.to_string(), direction))
    }

    /// Check if a shard is currently being loaded.
    pub fn is_shard_loading(&self, sort_field: &str, direction: SortDirection) -> bool {
        self.loading_shards.contains(&ShardKey::new(sort_field.to_string(), direction))
    }

    /// Mark a shard as loading (sentinel to prevent concurrent loads).
    pub fn mark_shard_loading(&mut self, sort_field: &str, direction: SortDirection) {
        let key = ShardKey::new(sort_field.to_string(), direction);
        self.pending_shards.remove(&key);
        self.loading_shards.insert(key);
    }

    /// Mark a shard as loaded (remove from pending and loading).
    pub fn mark_shard_loaded(&mut self, sort_field: &str, direction: SortDirection) {
        let key = ShardKey::new(sort_field.to_string(), direction);
        self.pending_shards.remove(&key);
        self.loading_shards.remove(&key);
    }

    /// Add pending shards (from meta.bin on startup).
    pub fn add_pending_shards(&mut self, shards: impl IntoIterator<Item = ShardKey>) {
        self.pending_shards.extend(shards);
    }

    /// Get all pending shard keys.
    pub fn pending_shards(&self) -> &HashSet<ShardKey> {
        &self.pending_shards
    }

    /// Insert a restored entry from disk (shard load). Does NOT register with
    /// meta-index (that was done during meta.bin load). Does NOT set meta_dirty.
    pub fn insert_restored_entry(&mut self, key: UnifiedKey, entry: UnifiedEntry) {
        let meta_id = entry.meta_id;
        let bytes = entry.memory_bytes();

        // Evict if needed
        while (self.total_bytes + bytes > self.config.max_bytes
            || self.entries.len() >= self.config.max_entries)
            && !self.entries.is_empty()
        {
            self.evict_lru();
        }

        self.total_bytes += bytes;
        self.meta_id_to_key.insert(meta_id, key.clone());
        self.entries.insert(key, entry);
    }

    /// Check if meta needs writing.
    pub fn is_meta_dirty(&self) -> bool {
        self.meta_dirty
    }

    /// Clear the meta dirty flag (after successful write).
    pub fn clear_meta_dirty(&mut self) {
        self.meta_dirty = false;
    }

    /// Set the meta dirty flag.
    pub fn set_meta_dirty(&mut self) {
        self.meta_dirty = true;
    }

    /// Get dirty shards that need writing.
    pub fn dirty_shards(&self) -> &HashSet<ShardKey> {
        &self.shard_dirty
    }

    /// Mark a shard as dirty.
    pub fn mark_shard_dirty(&mut self, key: ShardKey) {
        self.shard_dirty.insert(key);
    }

    /// Clear a shard dirty flag (after successful write).
    pub fn clear_shard_dirty(&mut self, key: &ShardKey) {
        self.shard_dirty.remove(key);
    }

    /// Check if an entry ID is in RAM (for tombstone decisions).
    pub fn has_entry_id(&self, meta_id: CacheEntryId) -> bool {
        self.meta_id_to_key.contains_key(&meta_id)
    }

    /// Collect entries for a specific shard (for merge thread shard write).
    /// Returns (meta_id, key, bitmap_clone) for each entry in the shard.
    pub fn entries_for_shard(&self, shard_key: &ShardKey) -> Vec<(CacheEntryId, UnifiedKey, RoaringBitmap)> {
        self.entries
            .iter()
            .filter(|(key, _)| key.sort_field == shard_key.sort_field && key.direction == shard_key.direction)
            .map(|(key, entry)| (entry.meta_id, key.clone(), entry.bitmap.as_ref().clone()))
            .collect()
    }

    /// Clear persist_dirty flags for entries in a specific shard (after successful write).
    pub fn clear_shard_entry_dirty(&mut self, shard_key: &ShardKey) {
        for (key, entry) in self.entries.iter_mut() {
            if key.sort_field == shard_key.sort_field && key.direction == shard_key.direction {
                entry.persist_dirty = false;
            }
        }
    }

    /// Tombstone an entry that isn't in RAM (flush thread: mutation to unloaded entry).
    /// Sets meta_dirty. Does NOT touch the shard (tombstone cleanup is deferred).
    pub fn tombstone_entry(&mut self, meta_id: CacheEntryId) {
        self.meta.tombstone(meta_id);
        self.meta_dirty = true;
    }

    /// Finalize shard write: clean up tombstones for entries that were omitted,
    /// deregister them from meta-index, and recycle their IDs.
    pub fn finalize_shard_write(&mut self, cleaned_ids: &[CacheEntryId]) {
        for &id in cleaned_ids {
            self.meta.clear_tombstone(id);
            self.meta.deregister(id);
        }
    }

    /// Check if >50% of a shard's entries are tombstoned (triggers forced cleanup).
    pub fn shard_needs_cleanup(&self, shard_key: &ShardKey) -> bool {
        // Count entries registered for this shard's sort spec
        let total = self.meta.entries_for_sort(&shard_key.sort_field, shard_key.direction)
            .map(|bm| bm.len())
            .unwrap_or(0);
        if total == 0 {
            return false;
        }
        let tombstoned = self.meta.entries_for_sort(&shard_key.sort_field, shard_key.direction)
            .map(|bm| {
                let mut count = 0u64;
                for id in bm.iter() {
                    if self.meta.is_tombstoned(id) {
                        count += 1;
                    }
                }
                count
            })
            .unwrap_or(0);
        tombstoned * 2 > total
    }

    /// Tombstone unloaded entries affected by filter field mutations.
    /// Returns the number of entries tombstoned.
    pub fn tombstone_unloaded_for_filter(&mut self, changed_fields: &[&str]) -> u64 {
        if !self.persistence_enabled {
            return 0;
        }
        let mut to_tombstone = Vec::new();
        for field in changed_fields {
            if let Some(bm) = self.meta.entries_for_filter_field(field) {
                for id in bm.iter() {
                    if !self.meta_id_to_key.contains_key(&id) && !self.meta.is_tombstoned(id) {
                        to_tombstone.push(id);
                    }
                }
            }
        }
        let count = to_tombstone.len() as u64;
        for id in to_tombstone {
            self.meta.tombstone(id);
            self.meta_dirty = true;
        }
        count
    }

    /// Tombstone unloaded entries affected by sort field mutations.
    /// Returns the number of entries tombstoned.
    pub fn tombstone_unloaded_for_sort(&mut self, changed_fields: &[&str]) -> u64 {
        if !self.persistence_enabled {
            return 0;
        }
        let mut to_tombstone = Vec::new();
        for field in changed_fields {
            let affected = self.meta.entries_for_sort_field(field);
            for id in affected.iter() {
                if !self.meta_id_to_key.contains_key(&id) && !self.meta.is_tombstoned(id) {
                    to_tombstone.push(id);
                }
            }
        }
        let count = to_tombstone.len() as u64;
        for id in to_tombstone {
            self.meta.tombstone(id);
            self.meta_dirty = true;
        }
        count
    }

    /// Tombstone ALL unloaded entries (registered in meta but not in RAM).
    /// Used when alive changes (deletes) affect all cache entries — we can't
    /// selectively remove a deleted slot from an unloaded entry's bitmap.
    /// Returns the number of entries tombstoned.
    pub fn tombstone_all_unloaded(&mut self) -> u64 {
        if !self.persistence_enabled {
            return 0;
        }
        let to_tombstone: Vec<u32> = self.meta.all_registered_ids()
            .filter(|id| !self.meta_id_to_key.contains_key(id) && !self.meta.is_tombstoned(*id))
            .collect();
        let count = to_tombstone.len() as u64;
        for id in to_tombstone {
            self.meta.tombstone(id);
            self.meta_dirty = true;
        }
        count
    }

    // ── Live Maintenance (Phase 3) ──────────────────────────────────────────

    /// Maintain cache entries when filter fields change.
    ///
    /// For each entry that references a changed field, evaluates each changed slot
    /// against the full filter predicate using contains() checks. Slots that now match
    /// AND have qualifying sort values are added. Slots that no longer match are removed.
    ///
    /// Called by the flush thread after applying mutations to staging.
    pub fn maintain_filter_changes(
        &mut self,
        filter_inserts: &HashMap<FilterGroupKey, Vec<u32>>,
        filter_removes: &HashMap<FilterGroupKey, Vec<u32>>,
        filters: &FilterIndex,
        sorts: &SortIndex,
    ) {
        // Collect changed slots per field name
        let mut changed_slots_per_field: HashMap<&str, HashSet<u32>> = HashMap::new();
        for (key, slots) in filter_inserts {
            changed_slots_per_field
                .entry(&key.field)
                .or_default()
                .extend(slots.iter().copied());
        }
        for (key, slots) in filter_removes {
            changed_slots_per_field
                .entry(&key.field)
                .or_default()
                .extend(slots.iter().copied());
        }

        if changed_slots_per_field.is_empty() {
            return;
        }

        // Clause-level narrowing: find entries matching specific (field, "eq", value)
        // combinations rather than broad field-level matching. This is a 25-50x
        // improvement when fields have many distinct values (e.g., 50 categories
        // → only entries with the specific changed values are checked, not all
        // entries mentioning the field).
        let mut affected_ids = RoaringBitmap::new();

        // Eq clause hits: exact value matches (handles the common case)
        for (key, _slots) in filter_inserts.iter().chain(filter_removes.iter()) {
            let value_repr = key.value.to_string();
            if let Some(bm) = self.meta.entries_for_clause(&key.field, "eq", &value_repr) {
                affected_ids |= bm;
            }
        }

        // Field-level fallback for non-Eq entries (In, Gt, Lt, NotEq, etc.)
        // These entries can't be found by clause-level lookup because their
        // value_repr format differs (e.g., "5,10" for In). Use the broader
        // field-level bitmap but subtract entries already found via clause-level.
        for field in changed_slots_per_field.keys() {
            if let Some(field_bm) = self.meta.entries_for_filter_field(field) {
                // Only add entries not already in affected_ids
                let new_entries = field_bm - &affected_ids;
                if !new_entries.is_empty() {
                    // Check if any of these are non-Eq entries (have ops other than "eq")
                    for meta_id in new_entries.iter() {
                        if let Some(key) = self.meta_id_to_key.get(&meta_id) {
                            // Include if any clause for this field uses a non-Eq op
                            let has_non_eq = key.filter_clauses.iter().any(|c| {
                                c.field == *field && c.op != "eq"
                            });
                            if has_non_eq {
                                affected_ids.insert(meta_id);
                            }
                        }
                    }
                }
            }
        }

        if affected_ids.is_empty() {
            return;
        }

        // Count total changed slots for budget estimation
        let total_changed_slots: usize = changed_slots_per_field.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_changed_slots;

        // Budget check: if maintenance would be too expensive, mark affected
        // entries for rebuild instead. Prevents positive feedback loops where
        // long maintenance → batch growth → even longer maintenance.
        if estimated_work > self.config.max_maintenance_work {
            for meta_id in affected_ids.iter() {
                if let Some(key) = self.meta_id_to_key.get(&meta_id) {
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.mark_for_rebuild();
                    }
                }
            }
            return;
        }

        // Collect affected keys (avoids borrow conflict between meta_id_to_key and entries)
        let affected_keys: Vec<UnifiedKey> = affected_ids
            .iter()
            .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
            .collect();

        // Iterate only affected entries
        for key in &affected_keys {
            let Some(entry) = self.entries.get_mut(key) else {
                continue;
            };
            if entry.needs_rebuild {
                continue;
            }

            // Collect slots to check: union of changed slots from the entry's referenced fields
            let mut slots_to_check = HashSet::new();
            for clause in &key.filter_clauses {
                if let Some(slots) = changed_slots_per_field.get(clause.field.as_str()) {
                    slots_to_check.extend(slots);
                }
            }

            if slots_to_check.is_empty() {
                continue;
            }

            for &slot in &slots_to_check {
                let sort_value = sorts
                    .get_field(&key.sort_field)
                    .map(|f| f.reconstruct_value(slot))
                    .unwrap_or(0);
                let matches = slot_matches_filter(slot, &key.filter_clauses, filters, sorts);
                if matches {
                    if entry.sort_qualifies(sort_value, key.direction) {
                        entry.add_slot(slot, sort_value);
                    }
                } else {
                    // Slot no longer matches filter — remove it
                    entry.remove_slot(slot, sort_value);
                }
            }
        }
    }

    /// Maintain cache entries when sort fields change.
    ///
    /// For each entry that sorts by a changed field, checks if changed slots have
    /// qualifying sort values. Only adds slots (never removes on sort change — bloat
    /// control handles cleanup).
    pub fn maintain_sort_changes(
        &mut self,
        sort_mutations: &HashMap<&str, HashSet<u32>>,
        filters: &FilterIndex,
        sorts: &SortIndex,
    ) {
        if sort_mutations.is_empty() {
            return;
        }

        // Use MetaIndex to find only entries that sort by changed fields
        let mut affected_ids = RoaringBitmap::new();
        for field in sort_mutations.keys() {
            affected_ids |= self.meta.entries_for_sort_field(field);
        }

        if affected_ids.is_empty() {
            return;
        }

        // Budget check for sort maintenance
        let total_sort_slots: usize = sort_mutations.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_sort_slots;

        if estimated_work > self.config.max_maintenance_work {
            for meta_id in affected_ids.iter() {
                if let Some(key) = self.meta_id_to_key.get(&meta_id) {
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.mark_for_rebuild();
                    }
                }
            }
            return;
        }

        // Collect affected keys (avoids borrow conflict)
        let affected_keys: Vec<UnifiedKey> = affected_ids
            .iter()
            .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
            .collect();

        // Iterate only affected entries
        for key in &affected_keys {
            let Some(entry) = self.entries.get_mut(key) else {
                continue;
            };
            if entry.needs_rebuild {
                continue;
            }

            let sort_slots = match sort_mutations.get(key.sort_field.as_str()) {
                Some(slots) => slots,
                None => continue,
            };

            for &slot in sort_slots {
                // Check sort qualification first (fast path)
                let sort_value = sorts
                    .get_field(&key.sort_field)
                    .map(|f| f.reconstruct_value(slot))
                    .unwrap_or(0);

                if !entry.sort_qualifies(sort_value, key.direction) {
                    continue;
                }

                // Sort qualifies — check filter match
                if slot_matches_filter(slot, &key.filter_clauses, filters, sorts) {
                    entry.add_slot(slot, sort_value);
                }
            }
        }
    }

    /// Remove a deleted slot from all cache entries.
    ///
    /// Called by the flush thread when a document is deleted. Targeted removal
    /// avoids marking all entries for rebuild, preserving cache effectiveness.
    pub fn remove_slot_from_all(&mut self, slot: u32) {
        for (_, entry) in self.entries.iter_mut() {
            entry.remove_slot_blind(slot);
        }
    }

    /// Mark all entries for rebuild when alive bitmap changes.
    ///
    /// Alive changes affect all filter evaluations (NotEq/Not bake alive into results).
    /// Rather than trying to maintain precisely, mark everything for rebuild.
    pub fn maintain_alive_changes(&mut self) {
        for (_, entry) in self.entries.iter_mut() {
            entry.mark_for_rebuild();
        }
    }

    /// Invalidate entries that reference a specific filter field.
    ///
    /// Marks matching entries for rebuild. Used when fine-grained maintenance
    /// isn't possible (e.g., compound clauses).
    pub fn invalidate_filter_field(&mut self, field: &str) {
        for (key, entry) in self.entries.iter_mut() {
            if key.filter_clauses.iter().any(|c| c.field == field) {
                entry.mark_for_rebuild();
            }
        }
    }

    // ── Time Bucket Diff Integration (Phase 4) ─────────────────────────────

    /// Maintain cache entries when a time bucket is rebuilt.
    ///
    /// `field` is the bucket field (e.g., "sortAt").
    /// `bucket_name` is the bucket name (e.g., "7d").
    /// `dropped_slots` contains slots that fell out of the bucket (old ANDNOT new).
    /// `added_slots` contains slots that entered the bucket (new ANDNOT old).
    ///
    /// Called by the flush thread after swapping in a rebuilt bucket bitmap.
    pub fn maintain_bucket_changes(
        &mut self,
        field: &str,
        bucket_name: &str,
        dropped_slots: &RoaringBitmap,
        added_slots: &RoaringBitmap,
        filters: &FilterIndex,
        sorts: &SortIndex,
    ) {
        if dropped_slots.is_empty() && added_slots.is_empty() {
            return;
        }

        for (key, entry) in self.entries.iter_mut() {
            if entry.needs_rebuild {
                continue;
            }

            // Check if this entry has a bucket clause matching this bucket
            let has_bucket = key.filter_clauses.iter().any(|c| {
                c.field == field && c.op == "bucket" && c.value_repr == bucket_name
            });

            if !has_bucket {
                continue;
            }

            // Remove dropped slots
            if !dropped_slots.is_empty() {
                let bm = Arc::make_mut(&mut entry.bitmap);
                *bm -= dropped_slots;
                // Also remove from radix (blind — no sort values for bulk drop)
                if let Some(ref mut radix) = entry.radix {
                    let r = Arc::make_mut(radix);
                    for slot in dropped_slots.iter() {
                        r.remove_blind(slot);
                    }
                }
            }

            // Add qualifying new slots
            if !added_slots.is_empty() {
                for slot in added_slots.iter() {
                    // Check all OTHER clauses (we already know bucket matches)
                    let other_clauses_match = key.filter_clauses.iter().all(|c| {
                        if c.field == field && c.op == "bucket" && c.value_repr == bucket_name {
                            true // skip the bucket clause itself
                        } else {
                            slot_matches_clause(slot, c, filters, sorts)
                        }
                    });

                    if !other_clauses_match {
                        continue;
                    }

                    let sort_value = sorts
                        .get_field(&key.sort_field)
                        .map(|f| f.reconstruct_value(slot))
                        .unwrap_or(0);

                    if entry.sort_qualifies(sort_value, key.direction) {
                        entry.add_slot(slot, sort_value);
                    }
                }
            }
        }
    }
}

// ── Filter Evaluation ──────────────────────────────────────────────────────

/// Evaluate whether a slot matches ALL clauses in a filter predicate.
///
/// Uses contains() checks on the filter index bitmaps for Eq/NotEq/In/NotIn.
/// Uses sort index reconstruct_value() for range clauses (Gte/Gt/Lt/Lte).
/// Bucket and compound clauses conservatively return true (handled by rebuild).
fn slot_matches_filter(
    slot: u32,
    clauses: &[CanonicalClause],
    filters: &FilterIndex,
    sorts: &SortIndex,
) -> bool {
    clauses.iter().all(|clause| slot_matches_clause(slot, clause, filters, sorts))
}

/// Evaluate whether a slot matches a single canonical clause.
fn slot_matches_clause(
    slot: u32,
    clause: &CanonicalClause,
    filters: &FilterIndex,
    sorts: &SortIndex,
) -> bool {
    match clause.op.as_str() {
        "eq" => {
            let value = match clause.value_repr.parse::<u64>() {
                Ok(v) => v,
                Err(_) => return true, // Can't evaluate — conservative
            };
            filters
                .get_field(&clause.field)
                .and_then(|f| f.get_versioned(value))
                .map(|vb| vb.contains(slot))
                .unwrap_or(false)
        }
        "neq" => {
            let value = match clause.value_repr.parse::<u64>() {
                Ok(v) => v,
                Err(_) => return true,
            };
            let contained = filters
                .get_field(&clause.field)
                .and_then(|f| f.get_versioned(value))
                .map(|vb| vb.contains(slot))
                .unwrap_or(false);
            !contained
        }
        "in" => {
            clause.value_repr.split(',').any(|v_str| {
                if let Ok(value) = v_str.parse::<u64>() {
                    filters
                        .get_field(&clause.field)
                        .and_then(|f| f.get_versioned(value))
                        .map(|vb| vb.contains(slot))
                        .unwrap_or(false)
                } else {
                    false
                }
            })
        }
        "notin" => {
            clause.value_repr.split(',').all(|v_str| {
                if let Ok(value) = v_str.parse::<u64>() {
                    let contained = filters
                        .get_field(&clause.field)
                        .and_then(|f| f.get_versioned(value))
                        .map(|vb| vb.contains(slot))
                        .unwrap_or(false);
                    !contained
                } else {
                    true
                }
            })
        }
        "gte" | "gt" | "lt" | "lte" => {
            // Range clauses: use sort index to get the slot's actual value
            let threshold = match clause.value_repr.parse::<u64>() {
                Ok(v) => v,
                Err(_) => return true, // Can't evaluate
            };
            // Try sort index first (range fields are typically sort fields)
            let slot_value = sorts
                .get_field(&clause.field)
                .map(|f| f.reconstruct_value(slot) as u64);

            match slot_value {
                Some(v) => match clause.op.as_str() {
                    "gte" => v >= threshold,
                    "gt" => v > threshold,
                    "lt" => v < threshold,
                    "lte" => v <= threshold,
                    _ => unreachable!(),
                },
                None => true, // Field not in sort index — conservative
            }
        }
        "bucket" => {
            // BucketBitmap — requires access to time bucket manager.
            // Phase 4 will add proper evaluation. Conservative: return true.
            true
        }
        op if op.starts_with("not(") => {
            // Compound not: "not(eq)" → evaluate inner and negate
            let inner_op = &op[4..op.len() - 1]; // strip "not(" and ")"
            // If inner is a compound clause (and/or), we can't evaluate it precisely.
            // The inner returns true conservatively, negating gives false — wrong.
            // Return true (conservative) for compound negations.
            if inner_op == "and" || inner_op == "or" {
                return true;
            }
            let inner_clause = CanonicalClause {
                field: clause.field.clone(),
                op: inner_op.to_string(),
                value_repr: clause.value_repr.clone(),
            };
            !slot_matches_clause(slot, &inner_clause, filters, sorts)
        }
        "and" | "or" => {
            // Compound And/Or — would need to parse sub-clauses from value_repr.
            // Conservative: return true (slot might match).
            // These entries will rely on bloat control for correctness.
            true
        }
        _ => true, // Unknown op — conservative
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;

    fn make_key(filters: &[(&str, &str, &str)], sort: &str, dir: SortDirection) -> UnifiedKey {
        UnifiedKey {
            filter_clauses: filters
                .iter()
                .map(|(f, o, v)| CanonicalClause {
                    field: f.to_string(),
                    op: o.to_string(),
                    value_repr: v.to_string(),
                })
                .collect(),
            sort_field: sort.to_string(),
            direction: dir,
        }
    }

    fn make_config() -> UnifiedCacheConfig {
        UnifiedCacheConfig {
            max_entries: 5,
            max_bytes: 1024 * 1024, // 1 MB — generous for tests
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 100,
            ..Default::default()
        }
    }

    #[test]
    fn test_store_and_exact_hit() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.lookup(&key).unwrap();
        assert_eq!(entry.cardinality(), 50);
        assert!(entry.has_more());
    }

    #[test]
    fn test_miss_returns_none() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        assert!(cache.lookup(&key).is_none());
    }

    #[test]
    fn test_different_sort_different_entry() {
        let mut cache = UnifiedCache::new(make_config());
        let key1 = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let key2 = make_key(&[("nsfwLevel", "eq", "1")], "sortAt", SortDirection::Desc);

        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key1.clone(), &slots, true, 100_000, |s| 1000 - s);
        cache.form_and_store(key2.clone(), &slots, true, 100_000, |s| s);

        assert!(cache.lookup(&key1).is_some());
        assert!(cache.lookup(&key2).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_different_direction_different_entry() {
        let mut cache = UnifiedCache::new(make_config());
        let key_desc = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let key_asc = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Asc);

        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key_desc.clone(), &slots, true, 100_000, |s| 1000 - s);
        cache.form_and_store(key_asc.clone(), &slots, false, 100_000, |s| s);

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_eviction_at_capacity() {
        let mut cache = UnifiedCache::new(make_config()); // max_entries = 5
        let slots: Vec<u32> = (0..10).collect();

        // Fill to capacity
        for i in 0..5 {
            let key = make_key(
                &[("field", "eq", &i.to_string())],
                "sort",
                SortDirection::Desc,
            );
            cache.form_and_store(key, &slots, true, 100_000, |s| s);
        }
        assert_eq!(cache.len(), 5);

        // Touch entries 1-4 to make entry 0 the LRU
        for i in 1..5 {
            let key = make_key(
                &[("field", "eq", &i.to_string())],
                "sort",
                SortDirection::Desc,
            );
            cache.lookup(&key);
        }

        // Add one more — should evict entry 0 (LRU)
        let new_key = make_key(&[("field", "eq", "5")], "sort", SortDirection::Desc);
        cache.form_and_store(new_key, &slots, true, 100_000, |s| s);

        assert_eq!(cache.len(), 5);
        let evicted_key = make_key(&[("field", "eq", "0")], "sort", SortDirection::Desc);
        assert!(cache.lookup(&evicted_key).is_none());
    }

    #[test]
    fn test_entry_formation_at_initial_capacity() {
        let config = UnifiedCacheConfig {
            initial_capacity: 10,
            max_capacity: 100,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        // Provide 50 slots but capacity is 10
        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.lookup(&key).unwrap();
        assert_eq!(entry.cardinality(), 10); // only initial_capacity slots
        assert_eq!(entry.capacity(), 10);
        assert!(entry.has_more());
    }

    #[test]
    fn test_dynamic_expansion() {
        let config = UnifiedCacheConfig {
            initial_capacity: 10,
            max_capacity: 80,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        // Initial formation with 10 slots
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get_mut(&key).unwrap();
        assert_eq!(entry.capacity(), 10);

        // Expand — jumps straight to max_capacity (80)
        let new_slots: Vec<u32> = (10..80).collect();
        let new_cap = entry.expand(&new_slots, |s| 1000 - s);
        assert_eq!(new_cap, 80); // jumped to max
        assert_eq!(entry.cardinality(), 80);
        assert_eq!(entry.capacity(), 80);
    }

    #[test]
    fn test_expansion_stops_at_max_capacity() {
        let config = UnifiedCacheConfig {
            initial_capacity: 10,
            max_capacity: 20,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get_mut(&key).unwrap();

        // First expansion: 10 -> 20 (jumps to max)
        let new_slots: Vec<u32> = (10..20).collect();
        let new_cap = entry.expand(&new_slots, |s| 1000 - s);
        assert_eq!(new_cap, 20); // jumped to max_capacity

        // Another expansion attempt: stays at max
        let new_slots: Vec<u32> = (20..30).collect();
        let new_cap = entry.expand(&new_slots, |s| 1000 - s);
        assert_eq!(new_cap, 20); // still at max
    }

    #[test]
    fn test_has_more_set_false_on_partial_expansion() {
        let config = UnifiedCacheConfig {
            initial_capacity: 100,
            max_capacity: 1600,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..100).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get_mut(&key).unwrap();
        assert!(entry.has_more());

        // Expand with fewer slots than expected chunk size (jumps to max 1600, chunk = 1500)
        // But we only provide 30 — means we've exhausted the result set
        let partial_slots: Vec<u32> = (100..130).collect();
        entry.expand(&partial_slots, |s| 1000 - s);
        assert!(!entry.has_more()); // exhausted
    }

    #[test]
    fn test_bloat_control_flags_rebuild() {
        let config = UnifiedCacheConfig {
            initial_capacity: 10,
            max_capacity: 100,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get_mut(&key).unwrap();
        assert!(!entry.needs_rebuild());

        // Add slots until bloat threshold (2 * capacity = 20)
        for i in 10..21u32 {
            entry.add_slot(i, 1000 - i);
        }
        assert!(entry.needs_rebuild());
    }

    #[test]
    fn test_sort_qualification_desc() {
        let config = make_config();
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        // Slots with values: 0->1000, 1->999, ..., 49->951
        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get(&key).unwrap();
        // min_tracked_value = value of last slot = 1000 - 49 = 951
        assert_eq!(entry.min_tracked_value(), 951);

        // Value 960 > 951 -> qualifies for Desc
        assert!(entry.sort_qualifies(960, SortDirection::Desc));
        // Value 950 < 951 -> does not qualify
        assert!(!entry.sort_qualifies(950, SortDirection::Desc));
    }

    #[test]
    fn test_sort_qualification_asc() {
        let config = make_config();
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "sortAt", SortDirection::Asc);

        // Slots with ascending values: 0->0, 1->1, ..., 49->49
        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| s);

        let entry = cache.get(&key).unwrap();
        // min_tracked_value = value of last slot = 49
        assert_eq!(entry.min_tracked_value(), 49);

        // Value 30 < 49 -> qualifies for Asc
        assert!(entry.sort_qualifies(30, SortDirection::Asc));
        // Value 50 > 49 -> does not qualify
        assert!(!entry.sort_qualifies(50, SortDirection::Asc));
    }

    #[test]
    fn test_rebuild_clears_flag() {
        let config = UnifiedCacheConfig {
            initial_capacity: 10,
            max_capacity: 100,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get_mut(&key).unwrap();
        entry.mark_for_rebuild();
        assert!(entry.needs_rebuild());

        let fresh_slots: Vec<u32> = (0..10).collect();
        entry.rebuild(&fresh_slots, |s| 1000 - s);
        assert!(!entry.needs_rebuild());
    }

    #[test]
    fn test_rebuild_guard() {
        let config = make_config();
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get_mut(&key).unwrap();
        assert!(entry.try_start_rebuild()); // first caller gets it
        assert!(!entry.try_start_rebuild()); // second caller blocked

        // Rebuild releases the guard
        let fresh_slots: Vec<u32> = (0..10).collect();
        entry.rebuild(&fresh_slots, |s| 1000 - s);
        assert!(entry.try_start_rebuild()); // available again
    }

    #[test]
    fn test_clear() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key, &slots, true, 100_000, |s| s);

        assert_eq!(cache.len(), 1);
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_overwrite_existing_entry() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots1: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots1, true, 100_000, |s| 1000 - s);

        let slots2: Vec<u32> = (100..120).collect();
        cache.form_and_store(key.clone(), &slots2, false, 100_000, |s| 2000 - s);

        assert_eq!(cache.len(), 1); // no duplicates
        let entry = cache.get(&key).unwrap();
        assert_eq!(entry.cardinality(), 20);
        assert!(!entry.has_more());
    }

    #[test]
    fn test_meta_index_registration() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(
            &[("nsfwLevel", "eq", "1"), ("type", "eq", "image")],
            "reactionCount",
            SortDirection::Desc,
        );

        let slots: Vec<u32> = (0..10).collect();
        let meta_id = cache.form_and_store(key, &slots, true, 100_000, |s| s);

        // Meta-index should have entries for both filter fields
        let nsfw_entries = cache.meta().entries_for_filter_field("nsfwLevel");
        assert!(nsfw_entries.is_some());
        assert!(nsfw_entries.unwrap().contains(meta_id));

        let type_entries = cache.meta().entries_for_filter_field("type");
        assert!(type_entries.is_some());
        assert!(type_entries.unwrap().contains(meta_id));

        // And for the sort field
        let sort_entries = cache.meta().entries_for_sort_field("reactionCount");
        assert!(sort_entries.contains(meta_id));
    }

    #[test]
    fn test_eviction_deregisters_from_meta() {
        let config = UnifiedCacheConfig {
            max_entries: 2,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let slots: Vec<u32> = (0..10).collect();

        // Add two entries
        let key1 = make_key(&[("field", "eq", "1")], "sort", SortDirection::Desc);
        let meta_id_1 = cache.form_and_store(key1.clone(), &slots, true, 100_000, |s| s);

        let key2 = make_key(&[("field", "eq", "2")], "sort", SortDirection::Desc);
        cache.form_and_store(key2.clone(), &slots, true, 100_000, |s| s);

        // Touch key2 to make key1 the LRU
        cache.lookup(&key2);

        // Add third — evicts key1
        let key3 = make_key(&[("field", "eq", "3")], "sort", SortDirection::Desc);
        cache.form_and_store(key3, &slots, true, 100_000, |s| s);

        // meta_id_1 should no longer be in the meta-index
        let entries = cache.meta().entries_for_clause("field", "eq", "1");
        let contains = entries.map(|bm| bm.contains(meta_id_1)).unwrap_or(false);
        assert!(!contains);
    }

    #[test]
    fn test_cold_entry_stays_small() {
        let config = UnifiedCacheConfig {
            initial_capacity: 10,
            max_capacity: 160,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Without any expansion, capacity stays at initial
        let entry = cache.get(&key).unwrap();
        assert_eq!(entry.capacity(), 10);
        assert_eq!(entry.cardinality(), 10);
    }

    #[test]
    fn test_empty_formation() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        cache.form_and_store(key.clone(), &[], false, 0, |_| 0);

        let entry = cache.get(&key).unwrap();
        assert_eq!(entry.cardinality(), 0);
        assert!(!entry.has_more());
        assert_eq!(entry.min_tracked_value(), 0);
    }

    #[test]
    fn test_add_and_remove_slot() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get_mut(&key).unwrap();
        assert_eq!(entry.cardinality(), 10);

        entry.add_slot(100, 900);
        assert_eq!(entry.cardinality(), 11);
        assert!(entry.bitmap().contains(100));

        entry.remove_slot(100, 900);
        assert_eq!(entry.cardinality(), 10);
        assert!(!entry.bitmap().contains(100));
    }

    #[test]
    fn test_meta_index_all_clause_types() {
        let mut cache = UnifiedCache::new(make_config());

        // Register entry with diverse clause types: eq, noteq, gte, in, and compound
        let key = UnifiedKey {
            filter_clauses: vec![
                CanonicalClause {
                    field: "nsfwLevel".to_string(),
                    op: "noteq".to_string(),
                    value_repr: "5".to_string(),
                },
                CanonicalClause {
                    field: "reactionCount".to_string(),
                    op: "gte".to_string(),
                    value_repr: "100".to_string(),
                },
                CanonicalClause {
                    field: "tagIds".to_string(),
                    op: "in".to_string(),
                    value_repr: "[4,8,15]".to_string(),
                },
            ],
            sort_field: "sortAt".to_string(),
            direction: SortDirection::Desc,
        };

        let slots: Vec<u32> = (0..10).collect();
        let meta_id = cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // All three filter fields should be in field-level index
        assert!(cache.meta().entries_for_filter_field("nsfwLevel").unwrap().contains(meta_id));
        assert!(cache.meta().entries_for_filter_field("reactionCount").unwrap().contains(meta_id));
        assert!(cache.meta().entries_for_filter_field("tagIds").unwrap().contains(meta_id));

        // Each specific clause should be findable
        assert!(cache.meta().entries_for_clause("nsfwLevel", "noteq", "5").unwrap().contains(meta_id));
        assert!(cache.meta().entries_for_clause("reactionCount", "gte", "100").unwrap().contains(meta_id));
        assert!(cache.meta().entries_for_clause("tagIds", "in", "[4,8,15]").unwrap().contains(meta_id));

        // Sort field
        assert!(cache.meta().entries_for_sort_field("sortAt").contains(meta_id));

        // find_matching_entries should find this entry with the exact clauses
        let matches = cache.meta().find_matching_entries(
            &key.filter_clauses,
            Some("sortAt"),
            Some(SortDirection::Desc),
        );
        assert!(matches.contains(meta_id));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_meta_index_range_and_lt_clauses() {
        let mut cache = UnifiedCache::new(make_config());

        let key = UnifiedKey {
            filter_clauses: vec![
                CanonicalClause {
                    field: "sortAt".to_string(),
                    op: "gte".to_string(),
                    value_repr: "1700000000".to_string(),
                },
                CanonicalClause {
                    field: "sortAt".to_string(),
                    op: "lt".to_string(),
                    value_repr: "1710000000".to_string(),
                },
            ],
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };

        let slots: Vec<u32> = (0..10).collect();
        let meta_id = cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Both range clauses should be registered
        assert!(cache.meta().entries_for_clause("sortAt", "gte", "1700000000").unwrap().contains(meta_id));
        assert!(cache.meta().entries_for_clause("sortAt", "lt", "1710000000").unwrap().contains(meta_id));

        // Field-level: only "sortAt" as filter field (deduplicated)
        let field_entries = cache.meta().entries_for_filter_field("sortAt").unwrap();
        assert_eq!(field_entries.len(), 1);
        assert!(field_entries.contains(meta_id));
    }

    #[test]
    fn test_min_tracked_value_after_expansion() {
        let config = UnifiedCacheConfig {
            initial_capacity: 5,
            max_capacity: 100,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        // Values: slot 0 -> 1000, slot 1 -> 999, ..., slot 4 -> 996
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get(&key).unwrap();
        assert_eq!(entry.min_tracked_value(), 996); // 1000 - 4

        // Expand with slots 5-9, values 995-991
        let entry = cache.get_mut(&key).unwrap();
        let new_slots: Vec<u32> = (5..10).collect();
        entry.expand(&new_slots, |s| 1000 - s);

        assert_eq!(entry.min_tracked_value(), 991); // 1000 - 9
    }

    #[test]
    fn test_radix_built_on_expand() {
        let config = UnifiedCacheConfig {
            initial_capacity: 5,
            max_capacity: 100,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        let entry = cache.get(&key).unwrap();
        assert!(entry.radix().is_none(), "no radix at initial capacity");

        // Expand
        let entry = cache.get_mut(&key).unwrap();
        let new_slots: Vec<u32> = (5..100).collect();
        entry.expand(&new_slots, |s| 1000 - s);
        assert!(entry.radix().is_some(), "radix should be built on expand");

        // Verify radix has all slots
        let radix = entry.radix().unwrap();
        assert_eq!(radix.total_slots(), 100);
    }

    #[test]
    fn test_radix_maintained_on_add_remove() {
        let config = UnifiedCacheConfig {
            initial_capacity: 5,
            max_capacity: 20,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Expand to build radix
        let entry = cache.get_mut(&key).unwrap();
        let new_slots: Vec<u32> = (5..20).collect();
        entry.expand(&new_slots, |s| 1000 - s);
        assert_eq!(entry.radix().unwrap().total_slots(), 20);

        // Add a slot — should appear in both bitmap and radix
        entry.add_slot(100, 500);
        assert!(entry.bitmap().contains(100));
        // Radix total should increase (after rebuild_counts)
        let radix = entry.radix().unwrap();
        assert!(radix.is_dirty()); // dirty from insert

        // Remove a slot
        entry.remove_slot(100, 500);
        assert!(!entry.bitmap().contains(100));
    }

    #[test]
    fn test_radix_rebuilt_on_rebuild() {
        let config = UnifiedCacheConfig {
            initial_capacity: 5,
            max_capacity: 10,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);

        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Expand to max capacity
        let entry = cache.get_mut(&key).unwrap();
        let new_slots: Vec<u32> = (5..10).collect();
        entry.expand(&new_slots, |s| 1000 - s);
        assert!(entry.radix().is_some());

        // Rebuild — should rebuild radix at expanded capacity
        let new_slots: Vec<u32> = (0..8).collect();
        entry.rebuild(&new_slots, |s| 1000 - s);
        assert!(entry.radix().is_some(), "radix should be rebuilt at expanded capacity");
        assert_eq!(entry.radix().unwrap().total_slots(), 8);
    }

    // ── Maintenance Tests ──────────────────────────────────────────────────

    /// Helper: create a FilterIndex with a field and set some slots for a value.
    fn make_filter_index(fields: &[(&str, &[(u64, &[u32])])]) -> FilterIndex {
        let mut fi = FilterIndex::new();
        for (name, values) in fields {
            fi.add_field(FilterFieldConfig {
                name: name.to_string(),
                field_type: FilterFieldType::SingleValue,
                behaviors: None,
                eviction: None,
                eager_load: false,
            });
            let field = fi.get_field_mut(name).unwrap();
            for (value, slots) in *values {
                field.insert_bulk(*value, slots.iter().copied());
            }
        }
        fi
    }

    /// Helper: create a SortIndex with a field and set sort values for slots.
    fn make_sort_index(fields: &[(&str, &[(u32, u32)])]) -> SortIndex {
        let mut si = SortIndex::new();
        for (name, slot_values) in fields {
            si.add_field(SortFieldConfig {
                name: name.to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
            });
            let field = si.get_field_mut(name).unwrap();
            for &(slot, value) in *slot_values {
                // Decompose value into bit layers
                for bit in 0..32 {
                    if value & (1 << bit) != 0 {
                        field.set_layer_bulk(bit, std::iter::once(slot));
                    }
                }
            }
            field.merge_dirty();
        }
        si
    }

    #[test]
    fn test_maintain_filter_insert_adds_qualifying_slot() {
        let mut cache = UnifiedCache::new(make_config());

        // Entry: Eq(nsfwLevel, 1), sort by reactionCount Desc
        // Initial slots 0..5, sort values: 0->1000, 1->999, ...
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        assert_eq!(cache.get(&key).unwrap().cardinality(), 5);

        // Slot 10 now has nsfwLevel=1 (just inserted) and reactionCount=1500 (qualifies for Desc)
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 2, 3, 4, 10])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);

        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10],
        );

        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);

        let entry = cache.get(&key).unwrap();
        assert!(entry.bitmap().contains(10));
        assert_eq!(entry.cardinality(), 6);
    }

    #[test]
    fn test_maintain_filter_remove_removes_slot() {
        let mut cache = UnifiedCache::new(make_config());

        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Slot 2 removed from nsfwLevel=1 (no longer matches Eq(nsfwLevel, 1))
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 3, 4])])]);
        let sorts = make_sort_index(&[("reactionCount", &[])]);

        let mut removes = HashMap::new();
        removes.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![2],
        );

        cache.maintain_filter_changes(&HashMap::new(), &removes, &filters, &sorts);

        let entry = cache.get(&key).unwrap();
        assert!(!entry.bitmap().contains(2));
        assert_eq!(entry.cardinality(), 4);
    }

    #[test]
    fn test_maintain_filter_does_not_add_sort_unqualified() {
        let mut cache = UnifiedCache::new(make_config());

        // Entry with min_tracked_value = 951 (Desc, slot 49 has value 951)
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        assert_eq!(cache.get(&key).unwrap().min_tracked_value(), 951);

        // Slot 100 matches filter but has reactionCount=500 (below 951 threshold)
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[100])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(100, 500)])]);

        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![100],
        );

        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);

        // Slot 100 should NOT have been added (sort value doesn't qualify)
        assert!(!cache.get(&key).unwrap().bitmap().contains(100));
    }

    #[test]
    fn test_maintain_filter_multi_clause_entry() {
        let mut cache = UnifiedCache::new(make_config());

        // Entry: Eq(nsfwLevel, 1) AND Eq(type, 2)
        let key = make_key(
            &[("nsfwLevel", "eq", "1"), ("type", "eq", "2")],
            "reactionCount",
            SortDirection::Desc,
        );
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Slot 10: has nsfwLevel=1 but NOT type=2
        let filters = make_filter_index(&[
            ("nsfwLevel", &[(1, &[0, 1, 2, 3, 4, 10])]),
            ("type", &[(2, &[0, 1, 2, 3, 4])]), // slot 10 NOT in type=2
        ]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);

        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10],
        );

        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);

        // Slot 10 should NOT be added (fails type=2 check)
        assert!(!cache.get(&key).unwrap().bitmap().contains(10));
    }

    #[test]
    fn test_maintain_filter_noteq_clause() {
        let mut cache = UnifiedCache::new(make_config());

        // Entry: NotEq(nsfwLevel, 5), sort by reactionCount Desc
        let key = make_key(&[("nsfwLevel", "neq", "5")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Slot 10 now has nsfwLevel=5 (should be excluded by NotEq)
        let filters = make_filter_index(&[("nsfwLevel", &[(5, &[10])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);

        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 5 },
            vec![10],
        );

        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);

        // Slot 10 should NOT be added (excluded by NotEq)
        assert!(!cache.get(&key).unwrap().bitmap().contains(10));
    }

    #[test]
    fn test_maintain_sort_adds_qualifying_slot() {
        let mut cache = UnifiedCache::new(make_config());

        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        // min_tracked_value = 951

        // Slot 100 already matches nsfwLevel=1, sort value now updated to 1500
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[100])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(100, 1500)])]);

        let mut sort_mutations: HashMap<&str, HashSet<u32>> = HashMap::new();
        sort_mutations.insert("reactionCount", [100].into());

        cache.maintain_sort_changes(&sort_mutations, &filters, &sorts);

        assert!(cache.get(&key).unwrap().bitmap().contains(100));
    }

    #[test]
    fn test_maintain_sort_skips_filter_nonmatch() {
        let mut cache = UnifiedCache::new(make_config());

        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..50).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Slot 100 does NOT match nsfwLevel=1 but has good sort value
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[])])]); // slot 100 not in nsfwLevel=1
        let sorts = make_sort_index(&[("reactionCount", &[(100, 1500)])]);

        let mut sort_mutations: HashMap<&str, HashSet<u32>> = HashMap::new();
        sort_mutations.insert("reactionCount", [100].into());

        cache.maintain_sort_changes(&sort_mutations, &filters, &sorts);

        assert!(!cache.get(&key).unwrap().bitmap().contains(100));
    }

    #[test]
    fn test_maintain_alive_marks_all_for_rebuild() {
        let mut cache = UnifiedCache::new(make_config());

        let key1 = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let key2 = make_key(&[("type", "eq", "2")], "sortAt", SortDirection::Desc);
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key1.clone(), &slots, true, 100_000, |s| s);
        cache.form_and_store(key2.clone(), &slots, true, 100_000, |s| s);

        assert!(!cache.get(&key1).unwrap().needs_rebuild());
        assert!(!cache.get(&key2).unwrap().needs_rebuild());

        cache.maintain_alive_changes();

        assert!(cache.get(&key1).unwrap().needs_rebuild());
        assert!(cache.get(&key2).unwrap().needs_rebuild());
    }

    #[test]
    fn test_maintain_skips_entries_needing_rebuild() {
        let mut cache = UnifiedCache::new(make_config());

        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);

        // Mark for rebuild
        cache.get_mut(&key).unwrap().mark_for_rebuild();

        // Try to add a qualifying slot — should be skipped
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[10])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);

        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10],
        );

        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);

        // Slot 10 NOT added because entry needs rebuild
        assert!(!cache.get(&key).unwrap().bitmap().contains(10));
    }

    #[test]
    fn test_maintain_bucket_drops_expired_slots() {
        let mut cache = UnifiedCache::new(make_config());

        // Entry with bucket clause: bucket(sortAt, "7d")
        let key = UnifiedKey {
            filter_clauses: vec![
                CanonicalClause {
                    field: "sortAt".to_string(),
                    op: "bucket".to_string(),
                    value_repr: "7d".to_string(),
                },
                CanonicalClause {
                    field: "nsfwLevel".to_string(),
                    op: "eq".to_string(),
                    value_repr: "1".to_string(),
                },
            ],
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        assert_eq!(cache.get(&key).unwrap().cardinality(), 10);

        // Bucket rebuild: slots 0, 1, 2 dropped out of the 7d window
        let mut dropped = RoaringBitmap::new();
        dropped.insert(0);
        dropped.insert(1);
        dropped.insert(2);

        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[])])]);
        let sorts = make_sort_index(&[("reactionCount", &[])]);

        cache.maintain_bucket_changes("sortAt", "7d", &dropped, &RoaringBitmap::new(), &filters, &sorts);

        let entry = cache.get(&key).unwrap();
        assert_eq!(entry.cardinality(), 7);
        assert!(!entry.bitmap().contains(0));
        assert!(!entry.bitmap().contains(1));
        assert!(!entry.bitmap().contains(2));
        assert!(entry.bitmap().contains(3));
    }

    #[test]
    fn test_maintain_bucket_adds_qualifying_new_slots() {
        let mut cache = UnifiedCache::new(make_config());

        let key = UnifiedKey {
            filter_clauses: vec![
                CanonicalClause {
                    field: "sortAt".to_string(),
                    op: "bucket".to_string(),
                    value_repr: "7d".to_string(),
                },
                CanonicalClause {
                    field: "nsfwLevel".to_string(),
                    op: "eq".to_string(),
                    value_repr: "1".to_string(),
                },
            ],
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        // min_tracked_value = 996

        // Slot 100 enters the bucket and matches nsfwLevel=1 with reactionCount=1500
        let mut added = RoaringBitmap::new();
        added.insert(100);

        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[100])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(100, 1500)])]);

        cache.maintain_bucket_changes("sortAt", "7d", &RoaringBitmap::new(), &added, &filters, &sorts);

        assert!(cache.get(&key).unwrap().bitmap().contains(100));
    }

    #[test]
    fn test_maintain_unaffected_entry_untouched() {
        let mut cache = UnifiedCache::new(make_config());

        // Entry on field "type", not "nsfwLevel"
        let key = make_key(&[("type", "eq", "2")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        let orig_cardinality = cache.get(&key).unwrap().cardinality();

        // Mutation only on "nsfwLevel" — should not affect "type" entry
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[10])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);

        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10],
        );

        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);

        assert_eq!(cache.get(&key).unwrap().cardinality(), orig_cardinality);
    }

    // --- Compound clause live maintenance tests ---

    #[test]
    fn test_slot_matches_clause_or_returns_true_conservatively() {
        // Or(...) should return true (conservative) since we can't evaluate sub-clauses
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let clause = CanonicalClause {
            field: "nsfwLevel".to_string(),
            op: "or".to_string(),
            value_repr: "".to_string(),
        };
        assert!(
            slot_matches_clause(42, &clause, &filters, &sorts),
            "Or clause should conservatively return true"
        );
    }

    #[test]
    fn test_slot_matches_clause_and_returns_true_conservatively() {
        // And(...) should return true (conservative)
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let clause = CanonicalClause {
            field: "nsfwLevel".to_string(),
            op: "and".to_string(),
            value_repr: "".to_string(),
        };
        assert!(
            slot_matches_clause(42, &clause, &filters, &sorts),
            "And clause should conservatively return true"
        );
    }

    #[test]
    fn test_slot_matches_clause_not_and_returns_true_conservatively() {
        // not(and) should return true (conservative).
        // Bug: inner "and" returns true, negation gives false — incorrectly rejects slots.
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let clause = CanonicalClause {
            field: "nsfwLevel".to_string(),
            op: "not(and)".to_string(),
            value_repr: "".to_string(),
        };
        assert!(
            slot_matches_clause(42, &clause, &filters, &sorts),
            "Not(And(...)) should conservatively return true, not negate the inner conservative true"
        );
    }

    #[test]
    fn test_slot_matches_clause_not_or_returns_true_conservatively() {
        // not(or) should return true (conservative).
        // Bug: inner "or" returns true, negation gives false — incorrectly rejects slots.
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let clause = CanonicalClause {
            field: "nsfwLevel".to_string(),
            op: "not(or)".to_string(),
            value_repr: "".to_string(),
        };
        assert!(
            slot_matches_clause(42, &clause, &filters, &sorts),
            "Not(Or(...)) should conservatively return true, not negate the inner conservative true"
        );
    }

    #[test]
    fn test_slot_matches_filter_with_not_and_clause() {
        // A filter with a Not(And(...)) clause should not reject slots
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[42])])]);
        let sorts = make_sort_index(&[]);
        let clauses = vec![
            CanonicalClause {
                field: "nsfwLevel".to_string(),
                op: "eq".to_string(),
                value_repr: "1".to_string(),
            },
            CanonicalClause {
                field: "type".to_string(),
                op: "not(and)".to_string(),
                value_repr: "".to_string(),
            },
        ];
        assert!(
            slot_matches_filter(42, &clauses, &filters, &sorts),
            "Filter with Not(And(...)) clause should not reject slot that matches other clauses"
        );
    }

    #[test]
    fn test_maintain_not_and_clause_does_not_reject_slot() {
        // E2E: cache entry with Not(And(...)) clause should keep slots during maintenance
        let mut cache = UnifiedCache::new(make_config());

        // Entry with a Not(And(...)) clause
        let key = make_key(
            &[("nsfwLevel", "eq", "1"), ("type", "not(and)", "")],
            "reactionCount",
            SortDirection::Desc,
        );
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        assert_eq!(cache.get(&key).unwrap().cardinality(), 5);

        // Insert slot 10 with nsfwLevel=1
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 2, 3, 4, 10])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);

        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10],
        );

        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);

        // Slot 10 should be added — the Not(And(...)) clause should not reject it
        let entry = cache.get(&key).unwrap();
        assert!(
            entry.bitmap().contains(10),
            "Slot 10 should be added to cache entry with Not(And(...)) clause"
        );
    }
}
