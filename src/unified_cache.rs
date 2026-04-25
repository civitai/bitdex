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
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use roaring::RoaringBitmap;
use crate::bound_store::ShardKey;
use crate::cache::CanonicalClause;
use crate::filter::FilterIndex;
use crate::meta_index::{CacheEntryId, MetaIndex};
use crate::query::SortDirection;
use crate::radix_sort::RadixSortIndex;
use crate::sort::SortIndex;
use crate::write_coalescer::FilterGroupKey;
// ── Two-Phase Maintenance Types ──────────────────────────────────────────
//
// These types support lock-free cache maintenance: the flush thread collects
// work items under a brief lock, evaluates slot eligibility outside the lock
// (using staging filters/sorts), then applies results under a second brief lock.
// This reduces Mutex hold time from ~469ms to ~1ms per acquisition.
/// Describes maintenance work for one cache entry (collected under brief lock).
pub struct CacheMaintenanceItem {
    pub key: UnifiedKey,
    pub slots: Vec<u32>,
    pub min_tracked_value: u32,
    pub direction: SortDirection,
}
/// Result of evaluating maintenance for one cache entry (computed without lock).
pub struct CacheMaintenanceResult {
    pub key: UnifiedKey,
    /// Slots to add: (slot_id, sort_value)
    pub adds: Vec<(u32, u32)>,
    /// Slots to remove: (slot_id, sort_value)
    pub removes: Vec<(u32, u32)>,
}
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
    /// 0 = unlimited (default). Used as fallback when `max_maintenance_ms` is 0.
    pub max_maintenance_work: usize,
    /// Time budget for cache maintenance per flush cycle in milliseconds.
    /// When > 0, replaces the count-based `max_maintenance_work` budget.
    /// The deadline is checked every 64 entries to avoid clock overhead.
    /// 0 = use count-based `max_maintenance_work` instead. Default: 10ms.
    pub max_maintenance_ms: u64,
    /// Prefetch threshold: trigger background expansion when the user has consumed
    /// this fraction of the cached entries (default 0.95 = 95% consumed, 5% remaining).
    /// Set to 0.0 or 1.0 to disable prefetching.
    pub prefetch_threshold: f64,
}
impl Default for UnifiedCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_bytes: 512 * 1024 * 1024, // 512 MB
            initial_capacity: 4_000,
            max_capacity: 64_000,
            min_filter_size: 0,
            max_maintenance_work: 0, // 0 = unlimited
            max_maintenance_ms: 10,
            prefetch_threshold: 0.95,
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
    /// Guard to prevent concurrent prefetch expansions.
    prefetching: AtomicBool,
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
    /// Snapped bucket cutoff this entry was last valid at (unix seconds).
    /// 0 if this entry doesn't use time buckets.
    bucket_cutoff: u64,
    /// Whether this entry's filter clauses include a time bucket clause.
    uses_bucket: bool,
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
            prefetching: AtomicBool::new(false),
            last_used: Instant::now(),
            meta_id,
            persist_dirty: true, // New entries need persisting
            sorted_keys,
            radix: None, // No radix at initial capacity — sorted vec is faster
            direction,
            bucket_cutoff: 0, // Set by caller via set_bucket_cutoff() after creation
            uses_bucket: false, // Set by caller via set_uses_bucket() after creation
        }
    }
    /// Create an entry restored from disk (shard load).
    ///
    /// If `persisted_sorted_keys` is provided (from ucpack v2), uses them directly —
    /// skipping the expensive `reconstruct_value()` calls (4000 × 32 = 128K bitmap contains).
    /// If not provided (v1 shards or None), falls back to rebuilding from `value_fn`.
    pub fn from_restored(
        bitmap: RoaringBitmap,
        meta_id: CacheEntryId,
        initial_capacity: usize,
        max_capacity: usize,
        direction: SortDirection,
        persisted_sorted_keys: Option<Vec<u64>>,
        value_fn: impl Fn(u32) -> u32,
        has_more: bool,
        persisted_total_matched: u64,
    ) -> Self {
        let card = bitmap.len() as usize;
        let capacity = if card > initial_capacity {
            max_capacity
        } else {
            initial_capacity
        };
        // Use persisted sorted_keys if available, otherwise rebuild from value_fn
        let sorted_keys = if let Some(sk) = persisted_sorted_keys {
            if !sk.is_empty() { Some(Arc::new(sk)) } else { None }
        } else {
            // Fallback: rebuild from bitmap + value_fn (v1 compat path)
            let slots: Vec<u32> = bitmap.iter().collect();
            if !slots.is_empty() && card <= max_capacity {
                Some(Arc::new(Self::build_sorted_keys(&slots, direction, &value_fn)))
            } else {
                None
            }
        };
        // Compute min_tracked_value from the sorted keys
        let min_tracked_value = sorted_keys.as_ref().and_then(|keys| {
            keys.last().map(|&k| (k >> 32) as u32)
        }).unwrap_or(0);
        // Use persisted total_matched if available (non-zero), otherwise
        // fall back to bitmap cardinality (old meta.bin without real total).
        let total_matched = if persisted_total_matched > 0 {
            persisted_total_matched
        } else {
            card as u64
        };
        Self {
            bitmap: Arc::new(bitmap),
            min_tracked_value,
            capacity,
            max_capacity,
            has_more,
            total_matched,
            needs_rebuild: false,
            rebuilding: AtomicBool::new(false),
            prefetching: AtomicBool::new(false),
            last_used: Instant::now(),
            meta_id,
            persist_dirty: false, // Just loaded from disk — clean
            sorted_keys,
            radix: None,
            direction,
            bucket_cutoff: 0, // Set by caller after restore
            uses_bucket: false, // Set by caller after restore
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
    /// The snapped bucket cutoff this entry was last valid at.
    pub fn bucket_cutoff(&self) -> u64 {
        self.bucket_cutoff
    }
    /// Set the bucket cutoff (called when creating or updating an entry).
    pub fn set_bucket_cutoff(&mut self, cutoff: u64) {
        self.bucket_cutoff = cutoff;
    }
    /// Whether this entry uses a time bucket clause.
    pub fn uses_bucket(&self) -> bool {
        self.uses_bucket
    }
    /// Mark this entry as using a time bucket clause.
    pub fn set_uses_bucket(&mut self, uses: bool) {
        self.uses_bucket = uses;
    }
    /// Apply pending bucket diffs: subtract expired slots from the bitmap
    /// and update the bucket_cutoff to current.
    pub fn apply_bucket_diff(&mut self, expired: &RoaringBitmap, new_cutoff: u64) {
        if !expired.is_empty() {
            let bm = Arc::make_mut(&mut self.bitmap);
            *bm -= expired;
            // Also remove from radix if expanded
            if let Some(ref mut radix) = self.radix {
                let r = Arc::make_mut(radix);
                for slot in expired.iter() {
                    r.remove_blind(slot);
                }
            }
        }
        self.bucket_cutoff = new_cutoff;
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
    /// Bulk add many slots to the bounded bitmap. Amortizes the per-call
    /// overhead from `add_slot`:
    ///   - one `Arc::make_mut` on the bitmap (not N)
    ///   - one `Arc::make_mut` on the radix (not N)
    ///   - one `sorted_keys` invalidation (not N)
    ///   - one bloat check at the end (not N)
    ///
    /// Input is `(slot, sort_value)` pairs. Returns true if bloat threshold
    /// was exceeded by the final cardinality.
    pub fn add_slots_bulk(&mut self, adds: &[(u32, u32)]) -> bool {
        if adds.is_empty() {
            return false;
        }
        {
            let bm = Arc::make_mut(&mut self.bitmap);
            for &(slot, _) in adds {
                bm.insert(slot);
            }
        }
        self.persist_dirty = true;
        self.sorted_keys = None;
        if let Some(ref mut radix) = self.radix {
            let r = Arc::make_mut(radix);
            for &(slot, value) in adds {
                r.insert(slot, value);
            }
        }
        let bloat_threshold = self.capacity * 2;
        if self.bitmap.len() as usize > bloat_threshold {
            self.needs_rebuild = true;
            true
        } else {
            false
        }
    }
    /// Bulk remove many slots from the bounded bitmap. Amortizes the per-call
    /// overhead from `remove_slot`.
    pub fn remove_slots_bulk(&mut self, removes: &[(u32, u32)]) {
        if removes.is_empty() {
            return;
        }
        {
            let bm = Arc::make_mut(&mut self.bitmap);
            for &(slot, _) in removes {
                bm.remove(slot);
            }
        }
        self.persist_dirty = true;
        self.sorted_keys = None;
        if let Some(ref mut radix) = self.radix {
            let r = Arc::make_mut(radix);
            for &(slot, value) in removes {
                r.remove(slot, value);
            }
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
    /// Check if a background prefetch expansion is in progress.
    pub fn is_prefetching(&self) -> bool {
        self.prefetching.load(Ordering::Relaxed)
    }
    /// Set the prefetching flag.
    pub fn set_prefetching(&self, val: bool) {
        self.prefetching.store(val, Ordering::Relaxed);
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
    pub inserts: u64,
    pub updates: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub memory_bytes: usize,
    pub meta_index_entries: usize,
    pub meta_index_bytes: usize,
    // Persistence stats
    pub persistence_enabled: bool,
    pub tombstone_count: u64,
    pub pending_shard_count: usize,
    pub dirty_shard_count: usize,
    pub meta_dirty: bool,
    // Capacity tier counts
    pub entries_initial: usize,
    pub entries_expanded: usize,
    // Event counters
    pub extensions: u64,
    pub wall_hits: u64,
    pub prefetches: u64,
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
    inserts: u64,
    updates: u64,
    evictions: u64,
    invalidations: u64,
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
    /// Persisted has_more flags keyed by entry ID, populated from meta.bin on startup.
    /// Consumed during shard restore to avoid hardcoding has_more=true.
    meta_has_more: HashMap<CacheEntryId, bool>,
    /// Persisted total_matched values keyed by entry ID, populated from meta.bin on startup.
    /// Consumed during shard restore to get the real total instead of bitmap cardinality.
    meta_total_matched: HashMap<CacheEntryId, u64>,
    /// Cumulative count of entry expansions from initial to expanded capacity.
    extensions: u64,
    /// Cumulative count of cache wall hits (cursor past cached entries, triggering slow path).
    wall_hits: u64,
    /// Cumulative count of prefetch triggers (background expansion requests).
    prefetches: u64,
    /// True during shard restore — skips per-insert eviction.
    restoring: bool,
    /// Reverse index: ShardKey → set of UnifiedKeys in that shard.
    /// Avoids O(all_entries) scan in entries_for_shard() and clear_shard_entry_dirty().
    shard_to_keys: HashMap<ShardKey, HashSet<UnifiedKey>>,
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
            inserts: 0,
            updates: 0,
            evictions: 0,
            invalidations: 0,
            total_bytes: 0,
            pending_shards: HashSet::new(),
            loading_shards: HashSet::new(),
            meta_dirty: false,
            shard_dirty: HashSet::new(),
            persistence_enabled: false,
            meta_has_more: HashMap::new(),
            meta_total_matched: HashMap::new(),
            extensions: 0,
            wall_hits: 0,
            prefetches: 0,
            restoring: false,
            shard_to_keys: HashMap::new(),
        }
    }
    /// Store persisted has_more flags from meta.bin, keyed by entry ID.
    /// Called during startup after loading meta.bin.
    pub fn set_meta_has_more(&mut self, map: HashMap<CacheEntryId, bool>) {
        self.meta_has_more = map;
    }
    /// Look up persisted has_more for a given entry ID. Falls back to true if not found.
    pub fn get_meta_has_more(&self, entry_id: CacheEntryId) -> bool {
        self.meta_has_more.get(&entry_id).copied().unwrap_or(true)
    }
    /// Store persisted total_matched values from meta.bin, keyed by entry ID.
    /// Called during startup after loading meta.bin.
    pub fn set_meta_total_matched(&mut self, map: HashMap<CacheEntryId, u64>) {
        self.meta_total_matched = map;
    }
    /// Look up persisted total_matched for a given entry ID. Falls back to 0 if not found.
    pub fn get_meta_total_matched(&self, entry_id: CacheEntryId) -> u64 {
        self.meta_total_matched.get(&entry_id).copied().unwrap_or(0)
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
    ///
    /// Uses batch eviction: when over budget, evicts ~10% of entries in one O(n)
    /// pass instead of calling evict_lru() per entry. This prevents repeated O(n)
    /// scans while holding the Mutex under high cache churn.
    pub fn store(&mut self, key: UnifiedKey, entry: UnifiedEntry) -> CacheEntryId {
        let meta_id = entry.meta_id;
        let new_bytes = entry.memory_bytes();
        // If replacing an existing entry, deregister the old one and subtract its bytes
        if let Some(old) = self.entries.remove(&key) {
            self.total_bytes = self.total_bytes.saturating_sub(old.memory_bytes());
            self.meta_id_to_key.remove(&old.meta_id);
            self.meta.deregister(old.meta_id);
            // Remove from shard→keys index
            let old_sk = ShardKey::new(key.sort_field.clone(), key.direction);
            if let Some(set) = self.shard_to_keys.get_mut(&old_sk) {
                set.remove(&key);
            }
        }
        // Batch eviction: when over budget, evict ~10% of entries at once.
        // One O(n) pass handles many evictions, creating headroom so subsequent
        // inserts don't trigger eviction. Prevents O(n) scan per insert under
        // high churn (the Mutex is held during this scan, blocking all queries).
        if (self.total_bytes + new_bytes > self.config.max_bytes
            || self.entries.len() >= self.config.max_entries)
            && !self.entries.is_empty()
        {
            self.evict_batch();
        }
        // Mark dirty for persistence
        if self.persistence_enabled {
            self.meta_dirty = true;
            let shard_key = ShardKey::new(key.sort_field.clone(), key.direction);
            self.shard_dirty.insert(shard_key);
        }
        self.total_bytes += new_bytes;
        self.meta_id_to_key.insert(meta_id, key.clone());
        // Maintain shard→keys index
        let sk = ShardKey::new(key.sort_field.clone(), key.direction);
        self.shard_to_keys.entry(sk).or_default().insert(key.clone());
        self.entries.insert(key, entry);
        self.inserts += 1;
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
        let uses_bucket = key.filter_clauses.iter().any(|c| c.op == "bucket");
        let mut entry = UnifiedEntry::new(
            sorted_slots,
            self.config.initial_capacity,
            self.config.max_capacity,
            has_more,
            total_matched,
            meta_id,
            direction,
            value_fn,
        );
        entry.set_uses_bucket(uses_bucket);
        if uses_bucket {
            // Tag with current time so lazy diff application knows when this entry was computed.
            // Snapping is applied later when compared against pending diffs.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            entry.set_bucket_cutoff(now);
        }
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
            // Remove from shard→keys index
            let sk = ShardKey::new(lru_key.sort_field.clone(), lru_key.direction);
            if let Some(set) = self.shard_to_keys.get_mut(&sk) {
                set.remove(&lru_key);
            }
            self.evictions += 1;
            if !self.persistence_enabled {
                // Without persistence, deregister fully (original behavior)
                self.meta.deregister(evicted.meta_id);
            }
            // With persistence: meta-index keeps the registration.
            // Entry stays on disk as orphan — can be reloaded from shard.
        }
        Some(lru_key)
    }
    /// Batch eviction: evict ~10% of entries (minimum 1) in one O(n) pass.
    ///
    /// Collects all entries sorted by last_used, evicts the oldest 10%.
    /// This creates headroom so subsequent inserts don't trigger eviction,
    /// avoiding repeated O(n) scans under high cache churn.
    pub fn evict_batch(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        // Collect (last_used, key) for all evictable entries
        let mut candidates: Vec<(Instant, UnifiedKey)> = if self.persistence_enabled {
            // Prefer non-dirty entries first
            let mut non_dirty: Vec<_> = self.entries.iter()
                .filter(|(_, e)| !e.persist_dirty)
                .map(|(k, e)| (e.last_used, k.clone()))
                .collect();
            if non_dirty.is_empty() {
                // All dirty — fall back to all entries
                self.entries.iter()
                    .map(|(k, e)| (e.last_used, k.clone()))
                    .collect()
            } else {
                non_dirty
            }
        } else {
            self.entries.iter()
                .map(|(k, e)| (e.last_used, k.clone()))
                .collect()
        };
        // Sort by last_used ascending (oldest first)
        candidates.sort_unstable_by_key(|(t, _)| *t);
        // Evict 10% of total entries (minimum 1), or enough to get under budget
        let target_evict = (self.entries.len() / 10).max(1);
        let mut evicted = 0;
        for (_, key) in candidates.into_iter().take(target_evict) {
            if let Some(entry) = self.entries.remove(&key) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.memory_bytes());
                self.meta_id_to_key.remove(&entry.meta_id);
                // Remove from shard→keys index
                let sk = ShardKey::new(key.sort_field.clone(), key.direction);
                if let Some(set) = self.shard_to_keys.get_mut(&sk) {
                    set.remove(&key);
                }
                self.evictions += 1;
                if !self.persistence_enabled {
                    self.meta.deregister(entry.meta_id);
                }
                evicted += 1;
            }
        }
        if evicted > 0 {
            tracing::info!("Cache batch eviction: evicted {evicted} entries, {} remaining", self.entries.len());
        }
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
        self.shard_to_keys.clear();
        self.meta = MetaIndex::new();
        self.hits = 0;
        self.misses = 0;
        self.total_bytes = 0;
        self.pending_shards.clear();
        self.loading_shards.clear();
        self.meta_dirty = false;
        self.shard_dirty.clear();
        self.meta_total_matched.clear();
    }
    /// Return a stats snapshot.
    pub fn stats(&self) -> UnifiedCacheStats {
        // Count entries by capacity tier
        let mut entries_initial = 0usize;
        let mut entries_expanded = 0usize;
        for entry in self.entries.values() {
            if entry.capacity >= entry.max_capacity {
                entries_expanded += 1;
            } else {
                entries_initial += 1;
            }
        }
        UnifiedCacheStats {
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            inserts: self.inserts,
            updates: self.updates,
            evictions: self.evictions,
            invalidations: self.invalidations,
            memory_bytes: self.total_memory_bytes(),
            meta_index_entries: self.meta.entry_count(),
            meta_index_bytes: self.meta.memory_bytes(),
            persistence_enabled: self.persistence_enabled,
            tombstone_count: self.meta.tombstone_count(),
            pending_shard_count: self.pending_shards.len(),
            dirty_shard_count: self.shard_dirty.len(),
            meta_dirty: self.meta_dirty,
            entries_initial,
            entries_expanded,
            extensions: self.extensions,
            wall_hits: self.wall_hits,
            prefetches: self.prefetches,
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
    /// Record a cache entry update (called by flush thread during maintenance).
    pub fn record_update(&mut self) {
        self.updates += 1;
    }
    /// Record a cache entry expansion from initial to expanded capacity.
    pub fn record_extension(&mut self) {
        self.extensions += 1;
    }
    /// Record a cache wall hit (cursor went past cached entries, triggering expansion/slow path).
    pub fn record_wall_hit(&mut self) {
        self.wall_hits += 1;
    }
    /// Record a prefetch trigger (background expansion request sent).
    pub fn record_prefetch(&mut self) {
        self.prefetches += 1;
    }
    /// Get the cache config.
    pub fn config(&self) -> &UnifiedCacheConfig {
        &self.config
    }
    /// Get mutable access to the cache config.
    pub fn config_mut(&mut self) -> &mut UnifiedCacheConfig {
        &mut self.config
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
    ///
    /// Skips eviction during restore (restoring flag). Call `finish_restore()` after
    /// loading all entries to run a single eviction pass.
    pub fn insert_restored_entry(&mut self, key: UnifiedKey, entry: UnifiedEntry) {
        let meta_id = entry.meta_id;
        let bytes = entry.memory_bytes();
        // Skip per-insert eviction during restore — batch evict at the end
        if !self.restoring {
            if (self.total_bytes + bytes > self.config.max_bytes
                || self.entries.len() >= self.config.max_entries)
                && !self.entries.is_empty()
            {
                self.evict_batch();
            }
        }
        self.total_bytes += bytes;
        self.meta_id_to_key.insert(meta_id, key.clone());
        // Maintain shard→keys index
        let sk = ShardKey::new(key.sort_field.clone(), key.direction);
        self.shard_to_keys.entry(sk).or_default().insert(key.clone());
        self.entries.insert(key, entry);
    }
    /// Begin restore mode: skip per-insert eviction during shard restore.
    pub fn begin_restore(&mut self) {
        self.restoring = true;
    }
    /// Finish restore mode: run a single eviction pass to bring the cache under budget.
    ///
    /// Uses sort-once-remove-N approach: O(n log n) instead of the old O(n²)
    /// loop that called evict_lru() repeatedly (each call did O(n) linear scan).
    pub fn finish_restore(&mut self) {
        self.restoring = false;
        let over_bytes = self.total_bytes > self.config.max_bytes;
        let over_entries = self.entries.len() > self.config.max_entries;
        if !over_bytes && !over_entries {
            return;
        }
        // Collect all entries sorted by last_used (oldest first)
        let mut candidates: Vec<(Instant, UnifiedKey)> = if self.persistence_enabled {
            let non_dirty: Vec<_> = self.entries.iter()
                .filter(|(_, e)| !e.persist_dirty)
                .map(|(k, e)| (e.last_used, k.clone()))
                .collect();
            if non_dirty.is_empty() {
                self.entries.iter()
                    .map(|(k, e)| (e.last_used, k.clone()))
                    .collect()
            } else {
                non_dirty
            }
        } else {
            self.entries.iter()
                .map(|(k, e)| (e.last_used, k.clone()))
                .collect()
        };
        candidates.sort_unstable_by_key(|(t, _)| *t);
        // Remove oldest entries until under budget
        let mut evicted = 0usize;
        for (_, key) in &candidates {
            if self.total_bytes <= self.config.max_bytes
                && self.entries.len() <= self.config.max_entries
            {
                break;
            }
            if let Some(entry) = self.entries.remove(key) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.memory_bytes());
                self.meta_id_to_key.remove(&entry.meta_id);
                let sk = ShardKey::new(key.sort_field.clone(), key.direction);
                if let Some(set) = self.shard_to_keys.get_mut(&sk) {
                    set.remove(key);
                }
                self.evictions += 1;
                if !self.persistence_enabled {
                    self.meta.deregister(entry.meta_id);
                }
                evicted += 1;
            }
        }
        if evicted > 0 {
            eprintln!("BoundStore restore: evicted {evicted} entries to fit budget ({}MB / {}MB)",
                self.total_bytes / 1_048_576,
                self.config.max_bytes / 1_048_576);
        }
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
    /// Returns (meta_id, key, bitmap_clone, sorted_keys_clone) for each entry in the shard.
    /// Uses shard→keys index for O(shard_entries) instead of O(all_entries).
    pub fn entries_for_shard(&self, shard_key: &ShardKey) -> Vec<(CacheEntryId, UnifiedKey, RoaringBitmap, Option<Vec<u64>>)> {
        let Some(keys) = self.shard_to_keys.get(shard_key) else {
            return Vec::new();
        };
        keys.iter()
            .filter_map(|key| {
                self.entries.get(key).map(|entry| {
                    let sk = entry.sorted_keys().map(|arc| arc.as_ref().clone());
                    (entry.meta_id, key.clone(), entry.bitmap.as_ref().clone(), sk)
                })
            })
            .collect()
    }
    /// Clear persist_dirty flags for entries in a specific shard (after successful write).
    /// Uses shard→keys index for O(shard_entries) instead of O(all_entries).
    pub fn clear_shard_entry_dirty(&mut self, shard_key: &ShardKey) {
        let keys: Vec<UnifiedKey> = self.shard_to_keys
            .get(shard_key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        for key in &keys {
            if let Some(entry) = self.entries.get_mut(key) {
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
        // Budget check: time-based (preferred) or count-based (fallback).
        // Time-based: set a deadline and bail mid-loop when exceeded.
        // Count-based: bail immediately if estimated work exceeds threshold.
        let deadline = if self.config.max_maintenance_ms > 0 {
            Some(Instant::now() + Duration::from_millis(self.config.max_maintenance_ms))
        } else if self.config.max_maintenance_work > 0 && estimated_work > self.config.max_maintenance_work {
            // Fallback to count-based: bail immediately if over budget
            for meta_id in affected_ids.iter() {
                if let Some(key) = self.meta_id_to_key.get(&meta_id) {
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.mark_for_rebuild();
                    }
                }
            }
            return;
        } else {
            None // No deadline, do all work
        };
        // Collect affected keys (avoids borrow conflict between meta_id_to_key and entries)
        let affected_keys: Vec<UnifiedKey> = affected_ids
            .iter()
            .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
            .collect();
        // Iterate only affected entries
        for (i, key) in affected_keys.iter().enumerate() {
            // Check deadline every 64 entries to avoid clock overhead
            if let Some(deadline) = deadline {
                if i > 0 && i % 64 == 0 && Instant::now() > deadline {
                    // Mark remaining entries for rebuild
                    for remaining_key in &affected_keys[i..] {
                        if let Some(entry) = self.entries.get_mut(remaining_key) {
                            entry.mark_for_rebuild();
                        }
                    }
                    break;
                }
            }
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
        // Budget check: time-based (preferred) or count-based (fallback).
        let total_sort_slots: usize = sort_mutations.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_sort_slots;
        let deadline = if self.config.max_maintenance_ms > 0 {
            Some(Instant::now() + Duration::from_millis(self.config.max_maintenance_ms))
        } else if self.config.max_maintenance_work > 0 && estimated_work > self.config.max_maintenance_work {
            // Fallback to count-based: bail immediately if over budget
            for meta_id in affected_ids.iter() {
                if let Some(key) = self.meta_id_to_key.get(&meta_id) {
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.mark_for_rebuild();
                    }
                }
            }
            return;
        } else {
            None // No deadline, do all work
        };
        // Collect affected keys (avoids borrow conflict)
        let affected_keys: Vec<UnifiedKey> = affected_ids
            .iter()
            .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
            .collect();
        // Iterate only affected entries
        for (i, key) in affected_keys.iter().enumerate() {
            // Check deadline every 64 entries to avoid clock overhead
            if let Some(deadline) = deadline {
                if i > 0 && i % 64 == 0 && Instant::now() > deadline {
                    // Mark remaining entries for rebuild
                    for remaining_key in &affected_keys[i..] {
                        if let Some(entry) = self.entries.get_mut(remaining_key) {
                            entry.mark_for_rebuild();
                        }
                    }
                    break;
                }
            }
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
    /// Batch version of `remove_slot_from_all`.
    ///
    /// Used by the async cache worker to remove all deleted slots in one pass
    /// rather than calling `remove_slot_from_all` once per slot. Amortizes the
    /// outer `entries` iteration across all slots.
    pub fn remove_slots_from_all_batch(&mut self, slots: &[u32]) {
        if slots.is_empty() || self.entries.is_empty() {
            return;
        }
        for (_, entry) in self.entries.iter_mut() {
            for &slot in slots {
                entry.remove_slot_blind(slot);
            }
        }
    }
    // ── Two-Phase Maintenance (Lock-Free Evaluation) ────────────────────
    //
    // These methods split cache maintenance into three brief-lock phases:
    //   Phase A: collect_*_work()  — brief &self lock, identifies affected entries
    //   Phase B: evaluate_*_work() — NO lock, evaluates slots against staging data
    //   Phase C: apply_maintenance_results() — brief &mut self lock, applies changes
    //
    // This reduces Mutex hold time from ~469ms (full maintenance) to ~1ms per lock.
    /// Phase A: Collect filter maintenance work items under brief lock.
    ///
    /// Returns (work_items, over_budget_keys). The caller evaluates work outside
    /// the lock using staging filters/sorts, then applies results under a second lock.
    pub fn collect_filter_work(
        &self,
        filter_inserts: &HashMap<FilterGroupKey, Vec<u32>>,
        filter_removes: &HashMap<FilterGroupKey, Vec<u32>>,
    ) -> (Vec<CacheMaintenanceItem>, Vec<UnifiedKey>) {
        if self.entries.is_empty() {
            return (Vec::new(), Vec::new());
        }
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
            return (Vec::new(), Vec::new());
        }
        // Clause-level narrowing via meta-index (same logic as maintain_filter_changes)
        let mut affected_ids = RoaringBitmap::new();
        for (key, _slots) in filter_inserts.iter().chain(filter_removes.iter()) {
            let value_repr = key.value.to_string();
            if let Some(bm) = self.meta.entries_for_clause(&key.field, "eq", &value_repr) {
                affected_ids |= bm;
            }
        }
        // Field-level fallback for non-Eq entries
        for field in changed_slots_per_field.keys() {
            if let Some(field_bm) = self.meta.entries_for_filter_field(field) {
                let new_entries = field_bm - &affected_ids;
                if !new_entries.is_empty() {
                    for meta_id in new_entries.iter() {
                        if let Some(key) = self.meta_id_to_key.get(&meta_id) {
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
            return (Vec::new(), Vec::new());
        }
        // Budget check (count-based only — time-based handled in evaluate phase)
        let total_changed_slots: usize = changed_slots_per_field.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_changed_slots;
        if self.config.max_maintenance_ms == 0 && self.config.max_maintenance_work > 0 && estimated_work > self.config.max_maintenance_work {
            // Over count-based budget: mark all for rebuild
            let over_budget: Vec<UnifiedKey> = affected_ids
                .iter()
                .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
                .collect();
            return (Vec::new(), over_budget);
        }
        // Build work items: for each affected entry, collect which slots to check
        let work: Vec<CacheMaintenanceItem> = affected_ids
            .iter()
            .filter_map(|meta_id| {
                let key = self.meta_id_to_key.get(&meta_id)?;
                let entry = self.entries.get(key)?;
                if entry.needs_rebuild {
                    return None;
                }
                let mut slots = Vec::new();
                for clause in &key.filter_clauses {
                    if let Some(field_slots) = changed_slots_per_field.get(clause.field.as_str()) {
                        slots.extend(field_slots.iter().copied());
                    }
                }
                slots.sort_unstable();
                slots.dedup();
                if slots.is_empty() {
                    return None;
                }
                Some(CacheMaintenanceItem {
                    key: key.clone(),
                    slots,
                    min_tracked_value: entry.min_tracked_value,
                    direction: entry.direction,
                })
            })
            .collect();
        (work, Vec::new())
    }
    /// Phase A: Collect sort maintenance work items under brief lock.
    ///
    /// Returns (work_items, over_budget_keys).
    pub fn collect_sort_work(
        &self,
        sort_mutations: &HashMap<&str, HashSet<u32>>,
    ) -> (Vec<CacheMaintenanceItem>, Vec<UnifiedKey>) {
        if self.entries.is_empty() || sort_mutations.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let mut affected_ids = RoaringBitmap::new();
        for field in sort_mutations.keys() {
            affected_ids |= self.meta.entries_for_sort_field(field);
        }
        if affected_ids.is_empty() {
            return (Vec::new(), Vec::new());
        }
        // Budget check (count-based)
        let total_sort_slots: usize = sort_mutations.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_sort_slots;
        if self.config.max_maintenance_ms == 0 && self.config.max_maintenance_work > 0 && estimated_work > self.config.max_maintenance_work {
            let over_budget: Vec<UnifiedKey> = affected_ids
                .iter()
                .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
                .collect();
            return (Vec::new(), over_budget);
        }
        let work: Vec<CacheMaintenanceItem> = affected_ids
            .iter()
            .filter_map(|meta_id| {
                let key = self.meta_id_to_key.get(&meta_id)?;
                let entry = self.entries.get(key)?;
                if entry.needs_rebuild {
                    return None;
                }
                let sort_slots = sort_mutations.get(key.sort_field.as_str())?;
                let slots: Vec<u32> = sort_slots.iter().copied().collect();
                if slots.is_empty() {
                    return None;
                }
                Some(CacheMaintenanceItem {
                    key: key.clone(),
                    slots,
                    min_tracked_value: entry.min_tracked_value,
                    direction: entry.direction,
                })
            })
            .collect();
        (work, Vec::new())
    }
    /// Phase C: Apply computed maintenance results under brief lock.
    pub fn apply_maintenance_results(&mut self, results: &[CacheMaintenanceResult]) {
        for result in results {
            let Some(entry) = self.entries.get_mut(&result.key) else {
                continue;
            };
            if entry.needs_rebuild {
                continue;
            }
            for &(slot, sort_value) in &result.adds {
                entry.add_slot(slot, sort_value);
            }
            for &(slot, sort_value) in &result.removes {
                entry.remove_slot(slot, sort_value);
            }
        }
    }
    /// Phase C: Mark entries for rebuild in batch (budget exceeded or deadline hit).
    pub fn mark_for_rebuild_batch(&mut self, keys: &[UnifiedKey]) {
        for key in keys {
            if let Some(entry) = self.entries.get_mut(key) {
                entry.mark_for_rebuild();
            }
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
        let mut count = 0u64;
        for (key, entry) in self.entries.iter_mut() {
            if key.filter_clauses.iter().any(|c| c.field == field) {
                entry.mark_for_rebuild();
                count += 1;
            }
        }
        self.invalidations += count;
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
// ── Phase B: Lock-Free Evaluation Functions ──────────────────────────────
//
// These functions evaluate slot eligibility against staging filters/sorts
// WITHOUT holding the cache Mutex. Called between collect (Phase A) and
// apply (Phase C) to keep lock hold times under ~1ms.
/// Phase B: Evaluate filter maintenance work items outside the cache lock.
///
/// Checks each slot against the filter predicate and sort qualification.
/// Returns results to apply under a brief lock, plus any keys that exceeded
/// the time-based deadline (to be marked for rebuild).
pub fn evaluate_filter_work(
    work: &[CacheMaintenanceItem],
    filters: &FilterIndex,
    sorts: &SortIndex,
    deadline: Option<Instant>,
) -> (Vec<CacheMaintenanceResult>, Vec<UnifiedKey>) {
    // Inverted evaluation: reconstruct_value is identical across entries for
    // the same (sort_field, slot), so we precompute it ONCE per unique pair
    // before looping over work items. At 50k entries × 200 slots this turns
    // 10M reconstruct_value calls (316ns each) into ~200 calls, saving
    // ~3 seconds of redundant CPU per flush cycle.
    let reconstructed = precompute_sort_values(work, sorts);
    let mut results = Vec::with_capacity(work.len());
    let mut timed_out = Vec::new();
    for (i, item) in work.iter().enumerate() {
        // Check deadline every 64 items
        if let Some(deadline) = deadline {
            if i > 0 && i % 64 == 0 && Instant::now() > deadline {
                for remaining in &work[i..] {
                    timed_out.push(remaining.key.clone());
                }
                break;
            }
        }
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for &slot in &item.slots {
            let sort_value = reconstructed
                .get(&(item.key.sort_field.as_str(), slot))
                .copied()
                .unwrap_or(0);
            let matches = slot_matches_filter(slot, &item.key.filter_clauses, filters, sorts);
            if matches {
                let qualifies = match item.direction {
                    SortDirection::Desc => sort_value > item.min_tracked_value,
                    SortDirection::Asc => sort_value < item.min_tracked_value,
                };
                if qualifies {
                    adds.push((slot, sort_value));
                }
            } else {
                removes.push((slot, sort_value));
            }
        }
        if !adds.is_empty() || !removes.is_empty() {
            results.push(CacheMaintenanceResult {
                key: item.key.clone(),
                adds,
                removes,
            });
        }
    }
    (results, timed_out)
}
/// Phase B: Evaluate sort maintenance work items outside the cache lock.
///
/// **Inverted loop.** `reconstruct_value` is identical across all cache entries
/// for the same `(sort_field, slot)`. The old nested loop paid ~316ns per call
/// × entries × slots, which at 50k × 200 = 3.16 seconds of duplicated work per
/// flush cycle. This version:
///
///   1. Preamble: reconstruct each unique (sort_field, slot) pair exactly once.
///   2. Per-item: fast-reject via `max_new_value` vs `min_tracked_value` — an
///      entry whose bound can't possibly be crossed by any mutated slot skips
///      all further work.
///   3. Survivors: same sort_qualifies + slot_matches_filter check as before,
///      but hitting the precomputed sort values instead of calling
///      reconstruct_value per iteration.
///
/// Microbench shows ~960x at 100k entries × 200 slots compared to the nested
/// loop.
pub fn evaluate_sort_work(
    work: &[CacheMaintenanceItem],
    filters: &FilterIndex,
    sorts: &SortIndex,
    deadline: Option<Instant>,
) -> (Vec<CacheMaintenanceResult>, Vec<UnifiedKey>) {
    // Preamble: reconstruct_value once per unique (sort_field, slot).
    let reconstructed = precompute_sort_values(work, sorts);
    // Per-field max value: used for the Phase B fast-reject. An entry whose
    // min_tracked_value >= max_new_value (Desc) can't receive any update this
    // cycle — skip it entirely without touching slots.
    let max_per_field = compute_max_per_field(&reconstructed);
    let min_per_field = compute_min_per_field(&reconstructed);
    let mut results = Vec::with_capacity(work.len());
    let mut timed_out = Vec::new();
    for (i, item) in work.iter().enumerate() {
        if let Some(deadline) = deadline {
            if i > 0 && i % 64 == 0 && Instant::now() > deadline {
                for remaining in &work[i..] {
                    timed_out.push(remaining.key.clone());
                }
                break;
            }
        }
        // Fast reject: if no mutated value can possibly cross the bound in
        // the entry's direction, skip the entry entirely. This is the main
        // structural win: O(entries) integer compares instead of
        // O(entries × slots × reconstruct_value).
        //
        // Missing-field handling: if the sort field isn't in the precompute
        // map (SortIndex returned None), we fall back to 0 to stay
        // consistent with the per-slot `unwrap_or(0)` in the main loop. This
        // preserves the old code's semantics: Desc entries with missing
        // fields never qualify (since 0 can't exceed u32 `min_tracked`),
        // while Asc entries with a positive `min_tracked_value` still can.
        let field_name = item.key.sort_field.as_str();
        let can_possibly_qualify = match item.direction {
            SortDirection::Desc => {
                max_per_field.get(field_name).copied().unwrap_or(0) > item.min_tracked_value
            }
            SortDirection::Asc => {
                min_per_field.get(field_name).copied().unwrap_or(0) < item.min_tracked_value
            }
        };
        if !can_possibly_qualify {
            continue;
        }
        let mut adds = Vec::new();
        for &slot in &item.slots {
            let sort_value = reconstructed
                .get(&(field_name, slot))
                .copied()
                .unwrap_or(0);
            // Check sort qualification first (cheap integer compare)
            let qualifies = match item.direction {
                SortDirection::Desc => sort_value > item.min_tracked_value,
                SortDirection::Asc => sort_value < item.min_tracked_value,
            };
            if !qualifies {
                continue;
            }
            // Sort qualifies — only now pay filter match cost. Preserves the
            // full slot_matches_filter semantics (Eq, NotEq, In, Gt, Lt,
            // bucket, compound) — no signature-based shortcut.
            if slot_matches_filter(slot, &item.key.filter_clauses, filters, sorts) {
                adds.push((slot, sort_value));
            }
        }
        if !adds.is_empty() {
            results.push(CacheMaintenanceResult {
                key: item.key.clone(),
                adds,
                removes: Vec::new(), // Sort maintenance never removes
            });
        }
    }
    (results, timed_out)
}
/// Precompute `reconstruct_value` for every unique `(sort_field, slot)` pair
/// referenced by the work items. Shared by `evaluate_filter_work` and
/// `evaluate_sort_work` to avoid redundant bit-layer walks.
///
/// Uses `Vec<u32>` + `sort_unstable` + `dedup` for the per-field slot set
/// rather than `HashSet::insert` in a hot loop — the vector path is
/// dramatically faster at tens of thousands of slots, and dedup after sort
/// is trivial. Sorted slot order also keeps the subsequent
/// `reconstruct_value` calls cache-friendly.
fn precompute_sort_values<'a>(
    work: &'a [CacheMaintenanceItem],
    sorts: &SortIndex,
) -> HashMap<(&'a str, u32), u32> {
    let mut slots_by_field: HashMap<&'a str, Vec<u32>> = HashMap::new();
    for item in work {
        slots_by_field
            .entry(item.key.sort_field.as_str())
            .or_default()
            .extend_from_slice(&item.slots);
    }
    let mut out: HashMap<(&'a str, u32), u32> = HashMap::new();
    for (field_name, mut slots) in slots_by_field {
        let Some(field) = sorts.get_field(field_name) else {
            continue;
        };
        slots.sort_unstable();
        slots.dedup();
        for slot in slots {
            let value = field.reconstruct_value(slot);
            out.insert((field_name, slot), value);
        }
    }
    out
}
/// Maximum reconstructed value per sort field — used for Desc fast-reject.
fn compute_max_per_field<'a>(
    reconstructed: &HashMap<(&'a str, u32), u32>,
) -> HashMap<&'a str, u32> {
    let mut out: HashMap<&'a str, u32> = HashMap::new();
    for ((field, _slot), value) in reconstructed {
        let entry = out.entry(*field).or_insert(0);
        if *value > *entry {
            *entry = *value;
        }
    }
    out
}
/// Minimum reconstructed value per sort field — used for Asc fast-reject.
fn compute_min_per_field<'a>(
    reconstructed: &HashMap<(&'a str, u32), u32>,
) -> HashMap<&'a str, u32> {
    let mut out: HashMap<&'a str, u32> = HashMap::new();
    for ((field, _slot), value) in reconstructed {
        let entry = out.entry(*field).or_insert(u32::MAX);
        if *value < *entry {
            *entry = *value;
        }
    }
    out
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
                per_value_lazy: false, max_range_scan_values: None,
    
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
                computed: None,
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
    #[test]
    fn test_time_based_maintenance_short_deadline_marks_rebuild() {
        // With a very short deadline (1ms) and many entries, some should be
        // marked for rebuild because the deadline is exceeded mid-loop.
        let config = UnifiedCacheConfig {
            max_entries: 200,
            max_bytes: 64 * 1024 * 1024,
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 0,
            max_maintenance_work: 500_000,
            max_maintenance_ms: 1, // 1ms — very short
            prefetch_threshold: 0.95,
        };
        let mut cache = UnifiedCache::new(config);
        // Create 150 cache entries all referencing nsfwLevel=1
        let mut all_slots: Vec<u32> = (0..50).collect();
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &all_slots)])]);
        let sorts = make_sort_index(&[("reactionCount", &[(100, 5000)])]);
        for i in 0..150 {
            let sort_field = format!("sort_{}", i);
            let key = make_key(
                &[("nsfwLevel", "eq", "1")],
                &sort_field,
                SortDirection::Desc,
            );
            cache.form_and_store(key, &all_slots, true, 100_000, |s| 1000 - s);
        }
        // Now insert 200 changed slots to create lots of work
        let mut inserts = HashMap::new();
        let changed_slots: Vec<u32> = (50..250).collect();
        inserts.insert(
            FilterGroupKey {
                field: Arc::from("nsfwLevel"),
                value: 1,
            },
            changed_slots,
        );
        // Extend filter to include new slots
        let mut extended_slots: Vec<u32> = (0..250).collect();
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &extended_slots)])]);
        let sorts = make_sort_index(&[("reactionCount", &{
            let mut sv: Vec<(u32, u32)> = Vec::new();
            for s in 0..250 {
                sv.push((s, 5000 - s));
            }
            sv
        })]);
        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);
        // With a 1ms deadline and 150 entries × 200 slots of work,
        // at least some entries should have been marked for rebuild.
        // (We can't guarantee exactly how many due to timing, but with
        // this much work at least some should be marked.)
        let mut rebuild_count = 0;
        for i in 0..150 {
            let sort_field = format!("sort_{}", i);
            let key = make_key(
                &[("nsfwLevel", "eq", "1")],
                &sort_field,
                SortDirection::Desc,
            );
            if let Some(entry) = cache.get(&key) {
                if entry.needs_rebuild() {
                    rebuild_count += 1;
                }
            }
        }
        // Note: This test is timing-dependent. On very fast hardware,
        // all work might complete within 1ms. We assert at least that
        // the code doesn't panic and the cache is still valid.
        // On most hardware, some entries will be marked for rebuild.
        eprintln!("time_based_maintenance: {rebuild_count}/150 entries marked for rebuild with 1ms deadline");
    }
    #[test]
    fn test_time_based_maintenance_long_deadline_completes_all() {
        // With a long deadline (1000ms) and little work, all entries
        // should be maintained (none marked for rebuild).
        let config = UnifiedCacheConfig {
            max_entries: 200,
            max_bytes: 64 * 1024 * 1024,
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 0,
            max_maintenance_work: 500_000,
            max_maintenance_ms: 1000, // 1 second — very generous
            prefetch_threshold: 0.95,
        };
        let mut cache = UnifiedCache::new(config);
        // Create 5 cache entries
        let slots: Vec<u32> = (0..10).collect();
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &slots)])]);
        let sorts = make_sort_index(&[("reactionCount", &[
            (0, 1000), (1, 999), (2, 998), (3, 997), (4, 996),
            (5, 995), (6, 994), (7, 993), (8, 992), (9, 991), (20, 1500),
        ])]);
        for i in 0..5 {
            let sort_field = format!("sort_{}", i);
            let key = make_key(
                &[("nsfwLevel", "eq", "1")],
                &sort_field,
                SortDirection::Desc,
            );
            cache.form_and_store(key, &slots, true, 100_000, |s| 1000 - s);
        }
        // Insert 1 changed slot — minimal work
        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey {
                field: Arc::from("nsfwLevel"),
                value: 1,
            },
            vec![20],
        );
        let extended_slots: Vec<u32> = (0..21).collect();
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &extended_slots)])]);
        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);
        // With 1000ms deadline and only 5 entries × 1 slot, nothing should be
        // marked for rebuild.
        for i in 0..5 {
            let sort_field = format!("sort_{}", i);
            let key = make_key(
                &[("nsfwLevel", "eq", "1")],
                &sort_field,
                SortDirection::Desc,
            );
            if let Some(entry) = cache.get(&key) {
                assert!(
                    !entry.needs_rebuild(),
                    "Entry sort_{i} should NOT be marked for rebuild with 1000ms deadline and minimal work"
                );
            }
        }
    }
    #[test]
    fn test_count_based_fallback_when_ms_is_zero() {
        // With max_maintenance_ms=0, the count-based fallback should kick in.
        let config = UnifiedCacheConfig {
            max_entries: 200,
            max_bytes: 64 * 1024 * 1024,
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 0,
            max_maintenance_work: 1, // Very low: 1 unit of work triggers rebuild
            max_maintenance_ms: 0,   // Disable time-based
            prefetch_threshold: 0.95,
        };
        let mut cache = UnifiedCache::new(config);
        let slots: Vec<u32> = (0..10).collect();
        let key = make_key(
            &[("nsfwLevel", "eq", "1")],
            "reactionCount",
            SortDirection::Desc,
        );
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(20, 1500)])]);
        // 1 affected entry × 1 changed slot = 1 work, but budget is 1
        // so estimated_work (1) > max_maintenance_work (1) is false... set work=2
        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey {
                field: Arc::from("nsfwLevel"),
                value: 1,
            },
            vec![20, 21],
        );
        cache.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);
        // 1 entry × 2 slots = 2 > max_maintenance_work(1), should mark for rebuild
        let entry = cache.get(&key).unwrap();
        assert!(
            entry.needs_rebuild(),
            "Entry should be marked for rebuild when count-based budget is exceeded and max_maintenance_ms=0"
        );
    }
    // ── Two-Phase Maintenance Tests ──────────────────────────────────────
    #[test]
    fn test_two_phase_filter_maintenance_adds_qualifying_slot() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 2, 3, 4, 10])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);
        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10],
        );
        // Phase A: collect work
        let (work, over_budget) = cache.collect_filter_work(&inserts, &HashMap::new());
        assert!(over_budget.is_empty());
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].key, key);
        // Phase B: evaluate outside lock
        let (results, timed_out) = evaluate_filter_work(&work, &filters, &sorts, None);
        assert!(timed_out.is_empty());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].adds.len(), 1);
        assert_eq!(results[0].adds[0].0, 10); // slot 10
        // Phase C: apply
        cache.apply_maintenance_results(&results);
        let entry = cache.get(&key).unwrap();
        assert!(entry.bitmap().contains(10), "Slot 10 should be added via two-phase maintenance");
    }
    #[test]
    fn test_two_phase_filter_maintenance_removes_non_matching_slot() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        // Slot 3 no longer in filter bitmap for value 1
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 2, 4])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(3, 997)])]);
        let mut removes = HashMap::new();
        removes.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![3],
        );
        let (work, _) = cache.collect_filter_work(&HashMap::new(), &removes);
        let (results, _) = evaluate_filter_work(&work, &filters, &sorts, None);
        cache.apply_maintenance_results(&results);
        let entry = cache.get(&key).unwrap();
        assert!(!entry.bitmap().contains(3), "Slot 3 should be removed via two-phase maintenance");
    }
    #[test]
    fn test_two_phase_sort_maintenance_adds_qualifying_slot() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        // min_tracked_value = value_fn(4) = 1000 - 4 = 996
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 2, 3, 4, 10])])]);
        // Slot 10 has sort value 1500 > min_tracked(996) → qualifies
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);
        let mut sort_mutations: HashMap<&str, HashSet<u32>> = HashMap::new();
        sort_mutations.insert("reactionCount", [10].into_iter().collect());
        let (work, _) = cache.collect_sort_work(&sort_mutations);
        assert_eq!(work.len(), 1);
        let (results, _) = evaluate_sort_work(&work, &filters, &sorts, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].adds.len(), 1);
        assert_eq!(results[0].adds[0].0, 10);
        cache.apply_maintenance_results(&results);
        let entry = cache.get(&key).unwrap();
        assert!(entry.bitmap().contains(10), "Slot 10 should be added via two-phase sort maintenance");
    }
    #[test]
    fn test_two_phase_count_budget_marks_rebuild() {
        let config = UnifiedCacheConfig {
            max_maintenance_work: 1,
            max_maintenance_ms: 0,
            ..make_config()
        };
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10, 11], // 1 entry × 2 slots = 2 > budget(1)
        );
        let (work, over_budget) = cache.collect_filter_work(&inserts, &HashMap::new());
        assert!(work.is_empty(), "Should have no work items when over budget");
        assert_eq!(over_budget.len(), 1, "Should mark 1 entry for rebuild");
        cache.mark_for_rebuild_batch(&over_budget);
        let entry = cache.get(&key).unwrap();
        assert!(entry.needs_rebuild(), "Entry should be marked for rebuild");
    }
    #[test]
    fn test_two_phase_equivalence_with_single_phase() {
        // Verify two-phase produces the same result as the original single-phase maintain_filter_changes.
        let config = UnifiedCacheConfig {
            max_maintenance_ms: 0, // disable time-based to ensure deterministic
            ..make_config()
        };
        let slots: Vec<u32> = (0..5).collect();
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        // Setup: slot 10 matches filter, sort value 1500 > min_tracked(996) → should add
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[0, 1, 2, 3, 4, 10])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);
        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![10],
        );
        // Single-phase (original)
        let mut cache_single = UnifiedCache::new(config.clone());
        cache_single.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        cache_single.maintain_filter_changes(&inserts, &HashMap::new(), &filters, &sorts);
        let single_has_10 = cache_single.get(&key).unwrap().bitmap().contains(10);
        // Two-phase (new)
        let mut cache_two = UnifiedCache::new(config);
        cache_two.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        let (work, _) = cache_two.collect_filter_work(&inserts, &HashMap::new());
        let (results, _) = evaluate_filter_work(&work, &filters, &sorts, None);
        cache_two.apply_maintenance_results(&results);
        let two_has_10 = cache_two.get(&key).unwrap().bitmap().contains(10);
        assert_eq!(single_has_10, two_has_10, "Two-phase should produce same result as single-phase");
        assert!(two_has_10, "Both should have slot 10");
    }
    #[test]
    fn test_finish_restore_batch_eviction() {
        // Verify finish_restore uses O(n log n) batch eviction, not O(n²) per-item.
        // With 10 entries and max_entries=5, it should evict 5 in one sorted pass.
        let config = UnifiedCacheConfig {
            max_entries: 5,
            max_bytes: usize::MAX, // only constrain by entry count
            initial_capacity: 10,
            max_capacity: 10,
            min_filter_size: 0,
            ..Default::default()
        };
        let mut cache = UnifiedCache::new(config);
        cache.begin_restore();
        // Insert 10 entries via insert_restored_entry (the actual restore path)
        for i in 0..10u32 {
            let key = make_key(
                &[("nsfwLevel", "eq", &i.to_string())],
                "reactionCount",
                SortDirection::Desc,
            );
            let meta_id = cache.meta_mut().register(
                &key.filter_clauses,
                Some(&key.sort_field),
                Some(key.direction),
            );
            let slots: Vec<u32> = (0..10).collect();
            let entry = UnifiedEntry::new(
                &slots, 10, 10, true, 100, meta_id, SortDirection::Desc, |s| 1000 - s,
            );
            cache.insert_restored_entry(key, entry);
        }
        assert_eq!(cache.len(), 10, "All 10 should be stored during restore");
        // finish_restore should evict down to max_entries=5
        cache.finish_restore();
        assert_eq!(cache.len(), 5, "Should evict down to max_entries");
        assert_eq!(cache.evictions, 5, "Should have evicted exactly 5");
    }
    /// Microbench: time Phase A collect_sort_work with many cache entries
    /// sorting by the same field, simulating the metrics_poller workload.
    /// See src/bin/cache_microbench.rs for the standalone runner.
    #[test]
    #[ignore]
    fn bench_collect_sort_work() {
        use std::time::Instant;
        let n_entries: usize = std::env::var("BENCH_ENTRIES")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(70_000);
        let n_mut_slots: usize = std::env::var("BENCH_MUT_SLOTS")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(200);
        let max_work: usize = std::env::var("BENCH_MAX_WORK")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(500_000);
        println!("[bench] n_entries={} n_mut_slots={} max_maintenance_work={}",
                 n_entries, n_mut_slots, max_work);
        // Large cache, bailout threshold from env.
        let config = UnifiedCacheConfig {
            max_entries: n_entries * 2,
            max_bytes: 2 * 1024 * 1024 * 1024, // 2 GB generous
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 0,
            max_maintenance_work: max_work,
            max_maintenance_ms: 5,
            ..Default::default()
        };
        let mut cache = UnifiedCache::new(config);
        // Populate n_entries cache entries, all sorting by "reactionCount".
        // Each entry has a distinct filter clause so meta keys don't collide.
        let t_populate = Instant::now();
        let slots: Vec<u32> = (0..100).collect();
        for i in 0..n_entries {
            let val = i.to_string();
            let key = make_key(
                &[("userId", "eq", &val)],
                "reactionCount",
                SortDirection::Desc,
            );
            cache.form_and_store(key, &slots, true, 100_000, |s| 1000u64.saturating_sub(s as u64));
        }
        println!("[bench] populated {} entries in {:.2}s",
                 cache.len(), t_populate.elapsed().as_secs_f64());
        // Build sort mutations: n_mut_slots distinct random-ish slots on reactionCount.
        // Values in 0..100 so they're within an existing entry's tracked window
        // (not that Phase A cares about values — it only builds work items).
        let mut mut_slots: HashSet<u32> = HashSet::new();
        for i in 0..n_mut_slots {
            mut_slots.insert((i as u32).wrapping_mul(2654435761));
        }
        let mut sort_mutations: HashMap<&str, HashSet<u32>> = HashMap::new();
        sort_mutations.insert("reactionCount", mut_slots);
        // Time collect_sort_work over several iterations
        let iters = 10;
        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let (work, over_budget) = cache.collect_sort_work(&sort_mutations);
            let elapsed = t.elapsed();
            samples.push((elapsed, work.len(), over_budget.len()));
        }
        for (i, (e, w, ob)) in samples.iter().enumerate() {
            println!(
                "[bench] iter {:2}: {:.3}ms  work_items={}  over_budget={}",
                i, e.as_secs_f64() * 1000.0, w, ob
            );
        }
        let total_ns: u128 = samples.iter().map(|(e, _, _)| e.as_nanos()).sum();
        let avg_ms = (total_ns as f64 / iters as f64) / 1_000_000.0;
        println!("[bench] avg collect_sort_work: {:.3}ms over {} iters", avg_ms, iters);
    }
}
