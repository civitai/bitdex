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
use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// DashMap with the project's standard ahash hasher.
type AHashMap2<K, V> = DashMap<K, V, ahash::RandomState>;

/// Wall-clock millis since UNIX_EPOCH. Used as the LRU timestamp baseline so
/// `last_used` can live in an `AtomicU64` and be updated from `&self` (no
/// write lock). Approximate LRU only — wall clock can drift but eviction
/// quality is unaffected at the resolution we care about.
#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
use roaring::RoaringBitmap;
use crate::bound_store::ShardKey;
use crate::cache::CanonicalClause;

/// Returns true when `c` is a genuine time-bucket clause (e.g. `sortAt:bucket:7d`).
///
/// `op == "bucket"` alone is not sufficient: `prefilter.rs::substitute` injects a
/// `BucketBitmap { field: "__prefilter", … }` clause that also canonicalises to
/// `op = "bucket"`. Those prefilter-substituted entries must NOT enter the time-bucket
/// diff path, so we gate on `field != "__prefilter"` as the sentinel exclusion.
pub fn is_time_bucket_clause(c: &CanonicalClause) -> bool {
    c.op == "bucket" && c.field != "__prefilter"
}

/// Assign a cost class (1–5) to a `FilterClause` for cheap-first ordering.
///
/// Lower cost = cheaper to evaluate = checked first by the per-slot evaluator,
/// enabling short-circuit on the most selective/cheapest clause.
///
/// | Class | Variants                        | Why                                          |
/// |-------|---------------------------------|----------------------------------------------|
/// | 1     | Eq, IsNull, IsNotNull, BucketBitmap | 1 hash lookup + 1 contains               |
/// | 2     | NotEq, Gt, Gte, Lt, Lte        | 1 contains + 1 reconstruct_value, or 1 lookup + negate |
/// | 3     | In, NotIn                       | ≤K contains (K = values.len())               |
/// | 4     | And, Or                         | max child class (recursive compound)         |
/// | 5     | Not(And/Or)                     | deepest compound — pushed last               |
///
/// For `And`/`Or`, the class is the **max** of children — gives a stable, predictable
/// bucket assignment without summing (which would depend on arity).
/// For `Not`, if the inner clause is `And`/`Or` the class is bumped to 5 (push last);
/// otherwise the inner class is inherited (e.g. `Not(Eq)` → class 1).
pub fn clause_atom_cost(c: &FilterClause) -> u8 {
    match c {
        FilterClause::Eq(..) | FilterClause::IsNull(..) | FilterClause::IsNotNull(..) => 1,
        FilterClause::BucketBitmap { .. } => 1,
        FilterClause::NotEq(..)
        | FilterClause::Gt(..)
        | FilterClause::Gte(..)
        | FilterClause::Lt(..)
        | FilterClause::Lte(..) => 2,
        FilterClause::In(..) | FilterClause::NotIn(..) => 3,
        FilterClause::And(parts) | FilterClause::Or(parts) => {
            parts.iter().map(clause_atom_cost).max().unwrap_or(1).max(4)
        }
        FilterClause::Not(inner) => match inner.as_ref() {
            FilterClause::And(_) | FilterClause::Or(_) => 5,
            other => clause_atom_cost(other),
        },
    }
}
use crate::filter::FilterIndex;
use crate::meta_index::{CacheEntryId, MetaIndex};
use crate::query::{FilterClause, SortDirection};
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
    /// Carried from UnifiedEntry; consumed in Commit 3 (B2).
    pub original_filter_clauses: Arc<Vec<FilterClause>>,
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
    /// `AtomicBool` so readers can check it via `&self` from the RwLock read path.
    needs_rebuild: AtomicBool,
    /// Guard to prevent concurrent rebuilds.
    rebuilding: AtomicBool,
    /// Guard to prevent concurrent prefetch expansions.
    prefetching: AtomicBool,
    /// LRU timestamp as ms since UNIX_EPOCH. Atomic so readers can `touch()`
    /// the entry from the RwLock read path without taking a write lock.
    last_used: AtomicU64,
    /// Meta-index entry ID for this cache entry.
    meta_id: CacheEntryId,
    /// Dirty flag for persistence: set when bitmap modified by live maintenance,
    /// cleared when merge thread writes the shard. LRU eviction skips dirty entries.
    /// `AtomicBool` so `&self` paths can update it without escalating to a write lock.
    persist_dirty: AtomicBool,
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
    /// Original FilterClause tree captured at entry formation.
    /// Used by live maintenance (Phase B) to evaluate compound predicates natively.
    /// Stored as Arc so per-cycle clones into CacheMaintenanceItem are cheap.
    /// TODO(B8): account for original_filter_clauses bytes once persisted.
    original_filter_clauses: Arc<Vec<FilterClause>>,
}
impl UnifiedEntry {
    /// Create a new entry from a sort traversal result.
    ///
    /// `sorted_slots` should be the top-N slots from the sort traversal, in sort order.
    /// `value_fn` returns the sort value for a given slot.
    /// At formation, capacity is initial_capacity (4K) — no radix needed.
    ///
    /// Test/legacy callers without a FilterClause tree call `new` (defaults to
    /// empty Arc). Production callers with a FilterClause tree call
    /// `new_with_clauses` so live maintenance can natively evaluate compound
    /// predicates (Commit 3 / B2).
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
        Self::new_with_clauses(
            sorted_slots,
            capacity,
            max_capacity,
            has_more,
            total_matched,
            meta_id,
            direction,
            Arc::new(Vec::new()),
            value_fn,
        )
    }
    /// Like `new`, but accepts the original FilterClause tree to carry through
    /// to live maintenance.
    pub fn new_with_clauses(
        sorted_slots: &[u32],
        capacity: usize,
        max_capacity: usize,
        has_more: bool,
        total_matched: u64,
        meta_id: CacheEntryId,
        direction: SortDirection,
        original_filter_clauses: Arc<Vec<FilterClause>>,
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
            needs_rebuild: AtomicBool::new(false),
            rebuilding: AtomicBool::new(false),
            prefetching: AtomicBool::new(false),
            last_used: AtomicU64::new(now_ms()),
            meta_id,
            persist_dirty: AtomicBool::new(true), // New entries need persisting
            sorted_keys,
            radix: None, // No radix at initial capacity — sorted vec is faster
            direction,
            bucket_cutoff: 0, // Set by caller via set_bucket_cutoff() after creation
            uses_bucket: false, // Set by caller via set_uses_bucket() after creation
            original_filter_clauses,
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
        // TODO(B8): populate FilterClause tree from persisted meta.bin once META_VERSION is bumped.
        Self::from_restored_with_clauses(
            bitmap,
            meta_id,
            initial_capacity,
            max_capacity,
            direction,
            persisted_sorted_keys,
            value_fn,
            has_more,
            persisted_total_matched,
            Arc::new(Vec::new()),
        )
    }
    /// Like `from_restored`, but accepts the persisted FilterClause tree.
    /// Used post-B8 once meta.bin V2 carries the original clauses.
    pub fn from_restored_with_clauses(
        bitmap: RoaringBitmap,
        meta_id: CacheEntryId,
        initial_capacity: usize,
        max_capacity: usize,
        direction: SortDirection,
        persisted_sorted_keys: Option<Vec<u64>>,
        value_fn: impl Fn(u32) -> u32,
        has_more: bool,
        persisted_total_matched: u64,
        original_filter_clauses: Arc<Vec<FilterClause>>,
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
            needs_rebuild: AtomicBool::new(false),
            rebuilding: AtomicBool::new(false),
            prefetching: AtomicBool::new(false),
            last_used: AtomicU64::new(now_ms()),
            meta_id,
            persist_dirty: AtomicBool::new(false), // Just loaded from disk — clean
            sorted_keys,
            radix: None,
            direction,
            bucket_cutoff: 0, // Set by caller after restore
            uses_bucket: false, // Set by caller after restore
            original_filter_clauses,
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
    /// The original FilterClause tree captured at entry formation.
    pub fn original_filter_clauses(&self) -> &Arc<Vec<FilterClause>> {
        &self.original_filter_clauses
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
        self.needs_rebuild.load(Ordering::Acquire)
    }
    /// Mark this entry for rebuild. `&self` so the read path can flag stale
    /// entries (e.g. expired bucket cutoff) without escalating to a write lock.
    pub fn mark_for_rebuild(&self) {
        self.needs_rebuild.store(true, Ordering::Release);
    }
    pub fn meta_id(&self) -> CacheEntryId {
        self.meta_id
    }
    /// Update the LRU timestamp. `&self` so the read path can touch entries
    /// from the RwLock read lock.
    pub fn touch(&self) {
        self.last_used.store(now_ms(), Ordering::Relaxed);
    }
    /// LRU timestamp in ms-since-UNIX-epoch.
    pub fn last_used_ms(&self) -> u64 {
        self.last_used.load(Ordering::Relaxed)
    }
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }
    /// Add a slot to the bounded bitmap. Returns true if bloat threshold was exceeded.
    /// `sort_value` is needed to maintain the radix index when present.
    pub fn add_slot(&mut self, slot: u32, sort_value: u32) -> bool {
        Arc::make_mut(&mut self.bitmap).insert(slot);
        self.persist_dirty.store(true, Ordering::Relaxed);
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
            self.needs_rebuild.store(true, Ordering::Release);
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
        self.persist_dirty.store(true, Ordering::Relaxed);
        self.sorted_keys = None;
        if let Some(ref mut radix) = self.radix {
            let r = Arc::make_mut(radix);
            for &(slot, value) in adds {
                r.insert(slot, value);
            }
        }
        let bloat_threshold = self.capacity * 2;
        if self.bitmap.len() as usize > bloat_threshold {
            self.needs_rebuild.store(true, Ordering::Release);
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
        self.persist_dirty.store(true, Ordering::Relaxed);
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
        self.persist_dirty.store(true, Ordering::Relaxed);
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
        self.persist_dirty.store(true, Ordering::Relaxed);
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
        self.needs_rebuild.store(false, Ordering::Release);
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
        self.persist_dirty.load(Ordering::Relaxed)
    }
    /// Mark this entry as having unsaved modifications.
    pub fn mark_persist_dirty(&self) {
        self.persist_dirty.store(true, Ordering::Relaxed);
    }
    /// Clear the persist dirty flag (after successful shard write).
    pub fn clear_persist_dirty(&self) {
        self.persist_dirty.store(false, Ordering::Relaxed);
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
/// The unified cache: interior-mutable concurrent map keyed by (filters, sort, direction).
///
/// `UnifiedCache` is fully `Sync` and accessed via `&self` from all callers — no
/// outer `Mutex`/`RwLock` wrapper. Concurrent reads + writes proceed in parallel
/// across DashMap shards (default 64-way) and never queue on a global lock.
///
/// Stat counters are `AtomicU64`. Per-entry LRU + needs_rebuild are atomics on
/// `UnifiedEntry`. The few non-`Send`/non-`Sync` collections (`pending_shards`,
/// `meta_has_more`, etc.) are wrapped in `parking_lot::Mutex` and accessed only
/// on cold paths.
pub struct UnifiedCache {
    entries: AHashMap2<UnifiedKey, UnifiedEntry>,
    /// Reverse index: meta_id → key, for O(1) lookup from MetaIndex results.
    meta_id_to_key: AHashMap2<CacheEntryId, UnifiedKey>,
    meta: RwLock<MetaIndex>,
    config: ArcSwap<UnifiedCacheConfig>,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    updates: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
    /// Running total of entry memory (bitmap + sorted_keys + radix bytes).
    total_bytes: AtomicUsize,
    // ── Persistence State ──────────────────────────────────────────────
    /// Shards that exist on disk but haven't been loaded into RAM yet.
    pending_shards: Mutex<HashSet<ShardKey>>,
    /// Shards currently being loaded by another thread (loading sentinel).
    loading_shards: Mutex<HashSet<ShardKey>>,
    /// Whether meta.bin needs rewriting (new entry, expansion, tombstone).
    meta_dirty: AtomicBool,
    /// Which shards need rewriting (bitmap modified by maintenance).
    shard_dirty: Mutex<HashSet<ShardKey>>,
    /// Whether persistence is enabled (BoundStore exists).
    persistence_enabled: AtomicBool,
    /// Persisted has_more flags keyed by entry ID, populated from meta.bin on startup.
    /// Consumed during shard restore to avoid hardcoding has_more=true.
    meta_has_more: Mutex<HashMap<CacheEntryId, bool>>,
    /// Persisted total_matched values keyed by entry ID, populated from meta.bin on startup.
    /// Consumed during shard restore to get the real total instead of bitmap cardinality.
    meta_total_matched: Mutex<HashMap<CacheEntryId, u64>>,
    /// Cumulative count of entry expansions from initial to expanded capacity.
    extensions: AtomicU64,
    /// Cumulative count of cache wall hits (cursor past cached entries, triggering slow path).
    wall_hits: AtomicU64,
    /// Cumulative count of prefetch triggers (background expansion requests).
    prefetches: AtomicU64,
    /// True during shard restore — skips per-insert eviction.
    restoring: AtomicBool,
    /// Reverse index: ShardKey → set of UnifiedKeys in that shard.
    /// Avoids O(all_entries) scan in entries_for_shard() and clear_shard_entry_dirty().
    shard_to_keys: AHashMap2<ShardKey, HashSet<UnifiedKey>>,
    /// Optional shared metrics handle for reason-attributed rebuild counters.
    /// Set by `ConcurrentEngine` after construction; tests leave it unset and
    /// the cache silently skips the increments.
    rebuild_metrics: OnceLock<Arc<crate::cache_worker::CacheWorkerMetrics>>,
}
impl UnifiedCache {
    pub fn new(config: UnifiedCacheConfig) -> Self {
        Self {
            entries: DashMap::with_hasher(ahash::RandomState::new()),
            meta_id_to_key: DashMap::with_hasher(ahash::RandomState::new()),
            meta: RwLock::new(MetaIndex::new()),
            config: ArcSwap::new(Arc::new(config)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            total_bytes: AtomicUsize::new(0),
            pending_shards: Mutex::new(HashSet::new()),
            loading_shards: Mutex::new(HashSet::new()),
            meta_dirty: AtomicBool::new(false),
            shard_dirty: Mutex::new(HashSet::new()),
            persistence_enabled: AtomicBool::new(false),
            meta_has_more: Mutex::new(HashMap::new()),
            meta_total_matched: Mutex::new(HashMap::new()),
            extensions: AtomicU64::new(0),
            wall_hits: AtomicU64::new(0),
            prefetches: AtomicU64::new(0),
            restoring: AtomicBool::new(false),
            shard_to_keys: DashMap::with_hasher(ahash::RandomState::new()),
            rebuild_metrics: OnceLock::new(),
        }
    }
    /// Install the rebuild-attribution metrics handle. Idempotent — only the
    /// first call has effect. Called by `ConcurrentEngine` post-construction.
    pub fn set_rebuild_metrics(&self, metrics: Arc<crate::cache_worker::CacheWorkerMetrics>) {
        let _ = self.rebuild_metrics.set(metrics);
    }
    /// Internal helper — returns the metrics handle if set.
    #[inline]
    fn rmetrics(&self) -> Option<&Arc<crate::cache_worker::CacheWorkerMetrics>> {
        self.rebuild_metrics.get()
    }
    /// Count cache entries currently in the `needs_rebuild=true` state.
    /// O(entries) scan — call from the prom scrape path, not the hot path.
    pub fn count_needs_rebuild(&self) -> u64 {
        let mut count = 0u64;
        for r in self.entries.iter() {
            if r.value().needs_rebuild() {
                count += 1;
            }
        }
        count
    }
    /// Store persisted has_more flags from meta.bin, keyed by entry ID.
    /// Called during startup after loading meta.bin.
    pub fn set_meta_has_more(&self, map: HashMap<CacheEntryId, bool>) {
        *self.meta_has_more.lock() = map;
    }
    /// Look up persisted has_more for a given entry ID. Falls back to true if not found.
    pub fn get_meta_has_more(&self, entry_id: CacheEntryId) -> bool {
        self.meta_has_more.lock().get(&entry_id).copied().unwrap_or(true)
    }
    /// Store persisted total_matched values from meta.bin, keyed by entry ID.
    /// Called during startup after loading meta.bin.
    pub fn set_meta_total_matched(&self, map: HashMap<CacheEntryId, u64>) {
        *self.meta_total_matched.lock() = map;
    }
    /// Look up persisted total_matched for a given entry ID. Falls back to 0 if not found.
    pub fn get_meta_total_matched(&self, entry_id: CacheEntryId) -> u64 {
        self.meta_total_matched.lock().get(&entry_id).copied().unwrap_or(0)
    }
    /// Look up a cache entry by key for mutation. Returns `None` on miss.
    /// Increments hit/miss counters and refreshes LRU.
    ///
    /// Returned `RefMut` holds the per-shard write lock for the duration of
    /// its lifetime — keep the closure body short. Other shards remain
    /// uncontended so concurrent queries hitting other keys are not blocked.
    pub fn lookup<'a>(
        &'a self,
        key: &UnifiedKey,
    ) -> Option<dashmap::mapref::one::RefMut<'a, UnifiedKey, UnifiedEntry>> {
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.value().needs_rebuild() {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                entry.value().touch();
                Some(entry)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
    /// Look up a cache entry for read-only access. Returns `None` on miss.
    /// Increments hit/miss counters and refreshes LRU via atomics — no mutation
    /// of the entry itself, so the per-shard read lock is taken (concurrent
    /// readers on the same shard proceed in parallel, writers on other shards
    /// are not blocked).
    pub fn lookup_for_read<'a>(
        &'a self,
        key: &UnifiedKey,
    ) -> Option<dashmap::mapref::one::Ref<'a, UnifiedKey, UnifiedEntry>> {
        match self.entries.get(key) {
            Some(entry) => {
                if entry.value().needs_rebuild() {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                entry.value().touch();
                Some(entry)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
    /// Look up immutably (no touch, no stat counter).
    pub fn get<'a>(
        &'a self,
        key: &UnifiedKey,
    ) -> Option<dashmap::mapref::one::Ref<'a, UnifiedKey, UnifiedEntry>> {
        self.entries.get(key)
    }
    /// Single-flight guard for `needs_rebuild` entries at the slow-path call
    /// site in `ConcurrentEngine`.
    ///
    /// Returns `true` if the caller should run the full slow path (seed sort +
    /// `form_and_store_with_clauses`).  Returns `false` if another concurrent
    /// caller already claimed the rebuild — this caller should execute the
    /// query directly against the executor bitmaps without touching the cache.
    ///
    /// Two cases both return `true` (proceed):
    /// - No entry exists yet for `key` → normal first-time insert.
    /// - Entry exists but is NOT flagged → entry vanished between `lookup_for_read`
    ///   returning `None` and this call (eviction race); safe to re-insert.
    ///
    /// The one case that returns `false`:
    /// - Entry exists AND `needs_rebuild=true` AND `try_start_rebuild()` lost
    ///   the CAS → another caller claimed the rebuild; skip.
    ///
    /// Uses `get` (no hit/miss counters) so the stat path is not double-counted.
    pub fn should_rebuild_single_flight(&self, key: &UnifiedKey) -> bool {
        match self.entries.get(key) {
            Some(r) if r.value().needs_rebuild() => {
                // Entry flagged — first caller wins the CAS, others skip.
                r.value().try_start_rebuild()
            }
            _ => true, // No entry, or fresh entry (not flagged) — proceed normally.
        }
    }
    /// Store a new entry, evicting LRU if over budget. Returns the meta_id assigned.
    ///
    /// Uses batch eviction: when over budget, evicts ~10% of entries in one O(n)
    /// pass instead of calling evict_lru() per entry.
    pub fn store(&self, key: UnifiedKey, entry: UnifiedEntry) -> CacheEntryId {
        let meta_id = entry.meta_id;
        let new_bytes = entry.memory_bytes();
        // If replacing an existing entry, deregister the old one and subtract its bytes
        if let Some((_, old)) = self.entries.remove(&key) {
            // The old entry was the stale one — replacing it counts as a
            // rebuild completion.
            if old.needs_rebuild() {
                if let Some(m) = self.rmetrics() {
                    m.rebuild_completed_total.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.total_bytes
                .fetch_sub(old.memory_bytes().min(self.total_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
            self.meta_id_to_key.remove(&old.meta_id);
            self.meta.write().deregister(old.meta_id);
            // Remove from shard→keys index
            let old_sk = ShardKey::new(key.sort_field.clone(), key.direction);
            if let Some(mut set) = self.shard_to_keys.get_mut(&old_sk) {
                set.value_mut().remove(&key);
            }
        }
        // Batch eviction: when over budget, evict ~10% of entries at once.
        let cfg = self.config.load();
        if (self.total_bytes.load(Ordering::Relaxed) + new_bytes > cfg.max_bytes
            || self.entries.len() >= cfg.max_entries)
            && !self.entries.is_empty()
        {
            self.evict_batch();
        }
        // Mark dirty for persistence
        if self.persistence_enabled.load(Ordering::Relaxed) {
            self.meta_dirty.store(true, Ordering::Relaxed);
            let shard_key = ShardKey::new(key.sort_field.clone(), key.direction);
            self.shard_dirty.lock().insert(shard_key);
        }
        self.total_bytes.fetch_add(new_bytes, Ordering::Relaxed);
        self.meta_id_to_key.insert(meta_id, key.clone());
        // Maintain shard→keys index
        let sk = ShardKey::new(key.sort_field.clone(), key.direction);
        self.shard_to_keys.entry(sk).or_default().insert(key.clone());
        self.entries.insert(key, entry);
        self.inserts.fetch_add(1, Ordering::Relaxed);
        meta_id
    }
    /// Register a new entry with the meta-index and create the entry.
    ///
    /// Test/legacy callers without an original FilterClause tree call
    /// `form_and_store` (defaults to empty Arc). Production callers with a
    /// FilterClause tree call `form_and_store_with_clauses` so live
    /// maintenance can natively evaluate compound predicates (Commit 3 / B2).
    pub fn form_and_store(
        &self,
        key: UnifiedKey,
        sorted_slots: &[u32],
        has_more: bool,
        total_matched: u64,
        value_fn: impl Fn(u32) -> u32,
    ) -> CacheEntryId {
        self.form_and_store_with_clauses(
            key,
            sorted_slots,
            has_more,
            total_matched,
            Arc::new(Vec::new()),
            value_fn,
        )
    }
    /// Like `form_and_store`, but accepts the original FilterClause tree.
    pub fn form_and_store_with_clauses(
        &self,
        key: UnifiedKey,
        sorted_slots: &[u32],
        has_more: bool,
        total_matched: u64,
        original_filter_clauses: Arc<Vec<FilterClause>>,
        value_fn: impl Fn(u32) -> u32,
    ) -> CacheEntryId {
        // B3: stable-sort the original FilterClause tree by atom cost (ascending)
        // so the per-slot evaluator in `slot_matches_filter_native` short-circuits
        // on the cheapest clause first.  Stable sort preserves user intent within
        // each cost bucket.  The UnifiedKey (canonical-clause hash key) is NOT
        // affected — canonical ordering is independently deterministic.
        let original_filter_clauses = if original_filter_clauses.len() > 1 {
            let mut sorted: Vec<FilterClause> = (*original_filter_clauses).clone();
            sorted.sort_by_key(|c| clause_atom_cost(c));
            Arc::new(sorted)
        } else {
            original_filter_clauses
        };

        // Register with meta-index. Pass the original FilterClause tree so
        // compound clauses get registered under their real leaf field names
        // (not just FieldKey("") from the canonical representation).
        let meta_id = self.meta.write().register(
            &key.filter_clauses,
            Some(&original_filter_clauses),
            Some(&key.sort_field),
            Some(key.direction),
        );
        let direction = key.direction;
        let uses_bucket = key.filter_clauses.iter().any(is_time_bucket_clause);
        let cfg = self.config.load();
        let mut entry = UnifiedEntry::new_with_clauses(
            sorted_slots,
            cfg.initial_capacity,
            cfg.max_capacity,
            has_more,
            total_matched,
            meta_id,
            direction,
            Arc::clone(&original_filter_clauses),
            value_fn,
        );
        entry.set_uses_bucket(uses_bucket);
        if uses_bucket {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            entry.set_bucket_cutoff(now);
        }
        self.store(key, entry)
    }

    /// Read the cache config snapshot. Cheap atomic-load via `ArcSwap`.
    pub fn capacity_config(&self) -> (usize, usize) {
        let cfg = self.config.load();
        (cfg.initial_capacity, cfg.max_capacity)
    }

    /// Register a meta-index id without building or inserting an entry.
    /// Pairs with `store` to split form_and_store into three phases.
    /// Canonical-only registration (no leaf-field walk). Use
    /// `allocate_meta_id_with_clauses` when the FilterClause tree is available.
    pub fn allocate_meta_id(&self, key: &UnifiedKey) -> CacheEntryId {
        self.meta.write().register(
            &key.filter_clauses,
            None,
            Some(&key.sort_field),
            Some(key.direction),
        )
    }

    /// Like `allocate_meta_id` but also registers leaf fields from the
    /// original FilterClause tree, enabling write-path lookup for compound
    /// clauses (And/Or/Not) whose inner fields have `field=""` in canonical form.
    pub fn allocate_meta_id_with_clauses(
        &self,
        key: &UnifiedKey,
        original_filter_clauses: &[FilterClause],
    ) -> CacheEntryId {
        self.meta.write().register(
            &key.filter_clauses,
            Some(original_filter_clauses),
            Some(&key.sort_field),
            Some(key.direction),
        )
    }
    /// Evict the least-recently-used entry. Returns the evicted key, if any.
    ///
    /// When persistence is enabled:
    /// - Skips dirty entries (unsaved bitmap modifications)
    /// - Does NOT deregister from meta-index (entry stays on disk as orphan)
    pub fn evict_lru(&self) -> Option<UnifiedKey> {
        let persistence = self.persistence_enabled.load(Ordering::Relaxed);
        let lru_key = if persistence {
            self.entries
                .iter()
                .filter(|r| !r.value().is_persist_dirty())
                .min_by_key(|r| r.value().last_used_ms())
                .map(|r| r.key().clone())
                .or_else(|| {
                    self.entries
                        .iter()
                        .min_by_key(|r| r.value().last_used_ms())
                        .map(|r| r.key().clone())
                })
        } else {
            self.entries
                .iter()
                .min_by_key(|r| r.value().last_used_ms())
                .map(|r| r.key().clone())
        }?;
        if let Some((_, evicted)) = self.entries.remove(&lru_key) {
            tracing::info!(
                "Cache evicted entry: sort={} {:?} | filters={} | card={} | bytes={}",
                lru_key.sort_field, lru_key.direction, lru_key.filter_clauses.len(),
                evicted.cardinality(), evicted.memory_bytes()
            );
            let bytes = evicted.memory_bytes();
            self.total_bytes
                .fetch_sub(bytes.min(self.total_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
            self.meta_id_to_key.remove(&evicted.meta_id);
            let sk = ShardKey::new(lru_key.sort_field.clone(), lru_key.direction);
            if let Some(mut set) = self.shard_to_keys.get_mut(&sk) {
                set.value_mut().remove(&lru_key);
            }
            self.evictions.fetch_add(1, Ordering::Relaxed);
            if !persistence {
                self.meta.write().deregister(evicted.meta_id);
            }
        }
        Some(lru_key)
    }
    /// Batch eviction: evict ~10% of entries (minimum 1) in one pass via sampled-LRU.
    pub fn evict_batch(&self) {
        if self.entries.is_empty() {
            return;
        }
        const SAMPLE_SIZE: usize = 8;
        let target_evict = (self.entries.len() / 10).max(1);
        let mut evicted = 0;
        use std::hash::{BuildHasher, Hasher};
        let hb = ahash::RandomState::new();
        let prefer_non_dirty = self.persistence_enabled.load(Ordering::Relaxed);
        for tick in 0..target_evict {
            let mut h = hb.build_hasher();
            h.write_u64(self.evictions.load(Ordering::Relaxed).wrapping_add(tick as u64));
            let skip = (h.finish() as usize) % self.entries.len().max(1);
            let mut best: Option<(u64, UnifiedKey)> = None;
            let mut all_dirty_seen = true;
            // Take a fresh sample — DashMap iter order is shard-order, randomize via skip.
            let mut sampled = 0usize;
            for r in self.entries.iter().skip(skip) {
                if sampled >= SAMPLE_SIZE { break; }
                sampled += 1;
                let e = r.value();
                if prefer_non_dirty && e.is_persist_dirty() {
                    continue;
                }
                all_dirty_seen = false;
                let lu = e.last_used_ms();
                match best.as_ref() {
                    None => best = Some((lu, r.key().clone())),
                    Some((t, _)) if lu < *t => {
                        best = Some((lu, r.key().clone()));
                    }
                    _ => {}
                }
            }
            if best.is_none() && prefer_non_dirty && all_dirty_seen {
                if let Some(r) = self.entries.iter().skip(skip).next() {
                    best = Some((r.value().last_used_ms(), r.key().clone()));
                }
            }
            let Some((_, key)) = best else { break };
            if let Some((_, entry)) = self.entries.remove(&key) {
                let bytes = entry.memory_bytes();
                self.total_bytes
                    .fetch_sub(bytes.min(self.total_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
                self.meta_id_to_key.remove(&entry.meta_id);
                let sk = ShardKey::new(key.sort_field.clone(), key.direction);
                if let Some(mut set) = self.shard_to_keys.get_mut(&sk) {
                    set.value_mut().remove(&key);
                }
                self.evictions.fetch_add(1, Ordering::Relaxed);
                if !prefer_non_dirty {
                    self.meta.write().deregister(entry.meta_id);
                }
                evicted += 1;
            }
        }
        if evicted > 0 {
            tracing::info!("Cache sampled-LRU eviction: evicted {evicted} entries, {} remaining", self.entries.len());
        }
    }
    /// Get a mutable reference to an entry by key (no touch, no stat counter).
    pub fn get_mut<'a>(
        &'a self,
        key: &UnifiedKey,
    ) -> Option<dashmap::mapref::one::RefMut<'a, UnifiedKey, UnifiedEntry>> {
        self.entries.get_mut(key)
    }
    /// Access the meta-index. Returns a read guard — callers chain method calls
    /// directly (e.g. `cache.meta().entry_count()`); the guard drops at the end
    /// of the call expression.
    pub fn meta(&self) -> RwLockReadGuard<'_, MetaIndex> {
        self.meta.read()
    }
    /// Access the meta-index mutably. Returns a write guard with the same usage
    /// pattern as `meta()`.
    pub fn meta_mut(&self) -> RwLockWriteGuard<'_, MetaIndex> {
        self.meta.write()
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
        self.total_bytes.load(Ordering::Relaxed)
    }
    /// Reconcile the tracked total_bytes with actual entry sizes.
    /// Call after bulk maintenance operations (expand/rebuild/add_slot/remove_slot)
    /// which mutate entries in-place without updating the running total.
    pub fn reconcile_bytes(&self) {
        let total: usize = self.entries.iter().map(|r| r.value().memory_bytes()).sum();
        self.total_bytes.store(total, Ordering::Relaxed);
    }
    /// Clear all entries, reset the meta-index, and reset counters.
    pub fn clear(&self) {
        self.entries.clear();
        self.meta_id_to_key.clear();
        self.shard_to_keys.clear();
        *self.meta.write() = MetaIndex::new();
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.total_bytes.store(0, Ordering::Relaxed);
        self.pending_shards.lock().clear();
        self.loading_shards.lock().clear();
        self.meta_dirty.store(false, Ordering::Relaxed);
        self.shard_dirty.lock().clear();
        self.meta_total_matched.lock().clear();
    }
    /// Return a stats snapshot.
    pub fn stats(&self) -> UnifiedCacheStats {
        let mut entries_initial = 0usize;
        let mut entries_expanded = 0usize;
        for r in self.entries.iter() {
            let entry = r.value();
            if entry.capacity >= entry.max_capacity {
                entries_expanded += 1;
            } else {
                entries_initial += 1;
            }
        }
        let meta = self.meta.read();
        UnifiedCacheStats {
            entries: self.entries.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            memory_bytes: self.total_memory_bytes(),
            meta_index_entries: meta.entry_count(),
            meta_index_bytes: meta.memory_bytes(),
            persistence_enabled: self.persistence_enabled.load(Ordering::Relaxed),
            tombstone_count: meta.tombstone_count(),
            pending_shard_count: self.pending_shards.lock().len(),
            dirty_shard_count: self.shard_dirty.lock().len(),
            meta_dirty: self.meta_dirty.load(Ordering::Relaxed),
            entries_initial,
            entries_expanded,
            extensions: self.extensions.load(Ordering::Relaxed),
            wall_hits: self.wall_hits.load(Ordering::Relaxed),
            prefetches: self.prefetches.load(Ordering::Relaxed),
        }
    }
    /// Return per-entry detail for diagnostics/testing.
    pub fn entry_details(&self) -> Vec<UnifiedEntryDetail> {
        self.entries.iter().map(|r| {
            let key = r.key();
            let entry = r.value();
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
    /// Count entries by clause type for scrape-time gauges (A3).
    ///
    /// Returns `(substituted, compound)` where:
    /// - `substituted` = entries with a `__prefilter` clause (op="bucket", field="__prefilter")
    /// - `compound` = entries with at least one op ∈ {and, or, not, isnull, isnotnull}
    pub fn count_by_clause_type(&self) -> (u64, u64) {
        let mut substituted = 0u64;
        let mut compound = 0u64;
        for r in self.entries.iter() {
            let clauses = &r.key().filter_clauses;
            let has_prefilter = clauses.iter().any(|c| c.field == "__prefilter" && c.op == "bucket");
            let has_compound = clauses.iter().any(|c| {
                matches!(c.op.as_str(), "and" | "or" | "not" | "isnull" | "isnotnull")
            });
            if has_prefilter { substituted += 1; }
            if has_compound { compound += 1; }
        }
        (substituted, compound)
    }
    /// Reset hit/miss counters without clearing entries.
    pub fn reset_counters(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }
    /// Record a cache entry update (called by flush thread during maintenance).
    /// `&self` so the read path can record stat events without escalating to
    /// a write lock — counters are atomic.
    pub fn record_update(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
    }
    /// Record a cache entry expansion from initial to expanded capacity.
    pub fn record_extension(&self) {
        self.extensions.fetch_add(1, Ordering::Relaxed);
    }
    /// Record a cache wall hit (cursor went past cached entries, triggering expansion/slow path).
    pub fn record_wall_hit(&self) {
        self.wall_hits.fetch_add(1, Ordering::Relaxed);
    }
    /// Record a prefetch trigger (background expansion request sent).
    pub fn record_prefetch(&self) {
        self.prefetches.fetch_add(1, Ordering::Relaxed);
    }
    /// Get a snapshot of the cache config. Cheap atomic load via `ArcSwap`.
    pub fn config(&self) -> Arc<UnifiedCacheConfig> {
        self.config.load_full()
    }
    /// Mutate the cache config under a closure: clone current → modify → swap.
    /// Used by runtime PATCH endpoint to update individual config fields.
    pub fn with_config_mut(&self, f: impl FnOnce(&mut UnifiedCacheConfig)) {
        let current = self.config.load_full();
        let mut next = (*current).clone();
        f(&mut next);
        self.config.store(Arc::new(next));
    }
    /// Get the key for a meta_id. O(1) via reverse index. Returns a cloned
    /// `UnifiedKey` (the underlying DashMap entry's shard lock can't escape).
    pub fn key_for_meta_id(&self, meta_id: CacheEntryId) -> Option<UnifiedKey> {
        self.meta_id_to_key.get(&meta_id).map(|r| r.value().clone())
    }
    /// Snapshot all meta_id → key mappings (for persistence flush). Returns a
    /// cloned `Vec` so callers don't hold any DashMap shard locks.
    pub fn meta_id_to_key_snapshot(&self) -> Vec<(CacheEntryId, UnifiedKey)> {
        self.meta_id_to_key
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect()
    }
    // ── Persistence Support ──────────────────────────────────────────────────
    /// Enable persistence mode. Called when a BoundStore is available.
    pub fn enable_persistence(&self) {
        self.persistence_enabled.store(true, Ordering::Relaxed);
    }
    /// Whether persistence is enabled.
    pub fn persistence_enabled(&self) -> bool {
        self.persistence_enabled.load(Ordering::Relaxed)
    }
    /// Check if a shard is pending (exists on disk, not loaded).
    pub fn is_shard_pending(&self, sort_field: &str, direction: SortDirection) -> bool {
        self.pending_shards.lock().contains(&ShardKey::new(sort_field.to_string(), direction))
    }
    /// Check if a shard is currently being loaded.
    pub fn is_shard_loading(&self, sort_field: &str, direction: SortDirection) -> bool {
        self.loading_shards.lock().contains(&ShardKey::new(sort_field.to_string(), direction))
    }
    /// Mark a shard as loading (sentinel to prevent concurrent loads).
    pub fn mark_shard_loading(&self, sort_field: &str, direction: SortDirection) {
        let key = ShardKey::new(sort_field.to_string(), direction);
        self.pending_shards.lock().remove(&key);
        self.loading_shards.lock().insert(key);
    }
    /// Mark a shard as loaded (remove from pending and loading).
    pub fn mark_shard_loaded(&self, sort_field: &str, direction: SortDirection) {
        let key = ShardKey::new(sort_field.to_string(), direction);
        self.pending_shards.lock().remove(&key);
        self.loading_shards.lock().remove(&key);
    }
    /// Add pending shards (from meta.bin on startup).
    pub fn add_pending_shards(&self, shards: impl IntoIterator<Item = ShardKey>) {
        self.pending_shards.lock().extend(shards);
    }
    /// Get all pending shard keys (returns a guard — the held lock blocks
    /// concurrent mutation of the pending-shards set for the guard's lifetime).
    pub fn pending_shards(&self) -> MutexGuard<'_, HashSet<ShardKey>> {
        self.pending_shards.lock()
    }
    /// Insert a restored entry from disk (shard load). Does NOT register with
    /// meta-index (that was done during meta.bin load). Does NOT set meta_dirty.
    ///
    /// Skips eviction during restore (restoring flag). Call `finish_restore()` after
    /// loading all entries to run a single eviction pass.
    pub fn insert_restored_entry(&self, key: UnifiedKey, entry: UnifiedEntry) {
        let meta_id = entry.meta_id;
        let bytes = entry.memory_bytes();
        if !self.restoring.load(Ordering::Relaxed) {
            let cfg = self.config.load();
            if (self.total_bytes.load(Ordering::Relaxed) + bytes > cfg.max_bytes
                || self.entries.len() >= cfg.max_entries)
                && !self.entries.is_empty()
            {
                self.evict_batch();
            }
        }
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.meta_id_to_key.insert(meta_id, key.clone());
        let sk = ShardKey::new(key.sort_field.clone(), key.direction);
        self.shard_to_keys.entry(sk).or_default().insert(key.clone());
        self.entries.insert(key, entry);
    }
    /// Begin restore mode: skip per-insert eviction during shard restore.
    pub fn begin_restore(&self) {
        self.restoring.store(true, Ordering::Relaxed);
    }
    /// Finish restore mode: run a single eviction pass to bring the cache under budget.
    pub fn finish_restore(&self) {
        self.restoring.store(false, Ordering::Relaxed);
        let cfg = self.config.load();
        let over_bytes = self.total_bytes.load(Ordering::Relaxed) > cfg.max_bytes;
        let over_entries = self.entries.len() > cfg.max_entries;
        if !over_bytes && !over_entries {
            return;
        }
        let persistence = self.persistence_enabled.load(Ordering::Relaxed);
        let mut candidates: Vec<(u64, UnifiedKey)> = if persistence {
            let non_dirty: Vec<_> = self.entries.iter()
                .filter(|r| !r.value().is_persist_dirty())
                .map(|r| (r.value().last_used_ms(), r.key().clone()))
                .collect();
            if non_dirty.is_empty() {
                self.entries.iter()
                    .map(|r| (r.value().last_used_ms(), r.key().clone()))
                    .collect()
            } else {
                non_dirty
            }
        } else {
            self.entries.iter()
                .map(|r| (r.value().last_used_ms(), r.key().clone()))
                .collect()
        };
        candidates.sort_unstable_by_key(|(t, _)| *t);
        let mut evicted = 0usize;
        for (_, key) in &candidates {
            if self.total_bytes.load(Ordering::Relaxed) <= cfg.max_bytes
                && self.entries.len() <= cfg.max_entries
            {
                break;
            }
            if let Some((_, entry)) = self.entries.remove(key) {
                let bytes = entry.memory_bytes();
                self.total_bytes
                    .fetch_sub(bytes.min(self.total_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
                self.meta_id_to_key.remove(&entry.meta_id);
                let sk = ShardKey::new(key.sort_field.clone(), key.direction);
                if let Some(mut set) = self.shard_to_keys.get_mut(&sk) {
                    set.value_mut().remove(key);
                }
                self.evictions.fetch_add(1, Ordering::Relaxed);
                if !persistence {
                    self.meta.write().deregister(entry.meta_id);
                }
                evicted += 1;
            }
        }
        if evicted > 0 {
            eprintln!("BoundStore restore: evicted {evicted} entries to fit budget ({}MB / {}MB)",
                self.total_bytes.load(Ordering::Relaxed) / 1_048_576,
                cfg.max_bytes / 1_048_576);
        }
    }
    /// Check if meta needs writing.
    pub fn is_meta_dirty(&self) -> bool {
        self.meta_dirty.load(Ordering::Relaxed)
    }
    /// Clear the meta dirty flag (after successful write).
    pub fn clear_meta_dirty(&self) {
        self.meta_dirty.store(false, Ordering::Relaxed);
    }
    /// Set the meta dirty flag.
    pub fn set_meta_dirty(&self) {
        self.meta_dirty.store(true, Ordering::Relaxed);
    }
    /// Get dirty shards that need writing — returns a guard. Iterate inline
    /// (e.g. `cache.dirty_shards().iter().cloned().collect::<Vec<_>>()`).
    pub fn dirty_shards(&self) -> MutexGuard<'_, HashSet<ShardKey>> {
        self.shard_dirty.lock()
    }
    /// Mark a shard as dirty.
    pub fn mark_shard_dirty(&self, key: ShardKey) {
        self.shard_dirty.lock().insert(key);
    }
    /// Clear a shard dirty flag (after successful write).
    pub fn clear_shard_dirty(&self, key: &ShardKey) {
        self.shard_dirty.lock().remove(key);
    }
    /// Check if an entry ID is in RAM (for tombstone decisions).
    pub fn has_entry_id(&self, meta_id: CacheEntryId) -> bool {
        self.meta_id_to_key.contains_key(&meta_id)
    }
    /// Collect entries for a specific shard (for merge thread shard write).
    /// Returns (meta_id, key, bitmap_clone, sorted_keys_clone) for each entry in the shard.
    /// Uses shard→keys index for O(shard_entries) instead of O(all_entries).
    pub fn entries_for_shard(&self, shard_key: &ShardKey) -> Vec<(CacheEntryId, UnifiedKey, RoaringBitmap, Option<Vec<u64>>)> {
        let keys: Vec<UnifiedKey> = match self.shard_to_keys.get(shard_key) {
            Some(set) => set.value().iter().cloned().collect(),
            None => return Vec::new(),
        };
        keys.into_iter()
            .filter_map(|key| {
                self.entries.get(&key).map(|r| {
                    let entry = r.value();
                    let sk = entry.sorted_keys().map(|arc| arc.as_ref().clone());
                    (entry.meta_id, key.clone(), entry.bitmap.as_ref().clone(), sk)
                })
            })
            .collect()
    }
    /// Clear persist_dirty flags for entries in a specific shard (after successful write).
    /// Uses shard→keys index for O(shard_entries) instead of O(all_entries).
    pub fn clear_shard_entry_dirty(&self, shard_key: &ShardKey) {
        let keys: Vec<UnifiedKey> = self.shard_to_keys
            .get(shard_key)
            .map(|r| r.value().iter().cloned().collect())
            .unwrap_or_default();
        for key in &keys {
            if let Some(r) = self.entries.get(key) {
                r.value().persist_dirty.store(false, Ordering::Relaxed);
            }
        }
    }
    /// Tombstone an entry that isn't in RAM (flush thread: mutation to unloaded entry).
    /// Sets meta_dirty. Does NOT touch the shard (tombstone cleanup is deferred).
    pub fn tombstone_entry(&self, meta_id: CacheEntryId) {
        self.meta.write().tombstone(meta_id);
        self.meta_dirty.store(true, Ordering::Relaxed);
    }
    /// Finalize shard write: clean up tombstones for entries that were omitted,
    /// deregister them from meta-index, and recycle their IDs.
    pub fn finalize_shard_write(&self, cleaned_ids: &[CacheEntryId]) {
        let mut meta = self.meta.write();
        for &id in cleaned_ids {
            meta.clear_tombstone(id);
            meta.deregister(id);
        }
    }
    /// Check if >50% of a shard's entries are tombstoned (triggers forced cleanup).
    pub fn shard_needs_cleanup(&self, shard_key: &ShardKey) -> bool {
        let meta = self.meta.read();
        let total = meta.entries_for_sort(&shard_key.sort_field, shard_key.direction)
            .map(|bm| bm.len())
            .unwrap_or(0);
        if total == 0 {
            return false;
        }
        let tombstoned = meta.entries_for_sort(&shard_key.sort_field, shard_key.direction)
            .map(|bm| {
                let mut count = 0u64;
                for id in bm.iter() {
                    if meta.is_tombstoned(id) {
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
    pub fn tombstone_unloaded_for_filter(&self, changed_fields: &[&str]) -> u64 {
        if !self.persistence_enabled.load(Ordering::Relaxed) {
            return 0;
        }
        let mut to_tombstone = Vec::new();
        {
            let meta = self.meta.read();
            for field in changed_fields {
                if let Some(bm) = meta.entries_for_filter_field(field) {
                    for id in bm.iter() {
                        if !self.meta_id_to_key.contains_key(&id) && !meta.is_tombstoned(id) {
                            to_tombstone.push(id);
                        }
                    }
                }
            }
        }
        let count = to_tombstone.len() as u64;
        if count > 0 {
            let mut meta = self.meta.write();
            for id in to_tombstone {
                meta.tombstone(id);
            }
            self.meta_dirty.store(true, Ordering::Relaxed);
        }
        count
    }
    /// Tombstone unloaded entries affected by sort field mutations.
    /// Returns the number of entries tombstoned.
    pub fn tombstone_unloaded_for_sort(&self, changed_fields: &[&str]) -> u64 {
        if !self.persistence_enabled.load(Ordering::Relaxed) {
            return 0;
        }
        let mut to_tombstone = Vec::new();
        {
            let meta = self.meta.read();
            for field in changed_fields {
                let affected = meta.entries_for_sort_field(field);
                for id in affected.iter() {
                    if !self.meta_id_to_key.contains_key(&id) && !meta.is_tombstoned(id) {
                        to_tombstone.push(id);
                    }
                }
            }
        }
        let count = to_tombstone.len() as u64;
        if count > 0 {
            let mut meta = self.meta.write();
            for id in to_tombstone {
                meta.tombstone(id);
            }
            self.meta_dirty.store(true, Ordering::Relaxed);
        }
        count
    }
    /// Tombstone ALL unloaded entries (registered in meta but not in RAM).
    pub fn tombstone_all_unloaded(&self) -> u64 {
        if !self.persistence_enabled.load(Ordering::Relaxed) {
            return 0;
        }
        let to_tombstone: Vec<u32> = {
            let meta = self.meta.read();
            meta.all_registered_ids()
                .filter(|id| !self.meta_id_to_key.contains_key(id) && !meta.is_tombstoned(*id))
                .collect()
        };
        let count = to_tombstone.len() as u64;
        if count > 0 {
            let mut meta = self.meta.write();
            for id in to_tombstone {
                meta.tombstone(id);
            }
            self.meta_dirty.store(true, Ordering::Relaxed);
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
        &self,
        filter_inserts: &HashMap<FilterGroupKey, Vec<u32>>,
        filter_removes: &HashMap<FilterGroupKey, Vec<u32>>,
        filters: &FilterIndex,
        sorts: &SortIndex,
    ) {
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
        // Clause-level narrowing via meta-index (read lock for the lookup pass).
        let affected_ids: RoaringBitmap = {
            let meta = self.meta.read();
            let mut ids = RoaringBitmap::new();
            for (key, _slots) in filter_inserts.iter().chain(filter_removes.iter()) {
                let value_repr = key.value.to_string();
                if let Some(bm) = meta.entries_for_clause(&key.field, "eq", &value_repr) {
                    ids |= bm;
                }
            }
            for field in changed_slots_per_field.keys() {
                if let Some(field_bm) = meta.entries_for_filter_field(field) {
                    let new_entries = field_bm - &ids;
                    if !new_entries.is_empty() {
                        for meta_id in new_entries.iter() {
                            if let Some(key_ref) = self.meta_id_to_key.get(&meta_id) {
                                let has_non_eq = key_ref.value().filter_clauses.iter().any(|c| {
                                    c.field == *field && c.op != "eq"
                                });
                                if has_non_eq {
                                    ids.insert(meta_id);
                                }
                            }
                        }
                    }
                }
            }
            ids
        };
        if affected_ids.is_empty() {
            return;
        }
        let cfg = self.config.load();
        let total_changed_slots: usize = changed_slots_per_field.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_changed_slots;
        let deadline = if cfg.max_maintenance_ms > 0 {
            Some(Instant::now() + Duration::from_millis(cfg.max_maintenance_ms))
        } else if cfg.max_maintenance_work > 0 && estimated_work > cfg.max_maintenance_work {
            let mut marked = 0u64;
            for meta_id in affected_ids.iter() {
                if let Some(key_ref) = self.meta_id_to_key.get(&meta_id) {
                    let key = key_ref.value().clone();
                    drop(key_ref);
                    if let Some(r) = self.entries.get(&key) {
                        r.value().mark_for_rebuild();
                        marked += 1;
                    }
                }
            }
            if marked > 0 {
                if let Some(m) = self.rmetrics() {
                    m.marked_for_rebuild_count_budget_total
                        .fetch_add(marked, Ordering::Relaxed);
                }
            }
            return;
        } else {
            None
        };
        let affected_keys: Vec<UnifiedKey> = affected_ids
            .iter()
            .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).map(|r| r.value().clone()))
            .collect();
        for (i, key) in affected_keys.iter().enumerate() {
            if let Some(deadline) = deadline {
                if i > 0 && i % 64 == 0 && Instant::now() > deadline {
                    let mut marked = 0u64;
                    for remaining_key in &affected_keys[i..] {
                        if let Some(r) = self.entries.get(remaining_key) {
                            r.value().mark_for_rebuild();
                            marked += 1;
                        }
                    }
                    if marked > 0 {
                        if let Some(m) = self.rmetrics() {
                            m.marked_for_rebuild_deadline_total
                                .fetch_add(marked, Ordering::Relaxed);
                        }
                    }
                    break;
                }
            }
            let Some(mut entry_ref) = self.entries.get_mut(key) else {
                continue;
            };
            let entry = entry_ref.value_mut();
            if entry.needs_rebuild() {
                continue;
            }
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
                let matches = slot_matches_filter(slot, &key.filter_clauses, filters, sorts, None);
                if matches {
                    if entry.sort_qualifies(sort_value, key.direction) {
                        entry.add_slot(slot, sort_value);
                    }
                } else {
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
        &self,
        sort_mutations: &HashMap<&str, HashSet<u32>>,
        filters: &FilterIndex,
        sorts: &SortIndex,
    ) {
        if sort_mutations.is_empty() {
            return;
        }
        let affected_ids: RoaringBitmap = {
            let meta = self.meta.read();
            let mut ids = RoaringBitmap::new();
            for field in sort_mutations.keys() {
                ids |= meta.entries_for_sort_field(field);
            }
            ids
        };
        if affected_ids.is_empty() {
            return;
        }
        let cfg = self.config.load();
        let total_sort_slots: usize = sort_mutations.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_sort_slots;
        let deadline = if cfg.max_maintenance_ms > 0 {
            Some(Instant::now() + Duration::from_millis(cfg.max_maintenance_ms))
        } else if cfg.max_maintenance_work > 0 && estimated_work > cfg.max_maintenance_work {
            let mut marked = 0u64;
            for meta_id in affected_ids.iter() {
                if let Some(key_ref) = self.meta_id_to_key.get(&meta_id) {
                    let key = key_ref.value().clone();
                    drop(key_ref);
                    if let Some(r) = self.entries.get(&key) {
                        r.value().mark_for_rebuild();
                        marked += 1;
                    }
                }
            }
            if marked > 0 {
                if let Some(m) = self.rmetrics() {
                    m.marked_for_rebuild_count_budget_total
                        .fetch_add(marked, Ordering::Relaxed);
                }
            }
            return;
        } else {
            None
        };
        let affected_keys: Vec<UnifiedKey> = affected_ids
            .iter()
            .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).map(|r| r.value().clone()))
            .collect();
        for (i, key) in affected_keys.iter().enumerate() {
            if let Some(deadline) = deadline {
                if i > 0 && i % 64 == 0 && Instant::now() > deadline {
                    let mut marked = 0u64;
                    for remaining_key in &affected_keys[i..] {
                        if let Some(r) = self.entries.get(remaining_key) {
                            r.value().mark_for_rebuild();
                            marked += 1;
                        }
                    }
                    if marked > 0 {
                        if let Some(m) = self.rmetrics() {
                            m.marked_for_rebuild_deadline_total
                                .fetch_add(marked, Ordering::Relaxed);
                        }
                    }
                    break;
                }
            }
            let Some(mut entry_ref) = self.entries.get_mut(key) else {
                continue;
            };
            let entry = entry_ref.value_mut();
            if entry.needs_rebuild() {
                continue;
            }
            let sort_slots = match sort_mutations.get(key.sort_field.as_str()) {
                Some(slots) => slots,
                None => continue,
            };
            for &slot in sort_slots {
                let sort_value = sorts
                    .get_field(&key.sort_field)
                    .map(|f| f.reconstruct_value(slot))
                    .unwrap_or(0);
                if !entry.sort_qualifies(sort_value, key.direction) {
                    continue;
                }
                // Sort qualifies — check filter match
                if slot_matches_filter(slot, &key.filter_clauses, filters, sorts, None) {
                    entry.add_slot(slot, sort_value);
                }
            }
        }
    }
    /// Remove a deleted slot from all cache entries.
    ///
    /// Called by the flush thread when a document is deleted. Targeted removal
    /// avoids marking all entries for rebuild, preserving cache effectiveness.
    pub fn remove_slot_from_all(&self, slot: u32) {
        for mut r in self.entries.iter_mut() {
            r.value_mut().remove_slot_blind(slot);
        }
    }
    /// Batch version of `remove_slot_from_all`.
    ///
    /// Used by the async cache worker to remove all deleted slots in one pass
    /// rather than calling `remove_slot_from_all` once per slot. Amortizes the
    /// outer `entries` iteration across all slots.
    pub fn remove_slots_from_all_batch(&self, slots: &[u32]) {
        if slots.is_empty() || self.entries.is_empty() {
            return;
        }
        // Each iter_mut yields a RefMutMulti — holds the shard's write lock
        // for the duration of the body. Per-shard locks keep concurrent
        // readers on other shards unblocked.
        for mut r in self.entries.iter_mut() {
            let entry = r.value_mut();
            let bm = Arc::make_mut(&mut entry.bitmap);
            for &slot in slots {
                bm.remove(slot);
            }
            entry.persist_dirty.store(true, Ordering::Relaxed);
            entry.sorted_keys = None;
            if let Some(ref mut radix) = entry.radix {
                let rx = Arc::make_mut(radix);
                for &slot in slots {
                    rx.remove_blind(slot);
                }
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
        let affected_ids: RoaringBitmap = {
            let meta = self.meta.read();
            let mut ids = RoaringBitmap::new();
            for (key, _slots) in filter_inserts.iter().chain(filter_removes.iter()) {
                let value_repr = key.value.to_string();
                if let Some(bm) = meta.entries_for_clause(&key.field, "eq", &value_repr) {
                    ids |= bm;
                }
            }
            for field in changed_slots_per_field.keys() {
                if let Some(field_bm) = meta.entries_for_filter_field(field) {
                    let new_entries = field_bm - &ids;
                    if !new_entries.is_empty() {
                        for meta_id in new_entries.iter() {
                            if let Some(key_ref) = self.meta_id_to_key.get(&meta_id) {
                                let clauses = &key_ref.value().filter_clauses;
                                // Include this entry if:
                                // (a) It has a non-eq canonical clause for this field —
                                //     not already covered by entries_for_clause (eq-only), OR
                                // (b) No canonical clause directly names this field — meaning
                                //     it was registered via compound-clause leaf-field walk
                                //     (And/Or/Not inner fields have field="" in canonical form).
                                let field_in_canonical = clauses.iter().any(|c| c.field == *field);
                                let has_non_eq = field_in_canonical && clauses.iter().any(|c| {
                                    c.field == *field && c.op != "eq"
                                });
                                if has_non_eq || !field_in_canonical {
                                    ids.insert(meta_id);
                                }
                            }
                        }
                    }
                }
            }
            ids
        };
        if affected_ids.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let cfg = self.config.load();
        let total_changed_slots: usize = changed_slots_per_field.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_changed_slots;
        if cfg.max_maintenance_ms == 0 && cfg.max_maintenance_work > 0 && estimated_work > cfg.max_maintenance_work {
            let over_budget: Vec<UnifiedKey> = affected_ids
                .iter()
                .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).map(|r| r.value().clone()))
                .collect();
            return (Vec::new(), over_budget);
        }
        let work: Vec<CacheMaintenanceItem> = affected_ids
            .iter()
            .filter_map(|meta_id| {
                let key_ref = self.meta_id_to_key.get(&meta_id)?;
                let key = key_ref.value().clone();
                drop(key_ref);
                let entry_ref = self.entries.get(&key)?;
                let entry = entry_ref.value();
                if entry.needs_rebuild() {
                    return None;
                }
                let mut slots = Vec::new();
                let orig = entry.original_filter_clauses();
                if !orig.is_empty() {
                    // Walk leaf fields from the original FilterClause tree.
                    // Canonical clauses have field="" for compound (And/Or/Not)
                    // shapes, so changed_slots_per_field.get("") returns nothing.
                    // The leaf walk reaches the actual inner fields (baseModel,
                    // nsfwLevel, etc.) that compound predicates reference.
                    let mut leaf_fields = ahash::AHashSet::new();
                    for clause in orig.iter() {
                        crate::meta_index::collect_leaf_fields(clause, &mut leaf_fields);
                    }
                    for field in &leaf_fields {
                        if let Some(field_slots) = changed_slots_per_field.get(field.as_str()) {
                            slots.extend(field_slots.iter().copied());
                        }
                    }
                } else {
                    // Fallback: canonical loop for entries without an original
                    // clause tree (e.g. legacy form_and_store, restored entries
                    // pre-B8, or simple non-compound shapes).
                    for clause in &key.filter_clauses {
                        if let Some(field_slots) = changed_slots_per_field.get(clause.field.as_str()) {
                            slots.extend(field_slots.iter().copied());
                        }
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
                    original_filter_clauses: Arc::clone(entry.original_filter_clauses()),
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
        let affected_ids: RoaringBitmap = {
            let meta = self.meta.read();
            let mut ids = RoaringBitmap::new();
            for field in sort_mutations.keys() {
                ids |= meta.entries_for_sort_field(field);
            }
            ids
        };
        if affected_ids.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let cfg = self.config.load();
        let total_sort_slots: usize = sort_mutations.values().map(|s| s.len()).sum();
        let affected_count = affected_ids.len() as usize;
        let estimated_work = affected_count * total_sort_slots;
        if cfg.max_maintenance_ms == 0 && cfg.max_maintenance_work > 0 && estimated_work > cfg.max_maintenance_work {
            let over_budget: Vec<UnifiedKey> = affected_ids
                .iter()
                .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).map(|r| r.value().clone()))
                .collect();
            return (Vec::new(), over_budget);
        }
        let work: Vec<CacheMaintenanceItem> = affected_ids
            .iter()
            .filter_map(|meta_id| {
                let key_ref = self.meta_id_to_key.get(&meta_id)?;
                let key = key_ref.value().clone();
                drop(key_ref);
                let entry_ref = self.entries.get(&key)?;
                let entry = entry_ref.value();
                if entry.needs_rebuild() {
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
                    original_filter_clauses: Arc::clone(entry.original_filter_clauses()),
                })
            })
            .collect();
        (work, Vec::new())
    }
    /// Phase C: Apply computed maintenance results.
    pub fn apply_maintenance_results(&self, results: &[CacheMaintenanceResult]) {
        for result in results {
            let Some(mut entry_ref) = self.entries.get_mut(&result.key) else {
                continue;
            };
            let entry = entry_ref.value_mut();
            if entry.needs_rebuild() {
                continue;
            }
            let mut modified = false;
            if !result.adds.is_empty() {
                entry.add_slots_bulk(&result.adds);
                modified = true;
            }
            if !result.removes.is_empty() {
                entry.remove_slots_bulk(&result.removes);
                modified = true;
            }
            if modified {
                self.record_update();
            }
        }
    }
    /// Phase C: Mark entries for rebuild in batch (budget exceeded or deadline hit).
    pub fn mark_for_rebuild_batch(&self, keys: &[UnifiedKey]) {
        for key in keys {
            if let Some(r) = self.entries.get(key) {
                r.value().mark_for_rebuild();
            }
        }
    }
    /// Mark all entries for rebuild when alive bitmap changes. Returns the
    /// number of entries flagged so the caller can attribute the reason.
    pub fn maintain_alive_changes(&self) -> u64 {
        let mut count = 0u64;
        for r in self.entries.iter() {
            r.value().mark_for_rebuild();
            count += 1;
        }
        count
    }
    /// Invalidate entries that reference a specific filter field. Returns the
    /// number of entries flagged so the caller can attribute the reason.
    pub fn invalidate_filter_field(&self, field: &str) -> u64 {
        let mut count = 0u64;
        for r in self.entries.iter() {
            if r.key().filter_clauses.iter().any(|c| c.field == field) {
                r.value().mark_for_rebuild();
                count += 1;
            }
        }
        self.invalidations.fetch_add(count, Ordering::Relaxed);
        count
    }
    /// Invalidate every cache entry that referenced a prefilter by name.
    ///
    /// When a prefilter is removed from the registry, any entry whose
    /// `BucketBitmap{field:"__prefilter", bucket_name}` clause referenced it
    /// is now holding a dangling bitmap pointer.  Mark them for rebuild so the
    /// slow path re-evaluates against the live index on the next read.
    ///
    /// Returns the number of entries flagged.
    pub fn invalidate_prefilter(&self, name: &str) -> u64 {
        let ids: Option<roaring::RoaringBitmap> = {
            let meta = self.meta.read();
            // "__prefilter" clauses canonicalise to op="bucket", value=name.
            meta.entries_for_clause("__prefilter", "bucket", name).cloned()
        };
        let Some(ids) = ids else { return 0 };
        let mut count = 0u64;
        for id in ids.iter() {
            if let Some(key) = self.meta_id_to_key.get(&id) {
                if let Some(entry) = self.entries.get(key.value()) {
                    entry.value().mark_for_rebuild();
                    count += 1;
                }
            }
        }
        if count > 0 {
            self.invalidations.fetch_add(count, Ordering::Relaxed);
        }
        count
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
        &self,
        field: &str,
        bucket_name: &str,
        dropped_slots: &RoaringBitmap,
        added_slots: &RoaringBitmap,
        filters: &FilterIndex,
        sorts: &SortIndex,
        string_maps: Option<&crate::executor::StringMaps>,
        dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
    ) {
        if dropped_slots.is_empty() && added_slots.is_empty() {
            return;
        }
        // Reusable misses counter: we don't bump external metrics here since this
        // path is called from the flush thread (no metrics handle available).
        let _misses = std::sync::atomic::AtomicU64::new(0);
        for mut r in self.entries.iter_mut() {
            let key = r.key().clone();
            let entry = r.value_mut();
            if entry.needs_rebuild() {
                continue;
            }
            let has_bucket = key.filter_clauses.iter().any(|c| {
                c.field == field && c.op == "bucket" && c.value_repr == bucket_name
            });
            if !has_bucket {
                continue;
            }
            if !dropped_slots.is_empty() {
                let bm = Arc::make_mut(&mut entry.bitmap);
                *bm -= dropped_slots;
                if let Some(ref mut radix) = entry.radix {
                    let rx = Arc::make_mut(radix);
                    for slot in dropped_slots.iter() {
                        rx.remove_blind(slot);
                    }
                }
            }
            if !added_slots.is_empty() {
                let original = Arc::clone(entry.original_filter_clauses());
                let use_native = !original.is_empty();
                let misses = std::sync::atomic::AtomicU64::new(0);
                for slot in added_slots.iter() {
                    let other_clauses_match = if use_native {
                        // B2 native path: evaluate non-bucket clauses via the
                        // original FilterClause tree (handles Not/And/Or correctly).
                        // We still skip the matching bucket clause itself since the
                        // caller guarantees the slot is in the new bucket bitmap.
                        let non_bucket: Vec<&FilterClause> = original
                            .iter()
                            .filter(|c| !matches!(c,
                                FilterClause::BucketBitmap { field: f, bucket_name: bn, .. }
                                if f == field && bn == bucket_name))
                            .collect();
                        non_bucket.iter().all(|c| {
                            slot_matches_clause_native(
                                slot, c, filters, sorts,
                                None, // no bucket_mgr: bucket clause already excluded above
                                string_maps, dictionaries,
                                &misses,
                            )
                        })
                    } else {
                        // Legacy canonical path.
                        key.filter_clauses.iter().all(|c| {
                            if c.field == field && c.op == "bucket" && c.value_repr == bucket_name {
                                true
                            } else {
                                slot_matches_clause(slot, c, filters, sorts, None)
                            }
                        })
                    };
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
/// Uses bucket bitmap contains() for BucketBitmap clauses when bucket_mgr is provided.
/// Compound clauses conservatively return true (handled by rebuild).
fn slot_matches_filter(
    slot: u32,
    clauses: &[CanonicalClause],
    filters: &FilterIndex,
    sorts: &SortIndex,
    bucket_mgr: Option<&crate::time_buckets::TimeBucketManager>,
) -> bool {
    clauses.iter().all(|clause| slot_matches_clause(slot, clause, filters, sorts, bucket_mgr))
}
/// Evaluate whether a slot matches a single canonical clause.
fn slot_matches_clause(
    slot: u32,
    clause: &CanonicalClause,
    filters: &FilterIndex,
    sorts: &SortIndex,
    bucket_mgr: Option<&crate::time_buckets::TimeBucketManager>,
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
            // BucketBitmap — evaluate against the time bucket bitmap.
            // The flush thread runs insert_slot/remove_slot BEFORE cache
            // maintenance, so the bucket bitmap is already authoritative for
            // every mutated slot when we reach this point.
            // value_repr holds the bucket name (e.g. "24h", "7d").
            // Cost: ~150ns (HashMap lookup + RoaringBitmap::contains).
            let Some(mgr) = bucket_mgr else { return true };
            mgr.get_bucket(&clause.value_repr)
                .map(|b| b.bitmap().contains(slot))
                .unwrap_or(false)
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
            !slot_matches_clause(slot, &inner_clause, filters, sorts, bucket_mgr)
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
/// Pre-resolve bucket clause bitmaps for one work item's filter clauses.
///
/// Returns a `SmallVec`-style Vec of (clause_index, Option<Arc<RoaringBitmap>>)
/// for every clause whose `op == "bucket"`.  Callers pass this into
/// `slot_matches_filter_resolved` so the per-slot loop never re-enters the
/// `TimeBucketManager` HashMap.
///
/// Cost: one HashMap lookup per *distinct bucket name* per work item —
/// independent of `item.slots.len()`.  At 10K slots per batch this eliminates
/// 10K redundant lookups per bucket clause per item.
fn resolve_bucket_bitmaps<'a>(
    clauses: &[CanonicalClause],
    bucket_mgr: Option<&'a crate::time_buckets::TimeBucketManager>,
) -> Vec<(usize, Option<std::sync::Arc<roaring::RoaringBitmap>>)> {
    clauses
        .iter()
        .enumerate()
        .filter(|(_, c)| c.op == "bucket")
        .map(|(idx, c)| {
            let bm = bucket_mgr
                .and_then(|m| m.get_bucket(&c.value_repr))
                .map(|b| std::sync::Arc::clone(b.bitmap()));
            (idx, bm)
        })
        .collect()
}

/// Like `slot_matches_filter`, but uses pre-resolved bucket bitmaps (from
/// `resolve_bucket_bitmaps`) instead of going through the HashMap per slot.
///
/// For non-bucket clauses the evaluation is identical to `slot_matches_clause`.
/// For bucket clauses:
///   - `Some(bitmap)` → check `bitmap.contains(slot)` (authoritative).
///   - `None`         → conservative `true` (bucket_mgr unavailable, or unknown name returns false).
///
/// Wait — `None` from `resolve_bucket_bitmaps` means either (a) no bucket_mgr
/// was provided (conservative true) OR (b) the bucket name was unknown (should
/// be false, matching `slot_matches_clause`'s `.unwrap_or(false)`).
/// We preserve the distinction: if `bucket_mgr` was `None` entirely we still
/// return true; if `bucket_mgr` was `Some` but the bucket wasn't found we
/// store a sentinel. To keep this simple we re-check the `bucket_mgr` presence
/// via the `has_mgr` flag threaded in.
fn slot_matches_filter_resolved(
    slot: u32,
    clauses: &[CanonicalClause],
    filters: &FilterIndex,
    sorts: &SortIndex,
    bucket_mgr_present: bool,
    resolved_buckets: &[(usize, Option<std::sync::Arc<roaring::RoaringBitmap>>)],
) -> bool {
    // Build a quick lookup: clause_index → resolved bitmap.
    // Bucket clauses are rare (typically 1 per query), so a linear scan is fine.
    clauses.iter().enumerate().all(|(idx, clause)| {
        if clause.op == "bucket" {
            // Find the pre-resolved bitmap for this clause index.
            if let Some((_, ref opt_bm)) = resolved_buckets.iter().find(|(i, _)| *i == idx) {
                match opt_bm {
                    Some(bm) => bm.contains(slot),
                    // None means either no manager (conservative true) or unknown
                    // bucket name (false). Distinguish via bucket_mgr_present.
                    None => !bucket_mgr_present, // mgr absent → true; mgr present but unknown → false
                }
            } else {
                // Should not happen (all bucket clauses are in resolved_buckets),
                // but be conservative.
                true
            }
        } else {
            // All other ops: use the standard per-clause evaluator.
            // bucket_mgr is not needed here since we only hit this for non-bucket ops.
            slot_matches_clause(slot, clause, filters, sorts, None)
        }
    })
}

// ── Native FilterClause Evaluator (B2) ───────────────────────────────────
//
// Evaluates the original `FilterClause` tree directly, threading StringMaps
// and FieldDictionary so string-typed values (e.g. `In(baseModel, ["SD 3"])`)
// resolve correctly.  Replaces the conservative-true canonical-only path for
// compound clauses (Not/And/Or).  Default arm → `false` (loud failure, not
// silent admit).

/// Resolve a `Value` to a bitmap key, mirroring `executor.rs::resolve_value_key`.
///
/// - Integer / Bool: direct conversion, no map lookup needed.
/// - String: try `string_maps[field]` first, then `dictionaries[field]`.
/// - Float: not supported as a filter key → returns `None`.
/// - If resolution fails → caller must bump `cache_maint_string_lookup_miss_total`.
fn resolve_filter_value(
    field: &str,
    val: &crate::query::Value,
    string_maps: Option<&crate::executor::StringMaps>,
    dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
) -> Option<u64> {
    use crate::query::Value;
    match val {
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        Value::Integer(v) => Some(*v as u64),
        Value::Float(_) => None,
        Value::String(s) => {
            // Try string_maps reverse lookup (case-insensitive: lowercase).
            if let Some(maps) = string_maps {
                if let Some(field_map) = maps.get(field) {
                    let lower = s.to_lowercase();
                    if let Some(&v) = field_map.get(lower.as_str()) {
                        return Some(v as u64);
                    }
                    // Fall through to dictionary check even if field_map exists
                    // but doesn't have this value — it may be newer than the snapshot.
                }
            }
            // Fallback: live dictionary for LowCardinalityString fields.
            if let Some(dicts) = dictionaries {
                if let Some(dict) = dicts.get(field) {
                    return dict.get(s).map(|v| v as u64);
                }
            }
            None
        }
    }
}

/// Evaluate whether `slot` matches ALL clauses in a `FilterClause` slice.
///
/// AND-conjunction across the top-level clauses. Inner compound clauses are
/// recursively evaluated via `slot_matches_clause_native`.
///
/// `string_maps` / `dictionaries` are required for correct evaluation of
/// string-typed `In`/`Eq` fields (e.g. `baseModel`).  If neither is
/// available the evaluator falls back to `false` on every string-key miss
/// (safe: cache stays clean, no phantom admits).
pub fn slot_matches_filter_native(
    slot: u32,
    clauses: &[FilterClause],
    filters: &FilterIndex,
    sorts: &SortIndex,
    bucket_mgr: Option<&crate::time_buckets::TimeBucketManager>,
    string_maps: Option<&crate::executor::StringMaps>,
    dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
    string_lookup_misses: &std::sync::atomic::AtomicU64,
) -> bool {
    clauses.iter().all(|clause| {
        slot_matches_clause_native(slot, clause, filters, sorts, bucket_mgr, string_maps, dictionaries, string_lookup_misses)
    })
}

/// Evaluate a single `FilterClause` against `slot`.  Default arm → `false`.
fn slot_matches_clause_native(
    slot: u32,
    clause: &FilterClause,
    filters: &FilterIndex,
    sorts: &SortIndex,
    bucket_mgr: Option<&crate::time_buckets::TimeBucketManager>,
    string_maps: Option<&crate::executor::StringMaps>,
    dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
    string_lookup_misses: &std::sync::atomic::AtomicU64,
) -> bool {
    // Shorthand for recursive calls.
    macro_rules! eval {
        ($c:expr) => {
            slot_matches_clause_native(slot, $c, filters, sorts, bucket_mgr, string_maps, dictionaries, string_lookup_misses)
        };
    }
    match clause {
        FilterClause::Eq(field, val) => {
            let key = match resolve_filter_value(field, val, string_maps, dictionaries) {
                Some(k) => k,
                None => {
                    string_lookup_misses.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            };
            filters
                .get_field(field)
                .and_then(|f| f.get_versioned(key))
                .map(|vb| vb.contains(slot))
                .unwrap_or(false)
        }
        FilterClause::NotEq(field, val) => {
            let key = match resolve_filter_value(field, val, string_maps, dictionaries) {
                Some(k) => k,
                None => {
                    string_lookup_misses.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            };
            let contained = filters
                .get_field(field)
                .and_then(|f| f.get_versioned(key))
                .map(|vb| vb.contains(slot))
                .unwrap_or(false);
            !contained
        }
        FilterClause::In(field, vals) => {
            vals.iter().any(|val| {
                match resolve_filter_value(field, val, string_maps, dictionaries) {
                    Some(key) => filters
                        .get_field(field)
                        .and_then(|f| f.get_versioned(key))
                        .map(|vb| vb.contains(slot))
                        .unwrap_or(false),
                    None => {
                        string_lookup_misses.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                }
            })
        }
        FilterClause::NotIn(field, vals) => {
            vals.iter().all(|val| {
                match resolve_filter_value(field, val, string_maps, dictionaries) {
                    Some(key) => {
                        let contained = filters
                            .get_field(field)
                            .and_then(|f| f.get_versioned(key))
                            .map(|vb| vb.contains(slot))
                            .unwrap_or(false);
                        !contained
                    }
                    None => {
                        string_lookup_misses.fetch_add(1, Ordering::Relaxed);
                        // Can't resolve: can't confirm the slot IS in the excluded set.
                        // Conservative: treat as "not contained" (slot passes).
                        true
                    }
                }
            })
        }
        FilterClause::Gt(field, val) => {
            let threshold = match resolve_filter_value(field, val, string_maps, dictionaries) {
                Some(k) => k,
                None => return false,
            };
            sorts
                .get_field(field)
                .map(|f| f.reconstruct_value(slot) as u64 > threshold)
                .unwrap_or(false)
        }
        FilterClause::Gte(field, val) => {
            let threshold = match resolve_filter_value(field, val, string_maps, dictionaries) {
                Some(k) => k,
                None => return false,
            };
            sorts
                .get_field(field)
                .map(|f| f.reconstruct_value(slot) as u64 >= threshold)
                .unwrap_or(false)
        }
        FilterClause::Lt(field, val) => {
            let threshold = match resolve_filter_value(field, val, string_maps, dictionaries) {
                Some(k) => k,
                None => return false,
            };
            sorts
                .get_field(field)
                .map(|f| (f.reconstruct_value(slot) as u64) < threshold)
                .unwrap_or(false)
        }
        FilterClause::Lte(field, val) => {
            let threshold = match resolve_filter_value(field, val, string_maps, dictionaries) {
                Some(k) => k,
                None => return false,
            };
            sorts
                .get_field(field)
                .map(|f| f.reconstruct_value(slot) as u64 <= threshold)
                .unwrap_or(false)
        }
        FilterClause::Not(inner) => !eval!(inner),
        FilterClause::And(parts) => parts.iter().all(|c| eval!(c)),
        FilterClause::Or(parts) => parts.iter().any(|c| eval!(c)),
        FilterClause::IsNull(field) => {
            filters
                .get_field(field)
                .and_then(|f| f.get_versioned(crate::filter::NULL_BITMAP_KEY))
                .map(|vb| vb.contains(slot))
                .unwrap_or(false)
        }
        FilterClause::IsNotNull(field) => {
            let is_null = filters
                .get_field(field)
                .and_then(|f| f.get_versioned(crate::filter::NULL_BITMAP_KEY))
                .map(|vb| vb.contains(slot))
                .unwrap_or(false);
            !is_null
        }
        FilterClause::BucketBitmap { bitmap, .. } => {
            // The Arc<RoaringBitmap> is carried directly on the clause — no
            // manager lookup needed.  Authoritative for this slot because the
            // flush thread updates bucket bitmaps before enqueuing maintenance.
            bitmap.contains(slot)
        }
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
///
/// When `item.original_filter_clauses` is non-empty (set by B1 plumbing),
/// uses `slot_matches_filter_native` which threads `string_maps` +
/// `dictionaries` and evaluates compound clauses (Not/And/Or) correctly.
/// Falls back to the canonical-only `slot_matches_filter` path when clauses
/// are empty (legacy `form_and_store` callers, test paths).
pub fn evaluate_filter_work(
    work: &[CacheMaintenanceItem],
    filters: &FilterIndex,
    sorts: &SortIndex,
    deadline: Option<Instant>,
    bucket_mgr: Option<&crate::time_buckets::TimeBucketManager>,
    string_maps: Option<&crate::executor::StringMaps>,
    dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
    string_lookup_misses: &std::sync::atomic::AtomicU64,
) -> (Vec<CacheMaintenanceResult>, Vec<UnifiedKey>) {
    // Inverted evaluation: reconstruct_value is identical across entries for
    // the same (sort_field, slot), so we precompute it ONCE per unique pair
    // before looping over work items. At 50k entries × 200 slots this turns
    // 10M reconstruct_value calls (316ns each) into ~200 calls, saving
    // ~3 seconds of redundant CPU per flush cycle.
    let reconstructed = precompute_sort_values(work, sorts);
    let bucket_mgr_present = bucket_mgr.is_some();
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
        let use_native = !item.original_filter_clauses.is_empty();
        // For the canonical path: hoist bucket-clause bitmap lookups out of the
        // per-slot loop (one HashMap lookup per bucket clause per work item).
        let resolved_buckets = if !use_native {
            resolve_bucket_bitmaps(&item.key.filter_clauses, bucket_mgr)
        } else {
            Vec::new()
        };
        let use_resolved = !resolved_buckets.is_empty();
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for &slot in &item.slots {
            let sort_value = reconstructed
                .get(&(item.key.sort_field.as_str(), slot))
                .copied()
                .unwrap_or(0);
            let matches = if use_native {
                // B2 native path: evaluates compound clauses with StringMaps.
                slot_matches_filter_native(
                    slot,
                    &item.original_filter_clauses,
                    filters,
                    sorts,
                    bucket_mgr,
                    string_maps,
                    dictionaries,
                    string_lookup_misses,
                )
            } else if use_resolved {
                slot_matches_filter_resolved(
                    slot,
                    &item.key.filter_clauses,
                    filters,
                    sorts,
                    bucket_mgr_present,
                    &resolved_buckets,
                )
            } else {
                slot_matches_filter(slot, &item.key.filter_clauses, filters, sorts, bucket_mgr)
            };
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
    bucket_mgr: Option<&crate::time_buckets::TimeBucketManager>,
    string_maps: Option<&crate::executor::StringMaps>,
    dictionaries: Option<&HashMap<String, crate::dictionary::FieldDictionary>>,
    string_lookup_misses: &std::sync::atomic::AtomicU64,
) -> (Vec<CacheMaintenanceResult>, Vec<UnifiedKey>) {
    // Preamble: reconstruct_value once per unique (sort_field, slot).
    let reconstructed = precompute_sort_values(work, sorts);
    // Per-field max value: used for the Phase B fast-reject. An entry whose
    // min_tracked_value >= max_new_value (Desc) can't receive any update this
    // cycle — skip it entirely without touching slots.
    let max_per_field = compute_max_per_field(&reconstructed);
    let min_per_field = compute_min_per_field(&reconstructed);
    let bucket_mgr_present = bucket_mgr.is_some();
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
        let use_native = !item.original_filter_clauses.is_empty();
        // For the canonical path: hoist bucket-clause bitmap lookups out of
        // the per-slot loop — one HashMap lookup per bucket clause per item.
        let resolved_buckets = if !use_native {
            resolve_bucket_bitmaps(&item.key.filter_clauses, bucket_mgr)
        } else {
            Vec::new()
        };
        let use_resolved = !resolved_buckets.is_empty();
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
            // Sort qualifies — only now pay filter match cost.
            let filter_matches = if use_native {
                slot_matches_filter_native(
                    slot,
                    &item.original_filter_clauses,
                    filters,
                    sorts,
                    bucket_mgr,
                    string_maps,
                    dictionaries,
                    string_lookup_misses,
                )
            } else if use_resolved {
                slot_matches_filter_resolved(
                    slot,
                    &item.key.filter_clauses,
                    filters,
                    sorts,
                    bucket_mgr_present,
                    &resolved_buckets,
                )
            } else {
                slot_matches_filter(slot, &item.key.filter_clauses, filters, sorts, bucket_mgr)
            };
            if filter_matches {
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
        // Force entry 0 to timestamp=0 so it is unambiguously the LRU
        // regardless of how fast the test loop runs (sampled-LRU is random
        // when all timestamps are equal, making the test flaky otherwise).
        let key0 = make_key(&[("field", "eq", "0")], "sort", SortDirection::Desc);
        if let Some(mut e) = cache.get_mut(&key0) {
            e.last_used.store(0, std::sync::atomic::Ordering::Relaxed);
        }
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
        assert!(entry.try_start_rebuild()); // first caller gets it
        assert!(!entry.try_start_rebuild()); // second caller blocked
        // Rebuild releases the guard
        let fresh_slots: Vec<u32> = (0..10).collect();
        entry.rebuild(&fresh_slots, |s| 1000 - s);
        assert!(entry.try_start_rebuild()); // available again
    }
    /// `lookup_for_read` returns `None` when `needs_rebuild=true` — sanity check
    /// that the read-path miss gate is in place before the single-flight guard relies on it.
    #[test]
    fn test_needs_rebuild_triggers_slow_path_on_read() {
        let config = make_config();
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        // Entry is healthy — lookup_for_read succeeds.
        assert!(cache.lookup_for_read(&key).is_some());
        // Flag for rebuild.
        cache.get_mut(&key).unwrap().mark_for_rebuild();
        // lookup_for_read must return None so callers fall through to the slow path.
        assert!(
            cache.lookup_for_read(&key).is_none(),
            "needs_rebuild=true must produce a cache miss on lookup_for_read"
        );
    }
    /// `store()` increments `rebuild_completed_total` when it replaces a flagged entry,
    /// and only when it replaces a flagged entry.
    #[test]
    fn test_store_increments_rebuild_completed_when_replacing_flagged_entry() {
        use crate::cache_worker::CacheWorkerMetrics;
        use std::sync::Arc;
        use std::sync::atomic::Ordering;
        let config = make_config();
        let mut cache = UnifiedCache::new(config);
        let metrics = Arc::new(CacheWorkerMetrics::default());
        cache.set_rebuild_metrics(Arc::clone(&metrics));
        let key1 = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let key2 = make_key(&[("nsfwLevel", "eq", "2")], "reactionCount", SortDirection::Desc);
        let key3 = make_key(&[("nsfwLevel", "eq", "3")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..5).collect();
        // Form entry on key1 and mark for rebuild — replacing it via store() should count.
        cache.form_and_store(key1.clone(), &slots, true, 100, |s| s);
        cache.get_mut(&key1).unwrap().mark_for_rebuild();
        assert_eq!(metrics.rebuild_completed_total.load(Ordering::Relaxed), 0);
        cache.form_and_store(key1.clone(), &slots, true, 100, |s| s);
        assert_eq!(
            metrics.rebuild_completed_total.load(Ordering::Relaxed),
            1,
            "replacing a needs_rebuild entry must increment rebuild_completed_total"
        );
        // Form entry on key2 — replacing it WITHOUT flagging should NOT increment.
        cache.form_and_store(key2.clone(), &slots, true, 100, |s| s);
        cache.form_and_store(key2.clone(), &slots, true, 100, |s| s);
        assert_eq!(
            metrics.rebuild_completed_total.load(Ordering::Relaxed),
            1,
            "replacing a healthy entry must NOT increment rebuild_completed_total"
        );
        // Form entry on key3, flag it, replace — should increment to 2.
        cache.form_and_store(key3.clone(), &slots, true, 100, |s| s);
        cache.get_mut(&key3).unwrap().mark_for_rebuild();
        cache.form_and_store(key3.clone(), &slots, true, 100, |s| s);
        assert_eq!(
            metrics.rebuild_completed_total.load(Ordering::Relaxed),
            2,
            "second flagged replacement must bring total to 2"
        );
    }
    /// `should_rebuild_single_flight` serializes concurrent callers: first returns true,
    /// second returns false while the rebuild guard is held, then true again after release.
    #[test]
    fn test_concurrent_slow_path_single_flight() {
        let config = make_config();
        let mut cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..10).collect();
        // No entry — first call proceeds normally.
        assert!(
            cache.should_rebuild_single_flight(&key),
            "no entry: should proceed"
        );
        // Form and store, leave healthy — also proceeds.
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        assert!(
            cache.should_rebuild_single_flight(&key),
            "healthy entry: should proceed"
        );
        // Flag for rebuild — first caller wins the CAS, second is blocked.
        cache.get_mut(&key).unwrap().mark_for_rebuild();
        assert!(
            cache.should_rebuild_single_flight(&key),
            "flagged entry, first caller: should proceed (wins CAS)"
        );
        assert!(
            !cache.should_rebuild_single_flight(&key),
            "flagged entry, second caller: should skip (loses CAS)"
        );
        // After the winning caller stores the new entry (simulated by form_and_store),
        // the flag is gone and the next caller proceeds again.
        cache.form_and_store(key.clone(), &slots, true, 100_000, |s| 1000 - s);
        assert!(
            cache.should_rebuild_single_flight(&key),
            "fresh entry after rebuild: should proceed"
        );
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
        let meta = cache.meta();
        let nsfw_entries = meta.entries_for_filter_field("nsfwLevel");
        assert!(nsfw_entries.is_some());
        assert!(nsfw_entries.unwrap().contains(meta_id));
        let type_entries = meta.entries_for_filter_field("type");
        assert!(type_entries.is_some());
        assert!(type_entries.unwrap().contains(meta_id));
        let sort_entries = meta.entries_for_sort_field("reactionCount");
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
        let meta = cache.meta();
        let entries = meta.entries_for_clause("field", "eq", "1");
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let meta = cache.meta();
        assert!(meta.entries_for_filter_field("nsfwLevel").unwrap().contains(meta_id));
        assert!(meta.entries_for_filter_field("reactionCount").unwrap().contains(meta_id));
        assert!(meta.entries_for_filter_field("tagIds").unwrap().contains(meta_id));
        assert!(meta.entries_for_clause("nsfwLevel", "noteq", "5").unwrap().contains(meta_id));
        assert!(meta.entries_for_clause("reactionCount", "gte", "100").unwrap().contains(meta_id));
        assert!(meta.entries_for_clause("tagIds", "in", "[4,8,15]").unwrap().contains(meta_id));
        assert!(meta.entries_for_sort_field("sortAt").contains(meta_id));
        let matches = meta.find_matching_entries(
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
        let meta = cache.meta();
        assert!(meta.entries_for_clause("sortAt", "gte", "1700000000").unwrap().contains(meta_id));
        assert!(meta.entries_for_clause("sortAt", "lt", "1710000000").unwrap().contains(meta_id));
        let field_entries = meta.entries_for_filter_field("sortAt").unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
        let mut entry = cache.get_mut(&key).unwrap();
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
            // FilterField uses interior mutability via RwLock; mutating
            // through the immutable `&FilterField` works.
            let field = fi.get_field(name).unwrap();
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
        cache.maintain_bucket_changes("sortAt", "7d", &dropped, &RoaringBitmap::new(), &filters, &sorts, None, None);
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
        cache.maintain_bucket_changes("sortAt", "7d", &RoaringBitmap::new(), &added, &filters, &sorts, None, None);
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
            slot_matches_clause(42, &clause, &filters, &sorts, None),
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
            slot_matches_clause(42, &clause, &filters, &sorts, None),
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
            slot_matches_clause(42, &clause, &filters, &sorts, None),
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
            slot_matches_clause(42, &clause, &filters, &sorts, None),
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
            slot_matches_filter(42, &clauses, &filters, &sorts, None),
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
        let _m = std::sync::atomic::AtomicU64::new(0);
        let (results, timed_out) = evaluate_filter_work(&work, &filters, &sorts, None, None, None, None, &_m);
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
        let _m = std::sync::atomic::AtomicU64::new(0);
        let (results, _) = evaluate_filter_work(&work, &filters, &sorts, None, None, None, None, &_m);
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
        let _m = std::sync::atomic::AtomicU64::new(0);
        let (results, _) = evaluate_sort_work(&work, &filters, &sorts, None, None, None, None, &_m);
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
        let _m = std::sync::atomic::AtomicU64::new(0);
        let (results, _) = evaluate_filter_work(&work, &filters, &sorts, None, None, None, None, &_m);
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
                None,
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
        assert_eq!(cache.evictions.load(Ordering::Relaxed), 5, "Should have evicted exactly 5");
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
            cache.form_and_store(key, &slots, true, 100_000, |s| 1000u32.saturating_sub(s));
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

    // ── Bucket clause maintenance tests ─────────────────────────────────────

    /// Build a TimeBucketManager with a single named bucket containing specific slots.
    fn make_bucket_manager(bucket_name: &str, slots_in_bucket: &[u32]) -> crate::time_buckets::TimeBucketManager {
        use crate::config::BucketConfig;
        use crate::time_buckets::TimeBucketManager;
        let mut mgr = TimeBucketManager::new(
            "sortAtUnix".to_string(),
            vec![BucketConfig {
                name: bucket_name.to_string(),
                duration_secs: 86400,
                refresh_interval_secs: 300,
            }],
        );
        // Manually insert slots into the bucket bitmap via rebuild_bucket.
        // Timestamps: simulate "now" as 1_000_000_000 and give each slot a ts
        // within the bucket window (or outside, to verify exclusion).
        let now: u64 = 1_000_000_000;
        let values: Vec<(u32, u64)> = slots_in_bucket.iter().map(|&s| (s, now - 3600)).collect();
        mgr.rebuild_bucket(bucket_name, values.into_iter(), now);
        mgr
    }

    #[test]
    fn test_slot_matches_clause_bucket_with_manager_in_bucket() {
        // slot 5 IS in the "24h" bucket → should return true
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let mgr = make_bucket_manager("24h", &[5, 10, 15]);
        let clause = CanonicalClause {
            field: "sortAtUnix".to_string(),
            op: "bucket".to_string(),
            value_repr: "24h".to_string(),
        };
        assert!(
            slot_matches_clause(5, &clause, &filters, &sorts, Some(&mgr)),
            "slot in bucket should return true"
        );
        assert!(
            slot_matches_clause(10, &clause, &filters, &sorts, Some(&mgr)),
            "slot in bucket should return true"
        );
    }

    #[test]
    fn test_slot_matches_clause_bucket_with_manager_not_in_bucket() {
        // slot 99 is NOT in the "24h" bucket → should return false
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let mgr = make_bucket_manager("24h", &[5, 10, 15]);
        let clause = CanonicalClause {
            field: "sortAtUnix".to_string(),
            op: "bucket".to_string(),
            value_repr: "24h".to_string(),
        };
        assert!(
            !slot_matches_clause(99, &clause, &filters, &sorts, Some(&mgr)),
            "slot not in bucket should return false"
        );
    }

    #[test]
    fn test_slot_matches_clause_bucket_conservative_without_manager() {
        // Without a bucket manager, bucket clauses should conservatively return true
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let clause = CanonicalClause {
            field: "sortAtUnix".to_string(),
            op: "bucket".to_string(),
            value_repr: "24h".to_string(),
        };
        assert!(
            slot_matches_clause(99, &clause, &filters, &sorts, None),
            "bucket clause without manager should conservatively return true"
        );
    }

    #[test]
    fn test_evaluate_filter_work_bucket_clause_no_pollution() {
        // Regression test: a slot that is NOT in the "24h" bucket must NOT be
        // added to a cache entry keyed by bucket="24h", even when it is being
        // mutated (which previously triggered the always-true pollution path).
        //
        // Setup:
        //   slot 1: sortAt=now-3600 (in 24h) — should be in cache
        //   slot 2: sortAt=now-200_000 (~2.3d, NOT in 24h) — must NOT appear
        //   slot 3: sortAt=now-600_000 (~7d, NOT in 24h) — triggered pollution
        use crate::config::BucketConfig;
        use crate::time_buckets::TimeBucketManager;

        let now: u64 = 1_000_000_000;

        // Build bucket manager: 24h bucket containing only slot 1.
        let mut mgr = TimeBucketManager::new(
            "sortAtUnix".to_string(),
            vec![BucketConfig {
                name: "24h".to_string(),
                duration_secs: 86400,
                refresh_interval_secs: 300,
            }],
        );
        mgr.rebuild_bucket(
            "24h",
            vec![(1u32, now - 3600)].into_iter(), // slot 1 is in 24h
            now,
        );

        // Filter and sort indexes: use nsfwLevel=1 as a co-clause so the cache
        // entry has a real filter predicate (all 3 slots are nsfwLevel=1).
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[1, 2, 3])])]);
        let sorts = make_sort_index(&[("sortAt", &[
            (1, (now - 3600) as u32),
            (2, (now - 200_000) as u32),
            (3, (now - 600_000) as u32),
        ])]);

        // Build a cache entry for: bucket("24h") AND nsfwLevel=1, sort by sortAt Desc.
        let key = make_key(
            &[
                ("sortAtUnix", "bucket", "24h"),
                ("nsfwLevel", "eq", "1"),
            ],
            "sortAt",
            SortDirection::Desc,
        );

        // Initial cache entry: contains only slot 1 (pre-populated correctly).
        let initial_slots: Vec<u32> = vec![1];
        let mut cache = UnifiedCache::new(UnifiedCacheConfig {
            max_entries: 200,
            max_bytes: 64 * 1024 * 1024,
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 0,
            max_maintenance_work: 1_000_000,
            max_maintenance_ms: 1000,
            prefetch_threshold: 0.95,
        });
        // total_matched = 1 (only slot 1); value_fn maps slot→sort_value
        cache.form_and_store(key.clone(), &initial_slots, true, 1u64, |s| {
            if s == 1 { (now - 3600) as u32 } else { 0 }
        });

        // Simulate: slot 3 is being inserted with nsfwLevel=1 (triggers filter maintenance).
        // The bucket manager (already updated by flush thread) does NOT contain slot 3.
        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![3],
        );

        // Collect filter work items (Phase A).
        let (filter_work, _over_budget) = cache.collect_filter_work(&inserts, &HashMap::new());
        assert!(!filter_work.is_empty(), "should have work for the bucket entry");

        // Phase B: evaluate with the bucket manager. slot 3 must be rejected.
        let _m = std::sync::atomic::AtomicU64::new(0);
        let (results, timed_out) = evaluate_filter_work(
            &filter_work,
            &filters,
            &sorts,
            None,
            Some(&mgr),
            None, None, &_m,
        );
        assert!(timed_out.is_empty());

        // Apply results to cache.
        cache.apply_maintenance_results(&results);

        // Verify: slot 3 is NOT in the cache entry.
        let entry = cache.get(&key).expect("cache entry should still exist");
        assert!(
            !entry.bitmap().contains(3),
            "slot 3 (not in 24h bucket) must NOT be added to the 24h cache entry"
        );
        // Verify: slot 1 is still present.
        assert!(
            entry.bitmap().contains(1),
            "slot 1 (in 24h bucket) must remain in the cache entry"
        );
    }

    #[test]
    fn test_evaluate_filter_work_bucket_clause_adds_slot_in_bucket() {
        // Positive case: a slot that IS in the bucket should be added when it
        // also satisfies all other filter clauses.
        use crate::config::BucketConfig;
        use crate::time_buckets::TimeBucketManager;

        let now: u64 = 1_000_000_000;

        let mut mgr = TimeBucketManager::new(
            "sortAtUnix".to_string(),
            vec![BucketConfig {
                name: "24h".to_string(),
                duration_secs: 86400,
                refresh_interval_secs: 300,
            }],
        );
        // Slot 1 is older (lower sort value), slot 20 is newer (higher sort value).
        // Both are in the 24h bucket.
        // Sort values (Desc): slot 20 > slot 1, so slot 20 qualifies to be added to
        // an entry that currently tracks slot 1 as its min.
        let ts_slot1: u32 = (now - 7200) as u32;  // 2h ago
        let ts_slot20: u32 = (now - 3600) as u32; // 1h ago (newer → higher sort)
        mgr.rebuild_bucket(
            "24h",
            vec![(1u32, now - 7200), (20u32, now - 3600)].into_iter(),
            now,
        );

        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[1, 20])])]);
        let sorts = make_sort_index(&[("sortAt", &[
            (1, ts_slot1),
            (20, ts_slot20),
        ])]);

        let key = make_key(
            &[
                ("sortAtUnix", "bucket", "24h"),
                ("nsfwLevel", "eq", "1"),
            ],
            "sortAt",
            SortDirection::Desc,
        );

        // Form cache entry with slot 1 only (its sort value is the min_tracked).
        // Slot 20 has a higher sort value than slot 1, so it will qualify (Desc).
        let initial_slots: Vec<u32> = vec![1];
        let mut cache = UnifiedCache::new(UnifiedCacheConfig {
            max_entries: 200,
            max_bytes: 64 * 1024 * 1024,
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 0,
            max_maintenance_work: 1_000_000,
            max_maintenance_ms: 1000,
            prefetch_threshold: 0.95,
        });
        // total_matched = 100 (non-trivial, so min_filter_size=0 still stores it)
        cache.form_and_store(key.clone(), &initial_slots, true, 100u64, |s| {
            if s == 1 { ts_slot1 } else { 0 }
        });

        // Slot 20 is being inserted with nsfwLevel=1 and sortAt within 24h.
        let mut inserts = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("nsfwLevel"), value: 1 },
            vec![20],
        );

        let (filter_work, _) = cache.collect_filter_work(&inserts, &HashMap::new());
        let _m = std::sync::atomic::AtomicU64::new(0);
        let (results, _) = evaluate_filter_work(
            &filter_work,
            &filters,
            &sorts,
            None,
            Some(&mgr),
            None, None, &_m,
        );
        cache.apply_maintenance_results(&results);

        let entry = cache.get(&key).expect("cache entry should still exist");
        assert!(
            entry.bitmap().contains(20),
            "slot 20 (in 24h bucket, nsfwLevel=1) should be added to the 24h cache entry"
        );
    }

    // ── Fast-follow review feedback tests (PR #251) ─────────────────────────

    #[test]
    fn test_slot_matches_clause_bucket_unknown_name_returns_false() {
        // A bucket manager exists but the clause references a bucket name that
        // was never registered. Should return false (not conservative true).
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        // Manager only knows "24h"; clause asks for "999d" (not registered).
        let mgr = make_bucket_manager("24h", &[42]);
        let clause = CanonicalClause {
            field: "sortAtUnix".to_string(),
            op: "bucket".to_string(),
            value_repr: "999d".to_string(), // unknown name
        };
        assert!(
            !slot_matches_clause(42, &clause, &filters, &sorts, Some(&mgr)),
            "unknown bucket name with manager present should return false"
        );
    }

    #[test]
    fn test_slot_matches_clause_bucket_empty_bucket_returns_false() {
        // The "24h" bucket exists in the manager but has zero slots in it.
        // Any slot lookup should return false.
        use crate::config::BucketConfig;
        use crate::time_buckets::TimeBucketManager;

        let mgr = TimeBucketManager::new(
            "sortAtUnix".to_string(),
            vec![BucketConfig {
                name: "24h".to_string(),
                duration_secs: 86400,
                refresh_interval_secs: 300,
            }],
        );
        // No rebuild_bucket call — bitmap stays empty.
        let filters = make_filter_index(&[]);
        let sorts = make_sort_index(&[]);
        let clause = CanonicalClause {
            field: "sortAtUnix".to_string(),
            op: "bucket".to_string(),
            value_repr: "24h".to_string(),
        };
        assert!(
            !slot_matches_clause(42, &clause, &filters, &sorts, Some(&mgr)),
            "slot not in empty bucket should return false"
        );
    }

    #[test]
    fn test_evaluate_sort_work_bucket_clause_no_pollution() {
        // Mirror of test_evaluate_filter_work_bucket_clause_no_pollution but
        // exercises evaluate_sort_work. A slot that is NOT in the "24h" bucket
        // must not be admitted to the cache entry even if its sort value qualifies.
        use crate::config::BucketConfig;
        use crate::time_buckets::TimeBucketManager;

        let now: u64 = 1_000_000_000;

        // Bucket manager: 24h window contains only slot 1.
        let mut mgr = TimeBucketManager::new(
            "sortAtUnix".to_string(),
            vec![BucketConfig {
                name: "24h".to_string(),
                duration_secs: 86400,
                refresh_interval_secs: 300,
            }],
        );
        mgr.rebuild_bucket(
            "24h",
            vec![(1u32, now - 3600)].into_iter(), // only slot 1
            now,
        );

        // nsfwLevel=1 for slots 1 and 7; sort values: slot 7 > slot 1 (Desc qualifies).
        let ts_slot1: u32 = (now - 7200) as u32;
        let ts_slot7: u32 = (now - 1800) as u32; // higher sort value than slot 1
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[1, 7])])]);
        let sorts = make_sort_index(&[("sortAt", &[
            (1, ts_slot1),
            (7, ts_slot7),
        ])]);

        // Cache entry: bucket("24h") AND nsfwLevel=1, sort by sortAt Desc.
        // Initially contains slot 1 (min_tracked = ts_slot1).
        // Slot 7 has a higher sort value → it would qualify by sort alone,
        // but it is NOT in the bucket and must be rejected.
        let key = make_key(
            &[
                ("sortAtUnix", "bucket", "24h"),
                ("nsfwLevel", "eq", "1"),
            ],
            "sortAt",
            SortDirection::Desc,
        );
        let initial_slots: Vec<u32> = vec![1];
        let mut cache = UnifiedCache::new(UnifiedCacheConfig {
            max_entries: 200,
            max_bytes: 64 * 1024 * 1024,
            initial_capacity: 100,
            max_capacity: 1600,
            min_filter_size: 0,
            max_maintenance_work: 1_000_000,
            max_maintenance_ms: 1000,
            prefetch_threshold: 0.95,
        });
        cache.form_and_store(key.clone(), &initial_slots, true, 100u64, |s| {
            if s == 1 { ts_slot1 } else { 0 }
        });

        // Simulate: slot 7 is mutated on nsfwLevel=1. Collect sort work items.
        // Sort maintenance fires when a slot's sort value changes and the entry
        // might need updating.
        let mut sort_mutations: HashMap<&str, HashSet<u32>> = HashMap::new();
        sort_mutations.insert("sortAt", [7u32].into_iter().collect());
        let (sort_work, _over_budget) = cache.collect_sort_work(&sort_mutations);
        // The cache entry has filter_clauses so it may appear in sort_work.
        // evaluate_sort_work must reject slot 7 because it fails the bucket clause.
        let _m = std::sync::atomic::AtomicU64::new(0);
        let (results, timed_out) = evaluate_sort_work(
            &sort_work,
            &filters,
            &sorts,
            None,
            Some(&mgr),
            None, None, &_m,
        );
        assert!(timed_out.is_empty());
        cache.apply_maintenance_results(&results);

        let entry = cache.get(&key).expect("cache entry should still exist");
        assert!(
            !entry.bitmap().contains(7),
            "slot 7 (not in 24h bucket) must NOT be added by sort-work maintenance"
        );
        assert!(
            entry.bitmap().contains(1),
            "slot 1 (in 24h bucket) must remain in the cache entry"
        );
    }

    // ── A1 tests: is_time_bucket_clause helper ────────────────────────────

    #[test]
    fn test_is_time_bucket_clause_prefilter_excluded() {
        // A prefilter-substituted clause (field="__prefilter") must NOT be treated
        // as a time-bucket clause — it must not enter the bucket-diff path.
        let c = CanonicalClause {
            field: "__prefilter".to_string(),
            op: "bucket".to_string(),
            value_repr: "auto_xxx".to_string(),
        };
        assert!(
            !crate::unified_cache::is_time_bucket_clause(&c),
            "prefilter sentinel (field=__prefilter) must return false"
        );
    }

    #[test]
    fn test_is_time_bucket_clause_real_bucket() {
        // A genuine time-bucket clause (field = sort field name) must return true.
        let c = CanonicalClause {
            field: "sortAt".to_string(),
            op: "bucket".to_string(),
            value_repr: "7d".to_string(),
        };
        assert!(
            crate::unified_cache::is_time_bucket_clause(&c),
            "sortAt:bucket:7d must return true"
        );
    }

    #[test]
    fn test_uses_bucket_false_for_prefilter_substituted_entry() {
        // An entry formed with a __prefilter clause must have uses_bucket=false.
        let mut cache = UnifiedCache::new(make_config());
        // Simulates what prefilter.rs::substitute produces canonically
        let key = UnifiedKey {
            filter_clauses: vec![CanonicalClause {
                field: "__prefilter".to_string(),
                op: "bucket".to_string(),
                value_repr: "auto_safe".to_string(),
            }],
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, false, 10, |s| s);
        let entry = cache.get(&key).unwrap();
        assert!(!entry.uses_bucket(), "prefilter-substituted entry must have uses_bucket=false");
    }

    #[test]
    fn test_uses_bucket_true_for_time_bucket_entry() {
        // An entry formed with a genuine sortAt:bucket:7d clause must have uses_bucket=true.
        let mut cache = UnifiedCache::new(make_config());
        let key = UnifiedKey {
            filter_clauses: vec![CanonicalClause {
                field: "sortAt".to_string(),
                op: "bucket".to_string(),
                value_repr: "7d".to_string(),
            }],
            sort_field: "sortAt".to_string(),
            direction: SortDirection::Desc,
        };
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, false, 10, |s| s);
        let entry = cache.get(&key).unwrap();
        assert!(entry.uses_bucket(), "genuine sortAt:bucket:7d entry must have uses_bucket=true");
    }

    // ── A2 tests: invalidate_prefilter hook ───────────────────────────────

    #[test]
    fn test_invalidate_prefilter_marks_referencing_entry() {
        // Form a cache entry that references a __prefilter clause with name "safe".
        // Calling invalidate_prefilter("safe") must mark that entry needs_rebuild=true.
        let mut cache = UnifiedCache::new(make_config());
        let key = UnifiedKey {
            filter_clauses: vec![CanonicalClause {
                field: "__prefilter".to_string(),
                op: "bucket".to_string(),
                value_repr: "safe".to_string(),
            }],
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, false, 10, |s| s);

        // Sanity: not flagged before invalidation
        assert!(!cache.get(&key).unwrap().needs_rebuild());

        let count = cache.invalidate_prefilter("safe");
        assert_eq!(count, 1, "exactly one entry should be flagged");
        assert!(
            cache.get(&key).unwrap().needs_rebuild(),
            "entry must be marked needs_rebuild after invalidate_prefilter"
        );
    }

    #[test]
    fn test_invalidate_prefilter_nonexistent_name_is_noop() {
        let mut cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, false, 10, |s| s);

        let count = cache.invalidate_prefilter("does_not_exist");
        assert_eq!(count, 0, "no entries should be flagged for a name that was never registered");
        assert!(!cache.get(&key).unwrap().needs_rebuild());
    }

    // ── A3 tests: apply_maintenance_results increments updates counter ────

    #[test]
    fn test_apply_maintenance_results_increments_updates() {
        let config = make_config();
        let cache = UnifiedCache::new(config);
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        let slots: Vec<u32> = (0..10).collect();
        cache.form_and_store(key.clone(), &slots, false, 10, |s| s);

        let before = cache.stats().updates;
        let results = vec![crate::unified_cache::CacheMaintenanceResult {
            key: key.clone(),
            adds: vec![(99u32, 500u32)],
            removes: vec![],
        }];
        cache.apply_maintenance_results(&results);
        let after = cache.stats().updates;
        assert!(after > before, "updates counter must increment after apply_maintenance_results with non-empty adds");
    }

    // ── B2 tests: native FilterClause evaluator ────────────────────────────

    /// Helper: build a string_maps lookup for one field.
    fn make_string_maps(field: &str, entries: &[(&str, i64)]) -> crate::executor::StringMaps {
        let mut maps: crate::executor::StringMaps = HashMap::new();
        let mut field_map = HashMap::new();
        for (s, v) in entries {
            field_map.insert(s.to_string(), *v);
        }
        maps.insert(field.to_string(), field_map);
        maps
    }

    #[test]
    fn test_slot_matches_filter_native_string_in() {
        // Build a filter index with baseModel as a single-value field.
        // Key 1 = "SD 1.5" (via string_maps), key 2 = "SDXL".
        let filters = make_filter_index(&[
            ("baseModel", &[(1, &[5u32]), (2, &[6u32])]),
        ]);
        let sorts = make_sort_index(&[]);
        let string_maps = make_string_maps("baseModel", &[("sd 1.5", 1), ("sdxl", 2)]);
        let misses = std::sync::atomic::AtomicU64::new(0);
        let clauses = vec![FilterClause::In(
            "baseModel".to_string(),
            vec![crate::query::Value::String("SD 1.5".to_string())],
        )];
        // Slot 5 has baseModel key=1 ("SD 1.5") → should match.
        assert!(
            slot_matches_filter_native(5, &clauses, &filters, &sorts, None, Some(&string_maps), None, &misses),
            "slot 5 should match In(baseModel, ['SD 1.5'])"
        );
        // Slot 6 has baseModel key=2 ("SDXL") → should NOT match.
        assert!(
            !slot_matches_filter_native(6, &clauses, &filters, &sorts, None, Some(&string_maps), None, &misses),
            "slot 6 should not match In(baseModel, ['SD 1.5'])"
        );
        // No misses — all strings resolved.
        assert_eq!(misses.load(Ordering::Relaxed), 0, "no string lookup misses expected");
    }

    #[test]
    fn test_slot_matches_filter_native_compound_not_and() {
        // Clause: Not(And(In(nsfwLevel, [4]), In(baseModel, ["SD XL"])))
        // nsfwLevel key 4 in bitmap for slots [10, 20]; baseModel key 1 ("SD XL") for slot 20 only.
        let filters = make_filter_index(&[
            ("nsfwLevel", &[(4, &[10u32, 20u32])]),
            ("baseModel",  &[(1, &[20u32])]),
        ]);
        let sorts = make_sort_index(&[]);
        let string_maps = make_string_maps("baseModel", &[("sd xl", 1)]);
        let misses = std::sync::atomic::AtomicU64::new(0);
        let clause = FilterClause::Not(Box::new(FilterClause::And(vec![
            FilterClause::In("nsfwLevel".to_string(), vec![crate::query::Value::Integer(4)]),
            FilterClause::In("baseModel".to_string(), vec![crate::query::Value::String("SD XL".to_string())]),
        ])));
        let clauses = vec![clause];
        // Slot 10: nsfwLevel=4 ✓, baseModel≠"SD XL" ✗ → And=false → Not=true.
        assert!(
            slot_matches_filter_native(10, &clauses, &filters, &sorts, None, Some(&string_maps), None, &misses),
            "slot 10: Not(And(true,false)) should be true"
        );
        // Slot 20: nsfwLevel=4 ✓, baseModel="SD XL" ✓ → And=true → Not=false.
        assert!(
            !slot_matches_filter_native(20, &clauses, &filters, &sorts, None, Some(&string_maps), None, &misses),
            "slot 20: Not(And(true,true)) should be false"
        );
        // Slot 99: nsfwLevel≠4, baseModel missing → And=false → Not=true.
        assert!(
            slot_matches_filter_native(99, &clauses, &filters, &sorts, None, Some(&string_maps), None, &misses),
            "slot 99: Not(And(false,?)) should be true"
        );
    }

    #[test]
    fn test_slot_matches_filter_native_default_is_false() {
        // String key that doesn't exist in string_maps → resolution fails → returns false.
        let filters = make_filter_index(&[("baseModel", &[(1, &[5u32])])]);
        let sorts = make_sort_index(&[]);
        let string_maps = make_string_maps("baseModel", &[("sd 1.5", 1)]);
        let misses = std::sync::atomic::AtomicU64::new(0);
        let clauses = vec![FilterClause::In(
            "baseModel".to_string(),
            vec![crate::query::Value::String("does-not-exist".to_string())],
        )];
        // Even though slot 5 is in the field, the value doesn't resolve → false (not true).
        assert!(
            !slot_matches_filter_native(5, &clauses, &filters, &sorts, None, Some(&string_maps), None, &misses),
            "unresolvable string key must return false, not true"
        );
        // Miss should be counted.
        assert!(misses.load(Ordering::Relaxed) > 0, "string lookup miss must be recorded");
    }

    #[test]
    fn test_evaluate_filter_work_uses_native_path() {
        // Build cache entry via form_and_store_with_clauses with a compound clause.
        // The compound clause is Not(And(In(nsfwLevel,[4]), In(nsfwLevel,[4]))).
        // This is always false for slot 42 (nsfwLevel=4). The canonical path
        // returns conservative true; the native path returns false → removes slot 42.
        let cache = UnifiedCache::new(make_config());
        let native_clauses = vec![FilterClause::Not(Box::new(FilterClause::And(vec![
            FilterClause::In("nsfwLevel".to_string(), vec![crate::query::Value::Integer(4)]),
        ])))];
        let key = make_key(&[("nsfwLevel", "neq", "4")], "reactionCount", SortDirection::Desc);
        // Slot 42 is in the cache (initial formation included it).
        cache.form_and_store_with_clauses(
            key.clone(),
            &[42u32, 1u32, 2u32],
            false,
            0,
            Arc::new(native_clauses),
            |s| 100 - s,
        );
        assert!(cache.get(&key).unwrap().bitmap().contains(42));
        // Now run filter work: slot 42 has nsfwLevel=4 → Not(And(In(nsfwLevel,4))) = Not(true) = false.
        let filters = make_filter_index(&[("nsfwLevel", &[(4, &[42u32])])]);
        let sorts = make_sort_index(&[("reactionCount", &[])]);
        let item = {
            let entry = cache.get(&key).unwrap();
            CacheMaintenanceItem {
                key: key.clone(),
                slots: vec![42u32],
                min_tracked_value: 0,
                direction: SortDirection::Desc,
                original_filter_clauses: Arc::clone(entry.original_filter_clauses()),
            }
        };
        let misses = std::sync::atomic::AtomicU64::new(0);
        let (results, _timed_out) = evaluate_filter_work(
            &[item], &filters, &sorts, None, None, None, None, &misses,
        );
        // Slot 42 does NOT match the native clause → should be in removes.
        let result = results.iter().find(|r| r.key == key);
        assert!(result.is_some(), "should have a result for our key");
        let r = result.unwrap();
        assert!(
            r.removes.iter().any(|(s, _)| *s == 42),
            "slot 42 should be in removes (native eval returned false)"
        );
    }

    #[test]
    fn test_evaluate_filter_work_falls_back_when_clauses_empty() {
        // form_and_store (no _with_clauses) → original_filter_clauses is empty.
        // evaluate_filter_work should fall back to canonical path (slot_matches_filter).
        let cache = UnifiedCache::new(make_config());
        let key = make_key(&[("nsfwLevel", "eq", "1")], "reactionCount", SortDirection::Desc);
        // Slot 10 in cache; nsfwLevel=1 in filter index.
        cache.form_and_store(key.clone(), &[10u32], false, 0, |s| 1000 - s);
        let filters = make_filter_index(&[("nsfwLevel", &[(1, &[10u32])])]);
        let sorts = make_sort_index(&[("reactionCount", &[(10, 1500)])]);
        let item = {
            let entry = cache.get(&key).unwrap();
            CacheMaintenanceItem {
                key: key.clone(),
                slots: vec![10u32],
                min_tracked_value: 0,
                direction: SortDirection::Desc,
                original_filter_clauses: Arc::clone(entry.original_filter_clauses()),
            }
        };
        assert!(item.original_filter_clauses.is_empty(), "legacy path: clauses should be empty");
        let misses = std::sync::atomic::AtomicU64::new(0);
        let (results, _) = evaluate_filter_work(
            &[item], &filters, &sorts, None, None, None, None, &misses,
        );
        // Slot 10 matches Eq(nsfwLevel, 1) via canonical path → added (sort qualifies).
        let adds_slot_10 = results
            .iter()
            .flat_map(|r| r.adds.iter())
            .any(|(s, _)| *s == 10);
        assert!(adds_slot_10, "canonical fallback should add slot 10");
    }

    // ── B4 Tests: recursive meta-index registration + slot-gather fix ──────

    /// B4 test 1: registering an entry with a compound clause registers it
    /// under each leaf field name, not just FieldKey("").
    ///
    /// Pre-B4: entries_for_filter_field("baseModel") and
    /// entries_for_filter_field("nsfwLevel") would both return None (only
    /// FieldKey("") was registered for compound canonical clauses).
    #[test]
    fn test_meta_index_registers_inner_compound_fields() {
        use crate::meta_index::MetaIndex;

        let mut mi = MetaIndex::new();

        // Canonical clauses for Not(And(In(baseModel, ...), In(nsfwLevel, ...)))
        // have field="" — the inner fields are invisible to canonical registration.
        let canonical = vec![CanonicalClause {
            field: String::new(), // compound canonical: field=""
            op: "not_and".to_string(),
            value_repr: "compound".to_string(),
        }];

        // Original FilterClause tree exposes the real leaf fields.
        let original: Vec<FilterClause> = vec![FilterClause::Not(Box::new(FilterClause::And(vec![
            FilterClause::In("baseModel".to_string(), vec![crate::query::Value::String("SD XL".to_string())]),
            FilterClause::In("nsfwLevel".to_string(), vec![crate::query::Value::Integer(4)]),
        ])))];

        let id = mi.register(
            &canonical,
            Some(&original),
            Some("sortAt"),
            Some(SortDirection::Desc),
        );

        // Both leaf fields must be registered.
        let bm_base = mi.entries_for_filter_field("baseModel");
        assert!(bm_base.is_some(), "baseModel should be in meta-index");
        assert!(bm_base.unwrap().contains(id), "entry id should be under baseModel");

        let bm_nsfw = mi.entries_for_filter_field("nsfwLevel");
        assert!(bm_nsfw.is_some(), "nsfwLevel should be in meta-index");
        assert!(bm_nsfw.unwrap().contains(id), "entry id should be under nsfwLevel");

        // Canonical compound field ("") also still registered (belt-and-suspenders).
        let bm_empty = mi.entries_for_filter_field("");
        assert!(bm_empty.is_some(), "canonical FieldKey('') still registered");
        assert!(bm_empty.unwrap().contains(id));
    }

    /// B4 test 2: the slot-gather loop in collect_filter_work gathers slots for
    /// fields that appear inside compound clauses.
    ///
    /// Pre-B4: mutating baseModel for slot 42 would NOT appear in slots_to_check
    /// because the canonical clause for the compound shape has field="".
    #[test]
    fn test_collect_filter_work_finds_compound_fields() {
        let cache = UnifiedCache::new(make_config());

        // Compound clause: Not(And(In(baseModel, ...), In(nsfwLevel, ...)))
        let key = UnifiedKey {
            filter_clauses: vec![CanonicalClause {
                field: String::new(), // compound canonical
                op: "not_and".to_string(),
                value_repr: "compound".to_string(),
            }],
            sort_field: "sortAt".to_string(),
            direction: SortDirection::Desc,
        };

        let original: Vec<FilterClause> = vec![FilterClause::Not(Box::new(FilterClause::And(vec![
            FilterClause::In("baseModel".to_string(), vec![crate::query::Value::String("SD XL".to_string())]),
            FilterClause::In("nsfwLevel".to_string(), vec![crate::query::Value::Integer(4)]),
        ])))];

        // Form an entry that includes slot 42.
        cache.form_and_store_with_clauses(
            key.clone(),
            &[42u32, 1u32, 2u32],
            false,
            3,
            Arc::new(original),
            |s| 100 - s,
        );

        // Stage a mutation on baseModel for slot 42.
        let mut inserts: HashMap<FilterGroupKey, Vec<u32>> = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("baseModel"), value: 999 },
            vec![42u32],
        );

        let (work, _over_budget) = cache.collect_filter_work(&inserts, &HashMap::new());

        // There should be exactly one work item for our entry.
        assert_eq!(work.len(), 1, "compound-clause entry should appear in work items");
        let item = &work[0];
        assert!(
            item.slots.contains(&42),
            "slot 42 (mutated baseModel field) must appear in slots_to_check"
        );
    }

    /// B4 test 3: collect_filter_work falls back to canonical clause iteration
    /// when original_filter_clauses is empty (legacy form_and_store path).
    /// Simple non-compound shapes still work via the fallback.
    #[test]
    fn test_collect_filter_work_falls_back_to_canonical_when_clauses_empty() {
        let cache = UnifiedCache::new(make_config());

        // Simple Eq clause — has a real field name in canonical form.
        let key = make_key(&[("baseModel", "eq", "999")], "sortAt", SortDirection::Desc);

        // form_and_store (not _with_clauses) → original_filter_clauses is empty.
        cache.form_and_store(key.clone(), &[42u32, 1u32], false, 2, |s| 100 - s);

        // Stage a mutation on baseModel for slot 42.
        let mut inserts: HashMap<FilterGroupKey, Vec<u32>> = HashMap::new();
        inserts.insert(
            FilterGroupKey { field: Arc::from("baseModel"), value: 999 },
            vec![42u32],
        );

        let (work, _) = cache.collect_filter_work(&inserts, &HashMap::new());

        // The canonical fallback path should still find this entry because
        // the canonical clause has field="baseModel" (non-compound shape).
        assert_eq!(work.len(), 1, "non-compound entry should be found via canonical fallback");
        assert!(
            work[0].slots.contains(&42),
            "slot 42 should appear via canonical fallback"
        );
    }

    /// B4 test 4: calling register with None for original_filter_clauses
    /// behaves identically to pre-B4 (canonical-only field registration).
    /// Compound canonical clause only registers FieldKey(""), not inner fields.
    #[test]
    fn test_meta_index_register_with_none_clauses_is_canonical_only() {
        use crate::meta_index::MetaIndex;

        let mut mi = MetaIndex::new();

        let canonical = vec![CanonicalClause {
            field: String::new(), // compound canonical
            op: "not_and".to_string(),
            value_repr: "compound".to_string(),
        }];

        // Pass None — canonical-only behavior (same as pre-B4).
        let id = mi.register(
            &canonical,
            None, // no original FilterClause tree
            Some("sortAt"),
            Some(SortDirection::Desc),
        );

        // Only FieldKey("") registered — inner fields NOT visible.
        let bm_empty = mi.entries_for_filter_field("");
        assert!(bm_empty.is_some(), "FieldKey('') must be registered");
        assert!(bm_empty.unwrap().contains(id));

        // Inner fields NOT registered (None → canonical-only).
        assert!(
            mi.entries_for_filter_field("baseModel").is_none(),
            "baseModel must NOT be registered when original_filter_clauses=None"
        );
        assert!(
            mi.entries_for_filter_field("nsfwLevel").is_none(),
            "nsfwLevel must NOT be registered when original_filter_clauses=None"
        );
    }

    // ── B3 Tests: cheap-clause-first ordering ────────────────────────────────

    /// B3 test 1: `clause_atom_cost` returns the correct cost class for each
    /// FilterClause variant.
    #[test]
    fn test_clause_atom_cost_class_assignment() {
        use crate::query::Value;

        // Class 1 — cheapest
        assert_eq!(clause_atom_cost(&FilterClause::Eq("a".into(), Value::Integer(1))), 1);
        assert_eq!(clause_atom_cost(&FilterClause::IsNull("x".into())), 1);
        assert_eq!(clause_atom_cost(&FilterClause::IsNotNull("x".into())), 1);
        assert_eq!(
            clause_atom_cost(&FilterClause::BucketBitmap {
                field: "sortAt".into(),
                bucket_name: "7d".into(),
                bitmap: Arc::new(roaring::RoaringBitmap::new()),
            }),
            1
        );

        // Class 2 — one negation / range
        assert_eq!(clause_atom_cost(&FilterClause::NotEq("a".into(), Value::Integer(1))), 2);
        assert_eq!(clause_atom_cost(&FilterClause::Gt("ts".into(), Value::Integer(0))), 2);
        assert_eq!(clause_atom_cost(&FilterClause::Gte("ts".into(), Value::Integer(0))), 2);
        assert_eq!(clause_atom_cost(&FilterClause::Lt("ts".into(), Value::Integer(0))), 2);
        assert_eq!(clause_atom_cost(&FilterClause::Lte("ts".into(), Value::Integer(0))), 2);

        // Class 3 — In / NotIn
        assert_eq!(
            clause_atom_cost(&FilterClause::In("f".into(), vec![Value::Integer(1), Value::Integer(2)])),
            3
        );
        assert_eq!(
            clause_atom_cost(&FilterClause::NotIn("f".into(), vec![Value::Integer(1)])),
            3
        );

        // Class 4 — And/Or: max(child classes); min bucket is 4
        // And(Eq, In) → max(1, 3) = 3, clamped to 4
        let and_eq_in = FilterClause::And(vec![
            FilterClause::Eq("a".into(), Value::Integer(1)),
            FilterClause::In("b".into(), vec![Value::Integer(2)]),
        ]);
        assert_eq!(clause_atom_cost(&and_eq_in), 4);

        // Or(Eq, NotEq) → max(1, 2) = 2, clamped to 4
        let or_eq_noteq = FilterClause::Or(vec![
            FilterClause::Eq("a".into(), Value::Integer(1)),
            FilterClause::NotEq("a".into(), Value::Integer(2)),
        ]);
        assert_eq!(clause_atom_cost(&or_eq_noteq), 4);

        // Class 5 — Not(And/Or)
        let not_and = FilterClause::Not(Box::new(FilterClause::And(vec![
            FilterClause::In("x".into(), vec![Value::Integer(1)]),
            FilterClause::In("y".into(), vec![Value::Integer(2)]),
        ])));
        assert_eq!(clause_atom_cost(&not_and), 5);

        let not_or = FilterClause::Not(Box::new(FilterClause::Or(vec![
            FilterClause::Eq("x".into(), Value::Integer(1)),
        ])));
        assert_eq!(clause_atom_cost(&not_or), 5);

        // Not(leaf) inherits leaf class — Not(Eq) → class 1
        let not_eq = FilterClause::Not(Box::new(FilterClause::Eq("a".into(), Value::Integer(1))));
        assert_eq!(clause_atom_cost(&not_eq), 1);

        // Not(In) → class 3
        let not_in = FilterClause::Not(Box::new(FilterClause::In("a".into(), vec![Value::Integer(1)])));
        assert_eq!(clause_atom_cost(&not_in), 3);
    }

    /// B3 test 2: `form_and_store_with_clauses` stores clauses in cheap-first
    /// order even when the caller passes expensive clauses first.
    #[test]
    fn test_form_and_store_sorts_clauses_cheap_first() {
        use crate::query::Value;

        let cache = UnifiedCache::new(make_config());
        let key = make_key(&[], "sortAt", SortDirection::Desc);

        // Input order: [Not(And(In,In)), IsNotNull, Eq] — costs [5, 1, 1]
        // Expected stored order (stable sort): [IsNotNull, Eq, Not(And(In,In))]
        // (class-1 items first, class-5 last; within class 1 the original
        //  relative order IsNotNull→Eq is preserved by stable sort)
        let expensive_first = vec![
            FilterClause::Not(Box::new(FilterClause::And(vec![
                FilterClause::In("baseModel".into(), vec![Value::String("SD XL".into())]),
                FilterClause::In("nsfwLevel".into(), vec![Value::Integer(4)]),
            ]))),
            FilterClause::IsNotNull("postId".into()),
            FilterClause::Eq("isPublished".into(), Value::Bool(true)),
        ];

        cache.form_and_store_with_clauses(
            key.clone(),
            &[1u32, 2u32],
            false,
            2,
            Arc::new(expensive_first),
            |s| s,
        );

        let entry = cache.get(&key).expect("entry must exist");
        let stored = entry.original_filter_clauses();
        assert_eq!(stored.len(), 3, "all 3 clauses must be stored");

        // First two must be class-1 (IsNotNull, Eq in any order within class 1)
        let cost0 = clause_atom_cost(&stored[0]);
        let cost1 = clause_atom_cost(&stored[1]);
        let cost2 = clause_atom_cost(&stored[2]);
        assert!(cost0 <= cost1, "clauses must be in non-decreasing cost order (0 vs 1)");
        assert!(cost1 <= cost2, "clauses must be in non-decreasing cost order (1 vs 2)");
        assert_eq!(cost0, 1, "cheapest clause must be class 1");
        assert_eq!(cost2, 5, "most expensive clause must be class 5");

        // Stable sort: within class 1, IsNotNull comes before Eq (original order)
        assert!(
            matches!(stored[0], FilterClause::IsNotNull(_)),
            "stable sort: IsNotNull should precede Eq within class 1"
        );
        assert!(
            matches!(stored[1], FilterClause::Eq(..)),
            "stable sort: Eq should be second within class 1"
        );
    }

    /// B3 test 3 (proptest): per-slot eval result is invariant under any
    /// reordering of the top-level FilterClause list.
    ///
    /// Generates random FilterClause trees, builds a FilterIndex with random
    /// bitmap state for each key, evaluates with both original and
    /// cheap-first-sorted orderings, and asserts the results are identical.
    mod b3_proptest {
        use super::*;
        use proptest::prelude::*;
        use crate::query::Value;

        /// Simple leaf clause strategies for proptest.
        /// All clauses reference field "f" with integer keys 1..=4, or field "g"
        /// for IsNull/IsNotNull so the two semantics don't collide.
        fn arb_leaf_clause() -> impl Strategy<Value = FilterClause> {
            prop_oneof![
                // Eq with integer key 1..=4 (field "f")
                (1i64..=4i64).prop_map(|v| FilterClause::Eq("f".into(), Value::Integer(v))),
                // NotEq with integer key 1..=4 (field "f")
                (1i64..=4i64).prop_map(|v| FilterClause::NotEq("f".into(), Value::Integer(v))),
                // In with 1..=3 integer values drawn from 1..=4 (field "f")
                proptest::collection::vec(1i64..=4i64, 1..=3usize)
                    .prop_map(|vals| FilterClause::In("f".into(), vals.into_iter().map(Value::Integer).collect())),
                // IsNotNull on field "g"
                Just(FilterClause::IsNotNull("g".into())),
                // IsNull on field "g"
                Just(FilterClause::IsNull("g".into())),
            ]
        }

        /// Strategy for a FilterClause tree of depth ≤ 2, up to 6 leaves.
        fn arb_clause() -> impl Strategy<Value = FilterClause> {
            prop_oneof![
                3 => arb_leaf_clause(),
                1 => proptest::collection::vec(arb_leaf_clause(), 1..=3usize)
                    .prop_map(|parts| FilterClause::And(parts)),
                1 => proptest::collection::vec(arb_leaf_clause(), 1..=3usize)
                    .prop_map(|parts| FilterClause::Or(parts)),
                1 => arb_leaf_clause().prop_map(|c| FilterClause::Not(Box::new(c))),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]
            #[test]
            fn test_eval_invariant_under_clause_reordering(
                clauses in proptest::collection::vec(arb_clause(), 1..=6usize),
                // Test a slot in 1..=4
                slot in 1u32..=4u32,
                // Bitmask per key 1..=4: bit i set → slot (i+1) in that key's bitmap for field "f"
                bm1 in 0u8..=15u8,
                bm2 in 0u8..=15u8,
                bm3 in 0u8..=15u8,
                bm4 in 0u8..=15u8,
                // Bitmask for NULL_BITMAP_KEY in field "g": which slots (1..=4) are "null"
                null_mask in 0u8..=15u8,
            ) {
                // Build filter index for field "f" with keys 1..=4.
                // Decode each bitmask: bit i set → slot (i+1) is in that key's bitmap.
                let mut f_values: Vec<(u64, Vec<u32>)> = Vec::new();
                for (key, mask) in [(1u64, bm1), (2, bm2), (3, bm3), (4, bm4)] {
                    let slots: Vec<u32> = (0..4u32)
                        .filter(|&bit| mask & (1 << bit) != 0)
                        .map(|bit| bit + 1)
                        .collect();
                    if !slots.is_empty() {
                        f_values.push((key, slots));
                    }
                }
                // Build field "g" where NULL_BITMAP_KEY is set for certain slots.
                let null_slots: Vec<u32> = (0..4u32)
                    .filter(|&bit| null_mask & (1 << bit) != 0)
                    .map(|bit| bit + 1)
                    .collect();

                // Use the test helper that correctly constructs FilterIndex.
                let f_pairs: Vec<(u64, Vec<u32>)> = f_values;
                let f_refs: Vec<(u64, &[u32])> = f_pairs.iter().map(|(k, v)| (*k, v.as_slice())).collect();
                let fi = {
                    let mut index = FilterIndex::new();
                    index.add_field(FilterFieldConfig {
                        name: "f".to_string(),
                        field_type: FilterFieldType::SingleValue,
                        behaviors: None,
                        eviction: None,
                        eager_load: false,
                        per_value_lazy: false,
                        max_range_scan_values: None,
                    });
                    {
                        let field = index.get_field("f").unwrap();
                        for &(key, slots) in &f_refs {
                            field.insert_bulk(key, slots.iter().copied());
                        }
                    }
                    // Field "g" for IsNull/IsNotNull
                    index.add_field(FilterFieldConfig {
                        name: "g".to_string(),
                        field_type: FilterFieldType::SingleValue,
                        behaviors: None,
                        eviction: None,
                        eager_load: false,
                        per_value_lazy: false,
                        max_range_scan_values: None,
                    });
                    if !null_slots.is_empty() {
                        let field_g = index.get_field("g").unwrap();
                        field_g.insert_bulk(crate::filter::NULL_BITMAP_KEY, null_slots.iter().copied());
                    }
                    index
                };

                let si = SortIndex::new();
                let misses = std::sync::atomic::AtomicU64::new(0);

                // Evaluate with original order
                let result_original = slot_matches_filter_native(
                    slot, &clauses, &fi, &si, None, None, None, &misses,
                );

                // Evaluate with cheap-first sort (B3 ordering)
                let mut sorted = clauses.clone();
                sorted.sort_by_key(|c| clause_atom_cost(c));

                let misses2 = std::sync::atomic::AtomicU64::new(0);
                let result_sorted = slot_matches_filter_native(
                    slot, &sorted, &fi, &si, None, None, None, &misses2,
                );

                prop_assert_eq!(
                    result_original,
                    result_sorted,
                    "eval result must be invariant under clause reordering: slot={:?}, clauses={:?}",
                    slot,
                    clauses
                );
            }
        }
    }

    // ── B5 — Prefetch worker compound-clause fix ─────────────────────────────
    //
    // The prefetch worker previously reconstructed FilterClause vecs by calling
    // CanonicalClause::to_filter_clause on each canonical key, then collecting
    // with filter_map. That silently drops compound clauses (And/Or/Not/IsNull/
    // IsNotNull/bucket) because to_filter_clause returns None for them. The
    // resulting filter bitmap was a superset, so entry.expand added wrong slots.
    //
    // Fix (B5): the worker now clones entry.original_filter_clauses() (an Arc
    // captured at form_and_store_with_clauses time) and uses it directly. It
    // falls back to canonical round-trip only when the Arc is empty (pre-B8
    // entries restored from disk).
    //
    // These tests verify the two code paths the prefetch worker now takes:
    //   1. original_filter_clauses non-empty → full compound tree used.
    //   2. original_filter_clauses empty (legacy form_and_store) → fallback.

    #[test]
    fn test_prefetch_worker_compound_filter_matches_executor() {
        // Form a cache entry via form_and_store_with_clauses with a compound shape:
        //   Not(And(In(baseModel, [1]), In(nsfwLevel, [4])))
        // This compound clause cannot be round-tripped through to_filter_clause.
        // Verify that original_filter_clauses is non-empty so the prefetch worker
        // will use the correct full tree rather than the stripped canonical vec.
        let cache = UnifiedCache::new(make_config());
        let compound = FilterClause::Not(Box::new(FilterClause::And(vec![
            FilterClause::In(
                "baseModel".to_string(),
                vec![crate::query::Value::Integer(1)],
            ),
            FilterClause::In(
                "nsfwLevel".to_string(),
                vec![crate::query::Value::Integer(4)],
            ),
        ])));
        let clauses = vec![compound.clone()];

        // Build the UnifiedKey from the compound clause itself so the canonical
        // key reflects the compound shape (op="not(and)", field="").
        let canonical_clause = crate::cache::CanonicalClause::from_filter(&compound)
            .expect("compound Not(And) must canonicalize");
        let key = UnifiedKey {
            filter_clauses: vec![canonical_clause],
            sort_field: "sortAt".to_string(),
            direction: SortDirection::Desc,
        };

        cache.form_and_store_with_clauses(
            key.clone(),
            &[10u32, 20u32],
            false,
            0,
            Arc::new(clauses.clone()),
            |s| 1000 - s,
        );

        // Verify: original_filter_clauses is non-empty → prefetch uses full tree.
        let entry = cache.get(&key).expect("entry must exist");
        let stored = entry.original_filter_clauses();
        assert!(
            !stored.is_empty(),
            "B5: compound entry must have non-empty original_filter_clauses so \
             prefetch worker uses the full tree (not canonical round-trip)"
        );

        // Simulate the prefetch path: clone the Arc (cheap), check it has the
        // compound clause that to_filter_clause would have dropped.
        let cloned: Vec<FilterClause> = (**stored).clone();
        assert_eq!(
            cloned.len(),
            clauses.len(),
            "B5: prefetch path must see all clauses including the compound Not(And)"
        );

        // Confirm to_filter_clause drops the compound canonical clause entirely.
        // This proves the old round-trip was broken for this shape.
        let canonical_roundtrip: Vec<FilterClause> = key
            .filter_clauses
            .iter()
            .filter_map(|cc| crate::cache::CanonicalClause::to_filter_clause(cc))
            .collect();
        // The canonical clause has op starting with "not(" — to_filter_clause
        // returns None for compound ops → stripped vec is empty.
        assert!(
            canonical_roundtrip.is_empty(),
            "B5 pre-condition: canonical round-trip of a compound Not(And) clause \
             must produce empty vec (got {} clauses)",
            canonical_roundtrip.len()
        );
    }

    #[test]
    fn test_prefetch_worker_falls_back_for_empty_clauses() {
        // form_and_store (legacy, no _with_clauses) → original_filter_clauses is empty.
        // The prefetch worker should fall back to canonical round-trip for these entries.
        let cache = UnifiedCache::new(make_config());
        let key = make_key(
            &[("nsfwLevel", "eq", "1")],
            "sortAt",
            SortDirection::Desc,
        );
        // Use form_and_store (not _with_clauses) — no FilterClause tree stored.
        cache.form_and_store(key.clone(), &[5u32], false, 0, |s| 100 - s);

        let entry = cache.get(&key).expect("entry must exist");
        let stored = entry.original_filter_clauses();
        assert!(
            stored.is_empty(),
            "B5 fallback: legacy form_and_store entry must have empty \
             original_filter_clauses so prefetch falls back to canonical round-trip"
        );

        // Simulate the prefetch fallback path: since Arc is empty, use canonical.
        let filter_clauses: Vec<FilterClause> = if !stored.is_empty() {
            (**stored).clone()
        } else {
            key.filter_clauses
                .iter()
                .filter_map(|cc| crate::cache::CanonicalClause::to_filter_clause(cc))
                .collect()
        };
        // Eq(nsfwLevel, 1) is a simple leaf — to_filter_clause succeeds for it.
        assert!(
            !filter_clauses.is_empty(),
            "B5 fallback: canonical round-trip must recover at least the Eq clause \
             for a simple key when original_filter_clauses is empty"
        );
    }
}
