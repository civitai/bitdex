use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::sync::Arc;
use roaring::RoaringBitmap;
use parking_lot::{Mutex, RwLock};
use crate::config::FilterFieldConfig;
use crate::versioned_bitmap::VersionedBitmap;

/// Reserved bitmap key for null values on nullable filter fields.
/// Null ops insert/remove this key in the existing value bitmap HashMap,
/// so the entire persistence/coalescing/flush/eviction pipeline works unchanged.
pub const NULL_BITMAP_KEY: u64 = u64::MAX;
/// Filter bitmap storage for a single field.
///
/// Each distinct value gets its own VersionedBitmap containing all slot positions
/// that have that value. This is the core of Bitdex's filtering.
///
/// Field types:
/// - single_value: each slot appears in exactly one bitmap per field
/// - multi_value: each slot can appear in multiple bitmaps (e.g., tags)
/// - boolean: two bitmaps (true/false), stored as values 0 and 1
///
/// Bitmaps use VersionedBitmap for deferred diff compaction and cheap snapshot cloning.
///
/// # Concurrency model
///
/// `bitmaps` is wrapped in a `parking_lot::RwLock` so mutations only need
/// a `&FilterField` reference — never `&mut FilterField`. This eliminates
/// the `Arc::make_mut` cascade that previously deep-cloned the entire
/// 22.5M-entry HashMap on every flush cycle for high-cardinality fields
/// like `postId`. With interior mutability, mutating one bitmap touches
/// only that one bitmap's diff layer (~microseconds), regardless of
/// how many distinct values the field has.
///
/// **Snapshot semantics relaxation:** Because mutations go through a
/// shared lock, the published ArcSwap snapshot's filter bitmaps are no
/// longer frozen for the duration of a query. Readers see eventually-
/// consistent state — individual bitmap reads return a coherent
/// `Arc<RoaringBitmap>` (since `VersionedBitmap` already uses
/// per-bitmap CoW), but two consecutive reads of the same field may
/// observe a value bitmap that's grown between them. This is correct
/// for BitDex's existing query semantics.
///
/// **Reader pattern:** acquire read lock, clone the
/// `VersionedBitmap` (cheap — internal Arcs), drop the lock, then
/// perform query work. Never hold the read lock across bitmap
/// intersection or doc fetch.
pub struct FilterField {
    /// One bitmap per distinct value. Key is the u64 bitmap key.
    /// Wrapped in RwLock for interior mutability — see struct doc.
    bitmaps: RwLock<HashMap<u64, VersionedBitmap>>,
    /// Side index of values whose VersionedBitmap has been mutated since
    /// the last successful merge. `merge_dirty()` walks only these values
    /// instead of iterating the full HashMap (769K entries for userId,
    /// 22.5M for postId) under the write lock.
    ///
    /// Without this set, every merge cycle held the bitmaps write lock
    /// long enough to walk the entire field — measured at 100 ms wait
    /// time on `remove_bulk` calls trying to acquire the lock during a
    /// merge cycle, which dominated flush apply on userId and produced
    /// the 142 ms apply-phase bottleneck observed in prod.
    ///
    /// Mutation paths (insert / remove / insert_bulk / remove_bulk /
    /// remove_from_all / insert_versioned-when-dirty) push values into
    /// this set. `merge_dirty()` drains the set, takes the write lock,
    /// and merges only the listed values. Unloaded-with-diff values
    /// (lazy fields where the base isn't in memory yet) get re-queued
    /// — `merge()` is a no-op until lazy-load brings the base in, but
    /// they must stay tracked so the diff isn't lost.
    dirty_values: Mutex<HashSet<u64>>,
    /// Field configuration.
    config: FilterFieldConfig,
}

// Manual Clone impl: takes the read lock and deep-clones the inner HashMap.
// **WARNING:** This is intentionally SLOW for high-cardinality fields
// (cloning 22.5M entries is ~1.6 GB memcpy). It exists for non-hot paths
// (tests, restoration, ad-hoc snapshots) and must NEVER be called from
// the flush thread or query path.
impl Clone for FilterField {
    fn clone(&self) -> Self {
        let r = self.bitmaps.read();
        let cloned_dirty = self.dirty_values.lock().clone();
        Self {
            bitmaps: RwLock::new(r.clone()),
            dirty_values: Mutex::new(cloned_dirty),
            config: self.config.clone(),
        }
    }
}
impl FilterField {
    pub fn new(config: FilterFieldConfig) -> Self {
        Self {
            bitmaps: RwLock::new(HashMap::new()),
            dirty_values: Mutex::new(HashSet::new()),
            config,
        }
    }
    /// Mark a value's VersionedBitmap as dirty so the next `merge_dirty()`
    /// cycle will pick it up. Cheap — single hash insert under a brief mutex.
    #[inline]
    fn mark_dirty(&self, value: u64) {
        self.dirty_values.lock().insert(value);
    }
    /// Bulk-load bitmaps from a map of (value -> bitmap).
    /// Used during startup to restore Tier 1 filter state from redb.
    /// Each bitmap becomes a VersionedBitmap base (no dirty diff).
    pub fn load_from(&self, data: HashMap<u64, RoaringBitmap>) {
        let mut w = self.bitmaps.write();
        for (value, bitmap) in data {
            w.insert(value, VersionedBitmap::new(bitmap));
        }
    }
    /// Get the field configuration.
    pub fn config(&self) -> &FilterFieldConfig {
        &self.config
    }
    /// Get the field name.
    pub fn name(&self) -> &str {
        &self.config.name
    }
    /// Get the field type.
    pub fn field_type(&self) -> &FilterFieldType {
        &self.config.field_type
    }
    /// Set a slot's bit in the bitmap for the given value.
    pub fn insert(&self, value: u64, slot: u32) {
        let mut w = self.bitmaps.write();
        w.entry(value)
            .or_insert_with(VersionedBitmap::new_empty)
            .insert(slot);
        drop(w);
        self.mark_dirty(value);
    }
    /// Clear a slot's bit from the bitmap for the given value.
    /// Always records the diff, even if the value is unloaded — creates a diff-only
    /// placeholder so the remove is preserved until the base is reloaded from disk.
    pub fn remove(&self, value: u64, slot: u32) {
        let mut w = self.bitmaps.write();
        w.entry(value)
            .or_insert_with(VersionedBitmap::new_unloaded)
            .remove(slot);
        drop(w);
        self.mark_dirty(value);
    }
    /// Bulk-insert multiple slots into the bitmap for the given value.
    /// Slots should be sorted for maximum roaring-rs `extend()` performance.
    pub fn insert_bulk(&self, value: u64, slots: impl IntoIterator<Item = u32>) {
        let t_lock = std::time::Instant::now();
        let mut w = self.bitmaps.write();
        let lock_us = t_lock.elapsed().as_micros();
        let t_entry = std::time::Instant::now();
        let entry = w.entry(value).or_insert_with(VersionedBitmap::new_empty);
        let entry_us = t_entry.elapsed().as_micros();
        let t_insert = std::time::Instant::now();
        entry.insert_bulk(slots);
        let insert_us = t_insert.elapsed().as_micros();
        drop(w);
        self.mark_dirty(value);
        // Only log unusually slow calls (>5ms total) so we don't drown the logs.
        let total_us = lock_us + entry_us + insert_us;
        if total_us > 5_000 {
            tracing::warn!(
                "[insert_bulk_slow] field={} value={} lock={}μs entry={}μs insert={}μs",
                self.config.name, value, lock_us, entry_us, insert_us
            );
        }
    }
    /// OR a RoaringBitmap directly into the base for the given value.
    /// Bypasses the diff layer for maximum bulk-load throughput.
    /// Creates the VersionedBitmap if it doesn't exist.
    pub fn or_bitmap(&self, value: u64, bitmap: &RoaringBitmap) {
        let mut w = self.bitmaps.write();
        w.entry(value)
            .or_insert_with(VersionedBitmap::new_empty)
            .or_into_base(bitmap);
        // or_into_base writes the base directly, leaving the diff
        // untouched, so the resulting VB is not "dirty" in the
        // diff sense. Don't mark it.
    }
    /// Bulk-remove multiple slots from the bitmap for the given value.
    /// If the value isn't in memory (lazy-loaded field), creates an unloaded
    /// entry with pending removes in the diff layer. The removes will be
    /// applied when the base is loaded during compaction or lazy-load.
    pub fn remove_bulk(&self, value: u64, slots: &[u32]) {
        let mut w = self.bitmaps.write();
        let vb = w.entry(value)
            .or_insert_with(VersionedBitmap::new_unloaded);
        for &slot in slots {
            vb.remove(slot);
        }
        drop(w);
        self.mark_dirty(value);
    }
    /// Clear a slot's bit from ALL bitmaps in this field.
    /// Used by autovac to clean dead slots from filter bitmaps.
    pub fn remove_from_all(&self, slot: u32) {
        let mut touched: Vec<u64> = Vec::new();
        {
            let mut w = self.bitmaps.write();
            for (k, vb) in w.iter_mut() {
                vb.remove(slot);
                touched.push(*k);
            }
        }
        let mut set = self.dirty_values.lock();
        for v in touched {
            set.insert(v);
        }
    }
    /// Get the base bitmap for a specific value as an owned `Arc` clone.
    ///
    /// Previously returned `&RoaringBitmap`. With interior mutability the
    /// outer reference can't escape the lock, so this returns an `Arc`
    /// clone of the inner base — refcount bump only, no data copy. The
    /// returned bitmap is a coherent point-in-time snapshot.
    pub fn get(&self, value: u64) -> Option<Arc<RoaringBitmap>> {
        let r = self.bitmaps.read();
        r.get(&value).map(|vb| Arc::clone(vb.base()))
    }
    /// Get a cloned `VersionedBitmap` for a specific value.
    ///
    /// Previously returned `&VersionedBitmap`. With interior mutability
    /// we return a cloned VB instead — `VersionedBitmap::clone()` is
    /// cheap (Arc refcount bumps on `base` and `diff`, no data copy).
    pub fn get_versioned(&self, value: u64) -> Option<VersionedBitmap> {
        let r = self.bitmaps.read();
        r.get(&value).cloned()
    }
    /// Closure-based access to a `VersionedBitmap` while holding the read
    /// lock. Avoids the VB clone if the closure only needs to peek.
    /// **Holds the read lock for the duration of the closure** — keep
    /// it short.
    pub fn with_versioned<R>(&self, value: u64, f: impl FnOnce(Option<&VersionedBitmap>) -> R) -> R {
        let r = self.bitmaps.read();
        f(r.get(&value))
    }
    /// Snapshot all value keys currently in this field's bitmap HashMap.
    /// Acquires the read lock briefly and returns owned keys.
    pub fn bitmap_keys(&self) -> Vec<u64> {
        let r = self.bitmaps.read();
        r.keys().copied().collect()
    }
    /// Remove a value's bitmap from the field (used by idle eviction).
    /// The bitmap can be re-loaded from disk on the next query.
    pub fn remove_value(&self, value: u64) {
        let mut w = self.bitmaps.write();
        w.remove(&value);
        drop(w);
        self.dirty_values.lock().remove(&value);
    }
    /// Number of distinct values currently loaded in memory.
    pub fn loaded_value_count(&self) -> usize {
        self.bitmaps.read().len()
    }
    /// Get the fused bitmap for a single value against a candidate set.
    /// Applies the diff (sets/clears) to the intersection of base and candidates.
    /// This is the primary diff-aware read path for Eq/NotEq queries.
    pub fn apply_diff_eq(&self, value: u64, candidates: &RoaringBitmap) -> Option<RoaringBitmap> {
        let r = self.bitmaps.read();
        r.get(&value).map(|vb| {
            if vb.is_dirty() {
                vb.apply_diff(candidates)
            } else {
                candidates & vb.base().as_ref()
            }
        })
    }
    /// Compute the union of multiple values with diff fusion against candidates.
    /// For each value, fuses diffs against candidates, then unions results.
    /// This is the diff-aware read path for In/Or queries.
    pub fn union_with_diff(&self, values: &[u64], candidates: &RoaringBitmap) -> RoaringBitmap {
        let r = self.bitmaps.read();
        let mut result = RoaringBitmap::new();
        for value in values {
            if let Some(vb) = r.get(value) {
                if vb.is_dirty() {
                    result |= vb.apply_diff(candidates);
                } else {
                    result |= candidates & vb.base().as_ref();
                }
            }
        }
        result
    }
    /// Get the cardinality (number of set bits) for a specific value.
    pub fn cardinality(&self, value: u64) -> u64 {
        let r = self.bitmaps.read();
        r.get(&value).map_or(0, |vb| vb.base_len())
    }
    /// Get the number of distinct values tracked.
    pub fn distinct_count(&self) -> usize {
        self.bitmaps.read().len()
    }
    /// Compute the union of bitmaps for multiple values (OR).
    pub fn union(&self, values: &[u64]) -> RoaringBitmap {
        let r = self.bitmaps.read();
        let mut result = RoaringBitmap::new();
        for value in values {
            if let Some(vb) = r.get(value) {
                result |= vb.base().as_ref();
            }
        }
        result
    }
    /// Compute the intersection of bitmaps for multiple values (AND).
    /// Returns None if any value has no bitmap.
    pub fn intersection(&self, values: &[u64]) -> Option<RoaringBitmap> {
        let r = self.bitmaps.read();
        let mut iter = values.iter();
        let first = iter.next()?;
        let mut result: RoaringBitmap = r.get(first)?.base().as_ref().clone();
        for value in iter {
            match r.get(value) {
                Some(vb) => result &= vb.base().as_ref(),
                None => return Some(RoaringBitmap::new()), // Empty intersection
            }
        }
        Some(result)
    }
    /// Closure-based iteration over (value, &VersionedBitmap) pairs.
    ///
    /// Holds the read lock for the duration of the iteration. For
    /// high-cardinality fields this can be ~1-2 seconds. Earlier
    /// per-key brief-lock variant was abandoned due to writer
    /// starvation under concurrent reader load (see `bitmap_bytes`).
    pub fn for_each_versioned<F: FnMut(u64, &VersionedBitmap)>(&self, mut f: F) {
        let r = self.bitmaps.read();
        for (k, vb) in r.iter() {
            f(*k, vb);
        }
    }
    /// Snapshot all (value, VersionedBitmap) pairs into an owned Vec under
    /// the read lock. The cloned VBs share inner `Arc<RoaringBitmap>`
    /// data with the originals (refcount bumps only) — cheap.
    ///
    /// Replaces the old reference-returning iter_versioned. Callers
    /// pattern-match on `(value, &vb)` from the returned Vec.
    pub fn iter_versioned(&self) -> Vec<(u64, VersionedBitmap)> {
        let r = self.bitmaps.read();
        r.iter().map(|(k, vb)| (*k, vb.clone())).collect()
    }
    /// Get the total number of bitmaps.
    pub fn bitmap_count(&self) -> usize {
        self.bitmaps.read().len()
    }
    /// Return the serialized byte size of all bitmaps in this field.
    ///
    /// Holds the read lock for the duration of the iteration. For
    /// high-cardinality fields (postId at 22.5M entries) this can take
    /// ~1-2 seconds. **DO NOT call this on the hot path** — it's
    /// intended for rare metrics scrapes only. The earlier chunked
    /// per-key-lock variant was abandoned because 22.5M brief lock
    /// acquisitions starved concurrent writers (`load_field_complete`)
    /// indefinitely under heavy reader load.
    ///
    /// TODO: cache the value with a TTL or update incrementally on
    /// mutation rather than recomputing on every metric scrape.
    pub fn bitmap_bytes(&self) -> usize {
        let r = self.bitmaps.read();
        r.values().map(|vb| vb.bitmap_bytes()).sum()
    }
    /// Drop all base bitmaps and mark every value as unloaded.
    /// The diff layers are preserved so mutations can accumulate
    /// while the field is not in memory.
    pub fn clear_bases_and_unload(&self) {
        let mut w = self.bitmaps.write();
        for vb in w.values_mut() {
            vb.clear_base_and_unload();
        }
    }
    /// Reload a complete field from disk, merging persisted bases into any
    /// existing diff-only placeholders. After loading, all values are marked loaded
    /// so merge_dirty() can compact their diffs normally.
    ///
    /// Holds the write lock for the entire operation. For postId at
    /// 22.5M entries this is ~1-2 seconds during which concurrent reads
    /// block. Earlier chunked variants were abandoned because they
    /// allowed reader starvation: per-chunk releases gave the memory
    /// scanner thread a window to grab the read lock for ~1.5s
    /// (`bitmap_bytes` iterates the full HashMap), and the writer
    /// would never make progress.
    ///
    /// The proper fix is per-value lazy writes (don't load the full
    /// field at all on first access; load each value's bitmap from
    /// ShardStore on demand). That's tracked separately.
    pub fn load_field_complete(&self, data: HashMap<u64, RoaringBitmap>) {
        let mut w = self.bitmaps.write();
        for (value, bitmap) in data {
            w.entry(value)
                .or_insert_with(VersionedBitmap::new_unloaded)
                .load_base(&bitmap);
        }
        // Mark any diff-only values (mutated while unloaded, not on
        // disk) as loaded. Same lock window.
        for vb in w.values_mut() {
            vb.mark_loaded();
        }
    }
    /// Reload specific values from disk (for per-value lazy loading of high-cardinality fields).
    /// Only the requested values are marked as loaded; others remain unloaded.
    pub fn load_values(&self, data: HashMap<u64, RoaringBitmap>, requested: &[u64]) {
        let mut w = self.bitmaps.write();
        for &value in requested {
            if let Some(bitmap) = data.get(&value) {
                w.entry(value)
                    .or_insert_with(VersionedBitmap::new_unloaded)
                    .load_base(bitmap);
            } else {
                // Value wasn't on disk — it's a new value created since last save.
                // Mark it as loaded so its diffs can be compacted.
                w.entry(value)
                    .or_insert_with(VersionedBitmap::new_empty)
                    .mark_loaded();
            }
        }
    }
    /// Merge all dirty VersionedBitmaps in this field.
    ///
    /// Walks the dirty side-set, not the full HashMap. See `dirty_values`
    /// doc on `FilterField` for why.
    pub fn merge_all(&self) {
        self.merge_dirty();
    }
    /// Returns true if any bitmap in this field has unmerged diffs.
    pub fn has_dirty(&self) -> bool {
        !self.dirty_values.lock().is_empty()
    }
    /// Returns true only if a *loaded* bitmap has an unmerged diff.
    /// Unloaded-but-dirty entries can't be compacted (merge() short-circuits on
    /// !is_loaded), so idle compaction must ignore them to avoid a hot loop
    /// where has_dirty stays true and merge is a no-op.
    pub fn has_loaded_dirty(&self) -> bool {
        let dirty: Vec<u64> = self.dirty_values.lock().iter().copied().collect();
        if dirty.is_empty() {
            return false;
        }
        let r = self.bitmaps.read();
        dirty.iter().any(|v| {
            r.get(v).map_or(false, |vb| vb.is_loaded() && vb.is_dirty())
        })
    }
    /// Merge dirty VersionedBitmaps, walking only the side dirty-set.
    ///
    /// Drains `dirty_values` first, then takes the bitmaps write lock once.
    /// Per-value work is `vb.merge()` — single-digit microseconds. Lock-hold
    /// scales with the number of dirty values per cycle, not the size of
    /// the field.
    ///
    /// Unloaded-with-diff values can't be merged (their base isn't in
    /// memory), so they get re-queued for the next cycle. They'll be
    /// merged when lazy-load brings the base in.
    pub fn merge_dirty(&self) {
        let dirty: Vec<u64> = {
            let mut set = self.dirty_values.lock();
            set.drain().collect()
        };
        if dirty.is_empty() {
            return;
        }
        let mut requeue: Vec<u64> = Vec::new();
        {
            let mut w = self.bitmaps.write();
            for value in dirty {
                if let Some(vb) = w.get_mut(&value) {
                    if !vb.is_dirty() {
                        continue; // already clean — drop from set
                    }
                    if vb.is_loaded() {
                        vb.merge();
                    } else {
                        // Diff exists but base isn't loaded; merge would
                        // be a no-op. Keep tracked so we don't lose it.
                        requeue.push(value);
                    }
                }
                // Missing key (evicted) — drop from set silently.
            }
        }
        if !requeue.is_empty() {
            let mut set = self.dirty_values.lock();
            for v in requeue {
                set.insert(v);
            }
        }
    }
    /// Merge a specific value's VersionedBitmap if it exists and is dirty.
    pub fn merge_field(&self, value: u64) {
        let mut merged_clean = false;
        let mut still_dirty_unloaded = false;
        {
            let mut w = self.bitmaps.write();
            if let Some(vb) = w.get_mut(&value) {
                if vb.is_dirty() {
                    if vb.is_loaded() {
                        vb.merge();
                        merged_clean = true;
                    } else {
                        still_dirty_unloaded = true;
                    }
                } else {
                    merged_clean = true;
                }
            }
        }
        let mut set = self.dirty_values.lock();
        if merged_clean {
            set.remove(&value);
        } else if still_dirty_unloaded {
            // Make sure it stays tracked even if a caller of merge_field
            // expected it to handle the dirty-set bookkeeping.
            set.insert(value);
        }
    }
    /// Insert a single (value, VersionedBitmap) pair directly into the map.
    /// Used by FilterIndex internal helpers (`unload_field`, `unload_from`)
    /// that build new fields from diff-only clones. Acquires the write lock.
    pub(crate) fn insert_versioned(&self, value: u64, vb: VersionedBitmap) {
        let dirty = vb.is_dirty();
        self.bitmaps.write().insert(value, vb);
        if dirty {
            self.mark_dirty(value);
        } else {
            // A clean VB replaces whatever was there — drop any stale
            // dirty-set entry for this value.
            self.dirty_values.lock().remove(&value);
        }
    }
}
/// The type of a filter field, determining how values map to bitmaps.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterFieldType {
    /// Each slot has exactly one value for this field.
    SingleValue,
    /// Each slot can have multiple values (e.g., tags).
    MultiValue,
    /// Boolean field: two bitmaps (true=1, false=0).
    Boolean,
}
/// Manages all filter fields.
///
/// Each FilterField is Arc-wrapped for clone-on-write at the field level.
/// Cloning FilterIndex copies only the outer HashMap (~5-10 entries, one per field)
/// and bumps Arc refcounts — O(num_fields), not O(total_distinct_values).
/// Mutation via `get_field_mut()` uses `Arc::make_mut()` to clone only the
/// specific field being modified when shared with a published snapshot.
#[derive(Clone)]
pub struct FilterIndex {
    /// Map from field name to Arc-wrapped FilterField.
    fields: HashMap<String, Arc<FilterField>>,
}
impl FilterIndex {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }
    /// Add a filter field from configuration.
    pub fn add_field(&mut self, config: FilterFieldConfig) {
        let name = config.name.clone();
        self.fields.insert(name, Arc::new(FilterField::new(config)));
    }
    /// Remove a filter field by name. Returns true if the field existed.
    pub fn remove_field(&mut self, name: &str) -> bool {
        self.fields.remove(name).is_some()
    }
    /// Get a reference to a filter field by name.
    ///
    /// Mutating methods on `FilterField` use interior mutability via
    /// `parking_lot::RwLock`, so this immutable reference is sufficient
    /// for both reads and writes. There is no `get_field_mut` because
    /// `Arc::make_mut` on `Arc<FilterField>` would deep-clone the entire
    /// HashMap (the bug we're fixing).
    pub fn get_field(&self, name: &str) -> Option<&FilterField> {
        self.fields.get(name).map(|f| f.as_ref())
    }
    /// Iterate over all fields.
    pub fn fields(&self) -> impl Iterator<Item = (&String, &FilterField)> {
        self.fields.iter().map(|(k, v)| (k, v.as_ref()))
    }
    /// Unload a field: replace its Arc with a new empty field, preserving only
    /// entries that have pending diffs (mutations received while loading/unloaded).
    /// Drops all clean entries entirely — critical for high-cardinality fields
    /// like postId (22.5M entries).
    pub fn unload_field(&mut self, name: &str) {
        if let Some(field_arc) = self.fields.get_mut(name) {
            let old = field_arc.as_ref();
            let new_field = FilterField::new(old.config.clone());
            // Preserve only entries with pending diffs
            old.for_each_versioned(|value, vb| {
                if vb.is_dirty() {
                    new_field.insert_versioned(value, vb.clone_diff_only());
                }
            });
            *field_arc = Arc::new(new_field);
        }
    }
    /// Copy a field's Arc from another FilterIndex (refcount bump only, no data copy).
    /// Used to preserve skipped fields during save_and_unload.
    pub fn copy_field_arc_from(&mut self, source: &FilterIndex, name: &str) {
        if let Some(arc) = source.fields.get(name) {
            self.fields.insert(name.to_string(), Arc::clone(arc));
        }
    }
    /// Build an unloaded version of a field from a source FilterIndex.
    /// Only preserves entries with pending diffs; all clean entries are dropped.
    pub fn unload_from(&mut self, source: &FilterIndex, name: &str) {
        if let Some(source_field) = source.fields.get(name) {
            let config = source_field.config.clone();
            let new_field = FilterField::new(config);
            source_field.for_each_versioned(|value, vb| {
                if vb.is_dirty() {
                    new_field.insert_versioned(value, vb.clone_diff_only());
                }
            });
            self.fields.insert(name.to_string(), Arc::new(new_field));
        }
    }
    /// Get the total number of bitmaps across all fields.
    pub fn total_bitmap_count(&self) -> usize {
        self.fields.values().map(|f| f.bitmap_count()).sum()
    }
    /// Return the serialized byte size of all bitmaps across all fields.
    pub fn bitmap_bytes(&self) -> usize {
        self.fields.values().map(|f| f.bitmap_bytes()).sum()
    }
    /// Return per-field bitmap byte sizes (field_name, bitmap_count, bytes).
    pub fn per_field_bytes(&self) -> Vec<(&str, usize, usize)> {
        self.fields
            .iter()
            .map(|(name, f)| (name.as_str(), f.bitmap_count(), f.bitmap_bytes()))
            .collect()
    }
}
impl Default for FilterIndex {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn make_single_value_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }
    }
    fn make_multi_value_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::MultiValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }
    }
    fn make_bool_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::Boolean,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        }
    }
    #[test]
    fn test_insert_and_get() {
        let mut field = FilterField::new(make_single_value_config("nsfwLevel"));
        field.insert(1, 100);
        field.insert(1, 200);
        field.insert(2, 300);
        field.merge_all();
        let bm = field.get(1).unwrap();
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(100));
        assert!(bm.contains(200));
        assert_eq!(field.cardinality(1), 2);
        assert_eq!(field.cardinality(2), 1);
        assert_eq!(field.cardinality(99), 0);
    }
    #[test]
    fn test_remove_specific_value() {
        let mut field = FilterField::new(make_single_value_config("userId"));
        field.insert(42, 10);
        field.insert(42, 20);
        field.insert(42, 30);
        field.merge_all();
        field.remove(42, 20);
        field.merge_dirty();
        assert_eq!(field.cardinality(42), 2);
        assert!(!field.get(42).unwrap().contains(20));
    }
    #[test]
    fn test_remove_last_cleans_up() {
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);
        field.merge_all();
        field.remove(1, 10);
        field.merge_dirty();
        // After merge, the bitmap exists but is empty (cleanup deferred to autovac)
        assert_eq!(field.cardinality(1), 0);
    }
    #[test]
    fn test_remove_from_all() {
        let mut field = FilterField::new(make_multi_value_config("tagIds"));
        field.insert(100, 5);
        field.insert(200, 5);
        field.insert(300, 5);
        field.insert(100, 10);
        field.merge_all();
        field.remove_from_all(5);
        field.merge_dirty();
        assert!(!field.get(100).unwrap().contains(5));
        assert!(field.get(100).unwrap().contains(10));
        assert_eq!(field.cardinality(200), 0); // Was only slot 5
        assert_eq!(field.cardinality(300), 0); // Was only slot 5
    }
    #[test]
    fn test_multi_value_field() {
        let mut field = FilterField::new(make_multi_value_config("tagIds"));
        // Document at slot 5 has tags 100, 200, 300
        field.insert(100, 5);
        field.insert(200, 5);
        field.insert(300, 5);
        // Document at slot 10 has tags 200, 400
        field.insert(200, 10);
        field.insert(400, 10);
        field.merge_all();
        assert!(field.get(100).unwrap().contains(5));
        assert!(field.get(200).unwrap().contains(5));
        assert!(field.get(200).unwrap().contains(10));
        assert!(!field.get(100).unwrap().contains(10));
    }
    #[test]
    fn test_boolean_field() {
        let mut field = FilterField::new(make_bool_config("onSite"));
        field.insert(1, 10); // true
        field.insert(1, 20); // true
        field.insert(0, 30); // false
        field.merge_all();
        assert_eq!(field.cardinality(1), 2);
        assert_eq!(field.cardinality(0), 1);
    }
    #[test]
    fn test_union() {
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);
        field.insert(1, 20);
        field.insert(2, 30);
        field.insert(2, 40);
        field.insert(3, 50);
        field.merge_all();
        let result = field.union(&[1, 2]);
        assert_eq!(result.len(), 4);
        assert!(result.contains(10));
        assert!(result.contains(20));
        assert!(result.contains(30));
        assert!(result.contains(40));
    }
    #[test]
    fn test_intersection() {
        let mut field = FilterField::new(make_multi_value_config("tagIds"));
        // Slot 5 has tags 100, 200
        field.insert(100, 5);
        field.insert(200, 5);
        // Slot 10 has tags 200, 300
        field.insert(200, 10);
        field.insert(300, 10);
        // Slot 15 has tag 100
        field.insert(100, 15);
        field.merge_all();
        let result = field.intersection(&[100, 200]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(5)); // Only slot 5 has both 100 and 200
    }
    #[test]
    fn test_intersection_missing_value() {
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);
        field.merge_all();
        let result = field.intersection(&[1, 999]).unwrap();
        assert!(result.is_empty()); // 999 doesn't exist, so intersection is empty
    }
    #[test]
    fn test_filter_index_multi_field() {
        let mut index = FilterIndex::new();
        index.add_field(make_single_value_config("nsfwLevel"));
        index.add_field(make_multi_value_config("tagIds"));
        index.add_field(make_bool_config("onSite"));
        // Insert some data
        index.get_field_mut("nsfwLevel").unwrap().insert(1, 100);
        index.get_field_mut("tagIds").unwrap().insert(456, 100);
        index.get_field_mut("tagIds").unwrap().insert(789, 100);
        index.get_field_mut("onSite").unwrap().insert(1, 100);
        // Merge before reading
        for (_name, field) in index.fields_mut() {
            field.merge_all();
        }
        // Verify
        assert_eq!(index.get_field("nsfwLevel").unwrap().cardinality(1), 1);
        assert_eq!(index.get_field("tagIds").unwrap().cardinality(456), 1);
        assert_eq!(index.get_field("onSite").unwrap().cardinality(1), 1);
    }
    #[test]
    fn test_filter_and_alive_gate() {
        // Simulate the query pattern: filter bitmap AND alive bitmap
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);
        field.insert(1, 20);
        field.insert(1, 30);
        field.merge_all();
        let mut alive = RoaringBitmap::new();
        alive.insert(10);
        alive.insert(20);
        // Slot 30 is deleted (not in alive)
        let filter_result = field.get(1).unwrap();
        let gated = filter_result & &alive;
        assert_eq!(gated.len(), 2);
        assert!(gated.contains(10));
        assert!(gated.contains(20));
        assert!(!gated.contains(30)); // Filtered out by alive gate
    }
}
