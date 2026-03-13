use std::collections::HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::config::FilterFieldConfig;
use crate::versioned_bitmap::VersionedBitmap;

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
#[derive(Clone)]
pub struct FilterField {
    /// One bitmap per distinct value. Key is the u64 bitmap key.
    bitmaps: HashMap<u64, VersionedBitmap>,
    /// Field configuration.
    config: FilterFieldConfig,
}

impl FilterField {
    pub fn new(config: FilterFieldConfig) -> Self {
        Self {
            bitmaps: HashMap::new(),
            config,
        }
    }

    /// Bulk-load bitmaps from a map of (value -> bitmap).
    /// Used during startup to restore Tier 1 filter state from redb.
    /// Each bitmap becomes a VersionedBitmap base (no dirty diff).
    pub fn load_from(&mut self, data: HashMap<u64, RoaringBitmap>) {
        for (value, bitmap) in data {
            self.bitmaps.insert(value, VersionedBitmap::new(bitmap));
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
    pub fn insert(&mut self, value: u64, slot: u32) {
        self.bitmaps
            .entry(value)
            .or_insert_with(VersionedBitmap::new_empty)
            .insert(slot);
    }

    /// Clear a slot's bit from the bitmap for the given value.
    /// Always records the diff, even if the value is unloaded — creates a diff-only
    /// placeholder so the remove is preserved until the base is reloaded from disk.
    pub fn remove(&mut self, value: u64, slot: u32) {
        self.bitmaps
            .entry(value)
            .or_insert_with(VersionedBitmap::new_unloaded)
            .remove(slot);
    }

    /// Bulk-insert multiple slots into the bitmap for the given value.
    /// Slots should be sorted for maximum roaring-rs `extend()` performance.
    pub fn insert_bulk(&mut self, value: u64, slots: impl IntoIterator<Item = u32>) {
        self.bitmaps
            .entry(value)
            .or_insert_with(VersionedBitmap::new_empty)
            .insert_bulk(slots);
    }

    /// OR a RoaringBitmap directly into the base for the given value.
    /// Bypasses the diff layer for maximum bulk-load throughput.
    /// Creates the VersionedBitmap if it doesn't exist.
    pub fn or_bitmap(&mut self, value: u64, bitmap: &RoaringBitmap) {
        self.bitmaps
            .entry(value)
            .or_insert_with(VersionedBitmap::new_empty)
            .or_into_base(bitmap);
    }

    /// Bulk-remove multiple slots from the bitmap for the given value.
    pub fn remove_bulk(&mut self, value: u64, slots: &[u32]) {
        if let Some(vb) = self.bitmaps.get_mut(&value) {
            for &slot in slots {
                vb.remove(slot);
            }
        }
    }

    /// Clear a slot's bit from ALL bitmaps in this field.
    /// Used by autovac to clean dead slots from filter bitmaps.
    pub fn remove_from_all(&mut self, slot: u32) {
        for vb in self.bitmaps.values_mut() {
            vb.remove(slot);
        }
    }

    /// Get the base bitmap for a specific value.
    /// Returns the base only (ignoring any pending diff). Use `get_versioned()`
    /// for diff-aware reads, or `apply_diff_eq()` for fused reads.
    pub fn get(&self, value: u64) -> Option<&RoaringBitmap> {
        self.bitmaps.get(&value).map(|vb| vb.base().as_ref())
    }

    /// Get the raw VersionedBitmap for a specific value, including its diff layer.
    /// Use this when you need to fuse diffs at read time.
    pub fn get_versioned(&self, value: u64) -> Option<&VersionedBitmap> {
        self.bitmaps.get(&value)
    }

    /// Iterator over all value keys in this field's bitmap HashMap.
    pub fn bitmap_keys(&self) -> impl Iterator<Item = &u64> {
        self.bitmaps.keys()
    }

    /// Remove a value's bitmap from the field (used by idle eviction).
    /// The bitmap can be re-loaded from disk on the next query.
    pub fn remove_value(&mut self, value: u64) {
        self.bitmaps.remove(&value);
    }

    /// Number of distinct values currently loaded in memory.
    pub fn loaded_value_count(&self) -> usize {
        self.bitmaps.len()
    }

    /// Get the fused bitmap for a single value against a candidate set.
    /// Applies the diff (sets/clears) to the intersection of base and candidates.
    /// This is the primary diff-aware read path for Eq/NotEq queries.
    pub fn apply_diff_eq(&self, value: u64, candidates: &RoaringBitmap) -> Option<RoaringBitmap> {
        self.bitmaps.get(&value).map(|vb| {
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
        let mut result = RoaringBitmap::new();
        for value in values {
            if let Some(vb) = self.bitmaps.get(value) {
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
        self.bitmaps.get(&value).map_or(0, |vb| vb.base_len())
    }

    /// Get the number of distinct values tracked.
    pub fn distinct_count(&self) -> usize {
        self.bitmaps.len()
    }

    /// Compute the union of bitmaps for multiple values (OR).
    pub fn union(&self, values: &[u64]) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for value in values {
            if let Some(vb) = self.bitmaps.get(value) {
                result |= vb.base().as_ref();
            }
        }
        result
    }

    /// Compute the intersection of bitmaps for multiple values (AND).
    /// Returns None if any value has no bitmap.
    pub fn intersection(&self, values: &[u64]) -> Option<RoaringBitmap> {
        let mut iter = values.iter();
        let first = iter.next()?;
        let mut result: RoaringBitmap = self.bitmaps.get(first)?.base().as_ref().clone();
        for value in iter {
            match self.bitmaps.get(value) {
                Some(vb) => result &= vb.base().as_ref(),
                None => return Some(RoaringBitmap::new()), // Empty intersection
            }
        }
        Some(result)
    }

    /// Iterate over all (value, bitmap) pairs (base only, no diff fusion).
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &RoaringBitmap)> {
        self.bitmaps.iter().map(|(k, vb)| (k, vb.base().as_ref()))
    }

    /// Iterate over all (value, VersionedBitmap) pairs for diff-aware access.
    /// Used by range scans that need to fuse diffs.
    pub fn iter_versioned(&self) -> impl Iterator<Item = (&u64, &VersionedBitmap)> {
        self.bitmaps.iter()
    }

    /// Get the total number of bitmaps.
    pub fn bitmap_count(&self) -> usize {
        self.bitmaps.len()
    }

    /// Return the serialized byte size of all bitmaps in this field.
    pub fn bitmap_bytes(&self) -> usize {
        self.bitmaps.values().map(|vb| vb.bitmap_bytes()).sum()
    }

    /// Drop all base bitmaps and mark every value as unloaded.
    /// The diff layers are preserved so mutations can accumulate
    /// while the field is not in memory.
    pub fn clear_bases_and_unload(&mut self) {
        for vb in self.bitmaps.values_mut() {
            vb.clear_base_and_unload();
        }
    }

    /// Reload a complete field from disk, merging persisted bases into any
    /// existing diff-only placeholders. After loading, all values are marked loaded
    /// so merge_dirty() can compact their diffs normally.
    pub fn load_field_complete(&mut self, data: HashMap<u64, RoaringBitmap>) {
        for (value, bitmap) in data {
            self.bitmaps
                .entry(value)
                .or_insert_with(VersionedBitmap::new_unloaded)
                .load_base(&bitmap);
        }
        // Mark any diff-only values (mutated while unloaded, not on disk) as loaded
        for vb in self.bitmaps.values_mut() {
            vb.mark_loaded();
        }
    }

    /// Reload specific values from disk (for per-value lazy loading of high-cardinality fields).
    /// Only the requested values are marked as loaded; others remain unloaded.
    pub fn load_values(&mut self, data: HashMap<u64, RoaringBitmap>, requested: &[u64]) {
        for &value in requested {
            if let Some(bitmap) = data.get(&value) {
                self.bitmaps
                    .entry(value)
                    .or_insert_with(VersionedBitmap::new_unloaded)
                    .load_base(bitmap);
            } else {
                // Value wasn't on disk — it's a new value created since last save.
                // Mark it as loaded so its diffs can be compacted.
                self.bitmaps
                    .entry(value)
                    .or_insert_with(VersionedBitmap::new_empty)
                    .mark_loaded();
            }
        }
    }

    /// Merge all dirty VersionedBitmaps in this field.
    pub fn merge_all(&mut self) {
        for vb in self.bitmaps.values_mut() {
            vb.merge();
        }
    }

    /// Merge only dirty VersionedBitmaps.
    /// Returns true if any bitmap in this field has unmerged diffs.
    pub fn has_dirty(&self) -> bool {
        self.bitmaps.values().any(|vb| vb.is_dirty())
    }

    pub fn merge_dirty(&mut self) {
        for vb in self.bitmaps.values_mut() {
            if vb.is_dirty() {
                vb.merge();
            }
        }
    }

    /// Merge a specific value's VersionedBitmap if it exists and is dirty.
    pub fn merge_field(&mut self, value: u64) {
        if let Some(vb) = self.bitmaps.get_mut(&value) {
            if vb.is_dirty() {
                vb.merge();
            }
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

    /// Get a reference to a filter field by name.
    pub fn get_field(&self, name: &str) -> Option<&FilterField> {
        self.fields.get(name).map(|f| f.as_ref())
    }

    /// Get a mutable reference to a filter field by name.
    /// Uses Arc::make_mut for clone-on-write: only clones the field's data
    /// when shared with a published snapshot (refcount > 1).
    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut FilterField> {
        self.fields.get_mut(name).map(|f| Arc::make_mut(f))
    }

    /// Iterate over all fields.
    pub fn fields(&self) -> impl Iterator<Item = (&String, &FilterField)> {
        self.fields.iter().map(|(k, v)| (k, v.as_ref()))
    }

    /// Iterate mutably over all fields.
    pub fn fields_mut(&mut self) -> impl Iterator<Item = (&String, &mut FilterField)> {
        self.fields.iter_mut().map(|(k, v)| (k, Arc::make_mut(v)))
    }

    /// Unload a field: replace its Arc with a new empty field, preserving only
    /// entries that have pending diffs (mutations received while loading/unloaded).
    /// This avoids Arc::make_mut deep-cloning the HashMap and drops all clean entries
    /// entirely — critical for high-cardinality fields like postId (13M entries).
    pub fn unload_field(&mut self, name: &str) {
        if let Some(field_arc) = self.fields.get_mut(name) {
            let old = field_arc.as_ref();
            let mut new_field = FilterField::new(old.config.clone());
            // Preserve only entries with pending diffs
            for (&value, vb) in old.iter_versioned() {
                if vb.is_dirty() {
                    new_field.bitmaps.insert(value, vb.clone_diff_only());
                }
            }
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
            let mut new_field = FilterField::new(config);
            for (&value, vb) in source_field.iter_versioned() {
                if vb.is_dirty() {
                    new_field.bitmaps.insert(value, vb.clone_diff_only());
                }
            }
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
        }
    }

    fn make_multi_value_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::MultiValue,
            behaviors: None,
            eviction: None,
        }
    }

    fn make_bool_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::Boolean,
            behaviors: None,
            eviction: None,
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
