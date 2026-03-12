use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::config::SortFieldConfig;
use crate::versioned_bitmap::VersionedBitmap;

/// Sort layer bitmaps for a single sortable field.
///
/// Each sortable numeric field is decomposed into N bitmaps, one per bit position.
/// A u32 field = 32 bitmaps (bit 0 through bit 31).
///
/// Bitmap `bit_layers[5]` has a 1 for every slot where bit 5 of that field's value is set.
///
/// Top-N retrieval: start at the most significant bit layer, AND with the filter result.
/// If the intersection has >= N results, narrow to that set and descend.
/// If < N, keep both groups and continue. This is binary search across all documents
/// simultaneously. All sort operations are bitmap AND operations.
///
/// Bit layers use VersionedBitmap for diff-based mutation with eager merge.
/// Mutations write to the diff layer; eager merge compacts diffs before
/// readers see them, so sort traversal always reads clean bases.
#[derive(Clone)]
pub struct SortField {
    /// One bitmap per bit position. Index 0 = LSB, index 31 = MSB (for 32-bit).
    bit_layers: Vec<VersionedBitmap>,
    /// Number of bit layers (typically 32 for u32).
    num_bits: usize,
    /// Field configuration.
    config: SortFieldConfig,
}

impl SortField {
    pub fn new(config: SortFieldConfig) -> Self {
        let num_bits = config.bits as usize;
        let bit_layers = (0..num_bits)
            .map(|_| VersionedBitmap::new_empty())
            .collect();
        Self {
            bit_layers,
            num_bits,
            config,
        }
    }

    /// Get the field name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get the number of bit layers.
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Insert a value for a slot. Sets the appropriate bits in each layer's diff.
    pub fn insert(&mut self, slot: u32, value: u32) {
        for bit in 0..self.num_bits {
            if (value >> bit) & 1 == 1 {
                self.bit_layers[bit].insert(slot);
            }
        }
    }

    /// Remove a slot from all bit layers. Used by autovac.
    pub fn remove(&mut self, slot: u32) {
        for layer in &mut self.bit_layers {
            layer.remove(slot);
        }
    }

    /// Update a slot's value using XOR diff.
    /// Only flips the bit layers where old and new values differ.
    pub fn update(&mut self, slot: u32, old_value: u32, new_value: u32) {
        let diff = old_value ^ new_value;
        for bit in 0..self.num_bits {
            if (diff >> bit) & 1 == 1 {
                // This bit changed — check base+diff to determine current state
                if self.bit_layers[bit].contains(slot) {
                    self.bit_layers[bit].remove(slot);
                } else {
                    self.bit_layers[bit].insert(slot);
                }
            }
        }
    }

    /// Bulk-set a bit layer for multiple slots. Slots should be sorted for performance.
    pub fn set_layer_bulk(&mut self, bit: usize, slots: impl IntoIterator<Item = u32>) {
        if let Some(layer) = self.bit_layers.get_mut(bit) {
            layer.insert_bulk(slots);
        }
    }

    /// OR a RoaringBitmap directly into a bit layer's base.
    /// Bypasses the diff layer for maximum bulk-load throughput.
    pub fn or_layer(&mut self, bit: usize, bitmap: &RoaringBitmap) {
        if let Some(layer) = self.bit_layers.get_mut(bit) {
            layer.or_into_base(bitmap);
        }
    }

    /// Bulk-clear a bit layer for multiple slots.
    pub fn clear_layer_bulk(&mut self, bit: usize, slots: &[u32]) {
        if let Some(layer) = self.bit_layers.get_mut(bit) {
            for &slot in slots {
                layer.remove(slot);
            }
        }
    }

    /// Get a reference to a specific bit layer's base bitmap.
    /// Requires that the layer has been merged (no pending diff).
    pub fn layer(&self, bit: usize) -> Option<&RoaringBitmap> {
        self.bit_layers.get(bit).map(|vb| {
            debug_assert!(!vb.is_dirty(), "sort layer {bit} has unmerged diff");
            vb.base().as_ref()
        })
    }

    /// Perform top-N sort traversal on a candidate set using MSB-to-LSB bifurcation.
    ///
    /// Traverses from MSB to LSB using pure bitmap AND operations to narrow candidates:
    /// - For descending: prefer slots with the bit SET (higher values first)
    /// - For ascending: prefer slots with the bit CLEAR (lower values first)
    ///
    /// At each bit layer, the candidates are split into "preferred" (matching the desired
    /// bit state) and "rest". If preferred has >= limit slots, narrow to preferred.
    /// If preferred has < limit, those are all winners — collect them, reduce limit,
    /// and continue with the rest.
    ///
    /// After collecting the top-N slot IDs, reconstructs values ONLY for those N slots
    /// (not all candidates) to produce the final ordered output.
    pub fn top_n(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
        cursor: Option<(u64, u32)>,
    ) -> Vec<u32> {
        if candidates.is_empty() || limit == 0 {
            return Vec::new();
        }

        // Apply cursor filtering if present
        let effective_candidates;
        let candidates = if let Some((cursor_sort_value, cursor_slot_id)) = cursor {
            effective_candidates =
                self.apply_cursor_filter(candidates, descending, cursor_sort_value, cursor_slot_id);
            &effective_candidates
        } else {
            candidates
        };

        if candidates.is_empty() {
            return Vec::new();
        }

        // MSB-to-LSB bifurcation: collect top-N slots via bitmap AND operations
        let top_n_bitmap = self.bifurcate(candidates, limit, descending);

        // Reconstruct values ONLY for the final top-N slots and sort them
        self.order_results(&top_n_bitmap, descending)
    }

    /// MSB-to-LSB bifurcation traversal.
    ///
    /// Walks bit layers from MSB to LSB, narrowing candidates at each layer.
    /// Returns a bitmap containing exactly min(limit, candidates.len()) top slots.
    fn bifurcate(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
    ) -> RoaringBitmap {
        let total = candidates.len() as usize;
        if total <= limit {
            return candidates.clone();
        }

        // result accumulates confirmed winners; remaining is the working set
        let mut result = RoaringBitmap::new();
        let mut remaining = candidates.clone();
        let mut remaining_limit = limit;

        for bit in (0..self.num_bits).rev() {
            if remaining_limit == 0 || remaining.is_empty() {
                break;
            }

            debug_assert!(!self.bit_layers[bit].is_dirty(), "sort layer {bit} has unmerged diff in bifurcate");
            let layer: &RoaringBitmap = self.bit_layers[bit].base();

            // preferred = slots that have the "better" bit value at this position
            let preferred = if descending {
                // Descending: prefer bit SET (higher values)
                &remaining & layer
            } else {
                // Ascending: prefer bit CLEAR (lower values)
                &remaining - layer
            };

            let preferred_count = preferred.len() as usize;

            if preferred_count == 0 {
                // No slots have the preferred bit — all remaining are equivalent at
                // this layer, continue to next bit with the same remaining set
                continue;
            } else if preferred_count >= remaining_limit {
                // More preferred slots than we need — narrow to preferred and continue
                remaining = preferred;
            } else {
                // Fewer preferred slots than limit — all preferred are winners.
                // Collect them, reduce limit, continue with the rest.
                result |= &preferred;
                remaining -= &preferred;
                remaining_limit -= preferred_count;
            }
        }

        // After all layers, if we still need more slots, take them from remaining
        if remaining_limit > 0 && !remaining.is_empty() {
            // remaining slots all have equal sort values at this point;
            // take up to remaining_limit from them
            let mut taken = 0;
            for slot in remaining.iter() {
                if taken >= remaining_limit {
                    break;
                }
                result.insert(slot);
                taken += 1;
            }
        }

        result
    }

    /// Order the top-N result bitmap into a sorted Vec.
    ///
    /// Reconstructs sort values ONLY for the small result set (not all candidates),
    /// then sorts by value with slot ID tiebreaker.
    fn order_results(&self, result_bitmap: &RoaringBitmap, descending: bool) -> Vec<u32> {
        let mut entries: Vec<(u32, u32)> = result_bitmap
            .iter()
            .map(|slot| (slot, self.reconstruct_value(slot)))
            .collect();

        if descending {
            entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }

        entries.into_iter().map(|(slot, _)| slot).collect()
    }

    /// Apply cursor-based filtering to candidates using bitmap operations.
    ///
    /// Walks bit layers from MSB to LSB, using the cursor's sort value bits to partition
    /// candidates into "strictly better than cursor", "equal so far", and "strictly worse".
    /// Only "strictly better" and the portion of "equal" that passes the slot ID tiebreaker
    /// are retained.
    fn apply_cursor_filter(
        &self,
        candidates: &RoaringBitmap,
        descending: bool,
        cursor_sort_value: u64,
        cursor_slot_id: u32,
    ) -> RoaringBitmap {
        let cursor_value = cursor_sort_value as u32;

        // We partition candidates into three groups as we descend bit layers:
        // - confirmed: slots whose sort value is strictly "better" than cursor (definitely included)
        // - equal: slots whose sort value matches cursor at all bits examined so far (still ambiguous)
        // - excluded: everything else (dropped)
        let mut confirmed = RoaringBitmap::new();
        let mut equal = candidates.clone();

        for bit in (0..self.num_bits).rev() {
            if equal.is_empty() {
                break;
            }

            let cursor_bit_set = (cursor_value >> bit) & 1 == 1;
            debug_assert!(!self.bit_layers[bit].is_dirty(), "sort layer {bit} has unmerged diff in apply_cursor_filter");
            let layer: &RoaringBitmap = self.bit_layers[bit].base();

            let equal_with_bit_set = &equal & layer;
            let equal_with_bit_clear = &equal - layer;

            if descending {
                // Descending: we want slots with value LESS than cursor (they come after cursor)
                if cursor_bit_set {
                    // Cursor has bit set. Slots with bit clear have LOWER value → confirmed (after cursor).
                    // Slots with bit set are still equal.
                    confirmed |= &equal_with_bit_clear;
                    equal = equal_with_bit_set;
                } else {
                    // Cursor has bit clear. Slots with bit set have HIGHER value → exclude (before cursor).
                    // Slots with bit clear are still equal.
                    equal = equal_with_bit_clear;
                }
            } else {
                // Ascending: we want slots with value GREATER than cursor (they come after cursor)
                if cursor_bit_set {
                    // Cursor has bit set. Slots with bit clear have LOWER value → exclude (before cursor).
                    // Slots with bit set are still equal.
                    equal = equal_with_bit_set;
                } else {
                    // Cursor has bit clear. Slots with bit set have HIGHER value → confirmed (after cursor).
                    // Slots with bit clear are still equal.
                    confirmed |= &equal_with_bit_set;
                    equal = equal_with_bit_clear;
                }
            }
        }

        // After all bits: `equal` contains slots with the exact same sort value as cursor.
        // Apply slot ID tiebreaker.
        for slot in equal.iter() {
            let dominated = if descending {
                slot < cursor_slot_id
            } else {
                slot > cursor_slot_id
            };
            if dominated {
                confirmed.insert(slot);
            }
        }

        confirmed
    }

    /// Reconstruct the sort value for a given slot by reading from the base bitmap.
    /// Requires that all layers have been merged.
    pub fn reconstruct_value(&self, slot: u32) -> u32 {
        let mut value = 0u32;
        for bit in 0..self.num_bits {
            debug_assert!(!self.bit_layers[bit].is_dirty(), "sort layer {bit} has unmerged diff in reconstruct_value");
            if self.bit_layers[bit].base().contains(slot) {
                value |= 1 << bit;
            }
        }
        value
    }

    /// Merge all bit layers, compacting diffs into bases.
    pub fn merge_all(&mut self) {
        for layer in &mut self.bit_layers {
            layer.merge();
        }
    }

    /// Returns true if any bit layer has unmerged diffs.
    pub fn has_dirty(&self) -> bool {
        self.bit_layers.iter().any(|layer| layer.is_dirty())
    }

    /// Merge only dirty bit layers (those with pending diffs).
    pub fn merge_dirty(&mut self) {
        for layer in &mut self.bit_layers {
            if layer.is_dirty() {
                layer.merge();
            }
        }
    }

    /// Load persisted base bitmaps into the sort layers, replacing existing bases.
    /// Each layer becomes a clean VersionedBitmap (no diff).
    pub fn load_layers(&mut self, layers: Vec<RoaringBitmap>) {
        for (i, bm) in layers.into_iter().enumerate() {
            if i < self.bit_layers.len() {
                self.bit_layers[i] = VersionedBitmap::new(bm);
            }
        }
    }

    /// Get base bitmap references for all layers (for persistence).
    /// Only valid when layers are clean (merged).
    pub fn layer_bases(&self) -> Vec<&RoaringBitmap> {
        self.bit_layers
            .iter()
            .map(|vb| {
                debug_assert!(!vb.is_dirty(), "persisting dirty sort layer");
                vb.base().as_ref()
            })
            .collect()
    }

    /// Return the serialized byte size of all bit layer bitmaps.
    pub fn bitmap_bytes(&self) -> usize {
        self.bit_layers.iter().map(|bm| bm.bitmap_bytes()).sum()
    }
}

/// Manages all sort fields.
///
/// Each SortField is Arc-wrapped for clone-on-write at the field level.
/// Cloning SortIndex copies only the outer HashMap and bumps Arc refcounts.
/// Mutation via `get_field_mut()` uses `Arc::make_mut()` to clone only the
/// specific sort field being modified when shared with a published snapshot.
#[derive(Clone)]
pub struct SortIndex {
    /// Map from field name to Arc-wrapped SortField.
    fields: std::collections::HashMap<String, Arc<SortField>>,
}

impl SortIndex {
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// Add a sort field from configuration.
    pub fn add_field(&mut self, config: SortFieldConfig) {
        let name = config.name.clone();
        self.fields.insert(name, Arc::new(SortField::new(config)));
    }

    /// Get a reference to a sort field by name.
    pub fn get_field(&self, name: &str) -> Option<&SortField> {
        self.fields.get(name).map(|f| f.as_ref())
    }

    /// Get a mutable reference to a sort field by name.
    /// Uses Arc::make_mut for clone-on-write: only clones the field's data
    /// when shared with a published snapshot (refcount > 1).
    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut SortField> {
        self.fields.get_mut(name).map(|f| Arc::make_mut(f))
    }

    /// Iterate over all fields.
    pub fn fields(&self) -> impl Iterator<Item = (&String, &SortField)> {
        self.fields.iter().map(|(k, v)| (k, v.as_ref()))
    }

    /// Iterate mutably over all fields.
    pub fn fields_mut(&mut self) -> impl Iterator<Item = (&String, &mut SortField)> {
        self.fields.iter_mut().map(|(k, v)| (k, Arc::make_mut(v)))
    }

    /// Return the serialized byte size of all bitmaps across all sort fields.
    pub fn bitmap_bytes(&self) -> usize {
        self.fields.values().map(|f| f.bitmap_bytes()).sum()
    }

    /// Return per-field bitmap byte sizes (field_name, bytes).
    pub fn per_field_bytes(&self) -> Vec<(&str, usize)> {
        self.fields
            .iter()
            .map(|(name, f)| (name.as_str(), f.bitmap_bytes()))
            .collect()
    }
}

impl Default for SortIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(name: &str) -> SortFieldConfig {
        SortFieldConfig {
            name: name.to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
        }
    }

    #[test]
    fn test_insert_and_reconstruct() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(10, 42);
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(10), 42);
    }

    #[test]
    fn test_insert_zero() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(5, 0);
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(5), 0);
    }

    #[test]
    fn test_insert_max_u32() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(5, u32::MAX);
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(5), u32::MAX);
    }

    #[test]
    fn test_bit_layers_correctness() {
        let mut sf = SortField::new(make_config("count"));
        // Value 5 = binary 101 -> bits 0 and 2 are set
        sf.insert(10, 5);
        sf.merge_all();

        assert!(sf.layer(0).unwrap().contains(10)); // bit 0
        assert!(!sf.layer(1).unwrap().contains(10)); // bit 1
        assert!(sf.layer(2).unwrap().contains(10)); // bit 2
        for bit in 3..32 {
            assert!(!sf.layer(bit).unwrap().contains(10));
        }
    }

    #[test]
    fn test_update_xor_diff() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(10, 100);
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(10), 100);

        sf.update(10, 100, 200);
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(10), 200);
    }

    #[test]
    fn test_update_only_changed_bits() {
        let mut sf = SortField::new(make_config("count"));
        // Value 5 = 101, Value 6 = 110 -> diff = 011 (bits 0 and 1 flip)
        sf.insert(10, 5);
        sf.merge_all();

        // Before update: bit 0 = 1, bit 1 = 0, bit 2 = 1
        assert!(sf.layer(0).unwrap().contains(10));
        assert!(!sf.layer(1).unwrap().contains(10));
        assert!(sf.layer(2).unwrap().contains(10));

        sf.update(10, 5, 6);
        sf.merge_all();

        // After update: bit 0 = 0, bit 1 = 1, bit 2 = 1
        assert!(!sf.layer(0).unwrap().contains(10));
        assert!(sf.layer(1).unwrap().contains(10));
        assert!(sf.layer(2).unwrap().contains(10));
        assert_eq!(sf.reconstruct_value(10), 6);
    }

    #[test]
    fn test_update_same_value_noop() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(10, 42);
        sf.merge_all();
        sf.update(10, 42, 42); // No change
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(10), 42);
    }

    #[test]
    fn test_remove() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(10, 255);
        sf.merge_all();
        sf.remove(10);
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(10), 0);
    }

    #[test]
    fn test_top_n_descending() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(1, 100);
        sf.insert(2, 500);
        sf.insert(3, 200);
        sf.insert(4, 50);
        sf.insert(5, 300);
        sf.merge_all();

        let mut candidates = RoaringBitmap::new();
        for i in 1..=5 {
            candidates.insert(i);
        }

        let result = sf.top_n(&candidates, 3, true, None);
        assert_eq!(result, vec![2, 5, 3]); // 500, 300, 200
    }

    #[test]
    fn test_top_n_ascending() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(1, 100);
        sf.insert(2, 500);
        sf.insert(3, 200);
        sf.insert(4, 50);
        sf.insert(5, 300);
        sf.merge_all();

        let mut candidates = RoaringBitmap::new();
        for i in 1..=5 {
            candidates.insert(i);
        }

        let result = sf.top_n(&candidates, 3, false, None);
        assert_eq!(result, vec![4, 1, 3]); // 50, 100, 200
    }

    #[test]
    fn test_top_n_with_limit_larger_than_candidates() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(1, 10);
        sf.insert(2, 20);
        sf.merge_all();

        let mut candidates = RoaringBitmap::new();
        candidates.insert(1);
        candidates.insert(2);

        let result = sf.top_n(&candidates, 100, true, None);
        assert_eq!(result.len(), 2);
        assert_eq!(result, vec![2, 1]); // 20, 10
    }

    #[test]
    fn test_top_n_tiebreak_by_slot_id() {
        let mut sf = SortField::new(make_config("count"));
        // Multiple slots with the same value
        sf.insert(10, 42);
        sf.insert(20, 42);
        sf.insert(30, 42);
        sf.merge_all();

        let mut candidates = RoaringBitmap::new();
        candidates.insert(10);
        candidates.insert(20);
        candidates.insert(30);

        // Descending: higher slot ID first for tiebreak
        let result = sf.top_n(&candidates, 3, true, None);
        assert_eq!(result, vec![30, 20, 10]);

        // Ascending: lower slot ID first for tiebreak
        let result = sf.top_n(&candidates, 3, false, None);
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[test]
    fn test_top_n_empty_candidates() {
        let sf = SortField::new(make_config("count"));
        let candidates = RoaringBitmap::new();
        let result = sf.top_n(&candidates, 10, true, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_cursor_pagination_descending() {
        let mut sf = SortField::new(make_config("reactionCount"));
        for i in 1..=10u32 {
            sf.insert(i, i * 10);
        }
        sf.merge_all();

        let mut candidates = RoaringBitmap::new();
        for i in 1..=10 {
            candidates.insert(i);
        }

        // First page: top 3 descending
        let page1 = sf.top_n(&candidates, 3, true, None);
        assert_eq!(page1, vec![10, 9, 8]); // values 100, 90, 80

        // Second page: cursor at (80, 8)
        let page2 = sf.top_n(&candidates, 3, true, Some((80, 8)));
        assert_eq!(page2, vec![7, 6, 5]); // values 70, 60, 50
    }

    #[test]
    fn test_cursor_pagination_ascending() {
        let mut sf = SortField::new(make_config("count"));
        for i in 1..=10u32 {
            sf.insert(i, i * 10);
        }
        sf.merge_all();

        let mut candidates = RoaringBitmap::new();
        for i in 1..=10 {
            candidates.insert(i);
        }

        // First page: top 3 ascending
        let page1 = sf.top_n(&candidates, 3, false, None);
        assert_eq!(page1, vec![1, 2, 3]); // values 10, 20, 30

        // Second page: cursor at (30, 3)
        let page2 = sf.top_n(&candidates, 3, false, Some((30, 3)));
        assert_eq!(page2, vec![4, 5, 6]); // values 40, 50, 60
    }

    #[test]
    fn test_sort_index_multi_field() {
        let mut index = SortIndex::new();
        index.add_field(make_config("reactionCount"));
        index.add_field(make_config("commentCount"));

        index.get_field_mut("reactionCount").unwrap().insert(1, 100);
        index.get_field_mut("reactionCount").unwrap().merge_all();
        index.get_field_mut("commentCount").unwrap().insert(1, 5);
        index.get_field_mut("commentCount").unwrap().merge_all();

        assert_eq!(
            index
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(1),
            100
        );
        assert_eq!(
            index
                .get_field("commentCount")
                .unwrap()
                .reconstruct_value(1),
            5
        );
    }

    #[test]
    fn test_multiple_slots_independent() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(1, 100);
        sf.insert(2, 200);
        sf.insert(3, 300);
        sf.merge_all();

        assert_eq!(sf.reconstruct_value(1), 100);
        assert_eq!(sf.reconstruct_value(2), 200);
        assert_eq!(sf.reconstruct_value(3), 300);

        // Update one doesn't affect others
        sf.update(2, 200, 999);
        sf.merge_all();
        assert_eq!(sf.reconstruct_value(1), 100);
        assert_eq!(sf.reconstruct_value(2), 999);
        assert_eq!(sf.reconstruct_value(3), 300);
    }
}
