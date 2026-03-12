//! Radix Sort Index — 8-bit bucketed sort structure for expanded cache entries.
//!
//! Replaces sorted Vec<u64> for entries >4K items. Slots are bucketed by the top 8 bits
//! of their sort value into 256 roaring bitmaps. Cumulative rank arrays enable O(1)
//! offset skipping for deep pagination. Maintenance is O(1) per slot (bitmap insert/remove
//! into the target bucket) vs O(n) memmove for sorted vecs.
//!
//! Benchmarked at 64K items:
//! - Formation: ~1ms (precomputed values)
//! - Deep pagination skip: 22-385x faster than fetch+drop
//! - Insert 1000: 58μs vs 4.2ms sorted vec (72x faster)

use roaring::RoaringBitmap;

use crate::query::SortDirection;

/// 8-bit radix sort index for fast pagination on expanded cache entries.
#[derive(Clone)]
pub struct RadixSortIndex {
    /// 256 buckets indexed by top 8 bits of sort value.
    /// None = empty bucket (zero allocation).
    buckets: [Option<RoaringBitmap>; 256],
    /// Cumulative slot counts for DESC iteration: cumulative_desc[i] = total slots in buckets 255..=i.
    cumulative_desc: [u32; 256],
    /// Cumulative slot counts for ASC iteration: cumulative_asc[i] = total slots in buckets 0..=i.
    cumulative_asc: [u32; 256],
    /// Dirty flag: set on insert/remove, cleared on cumulative rebuild.
    counts_dirty: bool,
}

impl RadixSortIndex {
    /// Build from pre-computed (slot, sort_value) pairs.
    /// This is the formation path used during expand().
    pub fn from_entries(entries: impl Iterator<Item = (u32, u32)>) -> Self {
        let mut buckets: [Option<RoaringBitmap>; 256] = std::array::from_fn(|_| None);

        for (slot, value) in entries {
            let prefix = (value >> 24) as usize;
            buckets[prefix]
                .get_or_insert_with(RoaringBitmap::new)
                .insert(slot);
        }

        let mut index = Self {
            buckets,
            cumulative_desc: [0; 256],
            cumulative_asc: [0; 256],
            counts_dirty: true,
        };
        index.rebuild_counts();
        index
    }

    /// Build from a bitmap + value function. Used during rebuild().
    pub fn from_bitmap(bitmap: &RoaringBitmap, value_fn: &impl Fn(u32) -> u32) -> Self {
        Self::from_entries(bitmap.iter().map(|slot| (slot, value_fn(slot))))
    }

    /// Insert a slot with known sort value.
    pub fn insert(&mut self, slot: u32, sort_value: u32) {
        let prefix = (sort_value >> 24) as usize;
        self.buckets[prefix]
            .get_or_insert_with(RoaringBitmap::new)
            .insert(slot);
        self.counts_dirty = true;
    }

    /// Remove a slot with known sort value.
    pub fn remove(&mut self, slot: u32, sort_value: u32) {
        let prefix = (sort_value >> 24) as usize;
        if let Some(ref mut bm) = self.buckets[prefix] {
            bm.remove(slot);
        }
        self.counts_dirty = true;
    }

    /// Remove a slot without knowing its sort value. Scans all buckets.
    /// Used on delete paths where sort value isn't readily available.
    pub fn remove_blind(&mut self, slot: u32) {
        for bucket in self.buckets.iter_mut().flatten() {
            if bucket.contains(slot) {
                bucket.remove(slot);
                self.counts_dirty = true;
                return;
            }
        }
    }

    /// Recompute cumulative count arrays from bucket cardinalities.
    pub fn rebuild_counts(&mut self) {
        // DESC: iterate 255 → 0
        let mut running = 0u32;
        for i in (0..256).rev() {
            running += self.buckets[i]
                .as_ref()
                .map(|bm| bm.len() as u32)
                .unwrap_or(0);
            self.cumulative_desc[i] = running;
        }

        // ASC: iterate 0 → 255
        let mut running = 0u32;
        for i in 0..256 {
            running += self.buckets[i]
                .as_ref()
                .map(|bm| bm.len() as u32)
                .unwrap_or(0);
            self.cumulative_asc[i] = running;
        }

        self.counts_dirty = false;
    }

    /// Whether cumulative counts need rebuilding.
    pub fn is_dirty(&self) -> bool {
        self.counts_dirty
    }

    /// Total number of slots across all buckets.
    pub fn total_slots(&self) -> u32 {
        // cumulative_desc[0] or cumulative_asc[255] holds the total
        if self.counts_dirty {
            self.buckets
                .iter()
                .filter_map(|b| b.as_ref())
                .map(|bm| bm.len() as u32)
                .sum()
        } else {
            self.cumulative_desc[0]
        }
    }

    /// Find which bucket contains the given offset and the within-bucket offset.
    ///
    /// Returns `(bucket_prefix, within_bucket_offset)`.
    /// Caller should then do `top_n(&bucket, within_offset + limit, ...)` and skip.
    ///
    /// Rebuilds cumulative counts if dirty.
    pub fn offset_to_bucket(&mut self, offset: usize, direction: SortDirection) -> Option<(u8, usize)> {
        if self.counts_dirty {
            self.rebuild_counts();
        }

        match direction {
            SortDirection::Desc => {
                // Walk from prefix 255 → 0
                let mut prev_cum = 0usize;
                for i in (0..256).rev() {
                    let cum = self.cumulative_desc[i] as usize;
                    // cumulative_desc[i] is running total from 255 down to i
                    // But we need: total from 255 down to (i+1) as prev_cum
                    // Actually cumulative_desc stores total from 255..=i
                    // So items above i: cumulative_desc[i+1] if i<255, else 0
                    // Let me just use the running sum approach
                    if cum > offset {
                        let within = offset - prev_cum;
                        return Some((i as u8, within));
                    }
                    prev_cum = cum;
                }
                None
            }
            SortDirection::Asc => {
                let mut prev_cum = 0usize;
                for i in 0..256 {
                    let cum = self.cumulative_asc[i] as usize;
                    if cum > offset {
                        let within = offset - prev_cum;
                        return Some((i as u8, within));
                    }
                    prev_cum = cum;
                }
                None
            }
        }
    }

    /// Get a reference to a specific bucket.
    pub fn bucket(&self, prefix: u8) -> Option<&RoaringBitmap> {
        self.buckets[prefix as usize].as_ref()
    }

    /// Iterate non-empty buckets in sort order.
    /// DESC: 255 → 0, ASC: 0 → 255.
    pub fn iter_buckets(&self, direction: SortDirection) -> RadixBucketIter<'_> {
        RadixBucketIter {
            buckets: &self.buckets,
            direction,
            pos: match direction {
                SortDirection::Desc => 255i16,
                SortDirection::Asc => 0,
            },
        }
    }

    /// Memory usage estimate.
    pub fn memory_bytes(&self) -> usize {
        let bucket_overhead = 256 * std::mem::size_of::<Option<RoaringBitmap>>();
        let cumulative_overhead = 2 * 256 * std::mem::size_of::<u32>();
        let bitmap_bytes: usize = self
            .buckets
            .iter()
            .filter_map(|b| b.as_ref())
            .map(|bm| bm.serialized_size())
            .sum();
        bucket_overhead + cumulative_overhead + bitmap_bytes + std::mem::size_of::<Self>()
    }

    /// Number of non-empty buckets.
    pub fn populated_buckets(&self) -> usize {
        self.buckets.iter().filter(|b| b.is_some()).count()
    }
}

/// Iterator over non-empty radix buckets in sort order.
pub struct RadixBucketIter<'a> {
    buckets: &'a [Option<RoaringBitmap>; 256],
    direction: SortDirection,
    pos: i16,
}

impl<'a> Iterator for RadixBucketIter<'a> {
    type Item = (u8, &'a RoaringBitmap);

    fn next(&mut self) -> Option<Self::Item> {
        match self.direction {
            SortDirection::Desc => {
                while self.pos >= 0 {
                    let idx = self.pos as usize;
                    self.pos -= 1;
                    if let Some(ref bm) = self.buckets[idx] {
                        if !bm.is_empty() {
                            return Some((idx as u8, bm));
                        }
                    }
                }
                None
            }
            SortDirection::Asc => {
                while self.pos <= 255 {
                    let idx = self.pos as usize;
                    self.pos += 1;
                    if let Some(ref bm) = self.buckets[idx] {
                        if !bm.is_empty() {
                            return Some((idx as u8, bm));
                        }
                    }
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(values: &[(u32, u32)]) -> Vec<(u32, u32)> {
        values.to_vec()
    }

    #[test]
    fn test_from_entries_basic() {
        // Slots with different top-8-bit prefixes
        let entries = make_entries(&[
            (0, 0xFF00_0000), // prefix 0xFF
            (1, 0xFE00_0000), // prefix 0xFE
            (2, 0x0100_0000), // prefix 0x01
            (3, 0x0000_0000), // prefix 0x00
        ]);

        let index = RadixSortIndex::from_entries(entries.into_iter());
        assert_eq!(index.total_slots(), 4);
        assert_eq!(index.populated_buckets(), 4);
        assert!(index.bucket(0xFF).unwrap().contains(0));
        assert!(index.bucket(0xFE).unwrap().contains(1));
        assert!(index.bucket(0x01).unwrap().contains(2));
        assert!(index.bucket(0x00).unwrap().contains(3));
    }

    #[test]
    fn test_from_entries_same_bucket() {
        let entries = make_entries(&[
            (0, 0x8000_0000),
            (1, 0x80FF_FFFF),
            (2, 0x8050_0000),
        ]);

        let index = RadixSortIndex::from_entries(entries.into_iter());
        assert_eq!(index.total_slots(), 3);
        assert_eq!(index.populated_buckets(), 1);
        let bucket = index.bucket(0x80).unwrap();
        assert_eq!(bucket.len(), 3);
    }

    #[test]
    fn test_insert_and_remove() {
        let mut index = RadixSortIndex::from_entries(std::iter::empty());
        assert_eq!(index.total_slots(), 0);

        index.insert(10, 0xFF00_0000);
        index.insert(20, 0xFF80_0000);
        index.insert(30, 0x0100_0000);
        assert!(index.is_dirty());

        index.rebuild_counts();
        assert!(!index.is_dirty());
        assert_eq!(index.total_slots(), 3);

        index.remove(10, 0xFF00_0000);
        assert!(index.is_dirty());
        index.rebuild_counts();
        assert_eq!(index.total_slots(), 2);
        assert!(!index.bucket(0xFF).unwrap().contains(10));
    }

    #[test]
    fn test_remove_blind() {
        let entries = make_entries(&[
            (0, 0xFF00_0000),
            (1, 0x8000_0000),
            (2, 0x0100_0000),
        ]);
        let mut index = RadixSortIndex::from_entries(entries.into_iter());
        assert_eq!(index.total_slots(), 3);

        index.remove_blind(1);
        index.rebuild_counts();
        assert_eq!(index.total_slots(), 2);
        assert!(index.bucket(0x80).is_some()); // bucket still exists but slot is gone
        assert!(!index.bucket(0x80).unwrap().contains(1));
    }

    #[test]
    fn test_cumulative_counts_desc() {
        // 3 slots in bucket 0xFF, 2 in 0x80, 1 in 0x00
        let entries = make_entries(&[
            (0, 0xFF00_0000), (1, 0xFF10_0000), (2, 0xFF20_0000),
            (3, 0x8000_0000), (4, 0x80FF_FFFF),
            (5, 0x0000_0000),
        ]);
        let index = RadixSortIndex::from_entries(entries.into_iter());

        // DESC cumulative: from 255 down
        // cumulative_desc[255] = 3 (bucket 0xFF)
        // cumulative_desc[128] = 3 + 2 = 5 (buckets 0xFF + 0x80)
        // cumulative_desc[0] = 5 + 1 = 6 (all)
        assert_eq!(index.cumulative_desc[255], 3);
        assert_eq!(index.cumulative_desc[128], 5);
        assert_eq!(index.cumulative_desc[0], 6);
    }

    #[test]
    fn test_cumulative_counts_asc() {
        let entries = make_entries(&[
            (0, 0xFF00_0000), (1, 0xFF10_0000), (2, 0xFF20_0000),
            (3, 0x8000_0000), (4, 0x80FF_FFFF),
            (5, 0x0000_0000),
        ]);
        let index = RadixSortIndex::from_entries(entries.into_iter());

        // ASC cumulative: from 0 up
        // cumulative_asc[0] = 1
        // cumulative_asc[128] = 1 + 2 = 3
        // cumulative_asc[255] = 3 + 3 = 6
        assert_eq!(index.cumulative_asc[0], 1);
        assert_eq!(index.cumulative_asc[128], 3);
        assert_eq!(index.cumulative_asc[255], 6);
    }

    #[test]
    fn test_offset_to_bucket_desc() {
        // 3 in 0xFF, 2 in 0x80, 1 in 0x00
        let entries = make_entries(&[
            (0, 0xFF00_0000), (1, 0xFF10_0000), (2, 0xFF20_0000),
            (3, 0x8000_0000), (4, 0x80FF_FFFF),
            (5, 0x0000_0000),
        ]);
        let mut index = RadixSortIndex::from_entries(entries.into_iter());

        // Offset 0: first bucket (0xFF), within_offset=0
        assert_eq!(index.offset_to_bucket(0, SortDirection::Desc), Some((0xFF, 0)));
        // Offset 2: still in 0xFF, within=2
        assert_eq!(index.offset_to_bucket(2, SortDirection::Desc), Some((0xFF, 2)));
        // Offset 3: past 0xFF (3 items), into 0x80, within=0
        assert_eq!(index.offset_to_bucket(3, SortDirection::Desc), Some((0x80, 0)));
        // Offset 5: past 0xFF+0x80 (5 items), into 0x00, within=0
        assert_eq!(index.offset_to_bucket(5, SortDirection::Desc), Some((0x00, 0)));
        // Offset 6: past all items
        assert_eq!(index.offset_to_bucket(6, SortDirection::Desc), None);
    }

    #[test]
    fn test_offset_to_bucket_asc() {
        let entries = make_entries(&[
            (0, 0xFF00_0000), (1, 0xFF10_0000), (2, 0xFF20_0000),
            (3, 0x8000_0000), (4, 0x80FF_FFFF),
            (5, 0x0000_0000),
        ]);
        let mut index = RadixSortIndex::from_entries(entries.into_iter());

        // Offset 0: first bucket ASC (0x00), within=0
        assert_eq!(index.offset_to_bucket(0, SortDirection::Asc), Some((0x00, 0)));
        // Offset 1: past 0x00 (1 item), into 0x80, within=0
        assert_eq!(index.offset_to_bucket(1, SortDirection::Asc), Some((0x80, 0)));
        // Offset 3: past 0x00+0x80 (3 items), into 0xFF, within=0
        assert_eq!(index.offset_to_bucket(3, SortDirection::Asc), Some((0xFF, 0)));
    }

    #[test]
    fn test_iter_buckets_desc() {
        let entries = make_entries(&[
            (0, 0xFF00_0000),
            (1, 0x8000_0000),
            (2, 0x0000_0000),
        ]);
        let index = RadixSortIndex::from_entries(entries.into_iter());

        let prefixes: Vec<u8> = index.iter_buckets(SortDirection::Desc).map(|(p, _)| p).collect();
        assert_eq!(prefixes, vec![0xFF, 0x80, 0x00]);
    }

    #[test]
    fn test_iter_buckets_asc() {
        let entries = make_entries(&[
            (0, 0xFF00_0000),
            (1, 0x8000_0000),
            (2, 0x0000_0000),
        ]);
        let index = RadixSortIndex::from_entries(entries.into_iter());

        let prefixes: Vec<u8> = index.iter_buckets(SortDirection::Asc).map(|(p, _)| p).collect();
        assert_eq!(prefixes, vec![0x00, 0x80, 0xFF]);
    }

    #[test]
    fn test_memory_bytes() {
        let entries = make_entries(&[(0, 0xFF00_0000), (1, 0x8000_0000)]);
        let index = RadixSortIndex::from_entries(entries.into_iter());
        let mem = index.memory_bytes();
        // Should be reasonable — struct overhead + 2 small bitmaps + cumulative arrays
        assert!(mem > 0);
        assert!(mem < 100_000); // well under 100KB for 2 items
    }

    #[test]
    fn test_empty_index() {
        let index = RadixSortIndex::from_entries(std::iter::empty());
        assert_eq!(index.total_slots(), 0);
        assert_eq!(index.populated_buckets(), 0);
        assert_eq!(index.iter_buckets(SortDirection::Desc).count(), 0);
    }

    #[test]
    fn test_from_bitmap() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0);
        bitmap.insert(1);
        bitmap.insert(2);

        // Value function: slot * 0x01000000 (each slot in different bucket)
        let index = RadixSortIndex::from_bitmap(&bitmap, &|slot| slot * 0x0100_0000);
        assert_eq!(index.total_slots(), 3);
        assert!(index.bucket(0x00).unwrap().contains(0));
        assert!(index.bucket(0x01).unwrap().contains(1));
        assert!(index.bucket(0x02).unwrap().contains(2));
    }

    #[test]
    fn test_dirty_flag_lifecycle() {
        let mut index = RadixSortIndex::from_entries(std::iter::empty());
        assert!(!index.is_dirty()); // clean after construction

        index.insert(0, 0xFF00_0000);
        assert!(index.is_dirty());

        index.rebuild_counts();
        assert!(!index.is_dirty());

        index.remove(0, 0xFF00_0000);
        assert!(index.is_dirty());
    }
}
