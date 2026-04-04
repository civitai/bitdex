use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::error::{BitdexError, Result};
use crate::versioned_bitmap::VersionedBitmap;

/// Manages slot allocation, the alive bitmap, and the clean bitmap for slot recycling.
///
/// Design:
/// - Each document's Postgres ID IS the slot (its position in every bitmap).
/// - Slots are monotonically assigned via atomic counter on insert.
/// - Deleted slots are NOT immediately recycled -- the alive bitmap hides them.
/// - An autovac process periodically produces a clean bitmap of recycled slots.
/// - New inserts check the clean bitmap first (grab first set bit), append only if none available.
///
/// The alive bitmap uses VersionedBitmap for diff-based mutation with eager merge.
/// Mutations write to the diff layer; merge compacts diffs before readers see them.
/// The clean bitmap remains Arc-wrapped since it is only touched by autovac.
pub struct SlotAllocator {
    /// Next slot to assign if no recycled slots are available.
    next_slot: AtomicU32,

    /// Bitmap of all alive (active) documents. ANDed into every query.
    alive: VersionedBitmap,

    /// Bitmap of slots that have been cleaned by autovac and are available for reuse.
    clean: Arc<RoaringBitmap>,

    /// Slots waiting for their scheduled activation time.
    /// Key: unix timestamp (seconds), Value: list of slots to activate.
    deferred: BTreeMap<u64, Vec<u32>>,

    /// True when no deletes have ever occurred (conservative: never reset to true).
    /// When true, the alive AND can be skipped entirely in queries since every
    /// allocated slot is alive.
    all_alive: bool,
}

impl Clone for SlotAllocator {
    fn clone(&self) -> Self {
        Self {
            next_slot: AtomicU32::new(self.next_slot.load(Ordering::Relaxed)),
            alive: self.alive.clone(),
            clean: Arc::clone(&self.clean),
            deferred: self.deferred.clone(),
            all_alive: self.all_alive,
        }
    }
}

impl SlotAllocator {
    pub fn new() -> Self {
        Self {
            next_slot: AtomicU32::new(0),
            alive: VersionedBitmap::new_empty(),
            clean: Arc::new(RoaringBitmap::new()),
            deferred: BTreeMap::new(),
            all_alive: true,
        }
    }

    /// Restore from a known state (e.g., after snapshot load).
    /// Conservative: assumes deletes may have occurred (all_alive = false).
    pub fn from_state(next_slot: u32, alive: RoaringBitmap, clean: RoaringBitmap) -> Self {
        Self {
            next_slot: AtomicU32::new(next_slot),
            alive: VersionedBitmap::new(alive),
            clean: Arc::new(clean),
            deferred: BTreeMap::new(),
            all_alive: false,
        }
    }

    /// Allocate a slot for the given Postgres ID.
    ///
    /// Since slot = Postgres ID, we just set the alive bit at that position.
    /// The atomic counter tracks the high-water mark for sequential inserts.
    pub fn allocate(&mut self, postgres_id: u32) -> Result<u32> {
        let slot = postgres_id;

        // Update high-water mark if this ID is beyond current counter
        let current = self.next_slot.load(Ordering::Relaxed);
        if slot >= current {
            self.next_slot.store(slot + 1, Ordering::Relaxed);
        }

        // Remove from clean bitmap if it was a recycled slot
        Arc::make_mut(&mut self.clean).remove(slot);

        // Mark as alive (writes to diff layer)
        self.alive.insert(slot);

        Ok(slot)
    }

    /// Allocate the next sequential slot (when no specific ID is needed).
    /// Checks the clean bitmap first for recycled slots.
    pub fn allocate_next(&mut self) -> u32 {
        // Try to reuse a cleaned slot first
        if let Some(recycled) = self.clean.min() {
            Arc::make_mut(&mut self.clean).remove(recycled);
            self.alive.insert(recycled);
            return recycled;
        }

        // No recycled slots, use next sequential
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        self.alive.insert(slot);
        slot
    }

    /// Delete a document by clearing its alive bit. This is the ENTIRE delete operation.
    /// Stale bits in filter/sort bitmaps are invisible behind the alive gate.
    pub fn delete(&mut self, slot: u32) -> Result<()> {
        if !self.alive.contains(slot) {
            return Err(BitdexError::SlotNotFound(slot));
        }
        self.alive.remove(slot);
        self.all_alive = false;
        Ok(())
    }

    /// Check if a slot is alive (checks base + diff).
    pub fn is_alive(&self, slot: u32) -> bool {
        self.alive.contains(slot)
    }

    /// Check if a slot was ever allocated (alive or dead with stale bits).
    /// Returns true if the slot ID is below the high-water mark, meaning it
    /// was previously used and may have stale filter/sort bits that need clearing.
    pub fn was_ever_allocated(&self, slot: u32) -> bool {
        slot < self.next_slot.load(Ordering::Relaxed)
    }

    /// Returns true when no deletes have ever occurred, meaning the alive bitmap
    /// equals the full set of allocated slots. When true, queries can skip the
    /// alive AND entirely (saves 0.5-2ms at 104M scale).
    pub fn all_slots_alive(&self) -> bool {
        self.all_alive
    }

    /// Get a reference to the alive bitmap's base. This is ANDed into every query.
    /// Requires that the alive bitmap has been merged (no pending diff).
    pub fn alive_bitmap(&self) -> &RoaringBitmap {
        self.alive.base()
    }

    /// Zero-copy alive bitmap: borrows the base when clean, creates a temp
    /// merged bitmap when dirty. Used for snapshot serialization.
    pub fn alive_fused_cow(&self) -> Cow<'_, RoaringBitmap> {
        self.alive.fused_cow()
    }

    /// Bulk-insert slots into the alive bitmap's diff layer.
    /// Also updates the high-water mark (next_slot) for persistence.
    pub fn alive_insert_bulk(&mut self, slots: impl IntoIterator<Item = u32>) {
        let mut max_seen = self.next_slot.load(Ordering::Relaxed);
        let slots_vec: Vec<u32> = slots.into_iter().collect();
        for &slot in &slots_vec {
            if slot >= max_seen {
                max_seen = slot + 1;
            }
        }
        if max_seen > self.next_slot.load(Ordering::Relaxed) {
            self.next_slot.store(max_seen, Ordering::Relaxed);
        }
        self.alive.insert_bulk(slots_vec);
    }

    /// OR a RoaringBitmap directly into the alive bitmap's base.
    /// Bypasses the diff layer for maximum bulk-load throughput.
    /// Also updates the high-water mark (next_slot).
    pub fn alive_or_bitmap(&mut self, bitmap: &RoaringBitmap) {
        if let Some(max_slot) = bitmap.max() {
            let current = self.next_slot.load(Ordering::Relaxed);
            if max_slot + 1 > current {
                self.next_slot.store(max_slot + 1, Ordering::Relaxed);
            }
        }
        self.alive.or_into_base(bitmap);
    }

    /// Remove a single slot from the alive bitmap's diff layer.
    pub fn alive_remove_one(&mut self, slot: u32) {
        self.alive.remove(slot);
        self.all_alive = false;
    }

    /// Merge the alive bitmap's diff into its base.
    pub fn merge_alive(&mut self) {
        self.alive.merge();
    }

    /// Get the clean bitmap (recycled slots available for reuse).
    pub fn clean_bitmap(&self) -> &RoaringBitmap {
        &self.clean
    }

    /// Get a mutable reference to the clean bitmap (CoW via Arc::make_mut).
    pub fn clean_bitmap_mut(&mut self) -> &mut RoaringBitmap {
        Arc::make_mut(&mut self.clean)
    }

    /// Mark a slot as clean (recycled by autovac, available for reuse).
    pub fn mark_clean(&mut self, slot: u32) {
        debug_assert!(
            !self.alive.contains(slot),
            "cannot mark alive slot as clean"
        );
        Arc::make_mut(&mut self.clean).insert(slot);
    }

    /// Get the number of alive documents (fused base + diff, always accurate).
    pub fn alive_count(&self) -> u64 {
        self.alive.fused_len()
    }

    /// Get the number of dead (deleted but not yet cleaned) slots.
    ///
    /// Note: In the slot=ID model, this includes "gap" slots that were never
    /// allocated but fall below the high-water mark. For exact dead count,
    /// autovac should scan `NOT alive AND NOT clean AND slot < counter`.
    pub fn dead_count(&self) -> u64 {
        let counter = self.next_slot.load(Ordering::Relaxed) as u64;
        counter.saturating_sub(self.alive.fused_len()).saturating_sub(self.clean.len())
    }

    /// Get the number of clean (recycled) slots available for reuse.
    pub fn clean_count(&self) -> u64 {
        self.clean.len()
    }

    /// Get the high-water mark (total slots ever assigned).
    pub fn slot_counter(&self) -> u32 {
        self.next_slot.load(Ordering::Relaxed)
    }

    /// Return the serialized byte size of all bitmaps owned by the slot allocator.
    pub fn bitmap_bytes(&self) -> usize {
        self.alive.bitmap_bytes() + self.clean.serialized_size()
    }

    /// Schedule a slot for deferred activation.
    ///
    /// The slot's high-water mark is updated (it is allocated), but it is NOT set alive.
    /// Instead, it is added to the deferred map to be activated at `activate_at_unix`.
    pub fn schedule_alive(&mut self, slot: u32, activate_at_unix: u64) {
        // Update high-water mark
        let current = self.next_slot.load(Ordering::Relaxed);
        if slot >= current {
            self.next_slot.store(slot + 1, Ordering::Relaxed);
        }
        // Add to deferred map (not alive yet)
        self.deferred.entry(activate_at_unix).or_default().push(slot);
    }

    /// Activate all slots whose scheduled time has arrived (key <= now_unix).
    ///
    /// Sets the alive bit for each activated slot and returns the list of newly-alive slots.
    pub fn activate_due(&mut self, now_unix: u64) -> Vec<u32> {
        // Collect all keys <= now_unix
        let due_keys: Vec<u64> = self
            .deferred
            .range(..=now_unix)
            .map(|(k, _)| *k)
            .collect();

        let mut activated = Vec::new();
        for key in due_keys {
            if let Some(slots) = self.deferred.remove(&key) {
                for slot in slots {
                    self.alive.insert(slot);
                    activated.push(slot);
                }
            }
        }
        activated
    }

    /// Get a reference to the deferred map (for persistence).
    pub fn deferred_map(&self) -> &BTreeMap<u64, Vec<u32>> {
        &self.deferred
    }

    /// Replace the deferred map (for restore from disk).
    pub fn set_deferred(&mut self, deferred: BTreeMap<u64, Vec<u32>>) {
        self.deferred = deferred;
    }

    /// Total number of slots waiting for deferred activation.
    pub fn deferred_count(&self) -> usize {
        self.deferred.values().map(|v| v.len()).sum()
    }

    /// Check if a slot is currently in the deferred (not-yet-alive) set.
    pub fn is_deferred(&self, slot: u32) -> bool {
        self.deferred.values().any(|slots| slots.contains(&slot))
    }
}

impl Default for SlotAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_specific_id() {
        let mut alloc = SlotAllocator::new();
        let slot = alloc.allocate(42).unwrap();
        assert_eq!(slot, 42);
        assert!(alloc.is_alive(42));
        alloc.merge_alive();
        assert_eq!(alloc.alive_count(), 1);
        assert_eq!(alloc.slot_counter(), 43);
    }

    #[test]
    fn test_allocate_next_sequential() {
        let mut alloc = SlotAllocator::new();
        let s0 = alloc.allocate_next();
        let s1 = alloc.allocate_next();
        let s2 = alloc.allocate_next();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        alloc.merge_alive();
        assert_eq!(alloc.alive_count(), 3);
        assert_eq!(alloc.slot_counter(), 3);
    }

    #[test]
    fn test_delete_clears_alive() {
        let mut alloc = SlotAllocator::new();
        alloc.allocate(10).unwrap();
        assert!(alloc.is_alive(10));

        alloc.delete(10).unwrap();
        assert!(!alloc.is_alive(10));
        alloc.merge_alive();
        assert_eq!(alloc.alive_count(), 0);
    }

    #[test]
    fn test_delete_nonexistent_fails() {
        let mut alloc = SlotAllocator::new();
        assert!(alloc.delete(999).is_err());
    }

    #[test]
    fn test_delete_only_clears_alive_bit() {
        // Verify that delete does NOT touch clean bitmap
        let mut alloc = SlotAllocator::new();
        // Allocate slots 0..5, then delete slot 5 to get an exact dead count
        for i in 0..=5 {
            alloc.allocate(i).unwrap();
        }
        alloc.delete(5).unwrap();

        // Slot is dead but NOT clean (autovac hasn't run)
        assert!(!alloc.is_alive(5));
        assert!(!alloc.clean_bitmap().contains(5));
        alloc.merge_alive();
        assert_eq!(alloc.dead_count(), 1);
    }

    #[test]
    fn test_clean_bitmap_reuse() {
        let mut alloc = SlotAllocator::new();

        // Allocate and delete slot 0
        alloc.allocate(0).unwrap();
        alloc.merge_alive();
        alloc.delete(0).unwrap();
        alloc.merge_alive();

        // Simulate autovac marking slot 0 as clean
        alloc.mark_clean(0);
        assert_eq!(alloc.clean_count(), 1);

        // Next allocation should reuse the cleaned slot
        let reused = alloc.allocate_next();
        assert_eq!(reused, 0);
        assert!(alloc.is_alive(0));
        assert_eq!(alloc.clean_count(), 0);
    }

    #[test]
    fn test_high_water_mark_updates() {
        let mut alloc = SlotAllocator::new();
        alloc.allocate(100).unwrap();
        assert_eq!(alloc.slot_counter(), 101);

        alloc.allocate(50).unwrap();
        assert_eq!(alloc.slot_counter(), 101); // Doesn't decrease

        alloc.allocate(200).unwrap();
        assert_eq!(alloc.slot_counter(), 201);
    }

    #[test]
    fn test_alive_bitmap_query_gate() {
        let mut alloc = SlotAllocator::new();
        for i in 0..10 {
            alloc.allocate(i).unwrap();
        }
        // Delete evens
        for i in (0..10).step_by(2) {
            alloc.delete(i).unwrap();
        }
        alloc.merge_alive();

        let alive = alloc.alive_bitmap();
        assert_eq!(alive.len(), 5);
        for i in 0..10 {
            if i % 2 == 0 {
                assert!(!alive.contains(i));
            } else {
                assert!(alive.contains(i));
            }
        }
    }

    #[test]
    fn test_dead_count() {
        let mut alloc = SlotAllocator::new();
        for i in 0..10 {
            alloc.allocate(i).unwrap();
        }
        alloc.delete(3).unwrap();
        alloc.delete(7).unwrap();
        alloc.merge_alive();
        assert_eq!(alloc.dead_count(), 2);

        // Clean one of them
        alloc.mark_clean(3);
        assert_eq!(alloc.dead_count(), 1);
        assert_eq!(alloc.clean_count(), 1);
    }

    #[test]
    fn test_from_state_restore() {
        let mut alive = RoaringBitmap::new();
        alive.insert(0);
        alive.insert(5);
        alive.insert(10);

        let mut clean = RoaringBitmap::new();
        clean.insert(3);

        let alloc = SlotAllocator::from_state(11, alive, clean);
        assert_eq!(alloc.alive_count(), 3);
        assert_eq!(alloc.clean_count(), 1);
        assert_eq!(alloc.slot_counter(), 11);
        assert!(alloc.is_alive(5));
        assert!(!alloc.is_alive(3));
    }

    #[test]
    fn test_schedule_alive_not_immediately_alive() {
        let mut alloc = SlotAllocator::new();
        // Schedule slot 42 to activate at unix time 9999
        alloc.schedule_alive(42, 9999);

        // Slot is NOT alive yet
        assert!(!alloc.is_alive(42));
        // But high-water mark is updated
        assert_eq!(alloc.slot_counter(), 43);
        // deferred_count reflects 1 pending slot
        assert_eq!(alloc.deferred_count(), 1);
        assert!(alloc.is_deferred(42));
    }

    #[test]
    fn test_activate_due_future_time_does_not_activate() {
        let mut alloc = SlotAllocator::new();
        alloc.schedule_alive(10, 1_000_000);

        // Passing a time before the scheduled time does nothing
        let activated = alloc.activate_due(500_000);
        assert!(activated.is_empty());
        assert!(!alloc.is_alive(10));
        assert_eq!(alloc.deferred_count(), 1);
    }

    #[test]
    fn test_activate_due_past_time_activates_slot() {
        let mut alloc = SlotAllocator::new();
        alloc.schedule_alive(10, 1_000_000);

        // Passing a time at or after the scheduled time activates
        let activated = alloc.activate_due(1_000_000);
        assert_eq!(activated, vec![10]);
        assert!(alloc.is_alive(10));
        assert_eq!(alloc.deferred_count(), 0);
        assert!(!alloc.is_deferred(10));
    }

    #[test]
    fn test_activate_due_drains_correct_subset() {
        let mut alloc = SlotAllocator::new();
        // Three slots at different times
        alloc.schedule_alive(1, 100);
        alloc.schedule_alive(2, 200);
        alloc.schedule_alive(3, 300);

        assert_eq!(alloc.deferred_count(), 3);

        // Activate up to t=150 — only slot 1 should activate
        let activated = alloc.activate_due(150);
        assert_eq!(activated, vec![1]);
        assert!(alloc.is_alive(1));
        assert!(!alloc.is_alive(2));
        assert!(!alloc.is_alive(3));
        assert_eq!(alloc.deferred_count(), 2);

        // Activate up to t=200 — slot 2 activates
        let activated = alloc.activate_due(200);
        assert_eq!(activated, vec![2]);
        assert!(alloc.is_alive(2));
        assert!(!alloc.is_alive(3));
        assert_eq!(alloc.deferred_count(), 1);

        // Activate up to t=9999 — slot 3 activates
        let activated = alloc.activate_due(9999);
        assert_eq!(activated, vec![3]);
        assert!(alloc.is_alive(3));
        assert_eq!(alloc.deferred_count(), 0);
    }

    #[test]
    fn test_deferred_count_accuracy() {
        let mut alloc = SlotAllocator::new();
        assert_eq!(alloc.deferred_count(), 0);

        alloc.schedule_alive(10, 500);
        alloc.schedule_alive(11, 500); // same timestamp bucket
        alloc.schedule_alive(12, 600);
        assert_eq!(alloc.deferred_count(), 3);

        let activated = alloc.activate_due(500);
        assert_eq!(activated.len(), 2);
        assert_eq!(alloc.deferred_count(), 1);

        alloc.activate_due(600);
        assert_eq!(alloc.deferred_count(), 0);
    }

    #[test]
    fn test_schedule_alive_updates_high_water_mark() {
        let mut alloc = SlotAllocator::new();
        alloc.schedule_alive(99, 1000);
        assert_eq!(alloc.slot_counter(), 100);

        // Lower ID does not decrease counter
        alloc.schedule_alive(50, 2000);
        assert_eq!(alloc.slot_counter(), 100);
    }

    #[test]
    fn test_is_deferred_false_after_activation() {
        let mut alloc = SlotAllocator::new();
        alloc.schedule_alive(7, 100);
        assert!(alloc.is_deferred(7));

        alloc.activate_due(100);
        assert!(!alloc.is_deferred(7));
        assert!(alloc.is_alive(7));
    }
}
