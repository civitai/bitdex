use ahash::AHashMap as HashMap;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};

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
pub struct SortField {
    /// One bitmap per bit position. Index 0 = LSB, index 31 = MSB (for 32-bit).
    bit_layers: Vec<VersionedBitmap>,
    /// Number of bit layers (typically 32 for u32).
    num_bits: usize,
    /// Field configuration.
    config: SortFieldConfig,
    /// Lazily-built fused view of every bit layer, shared across ALL queries
    /// against this SortField instance (2026-07-10 memory incident fix).
    ///
    /// Why this is safe: SortIndex wraps every SortField in an Arc and routes
    /// ALL mutation through `Arc::make_mut` (`get_field_mut`/`fields_mut`).
    /// When a published snapshot shares the field, staging's first mutation
    /// clones it — the snapshot's instance is frozen for its lifetime, so a
    /// fused view built against it can never go stale. Two backstops make
    /// the invariant unconditional rather than architectural: (1) `Clone`
    /// resets the cache (the make_mut clone starts cold), and (2) every
    /// `&mut self` method that touches `bit_layers` calls
    /// `invalidate_fused()` (covers same-instance mutation when staging is
    /// the sole owner, e.g. loading mode). MODULE RULE: never touch
    /// `bit_layers` mutably except through those methods — a new helper
    /// that writes layers directly MUST call `invalidate_fused()` or a
    /// frozen fused view will serve stale order until the next snapshot.
    ///
    /// Before this cache, `top_n` called `fused_cow()` on all layers PER
    /// QUERY, and a dirty layer (reactionCount is perpetually dirty between
    /// targeted compactions — the metrics poller writes continuously) paid a
    /// full base clone each time: ~22Gi of steady-state allocation churn on
    /// the wide-window serving pod. Now the fuse cost is paid once per
    /// field per published snapshot; clean layers are refcount bumps.
    fused_cache: Mutex<Option<Arc<Vec<Arc<RoaringBitmap>>>>>,
}

impl Clone for SortField {
    fn clone(&self) -> Self {
        Self {
            bit_layers: self.bit_layers.clone(),
            num_bits: self.num_bits,
            config: self.config.clone(),
            // The clone exists to be MUTATED (Arc::make_mut path) — start
            // cold so a stale fused view can never survive into it.
            fused_cache: Mutex::new(None),
        }
    }
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
            fused_cache: Mutex::new(None),
        }
    }

    /// Drop the cached fused view. Must be called by every `&mut self`
    /// method that changes any bit layer — exclusive access makes this a
    /// lock-free `get_mut`.
    fn invalidate_fused(&mut self) {
        *self
            .fused_cache
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// The fused (base + diff) view of every bit layer, built at most once
    /// per SortField instance and shared by all queries against it.
    ///
    /// Clean layers cost an Arc refcount bump; dirty layers are materialized
    /// exactly once. The build runs under the cache lock deliberately:
    /// concurrent first-queries against a fresh snapshot serialize on the
    /// fuse instead of each materializing their own multi-hundred-MB copy —
    /// the thundering herd IS the memory incident this fixes. Waiters pay
    /// ~the same wall time they would have paid fusing privately, minus the
    /// allocation. LOCK DISCIPLINE: the critical section must stay
    /// allocation-only — `fused_arc` takes no locks and must never grow a
    /// callback/lazy-load path that could re-enter this field's cache
    /// (self-deadlock). Poison is deliberately swallowed (into_inner): the
    /// cache is either None or a fully-assigned Arc, so a panicking peer
    /// cannot leave partial state.
    fn fused_layers(&self) -> Arc<Vec<Arc<RoaringBitmap>>> {
        let mut guard = self
            .fused_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(fused) = guard.as_ref() {
            return Arc::clone(fused);
        }
        let fused = Arc::new(
            self.bit_layers
                .iter()
                .map(|vb| vb.fused_arc())
                .collect::<Vec<_>>(),
        );
        *guard = Some(Arc::clone(&fused));
        fused
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
        self.invalidate_fused();
        for bit in 0..self.num_bits {
            if (value >> bit) & 1 == 1 {
                self.bit_layers[bit].insert(slot);
            }
        }
    }

    /// Remove a slot from all bit layers. Used by autovac.
    pub fn remove(&mut self, slot: u32) {
        self.invalidate_fused();
        for layer in &mut self.bit_layers {
            layer.remove(slot);
        }
    }

    /// Update a slot's value using XOR diff.
    /// Only flips the bit layers where old and new values differ.
    /// Uses `old_value` to decide insert vs remove (data-driven, not state-driven),
    /// so this works correctly even when bit layers are unloaded.
    pub fn update(&mut self, slot: u32, old_value: u32, new_value: u32) {
        self.invalidate_fused();
        let diff = old_value ^ new_value;
        for bit in 0..self.num_bits {
            if (diff >> bit) & 1 == 1 {
                if (old_value >> bit) & 1 == 1 {
                    // Old had this bit set, new doesn't → remove
                    self.bit_layers[bit].remove(slot);
                } else {
                    // Old didn't have this bit, new does → insert
                    self.bit_layers[bit].insert(slot);
                }
            }
        }
    }

    /// Bulk-set a bit layer for multiple slots. Slots should be sorted for performance.
    pub fn set_layer_bulk(&mut self, bit: usize, slots: impl IntoIterator<Item = u32>) {
        self.invalidate_fused();
        if let Some(layer) = self.bit_layers.get_mut(bit) {
            layer.insert_bulk(slots);
        }
    }

    /// OR a RoaringBitmap directly into a bit layer's base.
    /// Bypasses the diff layer for maximum bulk-load throughput.
    pub fn or_layer(&mut self, bit: usize, bitmap: &RoaringBitmap) {
        self.invalidate_fused();
        if let Some(layer) = self.bit_layers.get_mut(bit) {
            layer.or_into_base(bitmap);
        }
    }

    /// Bulk-clear a bit layer for multiple slots.
    pub fn clear_layer_bulk(&mut self, bit: usize, slots: &[u32]) {
        self.invalidate_fused();
        if let Some(layer) = self.bit_layers.get_mut(bit) {
            for &slot in slots {
                layer.remove(slot);
            }
        }
    }

    /// Get a reference to a specific bit layer's BASE bitmap (no diff
    /// fusion). For diff-aware reads, use `layer_fused` or fuse via
    /// `bit_layers[bit].fused_cow()`.
    ///
    /// Kept for callers that explicitly want the base only (e.g. tests
    /// asserting persisted state) or know the layer is clean.
    pub fn layer(&self, bit: usize) -> Option<&RoaringBitmap> {
        self.bit_layers.get(bit).map(|vb| vb.base().as_ref())
    }
    /// Get the fused (base + diff) bitmap for a specific bit layer.
    ///
    /// Returns `Cow::Borrowed(&base)` when the layer is clean (zero copy)
    /// or `Cow::Owned` materializing base | sets - clears when dirty.
    /// Use this in any read path that does AND/OR with sort layers; the
    /// query planner will get correct results regardless of pending diffs.
    pub fn layer_fused(&self, bit: usize) -> Option<Cow<'_, RoaringBitmap>> {
        self.bit_layers.get(bit).map(|vb| vb.fused_cow())
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

        let t_start = std::time::Instant::now();
        // Fused view from the per-snapshot cache: built once per SortField
        // instance, an Arc bump on every subsequent query. Replaces the
        // per-query fused_cow materialization that cloned every dirty
        // layer's full base per query (2026-07-10 memory incident). The
        // Cow::Borrowed wrappers borrow from `fused`, which the traversal
        // helpers below already accept — zero signature churn downstream.
        let fused = self.fused_layers();
        let layers: Vec<Cow<'_, RoaringBitmap>> =
            fused.iter().map(|arc| Cow::Borrowed(arc.as_ref())).collect();
        let t_fuse = t_start.elapsed();

        // Apply cursor filtering if present
        let effective_candidates;
        let candidates = if let Some((cursor_sort_value, cursor_slot_id)) = cursor {
            effective_candidates = self.apply_cursor_filter_with_layers(
                candidates,
                descending,
                cursor_sort_value,
                cursor_slot_id,
                &layers,
            );
            &effective_candidates
        } else {
            candidates
        };
        let t_cursor = t_start.elapsed() - t_fuse;

        if candidates.is_empty() {
            if t_start.elapsed().as_millis() > 10 {
                tracing::warn!(
                    "[sort-top_n] SLOW empty: fuse={:.1}ms cursor={:.1}ms total={:.1}ms input_card={} bits={}",
                    t_fuse.as_secs_f64()*1000.0, t_cursor.as_secs_f64()*1000.0,
                    t_start.elapsed().as_secs_f64()*1000.0, 0, self.num_bits,
                );
            }
            return Vec::new();
        }

        // MSB-to-LSB bifurcation: collect top-N slots via bitmap AND operations
        let top_n_bitmap = self.bifurcate_with_layers(candidates, limit, descending, &layers);
        let t_bifurcate = t_start.elapsed() - t_fuse - t_cursor;

        // Reconstruct values ONLY for the final top-N slots and sort them.
        // Thread the already-fused layer slice through so we don't re-fuse per bit per slot.
        let result = self.order_results_with_layers(&top_n_bitmap, descending, &layers);
        let t_order = t_start.elapsed() - t_fuse - t_cursor - t_bifurcate;

        if t_start.elapsed().as_millis() > 10 {
            tracing::warn!(
                "[sort-top_n] SLOW: fuse={:.1}ms cursor={:.1}ms bifurcate={:.1}ms order={:.1}ms total={:.1}ms input={} output={} bits={}",
                t_fuse.as_secs_f64()*1000.0, t_cursor.as_secs_f64()*1000.0,
                t_bifurcate.as_secs_f64()*1000.0, t_order.as_secs_f64()*1000.0,
                t_start.elapsed().as_secs_f64()*1000.0,
                candidates.len(), result.len(), self.num_bits,
            );
        }
        result
    }

    /// MSB-to-LSB bifurcation traversal.
    ///
    /// Walks bit layers from MSB to LSB, narrowing candidates at each layer.
    /// Returns a bitmap containing exactly min(limit, candidates.len()) top slots.
    ///
    /// Operates on pre-fused layers (Cow::Borrowed for clean, Cow::Owned for
    /// dirty) so callers can share one fused snapshot across multiple
    /// traversal passes.
    fn bifurcate_with_layers(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
        layers: &[Cow<'_, RoaringBitmap>],
    ) -> RoaringBitmap {
        let total = candidates.len() as usize;
        if total <= limit {
            return candidates.clone();
        }

        // result accumulates confirmed winners; remaining is the working set
        let mut result = RoaringBitmap::new();
        let mut remaining = candidates.clone();
        let mut remaining_limit = limit;

        let bif_start = std::time::Instant::now();
        let mut layers_touched = 0u32;
        let mut layers_narrowed = 0u32;
        let mut layers_skipped = 0u32;
        let input_card = remaining.len();

        for bit in (0..self.num_bits).rev() {
            if remaining_limit == 0 || remaining.is_empty() {
                break;
            }
            layers_touched += 1;

            let layer: &RoaringBitmap = &layers[bit];

            // preferred = slots that have the "better" bit value at this position
            let preferred = if descending {
                &remaining & layer
            } else {
                &remaining - layer
            };

            let preferred_count = preferred.len() as usize;

            if preferred_count == 0 {
                layers_skipped += 1;
                continue;
            } else if preferred_count >= remaining_limit {
                remaining = preferred;
                layers_narrowed += 1;
            } else {
                result |= &preferred;
                remaining -= &preferred;
                remaining_limit -= preferred_count;
                layers_narrowed += 1;
            }
        }

        if bif_start.elapsed().as_millis() > 5 {
            tracing::warn!(
                "[bifurcate] SLOW: {:.1}ms input={} output={} layers_touched={} narrowed={} skipped={} remaining={}",
                bif_start.elapsed().as_secs_f64() * 1000.0,
                input_card, result.len() + remaining.len().min(remaining_limit as u64),
                layers_touched, layers_narrowed, layers_skipped, remaining.len(),
            );
        }

        // After all layers, if we still need more slots, take them from
        // remaining — a band of slots with EQUAL sort values. The slots taken
        // MUST be the ones that sort FIRST under the tie order the rest of
        // the pipeline uses, or keyset pagination silently drops the rest of
        // the band: `order_results_with_layers` breaks ties by slot id
        // (descending: higher slot first) and `apply_cursor_filter_with_layers`
        // resumes a descending cursor at `slot_id < cursor.slot_id`. Taking
        // the ASCENDING end here made a descending page end on the band's
        // MINIMUM slot id, so the resume found nothing below it and the whole
        // band remainder vanished from every feed sweep (prod 2026-07-09:
        // top-reacted images with tied reactionCount at page boundaries
        // disappeared from paginated enumeration; bands of thousands at
        // rc 0-2 were each truncated to one page).
        if remaining_limit > 0 && !remaining.is_empty() {
            if descending {
                for slot in remaining.iter().rev().take(remaining_limit) {
                    result.insert(slot);
                }
            } else {
                for slot in remaining.iter().take(remaining_limit) {
                    result.insert(slot);
                }
            }
        }

        result
    }

    /// Order the top-N result bitmap into a sorted Vec.
    ///
    /// Accepts the pre-fused layer slice produced by `top_n` so we don't
    /// re-fuse base+diff per bit per slot.  On dirty layers that saves
    /// `limit × num_bits` redundant fused_cow() / fused_contains() calls
    /// (e.g. limit=20 × 32 bits = 640 ops → 0).
    ///
    /// Uses a SmallVec with inline capacity 64 so typical paginated pages (≤64 slots)
    /// avoid heap allocation entirely.
    fn order_results_with_layers(
        &self,
        result_bitmap: &RoaringBitmap,
        descending: bool,
        layers: &[Cow<'_, RoaringBitmap>],
    ) -> Vec<u32> {
        let mut entries: SmallVec<[(u32, u32); 64]> = result_bitmap
            .iter()
            .map(|slot| (slot, self.reconstruct_value_with_layers(slot, layers)))
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
    fn apply_cursor_filter_with_layers(
        &self,
        candidates: &RoaringBitmap,
        descending: bool,
        cursor_sort_value: u64,
        cursor_slot_id: u32,
        layers: &[Cow<'_, RoaringBitmap>],
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
            let layer: &RoaringBitmap = &layers[bit];

            // In-place ops: materialize at most one side per bit instead of two.
            // `equal` is updated in-place; the confirmed-feeder side is computed
            // as a single AND or SUB only when it will be OR'd into `confirmed`.
            if descending {
                // Descending: slots after cursor have a LOWER sort value.
                if cursor_bit_set {
                    // Cursor bit is 1.  Slots with bit=0 have lower value → confirmed.
                    // Slots with bit=1 remain equal.
                    // confirmed_feeder = equal - layer  (bit=0 side)
                    // new equal       = equal & layer   (bit=1 side)
                    let confirmed_feeder = &equal - layer; // one allocation
                    equal &= layer;                        // in-place, no alloc
                    confirmed |= confirmed_feeder;
                } else {
                    // Cursor bit is 0.  Slots with bit=1 have higher value → exclude.
                    // Slots with bit=0 remain equal.
                    // new equal = equal - layer  (bit=0 side)
                    equal -= layer; // in-place, no alloc
                }
            } else {
                // Ascending: slots after cursor have a HIGHER sort value.
                if cursor_bit_set {
                    // Cursor bit is 1.  Slots with bit=0 have lower value → exclude.
                    // Slots with bit=1 remain equal.
                    // new equal = equal & layer  (bit=1 side)
                    equal &= layer; // in-place, no alloc
                } else {
                    // Cursor bit is 0.  Slots with bit=1 have higher value → confirmed.
                    // Slots with bit=0 remain equal.
                    // confirmed_feeder = equal & layer  (bit=1 side)
                    // new equal        = equal - layer  (bit=0 side)
                    let confirmed_feeder = &equal & layer; // one allocation
                    equal -= layer;                        // in-place, no alloc
                    confirmed |= confirmed_feeder;
                }
            }
        }

        // After all bits: `equal` contains slots with the exact same sort value as cursor.
        // Apply slot ID tiebreaker using bitmap range ops (O(containers) not O(slots)).
        if !equal.is_empty() {
            if descending {
                // Descending: slots with lower slot_id come after cursor
                equal.remove_range(cursor_slot_id..=u32::MAX);
            } else {
                // Ascending: slots with higher slot_id come after cursor
                equal.remove_range(0..=cursor_slot_id);
            }
            confirmed |= equal;
        }

        confirmed
    }

    /// Reconstruct the sort value for a given slot.
    ///
    /// Diff-aware: uses `VersionedBitmap::fused_contains` so this works
    /// correctly when layers have unmerged diffs (lazy fuse). Cheap point
    /// query — does NOT materialize a fused bitmap, just checks each layer
    /// against base + diff.
    pub fn reconstruct_value(&self, slot: u32) -> u32 {
        let mut value = 0u32;
        for bit in 0..self.num_bits {
            if self.bit_layers[bit].fused_contains(slot) {
                value |= 1 << bit;
            }
        }
        value
    }

    /// Reconstruct the sort value for a given slot using pre-fused layer bitmaps.
    ///
    /// Callers that already hold a `Vec<Cow<RoaringBitmap>>` (e.g. `top_n`) should
    /// use this variant to avoid re-fusing base+diff for every bit on every slot.
    /// On dirty layers, `fused_cow()` materializes a new owned bitmap; calling it
    /// once per query and sharing the result is O(bits) cheaper per slot.
    fn reconstruct_value_with_layers(&self, slot: u32, layers: &[Cow<'_, RoaringBitmap>]) -> u32 {
        let mut value = 0u32;
        for (bit, layer) in layers.iter().enumerate() {
            if layer.contains(slot) {
                value |= 1 << bit;
            }
        }
        value
    }

    /// Find all slots in the given universe whose reconstructed value
    /// falls in `[min_value, max_value)`.
    ///
    /// Iterates every slot in `universe` and reconstructs its value from the
    /// bit layers. O(universe_size * num_bits) — acceptable when the matching
    /// fraction is small (e.g. a 300-second window out of 86400 seconds).
    pub fn slots_in_range(
        &self,
        universe: &RoaringBitmap,
        min_value: u32,
        max_value: u32,
    ) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for slot in universe.iter() {
            let val = self.reconstruct_value(slot);
            if val >= min_value && val < max_value {
                result.insert(slot);
            }
        }
        result
    }

    /// Merge all bit layers, compacting diffs into bases.
    pub fn merge_all(&mut self) {
        self.invalidate_fused();
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
        self.invalidate_fused();
        let total_start = std::time::Instant::now();
        let mut per_layer_us: Vec<(usize, u128, u128, u128, u64, u64, u64)> = Vec::new();
        // (bit, clone_us, set_us, sub_us, base_card_before, sets_card, clears_card)
        for (bit, layer) in self.bit_layers.iter_mut().enumerate() {
            if !layer.is_dirty() {
                continue;
            }
            let base_card = layer.base().len();
            let sets_card = layer.diff().sets.len();
            let clears_card = layer.diff().clears.len();
            let (clone_us, set_us, sub_us) = layer.merge_with_timing();
            if clone_us + set_us + sub_us > 1_000 {
                per_layer_us.push((bit, clone_us, set_us, sub_us, base_card, sets_card, clears_card));
            }
        }
        let total_us = total_start.elapsed().as_micros();
        if total_us > 50_000 {
            // Log only if total >50ms — surfaces sort merge spikes.
            // Top 5 slowest layers by total time.
            per_layer_us.sort_by(|a, b| (b.1 + b.2 + b.3).cmp(&(a.1 + a.2 + a.3)));
            let top: Vec<String> = per_layer_us
                .iter()
                .take(5)
                .map(|(b, c, s, d, bc, sc, cc)| {
                    format!(
                        "L{}(base={} sets={} clears={} clone={}μs |=={}μs -=={}μs)",
                        b, bc, sc, cc, c, s, d
                    )
                })
                .collect();
            tracing::warn!(
                "[sort_merge_slow] field={} total={}μs dirty_layers={} top=[{}]",
                self.config.name,
                total_us,
                per_layer_us.len(),
                top.join(", ")
            );
        }
    }

    /// Returns true when every bit layer is loaded (base in memory). False if
    /// any layer is in the unloaded placeholder state — in which case
    /// `reconstruct_value` may return partially-zeroed garbage for slots whose
    /// bits live only in the unloaded base.
    pub fn is_fully_loaded(&self) -> bool {
        self.bit_layers.iter().all(|vb| vb.is_loaded())
    }

    /// Load persisted base bitmaps into the sort layers, replacing the base
    /// while preserving any diff entries that accumulated while the layer was
    /// unloaded. Marks each layer as loaded.
    ///
    /// Diff preservation is the critical invariant here. When a sort field is
    /// unloaded (`save_and_unload` or `unload_from`), incoming ops continue to
    /// land in the diff layer of the unloaded VersionedBitmap so they don't
    /// disappear during the unload window. Lazy load reads the on-disk base
    /// and must merge it with those queued diffs, not replace them. The
    /// previous implementation built a fresh `VersionedBitmap::new(bm)` with
    /// an empty diff, silently dropping every op that arrived between unload
    /// and reload — leaving a sort field whose reconstructed values lagged
    /// the actual write history by however many ops were queued. The longer
    /// the unload window, the more drift a pod accumulated; this is the
    /// dominant cause of the cross-pod bucket-count divergence observed at
    /// the same WAL cursor in production.
    pub fn load_layers(&mut self, layers: Vec<RoaringBitmap>) {
        self.invalidate_fused();
        for (i, bm) in layers.into_iter().enumerate() {
            if i < self.bit_layers.len() {
                self.bit_layers[i].replace_base_preserve_diff(bm);
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

    /// Get fused bitmap references for all layers (for zero-copy persistence).
    /// Returns `Cow::Borrowed` when the layer is clean (zero copy),
    /// `Cow::Owned` when the layer has pending diffs.
    pub fn layer_bases_fused(&self) -> Vec<Cow<'_, RoaringBitmap>> {
        self.bit_layers.iter().map(|vb| vb.fused_cow()).collect()
    }

    /// Drop all base bitmaps and mark layers as unloaded.
    /// The diff layers are preserved so mutations can accumulate
    /// while the sort field is not in memory.
    pub fn clear_bases_and_unload(&mut self) {
        self.invalidate_fused();
        for layer in &mut self.bit_layers {
            layer.clear_base_and_unload();
        }
    }

    /// Return the serialized byte size of all bit layer bitmaps, INCLUDING
    /// bytes retained by the fused cache (review #306 finding 8: memory
    /// accounting that can't see the cache under-reports after warmup).
    pub fn bitmap_bytes(&self) -> usize {
        self.bit_layers.iter().map(|bm| bm.bitmap_bytes()).sum::<usize>()
            + self.fused_cache_bytes()
    }

    /// Bytes RETAINED by the fused cache beyond the layer bases: counts only
    /// dirty-layer materializations — clean layers share the base Arc, and
    /// counting those would double-report the base.
    pub fn fused_cache_bytes(&self) -> usize {
        let guard = self
            .fused_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.as_ref() {
            Some(fused) => fused
                .iter()
                .zip(self.bit_layers.iter())
                .filter(|(arc, vb)| !Arc::ptr_eq(arc, vb.base()))
                .map(|(arc, _)| arc.serialized_size() as usize)
                .sum(),
            None => 0,
        }
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
    fields: HashMap<String, Arc<SortField>>,
}

impl SortIndex {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Add a sort field from configuration.
    pub fn add_field(&mut self, config: SortFieldConfig) {
        let name = config.name.clone();
        self.fields.insert(name, Arc::new(SortField::new(config)));
    }

    /// Remove a sort field by name. Returns true if the field existed.
    pub fn remove_field(&mut self, name: &str) -> bool {
        self.fields.remove(name).is_some()
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

    /// Unload a sort field: replace its Arc with a new field containing empty layers.
    /// Diff layers are preserved for any in-flight mutations.
    pub fn unload_field(&mut self, name: &str) {
        if let Some(field_arc) = self.fields.get_mut(name) {
            let old = field_arc.as_ref();
            let mut new_field = SortField::new(old.config.clone());
            for (i, vb) in old.bit_layers.iter().enumerate() {
                if vb.is_dirty() {
                    new_field.bit_layers[i] = vb.clone_diff_only();
                } else {
                    new_field.bit_layers[i] = VersionedBitmap::new_unloaded();
                }
            }
            *field_arc = Arc::new(new_field);
        }
    }

    /// Copy a field's Arc from another SortIndex (refcount bump only, no data copy).
    pub fn copy_field_arc_from(&mut self, source: &SortIndex, name: &str) {
        if let Some(arc) = source.fields.get(name) {
            self.fields.insert(name.to_string(), Arc::clone(arc));
        }
    }

    /// Build an unloaded version of a sort field from a source SortIndex.
    /// Preserves diff layers for any in-flight mutations.
    pub fn unload_from(&mut self, source: &SortIndex, name: &str) {
        if let Some(source_field) = source.fields.get(name) {
            let mut new_field = SortField::new(source_field.config.clone());
            for (i, vb) in source_field.bit_layers.iter().enumerate() {
                if vb.is_dirty() {
                    new_field.bit_layers[i] = vb.clone_diff_only();
                } else {
                    new_field.bit_layers[i] = VersionedBitmap::new_unloaded();
                }
            }
            self.fields.insert(name.to_string(), Arc::new(new_field));
        }
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

    /// Generation isolation for the per-snapshot fused-layer cache
    /// (2026-07-10 memory incident fix), exercised through the EXACT prod
    /// sharing mechanics: SortIndex's Arc-per-field + Arc::make_mut CoW.
    /// A snapshot that fused its view before staging mutated must keep
    /// serving ITS OWN values from the SAME cached Arc, while staging's
    /// make_mut clone starts cold and fuses the new state.
    #[test]
    fn fused_cache_snapshot_isolation_across_make_mut() {
        let mut staging = SortIndex::new();
        staging.add_field(make_config("reactionCount"));
        // Dirty-layer state: inserts land in the diff (no merge).
        staging.get_field_mut("reactionCount").unwrap().insert(1, 5);

        // "Publish": snapshot clones the index — Arc refcount bump only.
        let snapshot = staging.clone();
        let snap_field = snapshot.get_field("reactionCount").unwrap();

        // First query against the snapshot builds + caches the fused view.
        let mut cands = RoaringBitmap::new();
        cands.insert(1);
        assert_eq!(snap_field.top_n(&cands, 10, true, None), vec![1]);
        let fused_a = snap_field.fused_layers();
        assert!(fused_a[0].contains(1), "bit0 of 5 set");

        // Staging mutates AFTER the snapshot fused: refcount > 1 forces
        // make_mut to clone; the snapshot's instance is frozen.
        staging
            .get_field_mut("reactionCount")
            .unwrap()
            .update(1, 5, 2);

        // Snapshot: same cached Arc, same old values.
        let fused_a2 = snap_field.fused_layers();
        assert!(
            Arc::ptr_eq(&fused_a, &fused_a2),
            "snapshot's cache must be untouched by staging mutation"
        );
        assert_eq!(snap_field.reconstruct_value(1), 5);

        // Staging: new value, cold cache (Clone reset), fresh fuse.
        let staged_field = staging.get_field("reactionCount").unwrap();
        assert_eq!(staged_field.reconstruct_value(1), 2);
        let fused_b = staged_field.fused_layers();
        assert!(
            !Arc::ptr_eq(&fused_a, &fused_b),
            "staging must not inherit the pre-mutation fused view"
        );
        assert!(!fused_b[0].contains(1), "bit0 of 2 clear");
        assert!(fused_b[1].contains(1), "bit1 of 2 set");
    }

    /// Review #306 required-change: EVERY mutating method must invalidate
    /// the fused cache — one case per method so a dropped hook fails by name.
    #[test]
    fn fused_cache_invalidated_by_every_mutator() {
        fn fresh() -> SortField {
            let mut sf = SortField::new(make_config("f"));
            sf.insert(1, 5);
            sf
        }
        let cases: Vec<(&str, Box<dyn Fn(&mut SortField)>)> = vec![
            ("insert", Box::new(|sf: &mut SortField| sf.insert(2, 9))),
            ("remove", Box::new(|sf: &mut SortField| sf.remove(1))),
            ("update", Box::new(|sf: &mut SortField| sf.update(1, 5, 2))),
            ("set_layer_bulk", Box::new(|sf: &mut SortField| sf.set_layer_bulk(0, [7u32]))),
            ("or_layer", Box::new(|sf: &mut SortField| {
                let mut bm = RoaringBitmap::new();
                bm.insert(9);
                sf.or_layer(0, &bm);
            })),
            ("clear_layer_bulk", Box::new(|sf: &mut SortField| sf.clear_layer_bulk(0, &[1]))),
            ("merge_all", Box::new(|sf: &mut SortField| sf.merge_all())),
            ("merge_dirty", Box::new(|sf: &mut SortField| sf.merge_dirty())),
            ("load_layers", Box::new(|sf: &mut SortField| {
                sf.load_layers(vec![RoaringBitmap::new()])
            })),
            ("clear_bases_and_unload", Box::new(|sf: &mut SortField| {
                sf.clear_bases_and_unload()
            })),
        ];
        for (name, mutate) in cases {
            let mut sf = fresh();
            let before = sf.fused_layers();
            mutate(&mut sf);
            let after = sf.fused_layers();
            assert!(
                !Arc::ptr_eq(&before, &after),
                "{name} must invalidate the fused cache"
            );
        }
    }

    /// Sole-owner mutation (loading mode / bulk load: refcount == 1, no
    /// make_mut clone) must invalidate the cache on the SAME instance —
    /// backstop (2) in the fused_cache doc comment.
    #[test]
    fn fused_cache_invalidated_by_in_place_mutation() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(1, 5);
        let f1 = sf.fused_layers();
        assert!(f1[0].contains(1) && f1[2].contains(1)); // 5 = 0b101

        sf.update(1, 5, 2); // in place — no Arc, no Clone
        let f2 = sf.fused_layers();
        assert!(!Arc::ptr_eq(&f1, &f2), "in-place mutation must invalidate");
        assert!(!f2[0].contains(1) && f2[1].contains(1)); // 2 = 0b010

        // merge_dirty (targeted compaction) must also invalidate: it moves
        // diff bits into the base, so a stale fused view would double-apply.
        let f3 = sf.fused_layers();
        sf.merge_dirty();
        let f4 = sf.fused_layers();
        assert!(!Arc::ptr_eq(&f3, &f4), "merge_dirty must invalidate");
        assert_eq!(sf.reconstruct_value(1), 2);
    }

    /// REGRESSION (prod 2026-07-09): descending keyset pagination silently
    /// dropped the remainder of every tied-value band that straddled a page
    /// boundary. `bifurcate_with_layers` took the tie-band tail in ASCENDING
    /// slot order while the ordering stage emits descending ties by
    /// DESCENDING slot id and the cursor resume keeps `slot_id < cursor` —
    /// so the page ended on the band's minimum slot and the resume found
    /// nothing. Prod effect: top-reacted feed images with tied
    /// reactionCounts vanished from paginated sweeps (bands of thousands at
    /// rc 0-2 truncated to one page; page-end cursor sequence showed zero
    /// repeated sort values). Paginating any tie band wider than the page
    /// size must enumerate EVERY slot exactly once.
    #[test]
    fn test_descending_pagination_enumerates_full_tie_band() {
        let mut sf = SortField::new(make_config("reactionCount"));
        let mut candidates = RoaringBitmap::new();
        // 30 slots with value 50 (the wide tie band), plus sentinels above
        // and below to make the band interior to the enumeration.
        for slot in 100..130 {
            sf.insert(slot, 50);
            candidates.insert(slot);
        }
        sf.insert(50, 99); // sorts first (desc)
        candidates.insert(50);
        sf.insert(200, 1); // sorts last
        candidates.insert(200);

        // Paginate with a page size smaller than the band.
        let mut seen = Vec::new();
        let mut cursor: Option<(u64, u32)> = None;
        loop {
            let page = sf.top_n(&candidates, 7, true, cursor);
            if page.is_empty() {
                break;
            }
            seen.extend_from_slice(&page);
            let last = *page.last().unwrap();
            cursor = Some((sf.reconstruct_value(last) as u64, last));
            if page.len() < 7 {
                break;
            }
        }

        let mut sorted_seen = seen.clone();
        sorted_seen.sort_unstable();
        sorted_seen.dedup();
        assert_eq!(
            seen.len(),
            sorted_seen.len(),
            "pagination must not emit duplicates"
        );
        let mut expected: Vec<u32> = (100..130).collect();
        expected.push(50);
        expected.push(200);
        expected.sort_unstable();
        assert_eq!(
            sorted_seen, expected,
            "every tied slot must be enumerated exactly once across pages"
        );
    }

    /// Ascending twin of the tie-band pagination regression — the ascending
    /// path took the correct end already; pin it so a symmetric refactor
    /// can't break it.
    #[test]
    fn test_ascending_pagination_enumerates_full_tie_band() {
        let mut sf = SortField::new(make_config("reactionCount"));
        let mut candidates = RoaringBitmap::new();
        for slot in 100..130 {
            sf.insert(slot, 50);
            candidates.insert(slot);
        }
        sf.insert(50, 1); // sorts first (asc)
        candidates.insert(50);
        sf.insert(200, 99); // sorts last
        candidates.insert(200);

        let mut seen = Vec::new();
        let mut cursor: Option<(u64, u32)> = None;
        loop {
            let page = sf.top_n(&candidates, 7, false, cursor);
            if page.is_empty() {
                break;
            }
            seen.extend_from_slice(&page);
            let last = *page.last().unwrap();
            cursor = Some((sf.reconstruct_value(last) as u64, last));
            if page.len() < 7 {
                break;
            }
        }
        let mut sorted_seen = seen.clone();
        sorted_seen.sort_unstable();
        sorted_seen.dedup();
        assert_eq!(seen.len(), sorted_seen.len());
        let mut expected: Vec<u32> = (100..130).collect();
        expected.push(50);
        expected.push(200);
        expected.sort_unstable();
        assert_eq!(sorted_seen, expected);
    }

    fn make_config(name: &str) -> SortFieldConfig {
        SortFieldConfig {
            name: name.to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        }
    }

    /// REGRESSION: `load_layers` previously replaced each VersionedBitmap with
    /// a fresh `VersionedBitmap::new(bm)` whose diff was empty, silently
    /// dropping any ops that landed in the diff while the field was unloaded.
    ///
    /// Production effect: between save_and_unload and the first query (which
    /// triggers lazy load), the WAL reader keeps applying sort_set/sort_clear
    /// ops to the unloaded VersionedBitmap's diff. On lazy load, those diffs
    /// vanished — the reload returned the on-disk snapshot, no more, no less.
    /// Every restart erased ops written since the last save. With longer
    /// unload windows or higher op rates, drift accumulated; in HA two pods
    /// diverged by ~70K slots at the same WAL cursor because they hit the
    /// load path with different amounts of queued diff.
    #[test]
    fn test_load_layers_preserves_diffs_accumulated_while_unloaded() {
        let mut sf = SortField::new(make_config("sortAt"));
        // Step 1: write an initial value (1234), save the fused state.
        sf.insert(7, 1234);
        sf.merge_all();
        let saved_layers: Vec<RoaringBitmap> = sf
            .layer_bases_fused()
            .into_iter()
            .map(|cow| cow.into_owned())
            .collect();
        // Step 2: unload — base goes empty, is_loaded=false. New ops land in
        // the diff layer.
        sf.clear_bases_and_unload();
        for layer in &sf.bit_layers {
            assert!(!layer.is_loaded(), "layers must be unloaded before reload");
        }
        // Apply a delta while unloaded: change slot 7's value to 5678.
        // sort_set / sort_clear would normally route through write_coalescer,
        // but the path under test is the per-bit diff merge, so update
        // VersionedBitmap diffs directly to mirror that contract.
        let target: u32 = 5678;
        for bit in 0..sf.num_bits {
            if (target >> bit) & 1 == 1 {
                sf.bit_layers[bit].insert(7);
            } else {
                sf.bit_layers[bit].remove(7);
            }
        }
        // Step 3: lazy load arrives. The disk-saved layers reflect the OLD
        // value (1234). load_layers must replace base with disk while
        // preserving the diff that holds the new value (5678).
        sf.load_layers(saved_layers);
        for layer in &sf.bit_layers {
            assert!(layer.is_loaded(), "layers must be marked loaded after reload");
        }
        // Pre-merge: fused_contains must already see the diff applied on top
        // of the disk-loaded base, so reconstruct returns the new value.
        assert_eq!(
            sf.reconstruct_value(7),
            5678,
            "fused_contains must reflect both the loaded base AND the preserved \
             diff. If this returns 1234, load_layers wiped the diff and we \
             regressed to the silent-drop behavior that drove cross-pod drift."
        );
        // Merge the preserved diff into the loaded base — final state must
        // still equal the new value with no leakage from the old.
        sf.merge_all();
        assert_eq!(
            sf.reconstruct_value(7),
            5678,
            "after merge, the slot's reconstructed value must equal the latest \
             write. If higher than 5678, prior bits leaked through."
        );
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

    /// Microbenchmark: slots_in_range vs full-rebuild approach.
    ///
    /// Simulates realistic scale: ~1M slots with sortAt values spread across 86400 seconds,
    /// a universe of ~700K slots (simulating a 24h alive bucket), and a target window of
    /// 300 seconds (~0.35% of the total range).
    ///
    /// Run with: cargo test --lib sort::tests::bench_slots_in_range_vs_rebuild -- --nocapture
    #[test]
    fn bench_slots_in_range_vs_rebuild() {
        use std::time::Instant;

        let total_slots: u32 = 1_000_000;
        let universe_size: u32 = 700_000;
        let max_sort_value: u32 = 86_400; // seconds in a day
        let window_start: u32 = 43_000;
        let window_end: u32 = 43_300; // 300-second window

        // Build sort field with ~1M slots, values spread across 0..86400
        let mut sf = SortField::new(make_config("sortAt"));
        for slot in 0..total_slots {
            // Spread values deterministically across the range
            let value = (slot as u64 * max_sort_value as u64 / total_slots as u64) as u32;
            sf.insert(slot, value);
        }
        sf.merge_all();

        // Build universe bitmap (~700K of the 1M slots)
        let mut universe = RoaringBitmap::new();
        for slot in 0..universe_size {
            universe.insert(slot);
        }

        // Approach 1: slots_in_range (targeted scan of universe)
        let start = Instant::now();
        let range_result = sf.slots_in_range(&universe, window_start, window_end);
        let range_elapsed = start.elapsed();

        // Approach 2: full rebuild (iterate ALL alive slots, reconstruct values,
        // build complete bucket bitmap from scratch — what current code does)
        let start = Instant::now();
        let mut rebuild_result = RoaringBitmap::new();
        for slot in universe.iter() {
            let val = sf.reconstruct_value(slot);
            // In a full rebuild you'd bucket ALL values, not just this window.
            // Simulate by checking the full range (every slot gets reconstructed).
            if val >= window_start && val < window_end {
                rebuild_result.insert(slot);
            }
        }
        let rebuild_elapsed = start.elapsed();

        // Both approaches should produce identical results
        assert_eq!(range_result, rebuild_result);

        // Approach 3: TRUE full rebuild — reconstruct ALL values and bucket them.
        // This is the real cost when rebuilding a complete time bucket from scratch
        // (e.g. what happens if you don't have an incremental range query).
        let all_alive = {
            let mut bm = RoaringBitmap::new();
            for slot in 0..total_slots {
                bm.insert(slot);
            }
            bm
        };
        let start = Instant::now();
        let mut full_rebuild_result = RoaringBitmap::new();
        for slot in all_alive.iter() {
            let val = sf.reconstruct_value(slot);
            if val >= window_start && val < window_end {
                full_rebuild_result.insert(slot);
            }
        }
        let full_rebuild_elapsed = start.elapsed();

        eprintln!("--- slots_in_range microbenchmark ---");
        eprintln!("  Total slots:     {total_slots}");
        eprintln!("  Universe size:   {universe_size}");
        eprintln!("  Window:          [{window_start}, {window_end}) = {} seconds", window_end - window_start);
        eprintln!("  Matches:         {}", range_result.len());
        eprintln!();
        eprintln!("  slots_in_range(universe):    {:>10?}", range_elapsed);
        eprintln!("  rebuild(universe):           {:>10?}  (same work, baseline)", rebuild_elapsed);
        eprintln!("  rebuild(ALL alive, 1M):      {:>10?}  (full bucket rebuild)", full_rebuild_elapsed);
        eprintln!();
        eprintln!("  Speedup vs full rebuild:     {:.1}x",
            full_rebuild_elapsed.as_nanos() as f64 / range_elapsed.as_nanos().max(1) as f64);
    }

    #[test]
    fn test_slots_in_range() {
        let mut sf = SortField::new(make_config("sortAt"));
        // Insert slots with various values
        sf.insert(1, 100);
        sf.insert(2, 200);
        sf.insert(3, 300);
        sf.insert(4, 400);
        sf.insert(5, 500);
        sf.insert(6, 250);
        sf.insert(7, 299);
        sf.insert(8, 301);
        sf.merge_all();

        let mut universe = RoaringBitmap::new();
        for i in 1..=8 {
            universe.insert(i);
        }

        // [200, 400) should match slots 2 (200), 3 (300), 6 (250), 7 (299), 8 (301)
        let result = sf.slots_in_range(&universe, 200, 400);
        assert_eq!(result.len(), 5);
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(result.contains(6));
        assert!(result.contains(7));
        assert!(result.contains(8));
        // Should NOT contain slots outside range
        assert!(!result.contains(1)); // 100 < 200
        assert!(!result.contains(4)); // 400 not < 400 (exclusive upper bound)
        assert!(!result.contains(5)); // 500 >= 400

        // Empty universe → empty result
        let empty = RoaringBitmap::new();
        assert!(sf.slots_in_range(&empty, 0, u32::MAX).is_empty());

        // Range matching nothing
        assert!(sf.slots_in_range(&universe, 600, 700).is_empty());

        // Single-value range [300, 301) should match only slot 3
        let single = sf.slots_in_range(&universe, 300, 301);
        assert_eq!(single.len(), 1);
        assert!(single.contains(3));

        // Partial universe — only check slots in universe
        let mut partial = RoaringBitmap::new();
        partial.insert(1);
        partial.insert(2);
        let partial_result = sf.slots_in_range(&partial, 0, 1000);
        assert_eq!(partial_result.len(), 2);
    }
}
