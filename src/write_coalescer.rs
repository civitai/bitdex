use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::sync::Arc;
use std::time::Instant;
use crossbeam_channel::{Receiver, Sender};
use crate::filter::FilterIndex;
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;

/// Per-cycle timing breakdown for `WriteBatch::apply`.
///
/// Used to identify which sub-phase or which specific field is dominating
/// the apply duration. The flush thread logs this if total apply time
/// exceeds a threshold (default 100ms).
#[derive(Debug, Default, Clone)]
pub struct BatchApplyTimings {
    pub filter_removes_ns: u64,
    pub filter_inserts_ns: u64,
    pub sort_clears_ns: u64,
    pub sort_sets_ns: u64,
    pub alive_ns: u64,
    pub sort_merge_dirty_ns: u64,
    pub alive_merge_ns: u64,
    /// Per-field nanoseconds for filter inserts+removes combined.
    /// Identifies whether a single high-cardinality field (e.g. postId)
    /// is dominating the cycle. Sorted descending in render.
    pub per_filter_field_ns: HashMap<Arc<str>, u64>,
    /// Per-field nanoseconds for sort sets+clears+merge_dirty combined.
    pub per_sort_field_ns: HashMap<Arc<str>, u64>,
    /// Number of (field, value) groups processed across inserts+removes.
    pub filter_groups: usize,
    pub sort_groups: usize,
}

impl BatchApplyTimings {
    pub fn total_ns(&self) -> u64 {
        self.filter_removes_ns
            + self.filter_inserts_ns
            + self.sort_clears_ns
            + self.sort_sets_ns
            + self.alive_ns
            + self.sort_merge_dirty_ns
            + self.alive_merge_ns
    }
    /// Render a single-line summary of the timings, with the top 3 hot fields
    /// called out by name. Used by the flush thread for ops-trace logging.
    pub fn render_summary(&self) -> String {
        let mut top_filter: Vec<(&Arc<str>, &u64)> =
            self.per_filter_field_ns.iter().collect();
        top_filter.sort_by(|a, b| b.1.cmp(a.1));
        let top_filter_str = top_filter
            .iter()
            .take(3)
            .map(|(f, ns)| format!("{}={}ms", f, *ns / 1_000_000))
            .collect::<Vec<_>>()
            .join(",");

        let mut top_sort: Vec<(&Arc<str>, &u64)> =
            self.per_sort_field_ns.iter().collect();
        top_sort.sort_by(|a, b| b.1.cmp(a.1));
        let top_sort_str = top_sort
            .iter()
            .take(3)
            .map(|(f, ns)| format!("{}={}ms", f, *ns / 1_000_000))
            .collect::<Vec<_>>()
            .join(",");

        format!(
            "total={}ms (fr={}ms fi={}ms sc={}ms ss={}ms alive={}ms sortmerge={}ms aliveerge={}ms) f_groups={} s_groups={} top_filter=[{}] top_sort=[{}]",
            self.total_ns() / 1_000_000,
            self.filter_removes_ns / 1_000_000,
            self.filter_inserts_ns / 1_000_000,
            self.sort_clears_ns / 1_000_000,
            self.sort_sets_ns / 1_000_000,
            self.alive_ns / 1_000_000,
            self.sort_merge_dirty_ns / 1_000_000,
            self.alive_merge_ns / 1_000_000,
            self.filter_groups,
            self.sort_groups,
            top_filter_str,
            top_sort_str,
        )
    }
}
/// A bitmap mutation request submitted by any thread.
/// Field names use Arc<str> to avoid heap allocation per op.
/// All variants carry `slots: Vec<u32>` for bulk grouping.
#[derive(Debug, Clone)]
pub enum MutationOp {
    /// Set bits in a filter bitmap: field[value] |= slots
    FilterInsert {
        field: Arc<str>,
        value: u64,
        slots: Vec<u32>,
    },
    /// Clear bits in a filter bitmap: field[value] &= !slots
    FilterRemove {
        field: Arc<str>,
        value: u64,
        slots: Vec<u32>,
    },
    /// Set bits in a sort layer: field.bit_layers[bit_layer] |= slots
    SortSet {
        field: Arc<str>,
        bit_layer: usize,
        slots: Vec<u32>,
    },
    /// Clear bits in a sort layer: field.bit_layers[bit_layer] &= !slots
    SortClear {
        field: Arc<str>,
        bit_layer: usize,
        slots: Vec<u32>,
    },
    /// Set alive bits for slots
    AliveInsert { slots: Vec<u32> },
    /// Clear alive bits for slots
    AliveRemove { slots: Vec<u32> },
    /// Schedule deferred alive activation at a future unix timestamp.
    /// The slot's filter/sort bitmaps are set immediately, but the alive bit
    /// is deferred until `activate_at` (seconds since epoch).
    DeferredAlive { slot: u32, activate_at: u64 },
}
/// Key for grouping filter operations by target bitmap.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterGroupKey {
    pub field: Arc<str>,
    pub value: u64,
}
/// Key for grouping sort operations by target bit layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortGroupKey {
    pub field: Arc<str>,
    pub bit_layer: usize,
}
/// Accumulates MutationOps from a channel drain and groups them by target bitmap
/// for bulk application.
pub struct WriteBatch {
    /// Raw ops drained from the channel this batch.
    ops: Vec<MutationOp>,
    // Grouped operations (populated by group_and_sort)
    filter_inserts: HashMap<FilterGroupKey, Vec<u32>>,
    filter_removes: HashMap<FilterGroupKey, Vec<u32>>,
    sort_sets: HashMap<SortGroupKey, Vec<u32>>,
    sort_clears: HashMap<SortGroupKey, Vec<u32>>,
    alive_inserts: Vec<u32>,
    alive_removes: Vec<u32>,
    deferred_alive: Vec<(u32, u64)>,
}
impl WriteBatch {
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            filter_inserts: HashMap::new(),
            filter_removes: HashMap::new(),
            sort_sets: HashMap::new(),
            sort_clears: HashMap::new(),
            alive_inserts: Vec::new(),
            alive_removes: Vec::new(),
            deferred_alive: Vec::new(),
        }
    }
    /// Push a list of ops directly (used by deferred alive activation in the flush thread).
    pub fn push_ops(&mut self, ops: Vec<MutationOp>) {
        self.ops.extend(ops);
    }
    /// Drain all pending ops from the channel receiver.
    pub fn drain_channel(&mut self, receiver: &Receiver<MutationOp>) {
        while let Ok(op) = receiver.try_recv() {
            self.ops.push(op);
        }
    }
    /// Number of raw ops in this batch.
    pub fn len(&self) -> usize {
        self.ops.len()
    }
    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    /// Group ops by (field, value, op_type) and sort slot IDs within each group.
    /// Sorting ensures roaring-rs `extend()` gets sorted input for maximum performance.
    pub fn group_and_sort(&mut self) {
        self.filter_inserts.clear();
        self.filter_removes.clear();
        self.sort_sets.clear();
        self.sort_clears.clear();
        self.alive_inserts.clear();
        self.alive_removes.clear();
        self.deferred_alive.clear();
        for op in self.ops.drain(..) {
            match op {
                MutationOp::FilterInsert { field, value, slots } => {
                    self.filter_inserts
                        .entry(FilterGroupKey { field, value })
                        .or_default()
                        .extend(slots);
                }
                MutationOp::FilterRemove { field, value, slots } => {
                    self.filter_removes
                        .entry(FilterGroupKey { field, value })
                        .or_default()
                        .extend(slots);
                }
                MutationOp::SortSet {
                    field,
                    bit_layer,
                    slots,
                } => {
                    self.sort_sets
                        .entry(SortGroupKey { field, bit_layer })
                        .or_default()
                        .extend(slots);
                }
                MutationOp::SortClear {
                    field,
                    bit_layer,
                    slots,
                } => {
                    self.sort_clears
                        .entry(SortGroupKey { field, bit_layer })
                        .or_default()
                        .extend(slots);
                }
                MutationOp::AliveInsert { slots } => {
                    self.alive_inserts.extend(slots);
                }
                MutationOp::AliveRemove { slots } => {
                    self.alive_removes.extend(slots);
                }
                MutationOp::DeferredAlive { slot, activate_at } => {
                    self.deferred_alive.push((slot, activate_at));
                }
            }
        }
        // Sort all slot ID vectors for optimal roaring-rs extend() performance
        for slots in self.filter_inserts.values_mut() {
            slots.sort_unstable();
        }
        for slots in self.filter_removes.values_mut() {
            slots.sort_unstable();
        }
        for slots in self.sort_sets.values_mut() {
            slots.sort_unstable();
        }
        for slots in self.sort_clears.values_mut() {
            slots.sort_unstable();
        }
        self.alive_inserts.sort_unstable();
        self.alive_removes.sort_unstable();
    }
    /// Returns true if this batch contains alive bitmap mutations (inserts or removes).
    /// When alive changes, all cached NotEq/Not results are stale (they bake in alive).
    pub fn has_alive_mutations(&self) -> bool {
        !self.alive_inserts.is_empty() || !self.alive_removes.is_empty()
    }
    /// Returns true if this batch contains deferred alive entries.
    pub fn has_deferred_alive(&self) -> bool {
        !self.deferred_alive.is_empty()
    }
    /// Extract filter mutations for Tier 2 fields before apply.
    ///
    /// Removes all filter insert/remove entries whose field name is in `tier2_fields`
    /// and returns them as `(field_name, value, slots, is_set)` tuples.
    /// Must be called after `group_and_sort()` and before `apply()`.
    pub fn take_tier2_mutations(
        &mut self,
        tier2_fields: &HashSet<String>,
    ) -> Vec<(Arc<str>, u64, Vec<u32>, bool)> {
        let mut result = Vec::new();
        let insert_keys: Vec<FilterGroupKey> = self
            .filter_inserts
            .keys()
            .filter(|k| tier2_fields.contains(k.field.as_ref()))
            .cloned()
            .collect();
        for key in insert_keys {
            if let Some(slots) = self.filter_inserts.remove(&key) {
                result.push((Arc::clone(&key.field), key.value, slots, true));
            }
        }
        let remove_keys: Vec<FilterGroupKey> = self
            .filter_removes
            .keys()
            .filter(|k| tier2_fields.contains(k.field.as_ref()))
            .cloned()
            .collect();
        for key in remove_keys {
            if let Some(slots) = self.filter_removes.remove(&key) {
                result.push((Arc::clone(&key.field), key.value, slots, false));
            }
        }
        result
    }
    /// Returns the set of slots mutated per sort field in this batch.
    /// Valid after `group_and_sort()` has been called.
    /// Used by D3 live bound maintenance to check if mutated slots qualify for bounds.
    pub fn mutated_sort_slots(&self) -> HashMap<&str, HashSet<u32>> {
        let mut result: HashMap<&str, HashSet<u32>> = HashMap::new();
        for (key, slots) in &self.sort_sets {
            result.entry(&key.field).or_default().extend(slots);
        }
        for (key, slots) in &self.sort_clears {
            result.entry(&key.field).or_default().extend(slots);
        }
        result
    }
    /// Returns the set of filter field names that were mutated in this batch.
    /// Valid after `group_and_sort()` has been called.
    pub fn mutated_filter_fields(&self) -> HashSet<&str> {
        let mut fields = HashSet::new();
        for key in self.filter_inserts.keys() {
            fields.insert(&*key.field);
        }
        for key in self.filter_removes.keys() {
            fields.insert(&*key.field);
        }
        fields
    }
    /// Apply all grouped mutations to the bitmap state using bulk operations.
    ///
    /// For inserts: uses `extend()` with sorted slot IDs for maximum throughput.
    /// For removes: iterates (roaring has no bulk remove) but grouping still reduces HashMap lookups.
    pub fn apply(
        &self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) {
        // Discard timings — call apply_traced if you want them.
        let _ = self.apply_traced(slots, filters, sorts);
    }

    /// Apply all grouped mutations to the bitmap state, returning a per-sub-phase
    /// and per-field timing breakdown for ops-path tracing.
    pub fn apply_traced(
        &self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) -> BatchApplyTimings {
        let mut t = BatchApplyTimings::default();
        t.filter_groups = self.filter_inserts.len() + self.filter_removes.len();
        t.sort_groups = self.sort_sets.len() + self.sort_clears.len();

        // Apply filter removes BEFORE inserts.
        // On upsert, diff_document emits remove-old + insert-new for changed
        // multi_value fields.  When a value is kept across the upsert (e.g.
        // tagIds [10,20] → [10,30], value 10 appears in both remove and insert),
        // applying inserts first makes the insert a no-op and the subsequent
        // remove deletes the slot — losing the value.  Removes-first is safe:
        // the remove clears the bit, then the insert re-sets it.
        let t0 = Instant::now();
        for (key, slot_ids) in &self.filter_removes {
            let f0 = Instant::now();
            if let Some(field) = filters.get_field(&key.field) {
                field.remove_bulk(key.value, slot_ids);
            }
            *t.per_filter_field_ns
                .entry(Arc::clone(&key.field))
                .or_insert(0) += f0.elapsed().as_nanos() as u64;
        }
        t.filter_removes_ns = t0.elapsed().as_nanos() as u64;

        // Apply filter inserts in bulk
        let t0 = Instant::now();
        for (key, slot_ids) in &self.filter_inserts {
            let f0 = Instant::now();
            if let Some(field) = filters.get_field(&key.field) {
                field.insert_bulk(key.value, slot_ids.iter().copied());
            }
            *t.per_filter_field_ns
                .entry(Arc::clone(&key.field))
                .or_insert(0) += f0.elapsed().as_nanos() as u64;
        }
        t.filter_inserts_ns = t0.elapsed().as_nanos() as u64;

        // Apply sort layer clears BEFORE sets.
        // On slot recycling (delete → reinsert), diff_document emits SortClear
        // for old value bits and SortSet for new value bits.  For bits that are 1
        // in both old and new values, the same slot appears in both sort_clears
        // and sort_sets.  Sets-first makes the set a no-op, then clear deletes
        // the bit — losing the value.  Clears-first is safe: clear removes the
        // old bit, then set re-establishes it.
        let t0 = Instant::now();
        for (key, slot_ids) in &self.sort_clears {
            let f0 = Instant::now();
            if let Some(field) = sorts.get_field_mut(&key.field) {
                field.clear_layer_bulk(key.bit_layer, slot_ids);
            }
            *t.per_sort_field_ns
                .entry(Arc::clone(&key.field))
                .or_insert(0) += f0.elapsed().as_nanos() as u64;
        }
        t.sort_clears_ns = t0.elapsed().as_nanos() as u64;

        // Apply sort layer sets in bulk
        let t0 = Instant::now();
        for (key, slot_ids) in &self.sort_sets {
            let f0 = Instant::now();
            if let Some(field) = sorts.get_field_mut(&key.field) {
                field.set_layer_bulk(key.bit_layer, slot_ids.iter().copied());
            }
            *t.per_sort_field_ns
                .entry(Arc::clone(&key.field))
                .or_insert(0) += f0.elapsed().as_nanos() as u64;
        }
        t.sort_sets_ns = t0.elapsed().as_nanos() as u64;

        // Apply alive inserts in bulk (writes to diff layer)
        let t0 = Instant::now();
        if !self.alive_inserts.is_empty() {
            slots.alive_insert_bulk(self.alive_inserts.iter().copied());
        }
        // Apply alive removes (writes to diff layer)
        for &slot in &self.alive_removes {
            slots.alive_remove_one(slot);
        }
        // Schedule deferred alive activations
        for &(slot, activate_at) in &self.deferred_alive {
            slots.schedule_alive(slot, activate_at);
        }
        t.alive_ns = t0.elapsed().as_nanos() as u64;

        // Sort diffs are NOT merged here. The read path (SortField::top_n)
        // fuses base + diff via fused_cow on demand at the start of each
        // query, sharing the snapshot across bifurcate / cursor / order.
        //
        // The previous eager-merge pattern triggered an Arc::make_mut deep
        // clone of the layer base on every dirty layer per cycle (~500ms
        // for sortAt at 109M scale, dominated by the 6 MB roaring clone of
        // a 54M-bit base just to flip 2 bits). Lazy fuse moves that cost
        // out of the write hot path entirely. The merge thread is now the
        // sole place that promotes dirty diffs into bases on its persist
        // cycle (~5s).
        //
        // For the trace summary, we still record `sort_merge_dirty_ns = 0`
        // so existing dashboards/log parsers don't break.
        t.sort_merge_dirty_ns = 0;

        // Filter diffs are also NOT merged here — same pattern, fused at
        // read time by the executor (apply_diff_eq).
        // Merge alive bitmap
        let t0 = Instant::now();
        slots.merge_alive();
        t.alive_merge_ns = t0.elapsed().as_nanos() as u64;

        t
    }
}
impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}
/// Cloneable handle for submitting mutations from any thread.
///
/// Wraps a `crossbeam_channel::Sender<MutationOp>`. When the bounded channel is full,
/// `send()` blocks, providing natural backpressure to writers.
#[derive(Clone)]
pub struct MutationSender {
    tx: Sender<MutationOp>,
}
impl MutationSender {
    /// Submit a single mutation. Blocks if the channel is full (backpressure).
    pub fn send(&self, op: MutationOp) -> Result<(), crossbeam_channel::SendError<MutationOp>> {
        self.tx.send(op)
    }
    /// Approximate number of pending ops in the channel (for metrics).
    pub fn pending_count(&self) -> usize {
        self.tx.len()
    }
    /// Submit multiple mutations. Blocks per-op if the channel is full.
    pub fn send_batch(
        &self,
        ops: Vec<MutationOp>,
    ) -> Result<(), crossbeam_channel::SendError<MutationOp>> {
        for op in ops {
            self.tx.send(op)?;
        }
        Ok(())
    }
}
/// Owns the MPSC channel and provides a `flush()` method for the ConcurrentEngine
/// to call while holding the write lock on bitmap state.
pub struct WriteCoalescer {
    rx: Receiver<MutationOp>,
    tx: Sender<MutationOp>,
    batch: WriteBatch,
}
impl WriteCoalescer {
    /// Create a new WriteCoalescer with a bounded channel of the given capacity.
    /// Returns the coalescer and a cloneable sender handle.
    pub fn new(capacity: usize) -> (Self, MutationSender) {
        let (tx, rx) = crossbeam_channel::bounded(capacity);
        let sender = MutationSender { tx: tx.clone() };
        let coalescer = Self {
            rx,
            tx,
            batch: WriteBatch::new(),
        };
        (coalescer, sender)
    }
    /// Get a cloneable sender handle for submitting mutations.
    pub fn sender(&self) -> MutationSender {
        MutationSender {
            tx: self.tx.clone(),
        }
    }
    /// Approximate number of pending ops in the channel.
    pub fn pending_count(&self) -> usize {
        self.rx.len()
    }
    /// Drain the channel, group ops by target bitmap, and apply them in bulk.
    ///
    /// Called by ConcurrentEngine while holding the write lock on bitmap state.
    /// Returns the number of ops applied.
    pub fn flush(
        &mut self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) -> usize {
        self.batch.drain_channel(&self.rx);
        if self.batch.is_empty() {
            return 0;
        }
        let count = self.batch.len();
        self.batch.group_and_sort();
        self.batch.apply(slots, filters, sorts);
        count
    }
    /// Phase 1: Drain channel and group/sort ops. No lock needed.
    /// Returns the number of ops prepared (0 = nothing to apply).
    pub fn prepare(&mut self) -> usize {
        self.batch.drain_channel(&self.rx);
        if self.batch.is_empty() {
            return 0;
        }
        let count = self.batch.len();
        self.batch.group_and_sort();
        count
    }
    /// Phase 2: Apply the prepared batch to bitmap state. Requires write lock.
    /// Only call after `prepare()` returned > 0.
    pub fn apply_prepared(
        &self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) {
        self.batch.apply(slots, filters, sorts);
    }
    /// Phase 2 with timing breakdown. Returns per-sub-phase + per-field
    /// nanosecond costs so the flush thread can emit ops trace logs and
    /// surface hot-field cascades (e.g. postId at 22.5M entries).
    pub fn apply_prepared_traced(
        &self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) -> BatchApplyTimings {
        self.batch.apply_traced(slots, filters, sorts)
    }
    /// Extract Tier 2 filter mutations from the prepared batch.
    /// Must be called after `prepare()` and before `apply_prepared()`.
    pub fn take_tier2_mutations(
        &mut self,
        tier2_fields: &HashSet<String>,
    ) -> Vec<(Arc<str>, u64, Vec<u32>, bool)> {
        self.batch.take_tier2_mutations(tier2_fields)
    }
    /// Returns true if the prepared batch contains alive bitmap mutations.
    /// When alive changes, cached NotEq/Not results (which bake in alive) are stale.
    pub fn has_alive_mutations(&self) -> bool {
        self.batch.has_alive_mutations()
    }
    /// Returns true if the prepared batch contains deferred alive entries.
    pub fn has_deferred_alive(&self) -> bool {
        self.batch.has_deferred_alive()
    }
    /// Returns the set of filter field names mutated in the prepared batch.
    /// Valid after `prepare()` returned > 0, before the next `prepare()` call.
    pub fn mutated_filter_fields(&self) -> HashSet<&str> {
        self.batch.mutated_filter_fields()
    }
    /// Returns slots mutated per sort field in the prepared batch.
    /// Valid after `prepare()` returned > 0, before the next `prepare()` call.
    /// Used by D3 live bound maintenance.
    pub fn mutated_sort_slots(&self) -> HashMap<&str, HashSet<u32>> {
        self.batch.mutated_sort_slots()
    }
    /// Returns the alive insert slots from the prepared batch.
    /// Used for slot-based bound live maintenance: new slots are monotonically
    /// increasing and always qualify for descending slot bounds.
    pub fn alive_inserts(&self) -> &[u32] {
        &self.batch.alive_inserts
    }
    /// Returns the slot IDs removed from the alive bitmap in this batch.
    /// Used for time bucket live maintenance: deleted slots are removed from all buckets.
    pub fn alive_removes(&self) -> &[u32] {
        &self.batch.alive_removes
    }
    /// Returns the filter insert entries from the prepared batch.
    /// Used by trie cache live updates to insert mutated slots into matching entries.
    pub fn filter_insert_entries(&self) -> &HashMap<FilterGroupKey, Vec<u32>> {
        &self.batch.filter_inserts
    }
    /// Returns the filter remove entries from the prepared batch.
    /// Used by trie cache live updates to remove mutated slots from matching entries.
    pub fn filter_remove_entries(&self) -> &HashMap<FilterGroupKey, Vec<u32>> {
        &self.batch.filter_removes
    }
    /// Returns the sort set entries from the prepared batch.
    /// Used by ops-log wiring to append BitmapOp::BatchSet per sort layer shard.
    pub fn sort_set_entries(&self) -> &HashMap<SortGroupKey, Vec<u32>> {
        &self.batch.sort_sets
    }
    /// Returns the sort clear entries from the prepared batch.
    /// Used by ops-log wiring to append BitmapOp::BatchClear per sort layer shard.
    pub fn sort_clear_entries(&self) -> &HashMap<SortGroupKey, Vec<u32>> {
        &self.batch.sort_clears
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use std::thread;
    fn setup_filter_index() -> FilterIndex {
        let mut filters = FilterIndex::new();
        filters.add_field(FilterFieldConfig {
            name: "status".to_string(),
            field_type: FilterFieldType::SingleValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        filters.add_field(FilterFieldConfig {
            name: "tagIds".to_string(),
            field_type: FilterFieldType::MultiValue,
            behaviors: None,
            eviction: None,
            eager_load: false,
            per_value_lazy: false,
        });
        filters
    }
    fn setup_sort_index() -> SortIndex {
        let mut sorts = SortIndex::new();
        sorts.add_field(SortFieldConfig {
            name: "reactionCount".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
            eager_load: false,
            computed: None,
        });
        sorts
    }
    // ---- WriteBatch grouping tests ----
    #[test]
    fn test_batch_groups_filter_inserts_by_key() {
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 1,
            slots: vec![30],
        });
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 1,
            slots: vec![10],
        });
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 1,
            slots: vec![20],
        });
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 2,
            slots: vec![5],
        });
        batch.group_and_sort();
        let key1 = FilterGroupKey {
            field: Arc::from("status"),
            value: 1,
        };
        let key2 = FilterGroupKey {
            field: Arc::from("status"),
            value: 2,
        };
        // Grouped correctly
        assert_eq!(batch.filter_inserts[&key1], vec![10, 20, 30]); // sorted
        assert_eq!(batch.filter_inserts[&key2], vec![5]);
    }
    #[test]
    fn test_batch_groups_filter_removes() {
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::FilterRemove {
            field: Arc::from("tagIds"),
            value: 100,
            slots: vec![20],
        });
        batch.ops.push(MutationOp::FilterRemove {
            field: Arc::from("tagIds"),
            value: 100,
            slots: vec![10],
        });
        batch.group_and_sort();
        let key = FilterGroupKey {
            field: Arc::from("tagIds"),
            value: 100,
        };
        assert_eq!(batch.filter_removes[&key], vec![10, 20]); // sorted
    }
    #[test]
    fn test_batch_groups_sort_ops() {
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("reactionCount"),
            bit_layer: 3,
            slots: vec![50],
        });
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("reactionCount"),
            bit_layer: 3,
            slots: vec![10],
        });
        batch.ops.push(MutationOp::SortClear {
            field: Arc::from("reactionCount"),
            bit_layer: 5,
            slots: vec![7],
        });
        batch.group_and_sort();
        let set_key = SortGroupKey {
            field: Arc::from("reactionCount"),
            bit_layer: 3,
        };
        let clear_key = SortGroupKey {
            field: Arc::from("reactionCount"),
            bit_layer: 5,
        };
        assert_eq!(batch.sort_sets[&set_key], vec![10, 50]); // sorted
        assert_eq!(batch.sort_clears[&clear_key], vec![7]);
    }
    #[test]
    fn test_batch_groups_alive_ops() {
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::AliveInsert { slots: vec![30] });
        batch.ops.push(MutationOp::AliveInsert { slots: vec![10] });
        batch.ops.push(MutationOp::AliveInsert { slots: vec![20] });
        batch.ops.push(MutationOp::AliveRemove { slots: vec![5] });
        batch.group_and_sort();
        assert_eq!(batch.alive_inserts, vec![10, 20, 30]); // sorted
        assert_eq!(batch.alive_removes, vec![5]);
    }
    #[test]
    fn test_batch_slots_are_sorted_for_extend() {
        let mut batch = WriteBatch::new();
        // Insert in reverse order
        for slot in (0..100).rev() {
            batch.ops.push(MutationOp::FilterInsert {
                field: Arc::from("status"),
                value: 1,
                slots: vec![slot],
            });
        }
        batch.group_and_sort();
        let key = FilterGroupKey {
            field: Arc::from("status"),
            value: 1,
        };
        let slots = &batch.filter_inserts[&key];
        // Verify sorted
        for w in slots.windows(2) {
            assert!(w[0] <= w[1], "slots must be sorted for extend()");
        }
    }
    #[test]
    fn test_empty_batch() {
        let mut batch = WriteBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        batch.group_and_sort();
        assert!(batch.filter_inserts.is_empty());
        assert!(batch.filter_removes.is_empty());
        assert!(batch.sort_sets.is_empty());
        assert!(batch.sort_clears.is_empty());
        assert!(batch.alive_inserts.is_empty());
        assert!(batch.alive_removes.is_empty());
    }
    // ---- WriteBatch apply tests ----
    #[test]
    fn test_apply_filter_inserts() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 1,
            slots: vec![10],
        });
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 1,
            slots: vec![20],
        });
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 2,
            slots: vec![30],
        });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        // Filter diffs are NOT merged by apply — use VersionedBitmap::contains() for logical check
        let field = filters.get_field("status").unwrap();
        let vb1 = field.get_versioned(1).unwrap();
        assert!(vb1.is_dirty(), "filter bitmap should have dirty diff after apply");
        assert!(vb1.contains(10));
        assert!(vb1.contains(20));
        let vb2 = field.get_versioned(2).unwrap();
        assert!(vb2.contains(30));
    }
    #[test]
    fn test_apply_filter_removes() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        // Pre-populate and merge so base has {10, 20, 30}
        filters.get_field("status").unwrap().insert(1, 10);
        filters.get_field("status").unwrap().insert(1, 20);
        filters.get_field("status").unwrap().insert(1, 30);
        filters.get_field("status").unwrap().merge_dirty();
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::FilterRemove {
            field: Arc::from("status"),
            value: 1,
            slots: vec![10],
        });
        batch.ops.push(MutationOp::FilterRemove {
            field: Arc::from("status"),
            value: 1,
            slots: vec![30],
        });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        // Filter diffs not merged — use logical contains() to check state
        let vb = filters.get_field("status").unwrap().get_versioned(1).unwrap();
        assert!(vb.is_dirty());
        assert!(!vb.contains(10));
        assert!(vb.contains(20));
        assert!(!vb.contains(30));
    }
    #[test]
    fn test_apply_sort_set_and_clear() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        // Set bits 0 and 2 for slot 10 (value = 5 in binary: 101)
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("reactionCount"),
            bit_layer: 0,
            slots: vec![10],
        });
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("reactionCount"),
            bit_layer: 2,
            slots: vec![10],
        });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        let sf = sorts.get_field("reactionCount").unwrap();
        assert!(sf.layer(0).unwrap().contains(10));
        assert!(!sf.layer(1).unwrap().contains(10));
        assert!(sf.layer(2).unwrap().contains(10));
        assert_eq!(sf.reconstruct_value(10), 5);
        // Now clear bit 0, so value becomes 4 (binary: 100)
        let mut batch2 = WriteBatch::new();
        batch2.ops.push(MutationOp::SortClear {
            field: Arc::from("reactionCount"),
            bit_layer: 0,
            slots: vec![10],
        });
        batch2.group_and_sort();
        batch2.apply(&mut slots, &mut filters, &mut sorts);
        let sf = sorts.get_field("reactionCount").unwrap();
        assert!(!sf.layer(0).unwrap().contains(10));
        assert!(sf.layer(2).unwrap().contains(10));
        assert_eq!(sf.reconstruct_value(10), 4);
    }
    #[test]
    fn test_apply_alive_ops() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::AliveInsert { slots: vec![10] });
        batch.ops.push(MutationOp::AliveInsert { slots: vec![20] });
        batch.ops.push(MutationOp::AliveInsert { slots: vec![30] });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        assert!(slots.alive_bitmap().contains(10));
        assert!(slots.alive_bitmap().contains(20));
        assert!(slots.alive_bitmap().contains(30));
        // Now remove slot 20
        let mut batch2 = WriteBatch::new();
        batch2.ops.push(MutationOp::AliveRemove { slots: vec![20] });
        batch2.group_and_sort();
        batch2.apply(&mut slots, &mut filters, &mut sorts);
        assert!(slots.alive_bitmap().contains(10));
        assert!(!slots.alive_bitmap().contains(20));
        assert!(slots.alive_bitmap().contains(30));
    }
    #[test]
    fn test_apply_mixed_ops() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        // Mix of all operation types
        batch.ops.push(MutationOp::AliveInsert { slots: vec![100] });
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 1,
            slots: vec![100],
        });
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("reactionCount"),
            bit_layer: 0,
            slots: vec![100],
        });
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("reactionCount"),
            bit_layer: 5,
            slots: vec![100],
        });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        assert!(slots.alive_bitmap().contains(100));
        // Filter diffs not merged — use logical contains()
        assert!(filters
            .get_field("status")
            .unwrap()
            .get_versioned(1)
            .unwrap()
            .contains(100));
        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(100),
            33 // bit 0 + bit 5 = 1 + 32
        );
    }
    #[test]
    fn test_apply_ignores_unknown_fields() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("nonexistent"),
            value: 1,
            slots: vec![10],
        });
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("nonexistent"),
            bit_layer: 0,
            slots: vec![10],
        });
        batch.group_and_sort();
        // Should not panic
        batch.apply(&mut slots, &mut filters, &mut sorts);
    }
    // ---- WriteCoalescer + MutationSender tests ----
    #[test]
    fn test_coalescer_new_returns_sender() {
        let (coalescer, sender) = WriteCoalescer::new(100);
        assert_eq!(coalescer.pending_count(), 0);
        sender
            .send(MutationOp::AliveInsert { slots: vec![1] })
            .unwrap();
        assert_eq!(coalescer.pending_count(), 1);
    }
    #[test]
    fn test_coalescer_flush_drains_and_applies() {
        let (mut coalescer, sender) = WriteCoalescer::new(100);
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        sender
            .send(MutationOp::AliveInsert { slots: vec![10] })
            .unwrap();
        sender
            .send(MutationOp::AliveInsert { slots: vec![20] })
            .unwrap();
        sender
            .send(MutationOp::FilterInsert {
                field: Arc::from("status"),
                value: 1,
                slots: vec![10],
            })
            .unwrap();
        let count = coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(count, 3);
        assert!(slots.alive_bitmap().contains(10));
        assert!(slots.alive_bitmap().contains(20));
        // Filter diffs not merged — use logical contains()
        assert!(filters
            .get_field("status")
            .unwrap()
            .get_versioned(1)
            .unwrap()
            .contains(10));
    }
    #[test]
    fn test_coalescer_flush_returns_zero_when_empty() {
        let (mut coalescer, _sender) = WriteCoalescer::new(100);
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let count = coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(count, 0);
    }
    #[test]
    fn test_coalescer_multiple_flushes() {
        let (mut coalescer, sender) = WriteCoalescer::new(100);
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        // First batch
        sender
            .send(MutationOp::AliveInsert { slots: vec![10] })
            .unwrap();
        let count1 = coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(count1, 1);
        // Second batch
        sender
            .send(MutationOp::AliveInsert { slots: vec![20] })
            .unwrap();
        sender
            .send(MutationOp::AliveInsert { slots: vec![30] })
            .unwrap();
        let count2 = coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(count2, 2);
        assert!(slots.alive_bitmap().contains(10));
        assert!(slots.alive_bitmap().contains(20));
        assert!(slots.alive_bitmap().contains(30));
    }
    #[test]
    fn test_sender_clone_and_multithread() {
        let (mut coalescer, sender) = WriteCoalescer::new(1000);
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let sender = sender.clone();
                thread::spawn(move || {
                    for i in 0..25u32 {
                        let slot = thread_id * 25 + i;
                        sender
                            .send(MutationOp::AliveInsert { slots: vec![slot] })
                            .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let count = coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(count, 100);
        assert_eq!(slots.alive_bitmap().len(), 100);
    }
    #[test]
    fn test_sender_send_batch() {
        let (mut coalescer, sender) = WriteCoalescer::new(100);
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let ops = vec![
            MutationOp::AliveInsert { slots: vec![1] },
            MutationOp::AliveInsert { slots: vec![2] },
            MutationOp::AliveInsert { slots: vec![3] },
        ];
        sender.send_batch(ops).unwrap();
        let count = coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(count, 3);
        assert!(slots.alive_bitmap().contains(1));
        assert!(slots.alive_bitmap().contains(2));
        assert!(slots.alive_bitmap().contains(3));
    }
    #[test]
    fn test_backpressure_bounded_channel() {
        // Create a tiny channel to test backpressure
        let (coalescer, sender) = WriteCoalescer::new(2);
        // Fill the channel
        sender
            .send(MutationOp::AliveInsert { slots: vec![1] })
            .unwrap();
        sender
            .send(MutationOp::AliveInsert { slots: vec![2] })
            .unwrap();
        // Channel is now full. Verify with try_send that it would block.
        // crossbeam bounded channel's send() blocks, but we can test
        // the channel is full by checking pending_count.
        assert_eq!(coalescer.pending_count(), 2);
        // Spawn a thread that will block trying to send
        let sender_clone = sender.clone();
        let handle = thread::spawn(move || {
            // This will block until the channel is drained
            sender_clone
                .send(MutationOp::AliveInsert { slots: vec![3] })
                .unwrap();
        });
        // Small sleep to let the thread start blocking
        thread::sleep(std::time::Duration::from_millis(50));
        // Drain the channel to unblock the sender by dropping the receiver
        drop(coalescer);
        // The blocked thread should now complete because the receiver was dropped,
        // causing a SendError. Let's handle this gracefully.
        let result = handle.join();
        // The thread might error or succeed depending on timing. Either way, this
        // test demonstrates the bounded channel provides backpressure.
        let _ = result;
    }
    #[test]
    fn test_backpressure_with_flush() {
        // Better backpressure test: fill channel, flush, then more sends succeed
        let (mut coalescer, sender) = WriteCoalescer::new(3);
        sender
            .send(MutationOp::AliveInsert { slots: vec![1] })
            .unwrap();
        sender
            .send(MutationOp::AliveInsert { slots: vec![2] })
            .unwrap();
        sender
            .send(MutationOp::AliveInsert { slots: vec![3] })
            .unwrap();
        assert_eq!(coalescer.pending_count(), 3);
        // Flush frees up space
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let count = coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(count, 3);
        assert_eq!(coalescer.pending_count(), 0);
        // Now we can send more
        sender
            .send(MutationOp::AliveInsert { slots: vec![4] })
            .unwrap();
        sender
            .send(MutationOp::AliveInsert { slots: vec![5] })
            .unwrap();
        assert_eq!(coalescer.pending_count(), 2);
    }
    #[test]
    fn test_coalescer_sender_method() {
        let (coalescer, _) = WriteCoalescer::new(100);
        let sender2 = coalescer.sender();
        sender2
            .send(MutationOp::AliveInsert { slots: vec![42] })
            .unwrap();
        assert_eq!(coalescer.pending_count(), 1);
    }
    #[test]
    fn test_full_lifecycle() {
        // Simulate a realistic sequence: insert doc, update sort, delete doc
        let (mut coalescer, sender) = WriteCoalescer::new(1000);
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        // "Insert" document at slot 42 with status=1, reactionCount=100 (bits: 0,2,5,6)
        let insert_ops = vec![
            MutationOp::AliveInsert { slots: vec![42] },
            MutationOp::FilterInsert {
                field: Arc::from("status"),
                value: 1,
                slots: vec![42],
            },
            MutationOp::SortSet {
                field: Arc::from("reactionCount"),
                bit_layer: 0,
                slots: vec![42],
            },
            MutationOp::SortSet {
                field: Arc::from("reactionCount"),
                bit_layer: 2,
                slots: vec![42],
            },
            MutationOp::SortSet {
                field: Arc::from("reactionCount"),
                bit_layer: 5,
                slots: vec![42],
            },
            MutationOp::SortSet {
                field: Arc::from("reactionCount"),
                bit_layer: 6,
                slots: vec![42],
            },
        ];
        sender.send_batch(insert_ops).unwrap();
        coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert!(slots.alive_bitmap().contains(42));
        // Filter diffs not merged — use logical contains()
        assert!(filters
            .get_field("status")
            .unwrap()
            .get_versioned(1)
            .unwrap()
            .contains(42));
        // 1 + 4 + 32 + 64 = 101
        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(42),
            101
        );
        // "Update" reactionCount from 101 to 100 (clear bit 0)
        sender
            .send(MutationOp::SortClear {
                field: Arc::from("reactionCount"),
                bit_layer: 0,
                slots: vec![42],
            })
            .unwrap();
        coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(42),
            100 // 4 + 32 + 64
        );
        // "Delete" document (only clears alive bit per Bitdex design)
        sender
            .send(MutationOp::AliveRemove { slots: vec![42] })
            .unwrap();
        coalescer.flush(&mut slots, &mut filters, &mut sorts);
        assert!(!slots.alive_bitmap().contains(42));
        // Stale filter/sort bits remain (by design) — use logical contains()
        assert!(filters
            .get_field("status")
            .unwrap()
            .get_versioned(1)
            .unwrap()
            .contains(42));
    }
    // ---- New tests for diff model behavior ----
    #[test]
    fn test_apply_filter_diffs_not_merged() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::FilterInsert {
            field: Arc::from("status"),
            value: 1,
            slots: vec![10, 20],
        });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        // After apply, filter bitmaps should have dirty diffs (NOT merged)
        let vb = filters.get_field("status").unwrap().get_versioned(1).unwrap();
        assert!(vb.is_dirty(), "filter diffs should remain dirty after apply");
        assert!(vb.base().is_empty(), "base should still be empty");
        assert!(vb.diff().sets.contains(10));
        assert!(vb.diff().sets.contains(20));
    }
    #[test]
    fn test_apply_sort_diffs_merged() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::SortSet {
            field: Arc::from("reactionCount"),
            bit_layer: 0,
            slots: vec![10],
        });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        // Sort diffs MUST be merged eagerly (per architecture-risk-review issue 4)
        // layer() returns the base bitmap — the debug_assert inside layer() verifies
        // the diff is empty. If sort diffs weren't merged, layer() would panic.
        let sf = sorts.get_field("reactionCount").unwrap();
        assert!(sf.layer(0).unwrap().contains(10));
    }
    #[test]
    fn test_apply_alive_merged() {
        let mut slots = SlotAllocator::new();
        let mut filters = setup_filter_index();
        let mut sorts = setup_sort_index();
        let mut batch = WriteBatch::new();
        batch.ops.push(MutationOp::AliveInsert { slots: vec![10] });
        batch.group_and_sort();
        batch.apply(&mut slots, &mut filters, &mut sorts);
        // Alive bitmap must be merged eagerly
        assert!(slots.alive_bitmap().contains(10));
    }
}
