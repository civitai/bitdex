use std::collections::{HashMap, HashSet};
use crossbeam_channel::Receiver;
use crate::engine::filter::FilterIndex;
use crate::mutation::MutationOp;
use crate::engine::slot::SlotAllocator;
use crate::engine::sort::SortIndex;
use super::FilterGroupKey;

/// Key for grouping sort operations by target bit layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SortGroupKey {
    pub field: std::sync::Arc<str>,
    pub bit_layer: usize,
}

/// Accumulates MutationOps and applies them in bulk to staging.
/// Replaces WriteCoalescer/WriteBatch after write_coalescer.rs was deleted.
pub(super) struct FlushBatch {
    pub ops: Vec<MutationOp>,
    pub filter_inserts: HashMap<FilterGroupKey, Vec<u32>>,
    pub filter_removes: HashMap<FilterGroupKey, Vec<u32>>,
    pub sort_sets: HashMap<SortGroupKey, Vec<u32>>,
    pub sort_clears: HashMap<SortGroupKey, Vec<u32>>,
    pub alive_inserts: Vec<u32>,
    pub alive_removes: Vec<u32>,
    pub deferred_alive: Vec<(u32, u64)>,
}

impl FlushBatch {
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

    pub fn push_ops(&mut self, ops: Vec<MutationOp>) {
        self.ops.extend(ops);
    }

    pub fn drain_channel(&mut self, rx: &Receiver<MutationOp>) {
        while let Ok(op) = rx.try_recv() {
            self.ops.push(op);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

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
                MutationOp::SortSet { field, bit_layer, slots } => {
                    self.sort_sets
                        .entry(SortGroupKey { field, bit_layer })
                        .or_default()
                        .extend(slots);
                }
                MutationOp::SortClear { field, bit_layer, slots } => {
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
        for slots in self.filter_inserts.values_mut() { slots.sort_unstable(); }
        for slots in self.filter_removes.values_mut() { slots.sort_unstable(); }
        for slots in self.sort_sets.values_mut() { slots.sort_unstable(); }
        for slots in self.sort_clears.values_mut() { slots.sort_unstable(); }
        self.alive_inserts.sort_unstable();
        self.alive_removes.sort_unstable();
    }

    pub fn has_alive_mutations(&self) -> bool {
        !self.alive_inserts.is_empty() || !self.alive_removes.is_empty()
    }

    pub fn mutated_filter_fields(&self) -> HashSet<&str> {
        let mut fields = HashSet::new();
        for key in self.filter_inserts.keys() { fields.insert(&*key.field); }
        for key in self.filter_removes.keys() { fields.insert(&*key.field); }
        fields
    }

    pub fn apply(
        &self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) {
        // Removes before inserts: on upsert, remove-old then insert-new is safe
        for (key, slot_ids) in &self.filter_removes {
            if let Some(field) = filters.get_field_mut(&key.field) {
                field.remove_bulk(key.value, slot_ids);
            }
        }
        for (key, slot_ids) in &self.filter_inserts {
            if let Some(field) = filters.get_field_mut(&key.field) {
                field.insert_bulk(key.value, slot_ids.iter().copied());
            }
        }
        // Clears before sets: on slot recycling, clear-old then set-new is safe
        for (key, slot_ids) in &self.sort_clears {
            if let Some(field) = sorts.get_field_mut(&key.field) {
                field.clear_layer_bulk(key.bit_layer, slot_ids);
            }
        }
        for (key, slot_ids) in &self.sort_sets {
            if let Some(field) = sorts.get_field_mut(&key.field) {
                field.set_layer_bulk(key.bit_layer, slot_ids.iter().copied());
            }
        }
        if !self.alive_inserts.is_empty() {
            slots.alive_insert_bulk(self.alive_inserts.iter().copied());
        }
        for &slot in &self.alive_removes {
            slots.alive_remove_one(slot);
        }
        for &(slot, activate_at) in &self.deferred_alive {
            slots.schedule_alive(slot, activate_at);
        }
        // Eager merge sort diffs
        let mut mutated_sort_fields: HashSet<&str> = HashSet::new();
        for key in self.sort_sets.keys() { mutated_sort_fields.insert(&key.field); }
        for key in self.sort_clears.keys() { mutated_sort_fields.insert(&key.field); }
        for field_name in &mutated_sort_fields {
            if let Some(field) = sorts.get_field_mut(field_name) {
                field.merge_dirty();
            }
        }
        slots.merge_alive();
    }
}
