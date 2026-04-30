//! Async cache maintenance worker.
//!
//! Moves unified-cache live maintenance off the flush thread onto a dedicated
//! worker thread. The flush thread publishes a `CacheWorkItem` per cycle with
//! the coalescer outputs; this worker drains the channel, merges overlapping
//! items, evaluates them against the latest published `InnerEngine` snapshot,
//! and applies results to the cache.
//!
//! Design: `docs/_in/design-async-cache-maintenance.md`.
//!
//! Enabled per-engine by `config.cache.async_maintenance`. When disabled the
//! flush thread runs Phases A/B/C inline as before. Zero-risk revert: toggle
//! the flag via `PATCH /indexes/{name}/config`.

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::concurrent_engine::InnerEngine;
use crate::unified_cache::{
    evaluate_filter_work, evaluate_sort_work, UnifiedCache,
};
use crate::write_coalescer::FilterGroupKey;

/// A unit of cache maintenance work produced by one flush cycle.
///
/// Built from `WriteCoalescer`'s grouped output after the flush thread has
/// applied the batch to staging. The flush thread holds the constructed item,
/// publishes the new snapshot via `inner.store(...)`, and only then `try_send`s
/// it to this worker. The post-publish send order is load-bearing: the worker
/// calls `inner.load()` at dequeue time to obtain the index handle, so an
/// enqueue before publish would let the worker evaluate maintenance work
/// against the previous published snapshot.
///
/// **Snapshot-monotonicity contract:** a work item may be processed by the
/// worker against a snapshot *newer* than the one whose coalesced mutations
/// produced it (e.g. when items N and N+1 are queued before the worker
/// dequeues N). This is safe and intended — the inserts/removes carried in
/// `filter_inserts` / `filter_removes` / `sort_mutations` act as **triggers**
/// that point the worker at the affected (key, value) buckets and sort
/// fields. Final cache membership is re-derived against the loaded
/// snapshot's filter/sort indexes via `evaluate_filter_work` /
/// `evaluate_sort_work`, not from the deltas themselves. Future refactors
/// MUST preserve this re-derivation; treating the deltas as authoritative
/// "apply blindly" patches would reintroduce stale-snapshot bugs.
#[derive(Debug)]
pub struct CacheWorkItem {
    /// Slots newly added to (field, value) buckets this cycle.
    pub filter_inserts: HashMap<FilterGroupKey, Vec<u32>>,
    /// Slots cleared from (field, value) buckets this cycle.
    pub filter_removes: HashMap<FilterGroupKey, Vec<u32>>,
    /// Slots whose sort value changed, grouped by sort field.
    pub sort_mutations: HashMap<Arc<str>, HashSet<u32>>,
    /// Slots whose alive bit was cleared.
    pub alive_removes: Vec<u32>,
    /// Filter field names touched (used for tombstone bookkeeping).
    pub mutated_filter_fields: HashSet<Arc<str>>,
    /// True when alive mutations occurred — triggers tombstone_all_unloaded
    /// on unloaded entries (matches Phase A behavior).
    pub has_alive_mutations: bool,
}

impl CacheWorkItem {
    pub fn is_empty(&self) -> bool {
        self.filter_inserts.is_empty()
            && self.filter_removes.is_empty()
            && self.sort_mutations.is_empty()
            && self.alive_removes.is_empty()
            && !self.has_alive_mutations
    }
}

/// Metrics exposed by the cache worker. Owned by the engine, referenced by
/// both the worker thread and the Prometheus metrics bridge.
#[derive(Default)]
pub struct CacheWorkerMetrics {
    pub queue_depth: AtomicU64,
    pub cycle_nanos: AtomicU64,
    pub items_coalesced_total: AtomicU64,
    pub drops_total: AtomicU64,
    pub over_budget_total: AtomicU64,
    pub backpressure_invalidations_total: AtomicU64,
    pub cycles_total: AtomicU64,
}

/// Cache worker configuration. Derived from `CacheConfig` + engine state.
#[derive(Clone)]
pub struct CacheWorkerConfig {
    /// Per-cycle deadline for work evaluation in milliseconds.
    ///
    /// `0` means unlimited — the worker processes all entries in the coalesced
    /// batch before moving on. This is the correct default now that maintenance
    /// runs on its own thread (the deadline existed to protect the flush thread
    /// from stalling; it is unnecessary here).
    ///
    /// Stored as `Arc<AtomicU64>` so the engine can update it at runtime via
    /// `PATCH /indexes/{name}/config` without restarting the worker thread.
    pub max_maintenance_ms: Arc<AtomicU64>,
    /// Drain at most this many items per cycle before evaluating. Past this
    /// we invalidate affected entries and drop the items — same fallback as
    /// the existing `max_maintenance_ms` deadline path, just applied to the
    /// whole backlog instead of a single cycle.
    pub backlog_drop_limit: usize,
}

impl Default for CacheWorkerConfig {
    fn default() -> Self {
        Self {
            // 0 = unlimited (no deadline). The worker has its own thread so
            // there is no flush-thread stall risk.
            max_maintenance_ms: Arc::new(AtomicU64::new(0)),
            backlog_drop_limit: 4096,
        }
    }
}

/// Coalesce N work items into one.
///
/// Coalescing rules (§Coalescing rules in design doc):
/// - `filter_inserts`/`filter_removes`: per-`FilterGroupKey`, concatenate slot
///   lists then cancel (inserts − removes, removes − inserts).
/// - `sort_mutations`: per-field set-union of slots. The worker re-derives
///   sort values from the published snapshot, so "latest wins" falls out
///   naturally — whatever the snapshot says is the final value.
/// - `alive_removes`: set-union.
/// - `mutated_filter_fields`: set-union.
/// - `has_alive_mutations`: OR.
pub fn coalesce_work_items(items: impl IntoIterator<Item = CacheWorkItem>) -> CacheWorkItem {
    let mut merged = CacheWorkItem {
        filter_inserts: HashMap::new(),
        filter_removes: HashMap::new(),
        sort_mutations: HashMap::new(),
        alive_removes: Vec::new(),
        mutated_filter_fields: HashSet::new(),
        has_alive_mutations: false,
    };

    // First pass: concatenate.
    let mut alive_set: HashSet<u32> = HashSet::new();
    for item in items {
        for (k, mut v) in item.filter_inserts {
            merged.filter_inserts.entry(k).or_default().append(&mut v);
        }
        for (k, mut v) in item.filter_removes {
            merged.filter_removes.entry(k).or_default().append(&mut v);
        }
        for (k, v) in item.sort_mutations {
            merged.sort_mutations.entry(k).or_default().extend(v);
        }
        alive_set.extend(item.alive_removes);
        merged.mutated_filter_fields.extend(item.mutated_filter_fields);
        merged.has_alive_mutations |= item.has_alive_mutations;
    }

    // Second pass: dedup + cancel (insert ⊖ remove) within each key.
    for (_k, slots) in merged.filter_inserts.iter_mut() {
        dedup_sort(slots);
    }
    for (_k, slots) in merged.filter_removes.iter_mut() {
        dedup_sort(slots);
    }
    cancel_pairs(&mut merged.filter_inserts, &mut merged.filter_removes);

    merged.alive_removes = alive_set.into_iter().collect();
    merged.alive_removes.sort_unstable();

    merged
}

fn dedup_sort(v: &mut Vec<u32>) {
    if v.len() <= 1 {
        return;
    }
    v.sort_unstable();
    v.dedup();
}

/// For each key present in both maps, remove slots that appear in both.
/// An insert+remove for the same slot in the same cycle is a no-op.
fn cancel_pairs(
    inserts: &mut HashMap<FilterGroupKey, Vec<u32>>,
    removes: &mut HashMap<FilterGroupKey, Vec<u32>>,
) {
    let shared_keys: Vec<FilterGroupKey> = inserts
        .keys()
        .filter(|k| removes.contains_key(*k))
        .cloned()
        .collect();

    for key in shared_keys {
        let ins = inserts.get_mut(&key).unwrap();
        let rem = removes.get_mut(&key).unwrap();
        // Both are sorted+deduped from dedup_sort above. Two-pointer difference.
        let (new_ins, new_rem) = symmetric_difference_sorted(ins, rem);
        if new_ins.is_empty() {
            inserts.remove(&key);
        } else {
            *ins = new_ins;
        }
        if new_rem.is_empty() {
            removes.remove(&key);
        } else {
            *rem = new_rem;
        }
    }
}

fn symmetric_difference_sorted(a: &[u32], b: &[u32]) -> (Vec<u32>, Vec<u32>) {
    let mut only_a = Vec::with_capacity(a.len());
    let mut only_b = Vec::with_capacity(b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                only_a.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                only_b.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    only_a.extend_from_slice(&a[i..]);
    only_b.extend_from_slice(&b[j..]);
    (only_a, only_b)
}

/// Runs in its own thread. Owns the receive side of the work-item channel.
pub struct CacheWorker {
    rx: crossbeam_channel::Receiver<CacheWorkItem>,
    cache: Arc<Mutex<UnifiedCache>>,
    engine: Arc<ArcSwap<InnerEngine>>,
    config: CacheWorkerConfig,
    metrics: Arc<CacheWorkerMetrics>,
    shutdown: Arc<AtomicBool>,
    /// Counter for periodic `reconcile_bytes` calls — see Phase C in `run`.
    cycles_since_reconcile: u32,
}

impl CacheWorker {
    pub fn new(
        rx: crossbeam_channel::Receiver<CacheWorkItem>,
        cache: Arc<Mutex<UnifiedCache>>,
        engine: Arc<ArcSwap<InnerEngine>>,
        config: CacheWorkerConfig,
        metrics: Arc<CacheWorkerMetrics>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            rx,
            cache,
            engine,
            config,
            metrics,
            shutdown,
            cycles_since_reconcile: 0,
        }
    }

    /// Main loop. Exits when the channel disconnects or `shutdown` is set.
    pub fn run(mut self) {
        let mut pending: VecDeque<CacheWorkItem> = VecDeque::new();
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            // Block for the first item (with a short timeout so we can observe
            // shutdown). If the channel disconnected, exit.
            let first = match self.rx.recv_timeout(Duration::from_millis(250)) {
                Ok(item) => item,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };
            pending.push_back(first);

            // Non-blocking drain of anything else that's accumulated.
            while let Ok(item) = self.rx.try_recv() {
                pending.push_back(item);
                if pending.len() > self.config.backlog_drop_limit {
                    // Backlog too deep — invalidate affected entries and drop
                    // the queue. Same fallback as the existing max_maintenance_ms
                    // escape hatch, applied to the whole backlog.
                    self.invalidate_and_drop(&pending);
                    self.metrics.drops_total.fetch_add(1, Ordering::Relaxed);
                    pending.clear();
                    break;
                }
            }
            if pending.is_empty() {
                continue;
            }

            let t = Instant::now();
            let n_items = pending.len() as u64;
            let merged = coalesce_work_items(pending.drain(..));
            self.metrics
                .items_coalesced_total
                .fetch_add(n_items, Ordering::Relaxed);

            if merged.is_empty() {
                // All cancelled — nothing to do.
                continue;
            }

            // Load the latest published snapshot. `load_full()` gives us an
            // owned Arc<InnerEngine> we can keep for the duration of this
            // cycle without fighting the ArcSwap guard lifetime.
            let snap = self.engine.load_full();

            // Phase A + C analogue: collect work items under the cache lock,
            // evaluate without it, then reapply. Unlike the flush-thread
            // inline path, this entire sequence runs off the flush cycle so
            // writers are unblocked regardless of how long we take.
            let ms = self.config.max_maintenance_ms.load(Ordering::Relaxed);
            let deadline = if ms > 0 {
                Some(Instant::now() + Duration::from_millis(ms))
            } else {
                None
            };

            let (filter_work, filter_over_budget, sort_work, sort_over_budget) = {
                let mut uc = self.cache.lock();

                // Batched alive removal.
                if !uc.is_empty() && !merged.alive_removes.is_empty() {
                    uc.remove_slots_from_all_batch(&merged.alive_removes);
                }

                // Tombstone bookkeeping for persistence-enabled caches.
                if uc.persistence_enabled() {
                    let filter_fields: Vec<&str> = merged
                        .mutated_filter_fields
                        .iter()
                        .map(|s| s.as_ref())
                        .collect();
                    if !filter_fields.is_empty() {
                        let _ = uc.tombstone_unloaded_for_filter(&filter_fields);
                    }
                    let sort_fields: Vec<&str> = merged
                        .sort_mutations
                        .keys()
                        .map(|s| s.as_ref())
                        .collect();
                    if !sort_fields.is_empty() {
                        let _ = uc.tombstone_unloaded_for_sort(&sort_fields);
                    }
                    if merged.has_alive_mutations && !merged.alive_removes.is_empty() {
                        let _ = uc.tombstone_all_unloaded();
                    }
                }

                let (fw, fob) = if !merged.mutated_filter_fields.is_empty() {
                    uc.collect_filter_work(&merged.filter_inserts, &merged.filter_removes)
                } else {
                    (Vec::new(), Vec::new())
                };
                // Sort work needs `HashMap<&str, HashSet<u32>>` — build it
                // from the owned Arc<str> keys. The &str refs borrow from
                // `merged` so they live as long as this scope.
                let sort_mutations_borrowed: HashMap<&str, HashSet<u32>> = merged
                    .sort_mutations
                    .iter()
                    .map(|(k, v)| (k.as_ref(), v.clone()))
                    .collect();
                let (sw, sob) = if !sort_mutations_borrowed.is_empty() {
                    uc.collect_sort_work(&sort_mutations_borrowed)
                } else {
                    (Vec::new(), Vec::new())
                };
                (fw, fob, sw, sob)
            };

            // Phase B analogue — lock-free eval against the published snapshot.
            let (filter_results, filter_timed_out) = if !filter_work.is_empty() {
                evaluate_filter_work(&filter_work, &snap.filters, &snap.sorts, deadline)
            } else {
                (Vec::new(), Vec::new())
            };

            let (sort_results, sort_timed_out) = if !sort_work.is_empty() {
                evaluate_sort_work(
                    &sort_work,
                    &snap.filters,
                    &snap.sorts,
                    deadline,
                )
            } else {
                (Vec::new(), Vec::new())
            };

            if !filter_results.is_empty()
                || !sort_results.is_empty()
                || !filter_over_budget.is_empty()
                || !sort_over_budget.is_empty()
                || !filter_timed_out.is_empty()
                || !sort_timed_out.is_empty()
            {
                let mut uc = self.cache.lock();
                uc.apply_maintenance_results(&filter_results);
                uc.apply_maintenance_results(&sort_results);
                uc.mark_for_rebuild_batch(&filter_over_budget);
                uc.mark_for_rebuild_batch(&sort_over_budget);
                uc.mark_for_rebuild_batch(&filter_timed_out);
                uc.mark_for_rebuild_batch(&sort_timed_out);
                // Reconcile total_bytes every 30th cycle instead of every
                // cycle. reconcile_bytes scans all entries calling
                // bitmap.serialized_size() — observed locally as ~tens of
                // ms under the cache mutex on a full 100 K cache. The
                // store/evict paths track total_bytes incrementally; only
                // bulk ops (add_slots_bulk / remove_slots_bulk) drift it,
                // and the drift is small per cycle. Reconciling
                // periodically keeps eviction-budget decisions honest
                // without paying the scan cost on every cycle.
                self.cycles_since_reconcile += 1;
                if self.cycles_since_reconcile >= 30 {
                    uc.reconcile_bytes();
                    self.cycles_since_reconcile = 0;
                }
            }

            let over_budget = (filter_over_budget.len()
                + sort_over_budget.len()
                + filter_timed_out.len()
                + sort_timed_out.len()) as u64;
            if over_budget > 0 {
                self.metrics
                    .over_budget_total
                    .fetch_add(over_budget, Ordering::Relaxed);
            }
            self.metrics
                .cycle_nanos
                .store(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.metrics.cycles_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .queue_depth
                .store(self.rx.len() as u64, Ordering::Relaxed);
        }
    }

    /// Drop the queue and mark affected entries for rebuild. Invoked when the
    /// worker's backlog exceeds `backlog_drop_limit` — at that point it's
    /// cheaper to let queries rebuild on-demand than to chase a runaway queue.
    ///
    /// Strategy: conservatively invalidate everything touched by any item in
    /// the dropped batch. If any item carried alive mutations, fall back to
    /// `maintain_alive_changes` which rebuilds every entry — alive flips
    /// affect filter eligibility globally, so precise invalidation isn't
    /// cheaper than a full sweep.
    fn invalidate_and_drop(&self, pending: &VecDeque<CacheWorkItem>) {
        let mut uc = self.cache.lock();
        let mut any_alive = false;
        let mut affected_filter_fields: HashSet<Arc<str>> = HashSet::new();
        for item in pending {
            affected_filter_fields.extend(item.mutated_filter_fields.iter().cloned());
            any_alive |= item.has_alive_mutations;
        }
        if any_alive {
            uc.maintain_alive_changes();
            return;
        }
        for field in &affected_filter_fields {
            uc.invalidate_filter_field(field);
        }
        // Sort mutations without alive changes: mark entries referencing any
        // mutated sort field for rebuild. Done via maintain_alive_changes
        // equivalent — too coarse for the fast path, but this is the fallback
        // path for backlog saturation so conservative invalidation is fine.
        let any_sort = pending.iter().any(|it| !it.sort_mutations.is_empty());
        if any_sort {
            uc.maintain_alive_changes();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_coalescer::FilterGroupKey;

    fn key(field: &str, value: u64) -> FilterGroupKey {
        FilterGroupKey {
            field: Arc::from(field),
            value,
        }
    }

    fn item_with_filter_insert(field: &str, value: u64, slots: Vec<u32>) -> CacheWorkItem {
        let mut filter_inserts = HashMap::new();
        filter_inserts.insert(key(field, value), slots);
        let mut fields = HashSet::new();
        fields.insert(Arc::<str>::from(field));
        CacheWorkItem {
            filter_inserts,
            filter_removes: HashMap::new(),
            sort_mutations: HashMap::new(),
            alive_removes: Vec::new(),
            mutated_filter_fields: fields,
            has_alive_mutations: false,
        }
    }

    fn item_with_filter_remove(field: &str, value: u64, slots: Vec<u32>) -> CacheWorkItem {
        let mut filter_removes = HashMap::new();
        filter_removes.insert(key(field, value), slots);
        let mut fields = HashSet::new();
        fields.insert(Arc::<str>::from(field));
        CacheWorkItem {
            filter_inserts: HashMap::new(),
            filter_removes,
            sort_mutations: HashMap::new(),
            alive_removes: Vec::new(),
            mutated_filter_fields: fields,
            has_alive_mutations: false,
        }
    }

    #[test]
    fn coalesce_cancels_insert_plus_remove_for_same_slot() {
        let items = vec![
            item_with_filter_insert("status", 1, vec![10, 20, 30]),
            item_with_filter_remove("status", 1, vec![20, 40]),
        ];
        let merged = coalesce_work_items(items);
        // 20 cancels on both sides; inserts keep 10, 30; removes keep 40.
        let k = key("status", 1);
        assert_eq!(merged.filter_inserts.get(&k), Some(&vec![10, 30]));
        assert_eq!(merged.filter_removes.get(&k), Some(&vec![40]));
    }

    #[test]
    fn coalesce_full_cancellation_removes_key() {
        let items = vec![
            item_with_filter_insert("status", 1, vec![10, 20]),
            item_with_filter_remove("status", 1, vec![10, 20]),
        ];
        let merged = coalesce_work_items(items);
        let k = key("status", 1);
        assert!(
            merged.filter_inserts.get(&k).is_none(),
            "fully cancelled inserts should be dropped"
        );
        assert!(
            merged.filter_removes.get(&k).is_none(),
            "fully cancelled removes should be dropped"
        );
    }

    #[test]
    fn coalesce_dedups_within_single_side() {
        let items = vec![
            item_with_filter_insert("status", 1, vec![10, 20]),
            item_with_filter_insert("status", 1, vec![20, 30]),
        ];
        let merged = coalesce_work_items(items);
        let k = key("status", 1);
        assert_eq!(merged.filter_inserts.get(&k), Some(&vec![10, 20, 30]));
    }

    #[test]
    fn coalesce_unions_sort_mutations() {
        let field: Arc<str> = Arc::from("reactionCount");
        let mut a = CacheWorkItem {
            filter_inserts: HashMap::new(),
            filter_removes: HashMap::new(),
            sort_mutations: HashMap::new(),
            alive_removes: Vec::new(),
            mutated_filter_fields: HashSet::new(),
            has_alive_mutations: false,
        };
        a.sort_mutations
            .insert(field.clone(), [1u32, 2, 3].iter().copied().collect());
        let mut b = CacheWorkItem {
            filter_inserts: HashMap::new(),
            filter_removes: HashMap::new(),
            sort_mutations: HashMap::new(),
            alive_removes: Vec::new(),
            mutated_filter_fields: HashSet::new(),
            has_alive_mutations: false,
        };
        b.sort_mutations
            .insert(field.clone(), [3u32, 4, 5].iter().copied().collect());

        let merged = coalesce_work_items(vec![a, b]);
        let got = merged.sort_mutations.get(&field).unwrap();
        assert_eq!(got, &[1u32, 2, 3, 4, 5].iter().copied().collect::<HashSet<u32>>());
    }

    #[test]
    fn coalesce_empty_input_yields_empty_item() {
        let merged = coalesce_work_items(Vec::<CacheWorkItem>::new());
        assert!(merged.is_empty());
    }

    #[test]
    fn coalesce_unions_alive_removes_and_filter_fields() {
        let items = vec![
            CacheWorkItem {
                filter_inserts: HashMap::new(),
                filter_removes: HashMap::new(),
                sort_mutations: HashMap::new(),
                alive_removes: vec![1, 2, 3],
                mutated_filter_fields: [Arc::<str>::from("a")].iter().cloned().collect(),
                has_alive_mutations: true,
            },
            CacheWorkItem {
                filter_inserts: HashMap::new(),
                filter_removes: HashMap::new(),
                sort_mutations: HashMap::new(),
                alive_removes: vec![2, 4],
                mutated_filter_fields: [Arc::<str>::from("b")].iter().cloned().collect(),
                has_alive_mutations: false,
            },
        ];
        let merged = coalesce_work_items(items);
        assert_eq!(merged.alive_removes, vec![1, 2, 3, 4]);
        assert!(merged.mutated_filter_fields.contains(&Arc::<str>::from("a")));
        assert!(merged.mutated_filter_fields.contains(&Arc::<str>::from("b")));
        assert!(merged.has_alive_mutations);
    }

    #[test]
    fn symmetric_difference_sorted_basic() {
        let a = vec![1u32, 2, 3, 5, 7];
        let b = vec![2u32, 3, 4, 6, 7];
        let (only_a, only_b) = symmetric_difference_sorted(&a, &b);
        assert_eq!(only_a, vec![1, 5]);
        assert_eq!(only_b, vec![4, 6]);
    }

    #[test]
    fn symmetric_difference_sorted_disjoint() {
        let a = vec![1u32, 3, 5];
        let b = vec![2u32, 4, 6];
        let (only_a, only_b) = symmetric_difference_sorted(&a, &b);
        assert_eq!(only_a, vec![1, 3, 5]);
        assert_eq!(only_b, vec![2, 4, 6]);
    }

    #[test]
    fn symmetric_difference_sorted_equal() {
        let a = vec![1u32, 2, 3];
        let b = vec![1u32, 2, 3];
        let (only_a, only_b) = symmetric_difference_sorted(&a, &b);
        assert!(only_a.is_empty());
        assert!(only_b.is_empty());
    }
}
