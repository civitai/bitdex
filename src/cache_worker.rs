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
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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
///
/// The reason-attributed `marked_*_total` counters and the `rebuild_completed_total`
/// counter are also incremented from `UnifiedCache` (the cache holds an Arc to this
/// struct for those updates). The single shared struct keeps the prom-export side
/// simple while letting both the worker and the cache contribute to the same totals.
#[derive(Default)]
pub struct CacheWorkerMetrics {
    pub queue_depth: AtomicU64,
    pub cycle_nanos: AtomicU64,
    pub items_coalesced_total: AtomicU64,
    pub drops_total: AtomicU64,
    pub over_budget_total: AtomicU64,
    pub backpressure_invalidations_total: AtomicU64,
    pub cycles_total: AtomicU64,
    /// Cache entries marked for rebuild because the time deadline was hit
    /// (`max_maintenance_ms` exceeded mid-cycle). `max_maintenance_ms` is now
    /// deprecated (no-op); this counter is retained for metric continuity.
    pub marked_for_rebuild_deadline_total: AtomicU64,
    /// Cache entries evicted (not merely marked for rebuild) because the count
    /// budget was hit (`max_maintenance_work` exceeded before evaluation started).
    /// `max_maintenance_work` defaults to 0 (disabled) in new configs.
    pub marked_for_rebuild_count_budget_total: AtomicU64,
    /// Cache entries marked for rebuild because the worker backlog overflowed
    /// `backlog_drop_limit` and we fell back to coarse invalidation.
    pub marked_for_rebuild_backlog_drop_total: AtomicU64,
    /// Cache entries marked for rebuild as a consequence of an alive-bitmap
    /// mutation (delete) — every entry is invalidated since negation operators
    /// bake alive into the result.
    pub marked_for_rebuild_alive_change_total: AtomicU64,
    /// Cache entries marked for rebuild via `invalidate_filter_field` (compound
    /// or otherwise un-narrowable filter mutations).
    pub marked_for_rebuild_filter_invalidation_total: AtomicU64,
    /// Cache entries that successfully completed a rebuild — either via
    /// `UnifiedEntry::rebuild` directly or by being replaced in `store()` when
    /// the prior entry had `needs_rebuild=true`.
    pub rebuild_completed_total: AtomicU64,
    /// String-key resolution failures during native FilterClause evaluation.
    /// Non-zero in prod means string_maps/dictionaries aren't threaded correctly.
    pub string_lookup_misses_total: AtomicU64,
    /// Cache entries skipped and marked for rebuild because their compound
    /// FilterClause tree exceeded `compound_eval_atom_limit` (B9 safety valve).
    pub marked_for_rebuild_compound_too_large_total: AtomicU64,
    /// Cache entries evicted (removed entirely) because estimated maintenance work
    /// exceeded `max_maintenance_work`. Eviction is preferred over mark-for-rebuild:
    /// an evicted entry re-populates on the next `store()`, while a rebuild-flagged
    /// entry causes every query to pay a cold-miss re-derive until it's rebuilt.
    pub evicted_on_overrun_total: AtomicU64,
    /// Histogram for per-cycle wall-clock time. Installed post-engine-construction
    /// by `set_metrics_bridge` so the worker can see the prom-bound `Histogram`.
    /// Optional — the worker no-ops when unset.
    ///
    /// Gated on the `server` feature: `prometheus` is a server-only dep, but
    /// `cache_worker` is part of the lib and is compiled for the pg-sync
    /// binary too (which doesn't link prometheus).
    #[cfg(feature = "server")]
    pub cycle_histogram: OnceLock<prometheus::Histogram>,
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
    cache: Arc<UnifiedCache>,
    engine: Arc<ArcSwap<InnerEngine>>,
    config: CacheWorkerConfig,
    metrics: Arc<CacheWorkerMetrics>,
    shutdown: Arc<AtomicBool>,
    /// Counter for periodic `reconcile_bytes` calls — see Phase C in `run`.
    cycles_since_reconcile: u32,
    /// Time bucket manager, if time buckets are configured. Used to evaluate
    /// `bucket` clauses during cache maintenance — the bitmap.contains(slot)
    /// check replaces the old always-true conservative fallback.
    time_buckets: Option<Arc<arc_swap::ArcSwap<crate::time_buckets::TimeBucketManager>>>,
    /// Shared string_maps from the engine. Updated by `set_string_maps` via ArcSwap.
    /// Threaded to `evaluate_filter_work` / `evaluate_sort_work` for native
    /// FilterClause eval (B2: string-typed In/Eq resolution).
    shared_string_maps: Arc<arc_swap::ArcSwap<Option<crate::executor::StringMaps>>>,
    /// Shared dictionaries from the engine. Updated by `set_dictionaries` via ArcSwap.
    /// Wraps `Arc<HashMap<...>>` so no clone of (non-Clone) FieldDictionary is needed.
    shared_dictionaries: Arc<arc_swap::ArcSwap<std::sync::Arc<ahash::AHashMap<String, crate::dictionary::FieldDictionary>>>>,
}

impl CacheWorker {
    pub fn new(
        rx: crossbeam_channel::Receiver<CacheWorkItem>,
        cache: Arc<UnifiedCache>,
        engine: Arc<ArcSwap<InnerEngine>>,
        config: CacheWorkerConfig,
        metrics: Arc<CacheWorkerMetrics>,
        shutdown: Arc<AtomicBool>,
        shared_string_maps: Arc<arc_swap::ArcSwap<Option<crate::executor::StringMaps>>>,
        shared_dictionaries: Arc<arc_swap::ArcSwap<std::sync::Arc<ahash::AHashMap<String, crate::dictionary::FieldDictionary>>>>,
    ) -> Self {
        Self {
            rx,
            cache,
            engine,
            config,
            metrics,
            shutdown,
            cycles_since_reconcile: 0,
            time_buckets: None,
            shared_string_maps,
            shared_dictionaries,
        }
    }

    /// Attach a time bucket manager so bucket clauses are evaluated precisely.
    /// Call this before `run()`.
    pub fn with_time_buckets(
        mut self,
        tb: Arc<arc_swap::ArcSwap<crate::time_buckets::TimeBucketManager>>,
    ) -> Self {
        self.time_buckets = Some(tb);
        self
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
            //
            // `max_maintenance_ms` is deprecated (no-op). The deadline existed to
            // protect the flush thread from stalling; the worker has its own thread
            // so no per-cycle deadline is needed. Work runs to completion in parallel
            // via rayon, then results are applied under a brief per-shard lock.

            // No outer lock — UnifiedCache is interior-mutable. Each method
            // call acquires the per-shard / per-field locks it needs and
            // releases them immediately. Concurrent queries on other shards
            // are not blocked while we run.
            let uc = &self.cache;

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

            let (filter_work, filter_over_budget) = if !merged.mutated_filter_fields.is_empty() {
                uc.collect_filter_work(&merged.filter_inserts, &merged.filter_removes)
            } else {
                (Vec::new(), Vec::new())
            };
            let sort_mutations_borrowed: HashMap<&str, HashSet<u32>> = merged
                .sort_mutations
                .iter()
                .map(|(k, v)| (k.as_ref(), v.clone()))
                .collect();
            let (sort_work, sort_over_budget) = if !sort_mutations_borrowed.is_empty() {
                uc.collect_sort_work(&sort_mutations_borrowed)
            } else {
                (Vec::new(), Vec::new())
            };

            // Phase B analogue — lock-free eval against the published snapshot.
            // Load the time bucket manager snapshot once per cycle; the flush
            // thread already ran insert_slot/remove_slot before enqueuing this
            // work item, so the bucket bitmaps are authoritative.
            let tb_guard = self
                .time_buckets
                .as_ref()
                .map(|arc| arc.load_full());
            let tb_ref = tb_guard.as_deref();
            // Load string_maps + dictionaries for native FilterClause eval (B2).
            let sm_guard = self.shared_string_maps.load_full();
            let sm_ref = (*sm_guard).as_ref();
            let dict_guard = self.shared_dictionaries.load_full();
            let dict_inner = std::sync::Arc::clone(&*dict_guard);
            let dict_ref: &ahash::AHashMap<String, crate::dictionary::FieldDictionary> = &*dict_inner;
            let string_misses = std::sync::atomic::AtomicU64::new(0);
            let compound_too_large = std::sync::atomic::AtomicU64::new(0);
            let compound_atom_limit = uc.compound_eval_atom_limit();
            // `None` deadline: no per-cycle time limit (see comment above).
            let (filter_results, filter_compound_too_large) = if !filter_work.is_empty() {
                evaluate_filter_work(
                    &filter_work, &snap.filters, &snap.sorts, None, tb_ref,
                    sm_ref, Some(dict_ref), &string_misses,
                    compound_atom_limit, &compound_too_large,
                )
            } else {
                (Vec::new(), Vec::new())
            };

            let (sort_results, sort_compound_too_large) = if !sort_work.is_empty() {
                evaluate_sort_work(
                    &sort_work,
                    &snap.filters,
                    &snap.sorts,
                    None,
                    tb_ref,
                    sm_ref,
                    Some(dict_ref),
                    &string_misses,
                    compound_atom_limit, &compound_too_large,
                )
            } else {
                (Vec::new(), Vec::new())
            };
            let misses = string_misses.load(Ordering::Relaxed);
            if misses > 0 {
                self.metrics.string_lookup_misses_total.fetch_add(misses, Ordering::Relaxed);
            }
            let too_large = compound_too_large.load(Ordering::Relaxed);
            if too_large > 0 {
                self.metrics
                    .marked_for_rebuild_compound_too_large_total
                    .fetch_add(too_large, Ordering::Relaxed);
            }

            if !filter_results.is_empty()
                || !sort_results.is_empty()
                || !filter_over_budget.is_empty()
                || !sort_over_budget.is_empty()
                || !filter_compound_too_large.is_empty()
                || !sort_compound_too_large.is_empty()
            {
                uc.apply_maintenance_results(&filter_results);
                uc.apply_maintenance_results(&sort_results);
                // `over_budget` keys: estimated work (entries × slots) exceeded
                // `max_maintenance_work` — evict them so the next query re-populates
                // cleanly rather than paying a per-query rebuild cost indefinitely.
                let cb = (filter_over_budget.len() + sort_over_budget.len()) as u64;
                if cb > 0 {
                    let evicted = uc.evict_keys_on_overrun(&filter_over_budget)
                        + uc.evict_keys_on_overrun(&sort_over_budget);
                    if evicted > 0 {
                        self.metrics
                            .evicted_on_overrun_total
                            .fetch_add(evicted, Ordering::Relaxed);
                    }
                }
                // `compound_too_large` keys: B9 safety valve — pathological compound
                // clause trees that exceed `compound_eval_atom_limit`. Evict so queries
                // can still populate a fresh entry; mark-for-rebuild would cause every
                // query to hit the rebuild path forever. The B9 atom-count check will
                // skip them again on the next maintenance cycle.
                let ctl = (filter_compound_too_large.len() + sort_compound_too_large.len()) as u64;
                if ctl > 0 {
                    let evicted = uc.evict_keys_on_overrun(&filter_compound_too_large)
                        + uc.evict_keys_on_overrun(&sort_compound_too_large);
                    if evicted > 0 {
                        self.metrics
                            .evicted_on_overrun_total
                            .fetch_add(evicted, Ordering::Relaxed);
                    }
                }
                // Reconcile total_bytes every 30th cycle. The store/evict
                // paths track total_bytes incrementally; only bulk ops drift
                // it, so periodic reconcile is enough to keep eviction-budget
                // decisions honest without paying the scan cost every cycle.
                self.cycles_since_reconcile += 1;
                if self.cycles_since_reconcile >= 30 {
                    uc.reconcile_bytes();
                    self.cycles_since_reconcile = 0;
                }
            }

            let over_budget = (filter_over_budget.len()
                + sort_over_budget.len()
                + filter_compound_too_large.len()
                + sort_compound_too_large.len()) as u64;
            if over_budget > 0 {
                self.metrics
                    .over_budget_total
                    .fetch_add(over_budget, Ordering::Relaxed);
            }
            let elapsed = t.elapsed();
            self.metrics
                .cycle_nanos
                .store(elapsed.as_nanos() as u64, Ordering::Relaxed);
            #[cfg(feature = "server")]
            if let Some(h) = self.metrics.cycle_histogram.get() {
                h.observe(elapsed.as_secs_f64());
            }
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
        let uc = &self.cache;
        let mut any_alive = false;
        let mut affected_filter_fields: HashSet<Arc<str>> = HashSet::new();
        for item in pending {
            affected_filter_fields.extend(item.mutated_filter_fields.iter().cloned());
            any_alive |= item.has_alive_mutations;
        }
        if any_alive {
            let n = uc.maintain_alive_changes();
            if n > 0 {
                self.metrics
                    .marked_for_rebuild_backlog_drop_total
                    .fetch_add(n, Ordering::Relaxed);
            }
            return;
        }
        for field in &affected_filter_fields {
            let n = uc.invalidate_filter_field(field);
            if n > 0 {
                self.metrics
                    .marked_for_rebuild_backlog_drop_total
                    .fetch_add(n, Ordering::Relaxed);
            }
        }
        let any_sort = pending.iter().any(|it| !it.sort_mutations.is_empty());
        if any_sort {
            let n = uc.maintain_alive_changes();
            if n > 0 {
                self.metrics
                    .marked_for_rebuild_backlog_drop_total
                    .fetch_add(n, Ordering::Relaxed);
            }
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
