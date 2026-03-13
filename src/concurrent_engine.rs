use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, Guard};
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use roaring::RoaringBitmap;

use rayon::prelude::*;

use crate::bitmap_fs::BitmapFs;
use crate::filter::FilterFieldType;
use crate::cache;
use crate::concurrency::InFlightTracker;
use crate::config::Config;
use crate::docstore::{DocStore, StoredDoc};
use crate::error::Result;
use crate::executor::{CaseSensitiveFields, QueryExecutor, StringMaps};
use crate::mutation::{diff_document, diff_patch, value_to_bitmap_key, value_to_sort_u32, Document, FieldRegistry, PatchPayload};
use crate::planner;
use crate::query::{BitdexQuery, FilterClause, SortClause};
use crate::time_buckets::TimeBucketManager;
use crate::types::QueryResult;
use crate::unified_cache::{UnifiedCache, UnifiedCacheConfig, UnifiedKey};
use crate::write_coalescer::{MutationOp, MutationSender, WriteCoalescer};

/// Commands sent to the flush thread for state transitions that must
/// go through the single writer. Keeps flush thread as sole ArcSwap writer.
enum FlushCommand {
    /// Force the flush thread to publish its current staging immediately.
    /// Used by `exit_loading_mode()` to guarantee readers see fresh data
    /// before the caller continues (e.g., before save_and_unload).
    ForcePublish {
        /// Oneshot sender — caller blocks on the receiver until publish completes.
        done: crossbeam_channel::Sender<()>,
    },
    /// Replace staging with an unloaded snapshot and publish it.
    /// Used by `save_and_unload()` to ensure the flush thread's private
    /// staging is synced to the unloaded state, preventing re-inflation
    /// on the next publish cycle.
    SyncUnloaded {
        /// The unloaded InnerEngine to replace staging with.
        unloaded: InnerEngine,
        /// Oneshot sender — caller blocks until staging is replaced and published.
        done: crossbeam_channel::Sender<()>,
    },
    /// Combined exit-loading + save + unload in one atomic operation.
    /// Saves bitmaps directly from staging (the single in-memory copy)
    /// without publishing a full intermediate snapshot. This eliminates
    /// the memory spike from `staging.clone()` that doubles bitmap memory
    /// at scale (e.g., 22GB → 38GB at 105M records).
    ///
    /// Flow: drain mutations → merge diffs → save staging to disk →
    /// build unloaded staging → publish unloaded → signal done.
    ExitLoadingSaveUnload {
        /// Sets to skip (already pending lazy loads — not in memory).
        skip_sorts: HashSet<String>,
        skip_filters: HashSet<String>,
        skip_lazy: HashSet<String>,
        /// Cursors to persist alongside bitmaps.
        cursors: HashMap<String, String>,
        /// Dictionaries to persist alongside bitmaps.
        dictionaries: Arc<HashMap<String, crate::dictionary::FieldDictionary>>,
        /// Oneshot sender — caller blocks until save+unload is complete.
        /// Returns Ok(()) on success or error message on failure.
        done: crossbeam_channel::Sender<std::result::Result<(), String>>,
    },
}

/// Lazy-load request sent from query threads to the flush thread.
/// Used during startup restore to load bitmaps on demand per field.
enum LazyLoad {
    FilterField {
        name: String,
        bitmaps: HashMap<u64, RoaringBitmap>,
    },
    /// Per-value lazy load for high-cardinality multi_value fields.
    /// Only the specific queried values are loaded from disk.
    FilterValues {
        field: String,
        values: HashMap<u64, RoaringBitmap>,
    },
    SortField {
        name: String,
        layers: Vec<RoaringBitmap>,
    },
}

/// Inner bitmap state published as immutable snapshots via ArcSwap.
///
/// All fields are Clone via Arc-per-bitmap CoW. Cloning bumps refcounts
/// on the Arc-wrapped bitmaps — zero data copy. Actual bitmap data is
/// only cloned on mutation via `Arc::make_mut()`.
#[derive(Clone)]
pub struct InnerEngine {
    pub slots: crate::slot::SlotAllocator,
    pub filters: crate::filter::FilterIndex,
    pub sorts: crate::sort::SortIndex,
}

/// Thread-safe engine using ArcSwap for lock-free snapshot reads.
///
/// Writers call `put`/`patch`/`delete` which compute diffs and send
/// MutationOps to a channel. A background flush thread applies batched
/// mutations to a private staging copy, then atomically publishes a
/// new snapshot via ArcSwap::store().
///
/// Readers load the current snapshot via `load_full()` — fully lock-free,
/// no contention with writers or the flush thread.
pub struct ConcurrentEngine {
    inner: Arc<ArcSwap<InnerEngine>>,
    sender: MutationSender,
    doc_tx: Sender<(u32, StoredDoc)>,
    docstore: Arc<parking_lot::Mutex<DocStore>>,
    config: Arc<Config>,
    field_registry: FieldRegistry,
    in_flight: InFlightTracker,
    shutdown: Arc<AtomicBool>,
    flush_handle: Option<JoinHandle<()>>,
    merge_handle: Option<JoinHandle<()>>,
    bitmap_store: Option<Arc<BitmapFs>>,
    loading_mode: Arc<AtomicBool>,
    dirty_since_snapshot: Arc<AtomicBool>,
    time_buckets: Option<Arc<parking_lot::Mutex<TimeBucketManager>>>,
    /// Fields not yet loaded from disk (lazy loading on first query).
    pending_filter_loads: Arc<parking_lot::Mutex<HashSet<String>>>,
    pending_sort_loads: Arc<parking_lot::Mutex<HashSet<String>>>,
    /// High-cardinality multi_value fields that use per-value lazy loading.
    /// These are never "fully loaded" — individual values load on demand.
    lazy_value_fields: Arc<parking_lot::Mutex<HashSet<String>>>,
    /// Channel for sending lazy-loaded field data to the flush thread.
    lazy_tx: Sender<LazyLoad>,
    /// Command channel for state transitions (force publish, unload, etc.).
    cmd_tx: Sender<FlushCommand>,
    /// Reverse string maps for MappedString field query resolution.
    string_maps: Option<Arc<StringMaps>>,
    /// Fields where string matching is case-sensitive (default is case-insensitive).
    case_sensitive_fields: Option<Arc<CaseSensitiveFields>>,
    /// Per-field dictionaries for LowCardinalityString fields.
    dictionaries: Arc<HashMap<String, crate::dictionary::FieldDictionary>>,
    /// Unified cache: primary query result cache.
    unified_cache: Arc<parking_lot::Mutex<UnifiedCache>>,
    /// Flush loop stats: total snapshot publishes (monotonic counter).
    flush_publish_count: Arc<AtomicU64>,
    /// Flush loop stats: cumulative flush duration in nanoseconds.
    flush_duration_nanos: Arc<AtomicU64>,
    /// Flush loop stats: most recent flush duration in nanoseconds.
    flush_last_duration_nanos: Arc<AtomicU64>,
    /// Named cursors: opaque key-value pairs persisted at checkpoint time.
    /// Callers (e.g. pg-sync sidecars) use these to track replication progress.
    cursors: Arc<parking_lot::Mutex<HashMap<String, String>>>,
    /// Positive existence sets for per-value lazy loading fields.
    /// Maps field_name → set of all value IDs that exist on disk.
    /// Queries for values NOT in this set skip disk I/O entirely.
    /// Updated by the flush thread when new distinct values appear.
    existing_keys: HashMap<String, Arc<ArcSwap<HashSet<u64>>>>,
    /// Per-value last-accessed flush cycle for idle eviction.
    /// Key: (field_name, value_id). Value: flush cycle when last touched.
    /// Shared between query threads (stamp) and flush thread (sweep).
    eviction_stamps: Arc<DashMap<(Arc<str>, u64), AtomicU64>>,
    /// Global flush cycle counter, incremented by flush thread.
    flush_cycle: Arc<AtomicU64>,
    /// Cumulative eviction counts per field (for Prometheus metrics).
    eviction_total: Arc<DashMap<String, AtomicU64>>,
}

impl ConcurrentEngine {
    /// Create a new concurrent engine with an in-memory docstore (for testing).
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let docstore = DocStore::open_temp()?;
        Self::build(config, docstore)
    }

    /// Create a new concurrent engine with an on-disk docstore.
    pub fn new_with_path(config: Config, path: &Path) -> Result<Self> {
        config.validate()?;
        let docstore = DocStore::open(path)?;
        Self::build(config, docstore)
    }

    fn build(config: Config, docstore: DocStore) -> Result<Self> {
        let mut filters = crate::filter::FilterIndex::new();
        let mut sorts = crate::sort::SortIndex::new();

        // All fields are in-memory (no tier 2 distinction).
        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        let field_registry = FieldRegistry::from_config(&config);

        // Open filesystem bitmap store if configured
        let bitmap_store = if let Some(ref path) = config.storage.bitmap_path {
            Some(Arc::new(BitmapFs::new(path)?))
        } else {
            None
        };

        // Track which fields need lazy loading from disk.
        // Alive + slot counter are always loaded eagerly (tiny, always needed).
        // Filter and sort bitmaps are deferred until first query.
        let mut pending_filter_loads: HashSet<String> = HashSet::new();
        let mut pending_sort_loads: HashSet<String> = HashSet::new();
        // Multi-value fields use per-value lazy loading (never fully loaded).
        let mut lazy_value_fields: HashSet<String> = HashSet::new();

        // Load alive bitmap and slot counter eagerly (small, always needed)
        let mut slots = crate::slot::SlotAllocator::new();
        if let Some(ref store) = bitmap_store {
            let alive = store.load_alive()?;
            let counter = store.load_slot_counter()?;
            if let Some(alive_bm) = alive {
                let counter_val = counter.unwrap_or(0);
                slots = crate::slot::SlotAllocator::from_state(
                    counter_val,
                    alive_bm,
                    RoaringBitmap::new(),
                );

                // Restore deferred alive map if persisted.
                if let Some(deferred) = store.load_deferred_alive()? {
                    if !deferred.is_empty() {
                        let total: usize = deferred.values().map(|v| v.len()).sum();
                        eprintln!("Restored {} deferred alive slots ({} timestamps)", total, deferred.len());
                        slots.set_deferred(deferred);
                    }
                }

                // Only register pending loads if there are actual records to restore.
                // Fields with no saved bitmaps don't need lazy loading.
                if counter_val > 0 {
                    for fc in &config.filter_fields {
                        if fc.field_type == FilterFieldType::MultiValue {
                            // High-cardinality: per-value lazy loading
                            lazy_value_fields.insert(fc.name.clone());
                        } else {
                            // Low-cardinality (single_value, boolean): full-field loading
                            pending_filter_loads.insert(fc.name.clone());
                        }
                    }
                    // Time bucket sort field: load eagerly (needed for bucket rebuild)
                    let tb_sort_field = config.time_buckets.as_ref()
                        .map(|tb| tb.sort_field.clone());

                    for sc in &config.sort_fields {
                        if tb_sort_field.as_deref() == Some(&sc.name) {
                            // Eagerly load the sort field used by time buckets
                            if let Some(ref store) = bitmap_store {
                                if let Ok(Some(layers)) = store.load_sort_layers(&sc.name, sc.bits as usize) {
                                    if !layers.is_empty() {
                                        sorts.add_field(sc.clone());
                                        if let Some(field) = sorts.get_field_mut(&sc.name) {
                                            field.load_layers(layers);
                                        }
                                        eprintln!("Eagerly loaded sort field '{}' for time buckets", sc.name);
                                        continue; // Don't add to pending
                                    }
                                }
                            }
                        }
                        pending_sort_loads.insert(sc.name.clone());
                    }
                }
            }
        }
        let unified_cache = Arc::new(parking_lot::Mutex::new(UnifiedCache::new(
            UnifiedCacheConfig::default(),
        )));
        let loading_mode = Arc::new(AtomicBool::new(false));

        // S3.3: Instantiate TimeBucketManager from top-level time_buckets config
        let time_buckets = config.time_buckets.as_ref().map(|tb_config| {
            let mut tb = TimeBucketManager::new_with_sort_field(
                tb_config.filter_field.clone(),
                tb_config.sort_field.clone(),
                tb_config.range_buckets.clone(),
            );

            // Restore persisted time bucket bitmaps from disk
            if let Some(ref store) = bitmap_store {
                match store.load_time_buckets() {
                    Ok(persisted) if !persisted.is_empty() => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let count = persisted.len();
                        tb.load_persisted(&persisted, now);
                        eprintln!("Restored {count} time bucket bitmaps from disk");
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("Warning: failed to load time buckets: {e}"),
                }
            }

            Arc::new(parking_lot::Mutex::new(tb))
        });

        let inner_engine = InnerEngine {
            slots,
            filters,
            sorts,
        };

        // Flush thread owns a staging clone; readers see published snapshots
        let mut staging = inner_engine.clone();
        let inner = Arc::new(ArcSwap::new(Arc::new(inner_engine)));

        let (mut coalescer, sender) = WriteCoalescer::new(config.channel_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = Arc::new(config);

        // Docstore write channel — bounded for backpressure
        let (doc_tx, doc_rx): (Sender<(u32, StoredDoc)>, Receiver<(u32, StoredDoc)>) =
            crossbeam_channel::bounded(config.channel_capacity);

        let docstore = Arc::new(parking_lot::Mutex::new(docstore));

        // Shared dirty flag: flush thread sets when mutations applied, merge thread
        // clears after persisting snapshot. Prevents continuous 20GB rewrites at idle.
        let dirty_flag = Arc::new(AtomicBool::new(false));

        // Load named cursors from disk (if any exist).
        let initial_cursors = if let Some(ref store) = bitmap_store {
            store.load_all_cursors().unwrap_or_default()
        } else {
            HashMap::new()
        };
        let cursors = Arc::new(parking_lot::Mutex::new(initial_cursors));

        // Lazy load channel: query threads send loaded field data here for staging sync.
        let (lazy_tx, lazy_rx): (Sender<LazyLoad>, Receiver<LazyLoad>) =
            crossbeam_channel::unbounded();

        // Command channel: external threads send state transition commands to flush thread.
        let (cmd_tx, cmd_rx): (Sender<FlushCommand>, Receiver<FlushCommand>) =
            crossbeam_channel::unbounded();

        let pending_filter_loads = Arc::new(parking_lot::Mutex::new(pending_filter_loads));
        let pending_sort_loads = Arc::new(parking_lot::Mutex::new(pending_sort_loads));

        // Build positive existence sets for per-value lazy loading fields.
        // Reads only .fpack headers (no bitmap payloads) — fast even at 31K keys.
        let mut existing_keys: HashMap<String, Arc<ArcSwap<HashSet<u64>>>> = HashMap::new();
        if let Some(ref store) = bitmap_store {
            for field_name in &lazy_value_fields {
                match store.list_field_keys(field_name) {
                    Ok(keys) => {
                        let count = keys.len();
                        if count > 0 {
                            eprintln!(
                                "Existence set for '{}': {} keys",
                                field_name, count
                            );
                        }
                        existing_keys.insert(
                            field_name.clone(),
                            Arc::new(ArcSwap::from_pointee(keys)),
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: failed to build existence set for '{}': {}",
                            field_name, e
                        );
                        existing_keys.insert(
                            field_name.clone(),
                            Arc::new(ArcSwap::from_pointee(HashSet::new())),
                        );
                    }
                }
            }
        }

        // Eviction-enabled fields must always be in lazy_value_fields so that
        // ensure_fields_loaded() can reload values after eviction, even when the
        // engine wasn't restored from disk.
        for fc in &config.filter_fields {
            if fc.eviction.is_some() && fc.field_type == FilterFieldType::MultiValue {
                lazy_value_fields.insert(fc.name.clone());
                // Ensure existence set exists (empty if no bitmap store)
                existing_keys.entry(fc.name.clone()).or_insert_with(|| {
                    Arc::new(ArcSwap::from_pointee(HashSet::new()))
                });
            }
        }

        let lazy_value_fields = Arc::new(parking_lot::Mutex::new(lazy_value_fields));

        // Eviction state
        let eviction_stamps: Arc<DashMap<(Arc<str>, u64), AtomicU64>> = Arc::new(DashMap::new());
        let flush_cycle = Arc::new(AtomicU64::new(0));
        let eviction_total: Arc<DashMap<String, AtomicU64>> = Arc::new(DashMap::new());
        let flush_publish_count = Arc::new(AtomicU64::new(0));
        let flush_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_last_duration_nanos = Arc::new(AtomicU64::new(0));

        let flush_handle = {
            let inner = Arc::clone(&inner);
            let shutdown = Arc::clone(&shutdown);
            let docstore = Arc::clone(&docstore);
            let flush_interval_us = config.flush_interval_us;
            let flush_unified_cache = Arc::clone(&unified_cache);
            let flush_loading_mode = Arc::clone(&loading_mode);
            let flush_dirty_flag = Arc::clone(&dirty_flag);
            let flush_time_buckets = time_buckets.as_ref().map(Arc::clone);
            let flush_pub_count = Arc::clone(&flush_publish_count);
            let flush_dur_nanos = Arc::clone(&flush_duration_nanos);
            let flush_last_dur_nanos = Arc::clone(&flush_last_duration_nanos);
            let flush_existing_keys: HashMap<String, Arc<ArcSwap<HashSet<u64>>>> =
                existing_keys.iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect();
            let flush_eviction_stamps = Arc::clone(&eviction_stamps);
            let flush_eviction_total = Arc::clone(&eviction_total);
            let flush_cycle_clone = Arc::clone(&flush_cycle);
            let flush_bitmap_store = bitmap_store.clone();
            let flush_config = Arc::clone(&config);
            let flush_field_registry = field_registry.clone();
            let eviction_sweep_interval = config.eviction_sweep_interval;
            // Build eviction config map: field_name → idle_seconds
            let eviction_configs: HashMap<String, f64> = config.filter_fields.iter()
                .filter_map(|fc| fc.eviction.as_ref().map(|e| (fc.name.clone(), e.idle_seconds)))
                .collect();

            thread::spawn(move || {
                let min_sleep = Duration::from_micros(flush_interval_us);
                let max_sleep = Duration::from_micros(flush_interval_us * 10);
                let mut current_sleep = min_sleep;
                let mut doc_batch: Vec<(u32, StoredDoc)> = Vec::new();
                let mut was_loading = false;
                let mut staging_dirty = false; // tracks unpublished mutations from loading mode
                let mut flush_cycle: u64 = 0;
                // Compact filter diffs every N flush cycles (~5s at 100μs interval).
                // Keeps diff layers small so apply_diff/fused stay fast.
                const COMPACTION_INTERVAL: u64 = 50;

                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(current_sleep);
                    let is_loading = flush_loading_mode.load(Ordering::Relaxed);

                    // Phase 1: Drain channel and group/sort (no lock, pure CPU work)
                    let bitmap_count = coalescer.prepare();

                    // Phase 1b: Drain lazy load channel — apply loaded fields to staging.
                    // This keeps staging in sync with snapshots published by ensure_loaded().
                    let mut lazy_loaded = false;
                    while let Ok(load) = lazy_rx.try_recv() {
                        match load {
                            LazyLoad::FilterField { name, bitmaps } => {
                                if let Some(field) = staging.filters.get_field_mut(&name) {
                                    field.load_field_complete(bitmaps);
                                }
                            }
                            LazyLoad::FilterValues { field, values } => {
                                if let Some(f) = staging.filters.get_field_mut(&field) {
                                    // For per-value loads, we use load_from since only
                                    // specific requested values are sent. The values in
                                    // the map are all that were requested.
                                    let requested: Vec<u64> = values.keys().copied().collect();
                                    f.load_values(values, &requested);
                                }
                            }
                            LazyLoad::SortField { name, layers } => {
                                if let Some(sf) = staging.sorts.get_field_mut(&name) {
                                    sf.load_layers(layers);
                                    // If time buckets use this sort field, force a rebuild on the
                                    // next periodic check (don't rebuild inline — iterating 100M+
                                    // slots while holding the lock would block queries).
                                    if let Some(ref tb_arc) = flush_time_buckets {
                                        let mut tb = tb_arc.lock();
                                        if tb.sort_field_name() == name {
                                            tb.force_refresh_due();
                                        }
                                    }
                                }
                            }
                        }
                        lazy_loaded = true;
                    }

                    // Phase 2: Apply mutations to staging (private, no lock needed)
                    let flush_start = Instant::now();
                    if bitmap_count > 0 {
                        staging_dirty = true;
                        flush_dirty_flag.store(true, Ordering::Release);
                        coalescer.apply_prepared(
                            &mut staging.slots,
                            &mut staging.filters,
                            &mut staging.sorts,
                        );

                        // Persist deferred map when new deferred entries are added.
                        if coalescer.has_deferred_alive() {
                            if let Some(ref store) = flush_bitmap_store {
                                if let Err(e) = store.write_deferred_alive(staging.slots.deferred_map()) {
                                    eprintln!("Warning: failed to persist deferred alive map: {e}");
                                }
                            }
                        }

                        // Update positive existence sets with any new distinct values.
                        // This is cheap (HashSet insert + Arc swap) and must be visible
                        // to query threads immediately, even during loading mode.
                        if !flush_existing_keys.is_empty() {
                            for (fgk, _slots) in coalescer.filter_insert_entries() {
                                if let Some(ek) = flush_existing_keys.get(fgk.field.as_ref()) {
                                    let current = ek.load();
                                    if !current.contains(&fgk.value) {
                                        let mut updated = (**current).clone();
                                        updated.insert(fgk.value);
                                        ek.store(Arc::new(updated));
                                    }
                                }
                            }
                        }

                        // In loading mode, skip all maintenance and snapshot publishing.
                        // This avoids the expensive staging.clone() → Arc::make_mut clone
                        // cascade that dominates write cost at scale.
                        if !flush_loading_mode.load(Ordering::Relaxed) {
                            // Live maintenance for time buckets: add newly-alive slots to
                            // qualifying buckets, remove deleted slots from all buckets.
                            if let Some(ref tb_arc) = flush_time_buckets {
                                let alive_inserts = coalescer.alive_inserts();
                                let alive_removes = coalescer.alive_removes();
                                if !alive_inserts.is_empty() || !alive_removes.is_empty() {
                                    let now_secs = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs();
                                    let mut tb = tb_arc.lock();
                                    if !alive_inserts.is_empty() {
                                        let sort_field_name = tb.sort_field_name().to_string();
                                        if let Some(sort_field) = staging.sorts.get_field(&sort_field_name) {
                                            for &slot in alive_inserts {
                                                let ts = sort_field.reconstruct_value(slot) as u64;
                                                tb.insert_slot(slot, ts, now_secs);
                                            }
                                        }
                                    }
                                    for &slot in alive_removes {
                                        tb.remove_slot(slot);
                                    }
                                }
                            }

                            // Unified cache live maintenance.
                            // Runs after bitmap mutations are applied to staging.
                            {
                                let mut uc = flush_unified_cache.lock();
                                if !uc.is_empty() {
                                    // Targeted alive removal: remove deleted slots from
                                    // all cache entries without blanket rebuild.
                                    for &slot in coalescer.alive_removes() {
                                        uc.remove_slot_from_all(slot);
                                    }
                                    // Filter maintenance
                                    if !coalescer.mutated_filter_fields().is_empty() {
                                        uc.maintain_filter_changes(
                                            coalescer.filter_insert_entries(),
                                            coalescer.filter_remove_entries(),
                                            &staging.filters,
                                            &staging.sorts,
                                        );
                                    }
                                    // Sort maintenance
                                    let sort_mutations = coalescer.mutated_sort_slots();
                                    if !sort_mutations.is_empty() {
                                        uc.maintain_sort_changes(
                                            &sort_mutations,
                                            &staging.filters,
                                            &staging.sorts,
                                        );
                                    }
                                    // Reconcile tracked byte total after in-place mutations
                                    uc.reconcile_bytes();
                                }
                            }

                            // Periodic filter diff compaction: merge dirty diffs into
                            // bases so apply_diff/fused don't accumulate unbounded diffs.
                            // Runs every COMPACTION_INTERVAL flush cycles (~5s).
                            // Sort diffs and alive are already merged eagerly in WriteBatch::apply().
                            if flush_cycle % COMPACTION_INTERVAL == 0 {
                                for (_name, field) in staging.filters.fields_mut() {
                                    field.merge_dirty();
                                }
                            }
                            flush_cycle += 1;
                            flush_cycle_clone.store(flush_cycle, Ordering::Relaxed);

                            // Publish new snapshot atomically (Arc-per-bitmap CoW clone)
                            inner.store(Arc::new(staging.clone()));
                            staging_dirty = false;

                            // Record flush stats for Prometheus
                            let flush_elapsed = flush_start.elapsed().as_nanos() as u64;
                            flush_pub_count.fetch_add(1, Ordering::Relaxed);
                            flush_dur_nanos.fetch_add(flush_elapsed, Ordering::Relaxed);
                            flush_last_dur_nanos.store(flush_elapsed, Ordering::Relaxed);
                        }
                    }

                    // Activate deferred alive slots whose time has come.
                    // Runs every flush cycle regardless of write activity for sub-second
                    // activation precision. On activation: read stored doc from docstore,
                    // replay the full mutation pipeline (filter/sort/alive ops) as if the
                    // document was just PUT for the first time. This ensures the document
                    // only becomes visible in bitmaps at activation time.
                    if staging.slots.deferred_count() > 0 {
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let activated = staging.slots.activate_due(now_unix);
                        if !activated.is_empty() {
                            // Collect all mutation ops for activated slots into a WriteBatch,
                            // then apply in bulk (same path as normal mutations).
                            let mut activation_batch = crate::write_coalescer::WriteBatch::new();
                            {
                                let ds = docstore.lock();
                                for &slot in &activated {
                                    match ds.get(slot) {
                                        Ok(Some(stored_doc)) => {
                                            let doc = crate::mutation::Document {
                                                fields: stored_doc.fields.clone(),
                                            };
                                            let ops = crate::mutation::diff_document(
                                                slot,
                                                None, // fresh insert — no old doc
                                                &doc,
                                                &flush_config,
                                                false, // not upsert
                                                &flush_field_registry,
                                            );
                                            activation_batch.push_ops(ops);
                                        }
                                        Ok(None) => {
                                            eprintln!("Warning: deferred slot {} has no stored doc, setting alive only", slot);
                                            activation_batch.push_ops(vec![
                                                MutationOp::AliveInsert { slots: vec![slot] },
                                            ]);
                                        }
                                        Err(e) => {
                                            eprintln!("Warning: failed to read deferred slot {}: {e}, setting alive only", slot);
                                            activation_batch.push_ops(vec![
                                                MutationOp::AliveInsert { slots: vec![slot] },
                                            ]);
                                        }
                                    }
                                }
                            } // docstore lock released
                            activation_batch.group_and_sort();
                            activation_batch.apply(
                                &mut staging.slots,
                                &mut staging.filters,
                                &mut staging.sorts,
                            );
                            staging_dirty = true;

                            // Persist the deferred map AFTER activation so the activated
                            // entries are already removed. On crash before persist, the
                            // old map is re-read and those slots get re-activated (idempotent).
                            if let Some(ref store) = flush_bitmap_store {
                                if let Err(e) = store.write_deferred_alive(staging.slots.deferred_map()) {
                                    eprintln!("Warning: failed to persist deferred alive map: {e}");
                                }
                            }
                        }
                    }

                    // Loading mode exit: force-publish if staging has unpublished mutations
                    if was_loading && !is_loading && staging_dirty {
                        // Compact all filter diffs accumulated during loading
                        for (_name, field) in staging.filters.fields_mut() {
                            field.merge_dirty();
                        }
                        // Invalidate unified cache — may be stale from the loading period
                        flush_unified_cache.lock().clear();
                        inner.store(Arc::new(staging.clone()));
                        staging_dirty = false;
                    }
                    was_loading = is_loading;

                    // Process flush commands (force publish, unload, etc.)
                    while let Ok(cmd) = cmd_rx.try_recv() {
                        match cmd {
                            FlushCommand::ForcePublish { done } => {
                                // Drain any remaining mutations from the channel
                                // before publishing — they may not have been picked
                                // up by the regular prepare() at the top of the loop.
                                let extra = coalescer.flush(
                                    &mut staging.slots,
                                    &mut staging.filters,
                                    &mut staging.sorts,
                                );
                                if extra > 0 {
                                    staging_dirty = true;
                                }
                                // Compact diffs before publishing
                                for (_name, field) in staging.filters.fields_mut() {
                                    field.merge_dirty();
                                }
                                flush_unified_cache.lock().clear();
                                inner.store(Arc::new(staging.clone()));
                                staging_dirty = false;
                                // Signal caller that publish is complete
                                let _ = done.send(());
                            }
                            FlushCommand::SyncUnloaded { unloaded, done } => {
                                // Drain any mutations that arrived between the save
                                // snapshot and now. prepare() drains + groups without
                                // applying, so we can swap staging first.
                                let pending = coalescer.prepare();
                                // Replace staging with the unloaded version.
                                staging = unloaded;
                                // Apply drained mutations to the new unloaded staging.
                                // These go into diff layers (bases are empty/unloaded),
                                // which is correct — they'll merge on lazy reload.
                                if pending > 0 {
                                    coalescer.apply_prepared(
                                        &mut staging.slots,
                                        &mut staging.filters,
                                        &mut staging.sorts,
                                    );
                                }
                                flush_unified_cache.lock().clear();
                                inner.store(Arc::new(staging.clone()));
                                staging_dirty = false;
                                let _ = done.send(());
                            }
                            FlushCommand::ExitLoadingSaveUnload {
                                skip_sorts, skip_filters, skip_lazy,
                                cursors, dictionaries, done,
                            } => {
                                // Combined exit-loading + save + unload.
                                //
                                // The NDJSON loader builds bitmaps in its own staging and
                                // publishes directly to ArcSwap via publish_staging(). The
                                // flush thread's private staging is therefore empty. We load
                                // the published snapshot from ArcSwap (just an Arc clone —
                                // no deep copy) and save from that. Then we build a tiny
                                // unloaded snapshot and publish it, releasing the full data.
                                //
                                // Memory profile: at no point do two full copies exist.
                                // The Arc<InnerEngine> from load_full() shares bitmaps with
                                // the published snapshot. After we publish the unloaded
                                // version, readers drop the old Arc and memory is freed.
                                eprintln!("  flush: ExitLoadingSaveUnload starting");

                                // 1. Load the published snapshot (loader already published here)
                                let published = inner.load_full();

                                // 2. Save from the published snapshot — no clone, just a borrow
                                if let Some(ref store) = flush_bitmap_store {
                                    let save_result = ConcurrentEngine::write_inner_to_store(
                                        store,
                                        &published,
                                        &flush_config,
                                        &skip_sorts,
                                        &skip_filters,
                                        &skip_lazy,
                                    );
                                    if let Err(e) = save_result {
                                        let _ = done.send(Err(format!("save failed: {e}")));
                                        continue;
                                    }

                                    // Persist cursors
                                    for (name, value) in &cursors {
                                        if let Err(e) = store.write_cursor(name, value) {
                                            eprintln!("Warning: failed to persist cursor '{}': {}", name, e);
                                        }
                                    }

                                    // Persist dictionaries
                                    if !dictionaries.is_empty() {
                                        let dict_dir = store.root_path().join("dictionaries");
                                        for (name, dict) in dictionaries.iter() {
                                            let snap = dict.snapshot();
                                            let path = dict_dir.join(format!("{}.dict", name));
                                            if let Err(e) = crate::dictionary::save_dictionary(&snap, &path) {
                                                eprintln!("Warning: failed to persist dictionary '{}': {}", name, e);
                                            }
                                        }
                                    }
                                }

                                // 3. Build unloaded staging — reuse field configs, clear bitmaps
                                let slots = published.slots.clone();
                                let mut new_filters = crate::filter::FilterIndex::new();
                                for fc in &flush_config.filter_fields {
                                    new_filters.add_field(fc.clone());
                                }
                                for fc in &flush_config.filter_fields {
                                    if skip_filters.contains(&fc.name) {
                                        new_filters.copy_field_arc_from(&published.filters, &fc.name);
                                    } else {
                                        new_filters.unload_from(&published.filters, &fc.name);
                                    }
                                }
                                let mut new_sorts = crate::sort::SortIndex::new();
                                for sc in &flush_config.sort_fields {
                                    new_sorts.add_field(sc.clone());
                                }
                                for sc in &flush_config.sort_fields {
                                    if skip_sorts.contains(&sc.name) {
                                        new_sorts.copy_field_arc_from(&published.sorts, &sc.name);
                                    } else {
                                        new_sorts.unload_from(&published.sorts, &sc.name);
                                    }
                                }

                                // 4. Drop the published snapshot reference before publishing
                                //    the unloaded version. This ensures only one full copy
                                //    exists when readers switch to the unloaded snapshot.
                                drop(published);

                                // 5. Replace staging and publish the unloaded version
                                staging = InnerEngine {
                                    slots,
                                    filters: new_filters,
                                    sorts: new_sorts,
                                };
                                flush_unified_cache.lock().clear();
                                inner.store(Arc::new(staging.clone()));
                                staging_dirty = false;

                                eprintln!("  flush: ExitLoadingSaveUnload complete");
                                let _ = done.send(Ok(()));
                            }
                        }
                    }

                    // --- Idle eviction sweep (wall-clock based) ---
                    // Runs every eviction_sweep_interval flush cycles. Stamps are
                    // wall-clock millis set by query threads on read, so values stay
                    // alive as long as they're being queried — independent of write
                    // activity.
                    if !is_loading && !eviction_configs.is_empty()
                        && flush_cycle > 0 && flush_cycle % eviction_sweep_interval == 0
                    {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        let mut any_evicted = false;
                        for (field_name, idle_seconds) in &eviction_configs {
                            let idle_ms = (*idle_seconds * 1000.0) as u64;
                            let cutoff_ms = now_ms.saturating_sub(idle_ms);

                            // Collect values to evict
                            let field = match staging.filters.get_field(field_name) {
                                Some(f) => f,
                                None => continue,
                            };
                            let field_name_arc: Arc<str> = Arc::from(field_name.as_str());

                            let to_evict: Vec<u64> = field.bitmap_keys()
                                .filter(|&value| {
                                    // Skip dirty bitmaps (unpersisted mutations)
                                    if let Some(vb) = field.get_versioned(*value) {
                                        if vb.is_dirty() {
                                            return false;
                                        }
                                    }
                                    // Check stamp (wall-clock millis)
                                    let key = (field_name_arc.clone(), *value);
                                    flush_eviction_stamps
                                        .get(&key)
                                        .map(|entry| entry.value().load(Ordering::Relaxed) < cutoff_ms)
                                        .unwrap_or(true) // no stamp = never touched = evict
                                })
                                .copied()
                                .collect();

                            if !to_evict.is_empty() {
                                let count = to_evict.len();
                                if let Some(field_mut) = staging.filters.get_field_mut(field_name) {
                                    for value in &to_evict {
                                        field_mut.remove_value(*value);
                                        flush_eviction_stamps.remove(
                                            &(field_name_arc.clone(), *value),
                                        );
                                    }
                                }
                                // Update eviction counter
                                flush_eviction_total
                                    .entry(field_name.clone())
                                    .or_insert_with(|| AtomicU64::new(0))
                                    .fetch_add(count as u64, Ordering::Relaxed);

                                eprintln!(
                                    "Evicted {} idle values from filter '{}' (idle_ms={}, now={})",
                                    count, field_name, idle_ms, now_ms
                                );
                                any_evicted = true;
                            }
                        }

                        if any_evicted {
                            // Publish snapshot without evicted values
                            inner.store(Arc::new(staging.clone()));
                        }
                    }

                    // Publish if lazy loads updated staging but no mutations triggered a publish.
                    // This ensures staging stays consistent with the snapshot published by
                    // ensure_loaded() on the query thread. Skipped during loading mode:
                    // staging.clone() triggers Arc refcount cascade that kills write throughput.
                    // Queries during loading are expected to see stale data anyway.
                    if lazy_loaded && bitmap_count == 0 && !is_loading {
                        inner.store(Arc::new(staging.clone()));
                    }

                    // Periodic time bucket refresh: runs independently of mutations since
                    // bucket validity is time-based (e.g., items age out of the 24h window).
                    // Must run even at idle to keep buckets fresh after restore from disk.
                    //
                    // Lock strategy: brief lock to check what's due + get config, then release
                    // lock while iterating 100M+ slots to build bitmaps, then brief lock to swap.
                    if !is_loading {
                        if let Some(ref tb_arc) = flush_time_buckets {
                            let now_secs = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();

                            // Brief lock: check which buckets need refresh and get their durations
                            let rebuild_info: Vec<(String, u64)> = {
                                let tb = tb_arc.lock();
                                let due = tb.refresh_due(now_secs);
                                if due.is_empty() {
                                    Vec::new()
                                } else {
                                    due.iter()
                                        .filter_map(|name| {
                                            tb.get_bucket(name).map(|b| (name.to_string(), b.duration_secs))
                                        })
                                        .collect()
                                }
                            }; // lock released

                            if !rebuild_info.is_empty() {
                                let tb_lock = tb_arc.lock();
                                let sort_field_name = tb_lock.sort_field_name().to_string();
                                let field_name = tb_lock.field_name().to_string();
                                drop(tb_lock); // release before heavy work

                                if let Some(sort_field) = staging.sorts.get_field(&sort_field_name) {
                                    let alive = staging.slots.alive_bitmap();
                                    let start = std::time::Instant::now();

                                    // Single pass: compute cutoffs, build all bitmaps simultaneously
                                    let cutoffs: Vec<u64> = rebuild_info.iter()
                                        .map(|(_, dur)| now_secs.saturating_sub(*dur))
                                        .collect();
                                    let mut bitmaps: Vec<roaring::RoaringBitmap> = (0..rebuild_info.len())
                                        .map(|_| roaring::RoaringBitmap::new())
                                        .collect();

                                    for slot in alive.iter() {
                                        let ts = sort_field.reconstruct_value(slot) as u64;
                                        if ts <= now_secs {
                                            for (i, cutoff) in cutoffs.iter().enumerate() {
                                                if ts >= *cutoff {
                                                    bitmaps[i].insert(slot);
                                                }
                                            }
                                        }
                                    }

                                    let _tb_elapsed = start.elapsed();

                                    // Brief lock: capture old bitmaps, swap in new ones
                                    let mut bucket_diffs: Vec<(String, RoaringBitmap, RoaringBitmap)> = Vec::new();
                                    {
                                        let mut tb = tb_arc.lock();
                                        for (i, (bucket_name, _)) in rebuild_info.iter().enumerate() {
                                            // Capture old bitmap for diff computation
                                            let old_bm = tb.get_bucket(bucket_name)
                                                .map(|b| b.bitmap().clone())
                                                .unwrap_or_default();
                                            let new_bm = &bitmaps[i];
                                            let dropped = &old_bm - new_bm;
                                            let added = new_bm - &old_bm;
                                            if !dropped.is_empty() || !added.is_empty() {
                                                bucket_diffs.push((bucket_name.clone(), dropped, added));
                                            }
                                            tb.rebuild_bucket_from_bitmap(
                                                bucket_name,
                                                std::mem::take(&mut bitmaps[i]),
                                                now_secs,
                                            );
                                        }
                                    }
                                    // Mark dirty so merge thread persists time buckets
                                    flush_dirty_flag.store(true, Ordering::Release);

                                    // Push bucket diffs to unified cache
                                    if !bucket_diffs.is_empty() {
                                        let mut uc = flush_unified_cache.lock();
                                        if !uc.is_empty() {
                                            for (bucket_name, dropped, added) in &bucket_diffs {
                                                uc.maintain_bucket_changes(
                                                    &field_name,
                                                    bucket_name,
                                                    dropped,
                                                    added,
                                                    &staging.filters,
                                                    &staging.sorts,
                                                );
                                            }
                                        }
                                    }
                                } else {
                                    eprintln!("Time bucket: sort field '{}' not found in staging", sort_field_name);
                                }
                            }
                        }
                    }

                    // Phase 3: Drain docstore channel and batch write
                    doc_batch.clear();
                    while let Ok(item) = doc_rx.try_recv() {
                        doc_batch.push(item);
                    }
                    let doc_count = doc_batch.len();
                    if doc_count > 0 {
                        if let Err(e) = docstore.lock().put_batch(&doc_batch) {
                            panic!("docstore batch write failed: {e}");
                        }
                    }

                    if bitmap_count > 0 || doc_count > 0 || lazy_loaded {
                        current_sleep = min_sleep;
                    } else {
                        current_sleep = (current_sleep * 2).min(max_sleep);
                    }
                }

                // Final flush on shutdown
                let count = coalescer.prepare();
                if count > 0 {
                    flush_dirty_flag.store(true, Ordering::Release);
                    coalescer.apply_prepared(
                        &mut staging.slots,
                        &mut staging.filters,
                        &mut staging.sorts,
                    );

                    // Compact all remaining filter diffs before final publish
                    for (_name, field) in staging.filters.fields_mut() {
                        field.merge_dirty();
                    }

                    inner.store(Arc::new(staging.clone()));
                }

                // Final docstore drain
                doc_batch.clear();
                while let Ok(item) = doc_rx.try_recv() {
                    doc_batch.push(item);
                }
                if !doc_batch.is_empty() {
                    if let Err(e) = docstore.lock().put_batch(&doc_batch) {
                        panic!("docstore final batch write failed: {e}");
                    }
                }
            })
        };

        let merge_handle = {
            let shutdown = Arc::clone(&shutdown);
            let merge_inner = Arc::clone(&inner);
            let merge_interval_ms = config.merge_interval_ms;
            let merge_bitmap_store = bitmap_store.clone();
            let merge_dirty_flag = Arc::clone(&dirty_flag);
            let sort_field_configs: Vec<crate::config::SortFieldConfig> =
                config.sort_fields.clone();
            let merge_pending_sorts = Arc::clone(&pending_sort_loads);
            let merge_pending_filters = Arc::clone(&pending_filter_loads);
            let merge_lazy_values = Arc::clone(&lazy_value_fields);
            let merge_time_buckets = time_buckets.as_ref().map(Arc::clone);
            let merge_cursors = Arc::clone(&cursors);

            thread::spawn(move || {
                let sleep_duration = Duration::from_millis(merge_interval_ms);
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(sleep_duration);

                    // Snapshot, compact filter diffs, persist to filesystem
                    // Only write if bitmaps have changed since last snapshot.
                    let needs_write = merge_dirty_flag.swap(false, Ordering::AcqRel);
                    if needs_write {
                    if let Some(ref store) = merge_bitmap_store {
                        let snap = merge_inner.load_full();
                        let mut compacted = (*snap).clone();

                        // Only persist fields that are (a) loaded and (b) dirty.
                        // Pending lazy-load fields are empty placeholders — writing
                        // them would overwrite real data on disk. Clean fields don't
                        // need rewriting.
                        let pending_s = merge_pending_sorts.lock().clone();
                        let pending_f = merge_pending_filters.lock().clone();
                        let lazy_v = merge_lazy_values.lock().clone();

                        // Collect filter bitmap entries — all loaded fields.
                        // Note: we don't check has_dirty() per-field because the flush
                        // thread's periodic compaction (merge_dirty) clears per-field
                        // dirty flags before the merge thread runs, creating a race.
                        // The dirty_flag AtomicBool gates the write at the top level.
                        let mut filter_entries: Vec<(String, u64, RoaringBitmap)> = Vec::new();
                        for (name, field) in compacted.filters.fields_mut() {
                            if pending_f.contains(name) || lazy_v.contains(name) {
                                continue;
                            }
                            field.merge_dirty();
                            for (&value, vb) in field.iter_versioned() {
                                filter_entries.push((
                                    name.clone(),
                                    value,
                                    vb.base().as_ref().clone(),
                                ));
                            }
                        }

                        // Collect sort layer bases — all loaded fields
                        let mut sort_data: Vec<(String, Vec<RoaringBitmap>)> = Vec::new();
                        for sc in &sort_field_configs {
                            if pending_s.contains(&sc.name) {
                                continue;
                            }
                            if let Some(sf) = compacted.sorts.get_field_mut(&sc.name) {
                                sf.merge_dirty();
                                let bases: Vec<RoaringBitmap> = sf
                                    .layer_bases()
                                    .iter()
                                    .map(|b| (*b).clone())
                                    .collect();
                                sort_data.push((sc.name.clone(), bases));
                            }
                        }

                        let filter_refs: Vec<(&str, u64, &RoaringBitmap)> = filter_entries
                            .iter()
                            .map(|(f, v, b)| (f.as_str(), *v, b))
                            .collect();
                        let alive = compacted.slots.alive_bitmap().clone();
                        let slot_counter = compacted.slots.slot_counter();

                        let sort_owned_refs: Vec<(String, Vec<&RoaringBitmap>)> = sort_data
                            .iter()
                            .map(|(name, layers)| {
                                (name.clone(), layers.iter().collect::<Vec<&RoaringBitmap>>())
                            })
                            .collect();
                        let sort_slice_refs: Vec<(&str, &[&RoaringBitmap])> = sort_owned_refs
                            .iter()
                            .map(|(name, refs)| (name.as_str(), refs.as_slice()))
                            .collect();

                        if let Err(e) = store.write_full_snapshot(
                            &filter_refs,
                            &alive,
                            &sort_slice_refs,
                            slot_counter,
                        ) {
                            eprintln!("merge thread: bitmap snapshot write failed: {e}");
                        }

                        // Persist time bucket bitmaps alongside filter/sort data
                        if let Some(ref tb_arc) = merge_time_buckets {
                            let tb = tb_arc.lock();
                            for (name, bitmap) in tb.all_buckets() {
                                if !bitmap.is_empty() {
                                    if let Err(e) = store.write_time_bucket(name, bitmap) {
                                        eprintln!("merge thread: time bucket write failed: {e}");
                                    }
                                }
                            }
                        }

                        // Persist named cursors alongside bitmap data
                        {
                            let cursor_snapshot = merge_cursors.lock().clone();
                            for (name, value) in &cursor_snapshot {
                                if let Err(e) = store.write_cursor(name, value) {
                                    eprintln!("merge thread: cursor write failed for {name}: {e}");
                                }
                            }
                        }
                    }
                    } // needs_write
                }
            })
        };

        Ok(Self {
            inner,
            sender,
            doc_tx,
            docstore,
            config,
            field_registry,
            in_flight: InFlightTracker::new(),
            shutdown,
            flush_handle: Some(flush_handle),
            merge_handle: Some(merge_handle),
            bitmap_store,
            loading_mode,
            dirty_since_snapshot: Arc::clone(&dirty_flag),
            time_buckets,
            pending_filter_loads,
            pending_sort_loads,
            lazy_value_fields,
            lazy_tx,
            cmd_tx,
            string_maps: None,
            case_sensitive_fields: None,
            dictionaries: Arc::new(HashMap::new()),
            unified_cache,
            flush_publish_count,
            flush_duration_nanos,
            flush_last_duration_nanos,
            cursors,
            existing_keys,
            eviction_stamps,
            flush_cycle,
            eviction_total,
        })
    }

    /// Set the string maps for MappedString field query resolution.
    /// Call after creating the engine with schema data that includes string_map entries.
    pub fn set_string_maps(&mut self, maps: StringMaps) {
        self.string_maps = Some(Arc::new(maps));
    }

    /// Set the case-sensitive fields for string matching control.
    pub fn set_case_sensitive_fields(&mut self, fields: CaseSensitiveFields) {
        self.case_sensitive_fields = Some(Arc::new(fields));
    }

    /// Set the per-field dictionaries for LowCardinalityString fields.
    pub fn set_dictionaries(&mut self, dicts: HashMap<String, crate::dictionary::FieldDictionary>) {
        self.dictionaries = Arc::new(dicts);
    }

    /// Get a reference to the dictionaries (for loader and upsert paths).
    pub fn dictionaries(&self) -> &HashMap<String, crate::dictionary::FieldDictionary> {
        &self.dictionaries
    }

    /// Get a cloneable Arc to the dictionaries (for passing into threads).
    pub fn dictionaries_arc(&self) -> Arc<HashMap<String, crate::dictionary::FieldDictionary>> {
        Arc::clone(&self.dictionaries)
    }

    /// Save all dictionaries to disk in the given directory.
    pub fn save_dictionaries(&self, dir: &std::path::Path) -> Result<()> {
        let dict_dir = dir.join("dictionaries");
        for (name, dict) in self.dictionaries.iter() {
            let snap = dict.snapshot();
            let path = dict_dir.join(format!("{}.dict", name));
            crate::dictionary::save_dictionary(&snap, &path)
                .map_err(|e| crate::error::BitdexError::Config(e))?;
        }
        Ok(())
    }

    /// Persist dirty dictionaries to disk. Call after upserts that may have
    /// created new LowCardinalityString values. Only writes dictionaries that
    /// have new entries since the last persist, and clears their dirty flags.
    ///
    /// This ensures dictionary mappings survive crashes even before the next
    /// full `save_snapshot()`. Dictionaries are small (typically < 1 KB), so
    /// the I/O cost is negligible.
    pub fn persist_dirty_dictionaries(&self) -> Result<()> {
        if self.dictionaries.is_empty() {
            return Ok(());
        }
        let store = match self.bitmap_store.as_ref() {
            Some(s) => s,
            None => return Ok(()), // no persistence configured
        };
        let dict_dir = store.root_path().join("dictionaries");
        for (name, dict) in self.dictionaries.iter() {
            if dict.is_dirty() {
                let snap = dict.snapshot();
                let path = dict_dir.join(format!("{}.dict", name));
                crate::dictionary::save_dictionary(&snap, &path)
                    .map_err(|e| crate::error::BitdexError::Config(e))?;
                dict.clear_dirty();
            }
        }
        Ok(())
    }

    /// Load dictionaries from disk for all LowCardinalityString fields in the schema.
    pub fn load_dictionaries(
        schema: &crate::config::DataSchema,
        dir: &std::path::Path,
    ) -> Result<HashMap<String, crate::dictionary::FieldDictionary>> {
        let dict_dir = dir.join("dictionaries");
        let mut dicts = HashMap::new();
        for mapping in &schema.fields {
            if mapping.value_type == crate::config::FieldValueType::LowCardinalityString {
                let path = dict_dir.join(format!("{}.dict", mapping.target));
                match crate::dictionary::load_dictionary(&path) {
                    Ok(Some(snap)) => {
                        dicts.insert(
                            mapping.target.clone(),
                            crate::dictionary::FieldDictionary::from_snapshot(&snap),
                        );
                    }
                    Ok(None) => {
                        // No persisted dictionary — create empty
                        dicts.insert(
                            mapping.target.clone(),
                            crate::dictionary::FieldDictionary::new(),
                        );
                    }
                    Err(e) => {
                        return Err(crate::error::BitdexError::Config(
                            format!("Failed to load dictionary for '{}': {}", mapping.target, e),
                        ));
                    }
                }
            }
        }
        Ok(dicts)
    }

    /// Load the current snapshot (lock-free, zero refcount ops).
    ///
    /// Returns a Guard that derefs to Arc<InnerEngine>. Unlike `load_full()`,
    /// this avoids atomic refcount increment/decrement and moves deallocation
    /// of old snapshots off the reader path onto the flush thread's `store()`.
    fn snapshot(&self) -> Guard<Arc<InnerEngine>> {
        self.inner.load()
    }

    /// PUT(id, document) -- full replace with upsert semantics.
    ///
    /// 1. Mark in-flight
    /// 2. Check alive status (lock-free snapshot)
    /// 3. Read old doc from docstore if upsert
    /// 4. Diff old vs new -> MutationOps
    /// 5. Send ops to coalescer channel
    /// 6. Enqueue doc write to docstore channel (flush thread batches these)
    /// 7. Clear in-flight
    pub fn put(&self, id: u32, doc: &Document) -> Result<()> {
        self.in_flight.mark_in_flight(id);

        let result = (|| -> Result<()> {
            // Check alive status via lock-free snapshot
            let (is_upsert, was_allocated) = {
                let snap = self.snapshot();
                let alive = snap.slots.is_alive(id);
                let alloc = if !alive {
                    snap.slots.was_ever_allocated(id)
                } else {
                    false
                };
                (alive, alloc)
            };

            // Read old doc from docstore if needed
            let old_doc = if is_upsert || was_allocated {
                self.docstore.lock().get(id)?
            } else {
                None
            };

            // Compute diff purely -> Vec<MutationOp>
            let ops = diff_document(id, old_doc.as_ref(), doc, &self.config, is_upsert, &self.field_registry);

            // Send ops to coalescer channel
            self.sender.send_batch(ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;

            // Enqueue doc write — flush thread will batch these
            let stored = StoredDoc {
                fields: doc.fields.clone(),
                schema_version: 0,
            };
            self.doc_tx.send((id, stored)).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "docstore channel disconnected".to_string(),
                )
            })?;

            Ok(())
        })();

        self.in_flight.clear_in_flight(id);
        result
    }

    /// PATCH(id, partial_fields) -- merge only provided fields.
    pub fn patch(&self, id: u32, patch: &PatchPayload) -> Result<()> {
        self.in_flight.mark_in_flight(id);

        let result = (|| -> Result<()> {
            // Verify the slot is alive via lock-free snapshot
            {
                let snap = self.snapshot();
                if !snap.slots.is_alive(id) {
                    return Err(crate::error::BitdexError::SlotNotFound(id));
                }
            }

            let ops = diff_patch(id, patch, &self.config, &self.field_registry);

            self.sender.send_batch(ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;

            Ok(())
        })();

        self.in_flight.clear_in_flight(id);
        result
    }

    /// DELETE(id) -- clean delete: clear filter/sort bitmaps then alive bit.
    ///
    /// Reads the doc from the docstore to determine exactly which filter and sort
    /// bitmaps need clearing. This makes filter bitmaps always clean (no stale bits),
    /// eliminating the alive AND from the query hot path.
    pub fn delete(&self, id: u32) -> Result<()> {
        self.in_flight.mark_in_flight(id);

        let result = (|| -> Result<()> {
            // Read the doc to know which bitmaps to clear
            let old_doc = self.docstore.lock().get(id)?;

            let mut ops = Vec::new();

            // Generate filter/sort cleanup ops from the stored doc
            if let Some(doc) = &old_doc {
                for fc in &self.config.filter_fields {
                    if let Some(val) = doc.fields.get(&fc.name) {
                        let arc_name = self.field_registry.get(&fc.name);
                        crate::mutation::collect_filter_remove_ops(&mut ops, &arc_name, id, val);
                    }
                }
                for sc in &self.config.sort_fields {
                    if let Some(val) = doc.fields.get(&sc.name) {
                        if let crate::mutation::FieldValue::Single(v) = val {
                            if let Some(sort_val) = crate::mutation::value_to_sort_u32(v) {
                                let arc_name = self.field_registry.get(&sc.name);
                                let num_bits = sc.bits as usize;
                                for bit in 0..num_bits {
                                    if (sort_val >> bit) & 1 == 1 {
                                        ops.push(MutationOp::SortClear {
                                            field: arc_name.clone(),
                                            bit_layer: bit,
                                            slots: vec![id],
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Clear the alive bit last
            ops.push(MutationOp::AliveRemove { slots: vec![id] });

            self.sender.send_batch(ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;
            Ok(())
        })();

        self.in_flight.clear_in_flight(id);
        result
    }

    /// Execute a query from individual filter/sort/limit components.
    pub fn query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<QueryResult> {
        // Lazy-load any fields not yet loaded from disk
        self.ensure_fields_loaded(filters, sort.map(|s| s.field.as_str()))?;

        let snap = self.snapshot(); // lock-free
        let tb_guard = self.time_buckets.as_ref().map(|tb| tb.lock());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let executor = {
            let mut base = QueryExecutor::new(
                &snap.slots,
                &snap.filters,
                &snap.sorts,
                self.config.max_page_size,
            );
            if let Some(ref maps) = self.string_maps {
                base = base.with_string_maps(maps);
            }
            if let Some(ref cs) = self.case_sensitive_fields {
                base = base.with_case_sensitive_fields(cs);
            }
            if !self.dictionaries.is_empty() {
                base = base.with_dictionaries(&self.dictionaries);
            }
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };

        let (filter_arc, use_simple_sort) =
            self.resolve_filters(&executor, filters, tb_guard.as_deref(), now_unix)?;

        let mut result =
            executor.execute_from_bitmap(&filter_arc, sort, limit, None, use_simple_sort)?;

        // Post-validation against in-flight writes
        self.post_validate(&mut result, filters, &executor)?;

        Ok(result)
    }

    /// Ensure all fields referenced by the query are loaded from disk.
    ///
    /// On startup with lazy loading, filter/sort bitmaps are not loaded until
    /// the first query touches them. This method handles two strategies:
    /// - **Full-field loading** for low-cardinality fields (single_value, boolean)
    /// - **Per-value loading** for high-cardinality multi_value fields (e.g. tagIds)
    ///
    /// Fast path: if no loads are pending and no lazy value fields exist, just returns.
    fn ensure_fields_loaded(
        &self,
        filters: &[FilterClause],
        sort_field: Option<&str>,
    ) -> Result<()> {
        // Fast path: check if any loads are pending at all
        let has_lazy_values = !self.lazy_value_fields.lock().is_empty();
        {
            let pf = self.pending_filter_loads.lock();
            let ps = self.pending_sort_loads.lock();
            if pf.is_empty() && ps.is_empty() && !has_lazy_values {
                return Ok(());
            }
        }

        // --- Full-field loading (single_value, boolean) ---
        let mut needed_filters: Vec<String> = Vec::new();
        let mut needed_sort: Option<String> = None;

        {
            let pf = self.pending_filter_loads.lock();
            for clause in filters {
                Self::collect_filter_fields(clause, &pf, &mut needed_filters);
            }
        }
        if let Some(sort_name) = sort_field {
            let ps = self.pending_sort_loads.lock();
            if ps.contains(sort_name) {
                needed_sort = Some(sort_name.to_string());
            }
        }

        // --- Per-value loading (multi_value) ---
        let mut needed_values: HashMap<String, Vec<u64>> = HashMap::new();
        if has_lazy_values {
            let lvf = self.lazy_value_fields.lock();
            for clause in filters {
                Self::collect_lazy_values(clause, &lvf, &mut needed_values);
            }
        }

        // Stamp accessed values for idle eviction tracking (wall-clock millis).
        // This runs for ALL queried values (already-loaded and new), ensuring
        // that reads keep values alive independent of write activity.
        if !needed_values.is_empty() {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for (field_name, values) in &needed_values {
                // Only stamp eviction-enabled fields
                if self.config.filter_fields.iter()
                    .any(|fc| fc.name == *field_name && fc.eviction.is_some())
                {
                    let field_arc: Arc<str> = Arc::from(field_name.as_str());
                    for &value in values {
                        self.eviction_stamps
                            .entry((field_arc.clone(), value))
                            .or_insert_with(|| AtomicU64::new(now_ms))
                            .store(now_ms, Ordering::Relaxed);
                    }
                }
            }
        }

        if needed_filters.is_empty() && needed_sort.is_none() && needed_values.is_empty() {
            return Ok(());
        }

        // Load from BitmapFs
        let store = match self.bitmap_store.as_ref() {
            Some(s) => s,
            None => return Ok(()), // no store, nothing to load
        };

        // Do all expensive disk I/O outside the rcu closure, collecting loaded data.
        let mut loaded_filters: Vec<(String, HashMap<u64, RoaringBitmap>)> = Vec::new();
        let mut loaded_values: Vec<(String, HashMap<u64, RoaringBitmap>, Vec<u64>)> = Vec::new();
        let mut loaded_sort: Option<(String, Vec<RoaringBitmap>)> = None;

        // Full-field loads (low-cardinality)
        for name in &needed_filters {
            let t0 = std::time::Instant::now();
            let bitmaps = store.load_field(name)?;
            let count = bitmaps.len();
            eprintln!(
                "Lazy-loaded filter '{}': {} values in {:.1}ms",
                name,
                count,
                t0.elapsed().as_secs_f64() * 1000.0
            );

            let _ = self.lazy_tx.send(LazyLoad::FilterField {
                name: name.clone(),
                bitmaps: bitmaps.clone(),
            });
            self.pending_filter_loads.lock().remove(name);
            loaded_filters.push((name.clone(), bitmaps));
        }

        // Per-value loads (high-cardinality multi_value)
        // We need the current snapshot to determine which values are missing.
        {
            let current: Arc<InnerEngine> = self.inner.load_full();
            for (field_name, values) in &needed_values {
                // Filter out values already present in the snapshot
                let missing: Vec<u64> = if let Some(field) = current.filters.get_field(field_name) {
                    values
                        .iter()
                        .copied()
                        .filter(|v| {
                            // Load if the value doesn't exist OR if it exists but is unloaded
                            match field.get_versioned(*v) {
                                None => true,
                                Some(vb) => !vb.is_loaded(),
                            }
                        })
                        .collect()
                } else {
                    values.clone()
                };

                // Filter out values that don't exist on disk (positive existence set).
                // This eliminates 30-50ms disk I/O per nonexistent value.
                let missing: Vec<u64> = if let Some(ek) = self.existing_keys.get(field_name.as_str()) {
                    let keys = ek.load();
                    missing.into_iter().filter(|v| keys.contains(v)).collect()
                } else {
                    missing
                };

                if missing.is_empty() {
                    continue;
                }

                let t0 = std::time::Instant::now();
                let loaded = store.load_field_values(field_name, &missing)?;
                if loaded.is_empty() {
                    continue;
                }
                let count = loaded.len();
                eprintln!(
                    "Lazy-loaded filter '{}': {} values (per-value) in {:.1}ms",
                    field_name,
                    count,
                    t0.elapsed().as_secs_f64() * 1000.0
                );

                let _ = self.lazy_tx.send(LazyLoad::FilterValues {
                    field: field_name.clone(),
                    values: loaded.clone(),
                });
                loaded_values.push((field_name.clone(), loaded, missing));
            }
        }

        // Sort field loads
        if let Some(ref sort_name) = needed_sort {
            let t0 = std::time::Instant::now();
            let bits = self
                .config
                .sort_fields
                .iter()
                .find(|sc| sc.name == *sort_name)
                .map(|sc| sc.bits as usize)
                .unwrap_or(32);
            if let Some(layers) = store.load_sort_layers(sort_name, bits)? {
                let layer_count = layers.len();
                eprintln!(
                    "Lazy-loaded sort '{}': {} layers in {:.1}ms",
                    sort_name,
                    layer_count,
                    t0.elapsed().as_secs_f64() * 1000.0
                );

                let _ = self.lazy_tx.send(LazyLoad::SortField {
                    name: sort_name.clone(),
                    layers: layers.clone(),
                });
                loaded_sort = Some((sort_name.clone(), layers));
            }

            self.pending_sort_loads.lock().remove(sort_name);
        }

        let any_loaded = !loaded_filters.is_empty() || !loaded_values.is_empty() || loaded_sort.is_some();
        if any_loaded {
            // Use rcu() to atomically apply loaded data without racing with
            // the flush thread's store(). The closure may be called multiple
            // times if the snapshot changes underneath, but it only does
            // cheap in-memory modifications (no disk I/O).
            self.inner.rcu(|current| {
                let mut updated = (**current).clone();

                for (name, bitmaps) in &loaded_filters {
                    if let Some(field) = updated.filters.get_field_mut(name) {
                        field.load_field_complete(bitmaps.clone());
                    }
                }

                for (field_name, loaded, requested_keys) in &loaded_values {
                    if let Some(field) = updated.filters.get_field_mut(field_name) {
                        field.load_values(loaded.clone(), requested_keys);
                    }
                }

                if let Some((ref sort_name, ref layers)) = loaded_sort {
                    if let Some(sf) = updated.sorts.get_field_mut(sort_name) {
                        sf.load_layers(layers.clone());
                    }
                }

                updated
            });
        }

        Ok(())
    }

    /// Recursively collect filter field names from a FilterClause that are still pending.
    fn collect_filter_fields(
        clause: &FilterClause,
        pending: &HashSet<String>,
        out: &mut Vec<String>,
    ) {
        match clause {
            FilterClause::Eq(f, _)
            | FilterClause::NotEq(f, _)
            | FilterClause::Gt(f, _)
            | FilterClause::Lt(f, _)
            | FilterClause::Gte(f, _)
            | FilterClause::Lte(f, _) => {
                if pending.contains(f) && !out.contains(f) {
                    out.push(f.clone());
                }
            }
            FilterClause::In(f, _) | FilterClause::NotIn(f, _) => {
                if pending.contains(f) && !out.contains(f) {
                    out.push(f.clone());
                }
            }
            FilterClause::Not(inner) => Self::collect_filter_fields(inner, pending, out),
            FilterClause::And(clauses) | FilterClause::Or(clauses) => {
                for c in clauses {
                    Self::collect_filter_fields(c, pending, out);
                }
            }
            FilterClause::BucketBitmap { field, .. } => {
                if pending.contains(field) && !out.contains(field) {
                    out.push(field.clone());
                }
            }
        }
    }

    /// Recursively collect (field, value) pairs from filter clauses for per-value
    /// lazy loading of high-cardinality multi_value fields.
    fn collect_lazy_values(
        clause: &FilterClause,
        lazy_fields: &HashSet<String>,
        out: &mut HashMap<String, Vec<u64>>,
    ) {
        match clause {
            FilterClause::Eq(f, v) => {
                if lazy_fields.contains(f) {
                    if let Some(key) = value_to_bitmap_key(v) {
                        out.entry(f.clone()).or_default().push(key);
                    }
                }
            }
            FilterClause::NotEq(f, v) => {
                if lazy_fields.contains(f) {
                    if let Some(key) = value_to_bitmap_key(v) {
                        out.entry(f.clone()).or_default().push(key);
                    }
                }
            }
            FilterClause::In(f, vs) | FilterClause::NotIn(f, vs) => {
                if lazy_fields.contains(f) {
                    let entry = out.entry(f.clone()).or_default();
                    for v in vs {
                        if let Some(key) = value_to_bitmap_key(v) {
                            entry.push(key);
                        }
                    }
                }
            }
            FilterClause::Gt(f, v)
            | FilterClause::Lt(f, v)
            | FilterClause::Gte(f, v)
            | FilterClause::Lte(f, v) => {
                if lazy_fields.contains(f) {
                    if let Some(key) = value_to_bitmap_key(v) {
                        out.entry(f.clone()).or_default().push(key);
                    }
                }
            }
            FilterClause::Not(inner) => Self::collect_lazy_values(inner, lazy_fields, out),
            FilterClause::And(clauses) | FilterClause::Or(clauses) => {
                for c in clauses {
                    Self::collect_lazy_values(c, lazy_fields, out);
                }
            }
            FilterClause::BucketBitmap { .. } => {}
        }
    }

    /// Execute a parsed BitdexQuery.
    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        // Lazy-load any fields not yet loaded from disk
        self.ensure_fields_loaded(
            &query.filters,
            query.sort.as_ref().map(|s| s.field.as_str()),
        )?;

        let snap = self.snapshot(); // lock-free
        let tb_guard = self.time_buckets.as_ref().map(|tb| tb.lock());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let executor = {
            let mut base = QueryExecutor::new(
                &snap.slots,
                &snap.filters,
                &snap.sorts,
                self.config.max_page_size,
            );
            if let Some(ref maps) = self.string_maps {
                base = base.with_string_maps(maps);
            }
            if let Some(ref cs) = self.case_sensitive_fields {
                base = base.with_case_sensitive_fields(cs);
            }
            if !self.dictionaries.is_empty() {
                base = base.with_dictionaries(&self.dictionaries);
            }
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };

        // ── Fast path: unified cache hit without expansion ──
        // Try cache lookup BEFORE computing filters. If we hit, we can skip
        // the expensive filter bitmap computation entirely (~2ms saved at 105M).
        if let Some(sort_clause) = query.sort.as_ref() {
            if let Some(clauses) = cache::canonicalize(&query.filters) {
                let ukey = UnifiedKey {
                    filter_clauses: clauses,
                    sort_field: sort_clause.field.clone(),
                    direction: sort_clause.direction,
                };

                let cache_data = {
                    let mut uc = self.unified_cache.lock();
                    uc.lookup(&ukey).map(|entry| {
                        let bm = Arc::clone(entry.bitmap());
                        let has_more = entry.has_more();
                        let min_val = entry.min_tracked_value();
                        let cap = entry.capacity();
                        let total = entry.total_matched();
                        let radix = entry.radix().cloned();
                        let direction = entry.direction();
                        let sorted_keys = entry.sorted_keys().map(Arc::clone);
                        (bm, has_more, min_val, cap, total, radix, direction, sorted_keys)
                    })
                };

                if let Some((unified_bm, has_more, min_val, capacity, cached_total, cached_radix, _cached_direction, cached_sorted_keys)) = cache_data {
                    // Check if cursor is past the cache boundary
                    let needs_expansion = if let Some(cursor) = query.cursor.as_ref() {
                        let strictly_past = match sort_clause.direction {
                            crate::query::SortDirection::Desc => cursor.sort_value < min_val as u64,
                            crate::query::SortDirection::Asc => cursor.sort_value > min_val as u64,
                        };
                        if strictly_past {
                            true
                        } else if cursor.sort_value == min_val as u64 {
                            !unified_bm.contains(cursor.slot_id)
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if !needs_expansion {
                        // FAST PATH: cache hit, no expansion needed.
                        // Skip filter computation entirely — use cached bitmap + total_matched.
                        let offset = if query.cursor.is_none() {
                            query.offset.unwrap_or(0)
                        } else {
                            0
                        };
                        let fetch_limit = query.limit.saturating_add(offset);

                        // Dispatch: sorted_keys (≤4K initial) → radix (64K expanded) → bitmap fallback
                        let mut result = if let Some(ref keys) = cached_sorted_keys {
                            // Sorted vec fast path: binary search O(log n) (~55ns)
                            executor.execute_from_sorted_keys(
                                keys, &sort_clause.field, sort_clause.direction,
                                fetch_limit, query.cursor.as_ref(), cached_total,
                            )?
                        } else if let Some(ref radix) = cached_radix {
                            // Radix fast path: bucket-based traversal (~250 items per bucket)
                            executor.execute_from_radix(
                                radix, sort_clause, fetch_limit,
                                query.cursor.as_ref(), cached_total,
                            )?
                        } else {
                            let use_simple = unified_bm.len() < 10_000;
                            executor.execute_from_bitmap(
                                &unified_bm,
                                query.sort.as_ref(),
                                fetch_limit,
                                query.cursor.as_ref(),
                                use_simple,
                            )?
                        };

                        // Short page from cache = cursor at boundary, need expansion.
                        // Compute filters (we skipped this) and expand.
                        if result.ids.len() < fetch_limit && query.cursor.is_some() && has_more {
                            let (filter_arc, use_simple_sort) = self.resolve_filters(
                                &executor, &query.filters, tb_guard.as_deref(), now_unix,
                            )?;
                            let max_cap = self.unified_cache.lock().config().max_capacity;
                            let expand_limit = max_cap.saturating_sub(capacity);
                            let expand_cursor = result.cursor.as_ref().or(query.cursor.as_ref());
                            let expand_result = executor.execute_from_bitmap_unclamped(
                                &filter_arc, query.sort.as_ref(), expand_limit,
                                expand_cursor, use_simple_sort,
                            )?;
                            if !expand_result.ids.is_empty() {
                                let sorted_slots: Vec<u32> = expand_result.ids.iter()
                                    .map(|&id| id as u32).collect();
                                let sort_field = snap.sorts.get_field(&sort_clause.field);
                                let value_fn = |slot: u32| -> u32 {
                                    sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                                };
                                let mut uc = self.unified_cache.lock();
                                if let Some(entry) = uc.lookup(&ukey) {
                                    entry.expand(&sorted_slots, value_fn);
                                }
                            }
                            // Re-query from expanded entry (now has radix)
                            let expanded_data = {
                                let mut uc = self.unified_cache.lock();
                                uc.lookup(&ukey).map(|e| {
                                    let radix = e.radix().cloned();
                                    let bm = Arc::clone(e.bitmap());
                                    (radix, bm)
                                })
                            };
                            if let Some((radix, bm)) = expanded_data {
                                if let Some(ref r) = radix {
                                    result = executor.execute_from_radix(
                                        r, sort_clause, fetch_limit,
                                        query.cursor.as_ref(), filter_arc.len(),
                                    )?;
                                } else {
                                    result = executor.execute_from_bitmap(
                                        &bm, query.sort.as_ref(), fetch_limit,
                                        query.cursor.as_ref(), bm.len() < 10_000,
                                    )?;
                                }
                            }
                            result.total_matched = filter_arc.len();
                            self.post_validate(&mut result, &query.filters, &executor)?;
                            return Ok(result);
                        }

                        // Use cached total_matched (avoids recomputing 21M-entry filter bitmap)
                        result.total_matched = cached_total;

                        // Apply offset
                        if offset > 0 && !result.ids.is_empty() {
                            if offset >= result.ids.len() {
                                result.ids.clear();
                                result.cursor = None;
                            } else {
                                result.ids = result.ids.split_off(offset);
                                if let Some(&last_id) = result.ids.last() {
                                    let slot = last_id as u32;
                                    if let Some(sort_field) = snap.sorts.get_field(&sort_clause.field) {
                                        result.cursor = Some(crate::query::CursorPosition {
                                            sort_value: sort_field.reconstruct_value(slot) as u64,
                                            slot_id: slot,
                                        });
                                    }
                                }
                            }
                        }

                        self.post_validate(&mut result, &query.filters, &executor)?;
                        return Ok(result);
                    }

                    // Expansion needed — fall through to slow path with pre-fetched cache data.
                    return self.execute_query_slow_path(
                        query, &snap, &executor, tb_guard.as_deref(), now_unix,
                        Some((ukey, unified_bm, has_more, min_val, capacity, cached_total)),
                    );
                }
            }
        }

        // ── Slow path: cache miss or unsorted query ──
        self.execute_query_slow_path(
            query, &snap, &executor, tb_guard.as_deref(), now_unix, None,
        )
    }

    /// Slow path for execute_query: computes full filter bitmap.
    /// Used for cache misses, expansions, and unsorted queries.
    fn execute_query_slow_path(
        &self,
        query: &BitdexQuery,
        snap: &Arc<InnerEngine>,
        executor: &QueryExecutor,
        time_buckets: Option<&TimeBucketManager>,
        now_unix: u64,
        // Pre-fetched cache data from fast path that detected expansion needed
        cached: Option<(UnifiedKey, Arc<RoaringBitmap>, bool, u32, usize, u64)>,
    ) -> Result<QueryResult> {
        let (filter_arc, use_simple_sort) =
            self.resolve_filters(executor, &query.filters, time_buckets, now_unix)?;

        let full_total_matched = filter_arc.len();

        // If we have pre-fetched cache data (expansion case), use it.
        // Otherwise, do a fresh cache lookup (miss case).
        let (unified_key, unified_hit) = if let Some((ukey, bm, has_more, min_val, cap, _total)) = cached {
            (Some(ukey), Some((bm, has_more, min_val, cap)))
        } else if let Some(sort_clause) = query.sort.as_ref() {
            let mut uc = self.unified_cache.lock();
            let min_size = uc.config().min_filter_size as u64;
            if full_total_matched >= min_size {
                if let Some(clauses) = cache::canonicalize(&query.filters) {
                    let ukey = UnifiedKey {
                        filter_clauses: clauses,
                        sort_field: sort_clause.field.clone(),
                        direction: sort_clause.direction,
                    };
                    let hit = uc.lookup(&ukey).map(|entry| {
                        let bm = Arc::clone(entry.bitmap());
                        let has_more = entry.has_more();
                        let min_val = entry.min_tracked_value();
                        let cap = entry.capacity();
                        (bm, has_more, min_val, cap)
                    });
                    (Some(ukey), hit)
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // Check if cursor is past the cache boundary — trigger expansion if so.
        let needs_expansion = if let (Some((ref unified_bm, _, min_val, _)), Some(cursor), Some(sort_clause))
            = (&unified_hit, query.cursor.as_ref(), query.sort.as_ref())
        {
            let strictly_past = match sort_clause.direction {
                crate::query::SortDirection::Desc => cursor.sort_value < *min_val as u64,
                crate::query::SortDirection::Asc => cursor.sort_value > *min_val as u64,
            };
            let at_boundary = cursor.sort_value == *min_val as u64;
            if strictly_past {
                true
            } else if at_boundary {
                !unified_bm.contains(cursor.slot_id)
            } else {
                false
            }
        } else {
            false
        };

        let (effective_bitmap, use_simple) = if needs_expansion {
            if let (Some(ref ukey), Some((_, has_more, _, capacity))) = (&unified_key, &unified_hit) {
                if *has_more {
                    let max_cap = self.unified_cache.lock().config().max_capacity;
                    let expand_limit = max_cap.saturating_sub(*capacity);
                    let expand_result = executor.execute_from_bitmap_unclamped(
                        &filter_arc,
                        query.sort.as_ref(),
                        expand_limit,
                        query.cursor.as_ref(),
                        use_simple_sort,
                    )?;

                    if !expand_result.ids.is_empty() {
                        let sorted_slots: Vec<u32> = expand_result.ids.iter()
                            .map(|&id| id as u32).collect();
                        let sort_field = snap.sorts.get_field(&ukey.sort_field);
                        let value_fn = |slot: u32| -> u32 {
                            sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                        };
                        let mut uc = self.unified_cache.lock();
                        if let Some(entry) = uc.lookup(ukey) {
                            entry.expand(&sorted_slots, value_fn);
                        }
                    }

                    let mut uc = self.unified_cache.lock();
                    if let Some(entry) = uc.lookup(ukey) {
                        let bm = Arc::clone(entry.bitmap());
                        let use_simple = bm.len() < 10_000;
                        (bm, use_simple)
                    } else {
                        (Arc::clone(&filter_arc), use_simple_sort)
                    }
                } else {
                    if let Some((ref unified_bm, ..)) = unified_hit {
                        let use_simple = unified_bm.len() < 10_000;
                        (Arc::clone(unified_bm), use_simple)
                    } else {
                        (Arc::clone(&filter_arc), use_simple_sort)
                    }
                }
            } else {
                (Arc::clone(&filter_arc), use_simple_sort)
            }
        } else if let Some((ref unified_bm, ..)) = unified_hit {
            let use_simple = unified_bm.len() < 10_000;
            (Arc::clone(unified_bm), use_simple)
        } else {
            (Arc::clone(&filter_arc), use_simple_sort)
        };

        let offset = if query.cursor.is_none() {
            query.offset.unwrap_or(0)
        } else {
            0
        };
        let fetch_limit = query.limit.saturating_add(offset);

        let bound_was_applied = effective_bitmap.len() < filter_arc.len();
        let mut result = executor.execute_from_bitmap(
            &effective_bitmap,
            query.sort.as_ref(),
            fetch_limit,
            query.cursor.as_ref(),
            use_simple,
        )?;

        // Bound exhaustion: if the bounded bitmap returned fewer results than requested,
        // expand the cache and re-query from the expanded bitmap.
        if result.ids.len() < fetch_limit && query.cursor.is_some() && bound_was_applied {
            let did_expand = if let (Some(ref ukey), Some((_, has_more, _, capacity))) = (&unified_key, &unified_hit) {
                if *has_more {
                    let max_cap = self.unified_cache.lock().config().max_capacity;
                    let expand_limit = max_cap.saturating_sub(*capacity);
                    let expand_cursor = result.cursor.as_ref().or(query.cursor.as_ref());
                    let expand_result = executor.execute_from_bitmap_unclamped(
                        &filter_arc,
                        query.sort.as_ref(),
                        expand_limit,
                        expand_cursor,
                        use_simple_sort,
                    )?;
                    if !expand_result.ids.is_empty() {
                        let sorted_slots: Vec<u32> = expand_result.ids.iter()
                            .map(|&id| id as u32).collect();
                        let sort_field = snap.sorts.get_field(&ukey.sort_field);
                        let value_fn = |slot: u32| -> u32 {
                            sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                        };
                        let mut uc = self.unified_cache.lock();
                        if let Some(entry) = uc.lookup(ukey) {
                            entry.expand(&sorted_slots, value_fn);
                        }
                    }
                    true
                } else { false }
            } else { false };

            // Re-query from expanded entry (use radix if available)
            let re_data = if did_expand {
                if let Some(ref ukey) = unified_key {
                    let mut uc = self.unified_cache.lock();
                    uc.lookup(ukey).map(|e| {
                        let radix = e.radix().cloned();
                        let bm = Arc::clone(e.bitmap());
                        (radix, bm)
                    })
                } else { None }
            } else { None };
            if let Some(sort_clause) = query.sort.as_ref() {
                if let Some((radix, bm)) = re_data {
                    if let Some(ref r) = radix {
                        result = executor.execute_from_radix(
                            r, sort_clause, fetch_limit,
                            query.cursor.as_ref(), full_total_matched,
                        )?;
                    } else {
                        result = executor.execute_from_bitmap(
                            &bm, query.sort.as_ref(), fetch_limit,
                            query.cursor.as_ref(), bm.len() < 10_000,
                        )?;
                    }
                } else {
                    result = executor.execute_from_bitmap(
                        filter_arc.as_ref(), query.sort.as_ref(), fetch_limit,
                        query.cursor.as_ref(), false,
                    )?;
                }
            }
        }

        result.total_matched = full_total_matched;

        // Apply offset
        if offset > 0 && !result.ids.is_empty() {
            if offset >= result.ids.len() {
                result.ids.clear();
                result.cursor = None;
            } else {
                result.ids = result.ids.split_off(offset);
                if let Some(sort_clause) = query.sort.as_ref() {
                    if let Some(&last_id) = result.ids.last() {
                        let slot = last_id as u32;
                        if let Some(sort_field) = snap.sorts.get_field(&sort_clause.field) {
                            result.cursor = Some(crate::query::CursorPosition {
                                sort_value: sort_field.reconstruct_value(slot) as u64,
                                slot_id: slot,
                            });
                        }
                    }
                }
            }
        }

        // Unified cache formation: on miss, seed a cache entry.
        // Zero-result queries get an empty entry (costs ~1 byte, avoids repeated filter traversals).
        if unified_hit.is_none() {
            if let Some(ukey) = unified_key {
                if result.ids.is_empty() {
                    // Zero-result cache: empty bitmap, total_matched=0, no sort traversal needed.
                    let value_fn = |_slot: u32| -> u32 { 0 };
                    self.unified_cache.lock().form_and_store(
                        ukey,
                        &[],
                        false,
                        full_total_matched,
                        value_fn,
                    );
                } else {
                    let initial_cap = self.unified_cache.lock().config().initial_capacity;
                    let seed_result = executor.execute_from_bitmap_unclamped(
                        &filter_arc,
                        query.sort.as_ref(),
                        initial_cap,
                        None,
                        use_simple_sort,
                    )?;
                    let sort_field = snap.sorts.get_field(&ukey.sort_field);
                    let sorted_slots: Vec<u32> = seed_result.ids.iter().map(|&id| id as u32).collect();
                    let has_more = full_total_matched > sorted_slots.len() as u64;
                    let value_fn = |slot: u32| -> u32 {
                        sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                    };
                    self.unified_cache.lock().form_and_store(
                        ukey,
                        &sorted_slots,
                        has_more,
                        full_total_matched,
                        value_fn,
                    );
                }
            }
        }

        self.post_validate(&mut result, &query.filters, executor)?;
        Ok(result)
    }

    /// Resolve filter clauses to a bitmap.
    ///
    /// Snaps range filters to time bucket bitmaps, plans clause ordering,
    /// and computes the filter intersection.
    fn resolve_filters(
        &self,
        executor: &QueryExecutor,
        filters: &[FilterClause],
        time_buckets: Option<&TimeBucketManager>,
        now_unix: u64,
    ) -> Result<(Arc<roaring::RoaringBitmap>, bool)> {
        // Snap range filters to pre-computed time bucket bitmaps (C3).
        // This must happen BEFORE canonicalization so cache keys use stable
        // bucket names ("7d") instead of moving timestamps.
        let snapped;
        let effective_filters = if let Some(tb) = time_buckets {
            let mut managers = std::collections::HashMap::new();
            managers.insert(tb.field_name().to_string(), tb);
            let ctx = crate::query::BucketSnapContext {
                managers: &managers,
                now_secs: now_unix,
                tolerance_pct: 0.10,
            };
            snapped = crate::query::snap_range_clauses(filters, &ctx);
            &snapped[..]
        } else {
            filters
        };

        let plan = planner::plan_query(effective_filters, executor.filter_index(), executor.slot_allocator());
        let filter_bitmap = Arc::new(executor.compute_filters(&plan.ordered_clauses)?);

        Ok((filter_bitmap, plan.use_simple_sort))
    }

    /// Post-validate query results against in-flight writes.
    fn post_validate(
        &self,
        result: &mut QueryResult,
        filters: &[FilterClause],
        executor: &QueryExecutor,
    ) -> Result<()> {
        if !self.in_flight.has_in_flight() {
            return Ok(());
        }

        let overlapping = self.in_flight.find_overlapping(&result.ids);
        if overlapping.is_empty() {
            return Ok(());
        }

        // The executor holds references to the snapshot's bitmap state
        // so we can revalidate in-flight slots.
        let mut invalid_slots: Vec<u32> = Vec::new();

        for &slot in &overlapping {
            if !executor.slot_matches_filters(slot, filters)? {
                invalid_slots.push(slot);
            }
        }

        if !invalid_slots.is_empty() {
            result
                .ids
                .retain(|id| !invalid_slots.contains(&(*id as u32)));
        }

        Ok(())
    }

    /// Load the current snapshot (lock-free). Public API for advanced use.
    pub fn snapshot_public(&self) -> Arc<InnerEngine> {
        self.inner.load_full()
    }

    /// Get the number of alive documents (lock-free snapshot).
    pub fn alive_count(&self) -> u64 {
        self.snapshot().slots.alive_count()
    }

    /// Flush loop stats: (publish_count, cumulative_duration_nanos, last_duration_nanos).
    pub fn flush_stats(&self) -> (u64, u64, u64) {
        (
            self.flush_publish_count.load(Ordering::Relaxed),
            self.flush_duration_nanos.load(Ordering::Relaxed),
            self.flush_last_duration_nanos.load(Ordering::Relaxed),
        )
    }

    /// Number of filter + sort fields still pending lazy load.
    pub fn pending_field_count(&self) -> usize {
        self.pending_filter_loads.lock().len() + self.pending_sort_loads.lock().len()
    }

    /// Get eviction stats: (field_name, evicted_total, resident_count).
    pub fn eviction_stats(&self) -> Vec<(String, u64, usize)> {
        let snap = self.snapshot();
        self.config
            .filter_fields
            .iter()
            .filter(|fc| fc.eviction.is_some())
            .map(|fc| {
                let total = self
                    .eviction_total
                    .get(&fc.name)
                    .map(|e| e.value().load(Ordering::Relaxed))
                    .unwrap_or(0);
                let resident = snap
                    .filters
                    .get_field(&fc.name)
                    .map(|f| f.loaded_value_count())
                    .unwrap_or(0);
                (fc.name.clone(), total, resident)
            })
            .collect()
    }

    /// Get the current flush cycle counter.
    pub fn flush_cycle(&self) -> u64 {
        self.flush_cycle.load(Ordering::Relaxed)
    }

    /// Get the high-water mark slot counter (lock-free snapshot).
    pub fn slot_counter(&self) -> u32 {
        self.snapshot().slots.slot_counter()
    }

    // ---- Named cursors ----

    /// Set a named cursor value. The value is persisted to disk at the next
    /// merge thread checkpoint, atomically alongside bitmap snapshots.
    pub fn set_cursor(&self, name: String, value: String) {
        self.cursors.lock().insert(name, value);
        // Mark dirty so the merge thread will write at next cycle.
        self.dirty_since_snapshot.store(true, Ordering::Release);
    }

    /// Get a named cursor value (in-memory, not from disk).
    pub fn get_cursor(&self, name: &str) -> Option<String> {
        self.cursors.lock().get(name).cloned()
    }

    /// Get all named cursors.
    pub fn get_all_cursors(&self) -> HashMap<String, String> {
        self.cursors.lock().clone()
    }

    /// Retrieve a stored document by slot ID from the docstore.
    pub fn get_document(&self, slot_id: u32) -> Result<Option<StoredDoc>> {
        self.docstore.lock().get(slot_id)
    }

    /// Compact the docstore, reclaiming space from old write transactions.
    pub fn compact_docstore(&self) -> Result<bool> {
        self.docstore.lock().compact()
    }

    /// Configure docstore field defaults from a DataSchema.
    /// Must be called before `prepare_bulk_writer()` so the BulkWriter inherits the defaults.
    pub fn set_docstore_defaults(&self, schema: &crate::config::DataSchema) {
        self.docstore.lock().set_field_defaults(schema);
    }

    /// Get the current schema version from the docstore.
    pub fn docstore_schema_version(&self) -> u8 {
        self.docstore.lock().schema_version()
    }

    /// Build the schema registry for version-aware default reconstruction.
    pub fn build_schema_registry(&self) -> std::collections::HashMap<u8, std::collections::HashMap<String, serde_json::Value>> {
        self.docstore.lock().build_schema_registry()
    }

    /// Prepare a BulkWriter for lock-free parallel docstore writes during bulk loading.
    /// The BulkWriter holds a snapshot of the field dictionary and can encode/write
    /// docs without acquiring the DocStore Mutex.
    pub fn prepare_bulk_writer(&self, field_names: &[String]) -> crate::error::Result<crate::docstore::BulkWriter> {
        self.docstore.lock().prepare_bulk_load(field_names)
    }

    /// Return the set of indexed field names (filter + sort + "id").
    /// Used by the loader to strip doc-only fields from the bitmap accumulator.
    pub fn indexed_field_names(&self) -> std::collections::HashSet<String> {
        let mut s = std::collections::HashSet::new();
        for f in &self.config.filter_fields {
            s.insert(f.name.clone());
        }
        for f in &self.config.sort_fields {
            s.insert(f.name.clone());
        }
        s.insert("id".to_string());
        s
    }

    /// Get the current pending buffer depth. Always 0 (tier 2 removed).
    pub fn pending_depth(&self) -> usize {
        0
    }

    /// Report bitmap memory usage broken down by component (lock-free snapshot).
    ///
    /// Returns (slot_bytes, filter_bytes, sort_bytes, cache_entries, cache_bytes,
    ///          filter_details, sort_details)
    /// where all sizes are serialized bitmap bytes — no allocator or redb overhead.
    #[allow(clippy::type_complexity)]
    pub fn bitmap_memory_report(
        &self,
    ) -> (usize, usize, usize, usize, usize, Vec<(String, usize, usize)>, Vec<(String, usize)>) {
        let snap = self.snapshot();
        let slot_bytes = snap.slots.bitmap_bytes();
        let filter_bytes = snap.filters.bitmap_bytes();
        let sort_bytes = snap.sorts.bitmap_bytes();
        let uc = self.unified_cache.lock();
        let cache_entries = uc.stats().entries;
        let cache_bytes = uc.stats().memory_bytes;
        drop(uc);
        let filter_details: Vec<(String, usize, usize)> = snap
            .filters
            .per_field_bytes()
            .into_iter()
            .map(|(name, count, bytes)| (name.to_string(), count, bytes))
            .collect();
        let sort_details: Vec<(String, usize)> = snap
            .sorts
            .per_field_bytes()
            .into_iter()
            .map(|(name, bytes)| (name.to_string(), bytes))
            .collect();
        (slot_bytes, filter_bytes, sort_bytes, cache_entries, cache_bytes, filter_details, sort_details)
    }

    /// Return unified cache stats (entries, hits, misses, memory).
    pub fn unified_cache_stats(&self) -> crate::unified_cache::UnifiedCacheStats {
        self.unified_cache.lock().stats()
    }

    /// Return per-entry cache details for diagnostics.
    pub fn unified_cache_entry_details(&self) -> Vec<crate::unified_cache::UnifiedEntryDetail> {
        self.unified_cache.lock().entry_details()
    }

    /// Clear unified cache entries and reset counters.
    pub fn clear_unified_cache(&self) {
        self.unified_cache.lock().clear();
    }

    /// Enter loading mode: skip snapshot publishing and maintenance during bulk inserts.
    ///
    /// In loading mode, the flush thread still applies mutations to the staging engine
    /// but skips the expensive `staging.clone()` snapshot publish. This eliminates the
    /// Arc::make_mut clone cascade that dominates write cost at scale (e.g., cloning
    /// a 104K-entry userId HashMap every 100μs flush cycle).
    ///
    /// Queries during loading mode see stale data (the last published snapshot).
    /// Call `exit_loading_mode()` to publish the final state and resume normal operation.
    pub fn enter_loading_mode(&self) {
        self.loading_mode.store(true, Ordering::Release);
    }

    /// Exit loading mode: publish the current staging state and resume normal operation.
    ///
    /// Invalidates all caches (stale from loading) and triggers a snapshot publish
    /// on the next flush cycle by briefly pausing to let the flush thread catch up.
    pub fn exit_loading_mode(&self) {
        self.loading_mode.store(false, Ordering::Release);

        // Send ForcePublish command and block until the flush thread confirms.
        // This guarantees readers see the fully-loaded data before the caller
        // continues (e.g., before save_and_unload).
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let _ = self.cmd_tx.send(FlushCommand::ForcePublish { done: done_tx });
        // Block until flush thread processes the command. Timeout after 30s
        // to avoid deadlock if flush thread is stuck.
        match done_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(()) => {}
            Err(_) => {
                eprintln!("Warning: exit_loading_mode timed out waiting for flush thread publish");
            }
        }
    }

    /// Combined exit-loading + save + unload that avoids the memory spike.
    ///
    /// Instead of:
    ///   1. exit_loading_mode() → publishes staging.clone() (doubles refcounts)
    ///   2. save_and_unload() → reads published snapshot, saves to disk
    ///
    /// This does:
    ///   1. Sends ExitLoadingSaveUnload to flush thread
    ///   2. Flush thread saves directly from staging (the single copy)
    ///   3. Builds unloaded staging, publishes only the unloaded version
    ///
    /// At 105M records this eliminates the 22GB→38GB RSS spike from the
    /// intermediate staging.clone() that bumps Arc refcounts.
    pub fn exit_loading_mode_and_save_unload(&self) -> Result<()> {
        self.loading_mode.store(false, Ordering::Release);

        let store = self.bitmap_store.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config(
                "no bitmap_path configured; cannot save_and_unload".to_string(),
            )
        })?;
        let _ = store; // Just validating it exists; flush thread has its own Arc

        let skip_sorts = self.pending_sort_loads.lock().clone();
        let skip_filters = self.pending_filter_loads.lock().clone();
        let skip_lazy = self.lazy_value_fields.lock().clone();
        let cursors = self.cursors.lock().clone();
        let dictionaries = Arc::clone(&self.dictionaries);

        // Mark all loaded fields as pending for lazy reload after unload.
        for fc in &self.config.filter_fields {
            if !skip_filters.contains(&fc.name) && !skip_lazy.contains(&fc.name) {
                self.pending_filter_loads.lock().insert(fc.name.clone());
            }
        }
        for sc in &self.config.sort_fields {
            if !skip_sorts.contains(&sc.name) {
                self.pending_sort_loads.lock().insert(sc.name.clone());
            }
        }

        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        match self.cmd_tx.send(FlushCommand::ExitLoadingSaveUnload {
            skip_sorts: skip_sorts.clone(),
            skip_filters: skip_filters.clone(),
            skip_lazy: skip_lazy.clone(),
            cursors,
            dictionaries,
            done: done_tx,
        }) {
            Ok(()) => {
                // Save can take minutes at 105M — use generous timeout
                match done_rx.recv_timeout(Duration::from_secs(600)) {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(msg)) => Err(crate::error::BitdexError::Config(msg)),
                    Err(_) => {
                        eprintln!("Warning: exit_loading_mode_and_save_unload timed out");
                        Err(crate::error::BitdexError::Config(
                            "timed out waiting for flush thread save".to_string(),
                        ))
                    }
                }
            }
            Err(_) => {
                // Flush thread is gone — fall back to separate exit + save_and_unload
                eprintln!("Warning: flush thread gone, falling back to separate exit+save");
                // Re-clear the pending loads we just set (save_and_unload will re-set them)
                for fc in &self.config.filter_fields {
                    if !skip_filters.contains(&fc.name) && !skip_lazy.contains(&fc.name) {
                        self.pending_filter_loads.lock().remove(&fc.name);
                    }
                }
                for sc in &self.config.sort_fields {
                    if !skip_sorts.contains(&sc.name) {
                        self.pending_sort_loads.lock().remove(&sc.name);
                    }
                }
                self.exit_loading_mode();
                self.save_and_unload()
            }
        }
    }

    /// Save a full snapshot of the current published state to the configured BitmapStore.
    ///
    /// Captures the current ArcSwap snapshot (what readers see) and writes all
    /// filter bitmaps, alive bitmap, sort layer bitmaps, and slot counter in a
    /// single atomic redb transaction via `write_full_snapshot()`.
    ///
    /// This is intended for persisting state after bulk loading is complete.
    /// For incremental persistence during normal operation, the merge thread
    /// handles that automatically.
    ///
    /// Returns an error if no bitmap_store is configured.
    pub fn save_snapshot(&self) -> Result<()> {
        let store = self.bitmap_store.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config(
                "no bitmap_path configured; cannot save snapshot".to_string(),
            )
        })?;
        let skip_sorts = self.pending_sort_loads.lock().clone();
        let skip_filters = self.pending_filter_loads.lock().clone();
        let skip_lazy = self.lazy_value_fields.lock().clone();
        Self::write_snapshot_to_store(store, &self.inner, &self.config, &skip_sorts, &skip_filters, &skip_lazy)?;

        // Persist named cursors alongside bitmaps so they survive process restart.
        // The merge thread also writes cursors periodically, but save_snapshot() is
        // used by the bulk loader which exits immediately after saving.
        let cursor_snapshot = self.cursors.lock().clone();
        for (name, value) in &cursor_snapshot {
            store.write_cursor(name, value)?;
        }

        // Save LowCardinalityString dictionaries alongside bitmaps.
        if !self.dictionaries.is_empty() {
            let bitmap_path = store.root_path();
            self.save_dictionaries(bitmap_path)?;
        }

        Ok(())
    }

    /// Save a full snapshot of the current published state to a BitmapFs at a custom path.
    ///
    /// Creates a new BitmapFs at the given path and writes the complete engine
    /// state. Useful for benchmarks that want to save to a specific location,
    /// or for creating point-in-time backups separate from the live store.
    pub fn save_snapshot_to(&self, path: &Path) -> Result<()> {
        let store = BitmapFs::new(path)?;
        let skip_sorts = self.pending_sort_loads.lock().clone();
        let skip_filters = self.pending_filter_loads.lock().clone();
        let skip_lazy = self.lazy_value_fields.lock().clone();
        Self::write_snapshot_to_store(&store, &self.inner, &self.config, &skip_sorts, &skip_filters, &skip_lazy)?;

        // Save LowCardinalityString dictionaries alongside bitmaps.
        if !self.dictionaries.is_empty() {
            self.save_dictionaries(path)?;
        }

        Ok(())
    }

    /// Internal: zero-copy snapshot serialization.
    ///
    /// Reads the published snapshot through Arc refs — no InnerEngine clone.
    /// Uses `fused_cow()` to borrow base bitmaps directly (zero copy when clean)
    /// or create temporary merged bitmaps (only when dirty). Processes one field
    /// at a time so memory overhead is minimal (~1.7 MB for tagIds' 31K Cow refs).
    ///
    /// Skips fields that haven't been loaded yet (still pending lazy-load) to avoid
    /// overwriting real persisted data with empty placeholders.
    fn write_snapshot_to_store(
        store: &BitmapFs,
        inner: &ArcSwap<InnerEngine>,
        config: &Config,
        skip_sorts: &HashSet<String>,
        skip_filters: &HashSet<String>,
        skip_lazy_values: &HashSet<String>,
    ) -> Result<()> {
        let snap: Arc<InnerEngine> = inner.load_full();
        Self::write_inner_to_store(store, &snap, config, skip_sorts, skip_filters, skip_lazy_values)
    }

    /// Write bitmaps from an InnerEngine directly to the store.
    /// This is used by both the ArcSwap-based path and the flush thread's
    /// direct-from-staging path (which avoids the intermediate clone).
    fn write_inner_to_store(
        store: &BitmapFs,
        snap: &InnerEngine,
        config: &Config,
        skip_sorts: &HashSet<String>,
        skip_filters: &HashSet<String>,
        skip_lazy_values: &HashSet<String>,
    ) -> Result<()> {
        use std::borrow::Cow;

        let save_start = std::time::Instant::now();

        // Write alive bitmap + slot counter + deferred map first (critical metadata).
        let alive_cow = snap.slots.alive_fused_cow();
        store.write_alive(&alive_cow)?;
        store.write_slot_counter(snap.slots.slot_counter())?;
        if snap.slots.deferred_count() > 0 {
            store.write_deferred_alive(snap.slots.deferred_map())?;
        }

        // Sort fields — one at a time, zero-copy via fused_cow.
        for sc in &config.sort_fields {
            if skip_sorts.contains(&sc.name) {
                continue;
            }
            if let Some(sf) = snap.sorts.get_field(&sc.name) {
                let t0 = std::time::Instant::now();
                let fused_layers: Vec<Cow<'_, RoaringBitmap>> = sf.layer_bases_fused();
                let layer_refs: Vec<&RoaringBitmap> =
                    fused_layers.iter().map(|c| c.as_ref()).collect();
                store.write_sort_layers(&sc.name, &layer_refs)?;
                eprintln!("  save: sort {} in {:.1}ms",
                    sc.name, t0.elapsed().as_secs_f64() * 1000.0);
            }
        }

        // Filter fields — stream one bucket at a time to minimize memory overhead.
        // Lazy-value fields (multi_value with per-value loading) are only skipped
        // if they have no loaded bitmaps. If they have data (from inserts or lazy
        // loads), save whatever is currently in memory so evict-then-reload works.
        for (name, field) in snap.filters.fields() {
            if skip_filters.contains(name) {
                continue;
            }
            if skip_lazy_values.contains(name) && field.bitmap_count() == 0 {
                continue;
            }
            let t0 = std::time::Instant::now();
            let num_values = field.bitmap_count();
            // Group entries by bucket (256 buckets max)
            let mut by_bucket: HashMap<u8, Vec<(u64, Cow<'_, RoaringBitmap>)>> = HashMap::new();
            for (&value, vb) in field.iter_versioned() {
                let bucket = (value >> 8) as u8;
                by_bucket.entry(bucket).or_default().push((value, vb.fused_cow()));
            }
            let num_buckets = by_bucket.len();
            // Write one bucket at a time
            for (bucket, entries) in by_bucket {
                let refs: Vec<(u64, &RoaringBitmap)> = entries
                    .iter()
                    .map(|(v, c)| (*v, c.as_ref()))
                    .collect();
                store.write_filter_bucket(name, bucket, &refs)?;
            }
            eprintln!("  save: filter {} ({} values, {} buckets) in {:.1}ms",
                name, num_values, num_buckets, t0.elapsed().as_secs_f64() * 1000.0);
        }

        eprintln!("  save: total write {:.1}s", save_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Save the current snapshot to disk, then unload all loaded fields from memory.
    /// After this call, bitmap memory drops to near-zero — fields are marked pending
    /// and will lazy-load from disk on the next query that touches them.
    ///
    /// The unload is routed through the flush thread's command channel so that
    /// the flush thread's private staging is also replaced. This prevents the
    /// old staging from re-inflating the snapshot on the next publish cycle.
    ///
    /// Safe with concurrent mutations: the flush thread drains any pending
    /// mutations and applies them to the unloaded staging's diff layers before
    /// publishing.
    pub fn save_and_unload(&self) -> Result<()> {
        let store = self.bitmap_store.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config(
                "no bitmap_path configured; cannot save_and_unload".to_string(),
            )
        })?;

        // Snapshot what's already pending — don't save or unload those.
        let skip_sorts = self.pending_sort_loads.lock().clone();
        let skip_filters = self.pending_filter_loads.lock().clone();
        let skip_lazy = self.lazy_value_fields.lock().clone();

        // Phase 1: Zero-copy write to disk.
        Self::write_snapshot_to_store(
            store,
            &self.inner,
            &self.config,
            &skip_sorts,
            &skip_filters,
            &skip_lazy,
        )?;

        // Phase 2: Build an unloaded snapshot directly — no clone_staging().
        // clone_staging() would bump refcounts on all Arc<FilterField>s, preventing
        // the old bitmap data from being freed until publish. Instead, we build the
        // new InnerEngine field by field: keep slots (always needed), and for each
        // filter/sort field either move the Arc as-is (if skipped) or create a new
        // empty field (if unloading). This way old Arcs are freed immediately on publish.
        let snap = self.inner.load_full();
        let slots = snap.slots.clone();

        let mut new_filters = crate::filter::FilterIndex::new();
        for fc in &self.config.filter_fields {
            new_filters.add_field(fc.clone());
        }
        // Unload ALL loaded fields — including lazy_value_fields (multi_value).
        // Previously, lazy_value_fields were skipped from unload, which kept
        // tagIds (~80% of bitmap memory) resident. Now they're unloaded and
        // will reload per-value on demand via the lazy loading path.
        for fc in &self.config.filter_fields {
            if skip_filters.contains(&fc.name) {
                // Field was never loaded (still pending) — keep as-is
                new_filters.copy_field_arc_from(&snap.filters, &fc.name);
            } else {
                // Unload: clear bases, preserve any in-flight diffs
                new_filters.unload_from(&snap.filters, &fc.name);
                // Route to correct reload path: multi_value fields use
                // per-value lazy loading, others use full-field loading.
                if skip_lazy.contains(&fc.name) {
                    // Already in lazy_value_fields — will reload per-value
                } else {
                    self.pending_filter_loads.lock().insert(fc.name.clone());
                }
            }
        }

        let mut new_sorts = crate::sort::SortIndex::new();
        for sc in &self.config.sort_fields {
            new_sorts.add_field(sc.clone());
        }
        for sc in &self.config.sort_fields {
            if skip_sorts.contains(&sc.name) {
                new_sorts.copy_field_arc_from(&snap.sorts, &sc.name);
            } else {
                new_sorts.unload_from(&snap.sorts, &sc.name);
                self.pending_sort_loads.lock().insert(sc.name.clone());
            }
        }

        // Drop our reference to the old snapshot before sending to flush thread.
        drop(snap);

        let unloaded = InnerEngine {
            slots,
            filters: new_filters,
            sorts: new_sorts,
        };

        // Phase 3: Route through flush thread — replaces both staging and
        // published snapshot atomically. Flush thread drains any pending
        // mutations and applies them to the unloaded staging before publishing.
        //
        // Fallback: if the flush thread is already shut down (e.g., tests that
        // call shutdown() before save_and_unload), publish directly. This is
        // safe because there's no flush thread to re-inflate the snapshot.
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        match self.cmd_tx.send(FlushCommand::SyncUnloaded {
            unloaded: unloaded.clone(),
            done: done_tx,
        }) {
            Ok(()) => {
                match done_rx.recv_timeout(Duration::from_secs(60)) {
                    Ok(()) => {}
                    Err(_) => {
                        eprintln!("Warning: save_and_unload timed out waiting for flush thread sync");
                        // Fallback: publish directly
                        self.publish_staging(unloaded);
                    }
                }
            }
            Err(_) => {
                // Channel disconnected — flush thread is gone, publish directly
                self.publish_staging(unloaded);
            }
        }
        Ok(())
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get a reference to the in-flight tracker.
    pub fn in_flight(&self) -> &InFlightTracker {
        &self.in_flight
    }

    /// PUT_MANY -- batch version of put() for throughput experiments.
    ///
    /// Batches the work: one snapshot load for all alive/allocation checks,
    /// computes all diffs, sends all ops, enqueues all docstore writes, then clears
    /// in-flight tracking.
    ///
    /// EXPERIMENTAL: This is a temporary method for benchmarking put_many vs put-in-loop.
    pub fn put_many(&self, docs: &[(u32, Document)]) -> Result<()> {
        // Phase 1: Mark all in-flight
        for &(id, _) in docs {
            self.in_flight.mark_in_flight(id);
        }

        let result = (|| -> Result<()> {
            // Phase 2: Single snapshot load for all alive/allocation checks
            let statuses: Vec<(u32, bool, bool)> = {
                let snap = self.snapshot();
                docs.iter()
                    .map(|&(id, _)| {
                        let alive = snap.slots.is_alive(id);
                        let alloc = if !alive {
                            snap.slots.was_ever_allocated(id)
                        } else {
                            false
                        };
                        (id, alive, alloc)
                    })
                    .collect()
            };

            // Phase 3: Batch docstore reads for upserts (outside any lock)
            let old_docs: Vec<Option<crate::docstore::StoredDoc>> = statuses
                .iter()
                .map(|&(id, is_upsert, was_allocated)| {
                    if is_upsert || was_allocated {
                        self.docstore.lock().get(id).ok().flatten()
                    } else {
                        None
                    }
                })
                .collect();

            // Phase 4: Compute all diffs and collect all ops
            let mut all_ops: Vec<MutationOp> = Vec::new();
            let mut doc_writes: Vec<(u32, crate::docstore::StoredDoc)> = Vec::new();

            for (i, &(id, ref doc)) in docs.iter().enumerate() {
                let (_, is_upsert, _) = statuses[i];
                let ops = diff_document(id, old_docs[i].as_ref(), doc, &self.config, is_upsert, &self.field_registry);
                all_ops.extend(ops);
                doc_writes.push((
                    id,
                    crate::docstore::StoredDoc {
                        fields: doc.fields.clone(),
                        schema_version: 0,
                    },
                ));
            }

            // Phase 5: Send all ops in one burst
            self.sender.send_batch(all_ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;

            // Phase 6: Enqueue all doc writes
            for item in doc_writes {
                self.doc_tx.send(item).map_err(|_| {
                    crate::error::BitdexError::CapacityExceeded(
                        "docstore channel disconnected".to_string(),
                    )
                })?;
            }

            Ok(())
        })();

        // Phase 7: Clear all in-flight
        for &(id, _) in docs {
            self.in_flight.clear_in_flight(id);
        }

        result
    }

    /// PUT_BULK -- high-throughput bulk insert for initial data loading.
    ///
    /// Bypasses the write coalescer entirely. Documents are decomposed into
    /// per-bitmap operations in parallel across N worker threads, each building
    /// thread-local HashMaps of RoaringBitmaps. Thread results are merged, then
    /// applied directly to a staging InnerEngine copy and published via ArcSwap.
    ///
    /// This is ~10x faster than put() for bulk loads because:
    /// - No per-doc channel send/receive overhead
    /// - No diff computation (fresh inserts, no old doc lookup)
    /// - Parallel JSON decompose + bitmap building
    /// - Single snapshot publish at the end
    ///
    /// Assumes all slot IDs are fresh inserts (not upserts). For mixed
    /// insert/update workloads, use put() or put_many().
    ///
    /// Documents are persisted to the docstore after bitmap updates.
    /// Returns the number of documents successfully inserted.
    /// Bulk-insert documents into the engine with parallel decomposition.
    ///
    /// Returns `(count, docstore_handle)` where the handle can be joined to wait
    /// for background docstore persistence. Bitmaps are published immediately.
    pub fn put_bulk(&self, docs: Vec<(u32, Document)>, num_threads: usize) -> Result<(usize, JoinHandle<()>)> {
        if docs.is_empty() {
            let handle = thread::spawn(|| {});
            return Ok((0, handle));
        }

        // Clone snapshot and apply
        let snap = self.inner.load_full();
        let mut staging = (*snap).clone();
        let count = Self::put_bulk_into(&self.config, &mut staging, &docs, num_threads);

        // Publish
        self.inner.store(Arc::new(staging));
        self.invalidate_all_caches();

        // Background docstore persistence
        let docstore_handle = self.spawn_docstore_writer(docs);

        Ok((count, docstore_handle))
    }

    /// Bulk-insert directly into a mutable InnerEngine without cloning or publishing.
    ///
    /// This is the "loading mode" variant — avoids the Arc::make_mut deep-clone cascade
    /// that happens when the published snapshot shares Arc references with the staging copy.
    /// Use this when loading many chunks sequentially: build up the InnerEngine, then publish once.
    pub fn put_bulk_loading(&self, staging: &mut InnerEngine, docs: &[(u32, Document)], num_threads: usize) -> usize {
        Self::put_bulk_into(&self.config, staging, docs, num_threads)
    }

    /// Publish a staging InnerEngine as the current snapshot and invalidate all caches.
    pub fn publish_staging(&self, staging: InnerEngine) {
        self.inner.store(Arc::new(staging));
        self.dirty_since_snapshot.store(true, Ordering::Release);
        self.invalidate_all_caches();
    }

    /// Take a clone of the current snapshot for mutation.
    pub fn clone_staging(&self) -> InnerEngine {
        let snap = self.inner.load_full();
        (*snap).clone()
    }

    fn invalidate_all_caches(&self) {
        self.unified_cache.lock().clear();
    }

    /// Persist documents to the docstore on a background thread.
    /// Returns a JoinHandle to wait for completion. The docs Vec is consumed.
    pub fn spawn_docstore_writer(&self, docs: Vec<(u32, Document)>) -> JoinHandle<()> {
        let docstore = Arc::clone(&self.docstore);
        thread::spawn(move || {
            let batch_size = 100_000;
            let mut batch: Vec<(u32, StoredDoc)> = Vec::with_capacity(batch_size);
            for (slot, doc) in docs {
                batch.push((slot, StoredDoc { fields: doc.fields, schema_version: 0 }));
                if batch.len() >= batch_size {
                    if let Err(e) = docstore.lock().put_batch(&batch) {
                        eprintln!("put_bulk: docstore batch write failed: {e}");
                    }
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                if let Err(e) = docstore.lock().put_batch(&batch) {
                    eprintln!("put_bulk: docstore batch write failed: {e}");
                }
            }
        })
    }

    /// Write documents to the docstore synchronously (inline, no background thread).
    /// Used during bulk loading to bound memory — docs are written immediately and freed
    /// after the next bitmap chunk flush instead of lingering in a background thread.
    pub fn write_docs_to_docstore(&self, docs: &[(u32, Document)]) {
        let batch_size = 10_000;
        let mut batch: Vec<(u32, StoredDoc)> = Vec::with_capacity(batch_size);
        for (slot, doc) in docs {
            batch.push((*slot, StoredDoc { fields: doc.fields.clone(), schema_version: 0 }));
            if batch.len() >= batch_size {
                if let Err(e) = self.docstore.lock().put_batch(&batch) {
                    eprintln!("write_docs_to_docstore: batch write failed: {e}");
                }
                batch.clear();
            }
        }
        if !batch.is_empty() {
            if let Err(e) = self.docstore.lock().put_batch(&batch) {
                eprintln!("write_docs_to_docstore: batch write failed: {e}");
            }
        }
    }

    /// Apply pre-built bitmap maps directly to a staging snapshot.
    /// Used by the fused parse+bitmap loader to skip the decompose/merge/apply pipeline.
    pub fn apply_bitmap_maps(
        staging: &mut InnerEngine,
        filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
        sort_maps: HashMap<String, HashMap<usize, RoaringBitmap>>,
        alive: RoaringBitmap,
    ) {
        for (field_name, value_map) in filter_maps {
            if let Some(field) = staging.filters.get_field_mut(&field_name) {
                for (value, bitmap) in value_map {
                    field.or_bitmap(value, &bitmap);
                }
            }
        }
        for (field_name, bit_map) in sort_maps {
            if let Some(field) = staging.sorts.get_field_mut(&field_name) {
                for (bit, bitmap) in bit_map {
                    field.or_layer(bit, &bitmap);
                }
            }
        }
        staging.slots.alive_or_bitmap(&alive);
    }

    /// Core decompose + merge + apply logic, shared by put_bulk() and put_bulk_loading().
    fn put_bulk_into(config: &Config, staging: &mut InnerEngine, docs: &[(u32, Document)], num_threads: usize) -> usize {
        let t0 = std::time::Instant::now();
        let num_threads = num_threads.max(1).min(docs.len());

        let filter_configs: Vec<_> = config.filter_fields.clone();
        let sort_configs: Vec<_> = config.sort_fields.clone();

        struct ThreadResult {
            filter_maps: HashMap<(String, u64), RoaringBitmap>,
            sort_maps: HashMap<(String, usize), RoaringBitmap>,
            alive_bitmap: RoaringBitmap,
            count: usize,
        }

        let chunk_size = (docs.len() + num_threads - 1) / num_threads;
        let filter_configs_ref = &filter_configs;
        let sort_configs_ref = &sort_configs;

        let thread_results: Vec<ThreadResult> = thread::scope(|s| {
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let start = t * chunk_size;
                    let end = (start + chunk_size).min(docs.len());
                    if start >= end {
                        return s.spawn(move || ThreadResult {
                            filter_maps: HashMap::new(),
                            sort_maps: HashMap::new(),
                            alive_bitmap: RoaringBitmap::new(),
                            count: 0,
                        });
                    }

                    s.spawn(move || {
                        let slice = &docs[start..end];
                        let mut filter_maps: HashMap<(String, u64), RoaringBitmap> =
                            HashMap::with_capacity(65_000);
                        let mut sort_maps: HashMap<(String, usize), RoaringBitmap> =
                            HashMap::with_capacity(256);
                        let mut alive_bitmap = RoaringBitmap::new();

                        for &(slot, ref doc) in slice {
                            alive_bitmap.insert(slot);

                            for fc in filter_configs_ref {
                                if let Some(fv) = doc.fields.get(&fc.name) {
                                    match fv {
                                        crate::mutation::FieldValue::Single(v) => {
                                            if let Some(key) = value_to_bitmap_key(v) {
                                                filter_maps
                                                    .entry((fc.name.clone(), key))
                                                    .or_insert_with(RoaringBitmap::new)
                                                    .insert(slot);
                                            }
                                        }
                                        crate::mutation::FieldValue::Multi(vals) => {
                                            for v in vals {
                                                if let Some(key) = value_to_bitmap_key(v) {
                                                    filter_maps
                                                        .entry((fc.name.clone(), key))
                                                        .or_insert_with(RoaringBitmap::new)
                                                        .insert(slot);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            for sc in sort_configs_ref {
                                if let Some(fv) = doc.fields.get(&sc.name) {
                                    if let crate::mutation::FieldValue::Single(
                                        crate::query::Value::Integer(v),
                                    ) = fv
                                    {
                                        let value = *v as u32;
                                        let num_bits = sc.bits as usize;
                                        for bit in 0..num_bits {
                                            if (value >> bit) & 1 == 1 {
                                                sort_maps
                                                    .entry((sc.name.clone(), bit))
                                                    .or_insert_with(RoaringBitmap::new)
                                                    .insert(slot);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        ThreadResult {
                            filter_maps,
                            sort_maps,
                            alive_bitmap,
                            count: slice.len(),
                        }
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let t1 = t0.elapsed();

        // Phase 2: Merge thread results
        let mut merged_filters: HashMap<(String, u64), RoaringBitmap> = HashMap::new();
        let mut merged_sorts: HashMap<(String, usize), RoaringBitmap> = HashMap::new();
        let mut merged_alive = RoaringBitmap::new();
        let mut total_count: usize = 0;

        for result in &thread_results {
            total_count += result.count;
            merged_alive |= &result.alive_bitmap;
        }

        for result in &thread_results {
            for ((field, value), bm) in &result.filter_maps {
                merged_filters
                    .entry((field.clone(), *value))
                    .and_modify(|e| *e |= bm)
                    .or_insert_with(|| bm.clone());
            }
            for ((field, bit), bm) in &result.sort_maps {
                merged_sorts
                    .entry((field.clone(), *bit))
                    .and_modify(|e| *e |= bm)
                    .or_insert_with(|| bm.clone());
            }
        }
        // Drop thread results to free memory before apply phase
        drop(thread_results);

        let t2 = t0.elapsed();

        // Phase 3: Apply to staging — OR directly into base (bypasses diff layer)
        for ((field_name, value), bitmap) in merged_filters {
            if let Some(field) = staging.filters.get_field_mut(&field_name) {
                field.or_bitmap(value, &bitmap);
            }
        }
        for ((field_name, bit), bitmap) in merged_sorts {
            if let Some(field) = staging.sorts.get_field_mut(&field_name) {
                field.or_layer(bit, &bitmap);
            }
        }
        staging.slots.alive_or_bitmap(&merged_alive);

        let t3 = t0.elapsed();

        eprintln!("put_bulk phases: decompose={:.2}s merge={:.2}s apply={:.2}s total={:.2}s",
            t1.as_secs_f64(),
            (t2 - t1).as_secs_f64(),
            (t3 - t2).as_secs_f64(),
            t3.as_secs_f64());

        total_count
    }

    /// Rebuild sort and/or filter bitmaps from the docstore.
    ///
    /// Iterates all alive slots, reads each document from the docstore, and
    /// reconstructs the requested bitmap fields from scratch. This is used to
    /// repair corrupt or empty bitmap snapshots when the docstore is intact.
    ///
    /// The rebuilt bitmaps completely replace the existing ones for the specified
    /// fields — existing data is cleared before the new bitmaps are applied.
    ///
    /// Returns (slots_processed, fields_rebuilt) on success.
    pub fn rebuild_fields_from_docstore(
        &self,
        sort_fields: Option<Vec<String>>,
        filter_fields: Option<Vec<String>>,
        progress: Arc<AtomicU64>,
    ) -> Result<(u64, Vec<String>)> {
        let t0 = Instant::now();

        // Determine which fields to rebuild
        let rebuild_all = sort_fields.is_none() && filter_fields.is_none();
        let sort_configs: Vec<_> = match &sort_fields {
            Some(names) => self.config.sort_fields.iter()
                .filter(|sc| names.contains(&sc.name))
                .cloned()
                .collect(),
            None if rebuild_all => self.config.sort_fields.clone(),
            None => vec![],
        };
        let filter_configs: Vec<_> = match &filter_fields {
            Some(names) => self.config.filter_fields.iter()
                .filter(|fc| names.contains(&fc.name))
                .cloned()
                .collect(),
            None if rebuild_all => self.config.filter_fields.clone(),
            None => vec![],
        };

        let rebuilt_names: Vec<String> = sort_configs.iter().map(|c| c.name.clone())
            .chain(filter_configs.iter().map(|c| c.name.clone()))
            .collect();

        if sort_configs.is_empty() && filter_configs.is_empty() {
            return Ok((0, rebuilt_names));
        }

        eprintln!("rebuild: sort fields={:?}, filter fields={:?}",
            sort_configs.iter().map(|c| &c.name).collect::<Vec<_>>(),
            filter_configs.iter().map(|c| &c.name).collect::<Vec<_>>());

        // Get alive bitmap from current snapshot
        let snap = self.inner.load_full();
        let alive = {
            let mut tmp = (*snap).clone();
            tmp.slots.merge_alive();
            tmp.slots.alive_bitmap().clone()
        };
        let total_alive = alive.len();
        eprintln!("rebuild: {} alive slots to process", total_alive);

        // Parallel shard-based iteration using rayon fold+reduce.
        // Open a second read-only DocStore (no mutex) for parallel reads.
        let ds_path = self.docstore.lock().path().to_path_buf();
        let reader = DocStore::open(&ds_path)
            .map_err(|e| crate::error::BitdexError::DocStore(
                format!("open reader docstore: {e}")))?;

        let max_slot = alive.max().unwrap_or(0);
        let max_shard = max_slot >> 9; // SHARD_SHIFT = 9
        let num_shards = max_shard + 1;
        eprintln!("rebuild: {} shards to scan with rayon", num_shards);

        // Pre-build field name lists for efficient lookup in inner loop
        let sort_names: Vec<&str> = sort_configs.iter().map(|c| c.name.as_str()).collect();
        let sort_bits: Vec<usize> = sort_configs.iter().map(|c| c.bits as usize).collect();
        let filter_names: Vec<&str> = filter_configs.iter().map(|c| c.name.as_str()).collect();

        // Accumulator: per-sort-field pre-allocated layer bitmaps + filter map
        type FilterMap = HashMap<(usize, u64), RoaringBitmap>; // (field_idx, value) -> bm
        struct Accum {
            // sort_layers[field_idx][bit] = bitmap
            sort_layers: Vec<Vec<RoaringBitmap>>,
            filter_map: FilterMap,
            count: u64,
        }

        let make_accum = || Accum {
            sort_layers: sort_bits.iter().map(|&b| {
                (0..b).map(|_| RoaringBitmap::new()).collect()
            }).collect(),
            filter_map: FilterMap::new(),
            count: 0,
        };

        // Chunk shards into batches of 500 for rayon — reduces task overhead
        // while still getting good parallelism (239K/500 = ~479 tasks)
        let chunk_size = 500u32;
        let num_chunks = (num_shards + chunk_size - 1) / chunk_size;

        let merged = (0..num_chunks)
            .into_par_iter()
            .fold(make_accum, |mut acc, chunk_idx| {
                let shard_start = chunk_idx * chunk_size;
                let shard_end = std::cmp::min(shard_start + chunk_size, num_shards);

                for shard_id in shard_start..shard_end {
                    let docs = match reader.get_shard(shard_id) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                    for (slot_id, doc) in &docs {
                        if !alive.contains(*slot_id) {
                            continue;
                        }
                        // Filter bitmap extraction (indexed by position)
                        for (fi, &fname) in filter_names.iter().enumerate() {
                            if let Some(fv) = doc.fields.get(fname) {
                                match fv {
                                    crate::mutation::FieldValue::Single(v) => {
                                        if let Some(key) = value_to_bitmap_key(v) {
                                            acc.filter_map
                                                .entry((fi, key))
                                                .or_insert_with(RoaringBitmap::new)
                                                .insert(*slot_id);
                                        }
                                    }
                                    crate::mutation::FieldValue::Multi(vals) => {
                                        for v in vals {
                                            if let Some(key) = value_to_bitmap_key(v) {
                                                acc.filter_map
                                                    .entry((fi, key))
                                                    .or_insert_with(RoaringBitmap::new)
                                                    .insert(*slot_id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // Sort bitmap extraction (direct layer access, no HashMap)
                        for (si, &sname) in sort_names.iter().enumerate() {
                            if let Some(fv) = doc.fields.get(sname) {
                                if let crate::mutation::FieldValue::Single(ref v) = fv {
                                    if let Some(value) = value_to_sort_u32(v) {
                                        let num_bits = sort_bits[si];
                                        for bit in 0..num_bits {
                                            if (value >> bit) & 1 == 1 {
                                                acc.sort_layers[si][bit].insert(*slot_id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        acc.count += 1;
                    }
                }

                // Update progress (approximate — each thread reports its own count)
                progress.fetch_add(acc.count, Ordering::Relaxed);
                acc.count = 0; // Reset so we don't double-count on next chunk

                acc
            })
            .reduce(make_accum, |mut a, b| {
                // Merge sort layers via OR
                for (si, b_layers) in b.sort_layers.into_iter().enumerate() {
                    for (bit, bm) in b_layers.into_iter().enumerate() {
                        a.sort_layers[si][bit] |= bm;
                    }
                }
                // Merge filter maps
                for (key, bm) in b.filter_map {
                    a.filter_map.entry(key)
                        .and_modify(|existing| *existing |= &bm)
                        .or_insert(bm);
                }
                a.count += b.count;
                a
            });

        let slots_processed = progress.load(Ordering::Relaxed);

        let read_elapsed = t0.elapsed();
        eprintln!("rebuild: read phase complete in {:.1}s ({} slots, {:.0} slots/s)",
            read_elapsed.as_secs_f64(), slots_processed,
            slots_processed as f64 / read_elapsed.as_secs_f64());

        // Apply to staging: clone current snapshot, clear target fields, OR in rebuilt data
        let mut staging = self.clone_staging();

        // Clear and replace sort fields
        for sc in &sort_configs {
            staging.sorts.add_field(sc.clone()); // replaces with fresh empty field
        }
        // Clear and replace filter fields
        for fc in &filter_configs {
            staging.filters.add_field(fc.clone()); // replaces with fresh empty field
        }

        // Apply rebuilt filter bitmaps (keyed by field index)
        for ((fi, value), bitmap) in merged.filter_map {
            let fname = &filter_configs[fi].name;
            if let Some(field) = staging.filters.get_field_mut(fname) {
                field.or_bitmap(value, &bitmap);
            }
        }
        // Apply rebuilt sort layer bitmaps
        for (si, layers) in merged.sort_layers.into_iter().enumerate() {
            let sname = &sort_configs[si].name;
            if let Some(field) = staging.sorts.get_field_mut(sname) {
                for (bit, bitmap) in layers.into_iter().enumerate() {
                    if !bitmap.is_empty() {
                        field.or_layer(bit, &bitmap);
                    }
                }
            }
        }

        // Publish the rebuilt staging
        self.publish_staging(staging);

        // Remove rebuilt fields from pending lazy-load sets (they're now loaded)
        {
            let mut pending = self.pending_filter_loads.lock();
            for fc in &filter_configs {
                pending.remove(&fc.name);
            }
        }
        {
            let mut pending = self.pending_sort_loads.lock();
            for sc in &sort_configs {
                pending.remove(&sc.name);
            }
        }

        let total_elapsed = t0.elapsed();
        eprintln!("rebuild: complete in {:.1}s — {} slots, {} fields rebuilt",
            total_elapsed.as_secs_f64(), slots_processed, rebuilt_names.len());

        Ok((slots_processed, rebuilt_names))
    }

    /// Shutdown the flush and merge threads gracefully.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_handle.take() {
            handle.join().ok();
        }
        if let Some(handle) = self.merge_handle.take() {
            handle.join().ok();
        }
    }
}

impl Drop for ConcurrentEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::FieldValue;
    use crate::query::{SortClause, SortDirection, Value};
    use std::sync::Arc;
    use std::thread;

    fn test_config() -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            }],
            max_page_size: 100,
            flush_interval_us: 50, // Fast flush for tests
            channel_capacity: 10_000,
            ..Default::default()
        }
    }

    fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
        Document {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    /// Wait for the flush thread to apply all pending mutations.
    fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
        let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
        while std::time::Instant::now() < deadline {
            if engine.alive_count() == expected_alive {
                // Give one more flush cycle to ensure everything is settled
                thread::sleep(Duration::from_millis(2));
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        // Final check
        assert_eq!(
            engine.alive_count(),
            expected_alive,
            "timed out waiting for flush; alive_count={} expected={}",
            engine.alive_count(),
            expected_alive
        );
    }

    // ---- Basic correctness tests ----

    #[test]
    fn test_put_and_query() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_put_multiple_and_sorted_query() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            )
            .unwrap();
        engine
            .put(
                3,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(300))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 3, 500);

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                Some(&sort),
                10,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2, 3, 1]); // 500, 300, 100
    }

    #[test]
    fn test_delete() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(1)),
                )]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(1)),
                )]),
            )
            .unwrap();

        wait_for_flush(&engine, 2, 500);

        engine.delete(1).unwrap();

        // Wait for delete to be flushed
        wait_for_flush(&engine, 1, 500);

        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2]);
    }

    #[test]
    fn test_upsert_correctness() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        // Initial insert
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(10))),
                ]),
            )
            .unwrap();

        // Must wait for first put to be fully flushed (alive bit set)
        // before doing upsert, otherwise the second put won't detect is_alive=true
        wait_for_flush(&engine, 1, 500);

        // Verify first insert is visible
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);

        // Upsert with new values — now the alive bit is set so diff will detect upsert
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                    ("reactionCount", FieldValue::Single(Value::Integer(99))),
                ]),
            )
            .unwrap();

        // Wait for upsert flush. alive_count stays 1 so we need a different signal.
        // Shutdown ensures final flush completes.
        engine.shutdown();

        // Old value should not match
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert!(result.ids.is_empty());

        // New value should match
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(2),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_execute_query() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        let query = BitdexQuery {
            filters: vec![FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            sort: Some(SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            limit: 50,
            cursor: None,
            offset: None,
        };

        let result = engine.execute_query(&query).unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    // ---- Concurrency tests ----

    #[test]
    fn test_concurrent_puts() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
        let num_threads = 4;
        let docs_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    for i in 0..docs_per_thread {
                        let id = (t * docs_per_thread + i + 1) as u32;
                        engine
                            .put(
                                id,
                                &make_doc(vec![
                                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                                    (
                                        "reactionCount",
                                        FieldValue::Single(Value::Integer(id as i64)),
                                    ),
                                ]),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let total = (num_threads * docs_per_thread) as u64;
        wait_for_flush(&engine, total, 2000);

        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.total_matched, total);
    }

    #[test]
    fn test_concurrent_reads_during_writes() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        // Pre-populate some docs
        for i in 1..=10u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 10)),
                        ),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 10, 500);

        // Spawn writer threads adding more docs
        let writer_handles: Vec<_> = (0..2)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    for i in 0..25 {
                        let id = 100 + t * 25 + i;
                        engine
                            .put(
                                id as u32,
                                &make_doc(vec![
                                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                                    (
                                        "reactionCount",
                                        FieldValue::Single(Value::Integer(id as i64)),
                                    ),
                                ]),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();

        // Spawn reader threads querying concurrently
        let reader_handles: Vec<_> = (0..4)
            .map(|_| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    let mut success_count = 0;
                    for _ in 0..50 {
                        let result = engine.query(
                            &[FilterClause::Eq(
                                "nsfwLevel".to_string(),
                                Value::Integer(1),
                            )],
                            None,
                            100,
                        );
                        assert!(result.is_ok(), "query should not fail");
                        success_count += 1;
                        thread::yield_now();
                    }
                    success_count
                })
            })
            .collect();

        for h in writer_handles {
            h.join().unwrap();
        }
        for h in reader_handles {
            let count = h.join().unwrap();
            assert_eq!(count, 50, "all reader queries should succeed");
        }
    }

    #[test]
    fn test_concurrent_mixed_read_write() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    for i in 0..20 {
                        if t % 2 == 0 {
                            // Writer
                            let id = (t * 20 + i + 1) as u32;
                            engine
                                .put(
                                    id,
                                    &make_doc(vec![(
                                        "nsfwLevel",
                                        FieldValue::Single(Value::Integer(1)),
                                    )]),
                                )
                                .unwrap();
                        } else {
                            // Reader
                            let _ = engine.query(
                                &[FilterClause::Eq(
                                    "nsfwLevel".to_string(),
                                    Value::Integer(1),
                                )],
                                None,
                                100,
                            );
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // No panics = success for concurrency safety
    }

    #[test]
    fn test_shutdown_flushes_remaining() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        for i in 1..=5u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![(
                        "nsfwLevel",
                        FieldValue::Single(Value::Integer(1)),
                    )]),
                )
                .unwrap();
        }

        // Shutdown triggers final flush
        engine.shutdown();

        assert_eq!(engine.alive_count(), 5);
    }

    #[test]
    fn test_multi_value_filter() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                )]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)]),
                )]),
            )
            .unwrap();

        wait_for_flush(&engine, 2, 500);

        // Query for tag 200 - should match both
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 2);

        // Query for tag 100 - should match only doc 1
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_merge_thread_starts_and_stops() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        // Just verify it starts and shuts down cleanly
        engine.shutdown();
    }

    #[test]
    fn test_two_threads_independent() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        // Insert a doc to exercise the flush thread
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Query to verify flush worked while merge thread is also running
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert!(result.ids.contains(&1));
    }

    // ---- S1.8: Integration tests for diff accumulation and merge compaction ----

    /// S1.8-1: Filter diffs are visible (dirty) in published snapshot after flush,
    /// and queries still return correct results via diff fusion.
    #[test]
    fn test_filter_diffs_visible_in_snapshot() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert a document
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(100)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Query should return correct results via diff fusion
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);

        // Verify the published snapshot's filter field has a dirty diff
        let snap = engine.snapshot_public();
        let field = snap.filters.get_field("nsfwLevel").unwrap();
        let vb = field.get_versioned(1).unwrap();
        // Between flush cycles and compaction, the diff should be dirty
        // (unless compaction just ran). The key assertion is that queries work.
        assert!(vb.contains(1), "slot 1 should be in nsfwLevel=1 bitmap");
    }

    /// S1.8-2: After compaction, filter diffs are merged into base.
    /// Wait long enough for the periodic compaction (COMPACTION_INTERVAL cycles).
    #[test]
    fn test_merge_compaction_cleans_diffs() {
        let mut cfg = test_config();
        cfg.flush_interval_us = 10; // Very fast flush so compaction triggers quickly
        let engine = ConcurrentEngine::new(cfg).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(50)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Wait for compaction to happen (50 cycles * 10μs = 500μs + overhead)
        // Give generous time for thread scheduling
        thread::sleep(Duration::from_millis(50));

        // Query should still be correct after compaction
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(5),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);

        // Check that the diff was compacted (base contains the bit)
        let snap = engine.snapshot_public();
        let field = snap.filters.get_field("nsfwLevel").unwrap();
        let vb = field.get_versioned(5).unwrap();
        // After compaction, the base should contain the bit
        assert!(vb.base().contains(1), "slot 1 should be in base after compaction");
    }

    /// S1.8-3: Sort layers are always clean (never dirty) in published snapshots.
    #[test]
    fn test_sort_layers_always_clean() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert several docs with different sort values
        for i in 1..=10u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("onSite", FieldValue::Single(Value::Bool(true))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 100)),
                        ),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 10, 500);

        // Verify sort layers are clean
        let snap = engine.snapshot_public();
        let sort_field = snap.sorts.get_field("reactionCount").unwrap();
        for bit_pos in 0..32usize {
            if let Some(layer) = sort_field.layer(bit_pos) {
                // layer() has an internal debug_assert that panics if dirty.
                // If we get here, the layer is clean. Verify it's accessible.
                let _ = layer.len();
            }
        }
    }

    /// S1.8-4: Filter diffs accumulate across multiple flush cycles.
    #[test]
    fn test_filter_diffs_accumulate_across_flushes() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert doc A
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(10)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Insert doc B with same nsfwLevel
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
                    ("onSite", FieldValue::Single(Value::Bool(false))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(20)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 2, 500);

        // Query should return both docs
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(3),
                )],
                None,
                100,
            )
            .unwrap();
        let mut ids = result.ids.clone();
        ids.sort();
        assert_eq!(ids, vec![1, 2], "both docs should match nsfwLevel=3");
    }

    /// S1.8-5: Concurrent reads during mutations return correct results.
    #[test]
    fn test_concurrent_reads_during_mutations() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        // Insert initial docs
        for i in 1..=20u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64 + 1))),
                        ("onSite", FieldValue::Single(Value::Bool(i % 2 == 0))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64)),
                        ),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 20, 1000);

        // Spawn reader threads that query continuously
        let mut handles = Vec::new();
        for _ in 0..4 {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    // Query should never panic or return inconsistent results
                    let result = eng
                        .query(
                            &[FilterClause::Eq(
                                "nsfwLevel".to_string(),
                                Value::Integer(1),
                            )],
                            None,
                            100,
                        )
                        .unwrap();
                    // Results should be non-empty (we inserted docs with nsfwLevel=1)
                    assert!(!result.ids.is_empty(), "query returned empty during concurrent reads");
                    thread::sleep(Duration::from_micros(100));
                }
            }));
        }

        // Concurrently insert more docs
        for i in 21..=40u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64 + 1))),
                        ("onSite", FieldValue::Single(Value::Bool(i % 2 == 0))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64)),
                        ),
                    ]),
                )
                .unwrap();
            thread::sleep(Duration::from_micros(200));
        }

        // Wait for all readers to finish
        for h in handles {
            h.join().unwrap();
        }

        // Final verification
        wait_for_flush(&engine, 40, 1000);
        let result = engine.query(&[], None, 1000).unwrap();
        assert_eq!(result.ids.len(), 40, "all 40 docs should be alive");
    }

    // ---- put_bulk tests ----

    #[test]
    fn test_put_bulk_basic() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = (1..=100u32)
            .map(|i| {
                (
                    i,
                    make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 5) as i64 + 1))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 10)),
                        ),
                    ]),
                )
            })
            .collect();

        let (count, ds_handle) = engine.put_bulk(docs, 4).unwrap();
        ds_handle.join().unwrap();
        assert_eq!(count, 100);
        assert_eq!(engine.alive_count(), 100);

        // Filter query
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                1000,
            )
            .unwrap();
        assert_eq!(result.total_matched, 20); // 1,6,11,...,96 → 20 docs

        // Sorted query
        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                Some(&sort),
                3,
            )
            .unwrap();
        // Top 3 by reactionCount desc with nsfwLevel=1: slots 100(1000), 95(950), 90(900)
        assert_eq!(result.ids, vec![100, 95, 90]);
    }

    #[test]
    fn test_put_bulk_with_multi_value() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs = vec![
            (
                1,
                make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                )]),
            ),
            (
                2,
                make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)]),
                )]),
            ),
            (
                3,
                make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(300)]),
                )]),
            ),
        ];

        let (_, ds_handle) = engine.put_bulk(docs, 2).unwrap();
        ds_handle.join().unwrap();

        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 2); // docs 1 and 2

        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 2); // docs 1 and 3
    }

    #[test]
    fn test_put_bulk_single_thread() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = (1..=10u32)
            .map(|i| {
                (
                    i,
                    make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64)),
                        ),
                    ]),
                )
            })
            .collect();

        let (count, ds_handle) = engine.put_bulk(docs, 1).unwrap();
        ds_handle.join().unwrap();
        assert_eq!(count, 10);
        assert_eq!(engine.alive_count(), 10);
    }

    #[test]
    fn test_put_bulk_then_query_with_sort() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = vec![
            (
                10,
                make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            ),
            (
                20,
                make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            ),
            (
                30,
                make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(300))),
                ]),
            ),
        ];

        let (_, ds_handle) = engine.put_bulk(docs, 2).unwrap();
        ds_handle.join().unwrap();

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                Some(&sort),
                10,
            )
            .unwrap();
        assert_eq!(result.ids, vec![10, 30, 20]); // 500, 300, 100
    }

    #[test]
    fn test_put_bulk_persists_to_docstore() {
        // Verify that put_bulk() persists docs so subsequent put() upserts can diff correctly.
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = vec![
            (1, make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(100))),
            ])),
            (2, make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                ("reactionCount", FieldValue::Single(Value::Integer(200))),
            ])),
        ];

        let (count, ds_handle) = engine.put_bulk(docs, 2).unwrap();
        ds_handle.join().unwrap(); // Wait for docstore persistence
        assert_eq!(count, 2);

        // put_bulk publishes directly — bitmaps visible immediately
        assert_eq!(engine.alive_count(), 2);

        // Verify initial state: nsfwLevel=1 should match slot 1
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
            None, 10,
        ).unwrap();
        assert_eq!(result.ids, vec![1]);

        // Now upsert slot 1 with changed nsfwLevel (1 → 3).
        // This requires docstore to have the old doc so it can clear the nsfwLevel=1 bitmap bit.
        let updated = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]);
        engine.put(1, &updated).unwrap();
        wait_for_flush(&engine, 2, 5_000);

        // nsfwLevel=1 should now be EMPTY (slot 1 moved to nsfwLevel=3)
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
            None, 10,
        ).unwrap();
        assert_eq!(result.total_matched, 0, "Stale nsfwLevel=1 bit not cleared — docstore persistence failed");

        // nsfwLevel=3 should match slot 1
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(3))],
            None, 10,
        ).unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_put_bulk_loading_then_persist() {
        // Verify that put_bulk_loading + manual docstore persistence works correctly.
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = vec![
            (1, make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(100))),
            ])),
            (2, make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                ("reactionCount", FieldValue::Single(Value::Integer(200))),
            ])),
        ];

        // Use loading mode
        let mut staging = engine.clone_staging();
        let count = engine.put_bulk_loading(&mut staging, &docs, 2);
        assert_eq!(count, 2);

        // Persist docs separately
        let ds_handle = engine.spawn_docstore_writer(docs);
        ds_handle.join().unwrap();

        // Publish staging
        engine.publish_staging(staging);

        // Bitmaps visible immediately after publish
        assert_eq!(engine.alive_count(), 2);

        // Verify initial state
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
            None, 10,
        ).unwrap();
        assert_eq!(result.ids, vec![1]);

        // Upsert slot 1 with changed nsfwLevel
        let updated = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]);
        engine.put(1, &updated).unwrap();
        wait_for_flush(&engine, 2, 5_000);

        // Verify diff worked correctly
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
            None, 10,
        ).unwrap();
        assert_eq!(result.total_matched, 0, "Stale nsfwLevel=1 bit not cleared — docstore persistence failed");

        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(3))],
            None, 10,
        ).unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    // ---- Snapshot save/restore tests ----

    fn test_config_with_bitmap_path(bitmap_path: std::path::PathBuf) -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            }],
            max_page_size: 100,
            flush_interval_us: 50,
            channel_capacity: 10_000,
            storage: crate::config::StorageConfig {
                bitmap_path: Some(bitmap_path),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_save_snapshot_no_bitmap_store_returns_error() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();
        let result = engine.save_snapshot();
        assert!(result.is_err(), "save_snapshot should fail without bitmap_path");
    }

    #[test]
    fn test_save_snapshot_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        // Phase 1: Create engine, insert data, save snapshot
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

            engine
                .put(
                    1,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                        ("onSite", FieldValue::Single(Value::Bool(true))),
                        ("reactionCount", FieldValue::Single(Value::Integer(500))),
                    ]),
                )
                .unwrap();
            engine
                .put(
                    2,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                        ("tagIds", FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)])),
                        ("onSite", FieldValue::Single(Value::Bool(false))),
                        ("reactionCount", FieldValue::Single(Value::Integer(100))),
                    ]),
                )
                .unwrap();
            engine
                .put(
                    3,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("tagIds", FieldValue::Multi(vec![Value::Integer(100)])),
                        ("onSite", FieldValue::Single(Value::Bool(true))),
                        ("reactionCount", FieldValue::Single(Value::Integer(300))),
                    ]),
                )
                .unwrap();

            // Shutdown to ensure all mutations are flushed and published
            engine.shutdown();

            // Verify data is visible before saving
            assert_eq!(engine.alive_count(), 3);

            // Save the snapshot
            engine.save_snapshot().unwrap();
        }

        // Phase 2: Create a NEW engine from the same config+paths and verify restoration
        {
            let engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

            // Verify alive count restored
            assert_eq!(
                engine.alive_count(),
                3,
                "alive count should be restored from snapshot"
            );

            // Verify slot counter restored
            assert_eq!(
                engine.slot_counter(),
                4,
                "slot counter should be restored (next_slot = max_id + 1)"
            );

            // Verify filter queries work
            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    None,
                    100,
                )
                .unwrap();
            let mut ids = result.ids.clone();
            ids.sort();
            assert_eq!(ids, vec![1, 3], "nsfwLevel=1 should match docs 1 and 3");

            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(2))],
                    None,
                    100,
                )
                .unwrap();
            assert_eq!(result.ids, vec![2], "nsfwLevel=2 should match doc 2");

            // Verify multi-value filter
            let result = engine
                .query(
                    &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                    None,
                    100,
                )
                .unwrap();
            assert_eq!(
                result.total_matched, 2,
                "tagIds=200 should match docs 1 and 2"
            );

            // Verify boolean filter
            let result = engine
                .query(
                    &[FilterClause::Eq("onSite".to_string(), Value::Bool(true))],
                    None,
                    100,
                )
                .unwrap();
            let mut ids = result.ids.clone();
            ids.sort();
            assert_eq!(ids, vec![1, 3], "onSite=true should match docs 1 and 3");

            // Verify sort works correctly (descending reactionCount)
            let sort = SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            };
            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    Some(&sort),
                    10,
                )
                .unwrap();
            assert_eq!(
                result.ids,
                vec![1, 3],
                "sort desc should return 500 (doc 1) before 300 (doc 3)"
            );
        }
    }

    #[test]
    fn test_save_snapshot_to_custom_path() {
        let dir = tempfile::tempdir().unwrap();
        let custom_bitmap_path = dir.path().join("custom_bitmaps");

        // Create engine without bitmap_path (in-memory only)
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
                    ("reactionCount", FieldValue::Single(Value::Integer(99))),
                ]),
            )
            .unwrap();

        engine.shutdown();
        assert_eq!(engine.alive_count(), 2);

        // Save to custom path
        engine.save_snapshot_to(&custom_bitmap_path).unwrap();

        // Verify the file was created and contains the data
        let store = crate::bitmap_fs::BitmapFs::new(&custom_bitmap_path).unwrap();
        let alive = store.load_alive().unwrap().unwrap();
        assert_eq!(alive.len(), 2, "alive bitmap should have 2 entries");
        assert!(alive.contains(1));
        assert!(alive.contains(2));

        let counter = store.load_slot_counter().unwrap().unwrap();
        assert!(counter >= 3, "slot counter should be at least 3");

        let nsfw = store.load_field("nsfwLevel").unwrap();
        assert!(nsfw.contains_key(&5), "nsfwLevel=5 should exist");
        assert_eq!(nsfw[&5].len(), 2, "nsfwLevel=5 should have 2 entries");

        let sort_layers = store.load_sort_layers("reactionCount", 32).unwrap();
        assert!(sort_layers.is_some(), "sort layers should be persisted");
    }

    #[test]
    fn test_save_snapshot_empty_engine() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        // Save snapshot of empty engine
        {
            let engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            engine.save_snapshot().unwrap();
        }

        // Restore from empty snapshot
        {
            let engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            assert_eq!(engine.alive_count(), 0, "empty snapshot should restore to 0 alive");
            assert_eq!(engine.slot_counter(), 0, "empty snapshot should restore counter to 0");
        }
    }

    #[test]
    fn test_save_snapshot_after_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        // Insert 3 docs, delete 1, then save and restore
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

            for i in 1..=3u32 {
                engine
                    .put(
                        i,
                        &make_doc(vec![
                            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                            ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 10))),
                        ]),
                    )
                    .unwrap();
            }

            wait_for_flush(&engine, 3, 500);

            // Delete doc 2
            engine.delete(2).unwrap();
            wait_for_flush(&engine, 2, 500);

            engine.shutdown();
            engine.save_snapshot().unwrap();
        }

        // Restore and verify
        {
            let engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

            assert_eq!(engine.alive_count(), 2, "should have 2 alive after delete");

            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    None,
                    100,
                )
                .unwrap();
            let mut ids = result.ids.clone();
            ids.sort();
            assert_eq!(ids, vec![1, 3], "deleted doc 2 should not appear");
        }
    }

    #[test]
    fn test_save_snapshot_preserves_sort_values() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        // Insert docs with specific sort values
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

            engine
                .put(
                    1,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("reactionCount", FieldValue::Single(Value::Integer(100))),
                    ]),
                )
                .unwrap();
            engine
                .put(
                    2,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("reactionCount", FieldValue::Single(Value::Integer(500))),
                    ]),
                )
                .unwrap();
            engine
                .put(
                    3,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("reactionCount", FieldValue::Single(Value::Integer(300))),
                    ]),
                )
                .unwrap();

            engine.shutdown();
            engine.save_snapshot().unwrap();
        }

        // Restore and verify sort order is preserved
        {
            let engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

            let sort = SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            };
            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    Some(&sort),
                    10,
                )
                .unwrap();
            assert_eq!(
                result.ids,
                vec![2, 3, 1],
                "descending sort should be 500, 300, 100 after restore"
            );

            let sort_asc = SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Asc,
            };
            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    Some(&sort_asc),
                    10,
                )
                .unwrap();
            assert_eq!(
                result.ids,
                vec![1, 3, 2],
                "ascending sort should be 100, 300, 500 after restore"
            );
        }
    }

    // ---- Named cursor tests ----

    #[test]
    fn test_cursor_set_and_get() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // No cursor initially
        assert!(engine.get_cursor("pg-sync-0").is_none());
        assert!(engine.get_all_cursors().is_empty());

        // Set a cursor
        engine.set_cursor("pg-sync-0".to_string(), "12345".to_string());
        assert_eq!(engine.get_cursor("pg-sync-0").unwrap(), "12345");

        // Set another
        engine.set_cursor("pg-sync-1".to_string(), "12300".to_string());
        let all = engine.get_all_cursors();
        assert_eq!(all.len(), 2);
        assert_eq!(all["pg-sync-0"], "12345");
        assert_eq!(all["pg-sync-1"], "12300");

        // Overwrite
        engine.set_cursor("pg-sync-0".to_string(), "12400".to_string());
        assert_eq!(engine.get_cursor("pg-sync-0").unwrap(), "12400");
    }

    #[test]
    fn test_cursor_persists_via_merge_thread() {
        // Create engine with on-disk bitmap store so merge thread can persist
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let doc_path = dir.path().join("docs");
        std::fs::create_dir_all(&bitmap_path).unwrap();
        std::fs::create_dir_all(&doc_path).unwrap();

        let mut config = test_config();
        config.storage.bitmap_path = Some(bitmap_path.clone());
        config.merge_interval_ms = 100; // fast merge for test

        let engine = ConcurrentEngine::new_with_path(config.clone(), &doc_path).unwrap();

        // Set a cursor
        engine.set_cursor("pg-sync-0".to_string(), "99999".to_string());

        // Wait for merge thread to checkpoint (merge interval + margin)
        thread::sleep(Duration::from_millis(300));

        // Verify cursor was written to disk
        let store = BitmapFs::new(&bitmap_path).unwrap();
        let on_disk = store.load_cursor("pg-sync-0").unwrap();
        assert_eq!(on_disk.unwrap(), "99999");

        drop(engine);

        // Create a new engine from the same path — cursor should be loaded
        let engine2 = ConcurrentEngine::new_with_path(config, &doc_path).unwrap();
        assert_eq!(engine2.get_cursor("pg-sync-0").unwrap(), "99999");
    }

    #[test]
    fn test_save_and_unload_then_query() {
        // Verify: save_and_unload drops bitmap memory but queries still work via lazy reload.
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

        // Insert test data
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)])),
                    ("onSite", FieldValue::Single(Value::Bool(false))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            )
            .unwrap();
        engine
            .put(
                3,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(100)])),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    ("reactionCount", FieldValue::Single(Value::Integer(300))),
                ]),
            )
            .unwrap();

        engine.shutdown();
        assert_eq!(engine.alive_count(), 3);

        // Capture pre-unload bitmap memory
        let bytes_before = {
            let snap = engine.inner.load_full();
            snap.filters.bitmap_bytes() + snap.sorts.bitmap_bytes()
        };
        assert!(bytes_before > 0, "should have bitmap data before unload");

        // Save and unload
        engine.save_and_unload().unwrap();

        // Verify bitmap memory dropped
        let bytes_after = {
            let snap = engine.inner.load_full();
            snap.filters.bitmap_bytes() + snap.sorts.bitmap_bytes()
        };
        assert!(
            bytes_after < bytes_before,
            "bitmap bytes should drop after unload: {} -> {}",
            bytes_before,
            bytes_after
        );

        // Verify fields are marked as pending
        assert!(
            !engine.pending_filter_loads.lock().is_empty(),
            "filter fields should be pending after unload"
        );
        assert!(
            !engine.pending_sort_loads.lock().is_empty(),
            "sort fields should be pending after unload"
        );

        // Query should still work via lazy reload
        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: crate::query::SortDirection::Desc,
        };
        let filters = vec![FilterClause::Eq(
            "nsfwLevel".to_string(),
            Value::Integer(1),
        )];
        let result = engine.query(&filters, Some(&sort), 10).unwrap();

        assert_eq!(result.ids, vec![1, 3], "query after unload should match pre-unload results");
    }

    #[test]
    fn test_save_and_unload_mutation_race() {
        // Verify: mutations during unloaded state are preserved after lazy reload.
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        let mut engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

        // Insert initial data
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            )
            .unwrap();

        engine.shutdown();

        // Save and unload
        engine.save_and_unload().unwrap();

        // Mutate while fields are unloaded — directly at the data structure level
        {
            let mut staging = engine.clone_staging();
            // Simulate a mutation: add nsfwLevel=1 for slot 10
            if let Some(field) = staging.filters.get_field_mut("nsfwLevel") {
                field.insert(1, 10);
            }
            engine.publish_staging(staging);
        }

        // The mutation (slot 10 in nsfwLevel=1) should be visible in the diff
        let snap = engine.inner.load_full();
        let field = snap.filters.get_field("nsfwLevel").unwrap();
        let vb = field.get_versioned(1).unwrap();
        assert!(vb.contains(10), "mutation during unloaded state should be visible");
    }

    #[test]
    fn test_save_and_unload_memory_drops_with_flush_thread_running() {
        // Regression test: save_and_unload must drop bitmap memory even when
        // the flush thread is still running. Previously, the flush thread's
        // private staging held the old data and re-inflated on next publish.
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        let engine = Arc::new(
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap(),
        );

        // Bulk insert via loading mode (the real-world path)
        engine.enter_loading_mode();
        for i in 1u32..=500 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 5) as i64))),
                        ("tagIds", FieldValue::Multi(vec![
                            Value::Integer((i % 100) as i64),
                            Value::Integer((i % 50 + 200) as i64),
                        ])),
                        ("onSite", FieldValue::Single(Value::Bool(i % 2 == 0))),
                        ("reactionCount", FieldValue::Single(Value::Integer(i as i64))),
                    ]),
                )
                .unwrap();
        }
        engine.exit_loading_mode();
        // Flush thread is still running — this is the key difference from
        // test_save_and_unload_then_query which calls shutdown() first.

        // Capture pre-unload memory from the published snapshot
        let (_, filter_before, sort_before, _, _, _, _) = engine.bitmap_memory_report();
        let total_before = filter_before + sort_before;
        assert!(total_before > 0, "should have bitmap data before unload");

        // Save and unload (flush thread still alive)
        engine.save_and_unload().unwrap();

        // Give the flush thread a few cycles to potentially re-inflate
        thread::sleep(Duration::from_millis(50));

        // Verify memory dropped in the published snapshot
        let (_, filter_after, sort_after, _, _, _, _) = engine.bitmap_memory_report();
        let total_after = filter_after + sort_after;
        assert!(
            total_after < total_before / 2,
            "bitmap memory should drop significantly after save_and_unload \
             (before={total_before}, after={total_after}). \
             If this fails, the flush thread's staging is re-inflating the snapshot."
        );

        // Verify queries still work via lazy reload
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(0))],
                Some(&SortClause {
                    field: "reactionCount".to_string(),
                    direction: crate::query::SortDirection::Desc,
                }),
                10,
            )
            .unwrap();
        assert!(!result.ids.is_empty(), "query should work after unload via lazy reload");

        // After lazy reload, memory comes back for queried fields only
        let (_, filter_reloaded, sort_reloaded, _, _, _, _) = engine.bitmap_memory_report();
        assert!(
            filter_reloaded + sort_reloaded > 0,
            "queried fields should be back in memory after lazy reload"
        );
    }

    #[test]
    fn test_exit_loading_mode_publishes_before_returning() {
        // Regression test: exit_loading_mode must guarantee the published
        // snapshot contains all mutations before returning. Previously it
        // just set an atomic flag and hoped the flush thread would catch up.
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        let engine =
            ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();

        engine.enter_loading_mode();
        for i in 1u32..=100 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("reactionCount", FieldValue::Single(Value::Integer(i as i64))),
                    ]),
                )
                .unwrap();
        }
        engine.exit_loading_mode();

        // Immediately after exit_loading_mode, the published snapshot must
        // contain all 100 records — no timing gap.
        assert_eq!(
            engine.alive_count(),
            100,
            "all records should be visible immediately after exit_loading_mode"
        );

        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                200,
            )
            .unwrap();
        assert_eq!(
            result.ids.len(),
            100,
            "query should return all 100 records immediately after exit_loading_mode"
        );
    }

    // ---- Regression tests for reliability fixes ----

    /// Regression test: delete() marks slots in-flight (just like put()),
    /// preventing concurrent readers from seeing partially-applied delete
    /// mutations.
    #[test]
    fn test_concurrent_put_delete_in_flight_race() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        let num_docs = 20u32;
        for id in 1..=num_docs {
            engine
                .put(
                    id,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((id % 3 + 1) as i64))),
                        ("reactionCount", FieldValue::Single(Value::Integer(id as i64 * 10))),
                    ]),
                )
                .unwrap();
        }
        wait_for_flush(&engine, num_docs as u64, 1000);

        let iterations = 100;
        let query_error_count = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let put_handles: Vec<_> = (0..4)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    let base = 100 + t * iterations;
                    for i in 0..iterations {
                        let id = (base + i) as u32;
                        let val = (i % 5 + 1) as i64;
                        engine
                            .put(
                                id,
                                &make_doc(vec![
                                    ("nsfwLevel", FieldValue::Single(Value::Integer(val))),
                                    ("reactionCount", FieldValue::Single(Value::Integer(val * 10))),
                                ]),
                            )
                            .ok();
                        thread::yield_now();
                    }
                })
            })
            .collect();

        let delete_handles: Vec<_> = (0..4)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    let start = t * 5 + 1;
                    for id in start..start + 5 {
                        engine.delete(id as u32).ok();
                        thread::yield_now();
                    }
                })
            })
            .collect();

        let reader_handles: Vec<_> = (0..4)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let errors = Arc::clone(&query_error_count);
                thread::spawn(move || {
                    for _ in 0..200 {
                        for val in 1..=5i64 {
                            match engine.query(
                                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(val))],
                                None,
                                1000,
                            ) {
                                Ok(_) => {}
                                Err(_) => { errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                            }
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        for h in put_handles { h.join().unwrap(); }
        for h in delete_handles { h.join().unwrap(); }
        for h in reader_handles { h.join().unwrap(); }

        assert_eq!(query_error_count.load(std::sync::atomic::Ordering::Relaxed), 0);

        let mut engine = Arc::try_unwrap(engine).ok().expect("refcount 1");
        engine.shutdown();

        let expected_alive = 400u64;
        assert_eq!(engine.alive_count(), expected_alive);

        let mut all_found: Vec<i64> = Vec::new();
        for val in 1..=5i64 {
            let result = engine
                .query(&[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(val))], None, 1000)
                .unwrap();
            all_found.extend_from_slice(&result.ids);
        }
        all_found.sort();
        all_found.dedup();
        assert_eq!(all_found.len(), expected_alive as usize);

        for id in 1..=num_docs as i64 {
            assert!(!all_found.contains(&id), "deleted slot {} found in filter query", id);
        }
    }

    /// Regression test: lazy field loading via rcu() must not clobber
    /// concurrent flush thread mutations.
    #[test]
    fn test_lazy_load_under_flush_pressure_rcu() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());

        // Phase 1: Create engine, insert seed data, save snapshot
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            for i in 1..=10u32 {
                engine
                    .put(
                        i,
                        &make_doc(vec![
                            ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3 + 1) as i64))),
                            ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 100))),
                        ]),
                    )
                    .unwrap();
            }
            engine.shutdown();
            assert_eq!(engine.alive_count(), 10);
            engine.save_snapshot().unwrap();
        }

        // Phase 2: Restore into new engine, concurrent lazy loads + mutations
        {
            let engine = Arc::new(
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap(),
            );
            assert_eq!(engine.alive_count(), 10);

            let mutation_ids: Vec<u32> = (20..30).collect();
            let query_engine = Arc::clone(&engine);
            let mutate_engine = Arc::clone(&engine);

            let query_handle = thread::spawn(move || {
                for _ in 0..50 {
                    let _ = query_engine.query(
                        &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                        Some(&SortClause { field: "reactionCount".to_string(), direction: SortDirection::Desc }),
                        100,
                    );
                    thread::yield_now();
                }
            });

            let mutate_handle = thread::spawn(move || {
                for &id in &mutation_ids {
                    mutate_engine
                        .put(
                            id,
                            &make_doc(vec![
                                ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
                                ("reactionCount", FieldValue::Single(Value::Integer(id as i64 * 10))),
                            ]),
                        )
                        .unwrap();
                    thread::yield_now();
                }
            });

            query_handle.join().unwrap();
            mutate_handle.join().unwrap();
            wait_for_flush(&engine, 20, 2000);

            let result = engine
                .query(&[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(5))], None, 100)
                .unwrap();
            let mut found_ids: Vec<i64> = result.ids.clone();
            found_ids.sort();
            let expected_ids: Vec<i64> = (20..30).map(|x| x as i64).collect();
            assert_eq!(found_ids, expected_ids, "all 10 mutations must survive lazy load");

            let result = engine
                .query(&[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))], None, 100)
                .unwrap();
            assert!(!result.ids.is_empty(), "seed data should be queryable after lazy load");

            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(5))],
                    Some(&SortClause { field: "reactionCount".to_string(), direction: SortDirection::Desc }),
                    100,
                )
                .unwrap();
            assert_eq!(result.ids.len(), 10);
            assert_eq!(result.ids[0], 29, "slot 29 should be first in desc sort");
        }
    }
}
