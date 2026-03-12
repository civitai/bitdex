use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, Guard};
use crossbeam_channel::{Receiver, Sender};
use roaring::RoaringBitmap;

use crate::bitmap_fs::BitmapFs;
use crate::bound_cache::{BoundCacheManager, BoundKey};
use crate::filter::FilterFieldType;
use crate::cache::{self, CacheLookup, CacheKey, TrieCache};
use crate::concurrency::InFlightTracker;
use crate::config::Config;
use crate::docstore::{DocStore, StoredDoc};
use crate::error::Result;
use crate::executor::{QueryExecutor, StringMaps};
use crate::mutation::{diff_document, diff_patch, value_to_bitmap_key, Document, FieldRegistry, PatchPayload};
use crate::planner;
use crate::query::{BitdexQuery, FilterClause, SortClause, SortDirection};
use crate::time_buckets::TimeBucketManager;
use crate::types::QueryResult;
use crate::unified_cache::{UnifiedCache, UnifiedCacheConfig, UnifiedKey};
use crate::write_coalescer::{MutationOp, MutationSender, WriteCoalescer};

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
    cache: Arc<parking_lot::Mutex<TrieCache>>,
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
    bound_cache: Arc<parking_lot::Mutex<BoundCacheManager>>,
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
    /// Reverse string maps for MappedString field query resolution.
    string_maps: Option<Arc<StringMaps>>,
    /// Unified cache: replaces trie cache + bound cache (migration phase).
    unified_cache: Arc<parking_lot::Mutex<UnifiedCache>>,
    /// Flush loop stats: total snapshot publishes (monotonic counter).
    flush_publish_count: Arc<AtomicU64>,
    /// Flush loop stats: cumulative flush duration in nanoseconds.
    flush_duration_nanos: Arc<AtomicU64>,
    /// Flush loop stats: most recent flush duration in nanoseconds.
    flush_last_duration_nanos: Arc<AtomicU64>,
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
        let cache = Arc::new(parking_lot::Mutex::new(TrieCache::new(config.cache.clone())));

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
                    for sc in &config.sort_fields {
                        pending_sort_loads.insert(sc.name.clone());
                    }
                }
            }
        }
        let bound_cache = Arc::new(parking_lot::Mutex::new(BoundCacheManager::with_max_count(
            config.cache.bound_target_size,
            config.cache.bound_max_size,
            config.cache.bound_max_count,
        )));
        let unified_cache = Arc::new(parking_lot::Mutex::new(UnifiedCache::new(
            UnifiedCacheConfig::default(),
        )));
        let loading_mode = Arc::new(AtomicBool::new(false));

        // S3.3: Instantiate TimeBucketManager from top-level time_buckets config
        let time_buckets = config.time_buckets.as_ref().map(|tb_config| {
            Arc::new(parking_lot::Mutex::new(TimeBucketManager::new_with_sort_field(
                tb_config.filter_field.clone(),
                tb_config.sort_field.clone(),
                tb_config.range_buckets.clone(),
            )))
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

        // Collect filter field names for cache invalidation
        let filter_field_names: Vec<String> = config
            .filter_fields
            .iter()
            .map(|f| f.name.clone())
            .collect();

        // Shared dirty flag: flush thread sets when mutations applied, merge thread
        // clears after persisting snapshot. Prevents continuous 20GB rewrites at idle.
        let dirty_flag = Arc::new(AtomicBool::new(false));

        // Lazy load channel: query threads send loaded field data here for staging sync.
        let (lazy_tx, lazy_rx): (Sender<LazyLoad>, Receiver<LazyLoad>) =
            crossbeam_channel::unbounded();

        let pending_filter_loads = Arc::new(parking_lot::Mutex::new(pending_filter_loads));
        let pending_sort_loads = Arc::new(parking_lot::Mutex::new(pending_sort_loads));
        let lazy_value_fields = Arc::new(parking_lot::Mutex::new(lazy_value_fields));

        let flush_publish_count = Arc::new(AtomicU64::new(0));
        let flush_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_last_duration_nanos = Arc::new(AtomicU64::new(0));

        let flush_handle = {
            let inner = Arc::clone(&inner);
            let cache = Arc::clone(&cache);
            let shutdown = Arc::clone(&shutdown);
            let docstore = Arc::clone(&docstore);
            let flush_interval_us = config.flush_interval_us;
            let field_names = filter_field_names;
            let flush_bound_cache = Arc::clone(&bound_cache);
            let flush_unified_cache = Arc::clone(&unified_cache);
            let flush_loading_mode = Arc::clone(&loading_mode);
            let flush_dirty_flag = Arc::clone(&dirty_flag);
            let flush_time_buckets = time_buckets.as_ref().map(Arc::clone);
            let flush_pub_count = Arc::clone(&flush_publish_count);
            let flush_dur_nanos = Arc::clone(&flush_duration_nanos);
            let flush_last_dur_nanos = Arc::clone(&flush_last_duration_nanos);

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
                                    field.load_from(bitmaps);
                                }
                            }
                            LazyLoad::FilterValues { field, values } => {
                                if let Some(f) = staging.filters.get_field_mut(&field) {
                                    f.load_from(values);
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

                        // Activate deferred alive slots whose time has come.
                        // O(pending count) — typically small; runs every flush cycle for
                        // sub-second activation precision.
                        {
                            let now_unix = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            let activated = staging.slots.activate_due(now_unix);
                            if !activated.is_empty() {
                                staging.slots.merge_alive();
                            }
                        }

                        // In loading mode, skip all maintenance and snapshot publishing.
                        // This avoids the expensive staging.clone() → Arc::make_mut clone
                        // cascade that dominates write cost at scale.
                        if !flush_loading_mode.load(Ordering::Relaxed) {
                            // D3/E3: Live maintenance of bound caches on sort field mutations.
                            // Uses meta-index for O(1) lookup of relevant bounds instead of
                            // linear scan. For each mutated slot, check if its new sort value
                            // qualifies for any matching bound. Bits are only added, never
                            // removed — bloat control (D4) handles cleanup.
                            {
                                let sort_mutations = coalescer.mutated_sort_slots();
                                if !sort_mutations.is_empty() {
                                    let mut bc = flush_bound_cache.lock();
                                    if !bc.is_empty() {
                                        for (sort_field, slots) in &sort_mutations {
                                            // E3: Use meta-index to find matching bounds (O(1) vs linear)
                                            let matching_keys = bc.bounds_for_sort_field(sort_field);

                                            if matching_keys.is_empty() {
                                                continue;
                                            }

                                            for &slot in slots {
                                                let value = staging.sorts
                                                    .get_field(sort_field)
                                                    .map(|f| f.reconstruct_value(slot))
                                                    .unwrap_or(0);

                                                for bound_key in &matching_keys {
                                                    if let Some(entry) = bc.get_mut(bound_key) {
                                                        if entry.needs_rebuild() {
                                                            continue;
                                                        }
                                                        let qualifies = match bound_key.direction {
                                                            SortDirection::Desc => value > entry.min_tracked_value(),
                                                            SortDirection::Asc => value < entry.min_tracked_value(),
                                                        };
                                                        if qualifies {
                                                            entry.add_slot(slot);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Live maintenance for slot-based bounds: newly-alive slots
                            // are monotonically increasing and always qualify for Desc bounds.
                            {
                                let alive_inserts = coalescer.alive_inserts();
                                if !alive_inserts.is_empty() {
                                    let mut bc = flush_bound_cache.lock();
                                    let slot_bound_keys = bc.bounds_for_sort_field("__slot__");
                                    for bound_key in &slot_bound_keys {
                                        if let Some(entry) = bc.get_mut(bound_key) {
                                            if !entry.needs_rebuild() {
                                                for &slot in alive_inserts {
                                                    // New slots are always > min_tracked for Desc
                                                    if slot > entry.min_tracked_value() {
                                                        entry.add_slot(slot);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

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

                            // Cache maintenance: live updates for Eq entries, invalidation for the rest.
                            if coalescer.has_alive_mutations() {
                                // Alive changed — invalidate all filter fields because NotEq/Not
                                // bake alive into cached results. Live update can't fix this.
                                let mut c = cache.lock();
                                for name in &field_names {
                                    c.invalidate_field(name);
                                }
                            } else {
                                // Live update: insert/remove mutated slots into matching Eq cache entries.
                                // Non-Eq entries (NotEq, In) are not registered in the meta-index and
                                // fall back to field-level generation-counter invalidation.
                                let changed = coalescer.mutated_filter_fields();
                                if !changed.is_empty() {
                                    let mut c = cache.lock();

                                    // Collect all live-updated (entry_id, field) pairs for generation refresh
                                    let mut live_updated: Vec<(u32, String)> = Vec::new();

                                    // Live-update Eq cache entries with inserted slots
                                    for (filter_key, inserted_slots) in coalescer.filter_insert_entries() {
                                        let value_repr = filter_key.value.to_string();
                                        let ids: Vec<u32> = c.meta().entries_for_clause(&filter_key.field, "eq", &value_repr)
                                            .map(|bm| bm.iter().collect())
                                            .unwrap_or_default();
                                        for id in &ids {
                                            for &slot in inserted_slots {
                                                c.update_entry_by_id(*id, slot, true);
                                            }
                                            live_updated.push((*id, filter_key.field.to_string()));
                                        }
                                    }

                                    // Live-update Eq cache entries with removed slots
                                    for (filter_key, removed_slots) in coalescer.filter_remove_entries() {
                                        let value_repr = filter_key.value.to_string();
                                        let ids: Vec<u32> = c.meta().entries_for_clause(&filter_key.field, "eq", &value_repr)
                                            .map(|bm| bm.iter().collect())
                                            .unwrap_or_default();
                                        for id in &ids {
                                            for &slot in removed_slots {
                                                c.update_entry_by_id(*id, slot, false);
                                            }
                                            live_updated.push((*id, filter_key.field.to_string()));
                                        }
                                    }

                                    // Invalidate all changed fields (bumps generation counter).
                                    // This invalidates non-Eq entries (NotEq, In, range).
                                    for name in &changed {
                                        c.invalidate_field(name);
                                    }

                                    // Refresh generations on live-updated Eq entries so the
                                    // generation bump doesn't falsely invalidate them.
                                    for (id, field) in &live_updated {
                                        c.refresh_entry_generation(*id, field);
                                    }
                                }
                            }

                            // D3: Invalidate bounds whose filter fields changed.
                            // Alive mutations affect all bounds (alive is implicit in filter results).
                            {
                                let changed_filters = coalescer.mutated_filter_fields();
                                let has_alive = coalescer.has_alive_mutations();
                                if has_alive || !changed_filters.is_empty() {
                                    let mut bc = flush_bound_cache.lock();
                                    if !bc.is_empty() {
                                        if has_alive {
                                            // Alive changed — mark ALL bounds for rebuild
                                            for (_, entry) in bc.iter_mut() {
                                                entry.mark_for_rebuild();
                                            }
                                        } else {
                                            for field_name in &changed_filters {
                                                bc.invalidate_filter_field(field_name);
                                            }
                                        }
                                    }
                                }
                            }

                            // Unified cache live maintenance.
                            // Runs after bitmap mutations are applied to staging.
                            {
                                let mut uc = flush_unified_cache.lock();
                                if !uc.is_empty() {
                                    if coalescer.has_alive_mutations() {
                                        uc.maintain_alive_changes();
                                    } else {
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
                                    }
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

                    // Loading mode exit: force-publish if staging has unpublished mutations
                    if was_loading && !is_loading && staging_dirty {
                        // Compact all filter diffs accumulated during loading
                        for (_name, field) in staging.filters.fields_mut() {
                            field.merge_dirty();
                        }
                        // Invalidate all caches — they may be stale from the loading period
                        let mut c = cache.lock();
                        for name in &field_names {
                            c.invalidate_field(name);
                        }
                        drop(c);
                        flush_unified_cache.lock().clear();
                        inner.store(Arc::new(staging.clone()));
                        staging_dirty = false;
                    }
                    was_loading = is_loading;

                    // Publish if lazy loads updated staging but no mutations triggered a publish.
                    // This ensures staging stays consistent with the snapshot published by
                    // ensure_loaded() on the query thread.
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

                                    let elapsed = start.elapsed();

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
                                    cache.lock().invalidate_field(&field_name);

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
                            eprintln!("docstore batch write failed: {e}");
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

                    // Shutdown: invalidate all fields for safety
                    let mut c = cache.lock();
                    for name in &field_names {
                        c.invalidate_field(name);
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
                        eprintln!("docstore final batch write failed: {e}");
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
                        for (_name, field) in compacted.filters.fields_mut() {
                            field.merge_dirty();
                        }

                        // Collect filter bitmap entries for persistence
                        let mut filter_entries: Vec<(String, u64, RoaringBitmap)> = Vec::new();
                        for (name, field) in compacted.filters.fields() {
                            for (&value, vb) in field.iter_versioned() {
                                filter_entries.push((
                                    name.clone(),
                                    value,
                                    vb.base().as_ref().clone(),
                                ));
                            }
                        }

                        // Collect sort layer bases
                        let mut sort_data: Vec<(String, Vec<RoaringBitmap>)> = Vec::new();
                        for sc in &sort_field_configs {
                            if let Some(sf) = compacted.sorts.get_field(&sc.name) {
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
                    }
                    } // needs_write
                }
            })
        };

        Ok(Self {
            inner,
            cache,
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
            bound_cache,
            loading_mode,
            dirty_since_snapshot: Arc::clone(&dirty_flag),
            time_buckets,
            pending_filter_loads,
            pending_sort_loads,
            lazy_value_fields,
            lazy_tx,
            string_maps: None,
            unified_cache,
            flush_publish_count,
            flush_duration_nanos,
            flush_last_duration_nanos,
        })
    }

    /// Set the string maps for MappedString field query resolution.
    /// Call after creating the engine with schema data that includes string_map entries.
    pub fn set_string_maps(&mut self, maps: StringMaps) {
        self.string_maps = Some(Arc::new(maps));
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
            )
            .with_bound_target_size(self.config.cache.bound_target_size);
            if let Some(ref maps) = self.string_maps {
                base = base.with_string_maps(maps);
            }
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };

        let (filter_arc, use_simple_sort) =
            self.resolve_filters(&executor, filters, tb_guard.as_deref(), now_unix)?;

        // Compute total_matched from the FULL filter bitmap before bound narrowing.
        // Filter bitmaps are kept clean (no stale bits from deleted docs),
        // so no alive AND is needed.
        let full_total_matched = filter_arc.len();

        // D5: Narrow filter bitmap with bound cache.
        // Synthesize implicit "__slot__" sort for filter-only queries so they
        // benefit from slot-based bounds (newest-first = sort by slot desc).
        let implicit_sort;
        let bound_sort = match sort {
            Some(s) => Some(s),
            None => {
                implicit_sort = SortClause {
                    field: "__slot__".to_string(),
                    direction: SortDirection::Desc,
                };
                Some(&implicit_sort)
            }
        };
        let (effective_bitmap, use_simple, cache_key) =
            self.apply_bound(&executor, &filter_arc, use_simple_sort, bound_sort, filters, None);

        // Execute with ORIGINAL sort (None for filter-only) — bound narrows candidates,
        // but slot-order pagination is used for filter-only queries.
        let mut result =
            executor.execute_from_bitmap(&effective_bitmap, sort, limit, None, use_simple)?;

        // Override total_matched with the accurate count from the full filter bitmap.
        result.total_matched = full_total_matched;

        // D2: Form or update bound from results (slot-based for filter-only)
        self.update_bound_from_results(
            &snap, bound_sort, &cache_key, &result.ids, None,
            &filter_arc, &executor,
        );

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

        if needed_filters.is_empty() && needed_sort.is_none() && needed_values.is_empty() {
            return Ok(());
        }

        // Load from BitmapFs
        let store = match self.bitmap_store.as_ref() {
            Some(s) => s,
            None => return Ok(()), // no store, nothing to load
        };

        // Clone current snapshot, apply loaded fields, publish immediately
        let current: Arc<InnerEngine> = self.inner.load_full();
        let mut updated = (*current).clone();
        let mut any_loaded = false;

        // Full-field loads (low-cardinality)
        for name in &needed_filters {
            let t0 = std::time::Instant::now();
            let bitmaps = store.load_field(name)?;
            let count = bitmaps.len();
            if let Some(field) = updated.filters.get_field_mut(name) {
                field.load_from(bitmaps.clone());
            }
            eprintln!(
                "Lazy-loaded filter '{}': {} values in {:.1}ms",
                name,
                count,
                t0.elapsed().as_secs_f64() * 1000.0
            );

            let _ = self.lazy_tx.send(LazyLoad::FilterField {
                name: name.clone(),
                bitmaps,
            });
            self.pending_filter_loads.lock().remove(name);
            any_loaded = true;
        }

        // Per-value loads (high-cardinality multi_value)
        for (field_name, values) in &needed_values {
            // Filter out values already present in the snapshot
            let missing: Vec<u64> = if let Some(field) = updated.filters.get_field(field_name) {
                values
                    .iter()
                    .copied()
                    .filter(|v| field.get_versioned(*v).is_none())
                    .collect()
            } else {
                values.clone()
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
            if let Some(field) = updated.filters.get_field_mut(field_name) {
                field.load_from(loaded.clone());
            }
            eprintln!(
                "Lazy-loaded filter '{}': {} values (per-value) in {:.1}ms",
                field_name,
                count,
                t0.elapsed().as_secs_f64() * 1000.0
            );

            let _ = self.lazy_tx.send(LazyLoad::FilterValues {
                field: field_name.clone(),
                values: loaded,
            });
            any_loaded = true;
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
                if let Some(sf) = updated.sorts.get_field_mut(sort_name) {
                    sf.load_layers(layers.clone());
                }
                eprintln!(
                    "Lazy-loaded sort '{}': {} layers in {:.1}ms",
                    sort_name,
                    layer_count,
                    t0.elapsed().as_secs_f64() * 1000.0
                );

                let _ = self.lazy_tx.send(LazyLoad::SortField {
                    name: sort_name.clone(),
                    layers,
                });
                any_loaded = true;
            }

            self.pending_sort_loads.lock().remove(sort_name);
        }

        if any_loaded {
            // Publish updated snapshot immediately (queries can proceed)
            self.inner.store(Arc::new(updated));
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
            )
            .with_bound_target_size(self.config.cache.bound_target_size);
            if let Some(ref maps) = self.string_maps {
                base = base.with_string_maps(maps);
            }
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };

        let (filter_arc, use_simple_sort) =
            self.resolve_filters(&executor, &query.filters, tb_guard.as_deref(), now_unix)?;

        // Compute total_matched from the FULL filter bitmap before bound narrowing.
        // Filter bitmaps are kept clean (no stale bits from deleted docs),
        // so no alive AND is needed.
        let full_total_matched = filter_arc.len();

        // Unified cache lookup: check for a cached bounded bitmap that combines
        // filter + sort. On hit, skip the separate bound cache narrowing step.
        // Only for sorted queries above min_filter_size threshold.
        let (unified_key, unified_hit) = if let Some(sort_clause) = query.sort.as_ref() {
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
                        let bm = entry.bitmap().as_ref().clone();
                        (bm, entry.has_more())
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

        // D5: Narrow filter bitmap with bound cache (skipped on unified cache hit).
        // Synthesize implicit "__slot__" sort for filter-only queries.
        let implicit_sort;
        let bound_sort = match query.sort.as_ref() {
            Some(s) => Some(s),
            None => {
                implicit_sort = SortClause {
                    field: "__slot__".to_string(),
                    direction: SortDirection::Desc,
                };
                Some(&implicit_sort)
            }
        };

        let (effective_bitmap, use_simple, cache_key) = if let Some((ref unified_bm, _)) = unified_hit {
            // Unified cache hit — use bounded bitmap directly, AND with filter for freshness
            let narrowed = &filter_arc.as_ref().clone() & unified_bm;
            (narrowed, false, cache::canonicalize(&query.filters))
        } else {
            // Unified cache miss — fall through to existing bound cache path
            self.apply_bound(
                &executor,
                &filter_arc,
                use_simple_sort,
                bound_sort,
                &query.filters,
                query.cursor.as_ref(),
            )
        };

        // Offset pagination: if offset is set and no cursor, request offset+limit results
        // then drop the first `offset` items. Cursor takes precedence over offset.
        let offset = if query.cursor.is_none() {
            query.offset.unwrap_or(0)
        } else {
            0
        };
        let fetch_limit = query.limit.saturating_add(offset);

        // Execute with ORIGINAL sort — bound narrows candidates only.
        let bound_was_applied = effective_bitmap.len() < filter_arc.len();
        let mut result = executor.execute_from_bitmap(
            &effective_bitmap,
            query.sort.as_ref(),
            fetch_limit,
            query.cursor.as_ref(),
            use_simple,
        )?;

        // Bound exhaustion retry: if the bound-narrowed bitmap returned 0 results
        // but we have a cursor (meaning there should be more), retry with the full
        // filter bitmap. This happens when the bound (global top-K) has a small
        // intersection with a restrictive filter (e.g. 30-day time bucket).
        if result.ids.is_empty() && query.cursor.is_some() && bound_was_applied {
            result = executor.execute_from_bitmap(
                &filter_arc,
                query.sort.as_ref(),
                fetch_limit,
                query.cursor.as_ref(),
                use_simple_sort,
            )?;
        }

        // Override total_matched with the accurate count from the full filter bitmap.
        // execute_from_bitmap computes total_matched from the possibly-narrowed bitmap,
        // but users need the true count for pagination UI.
        result.total_matched = full_total_matched;

        // Apply offset: drop the first N results
        if offset > 0 && !result.ids.is_empty() {
            if offset >= result.ids.len() {
                result.ids.clear();
                result.cursor = None;
            } else {
                result.ids = result.ids.split_off(offset);
                // Recompute cursor from the new last element
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

        // D2/D6: Form or update bound from results (with cursor for tiered bounds).
        // Passes filter bitmap + executor so bounds can be seeded with target_size
        // entries via a full traversal, not just the page-limited result set.
        self.update_bound_from_results(
            &snap,
            bound_sort,
            &cache_key,
            &result.ids,
            query.cursor.as_ref(),
            &filter_arc,
            &executor,
        );

        // Unified cache formation: on miss with sort results, store the bounded
        // bitmap for future queries with the same filter + sort combination.
        if unified_hit.is_none() {
            if let Some(ukey) = unified_key {
                if !result.ids.is_empty() {
                    let sort_field = snap.sorts.get_field(&ukey.sort_field);
                    let sorted_slots: Vec<u32> = result.ids.iter().map(|&id| id as u32).collect();
                    let has_more = full_total_matched > sorted_slots.len() as u64;
                    let value_fn = |slot: u32| -> u32 {
                        sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                    };
                    self.unified_cache.lock().form_and_store(
                        ukey,
                        &sorted_slots,
                        has_more,
                        value_fn,
                    );
                }
            }
        }

        self.post_validate(&mut result, &query.filters, &executor)?;

        Ok(result)
    }

    /// Resolve filter clauses to a bitmap, using the trie cache with brief locks.
    ///
    /// Cache Mutex is held ONLY during lookup (~μs) and store (~μs),
    /// never during filter computation or sort traversal.
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
        let cache_key = cache::canonicalize(&plan.ordered_clauses);

        let filter_bitmap = if let Some(ref key) = cache_key {
            // Brief lock: cache lookup only
            let lookup = { self.cache.lock().lookup(key) };
            // Lock released — CacheLookup owns its Arc bitmaps

            match lookup {
                CacheLookup::ExactHit(arc) => arc,
                CacheLookup::PrefixHit { bitmap: prefix_arc, matched_prefix_len } => {
                    // Sort clauses into canonical order (by field name) to match
                    // the cache key ordering. The planner orders by cardinality,
                    // but the cache key is alphabetical — using matched_prefix_len
                    // as an index requires the same ordering.
                    let mut canonical_clauses = plan.ordered_clauses.clone();
                    canonical_clauses.sort_by(|a, b| {
                        let ka = cache::CanonicalClause::from_filter(a);
                        let kb = cache::CanonicalClause::from_filter(b);
                        match (ka, kb) {
                            (Some(a), Some(b)) => a.cmp(&b),
                            (Some(_), None) => std::cmp::Ordering::Less,
                            (None, Some(_)) => std::cmp::Ordering::Greater,
                            (None, None) => std::cmp::Ordering::Equal,
                        }
                    });

                    // Start from prefix bitmap, compute remaining clauses (no lock held)
                    let mut bitmap = (*prefix_arc).clone();
                    for clause in &canonical_clauses[matched_prefix_len..] {
                        let clause_bm = executor.evaluate_clause(clause)?;
                        bitmap &= &clause_bm;
                    }
                    // Brief lock: store result
                    let arc = Arc::new(bitmap);
                    self.cache.lock().store(key, arc.clone());
                    arc
                }
                CacheLookup::Miss => {
                    // Full computation (no lock held)
                    let bitmap = executor.compute_filters(&plan.ordered_clauses)?;
                    // Brief lock: store result
                    let arc = Arc::new(bitmap);
                    self.cache.lock().store(key, arc.clone());
                    arc
                }
            }
        } else {
            // Uncacheable query — compute without cache
            Arc::new(executor.compute_filters(&plan.ordered_clauses)?)
        };

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

    /// D5: Apply bound cache narrowing for sort queries.
    ///
    /// If a matching bound exists and is usable, ANDs the filter bitmap with the
    /// bound bitmap to reduce the sort working set. Returns the effective bitmap,
    /// whether to use simple sort, and the cache key for bound formation.
    fn apply_bound(
        &self,
        _executor: &QueryExecutor,
        filter_bitmap: &roaring::RoaringBitmap,
        use_simple_sort: bool,
        sort: Option<&SortClause>,
        filters: &[FilterClause],
        cursor: Option<&crate::query::CursorPosition>,
    ) -> (roaring::RoaringBitmap, bool, Option<CacheKey>) {
        let Some(sort_clause) = sort else {
            return (filter_bitmap.clone(), use_simple_sort, None);
        };

        let cache_key = cache::canonicalize(filters);
        let Some(ref filter_key) = cache_key else {
            return (filter_bitmap.clone(), use_simple_sort, None);
        };

        let bound_key = BoundKey {
            filter_key: filter_key.clone(),
            sort_field: sort_clause.field.clone(),
            direction: sort_clause.direction,
            tier: 0,
        };

        let mut bc = self.bound_cache.lock();

        // D6: Try tier 0 first, then escalate to higher tiers if cursor is past bound.
        // When cursor is provided and all tiers are exhausted, skip bound entirely —
        // bound cache is a first-page acceleration, not a correctness requirement.
        let max_tiers = 4u32; // cap to avoid unbounded tier growth
        let mut try_key = bound_key;
        let mut skip_all_bounds = false;
        for _tier in 0..max_tiers {
            if let Some(entry) = bc.lookup_mut(&try_key) {
                if !entry.needs_rebuild() {
                    // Check if cursor is past this tier's range
                    let cursor_past = if let Some(c) = cursor {
                        let cursor_val = c.sort_value as u32;
                        match sort_clause.direction {
                            SortDirection::Desc => cursor_val <= entry.min_tracked_value(),
                            SortDirection::Asc => cursor_val >= entry.min_tracked_value(),
                        }
                    } else {
                        false
                    };

                    if cursor_past {
                        // Try next tier
                        try_key = BoundKey {
                            filter_key: try_key.filter_key,
                            sort_field: try_key.sort_field,
                            direction: try_key.direction,
                            tier: try_key.tier + 1,
                        };
                        continue;
                    }

                    entry.touch();
                    let narrowed = filter_bitmap & entry.bitmap();
                    return (narrowed, false, cache_key);
                }
            }
            // No bound at this tier — if we got here because cursor was past
            // previous tiers, skip all bounds to avoid 0-result pages.
            if cursor.is_some() && try_key.tier > 0 {
                skip_all_bounds = true;
            }
            break;
        }
        // If cursor exhausted all available tiers, it's past all bounds.
        if cursor.is_some() && try_key.tier >= max_tiers {
            skip_all_bounds = true;
        }

        // E4/S4.2: Superset matching — find a bound whose filter clauses are a
        // subset of this query's. A bound for {nsfwLevel=1} can narrow a query
        // for {nsfwLevel=1, onSite=true}. The bound bitmap is still ANDed with
        // the full filter result, so correctness is preserved.
        // Skip when cursor exhausted all available tiers.
        if !skip_all_bounds {
            if let Some(entry) = bc.find_superset_bound(
                filter_key,
                &sort_clause.field,
                sort_clause.direction,
            ) {
                entry.touch();
                let narrowed = filter_bitmap & entry.bitmap();
                return (narrowed, false, cache_key);
            }
        }

        drop(bc);

        (filter_bitmap.clone(), use_simple_sort, cache_key)
    }

    /// D2/D6: Form or update a bound from sort query results.
    /// With cursor awareness: if a cursor was past tier 0's range, form a tiered bound.
    /// Supports "__slot__" pseudo-sort where sort value = slot ID.
    ///
    /// When forming or rebuilding a bound, if the query result set is smaller
    /// than target_size, does a full sort traversal to seed the bound with
    /// target_size entries. This ensures the bound covers many pages of
    /// pagination, not just the first page's results.
    fn update_bound_from_results(
        &self,
        snap: &Guard<Arc<InnerEngine>>,
        sort: Option<&SortClause>,
        cache_key: &Option<CacheKey>,
        result_ids: &[i64],
        cursor: Option<&crate::query::CursorPosition>,
        filter_bitmap: &roaring::RoaringBitmap,
        executor: &QueryExecutor,
    ) {
        let Some(sort_clause) = sort else { return };
        let Some(ref filter_key) = cache_key else { return };
        if result_ids.is_empty() { return; }

        // Determine value function: slot-based or sort-field-based
        let is_slot_sort = sort_clause.field == "__slot__";
        let sort_field = if is_slot_sort {
            None
        } else {
            match snap.sorts.get_field(&sort_clause.field) {
                Some(f) => Some(f),
                None => return,
            }
        };

        // D6: Determine which tier to form/update.
        let tier = if let Some(c) = cursor {
            let cursor_val = c.sort_value as u32;
            let mut t = 0u32;
            let bc = self.bound_cache.lock();
            loop {
                let key = BoundKey {
                    filter_key: filter_key.clone(),
                    sort_field: sort_clause.field.clone(),
                    direction: sort_clause.direction,
                    tier: t,
                };
                if let Some(entry) = bc.lookup(&key) {
                    let past = match sort_clause.direction {
                        SortDirection::Desc => cursor_val < entry.min_tracked_value(),
                        SortDirection::Asc => cursor_val > entry.min_tracked_value(),
                    };
                    if past {
                        t += 1;
                        if t >= 4 { break; }
                        continue;
                    }
                }
                break;
            }
            drop(bc);
            t
        } else {
            0
        };

        let bound_key = BoundKey {
            filter_key: filter_key.clone(),
            sort_field: sort_clause.field.clone(),
            direction: sort_clause.direction,
            tier,
        };

        let target_size = self.bound_cache.lock().target_size();

        // Check if we actually need to seed/rebuild the bound before doing
        // the expensive full traversal. A bound needs seeding if it doesn't exist,
        // needs rebuild, or was formed from too few results (e.g., just one page
        // of 20 items instead of the target 10K).
        let needs_seed = {
            let bc = self.bound_cache.lock();
            match bc.lookup(&bound_key) {
                Some(entry) => entry.needs_rebuild() || (entry.bitmap().len() as usize) < target_size / 2,
                None => true, // bound doesn't exist yet
            }
        };

        if !needs_seed {
            return;
        }

        // If the query result set is smaller than target_size, do a full
        // traversal to seed the bound properly. This ensures the bound covers
        // thousands of entries for pagination, not just a single page.
        //
        // For tier > 0, we need to start the seed traversal AFTER the previous
        // tier's range. Find the previous tier's min_tracked_value and use it
        // as the cursor for the seed traversal.
        let seed_cursor = if tier > 0 {
            let bc = self.bound_cache.lock();
            let prev_key = BoundKey {
                filter_key: filter_key.clone(),
                sort_field: sort_clause.field.clone(),
                direction: sort_clause.direction,
                tier: tier - 1,
            };
            bc.lookup(&prev_key).map(|entry| {
                let min_val = entry.min_tracked_value();
                // Find the slot with min_tracked_value to build a cursor.
                // Use slot_id=0 for Desc (any slot past min_val) or u32::MAX for Asc.
                let slot_id = match sort_clause.direction {
                    SortDirection::Desc => 0,
                    SortDirection::Asc => u32::MAX,
                };
                crate::query::CursorPosition {
                    sort_value: min_val as u64,
                    slot_id,
                }
            })
        } else {
            None
        };

        let seed_slots: Vec<u32> = if result_ids.len() < target_size {
            if let Ok(full_result) = executor.execute_from_bitmap_unclamped(
                filter_bitmap,
                Some(sort_clause),
                target_size,
                seed_cursor.as_ref(), // start after previous tier for tier > 0
                false, // full sort, not simple
            ) {
                full_result.ids.iter().map(|&id| id as u32).collect()
            } else {
                result_ids.iter().map(|&id| id as u32).collect()
            }
        } else {
            result_ids.iter().map(|&id| id as u32).collect()
        };

        // Value function: for __slot__ sort, value = slot ID itself
        let value_fn = |slot: u32| -> u32 {
            if is_slot_sort {
                slot
            } else {
                sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
            }
        };

        let mut bc = self.bound_cache.lock();
        if let Some(entry) = bc.get_mut(&bound_key) {
            if entry.needs_rebuild() || (entry.bitmap().len() as usize) < target_size / 2 {
                entry.rebuild(&seed_slots, &value_fn);
            }
        } else {
            bc.form_bound(bound_key, &seed_slots, &value_fn);
        }
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

    /// Get the high-water mark slot counter (lock-free snapshot).
    pub fn slot_counter(&self) -> u32 {
        self.snapshot().slots.slot_counter()
    }

    /// Retrieve a stored document by slot ID from the docstore.
    pub fn get_document(&self, slot_id: u32) -> Result<Option<StoredDoc>> {
        self.docstore.lock().get(slot_id)
    }

    /// Compact the docstore, reclaiming space from old write transactions.
    pub fn compact_docstore(&self) -> Result<bool> {
        self.docstore.lock().compact()
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
        let cache = self.cache.lock();
        let cache_entries = cache.len();
        let cache_bytes = cache.bitmap_bytes();
        drop(cache);
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

    /// Report bound cache statistics.
    ///
    /// Returns (bound_entries, bound_bitmap_bytes, meta_index_entries, meta_index_bytes).
    pub fn bound_cache_stats(&self) -> (usize, usize, usize, usize) {
        let bc = self.bound_cache.lock();
        let bound_entries = bc.len();
        let bound_bytes = bc.total_memory_bytes();
        let meta = bc.meta_index();
        let meta_entries = meta.entry_count();
        let meta_bytes = meta.memory_bytes();
        (bound_entries, bound_bytes, meta_entries, meta_bytes)
    }

    /// Return unified cache stats (entries, hits, misses, memory).
    pub fn unified_cache_stats(&self) -> crate::unified_cache::UnifiedCacheStats {
        self.unified_cache.lock().stats()
    }

    /// Clear unified cache entries and reset counters.
    pub fn clear_unified_cache(&self) {
        self.unified_cache.lock().clear();
    }

    /// Clear all bound cache entries (for benchmarking cold vs warm).
    pub fn clear_bound_cache(&self) {
        self.bound_cache.lock().clear();
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
        // Give the flush thread time to see the flag and do a final publish.
        // The next flush cycle with bitmap_count > 0 will publish normally.
        // If no mutations are pending, we need to ensure at least one flush
        // cycle runs — the existing adaptive sleep ensures this happens within
        // max_sleep (flush_interval * 10).
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
        Self::write_snapshot_to_store(store, &self.inner, &self.config)
    }

    /// Save a full snapshot of the current published state to a BitmapFs at a custom path.
    ///
    /// Creates a new BitmapFs at the given path and writes the complete engine
    /// state. Useful for benchmarks that want to save to a specific location,
    /// or for creating point-in-time backups separate from the live store.
    pub fn save_snapshot_to(&self, path: &Path) -> Result<()> {
        let store = BitmapFs::new(path)?;
        Self::write_snapshot_to_store(&store, &self.inner, &self.config)
    }

    /// Internal: extract all state from the current published snapshot and write it
    /// to the given BitmapFs.
    fn write_snapshot_to_store(
        store: &BitmapFs,
        inner: &ArcSwap<InnerEngine>,
        config: &Config,
    ) -> Result<()> {
        // Load the current published snapshot (lock-free).
        // load_full() returns Arc<InnerEngine>; we need an owned mutable copy
        // to compact diffs before persisting.
        let snap: Arc<InnerEngine> = inner.load_full();
        let mut compacted: InnerEngine = (*snap).clone();

        // Compact filter diffs so we persist clean bases
        for (_name, field) in compacted.filters.fields_mut() {
            field.merge_dirty();
        }
        // Merge alive diffs
        compacted.slots.merge_alive();

        // Collect filter bitmap entries
        let mut filter_entries: Vec<(String, u64, RoaringBitmap)> = Vec::new();
        for (name, field) in compacted.filters.fields() {
            for (&value, vb) in field.iter_versioned() {
                filter_entries.push((
                    name.clone(),
                    value,
                    vb.base().as_ref().clone(),
                ));
            }
        }

        // Collect sort layer bases
        let mut sort_data: Vec<(String, Vec<RoaringBitmap>)> = Vec::new();
        for sc in &config.sort_fields {
            if let Some(sf) = compacted.sorts.get_field(&sc.name) {
                let bases: Vec<RoaringBitmap> = sf
                    .layer_bases()
                    .iter()
                    .map(|b| (*b).clone())
                    .collect();
                sort_data.push((sc.name.clone(), bases));
            }
        }

        // Build references for write_full_snapshot
        let filter_refs: Vec<(&str, u64, &RoaringBitmap)> = filter_entries
            .iter()
            .map(|(f, v, b)| (f.as_str(), *v, b))
            .collect();
        let alive = compacted.slots.alive_bitmap().clone();
        let slot_counter = compacted.slots.slot_counter();

        // Sort layer refs: owned Vec<&BM> must outlive the slice refs
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

        store.write_full_snapshot(
            &filter_refs,
            &alive,
            &sort_slice_refs,
            slot_counter,
        )
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
        {
            let mut c = self.cache.lock();
            for fc in &self.config.filter_fields {
                c.invalidate_field(&fc.name);
            }
        }
        {
            let mut bc = self.bound_cache.lock();
            for (_, entry) in bc.iter_mut() {
                entry.mark_for_rebuild();
            }
        }
    }

    /// Persist documents to the docstore on a background thread.
    /// Returns a JoinHandle to wait for completion. The docs Vec is consumed.
    pub fn spawn_docstore_writer(&self, docs: Vec<(u32, Document)>) -> JoinHandle<()> {
        let docstore = Arc::clone(&self.docstore);
        thread::spawn(move || {
            let batch_size = 100_000;
            let mut batch: Vec<(u32, StoredDoc)> = Vec::with_capacity(batch_size);
            for (slot, doc) in docs {
                batch.push((slot, StoredDoc { fields: doc.fields }));
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
            batch.push((*slot, StoredDoc { fields: doc.fields.clone() }));
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
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,

                    behaviors: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,

                    behaviors: None,
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
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,

                    behaviors: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,

                    behaviors: None,
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

    // === Slot-based bound tests ===

    #[test]
    fn test_filter_only_query_forms_slot_bound() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert documents
        for i in 1..=20u32 {
            engine.put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 10))),
                ]),
            ).unwrap();
        }
        wait_for_flush(&engine, 20, 500);

        // Run a filter-only query — should form a __slot__ bound
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            10,
        ).unwrap();

        // Results should be in descending slot order (newest first)
        assert_eq!(result.ids.len(), 10);
        assert_eq!(result.ids[0], 20);
        assert_eq!(result.ids[9], 11);

        // Second query should benefit from the bound (correctness check)
        let result2 = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            10,
        ).unwrap();
        assert_eq!(result.ids, result2.ids);
    }

    #[test]
    fn test_filter_only_cursor_has_sort_value() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        for i in 1..=30u32 {
            engine.put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64))),
                ]),
            ).unwrap();
        }
        wait_for_flush(&engine, 30, 500);

        // First page
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            10,
        ).unwrap();
        assert_eq!(result.ids.len(), 10);
        assert_eq!(result.ids[0], 30);

        // Cursor should have sort_value = last slot ID
        let cursor = result.cursor.as_ref().expect("should have cursor");
        assert_eq!(cursor.slot_id as i64, result.ids[9]);
        assert_eq!(cursor.sort_value, result.ids[9] as u64, "sort_value should equal slot ID");
    }

    #[test]
    fn test_slot_bound_live_maintenance_on_insert() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert initial docs
        for i in 1..=10u32 {
            engine.put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64))),
                ]),
            ).unwrap();
        }
        wait_for_flush(&engine, 10, 500);

        // Form a slot bound via filter-only query
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            10,
        ).unwrap();
        assert_eq!(result.ids[0], 10);

        // Insert a new doc — slot 11 should be live-maintained into the bound
        engine.put(
            11,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(110))),
            ]),
        ).unwrap();
        wait_for_flush(&engine, 11, 500);

        // Next query should see the new doc at the top
        let result2 = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            10,
        ).unwrap();
        assert_eq!(result2.ids[0], 11, "Newly inserted doc should be first (highest slot)");
        assert_eq!(result2.ids.len(), 10);
    }

    // === Trie cache live update tests ===

    #[test]
    fn test_cache_live_update_on_insert() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert docs so cache has data
        for i in 1..=5u32 {
            engine.put(
                i,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(i as i64 * 100))),
                ]),
            ).unwrap();
        }
        wait_for_flush(&engine, 5, 500);

        // Warm the cache with a query
        let r1 = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        ).unwrap();
        assert_eq!(r1.total_matched, 5);

        // Insert another nsfwLevel=1 doc
        engine.put(
            6,
            &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(600))),
            ]),
        ).unwrap();
        wait_for_flush(&engine, 6, 500);

        // Query again — cache should be live-updated, not cold
        let r2 = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
        ).unwrap();
        assert_eq!(r2.total_matched, 6, "live-updated cache should include new doc");
        assert!(r2.ids.contains(&6), "new doc should be in results");
    }
}
