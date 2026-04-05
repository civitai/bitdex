use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender};
use roaring::RoaringBitmap;
use crate::config::Config;
use crate::silos::doc_format::{StoredDoc};
use crate::silos::doc_silo_adapter::DocSiloAdapter;
use crate::error::Result;
use crate::engine::executor::{CaseSensitiveFields, StringMaps};
use crate::mutation::FieldRegistry;
use crate::time_buckets::TimeBucketManager;
use crate::mutation::{MutationOp, MutationSender};

/// Key for grouping filter operations by target bitmap.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FilterGroupKey {
    pub field: Arc<str>,
    pub value: u64,
}

/// Bridge for passing Prometheus metric handles from the server layer into
/// the engine's background threads (compaction worker).
/// Only available when compiled with the `server` feature.
#[cfg(feature = "server")]
pub struct MetricsBridge {
    pub lazy_load_duration: prometheus::HistogramVec,
    pub compaction_total: prometheus::IntCounterVec,
    pub compaction_duration: prometheus::HistogramVec,
    pub index_name: String,
}
/// Thread-safe engine using ArcSwap for lock-free snapshot reads.
///
/// Writers call `put`/`delete` which compute diffs and send
/// MutationOps to a channel. A background flush thread applies batched
/// mutations to a private staging copy, then atomically publishes a
/// new snapshot via ArcSwap::store().
///
/// Result of a compact_all() operation.
#[derive(Debug, Default, serde::Serialize)]
pub struct CompactResult {
    pub shards_scanned: u64,
    pub shards_compacted: u64,
    pub shards_skipped: u64,
    pub elapsed_secs: f64,
}

/// Thread-safe engine with RwLock-protected bitmap state.
///
/// Readers call `filters.read()` / `sorts.read()` / `slots.read()` —
/// multiple readers share access lock-free while flush thread holds
/// write locks only for the duration of batch application.
///
/// Bulk-load callers use `merge_bitmap_maps()` to OR-merge pre-built bitmaps
/// directly into the live state under write locks.
pub struct ConcurrentEngine {
    /// Slot allocator: alive bitmap + slot counter + deferred alive set.
    pub(crate) slots: Arc<parking_lot::RwLock<crate::engine::slot::SlotAllocator>>,
    /// Filter index: one VersionedBitmap per field × value.
    pub(crate) filters: Arc<parking_lot::RwLock<crate::engine::filter::FilterIndex>>,
    /// Sort index: per-field bit-layer bitmaps.
    pub(crate) sorts: Arc<parking_lot::RwLock<crate::engine::sort::SortIndex>>,
    pub(crate) sender: MutationSender,
    pub(crate) doc_tx: Sender<(u32, StoredDoc)>,
    pub(crate) docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>,
    pub(crate) config: Arc<Config>,
    pub(crate) field_registry: FieldRegistry,
    pub(crate) shutdown: Arc<AtomicBool>,
    pub(crate) flush_handle: Option<JoinHandle<()>>,
    pub(crate) merge_handle: Option<JoinHandle<()>>,
    /// Dirty flag: flush/write paths set true so the merge thread persists on next cycle.
    pub(crate) dirty_flag: Arc<AtomicBool>,
    pub(crate) time_buckets: Option<Arc<parking_lot::Mutex<TimeBucketManager>>>,
    /// Reverse string maps for MappedString field query resolution.
    pub(crate) string_maps: Option<Arc<StringMaps>>,
    /// Fields where string matching is case-sensitive (default is case-insensitive).
    pub(crate) case_sensitive_fields: Option<Arc<CaseSensitiveFields>>,
    /// Per-field dictionaries for LowCardinalityString fields.
    pub(crate) dictionaries: Arc<HashMap<String, crate::dictionary::FieldDictionary>>,
    /// CacheSilo: persistent cache backed by DataSilo.
    pub(crate) cache_silo: Option<Arc<parking_lot::RwLock<crate::silos::cache_silo::CacheSilo>>>,
    /// Flush loop stats: total flush cycles that applied mutations (monotonic counter).
    pub(crate) flush_apply_count: Arc<AtomicU64>,
    pub(crate) flush_duration_nanos: Arc<AtomicU64>,
    pub(crate) flush_last_duration_nanos: Arc<AtomicU64>,
    pub(crate) flush_apply_nanos: Arc<AtomicU64>,
    pub(crate) flush_cache_nanos: Arc<AtomicU64>,
    pub(crate) flush_opslog_nanos: Arc<AtomicU64>,
    pub(crate) flush_timebucket_nanos: Arc<AtomicU64>,
    pub(crate) flush_compact_nanos: Arc<AtomicU64>,
    /// Named cursors: opaque key-value pairs persisted at checkpoint time.
    pub(crate) cursors: Arc<parking_lot::Mutex<HashMap<String, String>>>,
    /// Metrics bridge: prometheus handles set by server layer, read by background threads.
    #[cfg(feature = "server")]
    pub(crate) metrics_bridge: Arc<ArcSwap<Option<Arc<MetricsBridge>>>>,
    /// BitmapSilo for frozen bitmap reads.
    pub(crate) bitmap_silo: Option<Arc<parking_lot::RwLock<crate::silos::bitmap_silo::BitmapSilo>>>,
    pub(crate) compaction_skipped: Arc<AtomicU64>,
    /// Monotonically increasing epoch counter. Incremented on every mutation batch.
    /// Used by cache staleness detection to invalidate entries whose fields changed.
    pub(crate) mutation_epoch: Arc<AtomicU64>,
    /// Per-field mutation epoch. Maps field name → epoch at last mutation.
    /// Query threads read this to check whether a cache entry's fields have changed.
    pub(crate) field_epochs: Arc<parking_lot::RwLock<HashMap<String, u64>>>,
}

// CacheStats and CacheEntryDetail stubs removed — CacheSilo has no in-memory entry tracking.

impl ConcurrentEngine {
    /// Create a new concurrent engine with an in-memory docstore (for testing).
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let docstore = DocSiloAdapter::open_temp()
            .map_err(|e| crate::error::BitdexError::Storage(format!("open temp: {e}")))?;
        Self::build(config, docstore)
    }
    /// Create a new concurrent engine with an on-disk docstore.
    pub fn new_with_path(config: Config, path: &Path) -> Result<Self> {
        config.validate()?;
        let docstore = DocSiloAdapter::open(path)
            .map_err(|e| crate::error::BitdexError::Storage(format!("open: {e}")))?;
        Self::build(config, docstore)
    }

    fn build(config: Config, docstore: DocSiloAdapter) -> Result<Self> {
        let mut filters = crate::engine::filter::FilterIndex::new();
        let mut sorts = crate::engine::sort::SortIndex::new();
        // All fields are in-memory (no tier 2 distinction).
        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }
        let field_registry = FieldRegistry::from_config(&config);

        // Restore from BitmapSilo: alive+meta loaded to heap; filter/sort stay frozen in mmap
        let mut slots = crate::engine::slot::SlotAllocator::new();
        let mut restored_cursors: HashMap<String, String> = HashMap::new();
        let mut bitmap_silo_arc: Option<Arc<parking_lot::RwLock<crate::silos::bitmap_silo::BitmapSilo>>> = None;
        if let Some(ref bitmap_path) = config.storage.bitmap_path {
            match crate::silos::bitmap_silo::BitmapSilo::open(bitmap_path) {
                Ok(silo) if silo.has_data() => {
                    let t_restore = std::time::Instant::now();
                    // Load alive bitmap with pending ops applied — used by SlotAllocator.
                    // get_alive_with_ops() reads the frozen base + scans both ops logs,
                    // so the restored bitmap reflects all written but not yet compacted ops.
                    if let Some(alive) = silo.get_alive_with_ops() {
                        let meta = silo.load_meta().ok().flatten();
                        let slot_counter = meta.as_ref()
                            .and_then(|m| m.get("slot_counter"))
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .unwrap_or(0);
                        let alive_count = alive.len();
                        slots = crate::engine::slot::SlotAllocator::from_state(
                            slot_counter,
                            alive,
                            roaring::RoaringBitmap::new(),
                        );
                        restored_cursors = meta.as_ref()
                            .and_then(|m| m.get("cursors"))
                            .and_then(|v| serde_json::from_value(v.clone()).ok())
                            .unwrap_or_default();
                        eprintln!("BitmapSilo: restored alive ({} slots, counter={})", alive_count, slot_counter);
                    }
                    // Mark filter/sort bitmaps as backed — NOT loaded to heap.
                    // Queries read frozen bitmaps from silo mmap at query time.
                    let filter_count = silo.mark_filters_backed(&mut filters);
                    eprintln!("BitmapSilo: marked {} filter bitmaps as frozen-backed", filter_count);
                    let sort_count = silo.mark_sorts_backed(&mut sorts);
                    eprintln!("BitmapSilo: marked {} sort layers as frozen-backed", sort_count);
                    eprintln!("BitmapSilo: restore complete in {:.1}ms", t_restore.elapsed().as_secs_f64() * 1000.0);
                    bitmap_silo_arc = Some(Arc::new(parking_lot::RwLock::new(silo)));
                }
                Ok(_) => {
                    eprintln!("BitmapSilo: no data found, starting fresh");
                }
                Err(e) => {
                    eprintln!("BitmapSilo: open error (starting fresh): {e}");
                }
            }
        }
        // CacheSilo: open the persistent cache store. Queries read directly via get_entry().
        let cache_silo_arc: Option<Arc<parking_lot::RwLock<crate::silos::cache_silo::CacheSilo>>> =
            config.storage.bitmap_path.as_ref().and_then(|bp| {
                let silo_path = std::path::Path::new(bp).join("cache_silo");
                match crate::silos::cache_silo::CacheSilo::open(&silo_path) {
                    Ok(silo) => {
                        eprintln!("CacheSilo: opened at {}", silo_path.display());
                        Some(Arc::new(parking_lot::RwLock::new(silo)))
                    }
                    Err(e) => {
                        eprintln!("CacheSilo: open error (skipping persistence): {e}");
                        None
                    }
                }
            });
        // S3.3: Instantiate TimeBucketManager from top-level time_buckets config
        let time_buckets = config.time_buckets.as_ref().map(|tb_config| {
            let tb = TimeBucketManager::new_with_sort_field(
                tb_config.filter_field.clone(),
                tb_config.sort_field.clone(),
                tb_config.range_buckets.clone(),
            );
            Arc::new(parking_lot::Mutex::new(tb))
        });
        // Initialize pending bucket diffs (load from append-only log on disk + compute boot diff)
        let pending_bucket_diffs = {
            let max_diffs = 100; // ~8 hours at 300s intervals
            let mut pending = crate::bucket_diff_log::PendingBucketDiffs::new(max_diffs);
            let diff_log_path = config.storage.bitmap_path.as_ref()
                .map(|bp| std::path::Path::new(bp).join("bucket_diffs.log"));
            // Step 1: Load persisted diffs from append-only log
            if let Some(ref log_path) = diff_log_path {
                if log_path.exists() {
                    let log = crate::bucket_diff_log::BucketDiffLog::new(
                        log_path.clone(), max_diffs, 0.3,
                    );
                    match log.read_retained() {
                        Ok(diffs) if !diffs.is_empty() => {
                            let count = diffs.len();
                            pending = crate::bucket_diff_log::PendingBucketDiffs::from_diffs(diffs, max_diffs);
                            eprintln!("Loaded {count} bucket diffs from disk (coverage: cutoff {} to {})",
                                pending.oldest_cutoff(), pending.current_cutoff());
                        }
                        Ok(_) => {}
                        Err(e) => eprintln!("Warning: failed to load bucket diffs: {e}"),
                    }
                }
            }
            // Step 2: Compute boot diff to cover the gap between persisted diffs and now.
            // The sort field for time buckets was eagerly loaded above, so it's available in `sorts`.
            if let Some(ref tb_config) = config.time_buckets {
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if let Some(ref tb_arc) = time_buckets {
                    let tb = tb_arc.lock();
                    let sort_field_name = tb.sort_field_name().to_string();
                    drop(tb);
                    if let Some(sort_field) = sorts.get_field(&sort_field_name) {
                        let tb = tb_arc.lock();
                        for bucket_config in &tb_config.range_buckets {
                            let bucket_name = &bucket_config.name;
                            if let Some(bucket) = tb.get_bucket(bucket_name) {
                                let current_cutoff = crate::bucket_diff_log::snap_cutoff(
                                    now_secs.saturating_sub(bucket_config.duration_secs),
                                    bucket_config.refresh_interval_secs,
                                );
                                // Determine where persisted diffs leave off
                                let persisted_cutoff = if pending.current_cutoff() > 0 {
                                    pending.current_cutoff()
                                } else {
                                    bucket.last_cutoff()
                                };
                                if current_cutoff > persisted_cutoff && persisted_cutoff > 0 {
                                    // Gap exists — compute boot diff by scanning bucket bitmap
                                    let gap_secs = current_cutoff - persisted_cutoff;
                                    // Safety check: if gap > bucket duration, the persisted bitmap
                                    // is meaningless. The flush thread will do a full rebuild on
                                    // the first refresh cycle. Don't compute a boot diff.
                                    if gap_secs > bucket_config.duration_secs {
                                        eprintln!("Boot diff: gap {}s exceeds bucket duration {}s for '{}' — skipping (full rebuild on first refresh)",
                                            gap_secs, bucket_config.duration_secs, bucket_name);
                                        continue;
                                    }
                                    let bucket_bm = bucket.bitmap();
                                    let old_cutoff_u32 = persisted_cutoff as u32;
                                    let new_cutoff_u32 = current_cutoff as u32;
                                    let start = std::time::Instant::now();
                                    let mut expired = roaring::RoaringBitmap::new();
                                    for slot in bucket_bm.iter() {
                                        let val = sort_field.reconstruct_value(slot);
                                        if val >= old_cutoff_u32 && val < new_cutoff_u32 {
                                            expired.insert(slot);
                                        }
                                    }
                                    let boot_elapsed = start.elapsed();
                                    let expired_count = expired.len();
                                    eprintln!("Boot diff for '{}': gap={}s, scanned {} bucket slots, found {} expired in {:?}",
                                        bucket_name, gap_secs, bucket_bm.len(), expired_count, boot_elapsed);
                                    if expired_count > 0 || gap_secs > 0 {
                                        let diff = crate::bucket_diff_log::BucketDiff {
                                            cutoff_before: persisted_cutoff,
                                            cutoff_after: current_cutoff,
                                            expired: std::sync::Arc::new(expired),
                                        };
                                        // Append boot diff to on-disk log
                                        if let Some(ref log_path) = diff_log_path {
                                            let log = crate::bucket_diff_log::BucketDiffLog::new(
                                                log_path.clone(), max_diffs, 0.3,
                                            );
                                            if let Err(e) = log.append(&diff) {
                                                eprintln!("Warning: failed to append boot diff to log: {e}");
                                            }
                                        }
                                        pending.push(diff);
                                    }
                                } else if persisted_cutoff == 0 {
                                    eprintln!("Boot diff: no persisted cutoff for '{}' — first boot, full rebuild on first refresh", bucket_name);
                                } else {
                                    eprintln!("Boot diff: '{}' already current (persisted={}, current={})", bucket_name, persisted_cutoff, current_cutoff);
                                }
                            }
                        }
                        drop(tb);
                        // Also apply boot diffs to the bucket bitmaps themselves
                        if pending.current_cutoff() > 0 {
                            let mut tb = tb_arc.lock();
                            for bucket_config in &tb_config.range_buckets {
                                if let Some(bucket) = tb.get_bucket_mut(&bucket_config.name) {
                                    let new_cutoff = crate::bucket_diff_log::snap_cutoff(
                                        now_secs.saturating_sub(bucket_config.duration_secs),
                                        bucket_config.refresh_interval_secs,
                                    );
                                    if new_cutoff > bucket.last_cutoff() {
                                        bucket.subtract_expired(pending.merged_expired(), new_cutoff);
                                        eprintln!("Applied boot diff to '{}' bucket bitmap (cutoff → {})",
                                            bucket_config.name, new_cutoff);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Arc::new(ArcSwap::new(Arc::new(pending)))
        };
        // Wrap live state in RwLocks — flush thread writes, query threads read.
        let slots_arc = Arc::new(parking_lot::RwLock::new(slots));
        let filters_arc = Arc::new(parking_lot::RwLock::new(filters));
        let sorts_arc = Arc::new(parking_lot::RwLock::new(sorts));
        let (mutation_tx, mutation_rx): (crossbeam_channel::Sender<MutationOp>, crossbeam_channel::Receiver<MutationOp>) =
            crossbeam_channel::bounded(config.channel_capacity);
        let sender = MutationSender { tx: mutation_tx };
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = Arc::new(config);
        // Docstore write channel — bounded for backpressure
        let (doc_tx, doc_rx): (Sender<(u32, StoredDoc)>, Receiver<(u32, StoredDoc)>) =
            crossbeam_channel::bounded(config.channel_capacity);
        // Compaction skip counter + metrics bridge (created before compact worker)
        let compaction_skipped = Arc::new(AtomicU64::new(0));
        #[cfg(feature = "server")]
        let metrics_bridge: Arc<ArcSwap<Option<Arc<MetricsBridge>>>> = Arc::new(ArcSwap::from_pointee(None));

        let docstore = Arc::new(parking_lot::Mutex::new(docstore));
        // Shared dirty flag: flush thread sets when mutations applied, merge thread
        // clears after persisting snapshot. Prevents continuous 20GB rewrites at idle.
        let dirty_flag = Arc::new(AtomicBool::new(false));
        // Restore cursors from BitmapSilo (if available), otherwise start empty.
        let cursors = Arc::new(parking_lot::Mutex::new(restored_cursors));
        let flush_apply_count = Arc::new(AtomicU64::new(0));
        let flush_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_last_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_apply_nanos = Arc::new(AtomicU64::new(0));
        let flush_cache_nanos = Arc::new(AtomicU64::new(0));
        let flush_timebucket_nanos = Arc::new(AtomicU64::new(0));
        let flush_compact_nanos = Arc::new(AtomicU64::new(0));
        let flush_opslog_nanos = Arc::new(AtomicU64::new(0));
        // Headless mode: skip all background threads.
        if config.headless {
            eprintln!("Engine starting in headless mode (no background threads)");
            return Ok(Self {
                slots: Arc::clone(&slots_arc),
                filters: Arc::clone(&filters_arc),
                sorts: Arc::clone(&sorts_arc),
                sender,
                doc_tx,
                docstore,
                config,
                field_registry,
                shutdown,
                flush_handle: None,
                merge_handle: None,
                dirty_flag,
                time_buckets,
                string_maps: None,
                case_sensitive_fields: None,
                dictionaries: Arc::new(HashMap::new()),
                cache_silo: cache_silo_arc,
                flush_apply_count,
                flush_duration_nanos,
                flush_last_duration_nanos,
                flush_apply_nanos,
                flush_cache_nanos,
                flush_timebucket_nanos,
                flush_compact_nanos,
                flush_opslog_nanos,
                cursors,
                #[cfg(feature = "server")]
                metrics_bridge: Arc::new(ArcSwap::from_pointee(None)),
                bitmap_silo: bitmap_silo_arc.clone(),
                compaction_skipped: Arc::new(AtomicU64::new(0)),
                mutation_epoch: Arc::new(AtomicU64::new(0)),
                field_epochs: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            });
        }
        let flush_handle = {
            let flush_slots = Arc::clone(&slots_arc);
            let flush_filters = Arc::clone(&filters_arc);
            let flush_sorts = Arc::clone(&sorts_arc);
            let shutdown = Arc::clone(&shutdown);
            let docstore = Arc::clone(&docstore);
            let flush_interval_us = config.flush_interval_us;
            let flush_cache_silo = cache_silo_arc.clone();
            let flush_dirty_flag = Arc::clone(&dirty_flag);
            let flush_time_buckets = time_buckets.as_ref().map(Arc::clone);
            let flush_pending_diffs = Arc::clone(&pending_bucket_diffs);
            let flush_diff_log_path = config.storage.bitmap_path.as_ref()
                .map(|bp| std::path::Path::new(bp).join("bucket_diffs.log"));
            let flush_apply_cnt = Arc::clone(&flush_apply_count);
            let flush_dur_nanos = Arc::clone(&flush_duration_nanos);
            let flush_last_dur_nanos = Arc::clone(&flush_last_duration_nanos);
            let flush_apply_ns = Arc::clone(&flush_apply_nanos);
            let flush_cache_ns = Arc::clone(&flush_cache_nanos);
            let flush_timebucket_ns = Arc::clone(&flush_timebucket_nanos);
            let flush_compact_ns = Arc::clone(&flush_compact_nanos);
            let flush_opslog_ns = Arc::clone(&flush_opslog_nanos);
            let flush_config = Arc::clone(&config);
            let flush_field_registry = field_registry.clone();
            let flush_mutation_rx = mutation_rx;
            let has_silo = bitmap_silo_arc.is_some();
            let flush_bitmap_silo = bitmap_silo_arc.clone();
            thread::spawn(move || {
                super::flush::run_flush_thread(super::flush::FlushArgs {
                    slots: flush_slots,
                    filters: flush_filters,
                    sorts: flush_sorts,
                    shutdown,
                    docstore,
                    flush_interval_us,
                    cache_silo: flush_cache_silo,
                    dirty_flag: flush_dirty_flag,
                    time_buckets: flush_time_buckets,
                    pending_diffs: flush_pending_diffs,
                    diff_log_path: flush_diff_log_path,
                    apply_cnt: flush_apply_cnt,
                    dur_nanos: flush_dur_nanos,
                    last_dur_nanos: flush_last_dur_nanos,
                    apply_ns: flush_apply_ns,
                    cache_ns: flush_cache_ns,
                    timebucket_ns: flush_timebucket_ns,
                    compact_ns: flush_compact_ns,
                    opslog_ns: flush_opslog_ns,
                    config: flush_config,
                    field_registry: flush_field_registry,
                    mutation_rx: flush_mutation_rx,
                    doc_rx,
                    bitmap_silo: flush_bitmap_silo,
                    has_silo,
                });
            })
        };
        let merge_handle = {
            let shutdown = Arc::clone(&shutdown);
            let merge_interval_ms = config.merge_interval_ms;
            let merge_dirty_flag = Arc::clone(&dirty_flag);
            let merge_docstore = Arc::clone(&docstore);
            let merge_cache_silo = cache_silo_arc.clone();
            let merge_bitmap_silo = bitmap_silo_arc.clone();

            thread::Builder::new()
                .name("bitdex-merge".to_string())
                .spawn(move || {
                    crate::janitor::run_janitor(
                        shutdown,
                        merge_interval_ms,
                        merge_dirty_flag,
                        merge_docstore,
                        merge_cache_silo,
                        merge_bitmap_silo,
                    );
                }).expect("failed to spawn merge thread")
        };
        // DataSilo mmap reads require no separate eviction thread
        Ok(Self {
            slots: slots_arc,
            filters: filters_arc,
            sorts: sorts_arc,
            sender,
            doc_tx,
            docstore,
            config,
            field_registry,
            shutdown,
            flush_handle: Some(flush_handle),
            merge_handle: Some(merge_handle),
            dirty_flag,
            time_buckets,
            string_maps: None,
            case_sensitive_fields: None,
            dictionaries: Arc::new(HashMap::new()),
            cache_silo: cache_silo_arc,
            flush_apply_count,
            flush_duration_nanos,
            flush_last_duration_nanos,
            flush_apply_nanos,
            flush_cache_nanos,
            flush_timebucket_nanos,
            flush_compact_nanos,
            flush_opslog_nanos,
            cursors,
            #[cfg(feature = "server")]
            metrics_bridge,
            bitmap_silo: bitmap_silo_arc.clone(),
            compaction_skipped,
            mutation_epoch: Arc::new(AtomicU64::new(0)),
            field_epochs: Arc::new(parking_lot::RwLock::new(HashMap::new())),
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
    /// Set the Prometheus metrics bridge. Called by the server layer after engine creation.
    /// Background threads (compaction worker) will start recording metrics.
    #[cfg(feature = "server")]
    pub fn set_metrics_bridge(&self, bridge: MetricsBridge) {
        self.metrics_bridge.store(Arc::new(Some(Arc::new(bridge))));
    }
    /// Get the cumulative count of compaction operations skipped due to channel backpressure.
    pub fn compaction_skipped_count(&self) -> u64 {
        self.compaction_skipped.load(Ordering::Relaxed)
    }
    /// Return the current global mutation epoch.
    /// Cache entries formed before this epoch may be stale.
    pub fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch.load(Ordering::Acquire)
    }
    /// Return the epoch at which the given field was last mutated.
    /// Returns 0 if the field has never been mutated in this process lifetime.
    pub fn field_epoch(&self, field: &str) -> u64 {
        self.field_epochs.read().get(field).copied().unwrap_or(0)
    }
    /// Bump the global mutation epoch and record per-field epochs for any
    /// FilterInsert / FilterRemove / SortSet / SortClear ops in the batch.
    ///
    /// Called by every write path before dispatching ops.
    /// Atomic Release ordering ensures query threads see updated epochs after
    /// their own Acquire loads.
    fn bump_field_epochs(&self, ops: &[MutationOp]) {
        let has_field_ops = ops.iter().any(|op| matches!(
            op,
            MutationOp::FilterInsert { .. }
            | MutationOp::FilterRemove { .. }
            | MutationOp::SortSet { .. }
            | MutationOp::SortClear { .. }
        ));
        if !has_field_ops {
            return;
        }
        let new_epoch = self.mutation_epoch.fetch_add(1, Ordering::Release) + 1;
        let mut guard = self.field_epochs.write();
        for op in ops {
            match op {
                MutationOp::FilterInsert { field, .. }
                | MutationOp::FilterRemove { field, .. }
                | MutationOp::SortSet { field, .. }
                | MutationOp::SortClear { field, .. } => {
                    guard.insert(field.to_string(), new_epoch);
                }
                _ => {}
            }
        }
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
        // No-op: BitmapSilo saves dictionaries at save_snapshot time.
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
    /// Route mutation ops to the BitmapSilo ops log (primary path) or the legacy
    /// coalescer channel (fallback for tests without a silo).
    ///
    /// When a BitmapSilo is present, ops go ONLY to the silo — the coalescer is
    /// NOT also notified. Filter/sort/alive reads all go through the silo
    /// (get_effective_bitmap, frozen_top_n, alive OnceCell), so the in-memory
    /// coalescer/flush-thread path is no longer needed for production writes.
    ///
    /// The coalescer fallback is kept for tests that construct a ConcurrentEngine
    /// without a silo. It is deprecated and will be removed once all tests are
    /// migrated to the silo path.
    pub(crate) fn send_mutation_ops(&self, ops: Vec<MutationOp>) -> Result<()> {
        // Bump epoch counters so stale cache entries are detected on next query.
        self.bump_field_epochs(&ops);
        if let Some(ref silo_arc) = self.bitmap_silo {
            // Silo present: write ONLY to the BitmapSilo ops log.
            let silo = silo_arc.read();
            for op in &ops {
                match op {
                    MutationOp::FilterInsert { field, value, slots } => {
                        for &slot in slots { let _ = silo.filter_set(field, *value, slot); }
                    }
                    MutationOp::FilterRemove { field, value, slots } => {
                        for &slot in slots { let _ = silo.filter_clear(field, *value, slot); }
                    }
                    MutationOp::SortSet { field, bit_layer, slots } => {
                        for &slot in slots { let _ = silo.sort_set(field, *bit_layer, slot); }
                    }
                    MutationOp::SortClear { field, bit_layer, slots } => {
                        for &slot in slots { let _ = silo.sort_clear(field, *bit_layer, slot); }
                    }
                    MutationOp::AliveInsert { slots } => {
                        for &slot in slots { let _ = silo.alive_set(slot); }
                    }
                    MutationOp::AliveRemove { slots } => {
                        for &slot in slots { let _ = silo.alive_clear(slot); }
                    }
                    MutationOp::DeferredAlive { .. } => {} // handled separately
                }
            }
        } else {
            // No silo: fall back to the legacy coalescer channel (test path only).
            // DEPRECATED — remove once all tests use a BitmapSilo.
            self.sender.send_batch(ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded("coalescer channel disconnected".to_string())
            })?;
        }
        Ok(())
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
        self.send_mutation_ops(ops)
    }
    /// Get the number of alive documents.
    pub fn alive_count(&self) -> u64 {
        self.slots.read().alive_count()
    }
    /// Flush loop stats: (apply_count, cumulative_duration_nanos, last_duration_nanos).
    pub fn flush_stats(&self) -> (u64, u64, u64) {
        (
            self.flush_apply_count.load(Ordering::Relaxed),
            self.flush_duration_nanos.load(Ordering::Relaxed),
            self.flush_last_duration_nanos.load(Ordering::Relaxed),
        )
    }
    /// Per-phase flush timing in nanoseconds: (apply, cache, 0, timebucket, compact, opslog).
    /// The third slot is 0 (previously measured ArcSwap publish, now removed).
    pub fn flush_phase_stats(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.flush_apply_nanos.load(Ordering::Relaxed),
            self.flush_cache_nanos.load(Ordering::Relaxed),
            0, // publish_nanos removed (no ArcSwap)
            self.flush_timebucket_nanos.load(Ordering::Relaxed),
            self.flush_compact_nanos.load(Ordering::Relaxed),
            self.flush_opslog_nanos.load(Ordering::Relaxed),
        )
    }
    /// Get the high-water mark slot counter.
    pub fn slot_counter(&self) -> u32 {
        self.slots.read().slot_counter()
    }
    /// Reconstruct the sort value for a given slot in the named sort field.
    /// Returns None if the field is not found in the in-memory sort index.
    pub fn reconstruct_sort_value(&self, field: &str, slot: u32) -> Option<u32> {
        self.sorts.read().get_field(field).map(|f| f.reconstruct_value(slot))
    }
    // ---- Named cursors ----
    /// Set a named cursor value. The value is persisted to disk at the next
    /// merge thread checkpoint, atomically alongside bitmap snapshots.
    pub fn set_cursor(&self, name: String, value: String) {
        self.cursors.lock().insert(name, value);
        // Mark dirty so the merge thread will write at next cycle.
        self.dirty_flag.store(true, Ordering::Release);
    }
    /// Get a named cursor value (in-memory, not from disk).
    pub fn get_cursor(&self, name: &str) -> Option<String> {
        self.cursors.lock().get(name).cloned()
    }
    /// Get all named cursors.
    pub fn get_all_cursors(&self) -> HashMap<String, String> {
        self.cursors.lock().clone()
    }
    /// Retrieve a stored document by slot ID.
    ///
    /// Checks the in-memory doc cache first. On miss, reads from disk and
    /// populates the cache for subsequent reads.
    pub fn get_document(&self, slot_id: u32) -> Result<Option<StoredDoc>> {
        // Read directly from DataSilo (no separate doc cache — DataSilo uses mmap).
        Ok(self.docstore.lock().get(slot_id)?)
    }
    /// Compact the docstore, reclaiming space from old write transactions.
    pub fn compact_docstore(&self) -> Result<bool> {
        Ok(self.docstore.lock().compact()?)
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

    /// Get a clone of the Arc<Mutex<DocSiloAdapter>> for external writers.
    pub fn docstore_arc(&self) -> Arc<parking_lot::Mutex<DocSiloAdapter>> {
        Arc::clone(&self.docstore)
    }
    /// Check if a slot is alive (for non-alive slot filtering in ops processing).
    pub fn is_slot_alive(&self, slot: u32) -> bool {
        self.slots.read().is_alive(slot)
    }
    /// Build the schema registry for version-aware default reconstruction.
    pub fn build_schema_registry(&self) -> std::collections::HashMap<u8, std::collections::HashMap<String, serde_json::Value>> {
        self.docstore.lock().build_schema_registry()
    }

    /// Prepare field names for bulk writing (ensures field dictionary is ready).
    pub fn prepare_field_names(&self, field_names: &[String]) -> crate::error::Result<()> {
        self.docstore.lock().prepare_field_names(field_names)
            .map_err(|e| crate::error::BitdexError::Storage(format!("prepare_field_names: {e}")))
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
    /// Approximate number of pending MutationOps in the write channel (for metrics).
    pub fn flush_queue_depth(&self) -> usize {
        self.sender.pending_count()
    }
    /// Report bitmap memory usage broken down by component (lock-free snapshot).
    ///
    /// Returns (slot_bytes, filter_bytes, sort_bytes, cache_entries, cache_bytes,
    ///          filter_details, sort_details)
    /// where all sizes are serialized bitmap bytes — no allocator or redb overhead.
    #[allow(clippy::type_complexity)]
    /// Lightweight memory totals — skips per-field detail for fast stats endpoint.
    pub fn bitmap_memory_totals(&self) -> (usize, usize, usize) {
        let slot_bytes = self.slots.read().bitmap_bytes();
        let filter_bytes = self.filters.read().bitmap_bytes();
        let sort_bytes = self.sorts.read().bitmap_bytes();
        (slot_bytes, filter_bytes, sort_bytes)
    }
    pub fn bitmap_memory_report(
        &self,
    ) -> (usize, usize, usize, usize, usize, Vec<(String, usize, usize)>, Vec<(String, usize)>) {
        let slot_bytes = self.slots.read().bitmap_bytes();
        let filter_bytes = self.filters.read().bitmap_bytes();
        let sort_bytes = self.sorts.read().bitmap_bytes();
        let cache_entries = 0usize;
        let cache_bytes = 0usize;
        let filter_details: Vec<(String, usize, usize)> = self.filters.read()
            .per_field_bytes()
            .into_iter()
            .map(|(name, count, bytes)| (name.to_string(), count, bytes))
            .collect();
        let sort_details: Vec<(String, usize)> = self.sorts.read()
            .per_field_bytes()
            .into_iter()
            .map(|(name, bytes)| (name.to_string(), bytes))
            .collect();
        (slot_bytes, filter_bytes, sort_bytes, cache_entries, cache_bytes, filter_details, sort_details)
    }
    /// Rebuild all time bucket bitmaps from scratch by scanning the sort field
    /// for all alive slots. Use after a bulk dump or when buckets are empty/stale.
    /// Returns (bucket_count, total_slots_scanned) or an error.
    pub fn rebuild_time_buckets(&self) -> crate::error::Result<(usize, u64)> {
        let tb_arc = self.time_buckets.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config("no time_buckets configured".into())
        })?;
        let sort_field_name = {
            let tb = tb_arc.lock();
            tb.sort_field_name().to_string()
        };
        // Collect (slot, timestamp) for all alive slots under read locks
        let slot_values: Vec<(u32, u64)> = {
            let sorts_r = self.sorts.read();
            let slots_r = self.slots.read();
            let sort_field = sorts_r.get_field(&sort_field_name).ok_or_else(|| {
                crate::error::BitdexError::Config(format!(
                    "time bucket sort field '{}' not loaded", sort_field_name
                ))
            })?;
            let alive = slots_r.alive_bitmap();
            let mut vals = Vec::with_capacity(alive.len() as usize);
            for slot in alive.iter() {
                let ts = sort_field.reconstruct_value(slot) as u64;
                vals.push((slot, ts));
            }
            vals
        };
        let slot_count = slot_values.len() as u64;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Rebuild each bucket
        let mut tb = tb_arc.lock();
        let bucket_names: Vec<String> = tb.bucket_names();
        for name in &bucket_names {
            tb.rebuild_bucket(name, slot_values.iter().copied(), now_secs);
        }
        let bucket_count = bucket_names.len();
        // Mark dirty so merge thread persists
        self.dirty_flag.store(true, std::sync::atomic::Ordering::Release);
        // CacheSilo entries will be recomputed on the next query miss after rebuild.
        eprintln!(
            "rebuild_time_buckets: rebuilt {} buckets from {} alive slots in sort field '{}'",
            bucket_count, slot_count, sort_field_name
        );
        Ok((bucket_count, slot_count))
    }

    /// Get per-bucket statistics (name, slot count, cutoff).
    pub fn time_bucket_stats(&self) -> serde_json::Value {
        if let Some(ref tb_arc) = self.time_buckets {
            let tb = tb_arc.lock();
            let mut buckets = serde_json::Map::new();
            for name in tb.bucket_names() {
                if let Some(bucket) = tb.get_bucket(&name) {
                    buckets.insert(name, serde_json::json!({
                        "slots": bucket.bitmap().len(),
                        "last_cutoff": bucket.last_cutoff(),
                    }));
                }
            }
            serde_json::Value::Object(buckets)
        } else {
            serde_json::Value::Null
        }
    }

    /// Update the refresh interval for a named time bucket.
    /// Returns true if the bucket was found and updated, false if no time bucket
    /// manager exists or the bucket name was not found.
    pub fn set_time_bucket_refresh_interval(&self, bucket_name: &str, interval_secs: u64) -> bool {
        if let Some(ref tb_arc) = self.time_buckets {
            tb_arc.lock().set_refresh_interval(bucket_name, interval_secs)
        } else {
            false
        }
    }
    /// Clear all CacheSilo entries. Stale entries will be recomputed on next query miss.
    pub fn clear_cache(&self) {
        if let Some(ref silo_arc) = self.cache_silo {
            if let Err(e) = silo_arc.write().compact() {
                eprintln!("clear_cache: compact error: {e}");
            }
        }
    }
    /// Purge the CacheSilo: entries are recomputed on next query miss.
    pub fn purge_bounds(&self) -> crate::error::Result<()> {
        self.clear_cache();
        eprintln!("purge_bounds: cleared CacheSilo");
        Ok(())
    }
    /// Save a full snapshot: bitmaps to BitmapSilo, field dict to disk.
    ///
    /// When a live BitmapSilo is present (ops-on-read path), all bitmap mutations have
    /// already been written to the silo ops log via `send_mutation_ops`. This method
    /// flushes the remaining in-memory state (slot_counter, cursors) to the silo and
    /// compacts the ops log into a frozen snapshot, then saves the field dictionary.
    ///
    /// When no live silo exists (no-silo fallback for tests), this is a no-op for bitmaps.
    pub fn save_snapshot(&self) -> Result<()> {
        // Save field dictionary
        self.docstore.lock().save_field_dict()
            .map_err(|e| crate::error::BitdexError::Storage(format!("save_field_dict: {e}")))?;

        if let Some(ref silo_arc) = self.bitmap_silo {
            // Ops-on-read path: bitmaps already written incrementally to silo ops log.
            // Flush metadata (slot_counter, cursors) and compact ops → frozen snapshot.
            let cursors = self.cursors.lock().clone();
            let slot_counter = self.slots.read().slot_counter();
            {
                let silo = silo_arc.read();
                silo.save_meta(slot_counter, &cursors)
                    .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::save_meta: {e}")))?;
            }
            {
                let mut silo = silo_arc.write();
                let count = silo.compact()
                    .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::compact: {e}")))?;
                eprintln!("save_snapshot: compacted {} silo entries", count);
            }
        }

        Ok(())
    }
    /// Save a full snapshot to a custom path.
    ///
    /// Serializes the current in-memory bitmap state to the given path. Used by
    /// the benchmark persist/restore phase to write a snapshot of an engine that
    /// was loaded without a bitmap_path (no live silo).
    pub fn save_snapshot_to(&self, path: &Path) -> Result<()> {
        let cursors = self.cursors.lock().clone();
        let filters_r = self.filters.read();
        let sorts_r = self.sorts.read();
        let slots_r = self.slots.read();
        let mut silo = crate::silos::bitmap_silo::BitmapSilo::open(path)
            .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::open: {e}")))?;
        let count = silo.save_all_parallel(&*filters_r, &*sorts_r, &*slots_r, &cursors)
            .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::save_all_parallel: {e}")))?;
        eprintln!("save_snapshot_to: saved {} bitmaps", count);
        Ok(())
    }
    /// Save the current snapshot to disk (via BitmapSilo) and replace the in-memory
    /// filter/sort state with empty unloaded versions to free memory.
    ///
    /// With BitmapSilo, all bitmap mutations are already in the silo ops log. This
    /// method flushes metadata, compacts the silo, then resets the in-memory indexes
    /// so memory drops to near-zero. Queries are served from the silo mmap after this.
    pub fn save_and_unload(&self) -> Result<()> {
        // First, flush metadata and compact the silo so the snapshot is durable.
        self.save_snapshot()?;
        // Build an unloaded staging buffer: keep slots (always needed), empty filter/sort fields.
        let (new_slots, new_filters, new_sorts) = {
            let slots_r = self.slots.read();
            let filters_r = self.filters.read();
            let sorts_r = self.sorts.read();
            let new_slots = slots_r.clone();
            let mut new_filters = crate::engine::filter::FilterIndex::new();
            for fc in &self.config.filter_fields {
                new_filters.add_field(fc.clone());
            }
            for fc in &self.config.filter_fields {
                new_filters.unload_from(&*filters_r, &fc.name);
            }
            let mut new_sorts = crate::engine::sort::SortIndex::new();
            for sc in &self.config.sort_fields {
                new_sorts.add_field(sc.clone());
            }
            for sc in &self.config.sort_fields {
                new_sorts.unload_from(&*sorts_r, &sc.name);
            }
            (new_slots, new_filters, new_sorts)
        };
        // Swap in unloaded state under write locks
        *self.slots.write() = new_slots;
        *self.filters.write() = new_filters;
        *self.sorts.write() = new_sorts;
        self.dirty_flag.store(true, Ordering::Release);
        self.invalidate_all_caches();
        Ok(())
    }
    /// Get a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// Get a cloneable MutationSender for submitting ops to the coalescer channel.
    /// Used by the WAL reader thread to send ops via CoalescerSink.
    pub fn mutation_sender(&self) -> MutationSender {
        self.sender.clone()
    }
    /// Pin BitmapSilo generations at capture boundaries.
    /// Returns Ok(None) until BitmapSilo generation pinning is implemented.
    pub fn pin_shard_generations(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Force-compact all shards. Compacts the DataSilo (applies pending ops).
    /// BitmapSilo compaction is handled at save_snapshot time.
    pub fn compact_all(
        &self,
        _threshold: u32,
        _workers: usize,
        _compact_bitmaps: bool,
        compact_docs: bool,
        progress: Arc<AtomicU64>,
    ) -> Result<CompactResult> {
        let t0 = std::time::Instant::now();
        let mut result = CompactResult::default();
        // Compact DataSilo (apply pending ops log)
        if compact_docs {
            let did_compact = self.docstore.lock().compact()
                .map_err(|e| crate::error::BitdexError::Storage(format!("DataSilo compact: {e}")))?;
            if did_compact {
                result.shards_compacted += 1;
            }
            result.shards_scanned += 1;
            progress.fetch_add(1, Ordering::Relaxed);
        }
        result.elapsed_secs = t0.elapsed().as_secs_f64();
        Ok(result)
    }

    fn invalidate_all_caches(&self) {
        // CacheSilo entries become stale after bulk loads; they'll be recomputed on miss.
        // Full purge via clear_cache() is available if needed.
    }
    /// Merge pre-built bitmap maps directly into the live engine state.
    ///
    /// Used by the NDJSON loader to apply accumulated bitmaps from a parsed chunk
    /// without the staging InnerEngine pattern. Takes write locks briefly to OR-merge
    /// filter/sort bitmaps and alive bits into the existing live state.
    pub fn merge_bitmap_maps(
        &self,
        filter_maps: HashMap<String, HashMap<u64, RoaringBitmap>>,
        sort_maps: HashMap<String, HashMap<usize, RoaringBitmap>>,
        alive: RoaringBitmap,
    ) {
        {
            let mut filters_w = self.filters.write();
            for (field_name, value_map) in filter_maps {
                if let Some(field) = filters_w.get_field_mut(&field_name) {
                    for (value, bitmap) in value_map {
                        field.or_bitmap(value, &bitmap);
                    }
                }
            }
        }
        {
            let mut sorts_w = self.sorts.write();
            for (field_name, bit_map) in sort_maps {
                if let Some(field) = sorts_w.get_field_mut(&field_name) {
                    for (bit, bitmap) in bit_map {
                        field.or_layer(bit, &bitmap);
                    }
                }
            }
        }
        {
            self.slots.write().alive_or_bitmap(&alive);
        }
        self.dirty_flag.store(true, Ordering::Release);
        self.invalidate_all_caches();
    }
    /// Signal background threads to stop (non-blocking, works through Arc).
    /// Threads will exit on their next loop iteration. Use this when you can't
    /// get `&mut self` (e.g., engine behind Arc with multiple references).
    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
    /// Shutdown the flush, merge, and compaction threads gracefully.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_handle.take() {
            handle.join().ok();
        }
        if let Some(handle) = self.merge_handle.take() {
            handle.join().ok();
        }
        // DataSilo: no separate compaction/eviction threads
    }
}
impl Drop for ConcurrentEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}
