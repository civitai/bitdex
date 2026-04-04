use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use arc_swap::{ArcSwap, Guard};
use crossbeam_channel::{Receiver, Sender};
use roaring::RoaringBitmap;
use crate::cache;
use crate::concurrency::InFlightTracker;
use crate::config::Config;
use crate::doc_format::{StoredDoc};
use crate::doc_silo_adapter::DocSiloAdapter;
use crate::error::Result;
use crate::executor::{CaseSensitiveFields, QueryExecutor, StringMaps};
use crate::mutation::{diff_document, diff_patch, value_to_bitmap_key, Document, FieldRegistry, PatchPayload};
use crate::planner;
use crate::query::{BitdexQuery, FilterClause, SortClause, SortDirection};
use crate::query_metrics::{QueryTrace, QueryTraceCollector, SortTrace};
use crate::time_buckets::TimeBucketManager;
use crate::types::QueryResult;
use crate::unified_cache::{
    UnifiedCache, UnifiedCacheConfig, UnifiedKey,
    evaluate_filter_work, evaluate_sort_work,
};
use crate::write_coalescer::{MutationOp, MutationSender, WriteCoalescer};
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
        /// Loading mode flag — handler clears this AFTER reading the published snapshot,
        /// preventing the flush thread's loading-exit force-publish from overwriting
        /// the loader's data before we save it.
        loading_mode: Arc<AtomicBool>,
        /// Oneshot sender — caller blocks until save+unload is complete.
        /// Returns Ok(()) on success or error message on failure.
        done: crossbeam_channel::Sender<std::result::Result<(), String>>,
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
/// Result of a compact_all() operation.
#[derive(Debug, Default, serde::Serialize)]
pub struct CompactResult {
    pub shards_scanned: u64,
    pub shards_compacted: u64,
    pub shards_skipped: u64,
    pub elapsed_secs: f64,
}

/// Readers load the current snapshot via `load_full()` — fully lock-free,
/// no contention with writers or the flush thread.
pub struct ConcurrentEngine {
    inner: Arc<ArcSwap<InnerEngine>>,
    sender: MutationSender,
    doc_tx: Sender<(u32, StoredDoc)>,
    docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>,
    config: Arc<Config>,
    field_registry: FieldRegistry,
    in_flight: InFlightTracker,
    shutdown: Arc<AtomicBool>,
    flush_handle: Option<JoinHandle<()>>,
    merge_handle: Option<JoinHandle<()>>,
    loading_mode: Arc<AtomicBool>,
    dirty_since_snapshot: Arc<AtomicBool>,
    time_buckets: Option<Arc<parking_lot::Mutex<TimeBucketManager>>>,
    /// Pending bucket diffs for lazy application on cache reads.
    /// Flush thread stores new snapshots; query threads load for diff application.
    pending_bucket_diffs: Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>,
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
    /// CacheSilo: persistent cache backed by DataSilo. Flush thread writes dirty
    /// entries; merge thread compacts; startup loads entries into UnifiedCache.
    /// None when bitmap_path is not configured.
    cache_silo: Option<Arc<parking_lot::RwLock<crate::cache_silo::CacheSilo>>>,
    /// Flush loop stats: total snapshot publishes (monotonic counter).
    flush_publish_count: Arc<AtomicU64>,
    /// Flush loop stats: cumulative flush duration in nanoseconds.
    flush_duration_nanos: Arc<AtomicU64>,
    /// Flush loop stats: most recent flush duration in nanoseconds.
    flush_last_duration_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last apply_prepared duration in nanoseconds.
    flush_apply_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last cache maintenance duration in nanoseconds.
    flush_cache_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last staging.clone() + ArcSwap publish duration in nanoseconds.
    flush_publish_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last ops-log append duration in nanoseconds (after publish).
    flush_opslog_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last time bucket maintenance duration in nanoseconds.
    flush_timebucket_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last diff compaction duration in nanoseconds.
    flush_compact_nanos: Arc<AtomicU64>,
    /// Named cursors: opaque key-value pairs persisted at checkpoint time.
    /// Callers (e.g. pg-sync sidecars) use these to track replication progress.
    cursors: Arc<parking_lot::Mutex<HashMap<String, String>>>,
    // BoundStore counters removed (DataSilo Phase 4)
    /// Metrics bridge: prometheus handles set by server layer, read by background threads.
    #[cfg(feature = "server")]
    metrics_bridge: Arc<ArcSwap<Option<Arc<MetricsBridge>>>>,
    /// BitmapSilo for frozen bitmap reads. Queries read filter/sort bitmaps
    /// directly from the silo's mmap via FrozenRoaringBitmap::view().
    /// RwLock: readers (queries) share access; writer (save_snapshot) gets exclusive.
    bitmap_silo: Option<Arc<parking_lot::RwLock<crate::bitmap_silo::BitmapSilo>>>,
    /// Compaction skip counter.
    compaction_skipped: Arc<AtomicU64>,
    /// Prefetch channel sender — sends UnifiedKey to background worker for
    /// async cache expansion. None when prefetch is disabled.
    prefetch_tx: Option<Sender<UnifiedKey>>,
    /// Background prefetch worker thread handle.
    prefetch_handle: Option<JoinHandle<()>>,
    /// WAL writer for Sync V2 write path. When set, put() and patch_document()
    /// decompose documents into ops and write to WAL instead of directly to coalescer.
    /// The WAL reader thread picks up ops and routes through apply_ops_batch.
    #[cfg(feature = "pg-sync")]
    wal_writer: Option<Arc<crate::ops_wal::WalWriter>>,
}
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

        // Restore from BitmapSilo: alive+meta loaded to heap; filter/sort stay frozen in mmap
        let mut slots = crate::slot::SlotAllocator::new();
        let mut restored_cursors: HashMap<String, String> = HashMap::new();
        let mut bitmap_silo_arc: Option<Arc<parking_lot::RwLock<crate::bitmap_silo::BitmapSilo>>> = None;
        if let Some(ref bitmap_path) = config.storage.bitmap_path {
            match crate::bitmap_silo::BitmapSilo::open(bitmap_path) {
                Ok(silo) if silo.has_data() => {
                    let t_restore = std::time::Instant::now();
                    // Load alive bitmap + metadata (always owned — used by SlotAllocator)
                    if let Ok(Some(alive)) = silo.load_alive() {
                        let meta = silo.load_meta().ok().flatten();
                        let slot_counter = meta.as_ref()
                            .and_then(|m| m.get("slot_counter"))
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .unwrap_or(0);
                        let alive_count = alive.len();
                        slots = crate::slot::SlotAllocator::from_state(
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
        let uc_config = UnifiedCacheConfig {
            max_entries: config.cache.max_entries,
            max_bytes: config.cache.max_bytes,
            initial_capacity: config.cache.initial_capacity,
            max_capacity: config.cache.max_capacity,
            min_filter_size: config.cache.min_filter_size,
            max_maintenance_work: config.cache.max_maintenance_work,
            max_maintenance_ms: config.cache.max_maintenance_ms,
            prefetch_threshold: config.cache.prefetch_threshold,
        };
        let mut uc = UnifiedCache::new(uc_config);
        // CacheSilo: open and restore persisted cache entries into UnifiedCache.
        let cache_silo_arc: Option<Arc<parking_lot::RwLock<crate::cache_silo::CacheSilo>>> =
            config.storage.bitmap_path.as_ref().and_then(|bp| {
                let silo_path = std::path::Path::new(bp).join("cache_silo");
                match crate::cache_silo::CacheSilo::open(&silo_path) {
                    Ok(silo) => Some(Arc::new(parking_lot::RwLock::new(silo))),
                    Err(e) => {
                        eprintln!("CacheSilo: open error (skipping persistence): {e}");
                        None
                    }
                }
            });
        // Restore persisted entries into the UnifiedCache before accepting queries.
        if let Some(ref cs_arc) = cache_silo_arc {
            let cs = cs_arc.read();
            match cs.load_all() {
                Ok(entries) => {
                    let count = entries.len();
                    uc.begin_restore();
                    for (_key_hash, entry_data) in entries {
                        // Reconstruct UnifiedEntry from CacheEntryData and insert
                        let key = entry_data.key.clone();
                        let entry = crate::unified_cache::UnifiedEntry::from_cache_entry_data(
                            entry_data,
                            uc.config().initial_capacity,
                            uc.config().max_capacity,
                        );
                        uc.insert_restored_entry(key, entry);
                    }
                    uc.finish_restore();
                    eprintln!("CacheSilo: restored {count} cache entries from disk");
                }
                Err(e) => {
                    eprintln!("CacheSilo: load_all error (starting with empty cache): {e}");
                }
            }
        }
        let unified_cache = Arc::new(parking_lot::Mutex::new(uc));
        let loading_mode = Arc::new(AtomicBool::new(false));
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
        // Command channel: external threads send state transition commands to flush thread.
        let (cmd_tx, cmd_rx): (Sender<FlushCommand>, Receiver<FlushCommand>) =
            crossbeam_channel::unbounded();
        let flush_publish_count = Arc::new(AtomicU64::new(0));
        let flush_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_last_duration_nanos = Arc::new(AtomicU64::new(0));
        let flush_apply_nanos = Arc::new(AtomicU64::new(0));
        let flush_cache_nanos = Arc::new(AtomicU64::new(0));
        let flush_publish_nanos = Arc::new(AtomicU64::new(0));
        let flush_timebucket_nanos = Arc::new(AtomicU64::new(0));
        let flush_compact_nanos = Arc::new(AtomicU64::new(0));
        let flush_opslog_nanos = Arc::new(AtomicU64::new(0));
        // Headless mode: skip all background threads.
        if config.headless {
            eprintln!("Engine starting in headless mode (no background threads)");
            return Ok(Self {
                inner,
                sender,
                doc_tx,
                docstore,
                config,
                field_registry,
                in_flight: InFlightTracker::new(),
                shutdown,
                flush_handle: None,
                merge_handle: None,
                loading_mode,
                dirty_since_snapshot: dirty_flag,
                time_buckets,
                pending_bucket_diffs: Arc::clone(&pending_bucket_diffs),
                cmd_tx,
                string_maps: None,
                case_sensitive_fields: None,
                dictionaries: Arc::new(HashMap::new()),
                unified_cache,
                cache_silo: cache_silo_arc,
                flush_publish_count,
                flush_duration_nanos,
                flush_last_duration_nanos,
                flush_apply_nanos,
                flush_cache_nanos,
                flush_publish_nanos,
                flush_timebucket_nanos,
                flush_compact_nanos,
                flush_opslog_nanos,
                cursors,
                #[cfg(feature = "server")]
                metrics_bridge: Arc::new(ArcSwap::from_pointee(None)),
                bitmap_silo: bitmap_silo_arc.clone(),
                compaction_skipped: Arc::new(AtomicU64::new(0)),
                prefetch_tx: None,
                prefetch_handle: None,
                #[cfg(feature = "pg-sync")]
                wal_writer: None,
            });
        }
        let flush_handle = {
            let inner = Arc::clone(&inner);
            let shutdown = Arc::clone(&shutdown);
            let docstore = Arc::clone(&docstore);
            let flush_interval_us = config.flush_interval_us;
            let flush_unified_cache = Arc::clone(&unified_cache);
            let flush_cache_silo = cache_silo_arc.clone();
            let flush_loading_mode = Arc::clone(&loading_mode);
            let flush_dirty_flag = Arc::clone(&dirty_flag);
            let flush_time_buckets = time_buckets.as_ref().map(Arc::clone);
            let flush_pending_diffs = Arc::clone(&pending_bucket_diffs);
            let flush_diff_log_path = config.storage.bitmap_path.as_ref()
                .map(|bp| std::path::Path::new(bp).join("bucket_diffs.log"));
            let flush_pub_count = Arc::clone(&flush_publish_count);
            let flush_dur_nanos = Arc::clone(&flush_duration_nanos);
            let flush_last_dur_nanos = Arc::clone(&flush_last_duration_nanos);
            let flush_apply_ns = Arc::clone(&flush_apply_nanos);
            let flush_cache_ns = Arc::clone(&flush_cache_nanos);
            let flush_publish_ns = Arc::clone(&flush_publish_nanos);
            let flush_timebucket_ns = Arc::clone(&flush_timebucket_nanos);
            let flush_compact_ns = Arc::clone(&flush_compact_nanos);
            let flush_opslog_ns = Arc::clone(&flush_opslog_nanos);
            let flush_config = Arc::clone(&config);
            let flush_field_registry = field_registry.clone();
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
                    let mut stale_fields: Vec<String> = Vec::new();
                    // Phase 2: Apply mutations to staging (private, no lock needed)
                    let flush_start = Instant::now();
                    if bitmap_count > 0 {
                        staging_dirty = true;
                        flush_dirty_flag.store(true, Ordering::Release);
                        let t_apply = Instant::now();
                        coalescer.apply_prepared(
                            &mut staging.slots,
                            &mut staging.filters,
                            &mut staging.sorts,
                        );
                        flush_apply_ns.store(t_apply.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        // Collect mutated field names for bitmap memory cache staleness tracking.
                        for fgk in coalescer.filter_insert_entries().keys() {
                            stale_fields.push(fgk.field.to_string());
                        }
                        for fgk in coalescer.filter_remove_entries().keys() {
                            stale_fields.push(fgk.field.to_string());
                        }
                        for sgk in coalescer.sort_set_entries().keys() {
                            stale_fields.push(sgk.field.to_string());
                        }
                        for sgk in coalescer.sort_clear_entries().keys() {
                            stale_fields.push(sgk.field.to_string());
                        }
                        // Yield CPU after apply to let tokio I/O threads deliver
                        // pending HTTP responses. Without this, the flush thread
                        // monopolizes CPU across apply+cache+publish (~20ms aggregate),
                        // causing 1-4s response delivery delays under concurrent load.
                        std::thread::yield_now();
                        // In loading mode, skip all maintenance and snapshot publishing.
                        // This avoids the expensive staging.clone() → Arc::make_mut clone
                        // cascade that dominates write cost at scale.
                        if !flush_loading_mode.load(Ordering::Relaxed) {
                            // Live maintenance for time buckets: add newly-alive slots to
                            // qualifying buckets, remove deleted slots from all buckets.
                            let t_tb = Instant::now();
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
                            flush_timebucket_ns.store(t_tb.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            // Unified cache live maintenance (two-phase).
                            //
                            // Split into three brief-lock phases to avoid blocking
                            // query handlers during the expensive slot evaluation:
                            //   Phase A: brief lock — collect work + cheap ops
                            //   Phase B: NO lock — evaluate slots against staging
                            //   Phase C: brief lock — apply results
                            let t_cache = Instant::now();
                            // Phase A: Brief lock — collect work items and do cheap ops
                            let (filter_work, filter_over_budget, sort_work, sort_over_budget) = {
                                let mut uc = flush_unified_cache.lock();
                                // Targeted alive removal (fast: O(1) per entry per remove)
                                if !uc.is_empty() {
                                    for &slot in coalescer.alive_removes() {
                                        uc.remove_slot_from_all(slot);
                                    }
                                }
                                // Collect filter maintenance work
                                let (fw, fob) = if !coalescer.mutated_filter_fields().is_empty() {
                                    uc.collect_filter_work(
                                        coalescer.filter_insert_entries(),
                                        coalescer.filter_remove_entries(),
                                    )
                                } else {
                                    (Vec::new(), Vec::new())
                                };
                                // Collect sort maintenance work
                                let sort_mutations = coalescer.mutated_sort_slots();
                                let (sw, sob) = if !sort_mutations.is_empty() {
                                    uc.collect_sort_work(&sort_mutations)
                                } else {
                                    (Vec::new(), Vec::new())
                                };
                                // Tombstone unloaded entries (fast meta-index ops).
                                // Runs even when cache is empty — meta-index may be
                                // populated from meta.bin after restart (§3.2).
                                if uc.persistence_enabled() {
                                    let filter_fields: Vec<&str> = coalescer
                                        .mutated_filter_fields()
                                        .iter()
                                        .copied()
                                        .collect();
                                    if !filter_fields.is_empty() {
                                        let n = uc.tombstone_unloaded_for_filter(&filter_fields);
                                        let _ = n;
                                    }
                                    let sort_mutations = coalescer.mutated_sort_slots();
                                    let sort_fields: Vec<&str> = sort_mutations
                                        .keys()
                                        .copied()
                                        .collect();
                                    if !sort_fields.is_empty() {
                                        let n = uc.tombstone_unloaded_for_sort(&sort_fields);
                                        let _ = n;
                                    }
                                    if coalescer.has_alive_mutations()
                                        && !coalescer.alive_removes().is_empty()
                                    {
                                        let n = uc.tombstone_all_unloaded();
                                        let _ = n;
                                    }
                                }
                                (fw, fob, sw, sob)
                            }; // Phase A lock released
                            // Phase B: NO lock — evaluate slots against staging data.
                            // This is the expensive part (slot_matches_filter, reconstruct_value)
                            // that previously held the Mutex for ~469ms.
                            let deadline = if flush_config.cache.max_maintenance_ms > 0 {
                                Some(Instant::now() + Duration::from_millis(flush_config.cache.max_maintenance_ms))
                            } else {
                                None
                            };
                            let (filter_results, filter_timed_out) = if !filter_work.is_empty() {
                                evaluate_filter_work(&filter_work, &staging.filters, &staging.sorts, deadline)
                            } else {
                                (Vec::new(), Vec::new())
                            };
                            let (sort_results, sort_timed_out) = if !sort_work.is_empty() {
                                evaluate_sort_work(&sort_work, &staging.filters, &staging.sorts, deadline)
                            } else {
                                (Vec::new(), Vec::new())
                            };
                            // Phase C: Brief lock — apply results
                            if !filter_results.is_empty() || !sort_results.is_empty()
                                || !filter_over_budget.is_empty() || !sort_over_budget.is_empty()
                                || !filter_timed_out.is_empty() || !sort_timed_out.is_empty()
                            {
                                let mut uc = flush_unified_cache.lock();
                                uc.apply_maintenance_results(&filter_results);
                                uc.apply_maintenance_results(&sort_results);
                                uc.mark_for_rebuild_batch(&filter_over_budget);
                                uc.mark_for_rebuild_batch(&sort_over_budget);
                                uc.mark_for_rebuild_batch(&filter_timed_out);
                                uc.mark_for_rebuild_batch(&sort_timed_out);
                                uc.reconcile_bytes();
                            }
                            flush_cache_ns.store(t_cache.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            // CacheSilo persistence: save dirty cache entries after maintenance.
                            // Only runs when a CacheSilo is configured. Collects (key_hash, encoded
                            // bytes) under a brief lock, then writes outside the lock.
                            if let Some(ref cs_arc) = flush_cache_silo {
                                let dirty: Vec<(u32, crate::cache_silo::CacheEntryData)> = {
                                    let mut uc = flush_unified_cache.lock();
                                    uc.drain_dirty_for_silo()
                                };
                                if !dirty.is_empty() {
                                    let cs = cs_arc.read();
                                    for (key_hash, entry_data) in dirty {
                                        if let Err(e) = cs.save_entry(key_hash, &entry_data) {
                                            eprintln!("CacheSilo: save_entry error: {e}");
                                        }
                                    }
                                }
                            }
                            // Yield CPU after cache maintenance to let tokio deliver responses.
                            std::thread::yield_now();
                            // Periodic filter diff compaction: merge dirty diffs into
                            // bases so apply_diff/fused don't accumulate unbounded diffs.
                            // Runs every COMPACTION_INTERVAL flush cycles (~5s).
                            // Sort diffs and alive are already merged eagerly in WriteBatch::apply().
                            //
                            // CRITICAL: Only compact fields that have dirty diffs. Using
                            // fields_mut() iterates ALL fields and calls Arc::make_mut on
                            // each — which deep-clones the entire FilterField HashMap when
                            // the Arc is shared with a published snapshot (refcount > 1).
                            // For tagIds (31K entries), this clone takes seconds. Targeted
                            // compaction avoids the clone cascade on untouched fields.
                            let t_compact = Instant::now();
                            if flush_cycle % COMPACTION_INTERVAL == 0 {
                                // Collect names of dirty fields first (read-only, no Arc::make_mut)
                                let dirty_fields: Vec<String> = staging.filters.fields()
                                    .filter(|(_, field)| field.has_dirty())
                                    .map(|(name, _)| name.clone())
                                    .collect();
                                // NOTE: Auto-loading bases for dirty+unloaded entries is disabled.
                                // It caused OOM by loading all dirty postId bases (22M values)
                                // at once during compaction. Dirty diffs on unloaded fields are
                                // small and persist safely via BitmapSilo ops log. They'll be
                                // merged when the field is eventually loaded by a query.
                                // Only make_mut + merge on fields that actually have dirty diffs
                                for name in &dirty_fields {
                                    if let Some(field) = staging.filters.get_field_mut(name) {
                                        field.merge_dirty();
                                    }
                                }
                            }
                            flush_compact_ns.store(t_compact.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            flush_cycle += 1;
                            // Publish new snapshot atomically (Arc-per-bitmap CoW clone)
                            let t_publish = Instant::now();
                            inner.store(Arc::new(staging.clone()));
                            flush_publish_ns.store(t_publish.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            staging_dirty = false;
                            stale_fields.clear();
                            // Record flush stats for Prometheus
                            let flush_elapsed = flush_start.elapsed().as_nanos() as u64;
                            flush_pub_count.fetch_add(1, Ordering::Relaxed);
                            flush_dur_nanos.fetch_add(flush_elapsed, Ordering::Relaxed);
                            flush_last_dur_nanos.store(flush_elapsed, Ordering::Relaxed);
                            // Yield after publish — snapshot is live, let tokio
                            // deliver responses before we do ops-log disk I/O.
                            std::thread::yield_now();
                            // ── Ops-log append (after publish) ─────────────
                            // Persist mutations as ops-log entries AFTER the
                            // snapshot is published. This removes disk I/O from
                            // the critical path — readers already see the new
                            // snapshot. On crash between publish and persist,
                            // pg-sync replays lost ops idempotently on restart.
                            let t_opslog = Instant::now();
                            flush_opslog_ns.store(t_opslog.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
                                let fp_start = std::time::Instant::now();
                                // Drain any remaining mutations from the channel
                                // before publishing — they may not have been picked
                                // up by the regular prepare() at the top of the loop.
                                let t_flush = std::time::Instant::now();
                                let extra = coalescer.flush(
                                    &mut staging.slots,
                                    &mut staging.filters,
                                    &mut staging.sorts,
                                );
                                if extra > 0 {
                                    #[allow(unused_assignments)]
                                    { staging_dirty = true; }
                                }
                                let flush_elapsed = t_flush.elapsed();
                                let t_merge = std::time::Instant::now();
                                if extra > 0 {
                                    for (_name, field) in staging.filters.fields_mut() {
                                        field.merge_dirty();
                                    }
                                }
                                let merge_elapsed = t_merge.elapsed();
                                let t_clone = std::time::Instant::now();
                                inner.store(Arc::new(staging.clone()));
                                let clone_elapsed = t_clone.elapsed();
                                staging_dirty = false;
                                tracing::debug!(
                                    "ForcePublish: flush={:.1}ms merge={:.1}ms clone={:.1}ms total={:.1}ms",
                                    flush_elapsed.as_secs_f64() * 1000.0,
                                    merge_elapsed.as_secs_f64() * 1000.0,
                                    clone_elapsed.as_secs_f64() * 1000.0,
                                    fp_start.elapsed().as_secs_f64() * 1000.0,
                                );
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
                                skip_sorts, skip_filters, loading_mode, done,
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
                                // 1b. NOW clear loading_mode — after we've captured the
                                // snapshot but before the next loop iteration. This prevents
                                // the was_loading→!is_loading force-publish from overwriting
                                // the loader's data.
                                loading_mode.store(false, Ordering::Release);
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
                    // Incremental time bucket refresh: instead of scanning 107M alive slots,
                    // compute expired slots via narrow range query on the sort layers.
                    // Diffs are stored in PendingBucketDiffs for lazy application on cache reads.
                    // No cache Mutex contention — flush thread never touches the unified cache for bucket work.
                    if !is_loading {
                        if let Some(ref tb_arc) = flush_time_buckets {
                            let now_secs = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            // Brief lock: check which buckets need refresh and get their config
                            let refresh_info: Vec<(String, u64, u64, u64)> = {
                                let tb = tb_arc.lock();
                                let due = tb.refresh_due(now_secs);
                                if due.is_empty() {
                                    Vec::new()
                                } else {
                                    due.iter()
                                        .filter_map(|name| {
                                            tb.get_bucket(name).map(|b| (
                                                name.to_string(),
                                                b.duration_secs,
                                                b.refresh_interval_secs,
                                                b.last_cutoff(),
                                            ))
                                        })
                                        .collect()
                                }
                            }; // lock released
                            if !refresh_info.is_empty() {
                                let tb_lock = tb_arc.lock();
                                let sort_field_name = tb_lock.sort_field_name().to_string();
                                drop(tb_lock);
                                if let Some(sort_field) = staging.sorts.get_field(&sort_field_name) {
                                    let start = std::time::Instant::now();
                                    for (bucket_name, duration_secs, refresh_interval, old_cutoff) in &refresh_info {
                                        let new_cutoff = crate::bucket_diff_log::snap_cutoff(
                                            now_secs.saturating_sub(*duration_secs),
                                            *refresh_interval,
                                        );
                                        if new_cutoff <= *old_cutoff {
                                            // No new expired slots since last cutoff
                                            // Still mark as refreshed so needs_refresh returns false
                                            let mut tb = tb_arc.lock();
                                            if let Some(bucket) = tb.get_bucket_mut(bucket_name) {
                                                bucket.subtract_expired(&RoaringBitmap::new(), new_cutoff);
                                            }
                                            continue;
                                        }
                                        // Find expired slots: those in the bucket bitmap with
                                        // sort value in [old_cutoff, new_cutoff)
                                        let bucket_bm = {
                                            let tb = tb_arc.lock();
                                            tb.get_bucket(bucket_name)
                                                .map(|b| RoaringBitmap::clone(b.bitmap()))
                                                .unwrap_or_default()
                                        };
                                        let old_cutoff_u32 = *old_cutoff as u32;
                                        let new_cutoff_u32 = new_cutoff as u32;
                                        let mut expired = RoaringBitmap::new();
                                        for slot in bucket_bm.iter() {
                                            let val = sort_field.reconstruct_value(slot);
                                            if val >= old_cutoff_u32 && val < new_cutoff_u32 {
                                                expired.insert(slot);
                                            }
                                        }
                                        let expired_count = expired.len();
                                        // Brief lock: subtract expired from bucket bitmap
                                        {
                                            let mut tb = tb_arc.lock();
                                            if let Some(bucket) = tb.get_bucket_mut(bucket_name) {
                                                bucket.subtract_expired(&expired, new_cutoff);
                                            }
                                        }
                                        // Store diff for lazy cache application (no cache Mutex!)
                                        let diff = crate::bucket_diff_log::BucketDiff {
                                            cutoff_before: *old_cutoff,
                                            cutoff_after: new_cutoff,
                                            expired: Arc::new(expired),
                                        };
                                        // Append to on-disk log
                                        if let Some(ref log_path) = flush_diff_log_path {
                                            let log = crate::bucket_diff_log::BucketDiffLog::new(
                                                log_path.clone(), 100, 0.3,
                                            );
                                            if let Err(e) = log.append(&diff) {
                                                eprintln!("Warning: failed to append bucket diff to log: {e}");
                                            }
                                            // Periodic compaction
                                            if let Err(e) = log.compact_if_needed() {
                                                eprintln!("Warning: bucket diff log compaction failed: {e}");
                                            }
                                        }
                                        // Update in-memory pending diffs (ArcSwap store)
                                        {
                                            let old_pending = flush_pending_diffs.load();
                                            let mut new_pending = crate::bucket_diff_log::PendingBucketDiffs::from_diffs(
                                                old_pending.diffs().to_vec(),
                                                100,
                                            );
                                            new_pending.push(diff);
                                            flush_pending_diffs.store(Arc::new(new_pending));
                                        }
                                        eprintln!("Time bucket '{}' incremental refresh: expired={} cutoff {}→{} in {:?}",
                                            bucket_name, expired_count, old_cutoff, new_cutoff, start.elapsed());
                                    }
                                    // Mark dirty so merge thread persists time buckets
                                    flush_dirty_flag.store(true, Ordering::Release);
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
                        // DataSilo mmap reads are fast enough — no cache needed
                        if let Err(e) = docstore.lock().put_batch(&doc_batch) {
                            eprintln!("WARNING: docstore batch write failed (skipping {} docs): {e}", doc_batch.len());
                        }
                    }
                    if bitmap_count > 0 || doc_count > 0 {
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
            let merge_interval_ms = config.merge_interval_ms;
            let merge_dirty_flag = Arc::clone(&dirty_flag);
            let merge_docstore = Arc::clone(&docstore);
            let merge_cache_silo = cache_silo_arc.clone();
            let merge_bitmap_silo = bitmap_silo_arc.clone();

            thread::Builder::new()
                .name("bitdex-merge".to_string())
                .spawn(move || {
                let sleep_duration = Duration::from_millis(merge_interval_ms);
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(sleep_duration);

                    // Compact DataSilo when dirty (apply pending doc ops to data file)
                    let needs_write = merge_dirty_flag.swap(false, Ordering::AcqRel);
                    if needs_write {
                        if let Err(e) = merge_docstore.lock().compact() {
                            eprintln!("merge: DataSilo compaction failed: {e}");
                        }
                    }

                    // Compact CacheSilo when it has accumulated enough dead space.
                    if let Some(ref cs_arc) = merge_cache_silo {
                        let needs_compact = cs_arc.read().needs_compaction();
                        if needs_compact {
                            if let Err(e) = cs_arc.write().compact() {
                                eprintln!("merge: CacheSilo compaction failed: {e}");
                            }
                        }
                    }

                    // Compact BitmapSilo when it has accumulated enough dead space.
                    if let Some(ref bs_arc) = merge_bitmap_silo {
                        let needs_compact = bs_arc.read().needs_compaction();
                        if needs_compact {
                            if let Err(e) = bs_arc.write().compact() {
                                eprintln!("merge: BitmapSilo compaction failed: {e}");
                            }
                        }
                    }
                }
            }).expect("failed to spawn merge thread")
        };
        // Prefetch worker: background cache expansion when cursor nears boundary.
        // Disabled when threshold is 0.0 or 1.0.
        let prefetch_threshold = config.cache.prefetch_threshold;
        let (prefetch_tx, prefetch_handle) = if prefetch_threshold > 0.0 && prefetch_threshold < 1.0 {
            let (tx, prefetch_rx): (Sender<UnifiedKey>, Receiver<UnifiedKey>) =
                crossbeam_channel::bounded(16);
            let pf_inner = Arc::clone(&inner);
            let pf_cache = Arc::clone(&unified_cache);
            let pf_config = Arc::clone(&config);
            let handle = thread::Builder::new()
                .name("bitdex-prefetch".to_string())
                .spawn(move || {
                    while let Ok(ukey) = prefetch_rx.recv() {
                        // Read entry state under lock, then drop lock before doing work
                        let work = {
                            let uc = pf_cache.lock();
                            if let Some(entry) = uc.get(&ukey) {
                                if entry.is_prefetching() || !entry.has_more()
                                    || entry.capacity() >= entry.max_capacity()
                                {
                                    None
                                } else {
                                    let cap = entry.capacity();
                                    let max_cap = entry.max_capacity();
                                    let min_val = entry.min_tracked_value();
                                    entry.set_prefetching(true);
                                    Some((cap, max_cap, min_val))
                                }
                            } else {
                                None
                            }
                        };
                        let Some((capacity, max_capacity, min_tracked_value)) = work else {
                            continue;
                        };
                        tracing::debug!(
                            "Prefetch: expanding {} {:?} (cap={}/{})",
                            ukey.sort_field, ukey.direction, capacity, max_capacity,
                        );
                        // Load snapshot and build executor
                        let snap = pf_inner.load();
                        let executor = QueryExecutor::new(
                            &snap.slots,
                            &snap.filters,
                            &snap.sorts,
                            pf_config.max_page_size,
                        );
                        // Convert canonical clauses back to FilterClauses
                        let filter_clauses: Vec<FilterClause> = ukey.filter_clauses.iter()
                            .filter_map(|cc| crate::cache::CanonicalClause::to_filter_clause(cc))
                            .collect();
                        // Resolve filters
                        let _now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let planner_ctx = crate::planner::PlannerContext {
                            string_maps: executor.string_maps(),
                            dictionaries: executor.dictionaries(),
                        };
                        let plan = crate::planner::plan_query_with_context(
                            &filter_clauses,
                            executor.filter_index(),
                            executor.slot_allocator(),
                            Some(&planner_ctx),
                        );
                        let filter_bitmap = match executor.compute_filters(&plan.ordered_clauses) {
                            Ok(bm) => Arc::new(bm),
                            Err(e) => {
                                tracing::debug!("Prefetch: filter resolution failed: {e}");
                                let uc = pf_cache.lock();
                                if let Some(entry) = uc.get(&ukey) {
                                    entry.set_prefetching(false);
                                }
                                continue;
                            }
                        };
                        // Expand: traverse from min_tracked_value cursor
                        let expand_limit = max_capacity.saturating_sub(capacity);
                        let sort_clause = crate::query::SortClause {
                            field: ukey.sort_field.clone(),
                            direction: ukey.direction,
                        };
                        let cursor = crate::query::CursorPosition {
                            sort_value: min_tracked_value as u64,
                            slot_id: 0, // Will start after min_tracked_value
                        };
                        let expand_result = executor.execute_from_bitmap_unclamped(
                            &filter_bitmap,
                            Some(&sort_clause),
                            expand_limit,
                            Some(&cursor),
                            plan.use_simple_sort,
                        );
                        match expand_result {
                            Ok(result) if !result.ids.is_empty() => {
                                let sorted_slots: Vec<u32> = result.ids.iter()
                                    .map(|&id| id as u32).collect();
                                let sort_field = snap.sorts.get_field(&sort_clause.field);
                                let value_fn = |slot: u32| -> u32 {
                                    sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
                                };
                                let mut uc = pf_cache.lock();
                                if let Some(entry) = uc.get_mut(&ukey) {
                                    entry.expand(&sorted_slots, value_fn);
                                    entry.set_prefetching(false);
                                    uc.record_extension();
                                    tracing::debug!(
                                        "Prefetch: expanded {} {:?} by {} slots",
                                        ukey.sort_field, ukey.direction, sorted_slots.len(),
                                    );
                                }
                            }
                            Ok(_) => {
                                // No results — nothing to expand
                                let uc = pf_cache.lock();
                                if let Some(entry) = uc.get(&ukey) {
                                    entry.set_prefetching(false);
                                }
                            }
                            Err(e) => {
                                tracing::debug!("Prefetch: sort traversal failed: {e}");
                                let uc = pf_cache.lock();
                                if let Some(entry) = uc.get(&ukey) {
                                    entry.set_prefetching(false);
                                }
                            }
                        }
                    }
                })
                .expect("Failed to spawn bitdex-prefetch thread");
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };
        // DataSilo mmap reads require no separate eviction thread
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
            loading_mode,
            dirty_since_snapshot: Arc::clone(&dirty_flag),
            time_buckets,
            pending_bucket_diffs,
            cmd_tx,
            string_maps: None,
            case_sensitive_fields: None,
            dictionaries: Arc::new(HashMap::new()),
            unified_cache,
            cache_silo: cache_silo_arc,
            flush_publish_count,
            flush_duration_nanos,
            flush_last_duration_nanos,
            flush_apply_nanos,
            flush_cache_nanos,
            flush_publish_nanos,
            flush_timebucket_nanos,
            flush_compact_nanos,
            flush_opslog_nanos,
            cursors,
            #[cfg(feature = "server")]
            metrics_bridge,
            bitmap_silo: bitmap_silo_arc.clone(),
            compaction_skipped,
            prefetch_tx,
            prefetch_handle,
            #[cfg(feature = "pg-sync")]
            wal_writer: None,
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
        // [2.7] WAL path: decompose to ops and write to WAL. The WAL reader
        // thread handles bitmap mutations + docstore writes asynchronously.
        #[cfg(feature = "pg-sync")]
        if let Some(ref wal) = self.wal_writer {
            return self.put_via_wal(id, doc, wal);
        }
        // Legacy direct path (when WAL writer is not configured)
        self.in_flight.mark_in_flight(id);
        let result = self.put_inner(id, doc);
        self.in_flight.clear_in_flight(id);
        result
    }
    /// PUT via WAL: decompose document into field-level ops and append to WAL.
    /// Returns after fsync — mutations become visible when WAL reader processes them.
    #[cfg(feature = "pg-sync")]
    fn put_via_wal(&self, id: u32, doc: &Document, wal: &crate::ops_wal::WalWriter) -> Result<()> {
        let is_alive = self.is_slot_alive(id);
        // Read old doc for upsert diffing
        let old_doc = if is_alive {
            self.docstore.lock().get(id)?
        } else {
            None
        };
        let ops = crate::ops_processor::document_to_ops(doc, old_doc.as_ref(), &self.config, false);
        let creates_slot = !is_alive;
        let entry = crate::pg_sync::ops::EntityOps {
            entity_id: id as i64,
            ops,
            creates_slot,
        };
        wal.append_batch(&[entry]).map_err(|e| {
            crate::error::BitdexError::Storage(format!("WAL write failed: {e}"))
        })?;
        Ok(())
    }
    /// PATCH via WAL: decompose partial document into field-level ops and append to WAL.
    #[cfg(feature = "pg-sync")]
    fn patch_document_via_wal(&self, id: u32, doc: &Document, wal: &crate::ops_wal::WalWriter) -> Result<()> {
        let is_alive = self.is_slot_alive(id);
        if !is_alive {
            // New slot — full PUT via WAL
            return self.put_via_wal(id, doc, wal);
        }
        // Read old doc for diffing
        let old_doc = self.docstore.lock().get(id)?;
        // For PATCH, only emit ops for fields present in the new doc
        let ops = crate::ops_processor::document_to_ops(doc, old_doc.as_ref(), &self.config, true);
        if ops.is_empty() {
            return Ok(());
        }
        let entry = crate::pg_sync::ops::EntityOps {
            entity_id: id as i64,
            ops,
            creates_slot: false,
        };
        wal.append_batch(&[entry]).map_err(|e| {
            crate::error::BitdexError::Storage(format!("WAL write failed: {e}"))
        })?;
        Ok(())
    }
    /// Inner PUT logic shared by put() and patch_document() (for new slots).
    /// Caller must handle in_flight marking.
    fn put_inner(&self, id: u32, doc: &Document) -> Result<()> {
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
    }
    /// PATCH(id, partial_fields) -- merge only provided fields into existing doc.
    /// Uses diff_document_partial which skips fields not present in the new doc.
    /// Also merges provided fields into the stored document.
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
    /// PATCH with a Document — partial update using diff_document_partial.
    /// Only fields present in the doc are diffed and updated. Missing fields
    /// are left untouched in both bitmaps and docstore.
    pub fn patch_document(&self, id: u32, doc: &Document) -> Result<()> {
        // [2.7] WAL path: decompose to ops and write to WAL.
        #[cfg(feature = "pg-sync")]
        if let Some(ref wal) = self.wal_writer {
            return self.patch_document_via_wal(id, doc, wal);
        }
        self.in_flight.mark_in_flight(id);
        let result = (|| -> Result<()> {
            let is_alive = {
                let snap = self.snapshot();
                snap.slots.is_alive(id)
            };
            if !is_alive {
                // Slot doesn't exist yet — fall through to full PUT semantics.
                // This handles new records (e.g., images created after the bulk load).
                return self.put_inner(id, doc);
            }
            // Read old doc for diffing
            let old_doc = self.docstore.lock().get(id)?;
            // Compute partial diff — only fields present in doc are processed
            let ops = crate::mutation::diff_document_partial(
                id, old_doc.as_ref(), doc, &self.config, &self.field_registry,
            );
            // Send bitmap mutations
            if !ops.is_empty() {
                self.sender.send_batch(ops).map_err(|_| {
                    crate::error::BitdexError::CapacityExceeded(
                        "coalescer channel disconnected".to_string(),
                    )
                })?;
            }
            // Merge provided fields into stored doc (preserve existing fields)
            let mut merged_fields = old_doc
                .map(|d| d.fields)
                .unwrap_or_default();
            for (k, v) in &doc.fields {
                merged_fields.insert(k.clone(), v.clone());
            }
            let stored = StoredDoc {
                fields: merged_fields,
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
    /// SYNC filter values for a slot on a filter_only multi-value field.
    ///
    /// Replaces all filter bitmap memberships for the given slot on the named field.
    /// Scans loaded bitmaps to find old values, diffs against new values, and sends
    /// targeted FilterInsert/FilterRemove ops. No docstore involvement.
    ///
    /// Used by the outbox poller for filter_only fields like collectionIds where
    /// the membership data comes from a separate table (CollectionItem), not the
    /// image document.
    pub fn sync_filter_values(&self, slot: u32, field_name: &str, new_values: &[u64]) -> Result<()> {
        self.in_flight.mark_in_flight(slot);
        let result = (|| -> Result<()> {
            // Skip if slot is not alive — the image hasn't been inserted yet.
            // The next outbox event for this image will trigger a PATCH (which
            // now falls through to PUT), and that will handle the full insert.
            // Setting filter bitmaps before the slot is alive would be pointless
            // since queries require alive status.
            {
                let snap = self.snapshot();
                if !snap.slots.is_alive(slot) {
                    return Ok(());
                }
            }
            // Find old values by scanning loaded bitmaps for this field
            let old_values: Vec<u64> = {
                let snap = self.snapshot();
                match snap.filters.get_field(field_name) {
                    Some(field) => field
                        .bitmap_keys()
                        .filter(|&&v| {
                            field.get(v).map_or(false, |bm| bm.contains(slot))
                        })
                        .copied()
                        .collect(),
                    None => Vec::new(),
                }
            };
            let new_set: std::collections::HashSet<u64> = new_values.iter().copied().collect();
            let old_set: std::collections::HashSet<u64> = old_values.iter().copied().collect();
            let arc_name = self.field_registry.get(field_name);
            let mut ops = Vec::new();
            // Remove slot from bitmaps for values no longer present
            for &val in old_set.difference(&new_set) {
                ops.push(MutationOp::FilterRemove {
                    field: arc_name.clone(),
                    value: val,
                    slots: vec![slot],
                });
            }
            // Insert slot into bitmaps for newly added values
            for &val in new_set.difference(&old_set) {
                ops.push(MutationOp::FilterInsert {
                    field: arc_name.clone(),
                    value: val,
                    slots: vec![slot],
                });
            }
            if !ops.is_empty() {
                self.sender.send_batch(ops).map_err(|_| {
                    crate::error::BitdexError::CapacityExceeded(
                        "coalescer channel disconnected".to_string(),
                    )
                })?;
            }
            Ok(())
        })();
        self.in_flight.clear_in_flight(slot);
        result
    }
    /// Execute a query from individual filter/sort/limit components.
    pub fn query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<QueryResult> {
        let snap = self.snapshot(); // lock-free
        let silo_guard = self.bitmap_silo.as_ref().map(|s| s.read());
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
            if let Some(ref guard) = silo_guard {
                base = base.with_bitmap_silo(guard);
            }
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
    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        let _query_start = std::time::Instant::now();
        let snap = self.snapshot(); // lock-free
        let silo_guard = self.bitmap_silo.as_ref().map(|s| s.read());
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
            if let Some(ref guard) = silo_guard {
                base = base.with_bitmap_silo(guard);
            }
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
        // ── Snap range filters to bucket bitmaps BEFORE cache key ──
        // This ensures cache keys use stable bucket names ("7d") instead of
        // moving timestamps, so all queries within the same bucket window share
        // a single cache entry.
        let snapped_filters;
        let effective_filters = if let Some(ref tb) = tb_guard {
            let mut managers = std::collections::HashMap::new();
            managers.insert(tb.field_name().to_string(), &**tb);
            let ctx = crate::query::BucketSnapContext {
                managers: &managers,
                now_secs: now_unix,
                tolerance_pct: 0.10,
                always_snap: true,
            };
            snapped_filters = crate::query::snap_range_clauses(&query.filters, &ctx);
            &snapped_filters[..]
        } else {
            &query.filters[..]
        };
        // ── skip_cache bypass: go straight to slow path without cache ──
        if query.skip_cache {
            tracing::info!("skip_cache=true: bypassing unified cache");
            return self.execute_query_slow_path(
                query, effective_filters, &snap, &executor, tb_guard.as_deref(), now_unix, None,
            );
        }
        // ── Fast path: unified cache hit without expansion ──
        // Try cache lookup BEFORE computing filters. If we hit, we can skip
        // the expensive filter bitmap computation entirely (~2ms saved at 105M).
        if let Some(sort_clause) = query.sort.as_ref() {
            if let Some(clauses) = cache::canonicalize(effective_filters) {
                let ukey = UnifiedKey {
                    filter_clauses: clauses,
                    sort_field: sort_clause.field.clone(),
                    direction: sort_clause.direction,
                };
                let cache_data = {
                    let mut uc = self.unified_cache.lock();
                    let pending = self.pending_bucket_diffs.load();
                    uc.lookup(&ukey).map(|entry| {
                        // Apply pending bucket diffs lazily before reading
                        if pending.current_cutoff() > 0
                            && entry.uses_bucket()
                            && entry.bucket_cutoff() < pending.current_cutoff()
                        {
                            if entry.bucket_cutoff() >= pending.oldest_cutoff() {
                                entry.apply_bucket_diff(pending.merged_expired(), pending.current_cutoff());
                            } else {
                                entry.mark_for_rebuild();
                            }
                        }
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
                        // Two cases: (a) short page with cursor (original), and
                        // (b) cache exhausted — returned results but no cursor.
                        if has_more && (
                            (result.cursor.is_none() && !result.ids.is_empty()) ||
                            (result.ids.len() < fetch_limit && query.cursor.is_some())
                        ) {
                            let (filter_arc, use_simple_sort) = self.resolve_filters(
                                &executor, effective_filters, tb_guard.as_deref(), now_unix,
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
                                    uc.record_extension();
                                }
                            }
                            self.unified_cache.lock().record_wall_hit();
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
                        // Prefetch proximity detection: if cursor is near the cache
                        // boundary, fire a background expansion request.
                        if has_more && capacity < self.unified_cache.lock().config().max_capacity {
                            if let Some(ref tx) = self.prefetch_tx {
                                if let Some(ref keys) = cached_sorted_keys {
                                    if let Some(ref cursor) = result.cursor {
                                        let cursor_key = (cursor.sort_value << 32) | (cursor.slot_id as u64);
                                        let sort_dir = query.sort.as_ref().map(|s| s.direction).unwrap_or(SortDirection::Desc);
                                        let pos = match sort_dir {
                                            SortDirection::Desc => keys.partition_point(|&k| k >= cursor_key),
                                            SortDirection::Asc => keys.partition_point(|&k| k <= cursor_key),
                                        };
                                        let threshold = self.unified_cache.lock().config().prefetch_threshold;
                                        if keys.len() > 0 && pos as f64 / keys.len() as f64 >= threshold {
                                            let _ = tx.try_send(ukey.clone());
                                            self.unified_cache.lock().record_prefetch();
                                        }
                                    }
                                }
                                // Skip prefetch for radix path — expanded entries are already at max_capacity
                            }
                        }
                        self.post_validate(&mut result, &query.filters, &executor)?;
                        return Ok(result);
                    }
                    // Expansion needed — fall through to slow path with pre-fetched cache data.
                    self.unified_cache.lock().record_wall_hit();
                    return self.execute_query_slow_path(
                        query, effective_filters, &snap, &executor, tb_guard.as_deref(), now_unix,
                        Some((ukey, unified_bm, has_more, min_val, capacity, cached_total)),
                    );
                }
            }
        }
        // ── Slow path: cache miss or unsorted query ──
        self.execute_query_slow_path(
            query, effective_filters, &snap, &executor, tb_guard.as_deref(), now_unix, None,
        )
    }
    /// Execute a query and produce a trace alongside the result.
    /// The trace captures overall timing, per-clause filter metrics (on cache miss),
    /// sort timing, and cache hit/miss status.
    ///
    /// Unlike the previous implementation which ran filters twice (once for tracing,
    /// once for the real result), this threads the trace collector through the real
    /// query path so timings reflect actual execution.
    pub fn execute_query_traced(&self, query: &BitdexQuery, index_name: &str) -> Result<(QueryResult, QueryTrace)> {
        let mut collector = QueryTraceCollector::new();
        let result = self.execute_query_with_collector(query, &mut collector)?;
        if let Some(sort_clause) = query.sort.as_ref() {
            collector.record_sort(SortTrace {
                field: sort_clause.field.clone(),
                dir: format!("{:?}", sort_clause.direction),
                input: result.total_matched,
                output: result.ids.len() as u64,
                time_us: collector.sort_us,
            });
        }
        let trace = collector.finalize(index_name, result.total_matched as u64);
        Ok((result, trace))
    }
    /// Execute a query while recording trace metrics into the collector.
    /// Mirrors `execute_query` but threads the collector through the real
    /// cache-aware path so timings are accurate.
    fn execute_query_with_collector(
        &self,
        query: &BitdexQuery,
        collector: &mut QueryTraceCollector,
    ) -> Result<QueryResult> {
        let _query_start = std::time::Instant::now();
        collector.lazy_load_us = 0;
        let snap = self.snapshot();
        let silo_guard = self.bitmap_silo.as_ref().map(|s| s.read());
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
            if let Some(ref guard) = silo_guard {
                base = base.with_bitmap_silo(guard);
            }
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
        // Snap range filters to bucket bitmaps BEFORE cache key
        let snapped_filters;
        let effective_filters = if let Some(ref tb) = tb_guard {
            let mut managers = std::collections::HashMap::new();
            managers.insert(tb.field_name().to_string(), &**tb);
            let ctx = crate::query::BucketSnapContext {
                managers: &managers,
                now_secs: now_unix,
                tolerance_pct: 0.10,
                always_snap: true,
            };
            snapped_filters = crate::query::snap_range_clauses(&query.filters, &ctx);
            &snapped_filters[..]
        } else {
            &query.filters[..]
        };
        // ── skip_cache bypass: go straight to slow path without cache ──
        if query.skip_cache {
            tracing::info!("skip_cache=true: bypassing unified cache (traced)");
            return self.execute_query_slow_path_traced(
                query, effective_filters, &snap, &executor, tb_guard.as_deref(), now_unix, None,
                collector,
            );
        }
        // ── Fast path: unified cache hit without expansion ──
        if let Some(sort_clause) = query.sort.as_ref() {
            if let Some(clauses) = cache::canonicalize(effective_filters) {
                let ukey = UnifiedKey {
                    filter_clauses: clauses,
                    sort_field: sort_clause.field.clone(),
                    direction: sort_clause.direction,
                };
                let cache_data = {
                    let mut uc = self.unified_cache.lock();
                    let pending = self.pending_bucket_diffs.load();
                    uc.lookup(&ukey).map(|entry| {
                        // Apply pending bucket diffs lazily before reading
                        if pending.current_cutoff() > 0
                            && entry.uses_bucket()
                            && entry.bucket_cutoff() < pending.current_cutoff()
                        {
                            if entry.bucket_cutoff() >= pending.oldest_cutoff() {
                                entry.apply_bucket_diff(pending.merged_expired(), pending.current_cutoff());
                            } else {
                                entry.mark_for_rebuild();
                            }
                        }
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
                        // CACHE HIT: record in trace — no filter computation happened
                        collector.cache_hit = true;
                        collector.filter_us = 0;
                        let offset = if query.cursor.is_none() {
                            query.offset.unwrap_or(0)
                        } else {
                            0
                        };
                        let fetch_limit = query.limit.saturating_add(offset);
                        let sort_start = Instant::now();
                        let mut result = if let Some(ref keys) = cached_sorted_keys {
                            executor.execute_from_sorted_keys(
                                keys, &sort_clause.field, sort_clause.direction,
                                fetch_limit, query.cursor.as_ref(), cached_total,
                            )?
                        } else if let Some(ref radix) = cached_radix {
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
                        // Two cases: (a) short page with cursor (original), and
                        // (b) cache exhausted — returned results but no cursor.
                        if has_more && (
                            (result.cursor.is_none() && !result.ids.is_empty()) ||
                            (result.ids.len() < fetch_limit && query.cursor.is_some())
                        ) {
                            // Expansion needs filters — trace them
                            let filter_start = Instant::now();
                            let (filter_arc, use_simple_sort) = self.resolve_filters_traced(
                                &executor, effective_filters, tb_guard.as_deref(), now_unix, collector,
                            )?;
                            collector.filter_us = filter_start.elapsed().as_micros() as u64;
                            collector.cache_hit = false; // expansion needed filters
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
                                    uc.record_extension();
                                }
                            }
                            self.unified_cache.lock().record_wall_hit();
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
                            collector.sort_us = sort_start.elapsed().as_micros() as u64;
                            self.post_validate(&mut result, &query.filters, &executor)?;
                            return Ok(result);
                        }
                        collector.sort_us = sort_start.elapsed().as_micros() as u64;
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
                        // Prefetch proximity detection (traced path)
                        if has_more && capacity < self.unified_cache.lock().config().max_capacity {
                            if let Some(ref tx) = self.prefetch_tx {
                                if let Some(ref keys) = cached_sorted_keys {
                                    if let Some(ref cursor) = result.cursor {
                                        let cursor_key = (cursor.sort_value << 32) | (cursor.slot_id as u64);
                                        let sort_dir = query.sort.as_ref().map(|s| s.direction).unwrap_or(SortDirection::Desc);
                                        let pos = match sort_dir {
                                            SortDirection::Desc => keys.partition_point(|&k| k >= cursor_key),
                                            SortDirection::Asc => keys.partition_point(|&k| k <= cursor_key),
                                        };
                                        let threshold = self.unified_cache.lock().config().prefetch_threshold;
                                        if keys.len() > 0 && pos as f64 / keys.len() as f64 >= threshold {
                                            let _ = tx.try_send(ukey.clone());
                                            self.unified_cache.lock().record_prefetch();
                                        }
                                    }
                                }
                            }
                        }
                        self.post_validate(&mut result, &query.filters, &executor)?;
                        return Ok(result);
                    }
                    // Expansion needed — fall through to slow path
                    self.unified_cache.lock().record_wall_hit();
                    return self.execute_query_slow_path_traced(
                        query, effective_filters, &snap, &executor, tb_guard.as_deref(), now_unix,
                        Some((ukey, unified_bm, has_more, min_val, capacity, cached_total)),
                        collector,
                    );
                }
            }
        }
        // ── Slow path: cache miss or unsorted query ──
        self.execute_query_slow_path_traced(
            query, effective_filters, &snap, &executor, tb_guard.as_deref(), now_unix, None,
            collector,
        )
    }
    /// Slow path for execute_query_with_collector: computes full filter bitmap
    /// with trace collection. Mirrors `execute_query_slow_path` but uses
    /// `resolve_filters_traced` for clause-level detail.
    fn execute_query_slow_path_traced(
        &self,
        query: &BitdexQuery,
        snapped_filters: &[FilterClause],
        snap: &Arc<InnerEngine>,
        executor: &QueryExecutor,
        time_buckets: Option<&TimeBucketManager>,
        now_unix: u64,
        cached: Option<(UnifiedKey, Arc<RoaringBitmap>, bool, u32, usize, u64)>,
        collector: &mut QueryTraceCollector,
    ) -> Result<QueryResult> {
        let _slow_start = std::time::Instant::now();
        let filter_start = Instant::now();
        let (filter_arc, use_simple_sort) =
            self.resolve_filters_traced(executor, snapped_filters, time_buckets, now_unix, collector)?;
        collector.filter_us = filter_start.elapsed().as_micros() as u64;
        let full_total_matched = filter_arc.len();
        // If we have pre-fetched cache data (expansion case), use it.
        // Otherwise, do a fresh cache lookup (miss case).
        // skip_cache=true forces (None, None) to bypass all cache operations.
        let (unified_key, unified_hit) = if query.skip_cache {
            (None, None)
        } else if let Some((ukey, bm, has_more, min_val, cap, _total)) = cached {
            (Some(ukey), Some((bm, has_more, min_val, cap)))
        } else if let Some(sort_clause) = query.sort.as_ref() {
            let mut uc = self.unified_cache.lock();
            let min_size = uc.config().min_filter_size as u64;
            if full_total_matched >= min_size {
                if let Some(clauses) = cache::canonicalize(snapped_filters) {
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
                            uc.record_extension();
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
        let sort_start = Instant::now();
        // ── Cache miss with sort: seed cache FIRST, serve from cache ──
        if unified_hit.is_none() && unified_key.is_some() && query.sort.is_some() {
            let ukey = unified_key.unwrap();
            let sort_clause = query.sort.as_ref().unwrap();
            if full_total_matched == 0 {
                let value_fn = |_slot: u32| -> u32 { 0 };
                self.unified_cache.lock().form_and_store(
                    ukey,
                    &[],
                    false,
                    full_total_matched,
                    value_fn,
                );
                let mut result = QueryResult {
                    ids: vec![],
                    total_matched: full_total_matched,
                    cursor: None,
                };
                collector.sort_us = sort_start.elapsed().as_micros() as u64;
                self.post_validate(&mut result, &query.filters, executor)?;
                return Ok(result);
            }
            let initial_cap = self.unified_cache.lock().config().initial_capacity;
            let seed_result = executor.execute_from_bitmap_unclamped(
                &filter_arc,
                query.sort.as_ref(),
                initial_cap,
                None,
                use_simple_sort,
            )?;
            let sort_field = snap.sorts.get_field(&sort_clause.field);
            let sorted_slots: Vec<u32> = seed_result.ids.iter().map(|&id| id as u32).collect();
            let has_more = full_total_matched > sorted_slots.len() as u64;
            let value_fn = |slot: u32| -> u32 {
                sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
            };
            self.unified_cache.lock().form_and_store(
                ukey.clone(),
                &sorted_slots,
                has_more,
                full_total_matched,
                value_fn,
            );
            let cached_keys = {
                let mut uc = self.unified_cache.lock();
                uc.lookup(&ukey).and_then(|entry| entry.sorted_keys().map(Arc::clone))
            };
            let mut result = if let Some(ref keys) = cached_keys {
                executor.execute_from_sorted_keys(
                    keys, &sort_clause.field, sort_clause.direction,
                    fetch_limit, query.cursor.as_ref(), full_total_matched,
                )?
            } else {
                let cached_bm = {
                    let mut uc = self.unified_cache.lock();
                    uc.lookup(&ukey).map(|entry| Arc::clone(entry.bitmap()))
                };
                if let Some(ref bm) = cached_bm {
                    let use_simple = bm.len() < 10_000;
                    executor.execute_from_bitmap(
                        bm, query.sort.as_ref(), fetch_limit,
                        query.cursor.as_ref(), use_simple,
                    )?
                } else {
                    executor.execute_from_bitmap(
                        &filter_arc, query.sort.as_ref(), fetch_limit,
                        query.cursor.as_ref(), use_simple_sort,
                    )?
                }
            };
            result.total_matched = full_total_matched;
            // Apply offset
            if offset > 0 && !result.ids.is_empty() {
                if offset >= result.ids.len() {
                    result.ids.clear();
                    result.cursor = None;
                } else {
                    result.ids = result.ids.split_off(offset);
                    if let Some(&last_id) = result.ids.last() {
                        let slot = last_id as u32;
                        if let Some(sort_field_ref) = snap.sorts.get_field(&sort_clause.field) {
                            result.cursor = Some(crate::query::CursorPosition {
                                sort_value: sort_field_ref.reconstruct_value(slot) as u64,
                                slot_id: slot,
                            });
                        }
                    }
                }
            }
            collector.sort_us = sort_start.elapsed().as_micros() as u64;
            self.post_validate(&mut result, &query.filters, executor)?;
            return Ok(result);
        }
        // ── Cache hit or unsorted query path ──
        let bound_was_applied = effective_bitmap.len() < filter_arc.len();
        let mut result = executor.execute_from_bitmap(
            &effective_bitmap,
            query.sort.as_ref(),
            fetch_limit,
            query.cursor.as_ref(),
            use_simple,
        )?;
        // Bound exhaustion: expand if needed
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
                            uc.record_extension();
                        }
                    }
                    true
                } else { false }
            } else { false };
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
        collector.sort_us = sort_start.elapsed().as_micros() as u64;
        self.post_validate(&mut result, &query.filters, executor)?;
        Ok(result)
    }
    /// Slow path for execute_query: computes full filter bitmap.
    /// Used for cache misses, expansions, and unsorted queries.
    fn execute_query_slow_path(
        &self,
        query: &BitdexQuery,
        snapped_filters: &[FilterClause],
        snap: &Arc<InnerEngine>,
        executor: &QueryExecutor,
        time_buckets: Option<&TimeBucketManager>,
        now_unix: u64,
        // Pre-fetched cache data from fast path that detected expansion needed
        cached: Option<(UnifiedKey, Arc<RoaringBitmap>, bool, u32, usize, u64)>,
    ) -> Result<QueryResult> {
        let slow_start = std::time::Instant::now();
        let t0 = std::time::Instant::now();
        let (filter_arc, use_simple_sort) =
            self.resolve_filters(executor, snapped_filters, time_buckets, now_unix)?;
        let filter_elapsed = t0.elapsed();
        let full_total_matched = filter_arc.len();
        tracing::debug!(
            "  slow_path: resolve_filters={:.1}ms, matched={}, use_simple={}",
            filter_elapsed.as_secs_f64() * 1000.0, full_total_matched, use_simple_sort
        );
        // If we have pre-fetched cache data (expansion case), use it.
        // Otherwise, do a fresh cache lookup (miss case).
        // skip_cache=true forces (None, None) to bypass all cache operations.
        let (unified_key, unified_hit) = if query.skip_cache {
            (None, None)
        } else if let Some((ukey, bm, has_more, min_val, cap, _total)) = cached {
            (Some(ukey), Some((bm, has_more, min_val, cap)))
        } else if let Some(sort_clause) = query.sort.as_ref() {
            let mut uc = self.unified_cache.lock();
            let min_size = uc.config().min_filter_size as u64;
            if full_total_matched >= min_size {
                if let Some(clauses) = cache::canonicalize(snapped_filters) {
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
                            uc.record_extension();
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
        // ── Cache miss with sort: seed cache FIRST, serve from cache (one traversal) ──
        // The seed traversal (4K results) is a superset of the user's request (e.g. 50),
        // so we do one traversal instead of two.
        if unified_hit.is_none() && unified_key.is_some() && query.sort.is_some() {
            let ukey = unified_key.unwrap();
            let sort_clause = query.sort.as_ref().unwrap();
            if full_total_matched == 0 {
                // Zero-result cache: empty bitmap, no sort traversal needed.
                let value_fn = |_slot: u32| -> u32 { 0 };
                self.unified_cache.lock().form_and_store(
                    ukey,
                    &[],
                    false,
                    full_total_matched,
                    value_fn,
                );
                let result = QueryResult {
                    ids: vec![],
                    total_matched: full_total_matched,
                    cursor: None,
                };
                // post_validate not needed for empty results, but call for consistency
                let mut result = result;
                self.post_validate(&mut result, &query.filters, executor)?;
                return Ok(result);
            }
            // Seed the cache with initial_capacity (4K) results — single sort traversal.
            let initial_cap = self.unified_cache.lock().config().initial_capacity;
            let t0 = std::time::Instant::now();
            let seed_result = executor.execute_from_bitmap_unclamped(
                &filter_arc,
                query.sort.as_ref(),
                initial_cap,
                None,
                use_simple_sort,
            )?;
            let sort_elapsed = t0.elapsed();
            tracing::debug!(
                "  slow_path: sort_seed={:.1}ms ({}→{} slots, simple={})",
                sort_elapsed.as_secs_f64() * 1000.0, full_total_matched, seed_result.ids.len(), use_simple_sort
            );
            let sort_field = snap.sorts.get_field(&sort_clause.field);
            let sorted_slots: Vec<u32> = seed_result.ids.iter().map(|&id| id as u32).collect();
            let has_more = full_total_matched > sorted_slots.len() as u64;
            let value_fn = |slot: u32| -> u32 {
                sort_field.map(|f| f.reconstruct_value(slot)).unwrap_or(0)
            };
            let t0 = std::time::Instant::now();
            self.unified_cache.lock().form_and_store(
                ukey.clone(),
                &sorted_slots,
                has_more,
                full_total_matched,
                value_fn,
            );
            let cache_elapsed = t0.elapsed();
            tracing::debug!(
                "  slow_path: cache_form={:.1}ms, total_slow={:.1}ms",
                cache_elapsed.as_secs_f64() * 1000.0,
                slow_start.elapsed().as_secs_f64() * 1000.0
            );
            // Serve the user's results from the freshly seeded cache.
            let cached_keys = {
                let mut uc = self.unified_cache.lock();
                uc.lookup(&ukey).and_then(|entry| entry.sorted_keys().map(Arc::clone))
            };
            let mut result = if let Some(ref keys) = cached_keys {
                executor.execute_from_sorted_keys(
                    keys, &sort_clause.field, sort_clause.direction,
                    fetch_limit, query.cursor.as_ref(), full_total_matched,
                )?
            } else {
                // sorted_keys not available (shouldn't happen for fresh seed), fall back to bitmap
                let cached_bm = {
                    let mut uc = self.unified_cache.lock();
                    uc.lookup(&ukey).map(|entry| Arc::clone(entry.bitmap()))
                };
                if let Some(ref bm) = cached_bm {
                    let use_simple = bm.len() < 10_000;
                    executor.execute_from_bitmap(
                        bm, query.sort.as_ref(), fetch_limit,
                        query.cursor.as_ref(), use_simple,
                    )?
                } else {
                    // Cache entry vanished (eviction race), fall back to filter bitmap
                    executor.execute_from_bitmap(
                        &filter_arc, query.sort.as_ref(), fetch_limit,
                        query.cursor.as_ref(), use_simple_sort,
                    )?
                }
            };
            result.total_matched = full_total_matched;
            // Apply offset
            if offset > 0 && !result.ids.is_empty() {
                if offset >= result.ids.len() {
                    result.ids.clear();
                    result.cursor = None;
                } else {
                    result.ids = result.ids.split_off(offset);
                    if let Some(&last_id) = result.ids.last() {
                        let slot = last_id as u32;
                        if let Some(sort_field_ref) = snap.sorts.get_field(&sort_clause.field) {
                            result.cursor = Some(crate::query::CursorPosition {
                                sort_value: sort_field_ref.reconstruct_value(slot) as u64,
                                slot_id: slot,
                            });
                        }
                    }
                }
            }
            self.post_validate(&mut result, &query.filters, executor)?;
            return Ok(result);
        }
        // ── Cache hit or unsorted query path ──
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
                            uc.record_extension();
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
        self.post_validate(&mut result, &query.filters, executor)?;
        Ok(result)
    }
    /// Like `resolve_filters`, but records per-clause metrics into a trace collector.
    fn resolve_filters_traced(
        &self,
        executor: &QueryExecutor,
        filters: &[FilterClause],
        time_buckets: Option<&TimeBucketManager>,
        now_unix: u64,
        collector: &mut QueryTraceCollector,
    ) -> Result<(Arc<roaring::RoaringBitmap>, bool)> {
        let snapped;
        let effective_filters = if let Some(tb) = time_buckets {
            let mut managers = std::collections::HashMap::new();
            managers.insert(tb.field_name().to_string(), tb);
            let ctx = crate::query::BucketSnapContext {
                managers: &managers,
                now_secs: now_unix,
                tolerance_pct: 0.10,
                always_snap: true,
            };
            snapped = crate::query::snap_range_clauses(filters, &ctx);
            &snapped[..]
        } else {
            filters
        };
        let planner_ctx = planner::PlannerContext {
            string_maps: executor.string_maps(),
            dictionaries: executor.dictionaries(),
        };
        let plan = planner::plan_query_with_context(effective_filters, executor.filter_index(), executor.slot_allocator(), Some(&planner_ctx));
        let filter_bitmap = Arc::new(executor.compute_filters_traced(&plan.ordered_clauses, Some(collector))?);
        Ok((filter_bitmap, plan.use_simple_sort))
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
                always_snap: true,
            };
            snapped = crate::query::snap_range_clauses(filters, &ctx);
            &snapped[..]
        } else {
            filters
        };
        let planner_ctx = planner::PlannerContext {
            string_maps: executor.string_maps(),
            dictionaries: executor.dictionaries(),
        };
        let plan = planner::plan_query_with_context(effective_filters, executor.filter_index(), executor.slot_allocator(), Some(&planner_ctx));
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
    /// Per-phase flush timing in nanoseconds: (apply, cache, publish, timebucket, compact, opslog).
    pub fn flush_phase_stats(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.flush_apply_nanos.load(Ordering::Relaxed),
            self.flush_cache_nanos.load(Ordering::Relaxed),
            self.flush_publish_nanos.load(Ordering::Relaxed),
            self.flush_timebucket_nanos.load(Ordering::Relaxed),
            self.flush_compact_nanos.load(Ordering::Relaxed),
            self.flush_opslog_nanos.load(Ordering::Relaxed),
        )
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
    /// Set the WAL writer for the V2 write path. When set, put() and patch_document()
    /// decompose documents into ops and write to WAL instead of directly to coalescer.
    #[cfg(feature = "pg-sync")]
    pub fn set_wal_writer(&mut self, writer: Arc<crate::ops_wal::WalWriter>) {
        self.wal_writer = Some(writer);
    }
    /// Check if a slot is alive (for non-alive slot filtering in ops processing).
    pub fn is_slot_alive(&self, slot: u32) -> bool {
        let snap = self.snapshot();
        snap.slots.is_alive(slot)
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
        let snap = self.snapshot();
        let slot_bytes = snap.slots.bitmap_bytes();
        let filter_bytes = snap.filters.bitmap_bytes();
        let sort_bytes = snap.sorts.bitmap_bytes();
        (slot_bytes, filter_bytes, sort_bytes)
    }
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
    pub fn unified_cache_stats(&self) -> crate::unified_cache::UnifiedCacheStats {
        self.unified_cache.lock().stats()
    }
    /// Return per-entry cache details for diagnostics.
    pub fn unified_cache_entry_details(&self) -> Vec<crate::unified_cache::UnifiedEntryDetail> {
        self.unified_cache.lock().entry_details()
    }
    /// Update the max_maintenance_work budget on the live unified cache.
    pub fn set_max_maintenance_work(&self, v: usize) {
        self.unified_cache.lock().config_mut().max_maintenance_work = v;
    }
    /// Update the max_maintenance_ms time budget on the live unified cache.
    pub fn set_max_maintenance_ms(&self, v: u64) {
        self.unified_cache.lock().config_mut().max_maintenance_ms = v;
    }
    /// Update the max_entries cap on the live unified cache.
    pub fn set_cache_max_entries(&self, v: usize) {
        self.unified_cache.lock().config_mut().max_entries = v;
    }
    /// Update the max_bytes cap on the live unified cache.
    pub fn set_cache_max_bytes(&self, v: usize) {
        self.unified_cache.lock().config_mut().max_bytes = v;
    }
    /// Update the initial_capacity on the live unified cache.
    pub fn set_cache_initial_capacity(&self, v: usize) {
        self.unified_cache.lock().config_mut().initial_capacity = v;
    }
    /// Update the max_capacity on the live unified cache.
    pub fn set_cache_max_capacity(&self, v: usize) {
        self.unified_cache.lock().config_mut().max_capacity = v;
    }
    /// Update the min_filter_size on the live unified cache.
    pub fn set_cache_min_filter_size(&self, v: usize) {
        self.unified_cache.lock().config_mut().min_filter_size = v;
    }
    /// Rebuild all time bucket bitmaps from scratch by scanning the sort field
    /// for all alive slots. Use after a bulk dump or when buckets are empty/stale.
    /// Returns (bucket_count, total_slots_scanned) or an error.
    pub fn rebuild_time_buckets(&self) -> crate::error::Result<(usize, u64)> {
        let tb_arc = self.time_buckets.as_ref().ok_or_else(|| {
            crate::error::BitdexError::Config("no time_buckets configured".into())
        })?;
        let snap = self.snapshot();
        let sort_field_name = {
            let tb = tb_arc.lock();
            tb.sort_field_name().to_string()
        };
        let sort_field = snap.sorts.get_field(&sort_field_name).ok_or_else(|| {
            crate::error::BitdexError::Config(format!(
                "time bucket sort field '{}' not loaded", sort_field_name
            ))
        })?;
        let alive = snap.slots.alive_bitmap();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Collect (slot, timestamp) for all alive slots
        let slot_count = alive.len();
        let mut slot_values: Vec<(u32, u64)> = Vec::with_capacity(slot_count as usize);
        for slot in alive.iter() {
            let ts = sort_field.reconstruct_value(slot) as u64;
            slot_values.push((slot, ts));
        }
        // Rebuild each bucket
        let mut tb = tb_arc.lock();
        let bucket_names: Vec<String> = tb.bucket_names();
        for name in &bucket_names {
            tb.rebuild_bucket(name, slot_values.iter().copied(), now_secs);
        }
        let bucket_count = bucket_names.len();
        // Mark dirty so merge thread persists
        self.dirty_since_snapshot.store(true, std::sync::atomic::Ordering::Release);
        // Invalidate cache — stale entries may hold 0-result bitmaps from before rebuild
        self.unified_cache.lock().clear();
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
    /// Clear unified cache entries and reset counters (RAM only).
    pub fn clear_unified_cache(&self) {
        self.unified_cache.lock().clear();
    }
    /// Purge the entire BoundStore: disk first, then memory.
    /// Order matters: wipe disk before clearing RAM to prevent stale shard loads.
    /// Safe to call while the server is running — the merge thread will simply
    /// start writing fresh data on the next cycle with dirty entries.
    pub fn purge_bounds(&self) -> crate::error::Result<()> {
        // Clear RAM cache (BoundStore removed — no disk to purge).
        self.unified_cache.lock().clear();
        eprintln!("purge_bounds: cleared RAM cache (BoundStore removed)");
        Ok(())
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
    /// Combined exit-loading + save + unload.
    ///
    /// Sends ExitLoadingSaveUnload to the flush thread which publishes the
    /// unloaded version. With BitmapSilo, bitmaps stay in mmap so no reload
    /// tracking is needed after unload.
    pub fn exit_loading_mode_and_save_unload(&self) -> Result<()> {
        let skip_sorts: HashSet<String> = HashSet::new();
        let skip_filters: HashSet<String> = HashSet::new();
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        match self.cmd_tx.send(FlushCommand::ExitLoadingSaveUnload {
            skip_sorts,
            skip_filters,
            loading_mode: Arc::clone(&self.loading_mode),
            done: done_tx,
        }) {
            Ok(()) => {
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
                eprintln!("Warning: flush thread gone, falling back to exit_loading_mode");
                self.exit_loading_mode();
                Ok(())
            }
        }
    }
    /// Save a full snapshot: bitmaps to BitmapSilo, field dict to disk.
    pub fn save_snapshot(&self) -> Result<()> {
        // Save field dictionary
        self.docstore.lock().save_field_dict()
            .map_err(|e| crate::error::BitdexError::Storage(format!("save_field_dict: {e}")))?;

        // Save bitmaps to BitmapSilo
        if let Some(ref bitmap_path) = self.config.storage.bitmap_path {
            let snap = self.snapshot();
            let cursors = self.cursors.lock().clone();
            let mut silo = crate::bitmap_silo::BitmapSilo::open(bitmap_path)
                .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::open: {e}")))?;
            let count = silo.save_all(&snap.filters, &snap.sorts, &snap.slots, &cursors)
                .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::save_all: {e}")))?;
            eprintln!("save_snapshot: saved {} bitmaps to BitmapSilo", count);
        }

        Ok(())
    }
    /// Save a full snapshot to a custom path.
    pub fn save_snapshot_to(&self, path: &Path) -> Result<()> {
        let snap = self.snapshot();
        let cursors = self.cursors.lock().clone();
        let mut silo = crate::bitmap_silo::BitmapSilo::open(path)
            .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::open: {e}")))?;
        silo.save_all(&snap.filters, &snap.sorts, &snap.slots, &cursors)
            .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::save_all: {e}")))?;
        Ok(())
    }
    /// Internal: zero-copy snapshot serialization via BitmapSilo.
    ///
    /// Reads the published snapshot through Arc refs — no InnerEngine clone.
    /// Uses `fused_cow()` to borrow base bitmaps directly (zero copy when clean)
    /// or create temporary merged bitmaps (only when dirty). Processes one field
    /// at a time so memory overhead is minimal (~1.7 MB for tagIds' 31K Cow refs).
    ///
    /// Skips fields that haven't been loaded yet (still pending lazy-load) to avoid
    /// overwriting real persisted data with empty placeholders.
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
    /// Save the current snapshot to disk (via BitmapSilo) and publish a fresh unloaded state.
    /// With BitmapSilo, all bitmaps are in the silo mmap — no lazy reload tracking needed.
    pub fn save_and_unload(&self) -> Result<()> {
        // Build an unloaded snapshot: keep slots (always needed), empty filter/sort fields.
        let snap = self.inner.load_full();
        let slots = snap.slots.clone();
        let mut new_filters = crate::filter::FilterIndex::new();
        for fc in &self.config.filter_fields {
            new_filters.add_field(fc.clone());
        }
        for fc in &self.config.filter_fields {
            new_filters.unload_from(&snap.filters, &fc.name);
        }
        let mut new_sorts = crate::sort::SortIndex::new();
        for sc in &self.config.sort_fields {
            new_sorts.add_field(sc.clone());
        }
        for sc in &self.config.sort_fields {
            new_sorts.unload_from(&snap.sorts, &sc.name);
        }
        drop(snap);
        let unloaded = InnerEngine {
            slots,
            filters: new_filters,
            sorts: new_sorts,
        };
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
                        self.publish_staging(unloaded);
                    }
                }
            }
            Err(_) => {
                self.publish_staging(unloaded);
            }
        }
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
            let old_docs: Vec<Option<StoredDoc>> = statuses
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
            let mut doc_writes: Vec<(u32, StoredDoc)> = Vec::new();

            for (i, &(id, ref doc)) in docs.iter().enumerate() {
                let (_, is_upsert, _) = statuses[i];
                let ops = diff_document(id, old_docs[i].as_ref(), doc, &self.config, is_upsert, &self.field_registry);
                all_ops.extend(ops);
                doc_writes.push((
                    id,
                    StoredDoc {
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
    /// Apply a BitmapAccum's accumulated bitmaps directly to staging.
    ///
    /// Used by the dump pipeline (Sync V2) to apply ops-derived bitmaps
    /// without going through the coalescer channel.
    ///
    /// **Caller must be in loading mode** (`enter_loading_mode()` before first call,
    /// `exit_loading_mode()` after all accums are applied). This avoids the Arc clone
    /// cascade — in loading mode, staging refcount=1 so clone is cheap.
    ///
    /// ORs filter bitmaps, sort layer bitmaps, and alive bitmap into staging.
    pub fn apply_accum(&self, accum: &crate::loader::BitmapAccum) {
        // In loading mode, the flush thread doesn't publish snapshots, so the
        // ArcSwap holds the sole reference. Clone is O(num_fields) — just Arc
        // pointer copies, no deep bitmap clones.
        let snap = self.inner.load_full();
        let mut staging = (*snap).clone();
        drop(snap);
        // Apply filter bitmaps
        for (field_name, value_map) in &accum.filter_maps {
            if let Some(field) = staging.filters.get_field_mut(field_name) {
                for (&value, bitmap) in value_map {
                    field.or_bitmap(value, bitmap);
                }
            }
        }
        // Apply sort layer bitmaps
        for (field_name, layer_map) in &accum.sort_maps {
            if let Some(field) = staging.sorts.get_field_mut(field_name) {
                for (&bit_layer, bitmap) in layer_map {
                    field.or_layer(bit_layer, bitmap);
                }
            }
        }
        // Apply alive bitmap (also updates slot counter)
        staging.slots.alive_or_bitmap(&accum.alive);
        // Store back — in loading mode, no snapshot publish overhead
        self.inner.store(Arc::new(staging));
    }
    /// Remove filter and/or sort fields from the engine.
    ///
    /// Removes the fields from the in-memory staging snapshot and publishes.
    /// Does NOT delete bitmap files on disk — orphaned files are overwritten
    /// on next `save_snapshot` or ignored on boot (field not in config = not loaded).
    /// The caller (server) is responsible for updating the persisted config.
    pub fn remove_fields(
        &self,
        filter_names: &[String],
        sort_names: &[String],
    ) -> Result<Vec<String>> {
        let mut staging = self.clone_staging();
        let mut removed = Vec::new();
        for name in filter_names {
            if staging.filters.remove_field(name) {
                removed.push(name.clone());
            }
        }
        for name in sort_names {
            if staging.sorts.remove_field(name) {
                removed.push(name.clone());
            }
        }
        if !removed.is_empty() {
            self.publish_staging(staging);
            eprintln!("remove_fields: removed {:?}", removed);
        }
        Ok(removed)
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
        // Drop the prefetch_tx sender to signal the prefetch worker to exit,
        // then join it. Must drop before join to avoid deadlock.
        drop(self.prefetch_tx.take());
        if let Some(handle) = self.prefetch_handle.take() {
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
                    eager_load: false,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
                computed: None,
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
            skip_cache: false,
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
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
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
        engine.shutdown();
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
                    eager_load: false,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false,
                    per_value_lazy: false,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
                eager_load: false,
                computed: None,
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
            let mut engine =
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
    fn test_save_snapshot_empty_engine() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());
        // Save snapshot of empty engine
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            engine.save_snapshot().unwrap();
        }
        // Restore from empty snapshot
        {
            let mut engine =
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
            let mut engine =
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
            let mut engine =
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
    fn test_save_and_unload_drops_bitmap_memory() {
        // Verify: save_and_unload drops filter and sort bitmap bytes from the
        // published snapshot. This is the core contract of save_and_unload —
        // clearing in-memory bitmaps to free RSS while leaving the slot
        // allocator intact.
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        let config = test_config_with_bitmap_path(bitmap_path.clone());
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
        engine.shutdown();
        assert_eq!(engine.alive_count(), 2);
        // Capture pre-unload bitmap memory
        let bytes_before = {
            let snap = engine.inner.load_full();
            snap.filters.bitmap_bytes() + snap.sorts.bitmap_bytes()
        };
        assert!(bytes_before > 0, "should have bitmap data before unload");
        // Unload — drops clean bitmaps from the published snapshot
        engine.save_and_unload().unwrap();
        // Verify bitmap memory dropped
        let bytes_after = {
            let snap = engine.inner.load_full();
            snap.filters.bitmap_bytes() + snap.sorts.bitmap_bytes()
        };
        assert!(
            bytes_after < bytes_before,
            "bitmap bytes should drop after save_and_unload: {} -> {}",
            bytes_before,
            bytes_after
        );
        // Alive count is preserved (slot allocator not cleared)
        assert_eq!(engine.alive_count(), 2, "alive count must survive unload");
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
        // test_save_and_unload_drops_bitmap_memory which calls shutdown() first.
        // Capture pre-unload memory from the published snapshot
        let (_, filter_before, sort_before, _, _, _, _) = engine.bitmap_memory_report();
        let total_before = filter_before + sort_before;
        assert!(total_before > 0, "should have bitmap data before unload");
        // Unload while flush thread is still alive
        engine.save_and_unload().unwrap();
        // Give the flush thread a few cycles to potentially re-inflate
        thread::sleep(Duration::from_millis(50));
        // Verify memory dropped in the published snapshot even with flush thread running
        let (_, filter_after, sort_after, _, _, _, _) = engine.bitmap_memory_report();
        let total_after = filter_after + sort_after;
        assert!(
            total_after < total_before / 2,
            "bitmap memory should drop significantly after save_and_unload \
             (before={total_before}, after={total_after}). \
             If this fails, the flush thread's staging is re-inflating the snapshot."
        );
        // Alive count is preserved
        assert_eq!(engine.alive_count(), 500, "alive count must survive unload");
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
    #[test]
    fn test_eager_load_fields_not_pending_after_restore() {
        let dir = tempfile::tempdir().unwrap();
        let bitmap_path = dir.path().join("bitmaps");
        let docstore_path = dir.path().join("docs");
        // Config: nsfwLevel is eager_load=true, onSite is eager_load=false
        let config = Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    behaviors: None,
                    eviction: None,
                    eager_load: true, // <-- eager
                    per_value_lazy: false,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    behaviors: None,
                    eviction: None,
                    eager_load: false, // <-- lazy (default)
                    per_value_lazy: false,
                },
            ],
            sort_fields: vec![
                SortFieldConfig {
                    name: "reactionCount".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                    eager_load: true, // <-- eager
                    computed: None,
                },
            ],
            max_page_size: 100,
            flush_interval_us: 50,
            channel_capacity: 10_000,
            storage: crate::config::StorageConfig {
                bitmap_path: Some(bitmap_path.clone()),
            },
            ..Default::default()
        };
        // Insert some data, save snapshot
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            engine
                .put(
                    1,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("onSite", FieldValue::Single(Value::Bool(true))),
                        ("reactionCount", FieldValue::Single(Value::Integer(42))),
                    ]),
                )
                .unwrap();
            engine
                .put(
                    2,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                        ("onSite", FieldValue::Single(Value::Bool(false))),
                        ("reactionCount", FieldValue::Single(Value::Integer(99))),
                    ]),
                )
                .unwrap();
            engine.shutdown();
            engine.save_snapshot().unwrap();
        }
        // Restore — pending_filter_loads / pending_sort_loads removed (BitmapSilo handles lazy loading).
        // Fields are all queryable after restore via BitmapSilo mmap.
        {
            let mut engine =
                ConcurrentEngine::new_with_path(config.clone(), &docstore_path).unwrap();
            let result = engine
                .query(
                    &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                    Some(&SortClause {
                        field: "reactionCount".to_string(),
                        direction: SortDirection::Desc,
                    }),
                    10,
                )
                .unwrap();
            assert_eq!(result.ids, vec![1]);
        }
    }
    #[test]
    fn test_sync_filter_values_add_and_remove() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        // Insert a doc with tagIds [100, 200]
        engine
            .put(
                1,
                &make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                )]),
            )
            .unwrap();
        wait_for_flush(&engine, 1, 500);
        // Verify initial state
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
        // Sync to [200, 300] — removes 100, keeps 200, adds 300
        engine.sync_filter_values(1, "tagIds", &[200, 300]).unwrap();
        // Wait for mutations to flush
        thread::sleep(Duration::from_millis(50));
        // Tag 100 should no longer match
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 0);
        // Tag 200 should still match
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
        // Tag 300 should now match
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(300))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
        engine.shutdown();
    }
    #[test]
    fn test_sync_filter_values_clear_all() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        engine
            .put(
                1,
                &make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(10), Value::Integer(20)]),
                )]),
            )
            .unwrap();
        wait_for_flush(&engine, 1, 500);
        // Sync to empty — removes all values
        engine.sync_filter_values(1, "tagIds", &[]).unwrap();
        thread::sleep(Duration::from_millis(50));
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(10))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 0);
        engine.shutdown();
    }
    #[test]
    fn test_sync_filter_values_slot_not_alive_skips() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        // Sync on non-existent slot should skip silently (not error)
        let result = engine.sync_filter_values(999, "tagIds", &[100]);
        assert!(result.is_ok(), "sync_filter_values should skip non-alive slots");
        engine.shutdown();
    }
    /// Reproduce the WAL reader stall: ops for alive slots should be applied,
    /// not silently skipped. This test exercises the exact code path used by
    /// the server WAL reader thread.
    #[cfg(feature = "pg-sync")]
    #[test]
    fn test_wal_reader_ops_alive_check() {
        use crate::pg_sync::ops::{EntityOps, Op};
        use crate::ops_processor::{FieldMeta, apply_ops_batch, DocWriter};
        use crate::ingester::CoalescerSink;
        use serde_json::json;

        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert doc to make slot 100 alive
        engine.put(100, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
        ])).unwrap();
        wait_for_flush(&engine, 1, 500);
        assert!(engine.is_slot_alive(100), "slot 100 should be alive");

        // Build ops processor components (same as server WAL reader thread)
        let meta = FieldMeta::from_config(engine.config());
        let sender = engine.mutation_sender();
        let mut sink = CoalescerSink::new(sender);
        let mut doc_writer = DocWriter::new(engine.docstore_arc());

        // Apply ops for alive slot — should succeed
        let mut entries = vec![EntityOps {
            entity_id: 100,
            creates_slot: false,
            ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(16) }],
        }];
        let (applied, skipped, errors) = apply_ops_batch(
            &mut sink, &meta, &mut entries, Some(&engine), Some(&mut doc_writer),
        );
        assert_eq!(applied, 1, "op for alive slot must be applied");
        assert_eq!(skipped, 0, "no ops should be skipped");
        assert_eq!(errors, 0, "no errors expected");

        // Apply ops for non-alive slot below slot_counter — should be skipped
        let sc = engine.slot_counter();
        eprintln!("slot_counter = {sc}");
        let dead_slot: i64 = if sc > 50 { 50 } else { (sc + 100) as i64 };
        let mut entries2 = vec![EntityOps {
            entity_id: dead_slot,
            creates_slot: false,
            ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(8) }],
        }];
        let (applied2, skipped2, errors2) = apply_ops_batch(
            &mut sink, &meta, &mut entries2, Some(&engine), Some(&mut doc_writer),
        );
        if (dead_slot as u32) < sc {
            assert_eq!(skipped2, 1, "non-alive slot below slot_counter should be skipped");
            assert_eq!(applied2, 0);
        } else {
            // Auto-promoted because beyond slot_counter
            assert_eq!(applied2, 1, "slot beyond slot_counter should be auto-promoted");
        }
        assert_eq!(errors2, 0);

        // Apply ops with creates_slot=true for new entity — should succeed
        let new_slot = (sc + 1000) as i64;
        let mut entries3 = vec![EntityOps {
            entity_id: new_slot,
            creates_slot: true,
            ops: vec![Op::Set { field: "nsfwLevel".into(), value: json!(4) }],
        }];
        let (applied3, skipped3, errors3) = apply_ops_batch(
            &mut sink, &meta, &mut entries3, Some(&engine), Some(&mut doc_writer),
        );
        assert_eq!(applied3, 1, "creates_slot=true should always succeed");
        assert_eq!(skipped3, 0);
        assert_eq!(errors3, 0);

        engine.shutdown();
    }
    #[test]
    fn test_patch_document_creates_new_slot() {
        // PATCH on a non-existent slot should fall through to PUT,
        // creating the document and setting bitmaps.
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        let doc = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("tagIds", FieldValue::Multi(vec![Value::Integer(42)])),
        ]);
        // Slot 999 doesn't exist — patch should create it via PUT fallback
        engine.patch_document(999, &doc).unwrap();
        wait_for_flush(&engine, 1, 500);
        // Verify the slot is alive and queryable
        assert_eq!(engine.alive_count(), 1);
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![999]);
        // Verify tag bitmap was set
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(42))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![999]);
        engine.shutdown();
    }
    #[test]
    fn test_patch_document_updates_existing_slot() {
        // PATCH on an existing slot should still work as partial update.
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        // Create the slot first via PUT
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(10)])),
                ]),
            )
            .unwrap();
        wait_for_flush(&engine, 1, 500);
        // PATCH only nsfwLevel — tagIds should be preserved
        let patch = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
        ]);
        engine.patch_document(1, &patch).unwrap();
        thread::sleep(Duration::from_millis(50));
        // nsfwLevel should be updated
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(2))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
        // tagIds should still be there (not wiped by PATCH)
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(10))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
        engine.shutdown();
    }
    // --- Write path audit items 2.11, 2.15, 2.16, 2.17 ---
    #[test]
    fn test_delete_cleans_filter_and_sort_bits() {
        // 2.11: DELETE should clear all filter/sort bitmap bits before clearing alive
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();
        wait_for_flush(&engine, 1, 500);
        // Verify it's queryable before delete
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 1);
        // Delete
        engine.delete(1).unwrap();
        thread::sleep(Duration::from_millis(50));
        // Verify alive is cleared
        assert_eq!(engine.alive_count(), 0);
        // Verify filter bitmaps are clean (no stale bits)
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 0, "nsfwLevel bitmap should be clean after delete");
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 0, "tagIds bitmap should be clean after delete");
        engine.shutdown();
    }
    #[test]
    fn test_multi_value_diff_add_and_remove() {
        // 2.15: Upsert that changes multi-value field should add new values and remove old
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        // Insert with tagIds [100, 200]
        engine
            .put(
                1,
                &make_doc(vec![
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                ]),
            )
            .unwrap();
        wait_for_flush(&engine, 1, 500);
        // Upsert with tagIds [200, 300] — should remove 100, keep 200, add 300
        engine
            .put(
                1,
                &make_doc(vec![
                    ("tagIds", FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)])),
                ]),
            )
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        // Tag 100 should be gone
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 0, "tag 100 should be removed after upsert");
        // Tag 200 should still be there
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
        // Tag 300 should be added
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(300))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
        engine.shutdown();
    }
    #[test]
    fn test_sort_bitmap_updates_on_value_change() {
        // 2.16: Changing a sort field value should update sort layer bitmaps
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        // Insert two docs with different reactionCounts
        engine
            .put(1, &make_doc(vec![
                ("reactionCount", FieldValue::Single(Value::Integer(10))),
            ]))
            .unwrap();
        engine
            .put(2, &make_doc(vec![
                ("reactionCount", FieldValue::Single(Value::Integer(20))),
            ]))
            .unwrap();
        wait_for_flush(&engine, 2, 500);
        // Sort by reactionCount desc — doc 2 (20) should come first
        let result = engine
            .query(
                &[],
                Some(&SortClause {
                    field: "reactionCount".to_string(),
                    direction: SortDirection::Desc,
                }),
                2,
            )
            .unwrap();
        assert_eq!(result.ids, vec![2, 1]);
        // Update doc 1 to have higher reactionCount
        engine
            .put(1, &make_doc(vec![
                ("reactionCount", FieldValue::Single(Value::Integer(30))),
            ]))
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        // Now doc 1 (30) should come first
        let result = engine
            .query(
                &[],
                Some(&SortClause {
                    field: "reactionCount".to_string(),
                    direction: SortDirection::Desc,
                }),
                2,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1, 2]);
        engine.shutdown();
    }
    // -----------------------------------------------------------------------
    // DataSilo E2E integration tests
    // -----------------------------------------------------------------------

    /// E2E: put() writes doc through flush thread → docstore, then get reads it back.
    #[test]
    fn test_docstore_v3_put_and_read_back() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ])).unwrap();

        // Wait for flush thread to persist the doc
        wait_for_flush(&engine, 1, 500);

        // Read the doc back from DataSilo
        let doc = engine.docstore.lock().get(1).unwrap();
        assert!(doc.is_some(), "doc should be readable after put + flush");
        let doc = doc.unwrap();
        assert_eq!(
            doc.fields.get("nsfwLevel"),
            Some(&FieldValue::Single(Value::Integer(5))),
            "nsfwLevel should roundtrip through DataSilo"
        );
        assert_eq!(
            doc.fields.get("reactionCount"),
            Some(&FieldValue::Single(Value::Integer(42))),
            "reactionCount should roundtrip through DataSilo"
        );

        engine.shutdown();
    }

    /// E2E: upsert reads old doc from DataSilo for diff, clears stale bits.
    #[test]
    fn test_docstore_v3_upsert_reads_old_doc() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert doc with nsfwLevel=1
        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(10))),
        ])).unwrap();
        wait_for_flush(&engine, 1, 500);

        // Verify nsfwLevel=1 matches
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
            None, 10,
        ).unwrap();
        assert_eq!(result.ids, vec![1], "nsfwLevel=1 should match before upsert");

        // Upsert with nsfwLevel=3 — this requires reading old doc from DataSilo
        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
            ("reactionCount", FieldValue::Single(Value::Integer(10))),
        ])).unwrap();
        wait_for_flush(&engine, 1, 500);

        // Old nsfwLevel=1 bitmap bit should be cleared (clean delete via docstore diff)
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
            None, 10,
        ).unwrap();
        assert_eq!(result.total_matched, 0, "nsfwLevel=1 should be cleared after upsert to 3");

        // New nsfwLevel=3 should match
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(3))],
            None, 10,
        ).unwrap();
        assert_eq!(result.ids, vec![1], "nsfwLevel=3 should match after upsert");

        // Verify the stored doc has the new values
        let doc = engine.docstore.lock().get(1).unwrap().unwrap();
        assert_eq!(
            doc.fields.get("nsfwLevel"),
            Some(&FieldValue::Single(Value::Integer(3))),
        );

        engine.shutdown();
    }

    /// E2E: delete reads old doc from DataSilo to clear all bitmap bits.
    #[test]
    fn test_docstore_v3_delete_reads_old_doc() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("reactionCount", FieldValue::Single(Value::Integer(99))),
        ])).unwrap();
        wait_for_flush(&engine, 1, 500);

        // Doc should exist
        assert!(engine.docstore.lock().get(1).unwrap().is_some());

        // Delete — this reads old doc from DataSilo to clear filter/sort bits
        engine.delete(1).unwrap();
        wait_for_flush(&engine, 0, 500);

        // Bitmap should be clean (no alive, no filter match)
        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".into(), Value::Integer(2))],
            None, 10,
        ).unwrap();
        assert_eq!(result.total_matched, 0, "nsfwLevel=2 should be cleared after delete");

        engine.shutdown();
    }

    // DocWriter E2E test lives in ops_processor.rs (needs private method access)
}
