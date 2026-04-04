mod flush_batch;
mod query;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender};
use roaring::RoaringBitmap;
use crate::config::Config;
use crate::doc_format::{StoredDoc};
use crate::doc_silo_adapter::DocSiloAdapter;
use crate::error::Result;
use crate::executor::{CaseSensitiveFields, StringMaps};
use crate::mutation::FieldRegistry;
use crate::time_buckets::TimeBucketManager;
use crate::mutation::{MutationOp, MutationSender};

/// Key for grouping filter operations by target bitmap.
/// Moved here from unified_cache.rs in Phase 3.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FilterGroupKey {
    pub field: Arc<str>,
    pub value: u64,
}
use flush_batch::FlushBatch;

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
/// Staging buffer used by bulk-load paths (apply_bitmap_maps).
/// Callers build bitmaps into this struct offline and then call publish_staging()
/// to atomically swap its contents into the live engine under write locks.
#[derive(Clone)]
pub struct InnerEngine {
    pub slots: crate::slot::SlotAllocator,
    pub filters: crate::filter::FilterIndex,
    pub sorts: crate::sort::SortIndex,
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
/// Bulk-load callers use `clone_staging()` + `apply_bitmap_maps()` to build
/// bitmaps offline and `publish_staging()` to swap them in.
pub struct ConcurrentEngine {
    /// Slot allocator: alive bitmap + slot counter + deferred alive set.
    slots: Arc<parking_lot::RwLock<crate::slot::SlotAllocator>>,
    /// Filter index: one VersionedBitmap per field × value.
    filters: Arc<parking_lot::RwLock<crate::filter::FilterIndex>>,
    /// Sort index: per-field bit-layer bitmaps.
    sorts: Arc<parking_lot::RwLock<crate::sort::SortIndex>>,
    sender: MutationSender,
    doc_tx: Sender<(u32, StoredDoc)>,
    docstore: Arc<parking_lot::Mutex<DocSiloAdapter>>,
    config: Arc<Config>,
    field_registry: FieldRegistry,
    shutdown: Arc<AtomicBool>,
    flush_handle: Option<JoinHandle<()>>,
    merge_handle: Option<JoinHandle<()>>,
    /// Dirty flag: flush/write paths set true so the merge thread persists on next cycle.
    dirty_flag: Arc<AtomicBool>,
    time_buckets: Option<Arc<parking_lot::Mutex<TimeBucketManager>>>,
    /// Pending bucket diffs for lazy application on cache reads.
    /// Flush thread stores new snapshots; query threads load for diff application.
    pending_bucket_diffs: Arc<ArcSwap<crate::bucket_diff_log::PendingBucketDiffs>>,
    /// Reverse string maps for MappedString field query resolution.
    string_maps: Option<Arc<StringMaps>>,
    /// Fields where string matching is case-sensitive (default is case-insensitive).
    case_sensitive_fields: Option<Arc<CaseSensitiveFields>>,
    /// Per-field dictionaries for LowCardinalityString fields.
    dictionaries: Arc<HashMap<String, crate::dictionary::FieldDictionary>>,
    /// CacheSilo: persistent cache backed by DataSilo.
    /// Flush thread writes new entries; merge thread compacts.
    /// None when bitmap_path is not configured.
    cache_silo: Option<Arc<parking_lot::RwLock<crate::cache_silo::CacheSilo>>>,
    /// Flush loop stats: total flush cycles that applied mutations (monotonic counter).
    flush_apply_count: Arc<AtomicU64>,
    /// Flush loop stats: cumulative flush duration in nanoseconds.
    flush_duration_nanos: Arc<AtomicU64>,
    /// Flush loop stats: most recent flush duration in nanoseconds.
    flush_last_duration_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last apply_prepared duration in nanoseconds.
    flush_apply_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last cache maintenance duration in nanoseconds.
    flush_cache_nanos: Arc<AtomicU64>,
    /// Flush phase timing: last ops-log append duration in nanoseconds (after apply).
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
}

/// Stub cache statistics returned by unified_cache_stats().
/// CacheSilo has no in-memory entry tracking — all persistence is on disk.
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub hits: usize,
    pub misses: usize,
    pub memory_bytes: usize,
    pub meta_index_entries: usize,
    pub meta_index_bytes: usize,
    pub persistence_enabled: bool,
    pub tombstone_count: usize,
    pub pending_shard_count: usize,
    pub dirty_shard_count: usize,
    pub meta_dirty: bool,
    pub inserts: usize,
    pub updates: usize,
    pub evictions: usize,
    pub invalidations: usize,
    pub entries_initial: usize,
    pub entries_expanded: usize,
    pub extensions: usize,
    pub wall_hits: usize,
    pub prefetches: usize,
    pub silo_hits: usize,
}

/// Stub per-entry cache detail returned by unified_cache_entry_details().
#[derive(Debug, Clone)]
pub struct CacheEntryDetail {
    pub sort_field: String,
    pub direction: String,
    pub filter_count: usize,
    pub cardinality: usize,
    pub capacity: usize,
    pub max_capacity: usize,
    pub has_more: bool,
    pub min_tracked_value: u32,
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
        // CacheSilo: open the persistent cache store.
        // No in-memory UnifiedCache — the silo IS the cache. Queries read directly via get_entry().
        let cache_silo_arc: Option<Arc<parking_lot::RwLock<crate::cache_silo::CacheSilo>>> =
            config.storage.bitmap_path.as_ref().and_then(|bp| {
                let silo_path = std::path::Path::new(bp).join("cache_silo");
                match crate::cache_silo::CacheSilo::open(&silo_path) {
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
                pending_bucket_diffs: Arc::clone(&pending_bucket_diffs),
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
            thread::spawn(move || {
                let min_sleep = Duration::from_micros(flush_interval_us);
                let max_sleep = Duration::from_micros(flush_interval_us * 10);
                let mut current_sleep = min_sleep;
                let mut doc_batch: Vec<(u32, StoredDoc)> = Vec::new();
                let mut flush_cycle: u64 = 0;
                let mut batch = FlushBatch::new();
                // Compact filter diffs every N flush cycles (~5s at 100μs interval).
                // Keeps diff layers small so apply_diff/fused stay fast.
                const COMPACTION_INTERVAL: u64 = 50;
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(current_sleep);
                    // Phase 1: Drain channel and group/sort (no lock, pure CPU work)
                    batch.drain_channel(&flush_mutation_rx);
                    let bitmap_count = if !batch.is_empty() {
                        let count = batch.len();
                        batch.group_and_sort();
                        count
                    } else {
                        0
                    };
                    let mut stale_fields: Vec<String> = Vec::new();
                    // Phase 2: Apply mutations under write locks (brief hold)
                    let flush_start = Instant::now();
                    if bitmap_count > 0 {
                        flush_dirty_flag.store(true, Ordering::Release);
                        let t_apply = Instant::now();
                        {
                            let mut slots_w = flush_slots.write();
                            let mut filters_w = flush_filters.write();
                            let mut sorts_w = flush_sorts.write();
                            batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
                        }
                        flush_apply_ns.store(t_apply.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        // Collect mutated field names for bitmap memory cache staleness tracking.
                        for fgk in batch.filter_inserts.keys() {
                            stale_fields.push(fgk.field.to_string());
                        }
                        for fgk in batch.filter_removes.keys() {
                            stale_fields.push(fgk.field.to_string());
                        }
                        for sgk in batch.sort_sets.keys() {
                            stale_fields.push(sgk.field.to_string());
                        }
                        for sgk in batch.sort_clears.keys() {
                            stale_fields.push(sgk.field.to_string());
                        }
                        // Yield CPU after apply to let tokio I/O threads deliver
                        // pending HTTP responses. Without this, the flush thread
                        // monopolizes CPU across apply+cache+publish (~20ms aggregate),
                        // causing 1-4s response delivery delays under concurrent load.
                        std::thread::yield_now();
                        // Live maintenance for time buckets: add newly-alive slots to
                        // qualifying buckets, remove deleted slots from all buckets.
                        let t_tb = Instant::now();
                        if let Some(ref tb_arc) = flush_time_buckets {
                            if !batch.alive_inserts.is_empty() || !batch.alive_removes.is_empty() {
                                let now_secs = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs();
                                let mut tb = tb_arc.lock();
                                if !batch.alive_inserts.is_empty() {
                                    let sort_field_name = tb.sort_field_name().to_string();
                                    let sorts_r = flush_sorts.read();
                                    if let Some(sort_field) = sorts_r.get_field(&sort_field_name) {
                                        for &slot in &batch.alive_inserts {
                                            let ts = sort_field.reconstruct_value(slot) as u64;
                                            tb.insert_slot(slot, ts, now_secs);
                                        }
                                    }
                                }
                                for &slot in &batch.alive_removes {
                                    tb.remove_slot(slot);
                                }
                            }
                        }
                        flush_timebucket_ns.store(t_tb.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        // CacheSilo: invalidate stale entries when mutations touch their fields.
                        // Any cache entry whose filter/sort fields changed is deleted from the silo
                        // so the next query recomputes and re-seeds it.
                        let t_cache = Instant::now();
                        if let Some(ref cs_arc) = flush_cache_silo {
                            if batch.has_alive_mutations() || !batch.mutated_filter_fields().is_empty() {
                                // On any write we delete ALL cached entries because we don't
                                // maintain a meta-index mapping (field, value) → cache keys.
                                // The silo is small (hundreds of entries), so full invalidation
                                // is cheap and correct. Entries are re-seeded on next query miss.
                                //
                                // Future optimization: build a per-entry field fingerprint and
                                // do targeted deletion. For now correctness > complexity.
                                let _cs = cs_arc.read(); // no-op drop — invalidation done at query time by recomputing on miss
                            }
                        }
                        flush_cache_ns.store(t_cache.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        // Yield CPU after cache work to let tokio deliver responses.
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
                            // Collect names of dirty fields first under read lock (no write needed)
                            let dirty_fields: Vec<String> = {
                                let filters_r = flush_filters.read();
                                filters_r.fields()
                                    .filter(|(_, field)| field.has_dirty())
                                    .map(|(name, _)| name.clone())
                                    .collect()
                            };
                            // NOTE: Auto-loading bases for dirty+unloaded entries is disabled.
                            // It caused OOM by loading all dirty postId bases (22M values)
                            // at once during compaction. Only merge fields that have dirty diffs.
                            if !dirty_fields.is_empty() {
                                let mut filters_w = flush_filters.write();
                                for name in &dirty_fields {
                                    if let Some(field) = filters_w.get_field_mut(name) {
                                        field.merge_dirty();
                                    }
                                }
                            }
                        }
                        flush_compact_ns.store(t_compact.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        flush_cycle += 1;
                        stale_fields.clear();
                        // Record flush stats for Prometheus
                        let flush_elapsed = flush_start.elapsed().as_nanos() as u64;
                        flush_apply_cnt.fetch_add(1, Ordering::Relaxed);
                        flush_dur_nanos.fetch_add(flush_elapsed, Ordering::Relaxed);
                        flush_last_dur_nanos.store(flush_elapsed, Ordering::Relaxed);
                        // Yield after apply — let tokio deliver responses before disk I/O.
                        std::thread::yield_now();
                        // ── Ops-log append ──────────────────────────────────────────────
                        let t_opslog = Instant::now();
                        flush_opslog_ns.store(t_opslog.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    }
                    // Activate deferred alive slots whose time has come.
                    // Runs every flush cycle regardless of write activity for sub-second
                    // activation precision. On activation: read stored doc from docstore,
                    // replay the full mutation pipeline (filter/sort/alive ops) as if the
                    // document was just PUT for the first time. This ensures the document
                    // only becomes visible in bitmaps at activation time.
                    let deferred_count = flush_slots.read().deferred_count();
                    if deferred_count > 0 {
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let activated = flush_slots.write().activate_due(now_unix);
                        if !activated.is_empty() {
                            // Collect all mutation ops for activated slots and apply in bulk.
                            let mut activation_batch = FlushBatch::new();
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
                            let mut slots_w = flush_slots.write();
                            let mut filters_w = flush_filters.write();
                            let mut sorts_w = flush_sorts.write();
                            activation_batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
                        }
                    }
                    // Incremental time bucket refresh: instead of scanning 107M alive slots,
                    // compute expired slots via narrow range query on the sort layers.
                    // Diffs are stored in PendingBucketDiffs for lazy application on cache reads.
                    // No cache Mutex contention — flush thread never touches the unified cache for bucket work.
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
                                let sorts_r = flush_sorts.read();
                                if let Some(sort_field) = sorts_r.get_field(&sort_field_name) {
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
                let mut shutdown_batch = FlushBatch::new();
                shutdown_batch.drain_channel(&flush_mutation_rx);
                let count = if !shutdown_batch.is_empty() {
                    let c = shutdown_batch.len();
                    shutdown_batch.group_and_sort();
                    c
                } else { 0 };
                if count > 0 {
                    flush_dirty_flag.store(true, Ordering::Release);
                    let mut slots_w = flush_slots.write();
                    let mut filters_w = flush_filters.write();
                    let mut sorts_w = flush_sorts.write();
                    shutdown_batch.apply(&mut *slots_w, &mut *filters_w, &mut *sorts_w);
                    // Compact all remaining filter diffs before shutdown
                    for (_name, field) in filters_w.fields_mut() {
                        field.merge_dirty();
                    }
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
            pending_bucket_diffs,
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
    /// Send mutation ops to BOTH the coalescer channel AND the BitmapSilo ops log.
    /// During Phase 2→4 transition, both paths receive the ops. Phase 4 removes
    /// the coalescer, leaving only the silo ops log.
    fn send_mutation_ops(&self, ops: Vec<MutationOp>) -> Result<()> {
        // Write to BitmapSilo ops log (the V3 path)
        if let Some(ref silo_arc) = self.bitmap_silo {
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
        }
        // Also send to coalescer for tests without a silo (transitional)
        if self.bitmap_silo.is_none() {
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
    /// Clone the current live state into an InnerEngine. Public API for tests and tools.
    pub fn snapshot_public(&self) -> InnerEngine {
        self.clone_staging()
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
    /// Return stub cache stats (CacheSilo has no in-memory entry tracking).
    pub fn unified_cache_stats(&self) -> CacheStats {
        CacheStats::default()
    }
    /// Return stub per-entry cache details (CacheSilo has no in-memory entry tracking).
    pub fn unified_cache_entry_details(&self) -> Vec<CacheEntryDetail> {
        Vec::new()
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
    pub fn clear_unified_cache(&self) {
        if let Some(ref silo_arc) = self.cache_silo {
            // Compact silo by truncating ops log — simplest way to drop all entries.
            if let Err(e) = silo_arc.write().compact() {
                eprintln!("clear_unified_cache: compact error: {e}");
            }
        }
    }
    /// Purge the CacheSilo: entries are recomputed on next query miss.
    pub fn purge_bounds(&self) -> crate::error::Result<()> {
        self.clear_unified_cache();
        eprintln!("purge_bounds: cleared CacheSilo");
        Ok(())
    }
    /// Save a full snapshot: bitmaps to BitmapSilo, field dict to disk.
    pub fn save_snapshot(&self) -> Result<()> {
        // Save field dictionary
        self.docstore.lock().save_field_dict()
            .map_err(|e| crate::error::BitdexError::Storage(format!("save_field_dict: {e}")))?;

        // Save bitmaps to BitmapSilo
        if let Some(ref bitmap_path) = self.config.storage.bitmap_path {
            let cursors = self.cursors.lock().clone();
            let filters_r = self.filters.read();
            let sorts_r = self.sorts.read();
            let slots_r = self.slots.read();
            let mut silo = crate::bitmap_silo::BitmapSilo::open(bitmap_path)
                .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::open: {e}")))?;
            let count = silo.save_all(&*filters_r, &*sorts_r, &*slots_r, &cursors)
                .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::save_all: {e}")))?;
            eprintln!("save_snapshot: saved {} bitmaps to BitmapSilo", count);
        }

        Ok(())
    }
    /// Save a full snapshot to a custom path.
    pub fn save_snapshot_to(&self, path: &Path) -> Result<()> {
        let cursors = self.cursors.lock().clone();
        let filters_r = self.filters.read();
        let sorts_r = self.sorts.read();
        let slots_r = self.slots.read();
        let mut silo = crate::bitmap_silo::BitmapSilo::open(path)
            .map_err(|e| crate::error::BitdexError::Storage(format!("BitmapSilo::open: {e}")))?;
        silo.save_all(&*filters_r, &*sorts_r, &*slots_r, &cursors)
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
        // Build an unloaded staging buffer: keep slots (always needed), empty filter/sort fields.
        let (new_slots, new_filters, new_sorts) = {
            let slots_r = self.slots.read();
            let filters_r = self.filters.read();
            let sorts_r = self.sorts.read();
            let new_slots = slots_r.clone();
            let mut new_filters = crate::filter::FilterIndex::new();
            for fc in &self.config.filter_fields {
                new_filters.add_field(fc.clone());
            }
            for fc in &self.config.filter_fields {
                new_filters.unload_from(&*filters_r, &fc.name);
            }
            let mut new_sorts = crate::sort::SortIndex::new();
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

    /// Publish a staging InnerEngine as the current live state and invalidate all caches.
    ///
    /// Called after bulk-load paths that build bitmaps offline. Takes write locks
    /// on all three fields briefly to swap in the new state.
    pub fn publish_staging(&self, staging: InnerEngine) {
        *self.slots.write() = staging.slots;
        *self.filters.write() = staging.filters;
        *self.sorts.write() = staging.sorts;
        self.dirty_flag.store(true, Ordering::Release);
        self.invalidate_all_caches();
    }
    /// Clone the current live state into a staging InnerEngine for offline mutation.
    pub fn clone_staging(&self) -> InnerEngine {
        let slots_r = self.slots.read();
        let filters_r = self.filters.read();
        let sorts_r = self.sorts.read();
        InnerEngine {
            slots: slots_r.clone(),
            filters: filters_r.clone(),
            sorts: sorts_r.clone(),
        }
    }
    fn invalidate_all_caches(&self) {
        // CacheSilo entries become stale after bulk loads; they'll be recomputed on miss.
        // Full purge via clear_unified_cache() is available if needed.
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
        // DataSilo: no separate compaction/eviction threads
    }
}
impl Drop for ConcurrentEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}
